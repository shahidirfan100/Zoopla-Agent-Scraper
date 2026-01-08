/**
 * Zoopla Agent Scraper - Production Ready v2.0.0
 *
 * Features:
 * - HTTP-first with got-scraping (Priority 1: JSON API)
 * - __NEXT_DATA__ extraction from correct path
 * - /_next/data/{buildId}/ JSON API for pagination
 * - Playwright/Camoufox fallback for Cloudflare bypass
 * - JSON-LD + HTML fallbacks (Priority 2)
 * - Full stealth: User-Agent rotation, delays, session persistence
 */

import { PlaywrightCrawler } from '@crawlee/playwright';
import { Actor, Dataset, log } from 'apify';
import { gotScraping } from 'got-scraping';
import { launchOptions as camoufoxLaunchOptions } from 'camoufox-js';
import { firefox } from 'playwright';
import { load as cheerioLoad } from 'cheerio';

// ============================================================================
// CONFIGURATION
// ============================================================================
const BASE_URL = 'https://www.zoopla.co.uk';
const DEFAULT_START_URL = 'https://www.zoopla.co.uk/find-agents/estate-agents/london/';
const MAX_CONCURRENCY = 1;
const AGENTS_PER_PAGE = 25;
const MAX_RETRIES = 3;
const BACKOFF_MS = 2000;

// User-Agent rotation pool
const USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:134.0) Gecko/20100101 Firefox/134.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.2 Safari/605.1.15',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
];

const getRandomUserAgent = () => USER_AGENTS[Math.floor(Math.random() * USER_AGENTS.length)];
const randomDelay = (min = 2000, max = 5000) => min + Math.random() * (max - min);
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

// ============================================================================
// UTILITY FUNCTIONS
// ============================================================================
const cleanText = (text) => (text ? String(text).replace(/\s+/g, ' ').trim() : null);

const ensureAbsoluteUrl = (value) => {
    if (!value) return null;
    let url = value;
    if (typeof url === 'object') {
        url = url.href || url.url || url.value || (typeof url.toString === 'function' ? url.toString() : null);
    }
    if (typeof url !== 'string') return null;
    const trimmed = url.trim();
    if (!trimmed || trimmed.startsWith('data:') || trimmed.startsWith('mailto:') || trimmed.startsWith('tel:')) return null;
    if (trimmed.startsWith('//')) return `https:${trimmed}`;
    if (trimmed.startsWith('http')) return trimmed;
    return `${BASE_URL}${trimmed.startsWith('/') ? '' : '/'}${trimmed}`;
};

const safeJsonParse = (value) => {
    if (!value || typeof value !== 'string') return null;
    try {
        return JSON.parse(value);
    } catch {
        return null;
    }
};

const parseNumber = (value) => {
    if (typeof value === 'number') return Number.isFinite(value) ? value : null;
    if (!value) return null;
    const numeric = String(value).replace(/[^\\d.]/g, '');
    return numeric ? Number(numeric) : null;
};

const extractUkPostcode = (value) => {
    if (!value) return null;
    const text = String(value);
    const patterns = [
        /\b([A-Z]{1,2}\d{1,2}[A-Z]?\s+\d[A-Z]{2})\b/i,
        /\b([A-Z]{1,2}\d{1,2}[A-Z]?\d[A-Z]{2})\b/i,
        /\b([A-Z]{1,2}\d{1,2}[A-Z]?)\s*$/i,
    ];
    for (const pattern of patterns) {
        const match = text.match(pattern);
        if (match) return match[1].toUpperCase();
    }
    return null;
};

const normalizePhone = (value) => {
    if (!value) return null;
    const raw = String(value).replace('tel:', '').trim();
    const match = raw.match(/(\+44\s?7\d{3}|\+44\s?\d{2}|\d{2,4})\s?\d{3,4}\s?\d{3,4}/);
    return match ? match[0].replace(/\s+/g, ' ').trim() : null;
};

const buildSearchUrlForPage = (startUrl, page) => {
    const url = new URL(startUrl);
    url.searchParams.delete('page');
    if (page > 1) {
        url.searchParams.set('pn', String(page));
    } else {
        url.searchParams.delete('pn');
    }
    return url.toString();
};

// ============================================================================
// HTTP-FIRST FETCHING (Priority 1: JSON API)
// ============================================================================
const fetchWithGot = async (url, proxyUrl, retries = MAX_RETRIES) => {
    for (let attempt = 1; attempt <= retries; attempt++) {
        try {
            const response = await gotScraping({
                url,
                proxyUrl,
                http2: true,
                timeout: { request: 30000 },
                headers: {
                    'User-Agent': getRandomUserAgent(),
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
                    'Accept-Language': 'en-GB,en;q=0.9',
                    'Accept-Encoding': 'gzip, deflate, br',
                    'Cache-Control': 'no-cache',
                    'Pragma': 'no-cache',
                    'sec-ch-ua': '"Not A(Brand";v="8", "Chromium";v="131", "Google Chrome";v="131"',
                    'sec-ch-ua-mobile': '?0',
                    'sec-ch-ua-platform': '"Windows"',
                    'sec-fetch-dest': 'document',
                    'sec-fetch-mode': 'navigate',
                    'sec-fetch-site': 'none',
                    'sec-fetch-user': '?1',
                    'upgrade-insecure-requests': '1',
                },
                responseType: 'text',
            });

            if (response.statusCode === 200) {
                return { success: true, body: response.body, statusCode: 200 };
            }
            if (response.statusCode === 403 || response.statusCode === 429) {
                log.warning(`HTTP ${response.statusCode} on attempt ${attempt}/${retries}`);
                if (attempt < retries) {
                    await sleep(BACKOFF_MS * Math.pow(2, attempt - 1));
                }
            }
        } catch (error) {
            log.warning(`Fetch error attempt ${attempt}: ${error.message}`);
            if (attempt < retries) {
                await sleep(BACKOFF_MS * Math.pow(2, attempt - 1));
            }
        }
    }
    return { success: false, body: null, statusCode: 0 };
};

const fetchJsonApi = async (apiUrl, proxyUrl) => {
    try {
        const response = await gotScraping({
            url: apiUrl,
            proxyUrl,
            http2: true,
            timeout: { request: 20000 },
            headers: {
                'User-Agent': getRandomUserAgent(),
                'Accept': 'application/json, text/plain, */*',
                'Accept-Language': 'en-GB,en;q=0.9',
                'Referer': BASE_URL,
            },
            responseType: 'json',
        });
        if (response.statusCode === 200) {
            return response.body;
        }
    } catch (error) {
        log.debug(`JSON API fetch failed: ${error.message}`);
    }
    return null;
};

// ============================================================================
// __NEXT_DATA__ EXTRACTION (Direct path access)
// ============================================================================
const extractNextDataFromHtml = (html) => {
    const match = html.match(/<script[^>]+id="__NEXT_DATA__"[^>]*>([\s\S]*?)<\/script>/i);
    if (!match) return null;
    return safeJsonParse(match[1]?.trim());
};

const buildNextDataApiUrl = (pageUrl, buildId) => {
    if (!buildId) return null;
    const url = new URL(pageUrl);
    let path = url.pathname;
    if (path.endsWith('/')) path = path.slice(0, -1);
    return `${url.origin}/_next/data/${buildId}${path}.json${url.search}`;
};

// Extract agents directly from the correct path in __NEXT_DATA__
const extractAgentsFromNextData = (nextData) => {
    if (!nextData?.props?.pageProps?.data?.agents?.results) {
        return { agents: [], totalCount: 0, buildId: nextData?.buildId || null };
    }

    const agentsData = nextData.props.pageProps.data.agents;
    const results = agentsData.results || [];
    const totalCount = agentsData.totalCount || results.length;
    const buildId = nextData.buildId || null;

    const agents = results.map((agent) => normalizeZooplaAgent(agent, 'api'));
    return { agents, totalCount, buildId };
};

// Normalize Zoopla agent record to output format
const normalizeZooplaAgent = (agent, source) => {
    if (!agent || typeof agent !== 'object') return null;

    // Extract listing statistics
    const residential = agent.listingsStatistics?.residential || {};
    const forSale = residential.forSale || {};
    const toRent = residential.toRent || {};

    const agentId = agent.id || agent.branchId || null;
    const name = cleanText(agent.displayName || agent.name || agent.branchName);
    const displayAddress = cleanText(agent.displayAddress || agent.address);

    return {
        agentId: agentId ? String(agentId) : null,
        name,
        branchName: cleanText(agent.branchName) || name,
        companyName: cleanText(agent.companyName || agent.company),
        url: ensureAbsoluteUrl(agent.uriName ? `/find-agents/branch/${agent.uriName}/${agent.id}/` : agent.url),
        address: displayAddress,
        postalCode: extractUkPostcode(displayAddress),
        locality: cleanText(agent.locality || agent.town),
        phone: normalizePhone(agent.contactNumber || agent.telephone || agent.phone),
        website: ensureAbsoluteUrl(agent.website),
        logo: ensureAbsoluteUrl(agent.logo),
        rating: parseNumber(agent.rating || agent.ratingValue),
        reviewCount: parseNumber(agent.reviewCount),
        listingsForSale: parseNumber(forSale.availableListings),
        listingsToRent: parseNumber(toRent.availableListings),
        avgAskingPrice: parseNumber(forSale.avgAskingPrice),
        avgRentPrice: parseNumber(toRent.avgAskingPrice),
        featured: Boolean(agent.featured),
        source,
    };
};

// ============================================================================
// JSON-LD EXTRACTION (Fallback)
// ============================================================================
const extractAgentsFromJsonLd = (html) => {
    const $ = cheerioLoad(html);
    const scripts = $('script[type="application/ld+json"]');
    const results = [];

    scripts.each((_, scriptEl) => {
        const jsonText = $(scriptEl).contents().text();
        const parsed = safeJsonParse(jsonText);
        if (!parsed) return;

        const walk = (node) => {
            if (!node || typeof node !== 'object') return;
            if (Array.isArray(node)) {
                node.forEach(walk);
                return;
            }
            const nodeType = node['@type'];
            const types = Array.isArray(nodeType) ? nodeType : nodeType ? [nodeType] : [];
            if (types.some((t) => ['RealEstateAgent', 'RealEstateAgency', 'Organization', 'LocalBusiness'].includes(t))) {
                const normalized = normalizeZooplaAgent(node, 'json-ld');
                if (normalized?.name) results.push(normalized);
            }
            for (const value of Object.values(node)) {
                if (value && typeof value === 'object') walk(value);
            }
        };
        walk(parsed);
    });

    return results;
};

// ============================================================================
// HTML EXTRACTION (Final Fallback)
// ============================================================================
const extractAgentsFromHtml = (html) => {
    const $ = cheerioLoad(html);
    const results = [];
    const seen = new Set();

    // Find agent cards via links to branch pages
    const branchLinks = $('a[href*="/find-agents/branch/"], a[href*="/estate-agents/branch/"]');

    branchLinks.each((_, linkEl) => {
        const link = $(linkEl);
        const href = link.attr('href');
        const url = ensureAbsoluteUrl(href);
        if (!url || seen.has(url)) return;
        seen.add(url);

        // Find parent card container
        const card = link.closest('article, li, div[class*="agent"], div[class*="card"]');
        const cardText = card.length ? card.text() : '';

        // Extract agent ID from URL
        const idMatch = href?.match(/\/(\d+)\/?$/);
        const agentId = idMatch ? idMatch[1] : null;

        const name = cleanText(
            card.find('h1, h2, h3, h4').first().text() ||
            link.find('[class*="name"]').text() ||
            link.text()
        );

        const address = cleanText(
            card.find('address').first().text() ||
            card.find('[class*="address"]').first().text()
        );

        const phone = normalizePhone(
            card.find('a[href^="tel:"]').first().attr('href')
        );

        const logo = ensureAbsoluteUrl(
            card.find('img[alt*="logo" i]').first().attr('src') ||
            card.find('img').first().attr('src')
        );

        // Parse listing counts from text
        const forSaleMatch = cardText.match(/(\d+)\s+propert(?:y|ies)\s+for\s+sale/i);
        const toRentMatch = cardText.match(/(\d+)\s+propert(?:y|ies)\s+to\s+rent/i);

        if (name) {
            results.push({
                agentId,
                name,
                branchName: name,
                companyName: null,
                url,
                address,
                postalCode: extractUkPostcode(address),
                locality: null,
                phone,
                website: null,
                logo,
                rating: null,
                reviewCount: null,
                listingsForSale: forSaleMatch ? Number(forSaleMatch[1]) : null,
                listingsToRent: toRentMatch ? Number(toRentMatch[1]) : null,
                avgAskingPrice: null,
                avgRentPrice: null,
                featured: false,
                source: 'html',
            });
        }
    });

    return results;
};

// ============================================================================
// DEDUPLICATION
// ============================================================================
const dedupeAgents = (agents) => {
    const seen = new Set();
    return agents.filter((agent) => {
        const key = agent.agentId || agent.url || `${agent.name}|${agent.address}`;
        if (!key || seen.has(key)) return false;
        seen.add(key);
        return true;
    });
};

// ============================================================================
// MAIN ACTOR
// ============================================================================
await Actor.init();

try {
    const input = (await Actor.getInput()) || {};

    // Parse inputs
    const startUrls = Array.isArray(input.startUrls) && input.startUrls.length ? input.startUrls : null;
    const startUrl = input.startUrl || (startUrls ? null : DEFAULT_START_URL);

    if (!startUrl && !startUrls) {
        log.error('Missing startUrl or startUrls');
        await Actor.exit({ exitCode: 1 });
    }

    const resultsWanted = Math.max(1, Number.isFinite(+input.results_wanted) ? +input.results_wanted : 50);
    const maxPagesInput = Number.isFinite(+input.max_pages) ? Math.max(1, +input.max_pages) : null;
    const estimatedPages = Math.ceil(resultsWanted / AGENTS_PER_PAGE);
    const maxPages = maxPagesInput ?? Math.max(1, estimatedPages);

    const proxyConfiguration = await Actor.createProxyConfiguration({
        useApifyProxy: true,
        apifyProxyGroups: ['RESIDENTIAL'],
        countryCode: 'GB',
        ...input.proxyConfiguration,
    });

    log.info('🏠 Zoopla Agent Scraper v2.0.0', { resultsWanted, maxPages });

    const seen = new Set();
    let saved = 0;
    let buildId = null;

    // Process each start URL
    const targets = startUrls || [startUrl];

    for (const targetUrl of targets) {
        if (saved >= resultsWanted) break;

        log.info(`📍 Starting: ${targetUrl}`);

        // Get proxy URL for got-scraping
        const proxyInfo = await proxyConfiguration.newUrl();

        // Phase 1: Try HTTP-first approach with got-scraping
        for (let page = 1; page <= maxPages && saved < resultsWanted; page++) {
            await sleep(randomDelay(2000, 4000));

            const pageUrl = buildSearchUrlForPage(targetUrl, page);
            log.info(`📄 Page ${page}/${maxPages}`);

            let agents = [];
            let usedPlaywright = false;

            // Try JSON API if we have buildId from previous page
            if (buildId && page > 1) {
                const apiUrl = buildNextDataApiUrl(pageUrl, buildId);
                if (apiUrl) {
                    log.debug(`Trying JSON API: ${apiUrl}`);
                    const apiData = await fetchJsonApi(apiUrl, proxyInfo);
                    if (apiData?.pageProps?.data?.agents?.results) {
                        const extracted = extractAgentsFromNextData({ props: apiData, buildId });
                        agents = extracted.agents;
                        log.info(`✅ JSON API: ${agents.length} agents`);
                    }
                }
            }

            // Fallback: HTTP fetch HTML and extract __NEXT_DATA__
            if (!agents.length) {
                const httpResult = await fetchWithGot(pageUrl, proxyInfo);

                if (httpResult.success) {
                    const html = httpResult.body;

                    // Check for Cloudflare block
                    if (html.includes('Just a moment') || html.includes('Verify you are human')) {
                        log.warning('⚠️ Cloudflare detected, falling back to Playwright');
                        usedPlaywright = true;
                    } else {
                        // Extract from __NEXT_DATA__ (Priority 1)
                        const nextData = extractNextDataFromHtml(html);
                        if (nextData) {
                            const extracted = extractAgentsFromNextData(nextData);
                            agents = extracted.agents;
                            buildId = extracted.buildId || buildId;
                            if (agents.length) {
                                log.info(`✅ __NEXT_DATA__: ${agents.length} agents (buildId: ${buildId})`);
                            }
                        }

                        // Fallback: JSON-LD (Priority 2)
                        if (!agents.length) {
                            agents = extractAgentsFromJsonLd(html);
                            if (agents.length) {
                                log.info(`✅ JSON-LD: ${agents.length} agents`);
                            }
                        }

                        // Fallback: HTML parsing (Priority 3)
                        if (!agents.length) {
                            agents = extractAgentsFromHtml(html);
                            if (agents.length) {
                                log.info(`✅ HTML: ${agents.length} agents`);
                            }
                        }
                    }
                } else {
                    log.warning('⚠️ HTTP fetch failed, falling back to Playwright');
                    usedPlaywright = true;
                }
            }

            // Final fallback: Playwright with Camoufox
            if (usedPlaywright || !agents.length) {
                log.info('🎭 Using Playwright/Camoufox fallback');

                const camoufoxOptions = await camoufoxLaunchOptions({ headless: true, geoip: true });
                const browser = await firefox.launch(camoufoxOptions);

                try {
                    const context = await browser.newContext({
                        proxy: proxyInfo ? { server: proxyInfo } : undefined,
                    });
                    const playwrightPage = await context.newPage();

                    await playwrightPage.goto(pageUrl, { waitUntil: 'domcontentloaded', timeout: 60000 });
                    await sleep(3000);

                    // Handle Cloudflare
                    const content = await playwrightPage.content();
                    if (content.includes('Just a moment') || content.includes('Verify you are human')) {
                        log.info('⏳ Waiting for Cloudflare...');
                        await sleep(10000);
                        await playwrightPage.waitForLoadState('networkidle', { timeout: 30000 }).catch(() => { });
                    }

                    // Scroll to load content
                    for (let i = 0; i < 3; i++) {
                        await playwrightPage.evaluate(() => window.scrollBy(0, 500));
                        await sleep(500);
                    }

                    const html = await playwrightPage.content();

                    // Extract with fallback chain
                    const nextData = extractNextDataFromHtml(html);
                    if (nextData) {
                        const extracted = extractAgentsFromNextData(nextData);
                        agents = extracted.agents;
                        buildId = extracted.buildId || buildId;
                    }
                    if (!agents.length) agents = extractAgentsFromJsonLd(html);
                    if (!agents.length) agents = extractAgentsFromHtml(html);

                    log.info(`🎭 Playwright: ${agents.length} agents`);
                } finally {
                    await browser.close();
                }
            }

            // Dedupe and save
            agents = dedupeAgents(agents);

            const toSave = [];
            for (const agent of agents) {
                if (saved >= resultsWanted) break;
                const key = agent.agentId || agent.url || `${agent.name}|${agent.address}`;
                if (!key || seen.has(key)) continue;
                seen.add(key);

                toSave.push({
                    ...agent,
                    scrapedAt: new Date().toISOString(),
                });
                saved++;
            }

            if (toSave.length) {
                await Dataset.pushData(toSave);
                log.info(`💾 Saved ${saved}/${resultsWanted} agents`);
            }

            // Stop if no agents found (likely last page)
            if (!agents.length) {
                log.info('📭 No more agents found, stopping pagination');
                break;
            }
        }
    }

    log.info(`✨ Done! Scraped ${saved} agents`);
    await Actor.setStatusMessage(`Scraped ${saved} agents`);

} catch (error) {
    log.error(`Error: ${error.message}`);
    throw error;
} finally {
    await Actor.exit();
}
