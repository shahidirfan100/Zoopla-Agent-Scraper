/**
 * Zoopla Agent Scraper - Production Ready v2.1.0
 *
 * Features:
 * - Playwright/Camoufox ONLY (no HTTP requests)
 * - __NEXT_DATA__ extraction from correct path
 * - JSON-LD + HTML fallbacks
 * - Full stealth: Camoufox fingerprinting, session persistence
 * - Proper proxy integration with PlaywrightCrawler
 */

import { PlaywrightCrawler } from '@crawlee/playwright';
import { Actor, Dataset, log } from 'apify';
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

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const randomDelay = (min = 2000, max = 5000) => min + Math.random() * (max - min);

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
    const numeric = String(value).replace(/[^\d.]/g, '');
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
// __NEXT_DATA__ EXTRACTION (Direct path access)
// ============================================================================
const extractNextDataFromHtml = (html) => {
    const match = html.match(/<script[^>]+id="__NEXT_DATA__"[^>]*>([\s\S]*?)<\/script>/i);
    if (!match) return null;
    return safeJsonParse(match[1]?.trim());
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

    const agents = results.map((agent) => normalizeZooplaAgent(agent, 'api')).filter(Boolean);
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

    if (!name) return null;

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

    // Proxy configuration - use Apify proxy properly
    const proxyConfiguration = await Actor.createProxyConfiguration({
        useApifyProxy: true,
        apifyProxyGroups: ['RESIDENTIAL'],
        countryCode: 'GB',
        ...input.proxyConfiguration,
    });

    log.info('🏠 Zoopla Agent Scraper v2.1.0 (Playwright Only)', { resultsWanted, maxPages });

    const seen = new Set();
    const queued = new Set();
    let saved = 0;

    // Build request queue
    const requestQueue = await Actor.openRequestQueue();
    const targets = startUrls || [startUrl];

    for (const target of targets) {
        const url = buildSearchUrlForPage(target, 1);
        queued.add(url);
        await requestQueue.addRequest({
            url,
            userData: { page: 1, rootUrl: target },
        });
    }

    // Camoufox stealth options
    const camoufoxOptions = await camoufoxLaunchOptions({ headless: true, geoip: true });

    const crawler = new PlaywrightCrawler({
        requestQueue,
        proxyConfiguration,
        maxConcurrency: MAX_CONCURRENCY,
        maxRequestRetries: 5,
        retryOnBlocked: true,
        useSessionPool: true,
        persistCookiesPerSession: true,
        sessionPoolOptions: {
            maxPoolSize: 10,
            sessionOptions: {
                maxUsageCount: 3,
            },
            blockedStatusCodes: [403, 429, 503],
        },
        requestHandlerTimeoutSecs: 180,
        navigationTimeoutSecs: 120,

        // Use Firefox with Camoufox stealth
        launchContext: {
            launcher: firefox,
            launchOptions: {
                ...camoufoxOptions,
                args: [...(camoufoxOptions.args || [])],
            },
        },

        browserPoolOptions: {
            useFingerprints: false,
            maxOpenPagesPerBrowser: 1,
            retireBrowserAfterPageCount: 3,
        },

        // Pre-navigation: random delay for stealth
        preNavigationHooks: [
            async () => {
                const delay = randomDelay(3000, 6000);
                log.debug(`Pre-nav delay: ${Math.round(delay)}ms`);
                await sleep(delay);
            },
        ],

        // Post-navigation: wait for content and scroll
        postNavigationHooks: [
            async ({ page }) => {
                await page.waitForLoadState('domcontentloaded');
                await sleep(2000);

                // Check for Cloudflare
                const content = await page.content();
                if (content.includes('Just a moment') || content.includes('Verify you are human')) {
                    log.info('⏳ Cloudflare detected, waiting...');
                    await sleep(10000);
                    await page.waitForLoadState('networkidle', { timeout: 60000 }).catch(() => { });
                    await sleep(3000);
                }

                // Scroll to simulate human behavior
                for (let i = 0; i < 4; i++) {
                    await page.evaluate(() => window.scrollBy(0, 400));
                    await sleep(300 + Math.random() * 300);
                }
                await sleep(1000);
            },
        ],

        async requestHandler({ request, page }) {
            const pageNum = request.userData.page || 1;

            if (saved >= resultsWanted) {
                log.debug(`Skip page ${pageNum} - target reached`);
                return;
            }

            log.info(`📄 Page ${pageNum}/${maxPages}`);

            // Get page content
            const html = await page.content();

            // Check if still on Cloudflare
            if (html.includes('Just a moment') || html.includes('Verify you are human')) {
                log.warning('⚠️ Still on Cloudflare, retrying...');
                throw new Error('Cloudflare challenge not passed');
            }

            let agents = [];

            // Priority 1: Extract from __NEXT_DATA__
            const nextData = extractNextDataFromHtml(html);
            if (nextData) {
                const extracted = extractAgentsFromNextData(nextData);
                agents = extracted.agents;
                if (agents.length) {
                    log.info(`✅ __NEXT_DATA__: ${agents.length} agents`);
                }
            }

            // Priority 2: JSON-LD fallback
            if (!agents.length) {
                agents = extractAgentsFromJsonLd(html);
                if (agents.length) {
                    log.info(`✅ JSON-LD: ${agents.length} agents`);
                }
            }

            // Priority 3: HTML parsing fallback
            if (!agents.length) {
                agents = extractAgentsFromHtml(html);
                if (agents.length) {
                    log.info(`✅ HTML: ${agents.length} agents`);
                }
            }

            if (!agents.length) {
                log.warning('⚠️ No agents found on this page');
                return;
            }

            // Dedupe
            agents = dedupeAgents(agents);

            // Save agents
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

            // Enqueue next page
            if (pageNum < maxPages && saved < resultsWanted) {
                const nextUrl = buildSearchUrlForPage(request.userData.rootUrl, pageNum + 1);
                if (!queued.has(nextUrl)) {
                    queued.add(nextUrl);
                    await requestQueue.addRequest({
                        url: nextUrl,
                        userData: {
                            page: pageNum + 1,
                            rootUrl: request.userData.rootUrl,
                        },
                    });
                    log.debug(`📥 Enqueued page ${pageNum + 1}`);
                }
            }
        },

        async failedRequestHandler({ request, error }) {
            log.error(`❌ Failed: ${request.url} - ${error.message}`);
        },
    });

    await crawler.run();

    log.info(`✨ Done! Scraped ${saved} agents`);
    await Actor.setStatusMessage(`Scraped ${saved} agents`);

} catch (error) {
    log.error(`Fatal error: ${error.message}`);
    throw error;
} finally {
    await Actor.exit();
}
