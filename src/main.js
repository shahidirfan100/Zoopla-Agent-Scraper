/**
 * Zoopla Agent Scraper - Production Ready v1.0.0
 *
 * Updates:
 * - Targets estate agent listings instead of properties
 * - API-first extraction with JSON parsing
 * - JSON-LD + HTML fallbacks
 * - Pagination tuned for agent directory pages
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
const AGENTS_PER_PAGE_ESTIMATE = 20;

// ============================================================================
// UTILITY FUNCTIONS
// ============================================================================
const cleanText = (text) => (text ? String(text).replace(/\s+/g, ' ').trim() : null);
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const ensureAbsoluteUrl = (value) => {
    if (!value) return null;
    let url = value;
    if (typeof url === 'object') {
        url = url.href || url.url || url.value || (typeof url.toString === 'function' ? url.toString() : null);
    }
    if (typeof url !== 'string') return null;
    const trimmed = url.trim();
    if (!trimmed) return null;
    if (trimmed.startsWith('data:') || trimmed.startsWith('mailto:') || trimmed.startsWith('tel:')) return null;
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

const buildAddressFromObject = (addressObj) => {
    if (!addressObj || typeof addressObj !== 'object') return null;
    const parts = [
        addressObj.streetAddress,
        addressObj.addressLocality,
        addressObj.addressRegion,
        addressObj.postalCode,
        addressObj.addressCountry,
    ].map(cleanText).filter(Boolean);
    return parts.length ? parts.join(', ') : null;
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

const extractAgentIdFromUrl = (url) => {
    if (!url) return null;
    const match = url.match(/\/branch\/[^/]*\/(\d+)\b/i);
    if (match) return match[1];
    const numericTail = url.match(/\/(\d+)\/?$/);
    if (numericTail) return numericTail[1];
    const slugMatch = url.match(/\/branch\/([^/]+)\/?/i);
    return slugMatch ? slugMatch[1] : null;
};

const parseCountFromText = (text, pattern) => {
    if (!text) return null;
    const match = text.match(pattern);
    if (!match) return null;
    const value = match[1]?.replace(/,/g, '');
    return value ? Number(value) : null;
};

const getPageParamName = (url) => {
    if (url.searchParams.has('pn')) return 'pn';
    if (url.searchParams.has('page')) return 'page';
    return 'pn';
};

const buildSearchUrlForPage = (startUrl, page) => {
    const url = new URL(startUrl);
    const pageParam = getPageParamName(url);
    if (page > 1) {
        url.searchParams.set(pageParam, String(page));
    } else {
        url.searchParams.delete(pageParam);
    }
    return url.toString();
};

// ============================================================================
// API (NEXT DATA) EXTRACTION
// ============================================================================
const extractNextDataFromHtml = (html) => {
    const match = html.match(/<script[^>]+id="__NEXT_DATA__"[^>]*>([\s\S]*?)<\/script>/i);
    if (!match) return null;
    return safeJsonParse(match[1]?.trim());
};

const buildNextDataUrl = (pageUrl, buildId) => {
    if (!buildId) return null;
    const url = new URL(pageUrl);
    const path = url.pathname.endsWith('/') ? url.pathname.slice(0, -1) : url.pathname;
    const dataPath = `${path}.json`;
    return `${url.origin}/_next/data/${buildId}${dataPath}${url.search}`;
};

const looksLikeAgentRecord = (record) => {
    if (!record || typeof record !== 'object') return false;
    const name = record.name || record.branchName || record.branch_name || record.companyName || record.company_name;
    const url = record.url || record.branchUrl || record.profileUrl || record.branch_url;
    const address = record.address || record.displayAddress || record.address_line || record.addressLine;
    const id = record.branchId || record.branch_id || record.agentId || record.id;
    return Boolean(name && (url || address || id));
};

const normalizeAgentRecord = (record, source) => {
    if (!record || typeof record !== 'object') return null;

    const addressObj = record.address && typeof record.address === 'object' ? record.address : null;
    const structuredAddress = buildAddressFromObject(addressObj);
    const address = cleanText(
        record.displayAddress ||
        record.address ||
        record.address_line ||
        record.addressLine ||
        structuredAddress
    );

    const locality = cleanText(
        record.locality ||
        record.town ||
        record.city ||
        addressObj?.addressLocality
    );

    const url = ensureAbsoluteUrl(
        record.url ||
        record.branchUrl ||
        record.profileUrl ||
        record.branch_url ||
        record.link
    );

    const name = cleanText(
        record.name ||
        record.branchName ||
        record.branch_name ||
        record.companyName ||
        record.company_name ||
        record.legalName
    );

    const branchName = cleanText(record.branchName || record.branch_name || record.branch);
    const companyName = cleanText(record.companyName || record.company_name || record.company || record.firmName);

    const phone = normalizePhone(
        record.telephone ||
        record.phone ||
        record.phoneNumber ||
        record.contactTelephone ||
        record.contact_phone ||
        record.contact?.telephone ||
        record.contact?.phone ||
        record.contact?.phoneNumber
    );

    const website = ensureAbsoluteUrl(
        record.website ||
        record.websiteUrl ||
        record.website_url ||
        record.contact?.url
    );

    const logo = ensureAbsoluteUrl(
        record.logo ||
        record.image ||
        record.branding?.logo ||
        record.branding?.logoUrl
    );

    const rating = parseNumber(
        record.rating ||
        record.ratingValue ||
        record.averageRating ||
        record.aggregateRating?.ratingValue
    );

    const reviewCount = parseNumber(
        record.reviewCount ||
        record.reviewsCount ||
        record.review_count ||
        record.aggregateRating?.reviewCount
    );

    const listingsForSale = parseNumber(
        record.listingsForSale ||
        record.listings_for_sale ||
        record.propertiesForSale ||
        record.numForSale
    );

    const listingsToRent = parseNumber(
        record.listingsToRent ||
        record.listings_to_rent ||
        record.propertiesToRent ||
        record.numToRent
    );

    const agentId = record.branchId || record.branch_id || record.agentId || record.id || extractAgentIdFromUrl(url);
    const postalCode = record.postcode || addressObj?.postalCode || extractUkPostcode(address);

    return {
        agentId: agentId ? String(agentId) : null,
        name,
        branchName: branchName || name,
        companyName,
        url,
        address,
        postalCode: postalCode ? String(postalCode).toUpperCase() : null,
        locality,
        phone,
        website,
        logo,
        rating,
        reviewCount,
        listingsForSale,
        listingsToRent,
        source,
    };
};

const extractAgentsFromApiPayload = (payload, source) => {
    if (!payload) return [];
    const results = [];
    const seen = new WeakSet();
    const queue = [payload];
    let steps = 0;

    while (queue.length && steps < 20000) {
        const current = queue.shift();
        steps += 1;

        if (!current || typeof current !== 'object') continue;
        if (seen.has(current)) continue;
        seen.add(current);

        if (Array.isArray(current)) {
            for (const entry of current) queue.push(entry);
            continue;
        }

        if (looksLikeAgentRecord(current)) {
            const normalized = normalizeAgentRecord(current, source);
            if (normalized) results.push(normalized);
        }

        for (const value of Object.values(current)) {
            if (value && typeof value === 'object') queue.push(value);
        }
    }

    return results;
};

// ============================================================================
// JSON-LD EXTRACTION
// ============================================================================
const extractAgentsFromJsonLd = (html) => {
    const $ = cheerioLoad(html);
    const scripts = $('script[type="application/ld+json"]');
    const results = [];
    const visited = new WeakSet();

    const walk = (node) => {
        if (!node || typeof node !== 'object') return;
        if (visited.has(node)) return;
        visited.add(node);

        if (Array.isArray(node)) {
            node.forEach(walk);
            return;
        }

        const nodeType = node['@type'];
        const types = Array.isArray(nodeType) ? nodeType : nodeType ? [nodeType] : [];
        const isAgentType = types.some((type) => [
            'RealEstateAgent',
            'RealEstateAgency',
            'Organization',
            'LocalBusiness',
            'ProfessionalService',
        ].includes(type));

        if (isAgentType) {
            const normalized = normalizeAgentRecord(node, 'json-ld');
            if (normalized) results.push(normalized);
        }

        if (node.itemListElement) walk(node.itemListElement);
        if (node.item) walk(node.item);
        if (node.mainEntity) walk(node.mainEntity);
        if (node.about) walk(node.about);

        for (const value of Object.values(node)) {
            if (value && typeof value === 'object') walk(value);
        }
    };

    scripts.each((_, scriptEl) => {
        const jsonText = $(scriptEl).contents().text();
        const parsed = safeJsonParse(jsonText);
        if (parsed) walk(parsed);
    });

    return results;
};

// ============================================================================
// HTML EXTRACTION
// ============================================================================
const extractAgentsFromHtml = (html) => {
    const $ = cheerioLoad(html);
    const results = [];
    const seen = new Set();

    const cardSelectors = [
        'article[data-testid*="agent"]',
        'div[data-testid*="agent"]',
        'li[data-testid*="agent"]',
        'article[class*="agent"]',
        'div[class*="agent"]',
        'li[class*="agent"]',
    ];

    let cards = $(cardSelectors.join(','));
    if (!cards.length) {
        const linkSelectors = [
            'a[href*="/find-agents/branch/"]',
            'a[href*="/find-agents/estate-agent/"]',
            'a[href*="/estate-agents/branch/"]',
            'a[href*="/estate-agent/branch/"]',
        ];
        const found = [];
        $(linkSelectors.join(',')).each((_, linkEl) => {
            const card = $(linkEl).closest('article, li, div');
            if (card.length) found.push(card.get(0));
        });
        cards = $(found);
    }

    cards.each((_, cardEl) => {
        const card = $(cardEl);
        const linkEl = card.find('a[href*="/find-agents/"], a[href*="/estate-agents/"], a[href*="/estate-agent/"]').first();
        const url = ensureAbsoluteUrl(linkEl.attr('href'));

        const name = cleanText(
            card.find('h1, h2, h3, h4').first().text() ||
            linkEl.text()
        );

        const address = cleanText(
            card.find('address').first().text() ||
            card.find('[class*="address"]').first().text()
        );

        const phone = normalizePhone(
            card.find('a[href^="tel:"]').first().attr('href') ||
            card.text()
        );

        const websiteEl = card.find('a').filter((_, el) => {
            const href = $(el).attr('href') || '';
            const text = $(el).text().toLowerCase();
            return (href.startsWith('http') && !href.includes('zoopla.co.uk')) || text.includes('website');
        }).first();
        const website = ensureAbsoluteUrl(websiteEl.attr('href'));

        const logo = ensureAbsoluteUrl(
            card.find('img[alt*="logo" i]').first().attr('src') ||
            card.find('img').first().attr('src')
        );

        const cardText = card.text();

        const rating = parseNumber(
            parseCountFromText(cardText, /(\d+(?:\.\d+)?)\s*out of\s*5/i)
        );

        const reviewCount = parseNumber(
            parseCountFromText(cardText, /(\d+)\s+reviews?/i)
        );

        const listingsForSale = parseNumber(
            parseCountFromText(cardText, /(\d+)\s+properties?\s+for\s+sale/i)
        );

        const listingsToRent = parseNumber(
            parseCountFromText(cardText, /(\d+)\s+properties?\s+to\s+rent/i)
        );

        const agentId = extractAgentIdFromUrl(url);

        const normalized = {
            agentId: agentId ? String(agentId) : null,
            name,
            branchName: name,
            companyName: null,
            url,
            address,
            postalCode: extractUkPostcode(address),
            locality: null,
            phone,
            website,
            logo,
            rating,
            reviewCount,
            listingsForSale,
            listingsToRent,
            source: 'html',
        };

        const key = normalized.agentId || normalized.url || `${normalized.name}|${normalized.address}`;
        if (!key || seen.has(key)) return;
        seen.add(key);

        results.push(normalized);
    });

    return results;
};

const findNextPageUrl = (html, currentUrl) => {
    const $ = cheerioLoad(html);
    const relNext = $('link[rel="next"]').attr('href') || $('a[rel="next"]').attr('href');
    if (relNext) return ensureAbsoluteUrl(relNext);

    const nextLink = $('a[aria-label*="Next"], a[data-testid*="pagination-next"], a[class*="next"]').first();
    if (nextLink.length) return ensureAbsoluteUrl(nextLink.attr('href'));

    const url = new URL(currentUrl);
    const pageParam = getPageParamName(url);
    const currentPage = parseNumber(url.searchParams.get(pageParam)) || 1;
    return buildSearchUrlForPage(currentUrl, currentPage + 1);
};

const dedupeAgents = (agents) => {
    const seen = new Set();
    const results = [];
    for (const agent of agents) {
        const key = agent.agentId || agent.url || `${agent.name}|${agent.address}`;
        if (!key || seen.has(key)) continue;
        seen.add(key);
        results.push(agent);
    }
    return results;
};

// ============================================================================
// MAIN ACTOR
// ============================================================================
await Actor.init();

try {
    const input = (await Actor.getInput()) || {};

    const startUrls = Array.isArray(input.startUrls) && input.startUrls.length ? input.startUrls : null;
    const startUrl = input.startUrl || (startUrls ? null : DEFAULT_START_URL);

    if (!startUrl && !startUrls) {
        log.error('Missing startUrl');
        await Actor.exit({ exitCode: 1 });
    }

    const resultsWanted = Math.max(1, Number.isFinite(+input.results_wanted) ? +input.results_wanted : 50);
    const maxPagesInput = Number.isFinite(+input.max_pages) ? Math.max(1, +input.max_pages) : null;
    const estimatedPages = Math.ceil(resultsWanted / AGENTS_PER_PAGE_ESTIMATE);
    const maxPages = maxPagesInput ?? Math.max(1, estimatedPages);

    const proxyConfiguration = await Actor.createProxyConfiguration({
        useApifyProxy: true,
        apifyProxyGroups: ['RESIDENTIAL'],
        countryCode: 'GB',
        ...input.proxyConfiguration,
    });

    log.info('Zoopla Agent Scraper v1.0.0', { resultsWanted, maxPages });

    const seen = new Set();
    const queued = new Set();
    let saved = 0;

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

    const camoufoxOptions = await camoufoxLaunchOptions({ headless: true, geoip: true });

    const crawler = new PlaywrightCrawler({
        requestQueue,
        proxyConfiguration,
        maxConcurrency: MAX_CONCURRENCY,
        maxRequestRetries: 3,
        retryOnBlocked: true,
        useSessionPool: true,
        persistCookiesPerSession: true,
        sessionPoolOptions: {
            maxUsageCount: 5,
            blockedStatusCodes: [403, 429],
        },
        requestHandlerTimeoutSecs: 120,
        navigationTimeoutSecs: 90,

        launchContext: {
            launcher: firefox,
            launchOptions: camoufoxOptions,
        },

        browserPoolOptions: {
            useFingerprints: false,
            maxOpenPagesPerBrowser: 1,
            retireBrowserAfterPageCount: 2,
        },

        preNavigationHooks: [
            async () => {
                await sleep(2000 + Math.random() * 2000);
            },
        ],

        postNavigationHooks: [
            async ({ page }) => {
                await page.waitForLoadState('domcontentloaded');
                await sleep(1500);

                await page.waitForSelector('body', { timeout: 15000 }).catch(() => { });

                for (let i = 0; i < 4; i++) {
                    await page.evaluate(() => window.scrollBy(0, 500));
                    await sleep(300);
                }
                await sleep(500);
            },
        ],

        async requestHandler({ request, page }) {
            const pageNum = request.userData.page || 1;

            if (saved >= resultsWanted) {
                log.debug(`Skip page ${pageNum}`);
                return;
            }

            const htmlResponse = await page.context().request.get(request.url, { timeout: 30000 }).catch(() => null);
            const html = htmlResponse && htmlResponse.ok() ? await htmlResponse.text() : await page.content();

            if (html.includes('Just a moment') || html.includes('Verify you are human')) {
                log.warning(`Cloudflare on page ${pageNum}, waiting...`);
                await sleep(8000);
                await page.waitForLoadState('networkidle', { timeout: 30000 }).catch(() => { });
            }

            log.info(`Page ${pageNum}/${maxPages}`);

            const nextData = extractNextDataFromHtml(html);
            const buildId = nextData?.buildId;

            let agents = [];

            if (buildId) {
                const apiUrl = buildNextDataUrl(request.url, buildId);
                if (apiUrl) {
                    const apiResponse = await page.context().request.get(apiUrl, { timeout: 30000 }).catch(() => null);
                    if (apiResponse && apiResponse.ok()) {
                        const apiPayload = await apiResponse.json().catch(() => null);
                        if (apiPayload) {
                            agents = extractAgentsFromApiPayload(apiPayload, 'api');
                        }
                    }
                }
            }

            if (!agents.length && nextData) {
                agents = extractAgentsFromApiPayload(nextData, 'api');
            }

            if (!agents.length) {
                agents = extractAgentsFromJsonLd(html);
            }

            if (!agents.length) {
                agents = extractAgentsFromHtml(html);
            }

            agents = dedupeAgents(agents);
            log.info(`Found ${agents.length} agents`);

            if (!agents.length) return;

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
                log.info(`Saved ${saved}/${resultsWanted}`);
            }

            if (pageNum < maxPages && saved < resultsWanted) {
                const nextUrl = findNextPageUrl(html, request.url);
                if (nextUrl && !queued.has(nextUrl)) {
                    queued.add(nextUrl);
                    await requestQueue.addRequest({
                        url: nextUrl,
                        userData: {
                            page: pageNum + 1,
                            rootUrl: request.userData.rootUrl,
                        },
                    });
                }
            }
        },

        async failedRequestHandler({ request, error }) {
            log.error(`Failed: ${request.url} - ${error.message}`);
        },
    });

    await crawler.run();

    log.info(`Done! ${saved} agents`);
    await Actor.setStatusMessage(`Scraped ${saved} agents`);

} catch (error) {
    log.error(error.message);
    throw error;
} finally {
    await Actor.exit();
}
