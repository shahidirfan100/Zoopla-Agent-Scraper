# Zoopla Agent Scraper

Collect estate agent and branch details from Zoopla's agent directory pages, including London and other UK locations. This actor targets Zoopla estate agents, branch profiles, and directory listings with a focus on accuracy, coverage, and clean data output.

## Overview

- Scrapes Zoopla estate agent directory pages such as `https://www.zoopla.co.uk/find-agents/estate-agents/london/`
- Prioritizes JSON data sources, then falls back to structured markup and HTML parsing
- Supports pagination, multiple start URLs, and configurable limits
- Produces a clean dataset ready for lead generation, market research, and competitive analysis

## What you can extract

Each agent item can include the following fields when available:

| Field | Description |
|---|---|
| `agentId` | Unique agent or branch identifier |
| `name` | Agent or branch name |
| `branchName` | Branch name if available |
| `companyName` | Company or brand name |
| `url` | Zoopla agent profile URL |
| `address` | Full branch address |
| `postalCode` | UK postcode |
| `locality` | Town or city |
| `phone` | Contact phone number |
| `website` | External website if listed |
| `logo` | Logo image URL |
| `rating` | Review rating value |
| `reviewCount` | Number of reviews |
| `listingsForSale` | Count of properties for sale |
| `listingsToRent` | Count of properties to rent |
| `source` | Data source used (`api`, `json-ld`, `html`) |
| `scrapedAt` | ISO timestamp |

## Input

| Field | Type | Description | Default |
|---|---|---|---|
| `startUrl` | string | Zoopla agent directory URL to scrape | `https://www.zoopla.co.uk/find-agents/estate-agents/london/` |
| `startUrls` | array | Optional list of agent directory URLs | `[]` |
| `results_wanted` | integer | Maximum agents to collect | `50` |
| `max_pages` | integer | Maximum pages per start URL | `5` |
| `proxyConfiguration` | object | Proxy settings (UK residential recommended) | Apify Proxy |

### Example input

```json
{
  "startUrl": "https://www.zoopla.co.uk/find-agents/estate-agents/london/",
  "results_wanted": 120,
  "max_pages": 8,
  "proxyConfiguration": {
    "useApifyProxy": true,
    "apifyProxyGroups": ["RESIDENTIAL"],
    "countryCode": "GB"
  }
}
```

## Output

Results are stored in the default dataset. Example output:

```json
{
  "agentId": "12345",
  "name": "Example Estate Agents",
  "branchName": "Example Estate Agents - London",
  "companyName": "Example Group",
  "url": "https://www.zoopla.co.uk/find-agents/branch/example-estate-agents-london/12345/",
  "address": "1 High Street, London SW1A 1AA",
  "postalCode": "SW1A 1AA",
  "locality": "London",
  "phone": "020 1234 5678",
  "website": "https://www.example.com",
  "logo": "https://lid.zoocdn.com/...",
  "rating": 4.7,
  "reviewCount": 214,
  "listingsForSale": 62,
  "listingsToRent": 18,
  "source": "api",
  "scrapedAt": "2026-01-07T12:00:00.000Z"
}
```

## Pagination behavior

- The actor crawls pages until it reaches `results_wanted` or `max_pages`
- Pagination links are followed automatically when present
- If pagination links are missing, the actor falls back to URL parameters

## Recommended settings

- Use UK residential proxies for higher success rates
- Keep `max_pages` reasonable to reduce blocks and improve consistency
- For large runs, use multiple start URLs and smaller limits per URL

## Use cases

- Build a directory of Zoopla estate agents in London or across the UK
- Collect estate agent contact details for outreach or sales operations
- Monitor agency presence, coverage, and listing activity in local markets
- Benchmark competitors by region, branch, or review performance

## Notes

- Zoopla applies anti-bot protection; proxy usage improves reliability
- Data availability depends on what Zoopla exposes on each page
- Review and rating fields may be missing for some agents

## License

ISC
