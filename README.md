# re-scraper — NI Estate Agent Property Scraper

Scrapes residential property listings (for-sale) from **35 Northern Ireland
estate agent websites**, geocodes the addresses, and uploads everything to a
Supabase `properties` table that feeds the property-swipe app.

## How it works

```
 estate agent sites
        │
        ▼
 scrapers/<key>_full_scrape.py      one BaseScraper subclass per agent
        │  writes raw JSON to properties/<key>/property_N/property_N.json
        ▼
 geocode.py                         address → lat/lng (cached in geocache.json)
        ▼
 supabase/migrate_data.py           upserts rows into Supabase `properties`
        │                            (conflict key: source + url)
        ▼
 Supabase ──────────────────►  mobile / web app
```

`config/sources.py` is the **single source of truth**: every agent is one entry
in the `SOURCES` dict (URLs, CSS selector family, scrape strategy, pagination
style, parallel group). All orchestrators — `full_scrape.py`,
`property_update.py`, `geocode.py`,
`supabase/migrate_data.py`, `test_all_scrapers.py` — are driven by that config,
so adding an agent to `SOURCES` wires it into every pipeline automatically.

## Setup

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

Create `.env` (gitignored) with:

```
SUPABASE_URL=https://<project>.supabase.co
SUPABASE_SERVICE_KEY=<service-role-key>
GOOGLE_GEOCODING_API_KEY=<key>        # used by geocode.py
```

## Common commands

| Command | What it does |
|---|---|
| `python3 full_scrape.py` | Fresh re-scrape of **all** agents in parallel, then geocode + migrate to Supabase |
| `python3 full_scrape.py nest bmc` | Scrape specific agents only |
| `python3 full_scrape.py --list` | List available sources |
| `python3 property_update.py` | Hourly sync: finds new listings, deletes delisted ones, detects price/status/description changes (runs hourly via GitHub Actions). Parallel by default: ~15 min for all agents |
| `python3 property_update.py --source-workers 1` | Same but serial (debugging) |
| `python3 property_update.py --no-text-update` | Fast new/delisted-only check (no per-property re-scrapes) |
| `python3 test_all_scrapers.py` | Smoke-test every scraper (`--limit 1 --fresh` each) |
| `python3 scrapers/nest_full_scrape.py --fresh` | Run one scraper directly |
| `./RUN_ALL_SCRAPERS.zsh` | Fire all scrapers in the background with per-agent logs |

Useful flags on every individual scraper: `--fresh` (clear existing data first),
`--limit N` (cap properties), `--test` (1 property only).

## Scraped data layout

```
properties/<key>/
├── property_index.json        # url → id index, used for incremental updates
├── summary.json               # last-run stats
└── property_1/
    ├── property_1.json        # address, price_str, status, bedrooms,
    │                          # description, key_features, rooms, image_urls…
    └── (images, only for agents with download_images: true)
```

`properties/`, `logs/` and `.env` are gitignored — only the code is committed.

## Supabase

`supabase/schema.sql` creates the target schema:

- `properties` — one row per listing (`source`, `source_id`, `url`, `address`,
  `price`, `price_value`, `status`, `property_type`, `bedrooms`, `bathrooms`,
  `receptions`, `epc_rating`, `description`, `key_features`, `rooms`,
  `image_urls`, `lat`, `lng`, …). Upserted on `(source, url)`.
- `swipes`, `folders`, `folder_properties` — app-side tables.

`supabase/migrate_data.py` converts every `property_N.json` into a row via
`config/supabase_utils.build_property_row` (normalises status to
*For Sale / Sale Agreed / Under Offer / Sold / Let / Let Agreed*, extracts
numeric `price_value`, merges geocache coordinates) and batch-upserts.

## Automation

Two GitHub Actions workflows keep data fresh automatically:

- `.github/workflows/scrape.yml` runs `python3 property_update.py` **hourly**
  (new listings, delistings, price/status/description changes — ~15 min)
- `.github/workflows/full_scrape.yml` runs `python3 full_scrape.py` **weekly,
  Sundays 03:00 UTC** (fresh rebuild of every agent + geocode + migrate)

Both accept manual triggers from the GitHub Actions tab (`workflow_dispatch`).

## Adding a new estate agent

1. **Inspect the target site** — find the listing pages, the detail-page URL
   pattern, the markup around price/address/bedrooms/images, and how
   pagination works (query param vs path).
2. **Add an entry to `SOURCES` in `config/sources.py`** — key, label, module
   name, `props_dir`, `base_url`, CMS family (`pp_classic` / `pp_bluecubes` /
   `pp_modern` / `wordpress` / …), strategy (`requests` or `selenium`),
   `link_pattern`, `listing_page` lambda, and `parallel_group`
   (1 = requests, 2 = Selenium).
3. **Create `scrapers/<key>_full_scrape.py`** — subclass `BaseScraper` and
   implement `get_listing_url`, `extract_property_links`,
   `scrape_detail_page`, `extract_image_urls`. If the site uses one of the
   PropertyPal CMS families, reuse the shared parsers in `scrapers/base.py`
   (see `abc_full_scrape.py`); WordPress/PropertyHive sites parse their own
   markup (see `bmc_full_scrape.py` or `nest_full_scrape.py`).
4. **Add it to `RUN_ALL_SCRAPERS.zsh`** (that list is hardcoded).
5. **Test**: `python3 scrapers/<key>_full_scrape.py --test`, then
   `python3 full_scrape.py <key>`.

Once it is in `SOURCES`, geocoding, Supabase migration, `property_update.py`,
`test_all_scrapers.py` and `text_update.py` all pick it up automatically.

## Current sources

| Key | Agent | CMS family | Strategy | Parallel group |
|---|---|---|---|---|
| `sb` | Simon Brien | custom_sb | requests | 1 |
| `ups` | Ulster Property Sales | pp_bluecubes | requests | 1 |
| `hc` | Hunter Campbell | pp_classic | requests | 1 |
| `jm` | John Minnis | pp_bluecubes | requests | 1 |
| `pp` | Property People NI | pp_classic | requests | 1 |
| `dh` | Daniel Henry | pp_bluecubes | requests+selenium fallback | 1 |
| `pinp` | Pinpoint Property | pp_bluecubes | requests | 1 |
| `rb` | Rodgers & Browne | pp_bluecubes | requests | 1 |
| `tr` | Templeton Robinson | pp_bluecubes | selenium | 2 |
| `mm` | McMillan McClure | pp_modern | requests | 1 |
| `ce` | Country Estates | pp_modern | selenium | 2 |
| `gm` | Gareth Mills Est. Agents | pp_modern | selenium | 2 |
| `mc` | Michael Chandler | pp_classic | requests | 1 |
| `ft` | Fetherstons | pp_classic | requests | 1 |
| `pr` | Peter Rodgers | pp_classic | requests | 1 |
| `cps` | CPS | pp_classic | requests | 1 |
| `hn` | Hannath | pp_classic | requests | 1 |
| `bt` | Brian Todd | pp_classic | requests | 1 |
| `rr` | Reeds Rains | reeds_rains | requests | 1 |
| `ee` | Edmonton Estates | pp_classic | requests | 1 |
| `ag` | Armstrong Gordon | pp_classic | requests | 1 |
| `ta` | The Agent | pp_classic | requests | 1 |
| `abc` | A Barton Company | pp_classic | requests | 1 |
| `hg` | Henry Graham | pp_classic | requests | 1 |
| `le` | Lennon Estates | pp_classic | requests | 1 |
| `amd` | Agar Murdoch and Deane | pp_classic | requests | 1 |
| `tm` | Tim Martin | pp_classic | requests | 1 |
| `ma` | McAllister | pp_classic | requests | 1 |
| `dl` | Dallas | pp_classic | requests | 1 |
| `bmc` | Bill McCann | wordpress | requests | 1 |
| `ag2` | Andrews & Gregg | pp_classic | requests | 1 |
| `ipe` | Independent Property Estates | pp_classic | requests | 1 |
| `mmc` | Montgomery & McCleary | pp_classic | requests | 1 |
| `pe` | Pauline Elliott | pp_classic | requests | 1 |
| `nest` | Nest Estate Agents | wordpress | requests | 1 |

## Other utilities

| Script | Purpose |
|---|---|
| `geocode.py` | Geocode any new addresses into `geocache.json` (Google, falls back gracefully) |
| `geocode_all.js` | One-shot bulk geocoder via Nominatim (1 req/sec, resumable) |
| `scrapers/text_update.py` | Text-only re-scrape to catch price/status/description changes |
| `archive_orphans.py` | Move duplicate/stale property folders (same URL scraped under multiple ids) to `properties/_archive_dupes/` — reversible |
| `dedupe_properties.py` | Remove duplicate Supabase rows |
| `normalise_statuses.py` | One-off status clean-up in Supabase |
| `fix_descriptions.py` / `decode_existing_entities.py` | HTML-entity clean-up passes |
| `backfill_ups_prices.py` / `scrapers/backfill_*.py` | Historical field backfills |

Logs land in `logs/` (per run, timestamped) for every scraper and orchestrator.

on GitHub Actions (supabase + geocoding keys from repo secrets) and uploads
the logs as artifacts.
