"""
check_new.py  –  Check all 6 estate agent sites for newly listed properties.

This script checks the listing pages of each site, compares URLs against the
already-scraped property index, and does a FULL scrape (text + images) for
any new properties it finds.

Intended to run frequently (e.g. every hour) to keep the dataset fresh without
re-scraping everything.

Usage:
    python3 check_new.py                  # check all 6 sources
    python3 check_new.py sb ups           # check specific sources
    python3 check_new.py --dry-run        # report new properties but don't scrape
    python3 check_new.py --text-only      # scrape new properties without images
"""

import requests
from bs4 import BeautifulSoup
import json
import os
import sys
import time
import random
import logging
import argparse
import importlib
import traceback
import subprocess
from datetime import datetime
from urllib.parse import urljoin

# Scraper modules live in scrapers/ — add to path so importlib can find them
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'scrapers'))

# ──────────────────────────────────────────────
# Source configuration
# ──────────────────────────────────────────────

SOURCES = {
    'sb': {
        'label':        'Simon Brien',
        'module':       'sb_full_scrape',
        'properties_dir': 'properties/sb',
        'link_pattern': '/buy/',
        # SB has a unique scraper API: scrape_property_details() + scrape_property_images()
        # rather than the single scrape_property_page() used by all other sources.
        'scrape_style': 'sb',
        'listing_page': lambda n: (
            'https://www.simonbrien.com/property-for-sale'
            if n == 1
            else f'https://www.simonbrien.com/property-for-sale/page{n}/?orderBy='
        ),
    },
    'ups': {
        'label':        'Ulster Property Sales',
        'module':       'ups_full_scrape',
        'properties_dir': 'properties/ups',
        'link_pattern': '/property/',
        'scrape_style': 'standard',
        'listing_page': lambda n: f'https://www.ulsterpropertysales.co.uk/property-for-sale/page{n}/',
    },
    'hc': {
        'label':        'Hunter Campbell',
        'module':       'hc_full_scrape',
        'properties_dir': 'properties/hc',
        'link_pattern': '/property/',
        'scrape_style': 'standard',
        # HC uses ?page=N
        'listing_page': lambda n: (
            'https://www.huntercampbell.co.uk/residential-sales'
            if n == 1
            else f'https://www.huntercampbell.co.uk/residential-sales?page={n}'
        ),
    },
    'jm': {
        'label':        'John Minnis',
        'module':       'jm_full_scrape',
        'properties_dir': 'properties/jm',
        'link_pattern': '/property/',
        'scrape_style': 'standard',
        'listing_page': lambda n: f'https://www.johnminnis.co.uk/search/906207/page{n}/',
    },
    'pp': {
        'label':        'Property People NI',
        'module':       'pp_full_scrape',
        'properties_dir': 'properties/pp',
        'link_pattern': '/property/',
        'scrape_style': 'standard',
        'listing_page': lambda n: f'https://www.propertypeopleni.com/property-for-sale/page{n}/',
    },
    'tr': {
        'label':        'Templeton Robinson',
        'module':       'tr_full_scrape',
        'properties_dir': 'properties/tr',
        'link_pattern': '/property/',
        'scrape_style': 'standard',
        'listing_page': lambda n: f'https://www.templetonrobinson.com/property-for-sale/page{n}/',
    },
}

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                  '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-GB,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
}

# ──────────────────────────────────────────────
# Logging
# ──────────────────────────────────────────────

os.makedirs('logs', exist_ok=True)
log_filename = f"logs/check_new_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename, encoding='utf-8'),
        logging.StreamHandler(),
    ]
)
logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────
# Index helpers
# ──────────────────────────────────────────────

def load_known_urls(source_key):
    """
    Return a set of all property URLs already scraped for this source.

    Handles two index formats:
      - {properties: [{id, url, ...}, ...], last_updated: ...}   ← used by all 6 scrapers
      - {url_or_id: value, ...}                                   ← flat dict fallback

    Falls back to reading each property_N.json directly if no index exists.
    """
    index_path = os.path.join(SOURCES[source_key]['properties_dir'], 'property_index.json')

    if os.path.isfile(index_path):
        try:
            with open(index_path, encoding='utf-8') as f:
                data = json.load(f)

            # Format 1: {properties: [{id, url}...], last_updated: ...}
            if isinstance(data, dict) and 'properties' in data:
                entries = data['properties']
                if isinstance(entries, list):
                    urls = set()
                    for entry in entries:
                        if isinstance(entry, dict) and entry.get('url'):
                            urls.add(entry['url'].rstrip('/'))
                        elif isinstance(entry, str) and entry.startswith('http'):
                            urls.add(entry.rstrip('/'))
                    logger.info(f"  Loaded {len(urls)} known URLs from index (list format)")
                    return urls

            # Format 2: flat dict  {key: value, ...}
            if isinstance(data, dict):
                urls = set()
                for k, v in data.items():
                    if k.startswith('http'):
                        urls.add(k.rstrip('/'))
                    elif isinstance(v, dict) and v.get('url'):
                        urls.add(v['url'].rstrip('/'))
                    elif isinstance(v, str) and v.startswith('http'):
                        urls.add(v.rstrip('/'))
                logger.info(f"  Loaded {len(urls)} known URLs from index (dict format)")
                return urls

        except Exception as e:
            logger.warning(f"  Could not parse index for {source_key}: {e}")

    # No index or unreadable – build from individual JSON files
    return build_known_urls_from_files(source_key)


def build_known_urls_from_files(source_key):
    """Scan every property_N.json and collect known URLs."""
    urls = set()
    props_dir = SOURCES[source_key]['properties_dir']
    if not os.path.isdir(props_dir):
        return urls

    for entry in os.listdir(props_dir):
        if not entry.startswith('property_'):
            continue
        json_path = os.path.join(props_dir, entry, f'{entry}.json')
        if not os.path.isfile(json_path):
            continue
        try:
            with open(json_path, encoding='utf-8') as f:
                d = json.load(f)
            if d.get('url'):
                urls.add(d['url'].rstrip('/'))
        except Exception:
            pass

    logger.info(f"  Built known URLs from files for {source_key}: {len(urls)} properties")
    return urls


def build_index_from_files(source_key):
    """Build an in-memory index of scraped URLs from individual property JSON files."""
    index = {}
    props_dir = SOURCES[source_key]['properties_dir']
    if not os.path.isdir(props_dir):
        return index

    for entry in os.listdir(props_dir):
        if not entry.startswith('property_'):
            continue
        json_path = os.path.join(props_dir, entry, f'{entry}.json')
        if not os.path.isfile(json_path):
            continue
        try:
            with open(json_path, encoding='utf-8') as f:
                data = json.load(f)
            if data.get('url'):
                index[data['url']] = {
                    'id': data.get('id', entry),
                    'scraped_at': data.get('scraped_at', ''),
                }
        except Exception:
            pass
    logger.info(f"  Built in-memory index for {source_key}: {len(index)} properties")
    return index

# ──────────────────────────────────────────────
# Listing page scraping
# ──────────────────────────────────────────────

def fetch(url, retries=3):
    """HTTP GET with retry."""
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=HEADERS, timeout=30)
            r.raise_for_status()
            return r
        except requests.exceptions.RequestException as e:
            logger.warning(f"  [attempt {attempt+1}] {url}: {e}")
            if attempt < retries - 1:
                time.sleep((2 ** attempt) * random.uniform(1, 2))
    return None


def extract_property_links(soup, page_url, link_pattern='/property/'):
    """
    Extract individual property URLs from a listing page.
    link_pattern: URL fragment that identifies a property detail link
                  (e.g. '/property/' for most sites, '/buy/' for Simon Brien).
    """
    links = set()
    for a in soup.find_all('a', href=True):
        href = a['href']
        if link_pattern in href:
            full = urljoin(page_url, href)
            # Normalise: strip query strings / fragments, no trailing slash
            full = full.split('?')[0].split('#')[0].rstrip('/')
            links.add(full)
    return links


def get_new_urls_for_source(source_key, known_urls, max_pages=50):
    """
    Walk the listing pages until we only see already-known URLs.
    Returns a set of new (not yet scraped) property URLs.
    """
    cfg = SOURCES[source_key]
    listing_page_fn = cfg['listing_page']
    new_urls = set()
    consecutive_all_known = 0

    for page_num in range(1, max_pages + 1):
        page_url = listing_page_fn(page_num)
        logger.info(f"  Checking listing page {page_num}: {page_url}")

        r = fetch(page_url)
        if not r:
            logger.warning(f"  Failed to fetch page {page_num}, stopping.")
            break

        soup = BeautifulSoup(r.content, 'html.parser')
        link_pattern = cfg.get('link_pattern', '/property/')
        links = extract_property_links(soup, page_url, link_pattern)

        if not links:
            logger.info(f"  No property links found on page {page_num}, stopping.")
            break

        # Normalise known_urls to no-trailing-slash for comparison
        known_norm = {u.rstrip('/') for u in known_urls}
        page_new = links - known_norm
        new_urls.update(page_new)
        logger.info(f"  Page {page_num}: {len(links)} listings, {len(page_new)} new")

        if not page_new:
            consecutive_all_known += 1
            # If 3 pages in a row are entirely known, we've caught up
            if consecutive_all_known >= 3:
                logger.info(f"  3 consecutive pages fully known. Stopping.")
                break
        else:
            consecutive_all_known = 0

        time.sleep(random.uniform(1.5, 3.0))

    return new_urls

# ──────────────────────────────────────────────
# Scrape new properties via the source's own module
# ──────────────────────────────────────────────

def scrape_new_properties(source_key, new_urls, text_only=False):
    """
    Given a set of new URLs, scrape each one using the source's own module.

    We do this by temporarily monkey-patching the module's known_urls list
    so it skips all already-scraped properties and only scrapes the new ones.

    Falls back to using text_update.py if text_only=True.
    """
    if text_only:
        # Use the text-only re-scraper for just these URLs
        from text_update import scrape_property_text
        props_dir = SOURCES[source_key]['properties_dir']

        # Determine the next property ID
        existing = [d for d in os.listdir(props_dir) if d.startswith('property_')]
        next_id = max(
            (int(d.split('_')[1]) for d in existing if d.split('_')[1].isdigit()),
            default=0
        ) + 1

        saved = 0
        for url in sorted(new_urls):
            prop_id = f'property_{next_id}'
            logger.info(f"  Scraping (text-only) {prop_id}: {url}")
            data = scrape_property_text(url, source_key)
            if data:
                data['id'] = prop_id
                data['url'] = url
                data['scraped_at'] = datetime.now().isoformat()
                out_dir = os.path.join(props_dir, prop_id)
                os.makedirs(out_dir, exist_ok=True)
                out_path = os.path.join(out_dir, f'{prop_id}.json')
                with open(out_path, 'w', encoding='utf-8') as f:
                    json.dump(data, f, indent=2, ensure_ascii=False)
                logger.info(f"    Saved {out_path}")
                saved += 1
                next_id += 1
            time.sleep(random.uniform(1.5, 3.0))
        return saved

    else:
        # Import the full scraper module and call the appropriate per-property function.
        #
        # Most scrapers:  scrape_property_page(url, prop_id)  – handles everything
        # Simon Brien:    scrape_property_details(url)         – returns data dict;
        #                 scrape_property_images(url, folder)  – downloads images
        #                 caller must create folder and save JSON
        cfg = SOURCES[source_key]
        module_name = cfg['module']
        scrape_style = cfg.get('scrape_style', 'standard')

        try:
            if module_name in sys.modules:
                del sys.modules[module_name]
            mod = importlib.import_module(module_name)
        except Exception as e:
            logger.error(f"  Could not import {module_name}: {e}")
            return 0

        props_dir = cfg['properties_dir']
        existing = [d for d in os.listdir(props_dir) if d.startswith('property_')]
        next_id = max(
            (int(d.split('_')[1]) for d in existing if d.split('_')[1].isdigit()),
            default=0
        ) + 1

        saved = 0
        for url in sorted(new_urls):
            prop_id = f'property_{next_id}'
            logger.info(f"  Scraping (full) {prop_id}: {url}")
            try:
                if scrape_style == 'sb':
                    # SB: two-step – details then images, then manual save
                    prop_folder = os.path.join(props_dir, prop_id)
                    os.makedirs(prop_folder, exist_ok=True)
                    data = mod.scrape_property_details(url)
                    if data:
                        data['id'] = prop_id
                        image_count = mod.scrape_property_images(url, prop_folder)
                        data['image_count'] = image_count
                        json_path = os.path.join(prop_folder, f'{prop_id}.json')
                        with open(json_path, 'w', encoding='utf-8') as f:
                            json.dump(data, f, indent=2, ensure_ascii=False)
                    else:
                        raise ValueError("scrape_property_details returned no data")
                else:
                    # Standard: single call handles folder, JSON, and images
                    mod.scrape_property_page(url, prop_id)

                saved += 1
                next_id += 1
                logger.info(f"    Saved {prop_id}")
            except Exception as e:
                logger.error(f"    Failed: {e}")
            time.sleep(random.uniform(1.5, 3.0))
        return saved

# ──────────────────────────────────────────────
# TR selenium backfill
# ──────────────────────────────────────────────

def run_tr_selenium_backfill():
    """
    After scraping new TR properties (which only get static HTML), launch
    tr_selenium_scrape.py as a subprocess to fill in descriptions, key features,
    and rooms for all TR properties that are currently missing them.

    The selenium scraper:
      - only processes properties where description or key_features are absent
      - skips properties that already have both fields
      - restarts Chrome every 50 properties to prevent memory build-up

    Returns True if the subprocess completed successfully, False otherwise.
    """
    script = os.path.join(os.path.dirname(__file__), 'scrapers', 'tr_selenium_scrape.py')
    if not os.path.isfile(script):
        logger.error(f"TR selenium scraper not found at: {script}")
        return False

    logger.info("─" * 60)
    logger.info("Running TR selenium backfill for missing descriptions / features…")
    logger.info(f"  Script: {script}")

    try:
        result = subprocess.run(
            [sys.executable, script],
            cwd=os.path.dirname(__file__),
            timeout=3600,  # 1-hour safety cap
        )
        if result.returncode == 0:
            logger.info("TR selenium backfill completed successfully.")
            return True
        else:
            logger.error(f"TR selenium backfill exited with code {result.returncode}")
            return False
    except subprocess.TimeoutExpired:
        logger.error("TR selenium backfill timed out after 1 hour.")
        return False
    except FileNotFoundError:
        logger.error(
            "Could not launch Python subprocess. "
            "Make sure you are running check_new.py with the correct virtual-env Python."
        )
        return False
    except Exception as e:
        logger.error(f"TR selenium backfill error: {e}")
        logger.error(traceback.format_exc())
        return False


# ──────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description='Check for newly listed properties on all estate agent sites.'
    )
    parser.add_argument('sources', nargs='*', default=list(SOURCES.keys()),
                        help='Sources to check (default: all)')
    parser.add_argument('--dry-run', action='store_true',
                        help='Report new URLs but do not scrape them')
    parser.add_argument('--text-only', action='store_true',
                        help='Scrape new properties as text-only (no images)')
    parser.add_argument('--max-pages', type=int, default=50,
                        help='Max listing pages to check per source (default: 50)')
    parser.add_argument('--no-selenium', action='store_true',
                        help='Skip the TR selenium backfill step (useful if Chrome/selenium not installed)')
    args = parser.parse_args()

    sources_to_run = [s for s in args.sources if s in SOURCES]
    unknown = [s for s in args.sources if s not in SOURCES]
    if unknown:
        logger.warning(f"Unknown sources (ignored): {unknown}")
    if not sources_to_run:
        logger.error("No valid sources to check.")
        sys.exit(1)

    mode = 'dry-run' if args.dry_run else ('text-only' if args.text_only else 'full')
    logger.info(f"check_new starting. Sources: {sources_to_run}  Mode: {mode}")
    logger.info(f"Log: {log_filename}")

    summary = {}

    for source_key in sources_to_run:
        cfg = SOURCES[source_key]
        logger.info(f"{'='*60}")
        logger.info(f"Source: {cfg['label']} ({source_key.upper()})")

        # Load existing known URLs
        known_urls = load_known_urls(source_key)
        logger.info(f"  Known properties: {len(known_urls)}")

        # Find new URLs
        try:
            new_urls = get_new_urls_for_source(source_key, known_urls, args.max_pages)
        except Exception as e:
            logger.error(f"  Error checking listings: {e}")
            logger.error(traceback.format_exc())
            summary[source_key] = {'found': 0, 'scraped': 0, 'error': str(e)}
            continue

        logger.info(f"  New properties found: {len(new_urls)}")

        if not new_urls:
            summary[source_key] = {'found': 0, 'scraped': 0}
            continue

        if args.dry_run:
            for url in sorted(new_urls):
                logger.info(f"    NEW: {url}")
            summary[source_key] = {'found': len(new_urls), 'scraped': 0}
            continue

        # Scrape new properties
        scraped = scrape_new_properties(source_key, new_urls, text_only=args.text_only)
        summary[source_key] = {'found': len(new_urls), 'scraped': scraped}

        # Save a record of newly found URLs for audit trail
        new_log_path = os.path.join(cfg['properties_dir'], 'new_found.json')
        try:
            existing_log = []
            if os.path.isfile(new_log_path):
                with open(new_log_path, encoding='utf-8') as f:
                    existing_log = json.load(f)
            for url in sorted(new_urls):
                existing_log.append({'url': url, 'found_at': datetime.now().isoformat()})
            with open(new_log_path, 'w', encoding='utf-8') as f:
                json.dump(existing_log, f, indent=2, ensure_ascii=False)
        except Exception as e:
            logger.warning(f"  Could not save new_found.json: {e}")

    # TR selenium backfill — fills descriptions/features for older TR properties that
    # were originally scraped with the static (non-selenium) scraper and are missing
    # those fields.  New TR properties scraped via check_new are now handled by
    # tr_full_scrape.py which uses selenium natively, so they won't need backfilling.
    # Skipped in dry-run mode or if --no-selenium is set.
    tr_was_checked = 'tr' in sources_to_run
    if tr_was_checked and not args.dry_run and not args.no_selenium:
        logger.info(
            "Running TR selenium backfill for any existing properties "
            "still missing descriptions (from the old static scraper)…"
        )
        run_tr_selenium_backfill()

    # Summary
    logger.info(f"{'='*60}")
    logger.info("check_new complete:")
    total_new = 0
    for source_key, stats in summary.items():
        label = SOURCES[source_key]['label']
        found = stats.get('found', 0)
        scraped = stats.get('scraped', 0)
        err = stats.get('error', '')
        total_new += found
        if err:
            logger.info(f"  {source_key.upper():4s}  {label:30s}  ERROR: {err}")
        elif args.dry_run:
            logger.info(f"  {source_key.upper():4s}  {label:30s}  {found} new (dry run)")
        else:
            logger.info(f"  {source_key.upper():4s}  {label:30s}  {found} new, {scraped} scraped")

    logger.info(f"Total new properties found: {total_new}")


if __name__ == '__main__':
    main()
