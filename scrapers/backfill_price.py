#!/usr/bin/env python3
"""
backfill_price.py  –  Backfill missing price_str for all PropertyPal CMS sources.

PropertyPal CMS does NOT store the price in ul.dettbl — it lives in span.pricesm
(or can be extracted from the div.dtsm bar text).  The original scrapers all
missed this, so many properties show "POA" in the app even though they have prices.

This script re-fetches each property page that is missing a price and extracts
price_str using the correct selectors.

Usage (run from re_app/ directory):
    python3 scrapers/backfill_price.py                   # all PropertyPal CMS sources
    python3 scrapers/backfill_price.py hc jm             # specific sources only
    python3 scrapers/backfill_price.py --force           # re-fetch even if price_str exists
    python3 scrapers/backfill_price.py --limit 20        # test on first 20 per source
    python3 scrapers/backfill_price.py --workers 8       # concurrent requests (default: 6)

After running:
    python3 supabase/migrate_data.py    # push updated prices to Supabase

Requires: pip3 install requests beautifulsoup4
"""

import os, sys, json, re, time, random, argparse, logging
from datetime import datetime
from urllib.parse import urljoin
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    import requests
    from bs4 import BeautifulSoup
except ImportError:
    print("Run: pip3 install requests beautifulsoup4")
    sys.exit(1)

# ── Config ─────────────────────────────────────────────────────────────────────

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# All PropertyPal CMS sources (Simon Brien uses a different CMS — skip it)
SOURCES = {
    'ups': os.path.join(ROOT, 'properties', 'ups'),
    'hc':  os.path.join(ROOT, 'properties', 'hc'),
    'jm':  os.path.join(ROOT, 'properties', 'jm'),
    'pp':  os.path.join(ROOT, 'properties', 'pp'),
    'dh':  os.path.join(ROOT, 'properties', 'dh'),
    'mm':  os.path.join(ROOT, 'properties', 'mm'),
    'ce':  os.path.join(ROOT, 'properties', 'ce'),
}

TIMEOUT  = 15
DELAY    = 0.1   # small per-worker delay — we're running multiple workers

HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
        'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    ),
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-GB,en;q=0.9',
}

# ── Logging ────────────────────────────────────────────────────────────────────

os.makedirs(os.path.join(ROOT, 'logs'), exist_ok=True)
log_file = os.path.join(
    ROOT, 'logs',
    f"backfill_price_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[
        logging.FileHandler(log_file, encoding='utf-8'),
        logging.StreamHandler(),
    ]
)
logger = logging.getLogger(__name__)

# ── HTTP fetch ─────────────────────────────────────────────────────────────────

def fetch(url, retries=3):
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
            r.raise_for_status()
            return r
        except requests.exceptions.RequestException as e:
            if attempt < retries - 1:
                time.sleep((2 ** attempt) * random.uniform(1, 2))
    return None

# ── Price extraction ───────────────────────────────────────────────────────────

def extract_price(html, url):
    """
    Extract price_str from a PropertyPal CMS property page.
    Returns the price string (e.g. '£250,000') or None.
    """
    soup = BeautifulSoup(html, 'html.parser')

    # 1. span.pricesm / h2.pricesm / div.pricesm  (primary PropertyPal CMS location)
    for sel in ('span.pricesm', 'h2.pricesm', 'h3.pricesm', 'div.pricesm', 'p.pricesm'):
        el = soup.select_one(sel)
        if el:
            val = el.get_text(strip=True)
            if val:
                return val

    # 2. Regex on div.dtsm text: pick up £NNN,NNN or POA
    dtsm = soup.select_one('div.dtsm')
    if dtsm:
        m = re.search(
            r'(£[\d,]+(?:\s*(?:pcm|pw|per\s+\w+))?|POA)',
            dtsm.get_text(separator=' ', strip=True), re.I
        )
        if m:
            return m.group(1)

    # 3. og:description meta — often starts with "£NNN,NNN · 3 bed · ..."
    og = soup.find('meta', property='og:description')
    if og and og.get('content'):
        m = re.search(r'(£[\d,]+|POA)', og['content'], re.I)
        if m:
            return m.group(1)

    # 4. ul.dettbl — price rarely lives here, but try as last resort
    for li in soup.select('ul.dettbl li'):
        key_el = li.find(class_='dt1')
        val_el = li.find(class_='dt2')
        if key_el and val_el and 'price' in key_el.get_text(strip=True).lower():
            val = val_el.get_text(strip=True)
            if val:
                return val

    return None

# ── Process one source ─────────────────────────────────────────────────────────

def collect_entries(props_dir, force=False, limit=0):
    """
    Returns list of (folder_name, json_path, url) for properties that need
    a price backfill (missing price_str, or --force).
    """
    entries = []
    if not os.path.isdir(props_dir):
        return entries

    dirs = sorted(
        [d for d in os.listdir(props_dir) if re.fullmatch(r'property_\d+', d)],
        key=lambda x: int(x.replace('property_', ''))
    )

    for d in dirs:
        jpath = os.path.join(props_dir, d, f'{d}.json')
        if not os.path.isfile(jpath):
            continue
        try:
            data = json.load(open(jpath, encoding='utf-8'))
        except Exception:
            continue

        # Skip if already has a price (unless --force)
        if not force and data.get('price_str'):
            continue

        url = data.get('url', '').strip()
        if not url:
            continue

        entries.append((d, jpath, url, data))
        if limit and len(entries) >= limit:
            break

    return entries


def process_source(source_key, props_dir, force=False, limit=0, workers=6):
    logger.info(f"\n{'─'*55}")
    logger.info(f"Source: {source_key.upper()}  ({props_dir})")

    entries = collect_entries(props_dir, force=force, limit=limit)
    if not entries:
        logger.info(f"  Nothing to backfill — all prices present (use --force to re-fetch)")
        return {'total_checked': 0, 'updated': 0, 'not_found': 0, 'errors': 0}

    logger.info(f"  {len(entries)} properties need price backfill")

    updated   = 0
    not_found = 0
    errors    = 0

    def task(entry):
        folder, jpath, url, data = entry
        time.sleep(DELAY + random.uniform(0, 0.1))
        r = fetch(url)
        if not r:
            return folder, None, 'fetch_failed'
        price = extract_price(r.text, url)
        return folder, price, jpath

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(task, e): e for e in entries}
        done = 0
        for future in as_completed(futures):
            folder, price, result = future.result()
            done += 1

            if result == 'fetch_failed':
                logger.warning(f"  [{done}/{len(entries)}] {folder}: fetch failed")
                errors += 1
                continue

            jpath = result
            entry_data = next(e[3] for e in entries if e[0] == folder)

            if price:
                entry_data['price_str'] = price
                entry_data['price_backfilled_at'] = datetime.now().isoformat()
                try:
                    with open(jpath, 'w', encoding='utf-8') as f:
                        json.dump(entry_data, f, indent=2, ensure_ascii=False)
                    logger.info(f"  [{done}/{len(entries)}] {folder}: {price}")
                    updated += 1
                except Exception as e:
                    logger.error(f"  [{done}/{len(entries)}] {folder}: write error: {e}")
                    errors += 1
            else:
                logger.info(f"  [{done}/{len(entries)}] {folder}: no price found on page")
                not_found += 1

    return {
        'total_checked': len(entries),
        'updated':       updated,
        'not_found':     not_found,
        'errors':        errors,
    }

# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description='Backfill missing price_str for all PropertyPal CMS property sources.'
    )
    parser.add_argument('sources', nargs='*', default=list(SOURCES.keys()),
                        help='Sources to process (default: all)')
    parser.add_argument('--force',   action='store_true',
                        help='Re-fetch even if price_str already set')
    parser.add_argument('--limit',   type=int, default=0,
                        help='Max properties per source (0 = all)')
    parser.add_argument('--workers', type=int, default=6,
                        help='Concurrent HTTP workers per source (default: 6)')
    args = parser.parse_args()

    valid   = [s for s in args.sources if s in SOURCES]
    unknown = [s for s in args.sources if s not in SOURCES]
    if unknown:
        logger.warning(f"Unknown sources (skipped): {', '.join(unknown)}")
    if not valid:
        logger.error('No valid sources.')
        sys.exit(1)

    logger.info('=' * 60)
    logger.info(f"backfill_price.py — {datetime.now().isoformat()}")
    logger.info(f"Sources: {', '.join(valid)}  force={args.force}  limit={args.limit or 'all'}")
    logger.info(f"Log: {log_file}")

    t0     = time.monotonic()
    totals = {'total_checked': 0, 'updated': 0, 'not_found': 0, 'errors': 0}

    for key in valid:
        stats = process_source(
            key, SOURCES[key],
            force=args.force, limit=args.limit, workers=args.workers
        )
        for k in totals:
            totals[k] += stats.get(k, 0)

    elapsed = time.monotonic() - t0
    logger.info(f"\n{'='*60}")
    logger.info(
        f"backfill_price complete — {elapsed:.0f}s\n"
        f"  Properties checked : {totals['total_checked']}\n"
        f"  Prices updated     : {totals['updated']}\n"
        f"  No price on page   : {totals['not_found']}\n"
        f"  Errors             : {totals['errors']}"
    )

    if totals['updated'] > 0:
        logger.info(
            "\nNext step — push updated prices to Supabase:\n"
            "  python3 supabase/migrate_data.py"
        )


if __name__ == '__main__':
    main()
