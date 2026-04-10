#!/usr/bin/env python3
"""
check_stale.py  –  Fast concurrent liveness checker for legacy-scraper sources.

For each property JSON in properties/{source}/, sends a lightweight HTTP GET
to the stored URL and checks whether the listing is still live.  Folders for
dead listings are deleted so stale data doesn't remain in Supabase.

Detection logic (works for all PropertyPal-CMS sites):
  • HTTP 4xx / 5xx  → delisted
  • Redirected away from a /property/ path → delisted (listing page 301/302)
  • Response body contains "property not found" / "no longer available" → delisted
  • HTTP 200 and URL still points to /property/ → still live

Concurrency: uses a thread pool so hundreds of checks complete in a few minutes
rather than hours.  Default 15 workers; tune with --workers.

Usage (run from re_app/ directory):
    python3 scrapers/check_stale.py                       # check all legacy sources
    python3 scrapers/check_stale.py sb hc                 # specific sources only
    python3 scrapers/check_stale.py --dry-run             # report but don't delete
    python3 scrapers/check_stale.py --workers 20          # more concurrency

Sources handled: sb, ups, hc, jm, pp, tr, dh
  (mm, ce, gm, pinp, rb manage their own stale detection via url_map.json)

Requires: pip3 install requests
"""

import os, sys, json, re, time, shutil, argparse, logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    import requests
except ImportError:
    print("Run: pip3 install requests")
    sys.exit(1)

# ── Config ─────────────────────────────────────────────────────────────────────

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Legacy sources only — mm/ce handle their own stale detection
SOURCES = {
    'sb':  os.path.join(ROOT, 'properties', 'sb'),
    'ups': os.path.join(ROOT, 'properties', 'ups'),
    'hc':  os.path.join(ROOT, 'properties', 'hc'),
    'jm':  os.path.join(ROOT, 'properties', 'jm'),
    'pp':  os.path.join(ROOT, 'properties', 'pp'),
    'tr':  os.path.join(ROOT, 'properties', 'tr'),
    'dh':  os.path.join(ROOT, 'properties', 'dh'),
}

TIMEOUT  = 12    # seconds per HTTP request
DELAY    = 0.05  # small delay between requests per worker to be polite

# Phrases that indicate a "property no longer available" page
DEAD_PHRASES = [
    'property not found',
    'no longer available',
    'listing not found',
    'page not found',
    'this property has been',
    'has been removed',
    'property has been sold',
    'sorry, we could not find',
    'sorry, this property',
]

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
    f"check_stale_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
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

# ── Liveness check ────────────────────────────────────────────────────────────

def is_live(url):
    """
    Returns (live: bool, reason: str).
    'live' = True means the listing is still active on the website.
    """
    try:
        r = requests.get(
            url,
            headers=HEADERS,
            timeout=TIMEOUT,
            allow_redirects=True,
        )
    except requests.exceptions.ConnectionError:
        return False, 'connection error'
    except requests.exceptions.Timeout:
        return False, 'timeout'
    except Exception as e:
        return False, f'request error: {e}'

    # HTTP error codes
    if r.status_code == 404:
        return False, 'HTTP 404'
    if r.status_code >= 400:
        return False, f'HTTP {r.status_code}'

    # Followed a redirect away from a /property/ path
    if '/property/' not in r.url:
        return False, f'redirected to {r.url[:80]}'

    # Page content analysis (only check the first 8 KB — fast, avoids big HTML)
    snippet = r.text[:8000].lower()
    for phrase in DEAD_PHRASES:
        if phrase in snippet:
            return False, f'page says "{phrase}"'

    return True, 'OK'

# ── Collect all property entries for a source ─────────────────────────────────

def collect_entries(source_key, props_dir):
    """
    Returns list of (folder_name, json_path, url) for all property_ dirs
    that have a valid URL in their JSON file.
    """
    entries = []
    if not os.path.isdir(props_dir):
        return entries

    for d in sorted(os.listdir(props_dir)):
        if not re.fullmatch(r'property_\d+', d):
            continue
        json_path = os.path.join(props_dir, d, f'{d}.json')
        if not os.path.isfile(json_path):
            continue
        try:
            with open(json_path, encoding='utf-8') as f:
                data = json.load(f)
            url = data.get('url', '').strip()
            if url:
                entries.append((d, json_path, url))
        except Exception:
            pass

    return entries

# ── Check one source ───────────────────────────────────────────────────────────

def check_source(source_key, props_dir, dry_run=False, workers=15):
    logger.info(f"\n{'─'*55}")
    logger.info(f"Source: {source_key.upper()}  ({props_dir})")

    entries = collect_entries(source_key, props_dir)
    if not entries:
        logger.info(f"  No properties found — skipping")
        return {'total': 0, 'live': 0, 'stale': 0, 'errors': 0, 'deleted': 0}

    logger.info(f"  Checking {len(entries)} properties with {workers} workers…")

    results = {}   # folder_name → (live, reason)

    def check(entry):
        folder, json_path, url = entry
        time.sleep(DELAY)
        live, reason = is_live(url)
        return folder, url, live, reason

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(check, e): e for e in entries}
        done = 0
        for future in as_completed(futures):
            folder, url, live, reason = future.result()
            results[folder] = (live, reason, url)
            done += 1
            if done % 50 == 0 or done == len(entries):
                logger.info(f"  … {done}/{len(entries)} checked")

    # Tally
    stale   = [(f, r, u) for f, (live, r, u) in results.items() if not live]
    live_ct = len(results) - len(stale)

    logger.info(f"\n  Results: {live_ct} live, {len(stale)} stale")

    deleted = 0
    for folder, reason, url in sorted(stale):
        logger.info(f"  {'[DRY-RUN] ' if dry_run else ''}STALE  {folder}  ({reason})")
        logger.info(f"           {url}")
        if not dry_run:
            prop_dir = os.path.join(props_dir, folder)
            if os.path.isdir(prop_dir):
                shutil.rmtree(prop_dir)
                deleted += 1

    if dry_run and stale:
        logger.info(f"  (dry-run: would delete {len(stale)} folders)")

    return {
        'total':   len(entries),
        'live':    live_ct,
        'stale':   len(stale),
        'errors':  0,
        'deleted': deleted,
    }

# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description='Check liveness of all local property listings and delete stale ones.'
    )
    parser.add_argument('sources', nargs='*', default=list(SOURCES.keys()),
                        help='Sources to check (default: all legacy sources)')
    parser.add_argument('--dry-run',  action='store_true',
                        help='Report stale listings but do not delete anything')
    parser.add_argument('--workers',  type=int, default=15,
                        help='Concurrent HTTP workers (default: 15)')
    args = parser.parse_args()

    sources_to_run = [s for s in args.sources if s in SOURCES]
    unknown        = [s for s in args.sources if s not in SOURCES]
    if unknown:
        logger.warning(f"Unknown/smart sources (skipped — they manage their own stale data): "
                       f"{', '.join(unknown)}")
    if not sources_to_run:
        logger.error('No valid legacy sources to check.')
        sys.exit(1)

    logger.info('=' * 60)
    logger.info(f"check_stale.py — {datetime.now().isoformat()}")
    logger.info(f"Sources: {', '.join(sources_to_run)}")
    logger.info(f"dry_run={args.dry_run}  workers={args.workers}")
    logger.info(f"Log: {log_file}")

    t0      = time.monotonic()
    totals  = {'total': 0, 'live': 0, 'stale': 0, 'deleted': 0}

    for key in sources_to_run:
        stats = check_source(key, SOURCES[key], dry_run=args.dry_run, workers=args.workers)
        for k in totals:
            totals[k] += stats.get(k, 0)

    elapsed = time.monotonic() - t0
    logger.info(f"\n{'='*60}")
    logger.info(
        f"check_stale complete — {elapsed:.0f}s\n"
        f"  Total checked : {totals['total']}\n"
        f"  Still live    : {totals['live']}\n"
        f"  Stale found   : {totals['stale']}\n"
        f"  Folders deleted: {totals['deleted']}"
        + (" (dry-run: nothing deleted)" if args.dry_run else "")
    )

    if totals['deleted'] > 0:
        logger.info(
            "\nNext step: push the delisted records to Supabase:\n"
            "  python3 supabase/migrate_data.py"
        )


if __name__ == '__main__':
    main()
