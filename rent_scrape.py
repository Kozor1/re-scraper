"""
rent_scrape.py  –  Run all 9 property scrapers in rental mode, then migrate
                   the fresh data to Supabase.

Usage (run from the swome-scraper/ directory):
    python3 rent_scrape.py                  # all agents, all properties
    python3 rent_scrape.py --limit 20       # cap to 20 per agent (testing)
    python3 rent_scrape.py sb hc jm         # specific agents only
    python3 rent_scrape.py --no-migrate     # scrape only, skip DB push
    python3 rent_scrape.py --list           # show available sources
"""

import sys
import os
import time
import logging
import argparse
import subprocess
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

SCRIPT_DIR     = os.path.dirname(os.path.abspath(__file__))
SCRAPERS_DIR   = os.path.join(SCRIPT_DIR, 'scrapers')
MIGRATE_SCRIPT = os.path.join(SCRIPT_DIR, 'supabase', 'migrate_data.py')

# Prefer the venv interpreter for migration (has the supabase package).
_venv_py = os.path.join(SCRIPT_DIR, 'venv', 'bin', 'python3')
MIGRATE_PYTHON = _venv_py if os.path.isfile(_venv_py) else sys.executable

DEFAULT_LIMIT = 0   # 0 = unlimited (scrape all rental properties)

# ──────────────────────────────────────────────────────────────
# Source registry  (key = rent source key used for migration)
# ──────────────────────────────────────────────────────────────
SOURCES = {
    'sb_rent':   {'script': 'sb_full_scrape.py',   'label': 'Simon Brien (rent)'},
    'ups_rent':  {'script': 'ups_full_scrape.py',  'label': 'Ulster Property Sales (rent)'},
    'hc_rent':   {'script': 'hc_full_scrape.py',   'label': 'Hunter Campbell (rent)'},
    'jm_rent':   {'script': 'jm_full_scrape.py',   'label': 'John Minnis (rent)'},
    'pp_rent':   {'script': 'pp_full_scrape.py',   'label': 'Property People NI (rent)'},
    'dh_rent':   {'script': 'dh_scrape.py',        'label': 'Daniel Henry (rent)'},
    'rb_rent':   {'script': 'rb_full_scrape.py',   'label': 'Rodgers & Browne (rent)'},
    'tr_rent':   {'script': 'tr_full_scrape.py',   'label': 'Templeton Robinson (rent)'},
    'mm_rent':   {'script': 'mm_full_scrape.py',   'label': 'McMillan McClure (rent)'},
    'ce_rent':   {'script': 'ce_full_scrape.py',   'label': 'Country Estates (rent)'},
    'gm_rent':   {'script': 'gm_full_scrape.py',   'label': 'Gareth Mills Est. Agents (rent)'},
}

# Run requests-only scrapers first (group 1), then Selenium-based (group 2)
PARALLEL_GROUPS = [
    ['sb_rent', 'ups_rent', 'hc_rent', 'jm_rent', 'pp_rent', 'dh_rent', 'rb_rent'],
    ['tr_rent', 'mm_rent', 'ce_rent', 'gm_rent'],
]

# ──────────────────────────────────────────────
# Logging
# ──────────────────────────────────────────────

os.makedirs(os.path.join(SCRIPT_DIR, 'logs'), exist_ok=True)
log_filename = os.path.join(
    SCRIPT_DIR, 'logs',
    f"rent_scrape_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
)
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
# Runner helpers
# ──────────────────────────────────────────────

def run_source(source_key, limit):
    """Launch a rental scraper in a subprocess and wait for it to finish."""
    info   = SOURCES[source_key]
    script = os.path.join(SCRAPERS_DIR, info['script'])
    label  = info['label']

    cmd = [sys.executable, script, '--rent', '--limit', str(limit)]

    logger.info(f"[{source_key.upper()}] Starting  →  {' '.join(cmd)}")
    start = time.time()

    try:
        result = subprocess.run(
            cmd,
            cwd=SCRIPT_DIR,
            capture_output=False,
            text=True,
        )
        elapsed = time.time() - start
        if result.returncode == 0:
            logger.info(f"[{source_key.upper()}] ✓ Complete in {elapsed:.0f}s")
            return True
        else:
            logger.error(f"[{source_key.upper()}] ✗ Exited with code {result.returncode} after {elapsed:.0f}s")
            return False

    except Exception as exc:
        elapsed = time.time() - start
        logger.error(f"[{source_key.upper()}] ✗ Exception after {elapsed:.0f}s: {exc}")
        logger.error(traceback.format_exc())
        return False


def run_migration(source_keys):
    """Run migrate_data.py for each successfully-scraped rental source."""
    if not os.path.isfile(MIGRATE_SCRIPT):
        logger.error(f"Migration script not found: {MIGRATE_SCRIPT}")
        return {k: False for k in source_keys}

    results = {}
    for key in source_keys:
        cmd = [MIGRATE_PYTHON, MIGRATE_SCRIPT, '--source', key]
        logger.info(f"[MIGRATE:{key.upper()}] {' '.join(cmd)}")
        start = time.time()
        try:
            result = subprocess.run(
                cmd,
                cwd=SCRIPT_DIR,
                capture_output=False,
                text=True,
            )
            elapsed = time.time() - start
            if result.returncode == 0:
                logger.info(f"[MIGRATE:{key.upper()}] ✓ Done in {elapsed:.0f}s")
                results[key] = True
            else:
                logger.error(f"[MIGRATE:{key.upper()}] ✗ Exit {result.returncode} after {elapsed:.0f}s")
                results[key] = False
        except Exception as exc:
            logger.error(f"[MIGRATE:{key.upper()}] ✗ Exception: {exc}")
            results[key] = False
    return results


def run_group_parallel(source_keys, limit, label):
    """Run a group of scrapers in parallel using a thread pool."""
    if not source_keys:
        return {}

    logger.info(f"\n{'='*60}")
    logger.info(f"Running {label}  ({len(source_keys)} sources in parallel)")
    logger.info(f"  {source_keys}")
    logger.info(f"{'='*60}")

    results = {}
    with ThreadPoolExecutor(max_workers=len(source_keys)) as pool:
        futures = {pool.submit(run_source, key, limit): key for key in source_keys}
        for future in as_completed(futures):
            key = futures[future]
            try:
                results[key] = future.result()
            except Exception as exc:
                logger.error(f"[{key.upper()}] Unexpected exception: {exc}")
                results[key] = False
    return results


# ──────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description='Run all rental property scrapers.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        'sources', nargs='*',
        default=list(SOURCES.keys()),
        metavar='SOURCE',
        help=f"Sources to scrape. Options: {', '.join(SOURCES.keys())} (default: all)"
    )
    parser.add_argument(
        '--limit', type=int, default=DEFAULT_LIMIT,
        help='Max properties per agent (default: 0 = unlimited)'
    )
    parser.add_argument(
        '--all-parallel', action='store_true',
        help='Run all 9 scrapers at once instead of in two groups'
    )
    parser.add_argument(
        '--no-migrate', action='store_true',
        help='Skip the automatic Supabase migration step after scraping'
    )
    parser.add_argument(
        '--list', action='store_true',
        help='List available sources and exit'
    )
    args = parser.parse_args()

    if args.list:
        print("Available rental sources:")
        for k, v in SOURCES.items():
            print(f"  {k:10s}  {v['label']}")
        return

    # Validate requested sources
    sources_to_run = [s for s in args.sources if s in SOURCES]
    unknown = [s for s in args.sources if s not in SOURCES]
    if unknown:
        logger.warning(f"Unknown sources (ignored): {unknown}")
    if not sources_to_run:
        logger.error("No valid sources to run.")
        sys.exit(1)

    logger.info(f"\n{'='*60}")
    logger.info(f"Rental scrape starting  —  {datetime.now().isoformat()}")
    logger.info(f"Sources : {sources_to_run}")
    logger.info(f"Limit   : {args.limit} properties per agent")
    logger.info(f"Migrate : {not args.no_migrate}")
    logger.info(f"Log     : {log_filename}")
    logger.info(f"{'='*60}")

    overall_start = time.time()
    all_results = {}

    if args.all_parallel:
        all_results = run_group_parallel(sources_to_run, args.limit, "all sources in parallel")
    else:
        for g_idx, group_keys in enumerate(PARALLEL_GROUPS):
            group = [k for k in group_keys if k in sources_to_run]
            if not group:
                continue
            group_label = f"group {g_idx + 1}"
            results = run_group_parallel(group, args.limit, group_label)
            all_results.update(results)

    overall_elapsed = time.time() - overall_start

    # ── Summary ──────────────────────────────────────────────────────
    scraped_ok = [k for k, ok in all_results.items() if ok]
    failed     = [k for k, ok in all_results.items() if not ok]

    migrate_results = {}
    if not args.no_migrate:
        if scraped_ok:
            migrate_results = run_migration(scraped_ok)
        else:
            logger.warning("No sources scraped successfully — skipping migration.")
    else:
        logger.info("\n--no-migrate: skipping Supabase migration.")

    logger.info(f"\n{'='*60}")
    logger.info(f"Rental scrape complete in {overall_elapsed:.0f}s")
    logger.info(f"{'='*60}")
    logger.info(f"{'Source':<12}  {'Scrape':<8}  {'Migrate'}")
    logger.info(f"{'-'*40}")
    for key in sources_to_run:
        scrape_ok_flag = '✓' if all_results.get(key) else '✗'
        if args.no_migrate:
            migrate_ok = '-'
        elif key in migrate_results:
            migrate_ok = '✓' if migrate_results[key] else '✗'
        else:
            migrate_ok = '-'
        label = SOURCES[key]['label']
        logger.info(f"  {key:<12}  scrape:{scrape_ok_flag}  migrate:{migrate_ok}  ({label})")

    migrate_failed = [k for k, ok in migrate_results.items() if not ok]
    if failed:
        logger.error(f"Scrape failed: {failed}")
    if migrate_failed:
        logger.error(f"Migration failed: {migrate_failed}")

    if failed or migrate_failed:
        sys.exit(1)


if __name__ == '__main__':
    main()
