"""
full_scrape.py  –  Run all property scrapers (sales and/or rentals), in parallel where possible,
                   then automatically migrate the fresh data to Supabase.

By default this is a FRESH scrape: existing property folders are cleared before
scraping begins.  Pass --no-fresh to do an incremental update instead (only
meaningful for dh / mm / ce which have smart-update logic).

Scrapers are split into two parallel groups so that the three Selenium-based
scrapers don't all fight for Chrome resources at the same time:

  Group 1 (requests-only, fully parallel): sb  ups  hc  jm  pp  dh  pinp  rb
  Group 2 (Selenium, fully parallel):      tr  mm  ce  gm

Both groups run concurrently within themselves; Group 2 starts as soon as
Group 1 finishes (or you can run everything at once with --all-parallel).

After all scrapers complete, migrate_data.py is automatically run for each
successfully scraped source so the Supabase database stays in sync.
Pass --no-migrate to skip this step.

Usage (run from re_app/ directory):
    python3 full_scrape.py                     # fresh scrape + migrate all (sales only)
    python3 full_scrape.py --rent             # scrape rentals only
    python3 full_scrape.py --all             # scrape both sales and rentals
    python3 full_scrape.py sb ups jm           # specific sources only
    python3 full_scrape.py --no-fresh          # incremental update
    python3 full_scrape.py --no-migrate        # scrape only, skip DB push
    python3 full_scrape.py --all-parallel      # all at once (more RAM)
    python3 full_scrape.py --list              # show available sources
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

SCRIPT_DIR    = os.path.dirname(os.path.abspath(__file__))
SCRAPERS_DIR  = os.path.join(SCRIPT_DIR, 'scrapers')
MIGRATE_SCRIPT = os.path.join(SCRIPT_DIR, 'supabase', 'migrate_data.py')
PROPERTIES_DIR = os.path.join(SCRIPT_DIR, 'properties')

sys.path.insert(0, SCRAPERS_DIR)
from image_sort_utils import fix_source_dir, NUMBERED_SOURCES

# Prefer the venv interpreter (has the supabase package) for migration;
# fall back to whatever is running this script.
_venv_py = os.path.join(SCRIPT_DIR, 'venv', 'bin', 'python3')
MIGRATE_PYTHON = _venv_py if os.path.isfile(_venv_py) else sys.executable

# ──────────────────────────────────────────────────────────────
# Source registry
#   fresh_flag=True  → pass --fresh on the command line
#   fresh_flag=False → scraper auto-clears when not in test mode
# ──────────────────────────────────────────────────────────────

SOURCES = {
    # Sales
    'sb':   {'script': 'sb_full_scrape.py',   'label': 'Simon Brien',              'fresh_flag': False, 'rent': False},
    'ups':  {'script': 'ups_full_scrape.py',  'label': 'Ulster Property Sales',    'fresh_flag': False, 'rent': False},
    'hc':   {'script': 'hc_full_scrape.py',   'label': 'Hunter Campbell',          'fresh_flag': False, 'rent': False},
    'jm':   {'script': 'jm_full_scrape.py',   'label': 'John Minnis',              'fresh_flag': False, 'rent': False},
    'pp':   {'script': 'pp_full_scrape.py',   'label': 'Property People NI',       'fresh_flag': False, 'rent': False},
    'dh':   {'script': 'dh_scrape.py',        'label': 'Daniel Henry',             'fresh_flag': True,  'rent': False},
    'pinp': {'script': 'pinp_full_scrape.py', 'label': 'Pinpoint Property',        'fresh_flag': True,  'rent': False},
    'rb':   {'script': 'rb_full_scrape.py',   'label': 'Rodgers & Browne',         'fresh_flag': True,  'rent': False},
    'tr':   {'script': 'tr_full_scrape.py',   'label': 'Templeton Robinson',       'fresh_flag': False, 'rent': False},
    'mm':   {'script': 'mm_full_scrape.py',   'label': 'McMillan McClure',         'fresh_flag': True,  'rent': False},
    'ce':   {'script': 'ce_full_scrape.py',   'label': 'Country Estates',          'fresh_flag': True,  'rent': False},
    'gm':   {'script': 'gm_full_scrape.py',   'label': 'Gareth Mills Est. Agents', 'fresh_flag': True,  'rent': False},
}

# Rental sources (same scripts with --rent flag)
RENT_SOURCES = {
    'sb_rent':   {'script': 'sb_full_scrape.py',   'label': 'Simon Brien (rent)',              'fresh_flag': False, 'rent': True},
    'ups_rent':  {'script': 'ups_full_scrape.py',  'label': 'Ulster Property Sales (rent)',    'fresh_flag': False, 'rent': True},
    'hc_rent':   {'script': 'hc_full_scrape.py',   'label': 'Hunter Campbell (rent)',          'fresh_flag': False, 'rent': True},
    'jm_rent':   {'script': 'jm_full_scrape.py',   'label': 'John Minnis (rent)',              'fresh_flag': False, 'rent': True},
    'pp_rent':   {'script': 'pp_full_scrape.py',   'label': 'Property People NI (rent)',       'fresh_flag': False, 'rent': True},
    'dh_rent':   {'script': 'dh_scrape.py',        'label': 'Daniel Henry (rent)',             'fresh_flag': True,  'rent': True},
    'rb_rent':   {'script': 'rb_full_scrape.py',   'label': 'Rodgers & Browne (rent)',         'fresh_flag': True,  'rent': True},
    'tr_rent':   {'script': 'tr_full_scrape.py',   'label': 'Templeton Robinson (rent)',       'fresh_flag': False, 'rent': True},
    'mm_rent':   {'script': 'mm_full_scrape.py',   'label': 'McMillan McClure (rent)',         'fresh_flag': True,  'rent': True},
    'ce_rent':   {'script': 'ce_full_scrape.py',   'label': 'Country Estates (rent)',          'fresh_flag': True,  'rent': True},
    'gm_rent':   {'script': 'gm_full_scrape.py',   'label': 'Gareth Mills Est. Agents (rent)', 'fresh_flag': True,  'rent': True},
}

# Default parallel groups:
#   Group 1 – requests-only (light on resources, run together first)
#   Group 2 – Selenium-based (Chrome instances, run together after group 1)
PARALLEL_GROUPS = [
    ['sb', 'ups', 'hc', 'jm', 'pp', 'dh', 'pinp', 'rb'],
    ['tr', 'mm', 'ce', 'gm'],
]

# ──────────────────────────────────────────────
# Logging
# ──────────────────────────────────────────────

os.makedirs(os.path.join(SCRIPT_DIR, 'logs'), exist_ok=True)
log_filename = os.path.join(
    SCRIPT_DIR, 'logs',
    f"full_scrape_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
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
# Runner
# ──────────────────────────────────────────────

def run_source(source_key, fresh=True, rent=False):
    """Launch a scraper in a subprocess and wait for it to finish."""
    # Check if it's a rental source
    if source_key.endswith('_rent'):
        rent = True
        info = RENT_SOURCES[source_key]
    else:
        info = SOURCES[source_key]
    script = os.path.join(SCRAPERS_DIR, info['script'])
    label  = info['label']

    cmd = [sys.executable, script]
    if fresh and info['fresh_flag']:
        cmd.append('--fresh')
    if rent or info.get('rent', False):
        cmd.append('--rent')

    logger.info(f"[{source_key.upper()}] Starting  →  {' '.join(cmd)}")
    start = time.time()

    try:
        result = subprocess.run(
            cmd,
            cwd=SCRIPT_DIR,          # run from re_app/ so relative paths work
            capture_output=False,    # let stdout/stderr stream to terminal
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
    """
    Run migrate_data.py once per successfully-scraped source.
    Returns dict of {key: bool} indicating success.
    """
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


def run_image_sort(source_keys):
    """Re-sort image_urls in every property JSON for each source in *source_keys*.

    Only runs for sources whose images carry a numeric suffix (NUMBERED_SOURCES).
    Returns dict of {key: bool} — True if sort completed without error.
    """
    results = {}
    sortable = [k for k in source_keys if k in NUMBERED_SOURCES]
    if not sortable:
        return results

    logger.info(f"\n{'='*60}")
    logger.info(f"Sorting image URLs for: {sortable}")
    logger.info(f"{'='*60}")

    for key in sortable:
        try:
            stats = fix_source_dir(key, PROPERTIES_DIR)
            logger.info(
                f"[SORT:{key.upper()}] fixed={stats['fixed']}  "
                f"unchanged={stats['unchanged']}  total={stats['total']}"
            )
            results[key] = True
        except Exception as exc:
            logger.error(f"[SORT:{key.upper()}] ✗ {exc}")
            results[key] = False

    return results


def run_group_parallel(keys, fresh, label, rent=False):
    """Run a list of sources in parallel; return dict of {key: bool}."""
    if not keys:
        return {}

    logger.info(f"\n{'='*60}")
    logger.info(f"Running {label}: {keys}")
    logger.info(f"{'='*60}")

    results = {}
    with ThreadPoolExecutor(max_workers=len(keys)) as pool:
        futures = {pool.submit(run_source, k, fresh, rent): k for k in keys}
        for future in as_completed(futures):
            key = futures[future]
            try:
                results[key] = future.result()
            except Exception as exc:
                logger.error(f"[{key.upper()}] Unexpected error: {exc}")
                results[key] = False
    return results


# ──────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description='Run all property scrapers (fresh by default).',
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
        '--no-fresh', action='store_true',
        help='Skip clearing existing data (incremental update for dh/mm/ce)'
    )
    parser.add_argument(
        '--all-parallel', action='store_true',
        help='Run all scrapers at once instead of in two groups'
    )
    parser.add_argument(
        '--no-migrate', action='store_true',
        help='Skip the automatic Supabase migration step after scraping'
    )
    parser.add_argument(
        '--list', action='store_true',
        help='List available sources and exit'
    )
    parser.add_argument(
        '--rent', action='store_true',
        help='Scrape rental listings only (default is sales only)'
    )
    parser.add_argument(
        '--all', action='store_true',
        help='Scrape both sales and rental listings'
    )
    args = parser.parse_args()

    if args.list:
        print("Available sources:")
        print("\nSales:")
        groups_flat = {k: g for g, group in enumerate(PARALLEL_GROUPS) for k in group}
        for k, v in SOURCES.items():
            g = groups_flat.get(k, '?')
            print(f"  {k:8s}  {v['label']:35s}  (group {g+1})")
        print("\nRentals:")
        for k, v in RENT_SOURCES.items():
            base_key = k.replace('_rent', '')
            g = groups_flat.get(base_key, '?')
            print(f"  {k:8s}  {v['label']:35s}  (group {g+1})")
        return

    # Determine which sources to run
    all_sales_sources = list(SOURCES.keys())
    all_rent_sources = list(RENT_SOURCES.keys())

    if args.all:
        # Scrape both sales and rentals
        sources_to_run = all_sales_sources + all_rent_sources
        scrape_type = "sales + rentals"
    elif args.rent:
        # Scrape rentals only
        sources_to_run = all_rent_sources
        scrape_type = "rentals only"
    else:
        # Default: sales only
        sources_to_run = [s for s in args.sources if s in SOURCES]
        unknown = [s for s in args.sources if s not in SOURCES]
        if unknown:
            logger.warning(f"Unknown sources (ignored): {unknown}")
        if not sources_to_run:
            logger.error("No valid sources to run.")
            sys.exit(1)
        scrape_type = "sales only"

    fresh = not args.no_fresh

    logger.info(f"\n{'='*60}")
    logger.info(f"Full scrape starting  —  {datetime.now().isoformat()}")
    logger.info(f"Type    : {scrape_type}")
    logger.info(f"Sources : {len(sources_to_run)}")
    logger.info(f"Fresh   : {fresh}")
    logger.info(f"Migrate : {not args.no_migrate}")
    logger.info(f"Log     : {log_filename}")
    logger.info(f"{'='*60}")

    overall_start = time.time()
    all_results = {}

    # Build parallel groups for rentals too
    RENT_PARALLEL_GROUPS = [
        [f'{k}_rent' for k in group] for group in PARALLEL_GROUPS
    ]

    if args.all_parallel:
        # Single group — everything at once
        all_results = run_group_parallel(sources_to_run, fresh, "all sources in parallel", rent=args.rent)
    else:
        # Two groups: requests-only first, then Selenium
        for g_idx, (sales_group, rent_group) in enumerate(zip(PARALLEL_GROUPS, RENT_PARALLEL_GROUPS)):
            # Combine sales and rental sources for this group
            group = []
            for k in sales_group:
                if k in sources_to_run:
                    group.append(k)
            for k in rent_group:
                if k in sources_to_run:
                    group.append(k)
            if not group:
                continue
            group_label = f"group {g_idx+1}"
            results = run_group_parallel(group, fresh, group_label, rent=args.rent)
            all_results.update(results)

    # ── Scrape summary ────────────────────────────────────────
    total_elapsed = time.time() - overall_start
    logger.info(f"\n{'='*60}")
    logger.info(f"Scraping complete in {total_elapsed/60:.1f} min")
    for key in sources_to_run:
        status = 'OK' if all_results.get(key) else 'FAILED'
        # Get label from appropriate source dict
        if key in SOURCES:
            label = SOURCES[key]['label']
        else:
            label = RENT_SOURCES[key]['label']
        logger.info(f"  {key.upper():8s}  {label:35s}  {status}")

    scraped_ok = [k for k in sources_to_run if all_results.get(k)]
    failed     = [k for k in sources_to_run if not all_results.get(k)]

    # ── Migration ─────────────────────────────────────────────
    migrate_results = {}
    if not args.no_migrate:
        if scraped_ok:
            logger.info(f"\n{'='*60}")
            logger.info(f"Migrating {len(scraped_ok)} source(s) to Supabase: {scraped_ok}")
            logger.info(f"{'='*60}")
            migrate_results = run_migration(scraped_ok)
        else:
            logger.warning("No sources scraped successfully — skipping migration.")
    else:
        logger.info("\n--no-migrate: skipping Supabase migration.")

    # ── Image sort ────────────────────────────────────────────
    sort_results = run_image_sort(scraped_ok)

    # ── Final summary ─────────────────────────────────────────
    logger.info(f"\n{'='*60}")
    logger.info("Done.")
    for key in sources_to_run:
        scrape_ok_flag  = '✓' if all_results.get(key) else '✗'
        if args.no_migrate:
            migrate_ok = '-'
        elif key in migrate_results:
            migrate_ok = '✓' if migrate_results[key] else '✗'
        else:
            migrate_ok = '-'
        if key in sort_results:
            sort_ok = '✓' if sort_results[key] else '✗'
        elif key not in NUMBERED_SOURCES:
            sort_ok = 'n/a'   # MM/CE use hash filenames — no sort needed
        else:
            sort_ok = '-'     # scrape failed, sort not attempted
        # Get label from appropriate source dict
        if key in SOURCES:
            label = SOURCES[key]['label']
        else:
            label = RENT_SOURCES[key]['label']
        logger.info(
            f"  {key.upper():8s}  {label:35s}  "
            f"scrape:{scrape_ok_flag}  migrate:{migrate_ok}  sort:{sort_ok}"
        )

    migrate_failed = [k for k, ok in migrate_results.items() if not ok]
    sort_failed    = [k for k, ok in sort_results.items()   if not ok]
    if failed:
        logger.error(f"Scrape failed: {failed}")
    if migrate_failed:
        logger.error(f"Migration failed: {migrate_failed}")
    if sort_failed:
        logger.error(f"Image sort failed: {sort_failed}")

    if failed or migrate_failed or sort_failed:
        sys.exit(1)
    else:
        logger.info("All sources scraped, migrated, and sorted successfully.")


if __name__ == '__main__':
    main()
