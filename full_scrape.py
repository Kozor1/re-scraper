"""
full_scrape.py  –  Run all property scrapers in parallel where possible,
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
    python3 full_scrape.py                     # fresh scrape + migrate all
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
GEOCODE_SCRIPT = os.path.join(SCRIPT_DIR, 'geocode.py')
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
    'sb':   {'script': 'sb_full_scrape.py',   'label': 'Simon Brien',              'fresh_flag': False},
    'ups':  {'script': 'ups_full_scrape.py',  'label': 'Ulster Property Sales',    'fresh_flag': False},
    'hc':   {'script': 'hc_full_scrape.py',   'label': 'Hunter Campbell',          'fresh_flag': False},
    'jm':   {'script': 'jm_full_scrape.py',   'label': 'John Minnis',              'fresh_flag': False},
    'pp':   {'script': 'pp_full_scrape.py',   'label': 'Property People NI',       'fresh_flag': False},
    'dh':   {'script': 'dh_scrape.py',        'label': 'Daniel Henry',             'fresh_flag': True,  'rent': False},
    'pinp': {'script': 'pinp_full_scrape.py', 'label': 'Pinpoint Property',        'fresh_flag': True,  'rent': False},
    'rb':   {'script': 'rb_full_scrape.py',   'label': 'Rodgers & Browne',         'fresh_flag': True,  'rent': False},
    'tr':   {'script': 'tr_full_scrape.py',   'label': 'Templeton Robinson',       'fresh_flag': False},
    'mm':   {'script': 'mm_full_scrape.py',   'label': 'McMillan McClure',         'fresh_flag': True,  'rent': False},
    'ce':   {'script': 'ce_full_scrape.py',   'label': 'Country Estates',          'fresh_flag': True,  'rent': False},
    'gm':   {'script': 'gm_full_scrape.py',   'label': 'Gareth Mills Est. Agents', 'fresh_flag': True,  'rent': False},
    # New agents (batch 2025-04)
    'mc':   {'script': 'mc_full_scrape.py',   'label': 'Michael Chandler',         'fresh_flag': False},
    'ft':   {'script': 'ft_full_scrape.py',   'label': 'Fetherstons',              'fresh_flag': False},
    'pr':   {'script': 'pr_full_scrape.py',   'label': 'Peter Rodgers',            'fresh_flag': False},
    'cps':  {'script': 'cps_full_scrape.py',  'label': 'CPS',                      'fresh_flag': False},
    'hn':   {'script': 'hn_full_scrape.py',   'label': 'Hannath',                  'fresh_flag': False},
    'bt':   {'script': 'bt_full_scrape.py',   'label': 'Brian Todd',               'fresh_flag': False},
    'rr':   {'script': 'rr_full_scrape.py',   'label': 'Reeds Rains',              'fresh_flag': False},
    'ee':   {'script': 'ee_full_scrape.py',   'label': 'Edmonton Estates',         'fresh_flag': False},
    'ag':   {'script': 'ag_full_scrape.py',   'label': 'Armstrong Gordon',         'fresh_flag': False},
    'ta':   {'script': 'ta_full_scrape.py',   'label': 'The Agent',                'fresh_flag': False},
    'abc':  {'script': 'abc_full_scrape.py',  'label': 'A Barton Company',         'fresh_flag': False},
    'hg':   {'script': 'hg_full_scrape.py',   'label': 'Henry Graham',             'fresh_flag': False},
    'le':   {'script': 'le_full_scrape.py',   'label': 'Lennon Estates',           'fresh_flag': False},
    'amd':  {'script': 'amd_full_scrape.py',  'label': 'Agar Murdoch and Deane',   'fresh_flag': False},
    'tm':   {'script': 'tm_full_scrape.py',   'label': 'Tim Martin',               'fresh_flag': False},
    'ma':   {'script': 'ma_full_scrape.py',   'label': 'McAllister',               'fresh_flag': False},
    'dl':   {'script': 'dl_full_scrape.py',   'label': 'Dallas',                   'fresh_flag': False},
    'bmc':  {'script': 'bmc_full_scrape.py',  'label': 'Bill McCann',              'fresh_flag': False},
    'ag2':  {'script': 'ag2_full_scrape.py',  'label': 'Andrews & Gregg',          'fresh_flag': False},
    'ipe':  {'script': 'ipe_full_scrape.py',  'label': 'Independent Property Est', 'fresh_flag': False},
    'mmc':  {'script': 'mmc_full_scrape.py',  'label': 'Montgomery & McCleary',    'fresh_flag': False},
    'pe':   {'script': 'pe_full_scrape.py',   'label': 'Pauline Elliott',          'fresh_flag': False},
}

# Default parallel groups:
#   Group 1 – requests-only (light on resources, run together first)
#   Group 2 – Selenium-based (Chrome instances, run together after group 1)
PARALLEL_GROUPS = [
    ['sb', 'ups', 'hc', 'jm', 'pp', 'dh', 'pinp', 'rb',
     'mc', 'ft', 'pr', 'cps', 'hn', 'bt', 'rr',
     'ee', 'ag', 'ta', 'abc', 'hg', 'le', 'amd',
     'tm', 'ma', 'dl', 'bmc', 'ag2', 'ipe', 'mmc', 'pe'],
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

def run_source(source_key, fresh=True):
    """Launch a scraper in a subprocess and wait for it to finish."""
    info = SOURCES[source_key]
    script = os.path.join(SCRAPERS_DIR, info['script'])
    label  = info['label']

    cmd = [sys.executable, script]
    if fresh and info['fresh_flag']:
        cmd.append('--fresh')

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


def run_geocoding(source_keys):
    """
    Run geocode.py for all successfully-scraped sources.
    Geocoding is done once for all sources together (geocode.py handles all properties).
    Returns bool indicating success.
    """
    if not os.path.isfile(GEOCODE_SCRIPT):
        logger.error(f"Geocode script not found: {GEOCODE_SCRIPT}")
        return False

    cmd = [sys.executable, GEOCODE_SCRIPT]
    logger.info(f"[GEOCODE] {' '.join(cmd)}")
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
            logger.info(f"[GEOCODE] ✓ Done in {elapsed:.0f}s")
            return True
        else:
            logger.error(f"[GEOCODE] ✗ Exit {result.returncode} after {elapsed:.0f}s")
            return False
    except Exception as exc:
        logger.error(f"[GEOCODE] ✗ Exception: {exc}")
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


def run_group_parallel(keys, fresh, label):
    """Run a list of sources in parallel; return dict of {key: bool}."""
    if not keys:
        return {}

    logger.info(f"\n{'='*60}")
    logger.info(f"Running {label}: {keys}")
    logger.info(f"{'='*60}")

    results = {}
    with ThreadPoolExecutor(max_workers=len(keys)) as pool:
        futures = {pool.submit(run_source, k, fresh): k for k in keys}
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
        '--no-geocode', action='store_true',
        help='Skip the geocoding step (skip calling geocode.py)'
    )
    parser.add_argument(
        '--list', action='store_true',
        help='List available sources and exit'
    )
    args = parser.parse_args()

    if args.list:
        print("Available sources:")
        groups_flat = {k: g for g, group in enumerate(PARALLEL_GROUPS) for k in group}
        for k, v in SOURCES.items():
            g = groups_flat.get(k, '?')
            print(f"  {k:8s}  {v['label']:35s}  (group {g+1})")
        return

    # Determine which sources to run
    sources_to_run = [s for s in args.sources if s in SOURCES]
    unknown = [s for s in args.sources if s not in SOURCES]
    if unknown:
        logger.warning(f"Unknown sources (ignored): {unknown}")
    if not sources_to_run:
        logger.error("No valid sources to run.")
        sys.exit(1)

    fresh = not args.no_fresh

    logger.info(f"\n{'='*60}")
    logger.info(f"Full scrape starting  —  {datetime.now().isoformat()}")
    logger.info(f"Sources : {len(sources_to_run)}")
    logger.info(f"Fresh   : {fresh}")
    logger.info(f"Migrate : {not args.no_migrate}")
    logger.info(f"Log     : {log_filename}")
    logger.info(f"{'='*60}")

    overall_start = time.time()
    all_results = {}

    if args.all_parallel:
        # Single group — everything at once
        all_results = run_group_parallel(sources_to_run, fresh, "all sources in parallel")
    else:
        # Two groups: requests-only first, then Selenium
        for g_idx, group in enumerate(PARALLEL_GROUPS):
            # Filter to only sources that need to run
            group_keys = [k for k in group if k in sources_to_run]
            if not group_keys:
                continue
            group_label = f"group {g_idx+1}"
            results = run_group_parallel(group_keys, fresh, group_label)
            all_results.update(results)

    # ── Scrape summary ────────────────────────────────────────
    total_elapsed = time.time() - overall_start
    logger.info(f"\n{'='*60}")
    logger.info(f"Scraping complete in {total_elapsed/60:.1f} min")
    for key in sources_to_run:
        status = 'OK' if all_results.get(key) else 'FAILED'
        label = SOURCES[key]['label']
        logger.info(f"  {key.upper():8s}  {label:35s}  {status}")

    scraped_ok = [k for k in sources_to_run if all_results.get(k)]
    failed     = [k for k in sources_to_run if not all_results.get(k)]

    # ── Image sort ────────────────────────────────────────────
    sort_results = run_image_sort(scraped_ok)

    # ── Geocoding ──────────────────────────────────────────────
    geocode_ok = False
    if not args.no_geocode:
        if scraped_ok:
            logger.info(f"\n{'='*60}")
            logger.info(f"Geocoding properties from {len(scraped_ok)} source(s)")
            logger.info(f"{'='*60}")
            geocode_ok = run_geocoding(scraped_ok)
        else:
            logger.warning("No sources scraped successfully — skipping geocoding.")
    else:
        logger.info("\n--no-geocode: skipping geocoding step.")

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

    # ── Final summary ─────────────────────────────────────────
    logger.info(f"\n{'='*60}")
    logger.info("Done.")

    # Geocode status (runs once for all sources)
    if args.no_geocode:
        geocode_status = 'skipped'
    else:
        geocode_status = '✓' if geocode_ok else '✗'
    logger.info(f"Geocode: {geocode_status}")

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
        label = SOURCES[key]['label']
        logger.info(
            f"  {key.upper():8s}  {label:35s}  "
            f"scrape:{scrape_ok_flag}  migrate:{migrate_ok}  sort:{sort_ok}"
        )

    migrate_failed = [k for k, ok in migrate_results.items() if not ok]
    sort_failed    = [k for k, ok in sort_results.items()   if not ok]
    if failed:
        logger.error(f"Scrape failed: {failed}")
    if not args.no_geocode and not geocode_ok:
        logger.error(f"Geocoding failed")
    if migrate_failed:
        logger.error(f"Migration failed: {migrate_failed}")
    if sort_failed:
        logger.error(f"Image sort failed: {sort_failed}")

    if failed or (not args.no_geocode and not geocode_ok) or migrate_failed or sort_failed:
        sys.exit(1)
    else:
        logger.info("All sources scraped, geocoded, migrated, and sorted successfully.")


if __name__ == '__main__':
    main()
