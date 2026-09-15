"""
full_scrape.py  –  Run all property scrapers in parallel where possible,
                   then automatically migrate the fresh data to Supabase.

By default this is a FRESH scrape: existing property folders are cleared before
scraping begins for every source whose fresh_flag is true (the default).
Pass --no-fresh to skip the clear step entirely (all sources have
fresh_flag: true in config/sources.py).

Scrapers are split into two parallel groups so that Selenium-based scrapers
don't all fight for Chrome resources at the same time:

  Group 1 (requests-only, 32 sources): runs fully in parallel
  Group 2 (Selenium, 3 sources):       tr  ce  gm

Both groups run concurrently within themselves; Group 2 starts as soon as
Group 1 finishes (or you can run everything at once with --all-parallel).

After all scrapers complete, migrate_data.py is automatically run for each
successfully scraped source so the Supabase database stays in sync.
Pass --no-migrate to skip this step.

Usage (run from re-scraper/ directory):
    python3 full_scrape.py                     # fresh scrape + migrate all
    python3 full_scrape.py sb ups jm           # specific sources only
    python3 full_scrape.py --no-fresh          # incremental update
    python3 full_scrape.py --no-migrate        # scrape only, skip DB push
    python3 full_scrape.py --all-parallel      # all at once (more RAM)
    python3 full_scrape.py --list              # show available sources
"""

from __future__ import annotations

import os
import sys
import time
import argparse
import subprocess
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
SCRAPERS_DIR = os.path.join(SCRIPT_DIR, "scrapers")
sys.path.insert(0, SCRIPT_DIR)

from config import (
    SOURCES,
    PARALLEL_GROUPS,
    NUMBERED_IMAGE_SOURCES,
    setup_logging,
)

PYTHON_ENV = {**os.environ, "PYTHONPATH": SCRIPT_DIR}
MIGRATE_SCRIPT = os.path.join(SCRIPT_DIR, "supabase", "migrate_data.py")
GEOCODE_SCRIPT = os.path.join(SCRIPT_DIR, "geocode.py")
PROPERTIES_DIR = os.path.join(SCRIPT_DIR, "properties")

# Prefer the venv interpreter for migration (macOS/Linux and Windows layouts)
_VENVPY_CANDIDATES = (
    os.path.join(SCRIPT_DIR, "venv", "bin", "python3"),        # macOS / Linux
    os.path.join(SCRIPT_DIR, "venv", "Scripts", "python.exe"),  # Windows
)
MIGRATE_PYTHON = next(
    (p for p in _VENVPY_CANDIDATES if os.path.isfile(p)), sys.executable
)

logger = setup_logging("full_scrape")


# ── Runner ────────────────────────────────────────────────────────────────────


def run_source(source_key: str, fresh: bool = True) -> bool:
    """Launch a scraper in a subprocess and wait for it to finish."""
    info = SOURCES[source_key]
    script = os.path.join(SCRAPERS_DIR, info["module"] + ".py")
    label = info["label"]

    cmd = [sys.executable, script]
    if fresh and info.get("fresh_flag", True):
        cmd.append("--fresh")

    logger.info(f"[{source_key.upper()}] Starting  →  {' '.join(cmd)}")
    start = time.time()

    try:
        result = subprocess.run(
            cmd, cwd=SCRIPT_DIR, capture_output=False, text=True, env=PYTHON_ENV
        )
        elapsed = time.time() - start
        if result.returncode == 0:
            logger.info(f"[{source_key.upper()}] ✓ Complete in {elapsed:.0f}s")
            return True
        else:
            logger.error(
                f"[{source_key.upper()}] ✗ Exited with code {result.returncode} after {elapsed:.0f}s"
            )
            return False
    except Exception as exc:
        elapsed = time.time() - start
        logger.error(f"[{source_key.upper()}] ✗ Exception after {elapsed:.0f}s: {exc}")
        logger.error(traceback.format_exc())
        return False


def run_geocoding() -> bool:
    """Run geocode.py for all sources. Returns success bool."""
    if not os.path.isfile(GEOCODE_SCRIPT):
        logger.error(f"Geocode script not found: {GEOCODE_SCRIPT}")
        return False
    cmd = [sys.executable, GEOCODE_SCRIPT]
    logger.info(f"[GEOCODE] {' '.join(cmd)}")
    start = time.time()
    try:
        result = subprocess.run(cmd, cwd=SCRIPT_DIR, capture_output=False, text=True, env=PYTHON_ENV)
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


def run_migration(source_keys: list[str]) -> dict[str, bool]:
    """Run migrate_data.py per source. Returns {key: bool}."""
    if not os.path.isfile(MIGRATE_SCRIPT):
        logger.error(f"Migration script not found: {MIGRATE_SCRIPT}")
        return {k: False for k in source_keys}

    results: dict[str, bool] = {}
    for key in source_keys:
        cmd = [MIGRATE_PYTHON, MIGRATE_SCRIPT, "--source", key]
        logger.info(f"[MIGRATE:{key.upper()}] {' '.join(cmd)}")
        start = time.time()
        try:
            result = subprocess.run(
                cmd, cwd=SCRIPT_DIR, capture_output=False, text=True, env=PYTHON_ENV
            )
            elapsed = time.time() - start
            if result.returncode == 0:
                logger.info(f"[MIGRATE:{key.upper()}] ✓ Done in {elapsed:.0f}s")
                results[key] = True
            else:
                logger.error(
                    f"[MIGRATE:{key.upper()}] ✗ Exit {result.returncode} after {elapsed:.0f}s"
                )
                results[key] = False
        except Exception as exc:
            logger.error(f"[MIGRATE:{key.upper()}] ✗ Exception: {exc}")
            results[key] = False
    return results


def run_image_sort(source_keys: list[str]) -> dict[str, bool]:
    """Re-sort image_urls for sources with numbered suffixes."""
    from scrapers.image_sort_utils import fix_source_dir

    results: dict[str, bool] = {}
    sortable = [k for k in source_keys if k in NUMBERED_IMAGE_SOURCES]
    if not sortable:
        return results

    logger.info(f"\n{'=' * 60}")
    logger.info(f"Sorting image URLs for: {sortable}")
    logger.info(f"{'=' * 60}")

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


def run_group_parallel(
    keys: list[str], fresh: bool, label: str
) -> dict[str, bool]:
    """Run a list of sources in parallel; return {key: bool}."""
    if not keys:
        return {}

    logger.info(f"\n{'=' * 60}")
    logger.info(f"Running {label}: {keys}")
    logger.info(f"{'=' * 60}")

    results: dict[str, bool] = {}
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


# ── Main ──────────────────────────────────────────────────────────────────────


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run all property scrapers (fresh by default).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "sources",
        nargs="*",
        default=list(SOURCES.keys()),
        metavar="SOURCE",
        help=f"Sources to scrape. Options: {', '.join(SOURCES.keys())} (default: all)",
    )
    parser.add_argument(
        "--no-fresh",
        action="store_true",
        help="Skip clearing existing data (incremental update)",
    )
    parser.add_argument(
        "--all-parallel",
        action="store_true",
        help="Run all scrapers at once instead of in two groups",
    )
    parser.add_argument(
        "--no-migrate",
        action="store_true",
        help="Skip the automatic Supabase migration step",
    )
    parser.add_argument(
        "--no-geocode",
        action="store_true",
        help="Skip the geocoding step",
    )
    parser.add_argument(
        "--list", action="store_true", help="List available sources and exit"
    )
    args = parser.parse_args()

    if args.list:
        print("Available sources:")
        groups_flat = {
            k: g for g, group in enumerate(PARALLEL_GROUPS) for k in group
        }
        for k, v in SOURCES.items():
            g = groups_flat.get(k, "?")
            print(f"  {k:8s}  {v['label']:35s}  (group {g + 1})")
        return

    sources_to_run = [s for s in args.sources if s in SOURCES]
    unknown = [s for s in args.sources if s not in SOURCES]
    if unknown:
        logger.warning(f"Unknown sources (ignored): {unknown}")
    if not sources_to_run:
        logger.error("No valid sources to run.")
        sys.exit(1)

    fresh = not args.no_fresh

    logger.info(f"\n{'=' * 60}")
    logger.info(f"Full scrape starting  —  {datetime.now().isoformat()}")
    logger.info(f"Sources : {len(sources_to_run)}")
    logger.info(f"Fresh   : {fresh}")
    logger.info(f"Migrate : {not args.no_migrate}")
    logger.info(f"{'=' * 60}")

    overall_start = time.time()
    all_results: dict[str, bool] = {}

    if args.all_parallel:
        all_results = run_group_parallel(
            sources_to_run, fresh, "all sources in parallel"
        )
    else:
        for g_idx, group in enumerate(PARALLEL_GROUPS):
            group_keys = [k for k in group if k in sources_to_run]
            if not group_keys:
                continue
            results = run_group_parallel(
                group_keys, fresh, f"group {g_idx + 1}"
            )
            all_results.update(results)

    # ── Scrape summary ──────────────────────────────────────────
    total_elapsed = time.time() - overall_start
    logger.info(f"\n{'=' * 60}")
    logger.info(f"Scraping complete in {total_elapsed / 60:.1f} min")
    for key in sources_to_run:
        status = "OK" if all_results.get(key) else "FAILED"
        logger.info(f"  {key.upper():8s}  {SOURCES[key]['label']:35s}  {status}")

    scraped_ok = [k for k in sources_to_run if all_results.get(k)]

    # ── Image sort ──────────────────────────────────────────────
    sort_results = run_image_sort(scraped_ok)

    # ── Geocoding ────────────────────────────────────────────────
    geocode_ok = False
    if not args.no_geocode:
        if scraped_ok:
            logger.info(f"\n{'=' * 60}")
            logger.info(f"Geocoding properties from {len(scraped_ok)} source(s)")
            logger.info(f"{'=' * 60}")
            geocode_ok = run_geocoding()
        else:
            logger.warning(
                "No sources scraped successfully — skipping geocoding."
            )
    else:
        logger.info("\n--no-geocode: skipping geocoding step.")

    # ── Migration ───────────────────────────────────────────────
    migrate_results: dict[str, bool] = {}
    if not args.no_migrate:
        if scraped_ok:
            logger.info(f"\n{'=' * 60}")
            logger.info(
                f"Migrating {len(scraped_ok)} source(s) to Supabase: {scraped_ok}"
            )
            logger.info(f"{'=' * 60}")
            migrate_results = run_migration(scraped_ok)
        else:
            logger.warning(
                "No sources scraped successfully — skipping migration."
            )
    else:
        logger.info("\n--no-migrate: skipping Supabase migration.")

    # ── Final summary ──────────────────────────────────────────
    logger.info(f"\n{'=' * 60}")
    logger.info("Done.")

    geocode_status = "skipped" if args.no_geocode else ("✓" if geocode_ok else "✗")
    logger.info(f"Geocode: {geocode_status}")

    for key in sources_to_run:
        scrape_ok_flag = "✓" if all_results.get(key) else "✗"
        if args.no_migrate:
            migrate_ok = "-"
        elif key in migrate_results:
            migrate_ok = "✓" if migrate_results[key] else "✗"
        else:
            migrate_ok = "-"
        if key in sort_results:
            sort_ok = "✓" if sort_results[key] else "✗"
        elif key not in NUMBERED_IMAGE_SOURCES:
            sort_ok = "n/a"
        else:
            sort_ok = "-"
        logger.info(
            f"  {key.upper():8s}  {SOURCES[key]['label']:35s}  "
            f"scrape:{scrape_ok_flag}  migrate:{migrate_ok}  sort:{sort_ok}"
        )

    migrate_failed = [k for k, ok in migrate_results.items() if not ok]
    sort_failed = [k for k, ok in sort_results.items() if not ok]
    failed = [k for k in sources_to_run if not all_results.get(k)]

    if failed:
        logger.error(f"Scrape failed: {failed}")
    if not args.no_geocode and not geocode_ok:
        logger.error("Geocoding failed")
    if migrate_failed:
        logger.error(f"Migration failed: {migrate_failed}")
    if sort_failed:
        logger.error(f"Image sort failed: {sort_failed}")

    if (
        failed
        or (not args.no_geocode and not geocode_ok)
        or migrate_failed
        or sort_failed
    ):
        sys.exit(1)
    else:
        logger.info(
            "All sources scraped, geocoded, migrated, and sorted successfully."
        )


if __name__ == "__main__":
    main()
