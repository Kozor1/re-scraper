#!/usr/bin/env python3
"""
property_update.py  –  Incremental (hourly) property sync for all NI estate agent sources.

For each source this script:
  1. Walks ALL listing pages to get the complete set of currently live property URLs
  2. Scrapes + uploads any NEW properties (text + images)
  3. Deletes DELISTED properties from Supabase and the local index
  4. Re-scrapes text fields (price, status, description) for EXISTING properties
     and pushes any changes to Supabase
  5. Runs the TR selenium backfill for any TR properties still missing descriptions

Runs hourly via GitHub Actions (.github/workflows/scrape.yml); can also be
run manually from the repo root:
    python3 property_update.py

Usage:
    python3 property_update.py                     # sync all sources
    python3 property_update.py sb tr               # sync specific sources only
    python3 property_update.py --dry-run           # report changes without applying them
    python3 property_update.py --no-text-update    # skip text re-scrape for existing properties
    python3 property_update.py --no-selenium       # skip TR selenium backfill
    python3 property_update.py --max-pages 20      # cap listing pages checked per source
"""

from __future__ import annotations

import os
import re
import sys
import json
import time
import shutil
import random
import logging
import argparse
import importlib
import inspect
import traceback
import subprocess
from datetime import datetime
from typing import Any
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)
sys.path.insert(0, os.path.join(SCRIPT_DIR, "scrapers"))

from config import (
    SOURCES,
    setup_logging,
    build_property_row,
    load_geocache,
    upsert_batch,
    delete_batch,
    get_supabase,
    redirected_off_page,
    HEADERS,
)

ROOT = SCRIPT_DIR
PYTHON_ENV = {**os.environ, "PYTHONPATH": ROOT}
GEOCODE_SCRIPT = os.path.join(ROOT, "geocode.py")

logger = setup_logging("property_update")

geocache = load_geocache()


# ── HTTP helpers ──────────────────────────────────────────────────────────────


def fetch(url: str, retries: int = 3) -> requests.Response | None:
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=HEADERS, timeout=30)
            r.raise_for_status()
        except requests.exceptions.RequestException as e:
            logger.warning(f"  [attempt {attempt + 1}] {url}: {e}")
            if attempt < retries - 1:
                time.sleep((2 ** attempt) * random.uniform(1, 2.5))
            continue
        # Delisted-listing guard: some agent sites bounce removed listings to
        # the homepage or a search/landing page instead of 404ing.
        # Without this check, the probe would report the property as live
        # forever.
        if redirected_off_page(url, r):
            logger.warning(f"  {url} redirected off-page → {r.url} (delisted)")
            return None
        return r
    return None


# ── Listing-page link extraction ──────────────────────────────────────────────


def extract_links(
    soup: BeautifulSoup,
    page_url: str,
    link_pattern: str,
    link_pattern_is_regex: bool = False,
) -> set[str]:
    links: set[str] = set()
    rx = re.compile(link_pattern) if link_pattern_is_regex else None
    for a in soup.find_all("a", href=True):
        href = a["href"]
        full_for_match = urljoin(page_url, href)
        matched = (
            bool(rx.search(urlparse(full_for_match).path))
            if rx
            else link_pattern in href
        )
        if matched:
            full = (
                urljoin(page_url, href).split("?")[0].split("#")[0].rstrip("/")
            )
            if any(
                skip in full.lower()
                for skip in (
                    "/property-for-rent",
                    "/property-to-rent",
                    "/for-rent/",
                    "/to-rent/",
                    "/letting",
                    "/lettings",
                    "/rent",
                )
            ):
                continue
            links.add(full)
    return links


def get_all_live_urls(source_key: str, max_pages: int = 200) -> set[str]:
    """Walk every listing page for a source. Returns set of currently live property URLs."""
    cfg = SOURCES[source_key]
    listing_page = cfg["listing_page"]
    link_pattern = cfg["link_pattern"]
    live_urls: set[str] = set()

    for page_num in range(1, max_pages + 1):
        page_url = listing_page(page_num)
        logger.info(f"  Listing page {page_num}: {page_url}")

        r = fetch(page_url)
        if not r:
            logger.warning(f"  Failed to fetch page {page_num}, stopping.")
            break

        soup = BeautifulSoup(r.content, "html.parser")
        links = extract_links(
            soup,
            page_url,
            link_pattern,
            link_pattern_is_regex=cfg.get("link_pattern_is_regex", False),
        )

        if not links:
            logger.info(
                f"  No property links on page {page_num} — end of listings."
            )
            break

        new_links = [l for l in links if l not in live_urls]
        if not new_links:
            # Site paginates past the end by repeating the first/last page
            # (e.g. Bill McCann) — stop rather than walking to max_pages.
            logger.info(
                f"  Page {page_num} repeated earlier listings — end of listings."
            )
            break

        live_urls.update(links)
        logger.info(
            f"  Page {page_num}: {len(links)} listings (running total: {len(live_urls)})"
        )
        time.sleep(random.uniform(1.5, 2.5))

    logger.info(f"  Total live URLs for {source_key.upper()}: {len(live_urls)}")
    return live_urls


def _classify_dead_urls(
    source_key: str, urls: set[str], workers: int = 6
) -> tuple[list[str], list[str]]:
    """
    Split "delisted" URLs (dead in the listing index) into:
      salvageable — detail page still returns 200 with property markup
                    (status flip to Sale Agreed/Sold etc. — keep, re-scrape);
      gone        — detail page truly 4xx/redirects-to-home (delete the row).
    """
    if not urls:
        return [], []

    from concurrent.futures import ThreadPoolExecutor, as_completed

    salvageable: list[str] = []
    gone: list[str] = []

    def _probe(url: str) -> tuple[str, bool]:
        r = fetch(url)  # cheap GET with retry & homepage-redirect guard
        # Standing-in for "page still exists" — if we got HTML at all we can
        # text-update it; 4xx/redirect-to-home returns None.
        # Some sites (UPS) serve a soft-404: HTTP 200 with an error page.
        if r is not None and "page no longer exists" in r.text.lower():
            logger.warning(f"  {url} — soft-404 (page says it no longer exists)")
            r = None
        return url, r is not None

    url_list = sorted(urls)
    total = len(url_list)
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futs = {pool.submit(_probe, u): u for u in url_list}
        for i, f in enumerate(as_completed(futs), 1):
            url, ok = f.result()
            (salvageable if ok else gone).append(url)
            if i % 50 == 0:
                logger.info(f"  dead-url probe {i}/{total}")

    logger.info(
        f"  delisted classification: {len(salvageable)} still-live, "
        f"{len(gone)} truly-gone"
    )
    return salvageable, gone


# ── Local index helpers ───────────────────────────────────────────────────────


def load_index(source_key: str) -> dict[str, Any]:
    index_path = os.path.join(
        SOURCES[source_key]["props_dir"], "property_index.json"
    )
    if os.path.isfile(index_path):
        try:
            with open(index_path, encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict) and "properties" in data:
                return data
        except (json.JSONDecodeError, OSError) as e:
            logger.warning(f"  Could not load index for {source_key}: {e}")
    return _build_index_from_files(source_key)


def _build_index_from_files(source_key: str) -> dict[str, Any]:
    props_dir = SOURCES[source_key]["props_dir"]
    entries: list[dict[str, Any]] = []
    if os.path.isdir(props_dir):
        for entry in os.listdir(props_dir):
            if not entry.startswith("property_"):
                continue
            json_path = os.path.join(props_dir, entry, f"{entry}.json")
            if not os.path.isfile(json_path):
                continue
            try:
                with open(json_path, encoding="utf-8") as f:
                    d = json.load(f)
                if d.get("url"):
                    entries.append(
                        {
                            "id": d.get("id", entry),
                            "url": d["url"],
                            "address": d.get("address", ""),
                            "title": d.get("title", ""),
                            "scraped_at": d.get("scraped_at", ""),
                        }
                    )
            except (json.JSONDecodeError, OSError):
                pass
    return {"properties": entries, "last_updated": None}


def save_index(source_key: str, index: dict[str, Any]) -> None:
    index_path = os.path.join(
        SOURCES[source_key]["props_dir"], "property_index.json"
    )
    index["last_updated"] = datetime.now().isoformat()
    with open(index_path, "w", encoding="utf-8") as f:
        json.dump(index, f, indent=2, ensure_ascii=False)


def get_known_url_map(index: dict[str, Any]) -> dict[str, str]:
    return {
        e["url"].rstrip("/"): e["id"]
        for e in index.get("properties", [])
        if e.get("url")
    }


def fetch_db_sale_urls(source_key: str) -> set[str]:
    """All sale-listing URLs currently in Supabase for this source (paged)."""
    sb = get_supabase()
    urls: set[str] = set()
    off = 0
    while True:
        r = (
            sb.table("properties")
            .select("url,listing_type")
            .eq("source", source_key)
            .order("id")
            .range(off, off + 999)
            .execute()
        )
        for x in r.data:
            if (x.get("listing_type") or "sale") == "sale":
                urls.add((x.get("url") or "").rstrip("/"))
        if len(r.data) < 1000:
            break
        off += 1000
    urls.discard("")
    return urls


def get_next_id(source_key: str) -> int:
    props_dir = SOURCES[source_key]["props_dir"]
    if not os.path.isdir(props_dir):
        return 1
    nums = [
        int(d.replace("property_", ""))
        for d in os.listdir(props_dir)
        if d.startswith("property_") and d.replace("property_", "").isdigit()
    ]
    return max(nums, default=0) + 1


# ── New property scraping ─────────────────────────────────────────────────────


def scrape_new_properties(
    source_key: str, new_urls: set[str]
) -> list[tuple[str, dict[str, Any]]]:
    """Scrape new properties using the source's own module."""
    cfg = SOURCES[source_key]
    module_name = cfg["module"]
    props_dir = cfg["props_dir"]

    try:
        if module_name in sys.modules:
            del sys.modules[module_name]
        mod = importlib.import_module(module_name)
    except Exception as e:
        logger.error(f"  Could not import {module_name}: {e}")
        return []

    # BaseScraper-based scrapers (ups, mm, ce, gm, pinp, rb, dh, nest, bmc, …)
    # don't expose the legacy module functions.  Scrape the new URLs inline via
    # the scraper class itself — the previous subprocess fallback ran a FULL
    # scrape of the entire source to fetch a handful of new listings (and its
    # results were never upserted / indexed here, leaving phantom "New"
    # entries and duplicate property folders behind).
    if not hasattr(mod, "scrape_property_page") and not hasattr(mod, "scrape_property_details"):
        scraped = _scrape_new_via_scraper_class(source_key, sorted(new_urls))
        if scraped is not None:
            return scraped
        return _scrape_via_subprocess(source_key, new_urls)

    next_id = get_next_id(source_key)
    results: list[tuple[str, dict[str, Any]]] = []

    for idx, url in enumerate(sorted(new_urls), 1):
        prop_id = f"property_{next_id + idx - 1}"
        logger.info(f"  Scraping ({idx}/{len(new_urls)}) {prop_id}: {url}")
        try:
            # Detect if the module uses Selenium for detail pages
            if hasattr(mod, "scrape_property_page"):
                sig = inspect.signature(mod.scrape_property_page)
                if "driver" in sig.parameters:
                    if not hasattr(scrape_new_properties, "_selenium_driver"):
                        scrape_new_properties._selenium_driver = (
                            mod.make_driver()
                        )
                    data = mod.scrape_property_page(
                        url, prop_id,
                        scrape_new_properties._selenium_driver,
                    )
                else:
                    data = mod.scrape_property_page(url, prop_id)
                if data:
                    results.append((prop_id, data))
                else:
                    raise ValueError("scrape_property_page returned None")
            else:
                prop_folder = os.path.join(props_dir, prop_id)
                os.makedirs(prop_folder, exist_ok=True)
                data = mod.scrape_property_details(url)
                if data:
                    data["id"] = prop_id
                    image_count = mod.scrape_property_images(url, prop_folder)
                    data["image_count"] = image_count
                    data["scraped_at"] = datetime.now().isoformat()
                    json_path = os.path.join(prop_folder, f"{prop_id}.json")
                    with open(json_path, "w", encoding="utf-8") as f:
                        json.dump(data, f, indent=2, ensure_ascii=False)
                    results.append((prop_id, data))
        except Exception as e:
            logger.error(f"  Failed {prop_id}: {e}")

        if idx < len(new_urls):
            time.sleep(random.uniform(1.5, 3.0))

    driver = getattr(scrape_new_properties, "_selenium_driver", None)
    if driver:
        try:
            driver.quit()
        except Exception:
            pass
        del scrape_new_properties._selenium_driver

    return results


def _scrape_new_via_scraper_class(
    source_key: str, urls: list[str]
) -> list[tuple[str, dict[str, Any]]] | None:
    """
    Inline-scrape new listing URLs using the source's own BaseScraper subclass
    (loaded through scrapers.text_update's shared loader).  Each property is
    fetched, parsed, saved to properties/<site>/property_N/ and returned, so
    callers can upsert + index it exactly like the legacy inline path.

    Returns None if no usable scraper class exists (caller falls back to the
    subprocess path).
    """
    try:
        from scrapers.text_update import _load_source_scraper

        scraper = _load_source_scraper(source_key)
    except Exception as e:
        logger.warning(f"  [{source_key}] could not load scraper class: {e}")
        return None
    if scraper is None:
        return None

    results: list[tuple[str, dict[str, Any]]] = []
    for idx, url in enumerate(urls, 1):
        prop_id = f"property_{scraper.get_next_property_id()}"
        logger.info(f"  Scraping ({idx}/{len(urls)}) {prop_id}: {url}")
        try:
            data = scraper._scrape_and_save_one(url, prop_id)
            if data:
                results.append((prop_id, data))
        except Exception as e:
            logger.error(f"  Failed {prop_id}: {e}")
        if idx < len(urls):
            scraper.delay()
    return results


def _scrape_via_subprocess(
    source_key: str, new_urls: set[str]
) -> list[tuple[str, dict[str, Any]]]:
    """Run the source's own full scraper as a subprocess for new properties.

    Smart scrapers (mm, ce, gm, pinp, rb, dh) handle their own incremental scraping.
    Running them via subprocess ensures they use their existing smart-update logic
    including url_map.json management.
    """
    cfg = SOURCES[source_key]
    module_name = cfg["module"]
    script_path = os.path.join(ROOT, "scrapers", f"{module_name}.py")

    if not os.path.isfile(script_path):
        logger.warning(
            f"  Cannot find scraper script for {source_key}: {script_path}"
        )
        return []

    logger.info(
        f"  [{source_key}] Running smart scraper as subprocess: {module_name}.py"
    )
    try:
        result = subprocess.run(
            [sys.executable, script_path],
            cwd=ROOT,
            capture_output=True,
            text=True,
            timeout=1800,
            env=PYTHON_ENV,
        )
        if result.returncode != 0:
            logger.error(
                f"  [{source_key}] Subprocess failed (rc={result.returncode})"
            )
            if result.stderr:
                logger.error(f"  stderr: {result.stderr[:500]}")
        else:
            logger.info(f"  [{source_key}] Subprocess completed successfully")
    except subprocess.TimeoutExpired:
        logger.error(f"  [{source_key}] Subprocess timed out after 30 min")
    except Exception as e:
        logger.error(f"  [{source_key}] Subprocess error: {e}")

    return []


def _supports_inline_scraping(source_key: str) -> bool:
    """Check if new listings for this source can be scraped inline: either via
    the legacy module-level functions, or via the source's BaseScraper class
    (handled by _scrape_new_via_scraper_class)."""
    cfg = SOURCES[source_key]
    module_name = cfg["module"]
    try:
        if module_name in sys.modules:
            del sys.modules[module_name]
        mod = importlib.import_module(module_name)
        if hasattr(mod, "scrape_property_page") or hasattr(
            mod, "scrape_property_details"
        ):
            return True
    except Exception:
        pass
    try:
        from scrapers.text_update import _load_source_scraper

        return _load_source_scraper(source_key) is not None
    except Exception:
        return False


# ── Text update for existing properties ──────────────────────────────────────


def text_update_source(
    source_key: str,
    known_url_map: dict[str, str],
    dry_run: bool = False,
    args_ref: argparse.Namespace | None = None,
) -> list[tuple[str, dict[str, Any]]]:
    """Re-scrape text fields for existing properties. Returns changed entries."""
    from scrapers.text_update import (
        scrape_property_text,
        detect_changes,
        normalise_price_key,
        TRACKED_FIELDS,
    )

    props_dir = SOURCES[source_key]["props_dir"]
    updated: list[tuple[str, dict[str, Any]]] = []
    items = list(known_url_map.items())
    total = len(items)

    def _update_one(task: tuple[str, str]) -> tuple[str, dict[str, Any]] | None:
        url, source_id = task
        json_path = os.path.join(props_dir, source_id, f"{source_id}.json")
        if not os.path.isfile(json_path):
            return None

        try:
            with open(json_path, encoding="utf-8") as f:
                existing = json.load(f)
        except (json.JSONDecodeError, OSError):
            return None

        new_data = scrape_property_text(url, source_key)
        if not new_data:
            return None

        new_data = normalise_price_key(existing, new_data)
        changes = detect_changes(existing, new_data)
        if changes:
            logger.info(
                f"    Changes in {source_id}: {[c['field'] for c in changes]}"
            )

        merged = dict(existing)
        for k, v in new_data.items():
            if k in ("url", "id", "scraped_at"):
                continue
            if k in TRACKED_FIELDS and k in existing and existing[k] != v and v:
                merged[f"_prev_{k}"] = existing[k]
            merged[k] = v
        merged["rescraped_at"] = datetime.now().isoformat()

        if not dry_run:
            with open(json_path, "w", encoding="utf-8") as f:
                json.dump(merged, f, indent=2, ensure_ascii=False)

        # Only queue genuinely changed properties for the Supabase upsert —
        # re-pushing identical rows hourly wastes egress and churns
        # updated_at timestamps in the app.
        if changes:
            return (source_id, merged)
        return None

    from concurrent.futures import ThreadPoolExecutor, as_completed

    workers = getattr(args_ref, "workers", 4) if args_ref else 4
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_update_one, item): item for item in items}
        done = 0
        for future in as_completed(futures):
            done += 1
            if done % 25 == 0:
                logger.info(f"  Text update [{done}/{total}]")
            result = future.result()
            if result:
                updated.append(result)
            time.sleep(random.uniform(0.4, 1.0))  # polite global pacing

    logger.info(
        f"  Text update done: {len(updated)} of {total} properties changed"
    )
    return updated


# ── TR selenium backfill ──────────────────────────────────────────────────────


def run_tr_selenium_backfill(dry_run: bool = False) -> bool:
    if dry_run:
        logger.info("  [dry-run] Would run TR selenium backfill")
        return True

    script = os.path.join(ROOT, "scrapers", "tr_selenium_scrape.py")
    if not os.path.isfile(script):
        logger.error(f"TR selenium scraper not found: {script}")
        return False

    logger.info("Launching TR selenium backfill…")
    try:
        result = subprocess.run(
            [sys.executable, script], cwd=ROOT, timeout=3600, env=PYTHON_ENV
        )
        if result.returncode == 0:
            logger.info("TR selenium backfill complete.")
            return True
        else:
            logger.error(
                f"TR selenium backfill exited with code {result.returncode}"
            )
            return False
    except subprocess.TimeoutExpired:
        logger.error("TR selenium backfill timed out after 1 hour.")
        return False
    except Exception as e:
        logger.error(f"TR selenium backfill error: {e}")
        return False


# ── Geocoding ─────────────────────────────────────────────────────────────────


def run_geocoding() -> bool:
    if not os.path.isfile(GEOCODE_SCRIPT):
        logger.error(f"Geocode script not found: {GEOCODE_SCRIPT}")
        return False
    cmd = [sys.executable, GEOCODE_SCRIPT]
    logger.info(f"[GEOCODE] {' '.join(cmd)}")
    start = time.time()
    try:
        result = subprocess.run(
            cmd, cwd=ROOT, capture_output=False, text=True, env=PYTHON_ENV
        )
        elapsed = time.time() - start
        if result.returncode == 0:
            logger.info(f"[GEOCODE] ✓ Done in {elapsed:.0f}s")
            return True
        else:
            logger.error(
                f"[GEOCODE] ✗ Exit {result.returncode} after {elapsed:.0f}s"
            )
            return False
    except Exception as exc:
        logger.error(f"[GEOCODE] ✗ Exception: {exc}")
        return False


# ── Per-source sync ───────────────────────────────────────────────────────────


def sync_source(
    source_key: str, args: argparse.Namespace
) -> dict[str, int]:
    cfg = SOURCES[source_key]
    logger.info(f"{'=' * 60}")
    logger.info(f"Source: {cfg['label']} ({source_key.upper()})")

    logger.info("Step 1: Walking listing pages for live URLs…")
    live_urls = get_all_live_urls(source_key, max_pages=args.max_pages)
    live_urls_norm = {u.rstrip("/") for u in live_urls}

    logger.info("Step 2: Comparing with local index…")
    index = load_index(source_key)
    known_url_map = get_known_url_map(index)

    new_urls = live_urls_norm - set(known_url_map.keys())
    dead_urls = set(known_url_map.keys()) - live_urls_norm
    extant_urls = live_urls_norm & set(known_url_map.keys())

    logger.info(
        f"  Live: {len(live_urls_norm)}  Known: {len(known_url_map)}"
    )
    logger.info(
        f"  → New: {len(new_urls)}  Delisted: {len(dead_urls)}  Existing: {len(extant_urls)}"
    )

    stats: dict[str, int] = {
        "live": len(live_urls_norm),
        "new": len(new_urls),
        "delisted": len(dead_urls),
        "updated": 0,
        "errors": 0,
    }
    salvageable: list[str] = []  # populated in Step 4 when a "dead" URL is actually live

    # ── 3. Scrape and upload new properties ─────────────────────────
    if new_urls:
        logger.info(f"Step 3: Scraping {len(new_urls)} new properties…")
        if not _supports_inline_scraping(source_key):
            if not args.dry_run:
                logger.info(
                    f"  [{source_key}] Smart scraper — delegating to subprocess"
                )
                scraped = _scrape_via_subprocess(source_key, new_urls)
            else:
                logger.info(
                    f"  [dry-run] Would run smart scraper for {len(new_urls)} new properties"
                )
        elif not args.dry_run:
            scraped = scrape_new_properties(source_key, new_urls)
            logger.info(
                f"  Scraped {len(scraped)} of {len(new_urls)} new properties"
            )
            rows = []
            for source_id, data in scraped:
                try:
                    rows.append(build_property_row(source_key, source_id, data, geocache))
                except Exception as e:
                    logger.error(f"  build_row error for {source_id}: {e}")
                    stats["errors"] += 1
            upsert_batch(rows)
            for source_id, data in scraped:
                index["properties"].append(
                    {
                        "id": source_id,
                        "url": data.get("url", ""),
                        "address": data.get("address", ""),
                        "title": data.get("title", ""),
                        "scraped_at": data.get("scraped_at", ""),
                    }
                )
            save_index(source_key, index)
        else:
            logger.info(
                f"  [dry-run] Would scrape and upload {len(new_urls)} new properties:"
            )
            for url in sorted(new_urls):
                logger.info(f"    + {url}")

    # ── 4. Remove delisted properties ───────────────────────────────
    if dead_urls:
        logger.info(
            f"Step 4: Handling {len(dead_urls)} delisted properties…"
        )

        # Some agents (e.g. Lennon Estates) *remove* sold/agreed listings from
        # the index pages without deleting the detail page. Don't nuke those —
        # probe the URL once and only drop it if the detail page is really
        # gone. The still-live ones are fed through the normal Step-5 refresh
        # so their status actually changes to Sale Agreed / Sold.
        salvageable, gone = [], []
        if not args.dry_run and dead_urls:
            salvageable, gone = _classify_dead_urls(source_key, dead_urls)
            if salvageable:
                logger.info(
                    f"  {len(salvageable)} still has live detail pages — "
                    "refreshing status instead of deleting"
                )

        if args.dry_run:
            for url in sorted(dead_urls):
                source_id = known_url_map.get(url, "?")
                logger.info(f"  [dry-run] Would delete {source_id}: {url}")
        else:
            if gone:
                delete_batch(source_key, gone)
                index["properties"] = [
                    e
                    for e in index["properties"]
                    if e.get("url", "").rstrip("/") not in gone
                ]
                save_index(source_key, index)
                delisted_dir = os.path.join(cfg["props_dir"], "delisted")
                os.makedirs(delisted_dir, exist_ok=True)
                for url in gone:
                    source_id = known_url_map.get(url)
                    if source_id:
                        src = os.path.join(cfg["props_dir"], source_id)
                        dst = os.path.join(delisted_dir, source_id)
                        if os.path.isdir(src) and not os.path.exists(dst):
                            shutil.move(src, dst)
                            logger.info(f"  Archived {source_id} → delisted/")

    # ── 4b. Orphan reconciliation ─────────────────────────────────────
    # DB rows that are in *neither* the live walk nor the local index are
    # invisible to the comparisons above and would linger forever if the
    # index ever loses track of an entry (observed in production: rows stuck
    # since delisting because the cached index no longer named them).
    # Probe each orphan once and delete only the truly-gone ones — the probe
    # requirement makes this safe even if the listing walk mis-fires.
    # Sale rows only: rentals aren't part of the walk.
    try:
        db_urls = fetch_db_sale_urls(source_key)
    except Exception as e:
        logger.warning(f"  Step 4b skipped — could not read DB URLs: {e}")
        db_urls = set()
    if db_urls:
        orphan_urls = db_urls - live_urls_norm - set(known_url_map.keys())
        if orphan_urls:
            if args.dry_run:
                logger.info(
                    f"  [dry-run] Step 4b: {len(orphan_urls)} orphan DB rows "
                    f"(in neither walk nor index) would be probed"
                )
            else:
                logger.info(
                    f"  Step 4b: {len(orphan_urls)} orphaned DB rows "
                    f"(in neither walk nor index) — probing…"
                )
                salvage_orphans, gone_orphans = _classify_dead_urls(
                    source_key, orphan_urls
                )
                if gone_orphans:
                    # delete_batch expects a set of (normalised) URLs
                    delete_batch(source_key, set(gone_orphans))
                    logger.info(
                        f"  deleted {len(gone_orphans)} orphaned rows from Supabase"
                    )
                for u in salvage_orphans:
                    logger.warning(
                        f"  orphan still live on site — left as-is until the "
                        f"next full scrape re-tracks it: {u}"
                    )

    # ── 5. Text update for existing + still-live "delisted" listings ──────
    refresh_urls = set(extant_urls) | set(salvageable)
    if not args.no_text_update and refresh_urls:
        logger.info(
            f"Step 5: Text re-scrape for {len(refresh_urls)} existing properties…"
        )
        extant_map = {
            u: known_url_map[u] for u in refresh_urls if u in known_url_map
        }
        if not args.dry_run:
            updated = text_update_source(
                source_key, extant_map, dry_run=False, args_ref=args
            )
            stats["updated"] = len(updated)
            rows = []
            for source_id, data in updated:
                try:
                    rows.append(build_property_row(source_key, source_id, data, geocache))
                except Exception as e:
                    logger.error(f"  build_row error for {source_id}: {e}")
            if rows:
                upsert_batch(rows)
                logger.info(f"  Pushed {len(rows)} text updates to Supabase")
        else:
            logger.info(
                f"  [dry-run] Would text-update {len(refresh_urls)} existing properties"
            )

    return stats


# ── Main ──────────────────────────────────────────────────────────────────────


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Daily property sync — finds new listings, removes delisted, updates prices."
    )
    parser.add_argument(
        "sources",
        nargs="*",
        default=list(SOURCES.keys()),
        help="Sources to sync (default: all)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report changes without scraping, uploading, or deleting anything",
    )
    parser.add_argument(
        "--no-text-update",
        action="store_true",
        help="Skip text re-scrape for existing properties",
    )
    parser.add_argument(
        "--no-selenium",
        action="store_true",
        help="Skip TR selenium backfill",
    )
    parser.add_argument(
        "--no-geocode",
        action="store_true",
        help="Skip the geocoding step",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=200,
        help="Max listing pages to walk per source (default: 200)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=4,
        help="Concurrent fetches per source during the text re-scrape (default: 4)",
    )
    parser.add_argument(
        "--source-workers",
        type=int,
        default=4,
        help="How many sources to sync in parallel (default: 4; use 1 to run serially)",
    )
    args = parser.parse_args()

    sources_to_run = [s for s in args.sources if s in SOURCES]
    unknown = [s for s in args.sources if s not in SOURCES]
    if unknown:
        logger.warning(f"Unknown sources (ignored): {unknown}")
    if not sources_to_run:
        logger.error("No valid sources to sync.")
        sys.exit(1)

    mode = "DRY RUN" if args.dry_run else "LIVE"
    logger.info(
        f"property_update starting [{mode}]. Sources: {sources_to_run}"
    )
    overall_start = time.time()

    all_stats: dict[str, dict[str, int]] = {}
    if args.source_workers <= 1 or len(sources_to_run) <= 1:
        for source_key in sources_to_run:
            try:
                stats = sync_source(source_key, args)
                all_stats[source_key] = stats
            except Exception as e:
                logger.error(f"Error syncing {source_key}: {e}")
                logger.error(traceback.format_exc())
                all_stats[source_key] = {"error": str(e)}
    else:
        # Sync several sources in parallel — each source is independent
        # (own files, own index, own Supabase batches), so this is safe and
        # takes the hourly run from hours to ~15 min.
        from concurrent.futures import ThreadPoolExecutor, as_completed

        with ThreadPoolExecutor(max_workers=args.source_workers) as pool:
            future_map = {
                pool.submit(sync_source, source_key, args): source_key
                for source_key in sources_to_run
            }
            for future in as_completed(future_map):
                source_key = future_map[future]
                try:
                    all_stats[source_key] = future.result()
                except Exception as e:
                    logger.error(f"Error syncing {source_key}: {e}")
                    logger.error(traceback.format_exc())
                    all_stats[source_key] = {"error": str(e)}

    # ── TR selenium backfill ────────────────────────────────────────
    if "tr" in sources_to_run and not args.no_selenium:
        logger.info(f"{'=' * 60}")
        logger.info("TR selenium backfill (fills missing descriptions)…")
        run_tr_selenium_backfill(dry_run=args.dry_run)

    # ── Geocoding ────────────────────────────────────────────────────
    geocode_ok = False
    if not args.no_geocode:
        if not args.dry_run:
            logger.info(f"{'=' * 60}")
            logger.info("Geocoding new addresses…")
            geocode_ok = run_geocoding()
        else:
            logger.info(f"{'=' * 60}")
            logger.info("[dry-run] Would run geocoding")
    else:
        logger.info("\n--no-geocode: skipping geocoding step.")

    # ── Coordinate backfill ──────────────────────────────────────────
    # Rows only receive lat/lng when they're upserted *after* the geocoder
    # cached them; rows that never changed text again stayed null forever.
    # After geocoding, sweep any remaining null-coord sale rows whose address
    # now has a cache hit and patch them in place.
    if not args.no_geocode and not args.dry_run:
        fresh_cache = load_geocache()
        try:
            sb = get_supabase()
            off = 0
            patched = 0
            while True:
                r = (
                    sb.table("properties")
                    .select("id,address")
                    .is_("lat", "null")
                    .or_("listing_type.eq.sale,listing_type.is.null")
                    .order("id")
                    .range(off, off + 999)
                    .execute()
                )
                if not r.data:
                    break
                patch = [
                    (x["id"], fresh_cache[a])
                    for x in r.data
                    for a in [x["address"].strip().rstrip(",")]
                    if a in fresh_cache
                ]
                for pid, geo in patch:
                    sb.table("properties").update(
                        {"lat": geo["lat"], "lng": geo["lng"]}
                    ).eq("id", pid).execute()
                patched += len(patch)
                if len(r.data) < 1000:
                    break
                off += 1000
            if patched:
                logger.info(f"Coordinate backfill: patched {patched} rows")
        except Exception as e:
            logger.warning(f"Coordinate backfill failed (non-fatal): {e}")

    # ── Summary ─────────────────────────────────────────────────────
    elapsed = time.time() - overall_start
    logger.info(f"{'=' * 60}")
    logger.info(f"property_update complete in {elapsed / 60:.1f} min")

    geocode_status = (
        "skipped"
        if args.no_geocode
        else ("dry-run" if args.dry_run else ("✓" if geocode_ok else "✗"))
    )
    logger.info(f"Geocode: {geocode_status}")

    header = f"{'Source':<6}  {'Label':<30}  {'New':>4}  {'Deleted':>7}  {'Updated':>7}"
    logger.info(header)
    logger.info("-" * 60)
    for source_key, stats in all_stats.items():
        if "error" in stats:
            logger.info(
                f"{source_key.upper():<6}  {SOURCES[source_key]['label']:<30}  ERROR: {stats['error']}"
            )
        else:
            logger.info(
                f"{source_key.upper():<6}  {SOURCES[source_key]['label']:<30}  "
                f"{stats.get('new', 0):>4}  "
                f"{stats.get('delisted', 0):>7}  "
                f"{stats.get('updated', 0):>7}"
            )


if __name__ == "__main__":
    main()
