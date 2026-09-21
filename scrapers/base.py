"""
scrapers/base.py  –  Base classes for all property scrapers.

Provides:
  BaseScraper      – requests-only scraper (listing + detail via plain HTTP)
  SeleniumScraper  – extends BaseScraper; uses headless Chrome for detail pages
  SmartUpdateMixin – mixin for scrapers that support --quick incremental mode

To create a new scraper, subclass one of these and implement the abstract methods.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import sys
import time
import random
import logging
import argparse
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

# Allow imports from the config package
SCRAPERS_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(SCRAPERS_DIR)
sys.path.insert(0, ROOT)

from config.sources import SOURCES, CMSFamily, ScrapeStrategy, IndexType, NUMBERED_IMAGE_SOURCES, HEADERS
from config.logging_config import setup_logging
from config.supabase_utils import (
    build_property_row,
    load_geocache,
    normalise_status,
    upsert_batch,
)

# ── Image URL sort/dedup ──────────────────────────────────────────────────────

_IMAGE_SORT_RE = re.compile(r'[_-](\d+)\.(jpg|jpeg|png|webp)$', re.IGNORECASE)


def sort_and_dedup_image_urls(urls: list[str]) -> list[str]:
    """Sort image URLs by trailing numeric suffix and deduplicate."""
    def _img_num(url: str) -> int:
        m = _IMAGE_SORT_RE.search(url)
        return int(m.group(1)) if m else 999999

    seen: set[str] = set()
    deduped: list[str] = []
    for url in urls:
        if url not in seen:
            seen.add(url)
            deduped.append(url)
    deduped.sort(key=_img_num)
    return deduped


def is_image_href(href: str) -> bool:
    """Return True if href points to an image file."""
    return bool(re.search(r"\.(jpg|jpeg|png|webp|gif)(\?.*)?$", href, re.IGNORECASE))


# ═══════════════════════════════════════════════════════════════════════════════
# Base Scraper (requests-only)
# ═══════════════════════════════════════════════════════════════════════════════


class BaseScraper(ABC):
    """Requests-based scraper for a single estate agent source.

    Subclasses must implement:
      - get_listing_url(page_num: int) -> str
      - extract_property_links(soup: BeautifulSoup, page_url: str) -> list[str]
      - scrape_detail_page(html: str, url: str) -> dict | None
      - extract_image_urls(soup: BeautifulSoup, page_url: str) -> list[str]

    Optional overrides:
      - cleanup_data(data: dict) -> dict   (normalize fields before saving)
      - get_next_property_id() -> int
    """

    source_key: str
    config: dict[str, Any]

    # ── Configuration defaults (override in subclass __init__) ────────────────
    max_pages: int = 1000
    max_properties: int = 100_000
    request_delay_min: float = 1.0
    request_delay_max: float = 3.0
    max_retries: int = 3
    test_mode: bool = False

    def __init__(self, source_key: str) -> None:
        self.source_key = source_key
        self.config = SOURCES[source_key]
        self.props_dir = self.config["props_dir"]
        self.output_dir = self.props_dir  # backward compat
        self.logger = setup_logging(f"scraper_{source_key}")

    # ── Abstract methods ──────────────────────────────────────────────────────

    @abstractmethod
    def get_listing_url(self, page_num: int) -> str: ...

    @abstractmethod
    def extract_property_links(self, soup: BeautifulSoup, page_url: str) -> list[str]: ...

    @abstractmethod
    def scrape_detail_page(self, html: str, url: str) -> dict[str, Any] | None: ...

    @abstractmethod
    def extract_image_urls(self, soup: BeautifulSoup, page_url: str) -> list[str]: ...

    # ── Optional overrides ────────────────────────────────────────────────────

    def cleanup_data(self, data: dict[str, Any]) -> dict[str, Any]:
        """Normalize field names and values before saving. Override in subclass."""
        return data

    def get_next_property_id(self) -> int:
        """Find the next available property_N integer."""
        if not os.path.isdir(self.props_dir):
            return 1
        nums = [
            int(d.replace("property_", ""))
            for d in os.listdir(self.props_dir)
            if d.startswith("property_") and d.replace("property_", "").isdigit()
        ]
        return max(nums, default=0) + 1

    # ── HTTP helpers ──────────────────────────────────────────────────────────

    def fetch(self, url: str, retries: int | None = None) -> requests.Response | None:
        """GET with retry and exponential back-off.

        Retries are skipped for 4xx client errors (e.g. 404) — those are
        deterministic "this page doesn't exist" responses, not transient
        failures. Re-trying a 404 wastes time and produces misleading logs
        when an agent legitimately has fewer pages than max_pages.
        """
        retries = retries or self.max_retries
        for attempt in range(retries):
            try:
                r = requests.get(url, headers=HEADERS, timeout=30)
                r.raise_for_status()
                return r
            except requests.exceptions.HTTPError as e:
                # Retry on 5xx (server error); fail fast on 4xx (client error).
                status = e.response.status_code if e.response is not None else None
                if status is not None and 400 <= status < 500:
                    self.logger.warning(f"[HTTP {status}] {url} — not retrying")
                    return None
                self.logger.warning(f"[attempt {attempt + 1}] {url}: {e}")
                if attempt < retries - 1:
                    time.sleep((2 ** attempt) * random.uniform(1, 2.5))
                else:
                    self.logger.error(
                        f"Failed to fetch {url} after {retries} attempts"
                    )
                    return None
            except requests.exceptions.RequestException as e:
                # DNS / connection / timeout errors etc.
                self.logger.warning(
                    f"[attempt {attempt + 1}] {url}: {e}"
                )
                if attempt < retries - 1:
                    time.sleep((2 ** attempt) * random.uniform(1, 2.5))
                else:
                    self.logger.error(
                        f"Failed to fetch {url} after {retries} attempts"
                    )
                    return None
        return None

    def delay(self) -> None:
        """Sleep for a random interval between requests."""
        time.sleep(random.uniform(self.request_delay_min, self.request_delay_max))

    # ── Listing page iteration ────────────────────────────────────────────────

    def collect_property_links(self, max_pages: int | None = None) -> list[str]:
        """Walk listing pages collecting all property detail URLs."""
        pages = max_pages or (1 if self.test_mode else self.max_pages)
        all_links: list[str] = []
        seen: set[str] = set()

        for page_num in range(1, pages + 1):
            page_url = self.get_listing_url(page_num)
            self.logger.info(f"Fetching listing page {page_num}: {page_url}")

            r = self.fetch(page_url)
            if not r:
                self.logger.warning(
                    f"Failed to fetch page {page_num}, stopping pagination"
                )
                break

            soup = BeautifulSoup(r.content, "html.parser")
            links = self.extract_property_links(soup, page_url)
            new_links = [l for l in links if l not in seen]

            self.logger.info(
                f"Page {page_num}: {len(links)} links ({len(new_links)} new)"
            )

            if not new_links:
                self.logger.info(
                    f"No new links on page {page_num}, stopping pagination"
                )
                break

            all_links.extend(new_links)
            seen.update(new_links)

            if len(all_links) >= self.max_properties:
                all_links = all_links[:self.max_properties]
                self.logger.info(
                    f"Reached max property limit ({self.max_properties})"
                )
                break

            self.delay()

        self.logger.info(f"Total property links collected: {len(all_links)}")
        return all_links

    # ── Property scraping loop ────────────────────────────────────────────────

    def scrape_properties(
        self, property_links: list[str], limit: int = 0
    ) -> list[dict[str, Any]]:
        """Iterate over property URLs, scrape each one, and save to disk."""
        if limit > 0:
            property_links = property_links[:limit]

        next_id = self.get_next_property_id()
        results: list[dict[str, Any]] = []

        for idx, url in enumerate(property_links, 1):
            prop_id = f"property_{next_id}"
            self.logger.info(
                f"[{idx}/{len(property_links)}] Scraping {prop_id}: {url}"
            )

            try:
                data = self._scrape_and_save_one(url, prop_id)
                if data:
                    results.append(data)
                    next_id += 1
            except Exception as e:
                self.logger.error(f"Exception scraping {prop_id}: {e}", exc_info=True)

            if idx % 10 == 0:
                self.logger.info(
                    f"Progress: {len(results)}/{len(property_links)} scraped"
                )

            if idx < len(property_links):
                self.delay()

        return results

    def _scrape_and_save_one(self, url: str, prop_id: str) -> dict[str, Any] | None:
        """Fetch, parse, and save one property. Returns the data dict or None."""
        r = self.fetch(url)
        if not r:
            return None

        data = self.scrape_detail_page(r.text, url)
        if not data:
            return None

        soup = BeautifulSoup(r.content, "html.parser")
        return self._finalize_and_save(data, soup, url, prop_id)

    def _finalize_and_save(
        self, data: dict[str, Any], soup: BeautifulSoup, url: str, prop_id: str
    ) -> dict[str, Any]:
        """Annotate, sort images, cleanup, and persist one property dict."""
        data["id"] = prop_id
        data["url"] = url
        data["scraped_at"] = datetime.now().isoformat()

        image_urls = self.extract_image_urls(soup, url)
        data["image_urls"] = image_urls

        if self.source_key in NUMBERED_IMAGE_SOURCES:
            data["image_urls"] = sort_and_dedup_image_urls(image_urls)

        data = self.cleanup_data(data)

        prop_folder = os.path.join(self.props_dir, prop_id)
        os.makedirs(prop_folder, exist_ok=True)
        json_path = os.path.join(prop_folder, f"{prop_id}.json")
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

        self.logger.debug(f"Saved {json_path}")
        return data

    # ── Property index management ─────────────────────────────────────────────

    def load_index(self) -> dict[str, Any]:
        """Load property_index.json if it exists."""
        index_path = os.path.join(self.props_dir, "property_index.json")
        if os.path.exists(index_path):
            try:
                with open(index_path, encoding="utf-8") as f:
                    return json.load(f)
            except (json.JSONDecodeError, OSError) as e:
                self.logger.warning(f"Could not load index: {e}")
        return {"properties": [], "last_updated": None}

    def save_index(self, index: dict[str, Any]) -> None:
        """Save property_index.json."""
        index_path = os.path.join(self.props_dir, "property_index.json")
        index["last_updated"] = datetime.now().isoformat()
        with open(index_path, "w", encoding="utf-8") as f:
            json.dump(index, f, indent=2, ensure_ascii=False)

    def update_index(self, scraped_properties: list[dict[str, Any]]) -> None:
        """Merge newly scraped properties into the local index."""
        index = self.load_index()
        existing_by_url = {
            p["url"]: p for p in index.get("properties", []) if p.get("url")
        }
        for data in scraped_properties:
            entry = {
                "id": data["id"],
                "url": data["url"],
                "address": data.get("address", ""),
                "title": data.get("title", ""),
                "scraped_at": data.get("scraped_at", ""),
            }
            existing_by_url[entry["url"]] = entry
        index["properties"] = list(existing_by_url.values())
        self.save_index(index)

    # ── Fresh scrape setup ────────────────────────────────────────────────────

    def clear_output_dir(self) -> None:
        """Remove all existing scraped data for a fresh start."""
        if os.path.exists(self.props_dir):
            self.logger.info(f"Clearing {self.props_dir} for fresh scrape...")
            shutil.rmtree(self.props_dir)
        os.makedirs(self.props_dir, exist_ok=True)
        os.makedirs(os.path.join(ROOT, "logs"), exist_ok=True)

    # ── Summary ───────────────────────────────────────────────────────────────

    def save_summary(self, total: int) -> None:
        """Write a summary.json for this source."""
        summary_path = os.path.join(self.props_dir, "summary.json")
        summary = {
            "total_properties": total,
            "scraped_at": datetime.now().isoformat(),
            "test_mode": self.test_mode,
        }
        with open(summary_path, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2, ensure_ascii=False)
        self.logger.info(f"Summary saved to {summary_path}")

    # ── Main entry point ──────────────────────────────────────────────────────

    def run(self, args: argparse.Namespace | None = None) -> int:
        """Standard main(): clear dir, collect links, scrape, update index, save summary."""
        fresh = args.fresh if args else True
        limit = args.limit if args else 0

        self.logger.info(f"Starting {self.config['label']} scraper")
        self.logger.info(f"Source key: {self.source_key}")

        if fresh:
            self.clear_output_dir()

        property_links = self.collect_property_links()
        if not property_links:
            self.logger.warning("No property links found. Site structure may have changed.")
            return 0

        scraped = self.scrape_properties(property_links, limit=limit)
        self.update_index(scraped)
        self.save_summary(len(scraped))

        self.logger.info(
            f"Scraping complete. Total: {len(scraped)} properties saved to {self.props_dir}"
        )
        return len(scraped)

    @classmethod
    def cli_main(cls, source_key: str) -> None:
        """Standard CLI entry point: parse args, create scraper, run."""
        parser = argparse.ArgumentParser(
            description=f"{SOURCES[source_key]['label']} scraper"
        )
        parser.add_argument(
            "--limit", type=int, default=0,
            help="Max properties to scrape (0 = unlimited)"
        )
        parser.add_argument(
            "--fresh", action="store_true",
            help="Clear existing data before scraping"
        )
        parser.add_argument(
            "--test", action="store_true",
            help="Test mode: scrape 1 property"
        )
        args = parser.parse_args()

        scraper = cls(source_key)
        if args.test:
            scraper.test_mode = True
        scraper.run(args)


# ═══════════════════════════════════════════════════════════════════════════════
# Selenium Scraper (headless Chrome for detail pages)
# ═══════════════════════════════════════════════════════════════════════════════


class SeleniumScraper(BaseScraper):
    """Base class for scrapers that need headless Chrome for detail pages.

    Listing pages are still fetched with plain requests, but detail pages
    are loaded via Selenium so JS-rendered content is available.
    """

    restart_every: int = 100  # Restart Chrome every N properties
    selenium_wait: int = 3     # Seconds to wait for JS to render
    use_gallery_collector: bool = False  # Subclasses set True for click-through gallery

    def __init__(self, source_key: str) -> None:
        super().__init__(source_key)
        self._driver: Any = None
        self._property_count: int = 0

    def make_driver(self) -> Any:
        """Create a headless Chrome WebDriver instance."""
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.chrome.service import Service
        from webdriver_manager.chrome import ChromeDriverManager

        opts = Options()
        opts.add_argument("--headless")
        opts.add_argument("--no-sandbox")
        opts.add_argument("--disable-dev-shm-usage")
        opts.add_argument("--disable-gpu")
        opts.add_argument("--window-size=1280,1024")
        opts.add_argument(f'--user-agent={HEADERS["User-Agent"]}')

        driver = webdriver.Chrome(
            service=Service(ChromeDriverManager().install()), options=opts
        )
        return driver

    def get_driver(self) -> Any:
        """Return the current driver, creating one if needed."""
        if self._driver is None:
            self.logger.info("Starting Selenium ChromeDriver...")
            self._driver = self.make_driver()
        return self._driver

    def restart_driver(self) -> Any:
        """Quit and restart the Chrome driver."""
        if self._driver:
            try:
                self._driver.quit()
            except Exception:
                pass
        self._driver = None
        self._property_count = 0
        time.sleep(2)
        return self.get_driver()

    def fetch_detail_selenium(self, url: str) -> str | None:
        """Load a detail page URL via Selenium and return its page source HTML."""
        self._property_count += 1
        if self._property_count > self.restart_every:
            self.logger.info("Restarting Chrome to free memory...")
            self.restart_driver()

        try:
            driver = self.get_driver()
            driver.get(url)
            time.sleep(self.selenium_wait)
            return driver.page_source
        except Exception as e:
            self.logger.error(f"Selenium error for {url}: {e}")
            return None

    def _collect_gallery_selenium(self) -> list[str]:
        """Use Selenium to click through gallery for all image URLs.

        Works with PropertyPal CDN galleries (slick/swiper carousels).
        Set ``use_gallery_collector = True`` on subclass to enable in
        ``_scrape_and_save_one``.
        """
        from selenium.webdriver.common.by import By

        drv = self.get_driver()

        COLLECT_JS = r"""
        var seen = new Set();
        document.querySelectorAll('img[src*="media.propertypal.com/sd/"]').forEach(function(img) {
            var best = img.src, bestW = 0;
            if (img.srcset) {
                img.srcset.split(',').forEach(function(p) {
                    var parts = p.trim().split(/\s+/);
                    if (parts.length >= 2) {
                        var w = parseInt(parts[1]);
                        if (w > bestW) { bestW = w; best = parts[0]; }
                    }
                });
            }
            seen.add(best);
        });
        return Array.from(seen);
        """

        seen: set[str] = set()
        urls: list[str] = []

        try:
            for u in (drv.execute_script(COLLECT_JS) or []):
                if u and u not in seen:
                    seen.add(u)
                    urls.append(u)
        except Exception:
            return urls

        next_selectors = [
            '[class*="next" i]',
            '[class*="Next"]',
            '[aria-label*="next" i]',
            '[aria-label*="Next"]',
            ".slick-next",
            ".swiper-button-next",
        ]

        next_btn = None
        for sel in next_selectors:
            try:
                candidates = drv.find_elements(By.CSS_SELECTOR, sel)
                for c in candidates:
                    if c.is_displayed():
                        next_btn = c
                        break
                if next_btn:
                    break
            except Exception:
                continue

        if next_btn:
            no_new = 0
            for _ in range(50):
                prev_count = len(urls)
                try:
                    next_btn.click()
                    time.sleep(0.4)
                except Exception:
                    break
                try:
                    for u in (drv.execute_script(COLLECT_JS) or []):
                        if u and u not in seen:
                            seen.add(u)
                            urls.append(u)
                except Exception:
                    break
                if len(urls) == prev_count:
                    no_new += 1
                    if no_new >= 3:
                        break
                else:
                    no_new = 0

        return urls

    def _scrape_and_save_one(self, url: str, prop_id: str) -> dict[str, Any] | None:
        """Override: use Selenium for detail pages, requests for everything else."""
        html = self.fetch_detail_selenium(url)
        if not html:
            return None

        data = self.scrape_detail_page(html, url)
        if not data:
            return None

        soup = BeautifulSoup(html, "html.parser")

        if self.use_gallery_collector:
            selenium_images = self._collect_gallery_selenium()
            if selenium_images:
                data["image_urls"] = selenium_images
            else:
                data["image_urls"] = self.extract_image_urls(soup, url)
        else:
            data["image_urls"] = self.extract_image_urls(soup, url)

        return self._finalize_and_save(data, soup, url, prop_id)

    def shutdown(self) -> None:
        """Clean up the Selenium driver."""
        if self._driver:
            try:
                self._driver.quit()
            except Exception:
                pass
            self._driver = None


# ═══════════════════════════════════════════════════════════════════════════════
# Smart Update Mixin (--quick incremental mode)
# ═══════════════════════════════════════════════════════════════════════════════


class SmartUpdateMixin:
    """Mixin for scrapers that support incremental updates via url_map.json.

    Provides:
      - load_url_map() / save_url_map() for tracking known URLs
      - quick_scan_listings() for --quick mode (stops after N consecutive known URLs)
      - diff_live_vs_known() for detecting new/delisted properties
    """

    quick_stop_after: int = 5  # Stop pagination after this many consecutive known URLs

    @property
    def logger(self) -> logging.Logger:
        """Return a logger instance, falling back to a module-level default."""
        if not hasattr(self, "_logger"):
            self._logger = logging.getLogger(self.__class__.__name__)
        return self._logger

    @logger.setter
    def logger(self, value: logging.Logger) -> None:
        self._logger = value

    def load_url_map(self) -> dict[str, str]:
        """Load {url: folder_name} from url_map.json, building from files if needed."""
        map_path = os.path.join(self.props_dir, "url_map.json")
        if os.path.isfile(map_path):
            try:
                return json.load(open(map_path, encoding="utf-8"))
            except (json.JSONDecodeError, OSError):
                pass
        return self._build_url_map_from_files()

    def save_url_map(self, url_map: dict[str, str]) -> None:
        """Persist url_map.json."""
        map_path = os.path.join(self.props_dir, "url_map.json")
        with open(map_path, "w", encoding="utf-8") as f:
            json.dump(url_map, f, indent=2, ensure_ascii=False)

    def _build_url_map_from_files(self) -> dict[str, str]:
        """Scan property folders and rebuild url_map from stored URLs."""
        url_map: dict[str, str] = {}
        if not os.path.isdir(self.props_dir):
            return url_map
        for d in os.listdir(self.props_dir):
            if not re.fullmatch(r"property_\d+", d):
                continue
            jpath = os.path.join(self.props_dir, d, f"{d}.json")
            if os.path.isfile(jpath):
                try:
                    with open(jpath, encoding="utf-8") as f:
                        data = json.load(f)
                    url = data.get("url", "").strip()
                    if url:
                        url_map[url] = d
                except (json.JSONDecodeError, OSError):
                    pass
        return url_map

    def quick_scan_listings(
        self, base_scraper: BaseScraper
    ) -> list[str]:
        """Quick listing scan: stops early after consecutive known URLs.

        Returns list of NEW property URLs that need scraping.
        """
        url_map = self.load_url_map()
        known: set[str] = set(url_map.keys())
        new_urls: list[str] = []

        for page_num in range(1, base_scraper.max_pages + 1):
            page_url = base_scraper.get_listing_url(page_num)
            self.logger.info(f"Quick scan page {page_num}: {page_url}")

            r = base_scraper.fetch(page_url)
            if not r:
                break

            soup = BeautifulSoup(r.content, "html.parser")
            links = base_scraper.extract_property_links(soup, page_url)
            new_this_page = 0
            consec_known = 0

            for link in links:
                canonical = link.split("?")[0].rstrip("/")
                if canonical not in known and canonical not in new_urls:
                    new_urls.append(canonical)
                    consec_known = 0
                    new_this_page += 1
                else:
                    consec_known += 1
                    if consec_known >= self.quick_stop_after:
                        break

            self.logger.info(
                f"Page {page_num}: {new_this_page} new "
                f"(consec_known={consec_known})"
            )

            if consec_known >= self.quick_stop_after or new_this_page == 0:
                break

            base_scraper.delay()

        self.logger.info(f"Quick scan found {len(new_urls)} new properties")
        return new_urls

    def diff_live_vs_known(
        self, live_urls: set[str], url_map: dict[str, str]
    ) -> tuple[set[str], set[str], set[str]]:
        """Compare live URLs against local url_map.

        Returns (new_urls, dead_urls, extant_urls).
        """
        known_urls = {u.rstrip("/") for u in url_map}
        live_norm = {u.rstrip("/") for u in live_urls}

        new = live_norm - known_urls
        dead = known_urls - live_norm
        extant = live_norm & known_urls

        return new, dead, extant

    def save_delisted(
        self, dead_urls: set[str], url_map: dict[str, str]
    ) -> None:
        """Archive delisted property folders and update url_map."""
        delisted_dir = os.path.join(self.props_dir, "delisted")
        os.makedirs(delisted_dir, exist_ok=True)

        for url in dead_urls:
            norm = url.rstrip("/")
            folder = url_map.get(norm) or url_map.get(url)
            if folder:
                src = os.path.join(self.props_dir, folder)
                dst = os.path.join(delisted_dir, folder)
                if os.path.isdir(src) and not os.path.exists(dst):
                    shutil.move(src, dst)
                    self.logger.info(f"Archived {folder} → delisted/")
                if norm in url_map:
                    del url_map[norm]
                if url in url_map:
                    del url_map[url]


# ═══════════════════════════════════════════════════════════════════════════════
# PropertyPal Classic CMS parser (shared selectors)
# ═══════════════════════════════════════════════════════════════════════════════


def parse_pp_classic_detail(html: str, url: str) -> dict[str, Any]:
    """Parse a PropertyPal Classic CMS detail page (PP, HC, BT, etc.).

    Extractors for ul.dettbl metadata, div.textbp description, ul.feats,
    ul#gallery images, and h1 address.
    """
    soup = BeautifulSoup(html, "html.parser")
    data: dict[str, Any] = {"url": url}

    # Address
    h1 = soup.find("h1")
    if h1:
        data["address"] = h1.get_text(separator=" ", strip=True)
    if not data.get("address"):
        title_tag = soup.find("title")
        if title_tag:
            t = title_tag.get_text(strip=True)
            for sep in [" | ", " - ", " — "]:
                if sep in t:
                    t = t[:t.rfind(sep)]
            data["address"] = t.strip()
    data.setdefault("address", "")
    data["title"] = data["address"]

    # Price (ul.dettbl)
    for li in soup.select("ul.dettbl li"):
        dt1 = li.find(class_="dt1")
        dt2 = li.find(class_="dt2")
        if not dt1 or not dt2:
            continue
        key = dt1.get_text(strip=True).lower()
        val = dt2.get_text(strip=True)
        if "price" in key:
            data["price_str"] = val
        elif "style" in key or "type" in key:
            data["property_type"] = val
        elif "bedroom" in key:
            data["bedrooms"] = val
        elif "reception" in key:
            data["receptions"] = val
        elif "bathroom" in key:
            data["bathrooms"] = val
        elif "status" in key:
            data["status"] = val
        elif "heating" in key:
            data["heating"] = val

    # Fallback price
    if not data.get("price_str"):
        for sel in ["span.price-amount", ".Price-priceValue", ".price-value"]:
            el = soup.select_one(sel)
            if el:
                t = el.get_text(strip=True)
                if t:
                    qualifier = soup.select_one('.Price-priceOffers, [class*="priceOffers"]')
                    q = (qualifier.get_text(strip=True) + " ") if qualifier else ""
                    data["price_str"] = (q + t).strip()
                    break

    # Fallback: Bluecubes-style blocks (e.g. Edmonton Estates detail pages) —
    # qualifier text in span.prop-det-price-text, amount in
    # span.prop-det-price-amount.
    if not data.get("price_str"):
        amount_el = soup.select_one(".prop-det-price-amount")
        if amount_el:
            amount = amount_el.get_text(strip=True)
            if amount:
                qual_el = soup.select_one(".prop-det-price-text")
                q = (qual_el.get_text(strip=True) + " ") if qual_el else ""
                data["price_str"] = (q + amount).strip()

    # Fallback: PropertyPal listing-card style price block — a span.dpp amount
    # with an optional span.dpt qualifier beside it, inside a price-ish wrapper
    # (Michael Chandler uses div.price, Pinpoint uses div.dcell.dprice).
    if not data.get("price_str"):
        dpp = soup.select_one("span.dpp")
        if dpp:
            amount = dpp.get_text(strip=True)
            if amount:
                dpt = dpp.find_previous_sibling("span", class_="dpt")
                # Only prepend a label that qualifies the price (e.g. "Price
                # Reduced From" is decoration, "Asking Price" adds nothing).
                q = ""
                if dpt:
                    label = dpt.get_text(strip=True)
                    if label.lower() not in ("asking price", "price"):
                        q = label + " "
                data["price_str"] = (q + amount).strip()

    # Status fallback — `.SingleListingPage-topEle` contains the full summary
    # blob ("Sale Agreed 3 bedrooms 2 receptions semi-detached"), so we
    # match status substrings inside the blob instead of asking
    # normalise_status to exact-match the whole thing.
    if not data.get("status"):
        for sel in [".SingleListingPage-topEle", "div.dtsm", ".status"]:
            el = soup.select_one(sel)
            if el:
                blob = el.get_text(separator=" ", strip=True).lower()
                if "sale agreed" in blob or "sale  agreed" in blob:
                    data["status"] = "Sale Agreed"
                elif "under offer" in blob:
                    data["status"] = "Under Offer"
                elif blob.startswith("sold"):
                    data["status"] = "Sold"
                elif "let agreed" in blob or "let  agreed" in blob:
                    data["status"] = "Let Agreed"
                elif blob.startswith("let"):
                    data["status"] = "Let"
                elif "for sale" in blob:
                    data["status"] = "For Sale"
                break

    # Key features
    feats: list[str] = []
    for sel in [
        "ul.feats li", "div.prop-det-feats .feat",
        ".DescriptionBox--bullets li", ".DescriptionBox--bullets p",
        "ul.features li",
    ]:
        feats = [
            el.get_text(strip=True)
            for el in soup.select(sel)
            if el.get_text(strip=True)
        ]
        if feats:
            break
    data["key_features"] = feats

    # Description
    desc = ""
    for sel in [
        "div.textbp", "div.prop-det-text .text", ".ListingDescr-text",
        "div.description",
    ]:
        el = soup.select_one(sel)
        if el:
            t = el.get_text(separator=" ", strip=True)
            if len(t) > len(desc):
                desc = t
    data["description"] = desc

    # Rooms
    rooms: list[dict[str, str]] = []
    for room_row in soup.select("div.prop-det-rooms div.room-row"):
        room_data: dict[str, str] = {"name": "", "dimensions": "", "description": ""}
        name_span = room_row.find("span", class_="room-name")
        desc_span = room_row.find("span", class_="room-desc")
        if name_span:
            dim_span = name_span.find("span")
            if dim_span:
                room_data["dimensions"] = dim_span.get_text(strip=True)
                room_data["name"] = (
                    name_span.get_text(strip=True)
                    .replace(dim_span.get_text(strip=True), "")
                    .strip()
                )
            else:
                room_data["name"] = name_span.get_text(strip=True)
        if desc_span:
            inner = desc_span.find("span")
            room_data["description"] = (
                inner.get_text(strip=True) if inner else desc_span.get_text(strip=True)
            )
        if room_data["name"]:
            rooms.append(room_data)
    data["rooms"] = rooms

    return data


def extract_pp_gallery_images(soup: BeautifulSoup, url: str) -> list[str]:
    """Extract image URLs from a PropertyPal CMS gallery (ul#gallery)."""
    seen: set[str] = set()
    image_urls: list[str] = []

    gallery = soup.find("ul", id="gallery") or soup.find("div", id="gallery")
    if gallery:
        for a in gallery.find_all("a", href=True):
            if "slick-cloned" not in (a.parent.get("class") or []):
                href = a["href"]
                if is_image_href(href):
                    full = urljoin(url, href) if not href.startswith("http") else href
                    if full not in seen:
                        seen.add(full)
                        image_urls.append(full)
        if image_urls:
            return image_urls

    # Fallback: property ID pattern from URL
    m = re.search(r"/property/[^/]+/([^/]+)/", url)
    if m:
        prop_id = m.group(1)
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if f"/images/property/1/{prop_id}/" in href or (
                prop_id in href and is_image_href(href)
            ):
                full = urljoin(url, href) if not href.startswith("http") else href
                if full not in seen:
                    seen.add(full)
                    image_urls.append(full)
        if image_urls:
            return image_urls

    # Last resort: any img with /images/property/ in src
    for img in soup.find_all("img"):
        src = img.get("src") or img.get("data-src") or img.get("data-lazy-src")
        if src and "/images/property/" in src:
            if not any(x in src.lower() for x in ("logo", "office", "icon", "favicon")):
                full = urljoin(url, src) if not src.startswith("http") else src
                if full not in seen:
                    seen.add(full)
                    image_urls.append(full)

    return image_urls


# ═══════════════════════════════════════════════════════════════════════════════
# PropertyPal Bluecubes CMS parser (shared selectors for JM, UPS, TR, etc.)
# ═══════════════════════════════════════════════════════════════════════════════


def parse_pp_bluecubes_detail(html: str, url: str) -> dict[str, Any]:
    """Parse a PropertyPal Bluecubes CMS detail page (JM, UPS, TR).

    Extractors for div.prop-det-info-row metadata, div.prop-det-text for
    description, div.prop-det-feats for key features, div#gallery for images,
    and h1/title for address.
    """
    soup = BeautifulSoup(html, "html.parser")

    # UPS serves a soft-404: HTTP 200 with <h1>Page No Longer Exists</h1>
    # for removed listings. Treat as a failed parse so nothing gets written.
    _h1 = soup.find("h1") or soup.find("h2")
    if _h1 and "page no longer exists" in _h1.get_text(" ", strip=True).lower():
        return None

    data: dict[str, Any] = {"url": url}

    # Address
    h1 = _h1
    if h1:
        data["address"] = h1.get_text(strip=True)
        data["title"] = h1.get_text(strip=True)

    title_tag = soup.find("title")
    if title_tag and not data.get("address"):
        t = title_tag.get_text(strip=True)
        for sep in [" | ", " - ", " — "]:
            if sep in t:
                t = t[:t.rfind(sep)]
        data["address"] = t.strip()
        data["title"] = data["address"]

    # UPS's h1 stops after the street with a dangling comma
    # ("12 Orpen Road,"); og:title carries the town as well
    # ("12 Orpen Road, Belfast for sale with UPS"). Take the richer
    # variant, and never publish a trailing comma.
    og_title = soup.find("meta", property="og:title")
    if og_title and og_title.get("content"):
        og_addr = re.split(
            r"\s+(?:for sale|for rent|to let|to rent)\b",
            og_title["content"], flags=re.I,
        )[0].strip().rstrip(",")
        if og_addr and len(og_addr) > len((data.get("address") or "").rstrip(",")):
            data["address"] = og_addr
            data["title"] = og_addr
    if data.get("address"):
        data["address"] = data["address"].rstrip(",")
        data["title"] = data["address"]

    # Metadata rows (div.prop-det-info-row)
    property_info: dict[str, str] = {}
    for row in soup.find_all("div", class_="prop-det-info-row"):
        left = row.find("span", class_="prop-det-info-left")
        right = row.find("span", class_="prop-det-info-right")
        if left and right:
            label = "".join(
                c for c in left.get_text(strip=True)
                if not ("\uf000" <= c <= "\uffff")
            ).strip()
            val = right.get_text(strip=True)
            property_info[label] = val
            key = label.lower()
            if key == "bedrooms":
                data["bedrooms"] = val
            elif "price" in key:
                data["price_str"] = val
            elif "status" in key:
                data["status"] = val
            elif "style" in key or "type" in key:
                data["property_type"] = val
            elif "reception" in key:
                data["receptions"] = val
            elif "bathroom" in key:
                data["bathrooms"] = val
            elif "heating" in key:
                data["heating"] = val
    if property_info:
        data["property_info"] = property_info

    # Fallback price (UPS uses prop-det-price-outer/amount on Bluecubes pages)
    if not data.get("price_str"):
        price_el = (
            soup.select_one("span.prop-det-price-amount")
            or soup.select_one(".prop-det-price-amount")
            or soup.find("span", class_="price-amount")
        )
        if price_el:
            # HTML entity &pound; = £; html.unescape is idempotent on plain text
            import html as _html
            data["price_str"] = _html.unescape(price_el.get_text(strip=True))

    # Fallback price (Pinpoint): listing-card block with span.dpp amount and an
    # optional span.dpt qualifier (e.g. "Offers Over").
    if not data.get("price_str"):
        dpp = soup.select_one("span.dpp")
        if dpp:
            amount = dpp.get_text(strip=True)
            if amount:
                dpt = dpp.find_previous_sibling("span", class_="dpt")
                q = ""
                if dpt:
                    label = dpt.get_text(strip=True)
                    if label.lower() not in ("asking price", "price"):
                        q = label + " "
                data["price_str"] = (q + amount).strip()

    # Status fallback 1: overlay badge. Bluecubes sale-agreed/sold pages display
    # an overlay div inside `.prop-det-status-outer` with classes like `sale-agr`
    # or `sold`. Detect the class directly (more reliable than inner text,
    # which is empty when the badge is just an SVG).
    if not data.get("status"):
        outer = soup.select_one(".prop-det-status-outer")
        if outer:
            cls = " ".join(c.lower() for el in outer.find_all(True) for c in (el.get("class") or []))
            alt = " ".join((img.get("alt") or "").lower() for img in outer.find_all("img"))
            blob = (cls + " " + alt).strip()
            if "sale-agr" in blob or "saleagr" in blob or "agreed" in blob:
                data["status"] = "Sale Agreed"
            elif "sold" in blob:
                data["status"] = "Sold"
            elif "under-offer" in blob or "underoffer" in blob:
                data["status"] = "Under Offer"
            elif "let-agr" in blob or "letagr" in blob:
                data["status"] = "Let Agreed"

    # Status fallback 2: `.SingleListingPage-topEle` contains the full summary
    # blob ("Sale Agreed 3 bedrooms 2 receptions semi-detached"), so we match
    # status substrings instead of asking normalise_status to exact-match.
    if not data.get("status"):
        for sel in [".SingleListingPage-topEle", "div.dtsm", ".status"]:
            el = soup.select_one(sel)
            if el:
                blob = el.get_text(separator=" ", strip=True).lower()
                if "sale agreed" in blob or "sale  agreed" in blob:
                    data["status"] = "Sale Agreed"
                elif "under offer" in blob:
                    data["status"] = "Under Offer"
                elif blob.startswith("sold"):
                    data["status"] = "Sold"
                elif "let agreed" in blob or "let  agreed" in blob:
                    data["status"] = "Let Agreed"
                elif blob.startswith("let"):
                    data["status"] = "Let"
                elif "for sale" in blob:
                    data["status"] = "For Sale"
                break

    # Key features (div.prop-det-feats .feat)
    feats: list[str] = []
    feats_div = soup.find("div", class_="prop-det-feats")
    if feats_div:
        feats = [
            el.get_text(strip=True)
            for el in feats_div.find_all("div", class_="feat")
            if el.get_text(strip=True)
        ]
    if not feats:
        for sel in [
            "ul.feats li", ".DescriptionBox--bullets li", "ul.features li",
        ]:
            feats = [
                el.get_text(strip=True)
                for el in soup.select(sel)
                if el.get_text(strip=True)
            ]
            if feats:
                break
    data["key_features"] = feats

    # Description (div.prop-det-text .text)
    desc_div = soup.find("div", class_="prop-det-text")
    if desc_div:
        text_div = desc_div.find("div", class_="text") or desc_div
        data["description"] = text_div.get_text(separator=" ", strip=True)
    if not data.get("description"):
        for sel in [
            "div.textbp", ".ListingDescr-text", "div.description",
        ]:
            el = soup.select_one(sel)
            if el:
                data["description"] = el.get_text(separator=" ", strip=True)
                break

    # Rooms (div.prop-det-rooms div.room-row)
    rooms: list[dict[str, str]] = []
    rooms_div = soup.find("div", class_="prop-det-rooms")
    if rooms_div:
        for room_row in rooms_div.find_all("div", class_="room-row"):
            room_data: dict[str, str] = {"name": "", "dimensions": "", "description": ""}
            name_span = room_row.find("span", class_="room-name")
            desc_span = room_row.find("span", class_="room-desc")
            if name_span:
                dim_span = name_span.find("span")
                if dim_span:
                    room_data["dimensions"] = dim_span.get_text(strip=True)
                    room_data["name"] = (
                        name_span.get_text(strip=True)
                        .replace(dim_span.get_text(strip=True), "")
                        .strip()
                    )
                else:
                    room_data["name"] = name_span.get_text(strip=True)
            if desc_span:
                inner = desc_span.find("span")
                room_data["description"] = (
                    inner.get_text(strip=True) if inner else desc_span.get_text(strip=True)
                )
            if room_data["name"]:
                rooms.append(room_data)
    data["rooms"] = rooms

    return data


def extract_pp_bluecubes_gallery(soup: BeautifulSoup, url: str) -> list[str]:
    """Extract image URLs from a PropertyPal Bluecubes CMS gallery (div#gallery)."""
    seen: set[str] = set()
    image_urls: list[str] = []

    gallery = soup.find("div", id="gallery")
    if gallery:
        for a in gallery.find_all("a", href=True):
            if is_image_href(a["href"]):
                full = urljoin(url, a["href"]) if not a["href"].startswith("http") else a["href"]
                if full not in seen:
                    seen.add(full)
                    image_urls.append(full)
        if image_urls:
            return image_urls

    # Fallback: img tags with /images/property/ in src
    for img in soup.find_all("img"):
        src = img.get("src") or img.get("data-src")
        if src and "/images/property/" in src:
            if not any(x in src.lower() for x in ("logo", "office", "icon", "favicon")):
                full = urljoin(url, src) if not src.startswith("http") else src
                if full not in seen:
                    seen.add(full)
                    image_urls.append(full)

    return image_urls


# ═══════════════════════════════════════════════════════════════════════════════
# PropertyPal Modern CMS parser (PP Modern style: mm, ce, gm)
# ═══════════════════════════════════════════════════════════════════════════════


def parse_pp_modern_detail(html: str, url: str) -> dict[str, Any]:
    """Parse a PropertyPal Modern CMS detail page (MM, CE, GM).

    Uses .Price-priceValue, .ListingDescr-text, .Address-* classes,
    .ListingPage-briefIcon* for beds/baths/receptions, and
    og:description meta for price fallback.
    """
    soup = BeautifulSoup(html, "html.parser")
    data: dict[str, Any] = {"url": url}

    # Address
    line1 = soup.select_one(".Address-addressLine1")
    town = soup.select_one(".Address-addressTown")
    outcode = soup.select_one(".Address-addressOutcode")
    incode = soup.select_one(".Address-addressIncode")
    if line1:
        l1 = line1.get_text(strip=True).rstrip(",")
        tw = (town.get_text(strip=True) if town else "").rstrip(",")
        pc = " ".join(filter(None, [
            outcode.get_text(strip=True) if outcode else "",
            incode.get_text(strip=True) if incode else "",
        ]))
        data["address"] = ", ".join(filter(None, [l1, tw, pc]))
    if not data.get("address"):
        h1 = soup.select_one("h1")
        if h1:
            data["address"] = h1.get_text(separator=" ", strip=True)
    if not data.get("address"):
        og = soup.find("meta", property="og:title")
        if og and og.get("content"):
            raw = og["content"]
            for sep in [" | ", " – ", " - "]:
                if sep in raw:
                    raw = raw[:raw.rfind(sep)]
            data["address"] = raw.strip()
    data.setdefault("address", "")
    data["title"] = data["address"]

    # Price
    price_val = soup.select_one(".Price-priceValue")
    if price_val:
        qualifier = soup.select_one(".Price-priceOffers")
        q = (qualifier.get_text(strip=True) + " ") if qualifier else ""
        data["price_str"] = (q + price_val.get_text(strip=True)).strip()
    if not data.get("price_str"):
        og_desc = soup.find("meta", property="og:description")
        if og_desc and og_desc.get("content"):
            m = re.search(r"(£[\d,]+|POA)", og_desc["content"], re.I)
            if m:
                data["price_str"] = m.group(1)

    # Bedrooms / Bathrooms / Receptions — PropertyPal Modern uses two different
    # class prefixes depending on the agent: `.ListingPage-briefIcon*` (PP, MM
    # newer pages) or `.PropBox-briefIcon*` (McMillan McClure). Handle both.
    bed_el = (
        soup.select_one(".ListingPage-briefIconBed")
        or soup.select_one(".PropBox-briefIconBed")
    )
    if bed_el:
        beds_text = bed_el.get_text(strip=True)
        for char in beds_text:
            if char.isdigit():
                data["bedrooms"] = beds_text
                break

    bath_el = (
        soup.select_one(".ListingPage-briefIconBath")
        or soup.select_one(".PropBox-briefIconBath")
    )
    if bath_el:
        data["bathrooms"] = bath_el.get_text(strip=True)

    recp_el = (
        soup.select_one(".ListingPage-briefIconRecp")
        or soup.select_one(".PropBox-briefIconRecp")
    )
    if recp_el:
        data["receptions"] = recp_el.get_text(strip=True)

    # Property type from style brief icon too (some agents rely on it)
    if not data.get("property_type"):
        style_el = (
            soup.select_one(".ListingPage-briefIconStyle")
            or soup.select_one(".PropBox-briefIconStyle")
        )
        if style_el:
            data["property_type"] = style_el.get_text(strip=True)

    # Status
    status_el = (
        soup.select_one(".ListingTop-detailsStatus")
        or soup.select_one(".ListingPage-status")
    )
    if status_el:
        data["status"] = normalise_status(status_el.get_text(strip=True))

    # McMillan McClure shows status only as a banner overlay on the photo
    # scroller (e.g. <span class="ImageSliderBanner"><span>Sale Agreed</span></span>)
    if not data.get("status"):
        banner = soup.select_one(".ImageSliderBanner span") or soup.select_one(
            ".ImageSliderBanner"
        )
        if banner:
            txt = banner.get_text(separator=" ", strip=True)
            if txt:
                data["status"] = normalise_status(txt) or txt

    # Key features
    bullets = soup.select(".ListingPage-bullets li")
    if not bullets:
        bullets = soup.select(".ListingPage-bullets p")
    data["key_features"] = [
        el.get_text(strip=True) for el in bullets if el.get_text(strip=True)
    ]

    # Description
    desc_els = soup.select(".ListingDescr-text")
    full_desc = ""
    for el in desc_els:
        t = el.get_text(separator=" ", strip=True)
        if len(t) > len(full_desc):
            full_desc = t
    data["description"] = full_desc
    data["rooms"] = []

    return data


def extract_pp_modern_gallery(soup: BeautifulSoup, url: str) -> list[str]:
    """Extract image URLs from a PropertyPal Modern CMS page (media.propertypal.com CDN)."""
    seen: set[str] = set()
    image_urls: list[str] = []

    def _add(src: str) -> None:
        if src:
            full = urljoin(url, src) if not src.startswith("http") else src
            if full not in seen and full.startswith("http"):
                seen.add(full)
                image_urls.append(full)

    # ul#pphoto — all gallery image hrefs pre-rendered
    pphoto = soup.find("ul", id="pphoto")
    if pphoto:
        for a in pphoto.find_all("a", href=True):
            href = a["href"]
            if any(ext in href.lower() for ext in (".jpg", ".jpeg", ".png", ".webp")):
                _add(href)
        if not image_urls:
            for img in pphoto.find_all("img"):
                _add(img.get("src") or img.get("data-src") or img.get("data-lazy-src"))
        if image_urls:
            return image_urls

    # a[href*="media.propertypal.com"] — CDN links in DOM order
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "media.propertypal.com" in href:
            _add(href)
    if image_urls:
        return image_urls

    # img[src*="media.propertypal.com/sd/"] — pick largest srcset
    for img in soup.select('img[src*="media.propertypal.com/sd/"]'):
        src = img.get("src", "").strip()
        if not src or src in seen:
            continue
        best = src
        best_width = 0
        srcset = img.get("srcset", "")
        if srcset:
            for part in srcset.split(","):
                part = part.strip()
                pieces = part.split()
                if len(pieces) >= 2:
                    try:
                        w = int(pieces[1].rstrip("w"))
                        if w > best_width:
                            best_width = w
                            best = pieces[0]
                    except ValueError:
                        pass
        _add(best)
    if image_urls:
        return image_urls

    # ul#gallery / div#gallery / div.gallery fallback
    gallery = (
        soup.find("ul", id="gallery")
        or soup.find("div", id="gallery")
        or soup.find("div", class_="gallery")
    )
    if gallery:
        for a in gallery.find_all("a", href=True):
            if is_image_href(a["href"]):
                _add(a["href"])
        for img in gallery.find_all("img"):
            _add(img.get("src") or img.get("data-src") or img.get("data-lazy-src"))
        if image_urls:
            return image_urls

    # JSON in script tags
    for script in soup.find_all("script"):
        text = script.string or ""
        found = re.findall(
            r'["\']('
            r'(?:https?://[^"\']+)?'
            r'/(?:images?|photos?|property-images?|uploads?)/[^"\']+\.(?:jpe?g|png|webp)'
            r')["\']',
            text, re.I,
        )
        for f in found:
            _add(f if f.startswith("http") else urljoin(url, f))
        if image_urls:
            return image_urls

    # Fallback: any img with /images/property/ in src
    for img in soup.find_all("img"):
        src = img.get("src") or img.get("data-src") or img.get("data-lazy-src")
        if not src:
            continue
        full = urljoin(url, src)
        if any(p in full for p in ("/images/property/", "/property-images/",
                                     "/property_images/", "/uploads/property/")) and \
                not any(x in full.lower() for x in ("logo", "office", "icon", "favicon")):
            _add(full)

    return image_urls


# ═══════════════════════════════════════════════════════════════════════════════
# Config __init__ re-export to ease import
# ═══════════════════════════════════════════════════════════════════════════════

__all__ = [
    "BaseScraper",
    "SeleniumScraper",
    "SmartUpdateMixin",
    "HEADERS",
    "sort_and_dedup_image_urls",
    "is_image_href",
    "parse_pp_classic_detail",
    "extract_pp_gallery_images",
    "parse_pp_bluecubes_detail",
    "extract_pp_bluecubes_gallery",
    "parse_pp_modern_detail",
    "extract_pp_modern_gallery",
    "setup_logging",
    "build_property_row",
    "load_geocache",
    "normalise_status",
    "upsert_batch",
]
