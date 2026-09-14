#!/usr/bin/env python3
"""
Simon Brien Full Property Scraper

Uses the BaseScraper infrastructure from scrapers/base.py.
Simon Brien has a unique CMS (CUSTOM_SB) with a two-step scrape:
details then images, with a specific gallery link filter.
"""

from __future__ import annotations

import os
import re
import sys
import argparse
from typing import Any
from urllib.parse import urljoin

from bs4 import BeautifulSoup

SCRAPERS_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(SCRAPERS_DIR)
sys.path.insert(0, ROOT)

from scrapers.base import (
    BaseScraper,
    HEADERS,
    sort_and_dedup_image_urls,
    is_image_href,
)

OUTPUT_DIR = "properties/sb"
SOURCE_KEY = "sb"


class SimonBrienScraper(BaseScraper):
    source_key = SOURCE_KEY

    # ── Override defaults ────────────────────────────────────────────────────
    max_pages = 1000
    max_properties = 100_000
    request_delay_min = 1
    request_delay_max = 3

    def __init__(self) -> None:
        super().__init__(SOURCE_KEY)

    # ── Listing pages ────────────────────────────────────────────────────────

    def get_listing_url(self, page_num: int) -> str:
        if page_num == 1:
            return "https://www.simonbrien.com/property-for-sale"
        return f"https://www.simonbrien.com/property-for-sale/page{page_num}/?orderBy="

    def extract_property_links(
        self, soup: BeautifulSoup, page_url: str
    ) -> list[str]:
        links: list[str] = []
        for card in soup.find_all("a", class_="prop-card"):
            href = card.get("href")
            if href:
                full = urljoin(self.config["base_url"], href)
                links.append(full)
        self.logger.info(
            f"Found {len(links)} properties on page: {page_url}"
        )
        return links

    # ── Detail page ──────────────────────────────────────────────────────────

    def scrape_detail_page(
        self, html: str, url: str
    ) -> dict[str, Any] | None:
        """Parse Simon Brien property detail page."""
        soup = BeautifulSoup(html, "html.parser")

        data: dict[str, Any] = {"url": url}

        # Address
        address_one = soup.find("h1", class_="prop-det-address-one")
        address_two = soup.find("h2", class_="prop-det-address-two")
        if address_one and address_two:
            data["address"] = (
                f"{address_one.get_text(strip=True)}, "
                f"{address_two.get_text(strip=True)}"
            )
        elif address_one:
            data["address"] = address_one.get_text(strip=True)
        else:
            title = soup.find("h1") or soup.find("h2")
            data["title"] = (
                title.get_text(strip=True) if title else "Unknown"
            )

        # Price
        price_el = soup.find("span", class_="prop-det-price-amount")
        if price_el:
            data["price_str"] = price_el.get_text(strip=True)

        # Info rows (style, bedrooms, status, EPC)
        for row in soup.find_all("div", class_="prop-det-info-row"):
            left = row.find("span", class_="prop-det-info-left")
            right = row.find("span", class_="prop-det-info-right")
            if left and right:
                label = left.get_text(strip=True).lower()
                value = right.get_text(strip=True)
                if label == "style":
                    data["property_type"] = value
                elif label == "bedrooms":
                    data["bedrooms"] = value
                elif label == "status":
                    data["status"] = value
                elif label == "epc rating":
                    data["epc_rating"] = value

        # Key Features
        feats_title = soup.find(
            "h2", class_="prop-det-title", string="Key Features"
        )
        if feats_title:
            feats_div = feats_title.find_next_sibling(
                "div", class_="prop-det-feats"
            )
            if feats_div:
                features: list[str] = []
                for feat_div in feats_div.find_all("div", class_="feat"):
                    icon = feat_div.find("i", class_="fa")
                    if icon:
                        icon.decompose()
                    text = feat_div.get_text(strip=True)
                    if text:
                        features.append(text)
                data["key_features"] = features

        # Description (preserving paragraph structure)
        desc_title = soup.find(
            "h2", class_="prop-det-title", string="Description"
        )
        if desc_title:
            desc_div = desc_title.find_next_sibling(
                "div", class_="prop-det-text"
            )
            if desc_div:
                for tag in desc_div.find_all(["p", "br", "div"]):
                    if tag.name == "br":
                        tag.replace_with("\n")
                    else:
                        tag.insert_before("\n\n")
                raw = desc_div.get_text(separator="", strip=False)
                raw = re.sub(r"\n{3,}", "\n\n", raw).strip()
                data["description"] = raw

        # Rooms
        rooms_title = soup.find(
            "h2", class_="prop-det-title", string="Rooms"
        )
        if rooms_title:
            rooms_div = rooms_title.find_next_sibling(
                "div", class_="prop-det-rooms"
            )
            if rooms_div:
                rooms: list[dict[str, str]] = []
                for room_row in rooms_div.find_all(
                    "div", class_="room-row"
                ):
                    room = {"name": "", "description": ""}
                    rn = room_row.find("span", class_="room-name")
                    rd = room_row.find("span", class_="room-desc")
                    if rn:
                        room["name"] = rn.get_text(strip=True)
                    if rd:
                        room["description"] = rd.get_text(strip=True)
                    if room["name"] or room["description"]:
                        rooms.append(room)
                data["rooms"] = rooms

        return data

    # ── Image extraction ─────────────────────────────────────────────────────

    def extract_image_urls(
        self, soup: BeautifulSoup, page_url: str
    ) -> list[str]:
        """Extract image URLs from Simon Brien gallery.

        Filters out Slick-carousel clones and non-image links (videos).
        Preserves DOM insertion order (SB sometimes assigns a high numeric
        suffix to the hero shot; DOM order keeps it first).
        """
        gallery = soup.find("ul", id="gallery")
        if not gallery:
            self.logger.warning(f"No gallery found for {page_url}")
            return []

        all_links = gallery.find_all("a")
        gallery_links = [
            a
            for a in all_links
            if "slick-cloned" not in (a.parent.get("class") or [])
            and is_image_href(a.get("href", ""))
        ]
        self.logger.info(
            f"Found {len(all_links)} gallery links "
            f"({len(all_links) - len(gallery_links)} excluded: clones/non-image)"
        )

        seen: set[str] = set()
        image_urls: list[str] = []
        for link in gallery_links:
            full = urljoin(self.config["base_url"], link["href"])
            if full not in seen:
                seen.add(full)
                image_urls.append(full)

        return image_urls

    # ── Cleanup ──────────────────────────────────────────────────────────────

    def cleanup_data(self, data: dict[str, Any]) -> dict[str, Any]:
        """SB specific: ensure price_str and image_urls keys."""
        if "price" in data and "price_str" not in data:
            data["price_str"] = data.pop("price")
        if "images" in data and "image_urls" not in data:
            data["image_urls"] = data.pop("images")
        return data

    # ── CLI entry point ──────────────────────────────────────────────────────

    def run(self, args: argparse.Namespace | None = None) -> int:
        """SB-specific run: handles test_mode and fresh flag."""
        limit = args.limit if args else 0
        fresh = args.fresh if args else True

        self.logger.info(f"Starting {self.config['label']} scraper")

        if fresh and not self.test_mode:
            self.clear_output_dir()

        if self.test_mode:
            property_links = self.collect_property_links(max_pages=1)
            scraped = self.scrape_properties(property_links, limit=1)
        elif limit > 0:
            property_links = self.collect_property_links()
            scraped = self.scrape_properties(property_links, limit=limit)
        else:
            property_links = self.collect_property_links()
            scraped = self.scrape_properties(property_links)

        self.update_index(scraped)
        self.save_summary(len(scraped))

        self.logger.info(
            f"Scraping complete. Total: {len(scraped)} properties"
        )
        return len(scraped)


# ── Compatibility shim for callers that import scrape_property_details ────────

_scraper_instance: SimonBrienScraper | None = None


def _get_scraper() -> SimonBrienScraper:
    global _scraper_instance
    if _scraper_instance is None:
        _scraper_instance = SimonBrienScraper()
    return _scraper_instance


def scrape_property_details(url: str) -> dict[str, Any] | None:
    """Legacy API: scrape property details (used by property_update.py)."""
    scraper = _get_scraper()
    r = scraper.fetch(url)
    if not r:
        return None
    return scraper.scrape_detail_page(r.text, url)


def scrape_property_images(
    url: str, folder: str
) -> int:
    """Legacy API: scrape images (returns count). Used by property_update.py."""
    scraper = _get_scraper()
    r = scraper.fetch(url)
    if not r:
        return 0
    soup = BeautifulSoup(r.content, "html.parser")
    urls = scraper.extract_image_urls(soup, url)
    return len(urls)


def scrape_property_page(url: str, prop_id: str) -> dict[str, Any] | None:
    """Legacy API: full scrape of one property page."""
    scraper = _get_scraper()
    return scraper._scrape_and_save_one(url, prop_id)


# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Simon Brien property scraper"
    )
    parser.add_argument(
        "--limit", type=int, default=0,
        help="Max properties to scrape (0 = unlimited)"
    )
    parser.add_argument(
        "--fresh", action="store_true",
        help="Clear existing data before scraping"
    )
    args = parser.parse_args()

    scraper = SimonBrienScraper()
    scraper.run(args)
