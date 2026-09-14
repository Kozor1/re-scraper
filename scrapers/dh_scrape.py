from __future__ import annotations

import os as _os, sys as _sys
_PKG_ROOT = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
_sys.path.insert(0, _PKG_ROOT)


from scrapers.base import (
    BaseScraper,
    parse_pp_bluecubes_detail,
    extract_pp_bluecubes_gallery,
)
from bs4 import BeautifulSoup
from typing import Any
from urllib.parse import urljoin


class DanielHenryScraper(BaseScraper):
    """Daniel Henry scraper — requests by default, Selenium fallback on failure."""

    request_delay_min: float = 1.2
    request_delay_max: float = 2.5
    _use_selenium: bool = False

    def get_listing_url(self, page_num: int) -> str:
        return self.config["listing_page"](page_num)

    def extract_property_links(
        self, soup: BeautifulSoup, page_url: str
    ) -> list[str]:
        links: list[str] = []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if self.config["link_pattern"] in href:
                full = urljoin(page_url, href).split("?")[0].rstrip("/")
                if full not in links:
                    links.append(full)
        return links

    def scrape_detail_page(
        self, html: str, url: str
    ) -> dict[str, Any] | None:
        data = parse_pp_bluecubes_detail(html, url)
        if not data.get("address") and not data.get("description"):
            self._use_selenium = True
            return None
        return data

    def extract_image_urls(
        self, soup: BeautifulSoup, page_url: str
    ) -> list[str]:
        return extract_pp_bluecubes_gallery(soup, page_url)


if __name__ == "__main__":
    DanielHenryScraper.cli_main("dh")
