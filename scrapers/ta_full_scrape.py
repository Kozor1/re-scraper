from __future__ import annotations

import os as _os, sys as _sys
_PKG_ROOT = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
_sys.path.insert(0, _PKG_ROOT)


from scrapers.base import (
    BaseScraper,
    parse_pp_classic_detail,
    extract_pp_gallery_images,
)
from bs4 import BeautifulSoup
from typing import Any
from urllib.parse import urljoin


class TaScraper(BaseScraper):
    def get_listing_url(self, page_num: int) -> str:
        return self.config["listing_page"](page_num)

    def extract_property_links(
        self, soup: BeautifulSoup, page_url: str
    ) -> list[str]:
        links: list[str] = []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if self.config["link_pattern"] in href:
                full = urljoin(page_url, href)
                if full not in links:
                    links.append(full)
        return links

    def scrape_detail_page(
        self, html: str, url: str
    ) -> dict[str, Any] | None:
        return parse_pp_classic_detail(html, url)

    def extract_image_urls(
        self, soup: BeautifulSoup, page_url: str
    ) -> list[str]:
        return extract_pp_gallery_images(soup, page_url)


if __name__ == "__main__":
    TaScraper.cli_main("ta")
