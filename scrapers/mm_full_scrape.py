"""McMillan McClure full scrape.

Site: https://www.mcmillanmcclure.com/search?sta=forSale&st=sale&pt=residential

Property URLs follow /<slug>/<numeric_id> (e.g. /12-hillview-drive-newtownabbey/1095700),
NOT /property/… like the other PropertyPal sites. Pages render property cards
server-side (no JS required), so this uses plain requests rather than Selenium.

Detail page uses the PropertyPal Modern markup (parse_pp_modern_detail).
"""

from __future__ import annotations

import os as _os, sys as _sys
_PKG_ROOT = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
_sys.path.insert(0, _PKG_ROOT)


import re

from scrapers.base import (
    BaseScraper,
    SmartUpdateMixin,
    parse_pp_modern_detail,
    extract_pp_modern_gallery,
)
from bs4 import BeautifulSoup
from typing import Any
from urllib.parse import urljoin

# Property detail URLs follow /<slug>/<7-digit-id> (e.g. /12-hillview-drive-...
# /1095700). The /p/<id> short-alias only appears when listing Sale Agreed/Sold
# — those are excluded by our sta=forSale-only filter, so we don't bother
# matching them here.
_PROP_URL_RE = re.compile(r"^/[a-z0-9][a-z0-9-]*/\d{6,}/?$", re.IGNORECASE)


class McMillanMcClureScraper(SmartUpdateMixin, BaseScraper):
    def get_listing_url(self, page_num: int) -> str:
        return self.config["listing_page"](page_num)

    def extract_property_links(
        self, soup: BeautifulSoup, page_url: str
    ) -> list[str]:
        links: list[str] = []
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            if not href.startswith("/"):
                continue
            path = href.split("?")[0].split("#")[0]
            if not _PROP_URL_RE.match(path):
                continue
            full = urljoin(page_url, path).rstrip("/")
            if full not in links:
                links.append(full)
        return links

    def scrape_detail_page(
        self, html: str, url: str
    ) -> dict[str, Any] | None:
        return parse_pp_modern_detail(html, url)

    def extract_image_urls(
        self, soup: BeautifulSoup, page_url: str
    ) -> list[str]:
        return extract_pp_modern_gallery(soup, page_url)


if __name__ == "__main__":
    McMillanMcClureScraper.cli_main("mm")
