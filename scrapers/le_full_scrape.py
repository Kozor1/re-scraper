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
import re


class LeScraper(BaseScraper):
    def get_listing_url(self, page_num: int) -> str:
        return self.config["listing_page"](page_num)

    def extract_property_links(
        self, soup: BeautifulSoup, page_url: str
    ) -> list[str]:
        # Real properties live at /property/<slug>/; the archive/pagination
        # pages are /properties/, /properties/for-sale/, /properties/page/N/ —
        # they must not be treated as listings.
        links: list[str] = []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "/property/" not in href:
                continue
            if "/properties/" in href:  # archive/pagination wrappers
                continue
            full = urljoin(page_url, href).split("?")[0].split("#")[0].rstrip("/")
            if full not in links:
                links.append(full)
        return links

    def scrape_detail_page(
        self, html: str, url: str
    ) -> dict[str, Any] | None:
        return self._parse_detail(html, url)

    def _parse_detail(self, html: str, url: str) -> dict[str, Any] | None:
        """Lennon Estates bespoke (Tailwind 'rane') theme — none of the PP
        markup families apply."""
        soup = BeautifulSoup(html, "html.parser")
        data: dict[str, Any] = {"url": url}

        h1 = soup.find("h1")
        if h1:
            data["address"] = h1.get_text(" ", strip=True)
        if not data.get("address"):
            self.logger.warning(f"No address on page: {url}")
            return None
        data["title"] = data["address"]

        # Price: big bold figure on for-sale pages (span.text-2xl…),
        # shrunk to a small chip once the status flips (Sale Agreed/Sold).
        price_el = soup.select_one("span.text-2xl.font-bold.text-primary")
        if price_el is None:
            # On Sale-Agreed/Sold pages the price shrinks to a small chip —
            # take the largest £ amount among them (rent/PCM chips can share
            # the same classes, so first-hit is unreliable).
            chips = [
                sp.get_text(strip=True)
                for sp in soup.select("span.text-base.font-bold.text-primary")
            ]
            amounts = []
            for t in chips:
                m = re.search(r"£\s*([\d,]+)", t)
                if m:
                    amounts.append(int(m.group(1).replace(",", "")))
            if amounts:
                data["price_str"] = f"£{max(amounts):,}"
        if price_el is not None:
            data["price_str"] = re.sub(r"\s+", " ", price_el.get_text()).strip()

        # Facilities: small chips like <div class=\"text-black text-xs\">4
        # Beds</div>, <div ...>2 Baths</div>, <div ...>Detached</div>
        for chip in soup.select("div.text-black.text-xs"):
            t = " ".join(chip.get_text().split())
            m = re.match(r"^(\d+)\s+Beds$", t)
            if m:
                data["bedrooms"] = m.group(1)
                continue
            m = re.match(r"^(\d+)\s+Baths$", t)
            if m:
                data["bathrooms"] = m.group(1)
                continue
            if t in ("Detached", "Semi-Detached", "Semi Detached", "Terrace",
                     "End of Terrace", "Apartment", "Bungalow", "Townhouse",
                     "Cottage", "Duplex"):
                data["property_type"] = t

        # Status: corner ribbon badge (For Sale / Sale Agreed / Sold) — the
        # listing's status ribbon is an absolutely-positioned span overlay.
        for sp in soup.select("span"):
            txt = " ".join(sp.get_text().split())
            if txt in ("For Sale", "Sale Agreed", "Sold", "Under Offer"):
                cls = " ".join(sp.get("class") or [])
                if "absolute" in cls:
                    data["status"] = txt
                    break

        # Description: text of the div directly wrapping the "Property
        # Overview" h2 (that container holds the heading + the marketing copy,
        # and nothing else).
        overview_h2 = soup.find(
            "h2", string=lambda s: s and s.strip() == "Property Overview"
        )
        if overview_h2 and overview_h2.parent:
            text = overview_h2.parent.get_text(" ", strip=True)
            text = text.replace("Property Overview", "", 1).strip()
            if text:
                data["description"] = text

        return data

    def extract_image_urls(
        self, soup: BeautifulSoup, page_url: str
    ) -> list[str]:
        # le's theme keeps the property gallery as lazy-loaded slides:
        # <div data-fancybox data-src="https://lennon-estates.com/wp-content/uploads/…">
        # The shared PropertyPal selectors find nothing on these pages.
        urls: list[str] = []
        seen: set[str] = set()
        for el in soup.select("[data-src]"):
            src = el.get("data-src") or ""
            if not re.search(r"\.(jpe?g|png|webp)$", src.split("?")[0], re.I):
                continue
            full = urljoin(page_url, src)
            if "wp-content/uploads" in full and full not in seen:
                seen.add(full)
                urls.append(full)
        if urls:
            return urls
        return extract_pp_gallery_images(soup, page_url)


if __name__ == "__main__":
    LeScraper.cli_main("le")
