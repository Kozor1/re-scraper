from __future__ import annotations

import re
from bs4 import BeautifulSoup
from typing import Any
from urllib.parse import urljoin

import os as _os, sys as _sys
_PKG_ROOT = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
_sys.path.insert(0, _PKG_ROOT)


from scrapers.base import BaseScraper


def _extract_price(soup: BeautifulSoup) -> str | None:
    p_el = soup.find("p", class_="price")
    if p_el:
        price_text = p_el.get_text(strip=True)
        m = re.search(r"£[\d,]+", price_text)
        if m:
            return m.group(0)
    for el in soup.find_all(["p", "span"], string=re.compile(r"£[\d,]+")):
        m = re.search(r"£[\d,]+", el.get_text(strip=True))
        if m:
            return m.group(0)
    return None


def _extract_from_description(desc: str, pattern: str) -> str | None:
    if not desc:
        return None
    m = re.search(pattern, desc, re.IGNORECASE)
    if m:
        return m.group(1)
    return None


class BillMcCannScraper(BaseScraper):
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
        soup = BeautifulSoup(html, "html.parser")
        data: dict[str, Any] = {"url": url}

        # Check for redirect-to-homepage (delisted property)
        h1 = soup.find("h1")
        h1_text = h1.get_text(strip=True).lower() if h1 else ""
        if "we find the home" in h1_text:
            return None

        # Address
        addr_h1 = soup.find("h1", class_=lambda x: x and "font-gilroybold" in x)
        if addr_h1:
            t = addr_h1.get_text(strip=True)
            if len(t) > 10 and "find the home" not in t.lower():
                data["address"] = t
                data["title"] = t
        if not data.get("address"):
            for h in soup.find_all("h1"):
                t = h.get_text(strip=True)
                if len(t) > 10 and "find the home" not in t.lower():
                    data["address"] = t
                    data["title"] = t
                    break
        if not data.get("address"):
            title_tag = soup.find("title")
            if title_tag:
                t = title_tag.get_text(strip=True)
                for suffix in [
                    " | Bill McCann", " - Bill McCann Estate Agency",
                    " | Bill McCann Estate Agency",
                ]:
                    t = t.replace(suffix, "")
                if "find the home" not in t.lower() and len(t) > 10:
                    data["address"] = t.strip()
                    data["title"] = data["address"]

        # Price
        price = _extract_price(soup)
        if price:
            data["price_str"] = price

        # Description
        desc_div = soup.find("div", {"data-content": "description"})
        if desc_div:
            paragraphs = [
                p.get_text(strip=True)
                for p in desc_div.find_all("p")
                if p.get_text(strip=True)
            ]
            if paragraphs:
                data["description"] = "\n\n".join(paragraphs)
        else:
            prop_details = soup.find("div", {"id": "property-details"})
            if prop_details:
                data["description"] = prop_details.get_text(
                    separator="\n", strip=True
                )

        # Bedrooms from description
        desc = data.get("description", "")
        beds = _extract_from_description(desc, r"(\d+)\s*bedroom")
        if beds:
            data["bedrooms"] = beds

        baths = _extract_from_description(desc, r"(\d+)\s*bathroom")
        if baths:
            data["bathrooms"] = baths

        return data

    def extract_image_urls(
        self, soup: BeautifulSoup, page_url: str
    ) -> list[str]:
        image_urls: list[str] = []
        seen: set[str] = set()

        gallery = soup.find("div", class_="property-gallery")
        if gallery:
            for img in gallery.find_all("img"):
                src = img.get("src") or img.get("data-src")
                if src and "wp-content/uploads" in src:
                    if "themes/billmccann_theme" not in src:
                        full = urljoin(page_url, src) if not src.startswith("http") else src
                        if full not in seen:
                            seen.add(full)
                            image_urls.append(full)

        if not image_urls:
            for img in soup.find_all("img"):
                src = img.get("src") or img.get("data-src")
                if src and "wp-content/uploads" in src:
                    if "logo" not in src.lower() and "themes/billmccann_theme" not in src:
                        full = urljoin(page_url, src) if not src.startswith("http") else src
                        if full not in seen:
                            seen.add(full)
                            image_urls.append(full)

        return image_urls


if __name__ == "__main__":
    BillMcCannScraper.cli_main("bmc")
