from __future__ import annotations

import re
import json
from bs4 import BeautifulSoup
from typing import Any
from urllib.parse import urljoin

import os as _os, sys as _sys
_PKG_ROOT = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
_sys.path.insert(0, _PKG_ROOT)


from scrapers.base import BaseScraper


class ReedsRainsScraper(BaseScraper):
    def get_listing_url(self, page_num: int) -> str:
        return self.config["listing_page"](page_num)

    def extract_property_links(
        self, soup: BeautifulSoup, page_url: str
    ) -> list[str]:
        links: list[str] = []
        seen: set[str] = set()
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if self.config["link_pattern"] in href:
                full = urljoin(page_url, href).split("?")[0].rstrip("/")
                if full not in seen:
                    seen.add(full)
                    links.append(full)
        return links

    def scrape_detail_page(
        self, html: str, url: str
    ) -> dict[str, Any] | None:
        soup = BeautifulSoup(html, "html.parser")
        data: dict[str, Any] = {"url": url}

        def _clean_address(text: str) -> str:
            for pat in [
                r"^(\d+)\s+bedroom\s+[^,]+\s+for\s+sale,\s*",
                r"^[^,]+\s+for\s+sale,\s*",
            ]:
                text = re.sub(pat, "", text.strip(), flags=re.IGNORECASE)
            return text.strip()

        h1 = soup.find("h1")
        if h1:
            data["address"] = _clean_address(h1.get_text(strip=True))
            data["title"] = h1.get_text(strip=True)
        if not data.get("address"):
            title_tag = soup.find("title")
            if title_tag:
                t = title_tag.get_text(strip=True)
                for suffix in [
                    " for sale with Reeds Rains", " | Reeds Rains", " - Reeds Rains",
                ]:
                    t = t.replace(suffix, "")
                data["address"] = _clean_address(t)
                data["title"] = data["address"]

        # Price from HTML comment
        price_match = re.search(r'<!--property-price:"([^"]+)"-->', html)
        if price_match:
            data["price_str"] = f"£{price_match.group(1)}"
        else:
            price_el = soup.select_one(".property-info__price .sale_price")
            if price_el:
                data["price_str"] = price_el.get_text(strip=True)

        # Property type
        blurb = soup.select_one(".property-info__blurb")
        if blurb:
            type_match = re.search(
                r"^(.+?)\s+for\s+sale", blurb.get_text(strip=True), re.IGNORECASE
            )
            if type_match:
                data["property_type"] = type_match.group(1)

        # Status
        callout = soup.select_one(".property-info__callout")
        if callout:
            data["status"] = callout.get_text(strip=True)

        # Key features
        feats = [
            li.get_text(strip=True)
            for li in soup.select("ul.property-features li")
            if li.get_text(strip=True)
        ]
        if feats:
            data["key_features"] = feats

        # Bedrooms (multi-strategy: HTML comment, DOM, JSON-LD, title, features)
        bedrooms = None
        bed_comment = re.search(r'<!--property-bedrooms:"(\d+)"-->', html)
        if bed_comment:
            bedrooms = bed_comment.group(1)
        if not bedrooms:
            beds_el = soup.select_one(".property-info__feature--beds")
            if beds_el:
                bed_match = re.search(r"(\d+)", beds_el.get_text(strip=True))
                if bed_match:
                    bedrooms = bed_match.group(1)
        if not bedrooms and h1:
            bed_match = re.search(r"(\d+)\s*bedroom", h1.get_text(strip=True), re.IGNORECASE)
            if bed_match:
                bedrooms = bed_match.group(1)
        if not bedrooms:
            schema_match = re.search(
                r'<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>',
                html, re.DOTALL,
            )
            if schema_match:
                try:
                    sd = json.loads(schema_match.group(1))
                    if isinstance(sd, dict) and "numberOfBedrooms" in sd:
                        bedrooms = str(sd["numberOfBedrooms"])
                except (json.JSONDecodeError, AttributeError):
                    pass
        if not bedrooms:
            text_to_num = {
                "one": "1", "two": "2", "three": "3", "four": "4",
                "five": "5", "six": "6", "seven": "7", "eight": "8",
                "nine": "9", "ten": "10",
            }
            for feat in feats:
                m = re.search(r"(\d+)\s*bedroom", feat, re.IGNORECASE)
                if m:
                    bedrooms = m.group(1)
                    break
                for text_n, num in text_to_num.items():
                    if re.search(rf"\b{text_n}\b.*bedroom", feat, re.IGNORECASE):
                        bedrooms = num
                        break
                if bedrooms:
                    break
        if bedrooms:
            data["bedrooms"] = bedrooms

        # Description — ONLY the marketing blurb in .property-description__truncated.
        # The container also holds .property-features__description__more with
        # per-room blocks; blanket find_all("p") previously picked each room's
        # outer <p> (heading+body glued together) AND its inner title/dims/body
        # <p>s, so every section appeared twice with mangled spacing, plus
        # legal boilerplate sections (PERSONAL INTEREST, CUSTOMER DUE
        # DILIGENCE) tacked on. Rooms go to the structured rooms field instead.
        desc_container = soup.select_one(".property-features__description")
        if desc_container:
            blurb = desc_container.select_one(".property-description__truncated")
            if blurb:
                desc_parts = [
                    t
                    for p in blurb.find_all("p")
                    if (t := p.get_text(strip=True))
                ]
                if desc_parts:
                    data["description"] = "\n\n".join(desc_parts)

        # Rooms — even though the markup is invalid nesting (<p> inside <p>),
        # html.parser tolerates it and the inner classes still resolve.
        rooms: list[dict[str, str]] = []
        for block in soup.select("p.property-rooms-description"):
            title_el = block.select_one(".property-rooms-description__title")
            dims_el = block.select_one(".property-rooms-description__dimensions")
            body_el = block.select_one(".property-rooms-description__description")
            if not title_el:
                continue
            name = title_el.get_text(strip=True)
            if not name:
                continue
            rooms.append(
                {
                    "name": name,
                    "dimensions": dims_el.get_text(strip=True) if dims_el else "",
                    "description": body_el.get_text(strip=True) if body_el else "",
                }
            )
        if rooms:
            data["rooms"] = rooms

        return data

    def extract_image_urls(
        self, soup: BeautifulSoup, page_url: str
    ) -> list[str]:
        image_urls: list[str] = []
        seen: set[str] = set()

        # HTML comment source (most reliable)
        page_html = str(soup)
        images_match = re.search(r'<!--property-images:"([^"]+)"-->', page_html)
        if images_match:
            for raw in images_match.group(1).split(";"):
                url = raw.strip()
                if url:
                    full = urljoin(page_url, url) if not url.startswith("http") else url
                    if full not in seen:
                        seen.add(full)
                        image_urls.append(full)

        if image_urls:
            return image_urls

        # Fallback: DOM images
        _img_href_re = re.compile(r"\.(jpg|jpeg|png|webp|gif)(\?.*)?$", re.IGNORECASE)
        for img in soup.select(".property-header__visuals__feature__image img"):
            src = img.get("src") or img.get("data-src")
            if src and _img_href_re.search(src):
                full = urljoin(page_url, src) if not src.startswith("http") else src
                if full not in seen:
                    seen.add(full)
                    image_urls.append(full)

        return image_urls


if __name__ == "__main__":
    ReedsRainsScraper.cli_main("rr")
