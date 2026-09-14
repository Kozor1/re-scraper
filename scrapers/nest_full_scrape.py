"""Nest Estate Agents full scrape.

Site: https://nestestateagents.com/sales/

WordPress + PropertyHive (Divi child theme). Listing pages render property
cards server-side (<li class="... type-property ..."> with links to
/property/<slug>/), so plain requests suffice — no Selenium needed.

Pagination is /sales/page/N/.  Pages beyond the last listing return HTTP 200
with an empty archive (no property links), so BaseScraper's "no new links"
stop condition terminates pagination naturally.

Detail pages are standard PropertyHive single-property pages:
    h1.property_title               address / title
    div.price (+ span.price-qualifier)
    div.property_meta table         Ref / Type / Availability / Bedrooms /
                                    Bathrooms / Reception Rooms / Tenure
    div.description-contents p.room first paragraph (no strong.name) is the
                                    main description; the rest are rooms with
                                    <strong class="name"> + <span class="dimension">
    a.propertyhive-main-image       full-res gallery image hrefs
"""

from __future__ import annotations

import os as _os, sys as _sys
_PKG_ROOT = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
_sys.path.insert(0, _PKG_ROOT)


import re

from scrapers.base import BaseScraper
from bs4 import BeautifulSoup
from typing import Any
from urllib.parse import urljoin

# Address suffixes appended by the site to the <title> tag.
_TITLE_SUFFIXES = (
    " | Nest Estate Agents",
    " - Nest Estate Agents",
    " | Nest",
    " - Nest",
)


class NestScraper(BaseScraper):
    def get_listing_url(self, page_num: int) -> str:
        return self.config["listing_page"](page_num)

    def extract_property_links(
        self, soup: BeautifulSoup, page_url: str
    ) -> list[str]:
        links: list[str] = []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if self.config["link_pattern"] in href:
                full = urljoin(page_url, href).split("?")[0].split("#")[0].rstrip("/")
                if full not in links:
                    links.append(full)
        return links

    def scrape_detail_page(
        self, html: str, url: str
    ) -> dict[str, Any] | None:
        soup = BeautifulSoup(html, "html.parser")
        data: dict[str, Any] = {"url": url}

        # ── Address / title ───────────────────────────────────────────────────
        h1 = soup.find("h1", class_="property_title") or soup.find("h1")
        if h1:
            data["address"] = h1.get_text(separator=" ", strip=True)
        if not data.get("address"):
            title_tag = soup.find("title")
            if title_tag:
                t = title_tag.get_text(strip=True)
                for suffix in _TITLE_SUFFIXES:
                    if suffix.lower() in t.lower():
                        t = t[: t.lower().index(suffix.lower())]
                t = t.strip()
                if t:
                    data["address"] = t
        if not data.get("address"):
            self.logger.warning(f"No address found on detail page: {url}")
            return None
        data["title"] = data["address"]

        # ── Price (+ qualifier, e.g. "Offers Over" / "Guide Price") ───────────
        price_div = soup.find("div", class_="price")
        if price_div:
            qual_el = price_div.find("span", class_="price-qualifier")
            qualifier = qual_el.get_text(strip=True) if qual_el else ""
            m = re.search(r"£([\d,]+)", price_div.get_text())
            if m:
                amount = f"£{int(m.group(1).replace(',', '')):,}"
                data["price_str"] = f"{qualifier} {amount}".strip()
            else:
                raw = " ".join(price_div.get_text().split())
                if raw:
                    data["price_str"] = raw  # e.g. "POA"

        # ── Meta table (Ref / Type / Availability / Bedrooms / …) ─────────────
        meta: dict[str, str] = {}
        for row in soup.select("div.property_meta tr"):
            th = row.find("th")
            td = row.find("td")
            if th and td:
                key = th.get_text(strip=True).rstrip(":").strip().lower()
                meta[key] = td.get_text(strip=True)

        if meta.get("bedrooms"):
            data["bedrooms"] = meta["bedrooms"]
        if meta.get("bathrooms"):
            data["bathrooms"] = meta["bathrooms"]
        if meta.get("reception rooms"):
            data["receptions"] = meta["reception rooms"]
        if meta.get("type"):
            data["property_type"] = meta["type"]
        if meta.get("tenure"):
            data["tenure"] = meta["tenure"]
        if meta.get("ref"):
            data["reference"] = meta["ref"]
        # Availability values seen: "For Sale" / "Sale Agreed" — both map
        # cleanly through config.supabase_utils.normalise_status.
        if meta.get("availability"):
            data["status"] = meta["availability"]

        # ── Description + rooms (div.description-contents > p.room) ───────────
        container = soup.find("div", class_="description-contents")
        if container is None:
            container = soup.find("div", class_="description")
        room_paras = container.find_all("p", class_="room") if container else []

        description_parts: list[str] = []
        rooms: list[dict[str, str]] = []

        for p in room_paras:
            name_el = p.find("strong", class_="name")
            if not name_el:
                # Main marketing description — <br> tags separate paragraphs.
                for br in p.find_all("br"):
                    br.replace_with("\n")
                for seg in p.get_text().split("\n"):
                    seg = " ".join(seg.split())
                    if seg:
                        description_parts.append(seg)
                continue

            # Room paragraph: <strong class="name">, <span class="dimension">,
            # then free text after a <br>.
            name = name_el.get_text(strip=True).rstrip(":").strip()
            dim_el = p.find("span", class_="dimension")
            dimensions = dim_el.get_text(strip=True) if dim_el else ""
            name_el.extract()
            if dim_el:
                dim_el.extract()
            for br in p.find_all("br"):
                br.replace_with("\n")
            room_desc = " ".join(p.get_text().split())
            if name:
                rooms.append(
                    {"name": name, "dimensions": dimensions, "description": room_desc}
                )

        if description_parts:
            data["description"] = "\n\n".join(description_parts)
        if rooms:
            data["rooms"] = rooms

        return data

    def extract_image_urls(
        self, soup: BeautifulSoup, page_url: str
    ) -> list[str]:
        image_urls: list[str] = []
        seen: set[str] = set()

        # PropertyHive gallery: full-res URLs on a.propertyhive-main-image.
        # (Floorplan / EPC / brochure links use data-fancybox="floorplans" etc.
        # and do NOT carry this class, so they are excluded automatically.)
        for a in soup.select("a.propertyhive-main-image"):
            href = a.get("href")
            if not href:
                img = a.find("img")
                href = img.get("src") if img else None
            if not href:
                continue
            full = urljoin(page_url, href)
            if full not in seen:
                seen.add(full)
                image_urls.append(full)

        # Fallback: any uploaded media image if the gallery markup changes.
        if not image_urls:
            for img in soup.find_all("img"):
                src = img.get("src") or img.get("data-src")
                if src and "wp-content/uploads" in src:
                    full = urljoin(page_url, src)
                    if full not in seen:
                        seen.add(full)
                        image_urls.append(full)

        return image_urls


if __name__ == "__main__":
    NestScraper.cli_main("nest")
