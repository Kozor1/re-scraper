"""
config/sources.py  –  Single source of truth for all estate agent configurations.

Import this module from any orchestrator or scraper instead of duplicating source
definitions.  Each source entry contains every piece of configuration needed:
URLs, selectors, scrape style, pagination format, and CMS family.

All orchestrator scripts (full_scrape.py, property_update.py,
geocode.py, migrate_data.py) import from here.
"""

from __future__ import annotations

import os
from enum import Enum
from typing import Callable

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PROPERTIES_DIR = os.path.join(ROOT, "properties")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-GB,en;q=0.9",
    "Connection": "keep-alive",
}


# ── Pagination format enum ────────────────────────────────────────────────────


class PaginationFormat(Enum):
    """How listing page URLs are constructed for page 2+."""

    QUERY_PAGE = "?page=N"
    PATH_SLASH_PAGE_N = "/page{N}/"
    PATH_PAGE_DASH_N = "/page-{N}"
    PATH_PAGE_N = "/page/{N}/"
    SEARCH_SLASH_PAGE_N = "/search/1/Page{N}/"
    BANG_SLASH_PAGE = "/!/page/{N}"


# ── CMS families ──────────────────────────────────────────────────────────────


class CMSFamily(Enum):
    """Which CSS selector family a source uses for detail-page parsing."""

    PROPERTYPAL_CLASSIC = "pp_classic"       # ul.dettbl, div.textbp, ul.feats, ul#gallery
    PROPERTYPAL_BLUECUBES = "pp_bluecubes"   # div.prop-det-info-row, div.prop-det-rooms, etc.
    PROPERTYPAL_MODERN = "pp_modern"          # .Price-priceValue, .ListingDescr-text, PropertyPal CDN
    WORDPRESS = "wordpress"                   # WordPress/PropertyHive
    REEDS_RAINS = "reeds_rains"              # HTML comments + proprietary selectors
    CUSTOM_SB = "custom_sb"                   # Simon Brien specific


# ── Scrape strategy ───────────────────────────────────────────────────────────


class ScrapeStrategy(Enum):
    """How detail pages are fetched."""

    REQUESTS = "requests"               # Plain requests (no JS needed)
    SELENIUM = "selenium"               # Headless Chrome (JS-rendered content)
    REQUESTS_SELENIUM_FALLBACK = "requests_selenium_fallback"  # Try requests first, fall back to Selenium


class IndexType(Enum):
    """What local index file format is used."""

    PROPERTY_INDEX = "property_index"    # property_index.json
    URL_MAP = "url_map"                  # url_map.json
    BOTH = "both"                        # Both files maintained


# ── Source registry ───────────────────────────────────────────────────────────


SOURCES: dict[str, dict] = {
    # ── Sales ────────────────────────────────────────────────────────────────
    "sb": {
        "label": "Simon Brien",
        "module": "sb_full_scrape",
        "props_dir": os.path.join(PROPERTIES_DIR, "sb"),
        "base_url": "https://www.simonbrien.com",
        "cms": CMSFamily.CUSTOM_SB,
        "strategy": ScrapeStrategy.REQUESTS,
        "index_type": IndexType.PROPERTY_INDEX,
        "download_images": False,
        "link_pattern": "/buy/",
        "fresh_flag": True,
        "parallel_group": 1,
        "quick_type": "legacy",
        "quick_list_urls": [
            ("https://www.simonbrien.com/property-for-sale",
             "https://www.simonbrien.com/property-for-sale/page{N}/?orderBy="),
        ],
        "listing_page": lambda n: (
            "https://www.simonbrien.com/property-for-sale"
            if n == 1
            else f"https://www.simonbrien.com/property-for-sale/page{n}/?orderBy="
        ),
        "pagination_format": PaginationFormat.PATH_SLASH_PAGE_N,
    },
    "ups": {
        "label": "Ulster Property Sales",
        "module": "ups_full_scrape",
        "props_dir": os.path.join(PROPERTIES_DIR, "ups"),
        "base_url": "https://www.ulsterpropertysales.co.uk",
        "cms": CMSFamily.PROPERTYPAL_BLUECUBES,
        "strategy": ScrapeStrategy.REQUESTS,
        "index_type": IndexType.PROPERTY_INDEX,
        "download_images": False,
        "link_pattern": "/property/",
        "fresh_flag": True,
        "parallel_group": 1,
        "quick_type": "legacy",
        "quick_list_urls": [
            ("https://www.ulsterpropertysales.co.uk/property-for-sale",
             "https://www.ulsterpropertysales.co.uk/property-for-sale/page{N}/"),
        ],
        "listing_page": lambda n: f"https://www.ulsterpropertysales.co.uk/property-for-sale/page{n}/",
        "pagination_format": PaginationFormat.PATH_SLASH_PAGE_N,
    },
    "hc": {
        "label": "Hunter Campbell",
        "module": "hc_full_scrape",
        "props_dir": os.path.join(PROPERTIES_DIR, "hc"),
        "base_url": "https://www.huntercampbell.co.uk",
        "cms": CMSFamily.PROPERTYPAL_CLASSIC,
        "strategy": ScrapeStrategy.REQUESTS,
        "index_type": IndexType.PROPERTY_INDEX,
        "download_images": False,
        "link_pattern": "/property/",
        "fresh_flag": True,
        "parallel_group": 1,
        "quick_type": "legacy",
        "quick_list_urls": [
            ("https://www.huntercampbell.co.uk/property-for-sale",
             "https://www.huntercampbell.co.uk/property-for-sale/page{N}/"),
        ],
        "listing_page": lambda n: (
            "https://www.huntercampbell.co.uk/residential-sales"
            if n == 1
            else f"https://www.huntercampbell.co.uk/residential-sales?page={n}"
        ),
        "pagination_format": PaginationFormat.QUERY_PAGE,
    },
    "jm": {
        "label": "John Minnis",
        "module": "jm_full_scrape",
        "props_dir": os.path.join(PROPERTIES_DIR, "jm"),
        "base_url": "https://www.johnminnis.co.uk",
        "cms": CMSFamily.PROPERTYPAL_BLUECUBES,
        "strategy": ScrapeStrategy.REQUESTS,
        "index_type": IndexType.PROPERTY_INDEX,
        "download_images": False,
        "link_pattern": "/property/",
        "fresh_flag": True,
        "parallel_group": 1,
        "quick_type": "legacy",
        "quick_list_urls": [
            ("https://www.johnminnis.co.uk/search/906207/page1/",
             "https://www.johnminnis.co.uk/search/906207/page{N}/"),
        ],
        "listing_page": lambda n: f"https://www.johnminnis.co.uk/search/906207/page{n}/",
        "pagination_format": PaginationFormat.PATH_SLASH_PAGE_N,
    },
    "pp": {
        "label": "Property People NI",
        "module": "pp_full_scrape",
        "props_dir": os.path.join(PROPERTIES_DIR, "pp"),
        "base_url": "https://www.propertypeopleni.com",
        "cms": CMSFamily.PROPERTYPAL_CLASSIC,
        "strategy": ScrapeStrategy.REQUESTS,
        "index_type": IndexType.PROPERTY_INDEX,
        "download_images": False,
        "link_pattern": "/property/",
        "fresh_flag": True,
        "parallel_group": 1,
        "quick_type": "legacy",
        "quick_list_urls": [
            ("https://www.propertypeopleni.com/property-for-sale/page1/",
             "https://www.propertypeopleni.com/property-for-sale/page{N}/"),
        ],
        "listing_page": lambda n: f"https://www.propertypeopleni.com/property-for-sale/page{n}/",
        "pagination_format": PaginationFormat.PATH_SLASH_PAGE_N,
    },
    "dh": {
        "label": "Daniel Henry",
        "module": "dh_scrape",
        "props_dir": os.path.join(PROPERTIES_DIR, "dh"),
        "base_url": "https://www.danielhenry.co.uk",
        "cms": CMSFamily.PROPERTYPAL_BLUECUBES,
        "strategy": ScrapeStrategy.REQUESTS_SELENIUM_FALLBACK,
        "index_type": IndexType.URL_MAP,
        "download_images": False,
        "link_pattern": "/property/",
        "fresh_flag": True,
        "rent_only": False,
        "parallel_group": 1,
        "quick_type": "legacy",
        "quick_list_urls": [
            ("https://www.danielhenry.co.uk/property-for-sale",
             "https://www.danielhenry.co.uk/property-for-sale/page{N}/"),
        ],
        "listing_page": lambda n: (
            "https://www.danielhenry.co.uk/property-for-sale"
            if n == 1
            else f"https://www.danielhenry.co.uk/property-for-sale?page={n}"
        ),
        "pagination_format": PaginationFormat.QUERY_PAGE,
    },
    "pinp": {
        "label": "Pinpoint Property",
        "module": "pinp_full_scrape",
        "props_dir": os.path.join(PROPERTIES_DIR, "pinp"),
        "base_url": "https://pinpointproperty.com",
        "cms": CMSFamily.PROPERTYPAL_BLUECUBES,
        "strategy": ScrapeStrategy.REQUESTS,
        "index_type": IndexType.URL_MAP,
        "download_images": True,
        "link_pattern": "/property/",
        "fresh_flag": True,
        "rent_only": False,
        "parallel_group": 1,
        "quick_type": "smart",
        "quick_script": os.path.join(ROOT, "scrapers", "pinp_full_scrape.py"),
        "listing_page": lambda n: (
            "https://pinpointproperty.com/property-for-sale"
            if n == 1
            else f"https://pinpointproperty.com/property-for-sale/page{n}/"
        ),
        "pagination_format": PaginationFormat.PATH_SLASH_PAGE_N,
    },
    "rb": {
        "label": "Rodgers & Browne",
        "module": "rb_full_scrape",
        "props_dir": os.path.join(PROPERTIES_DIR, "rb"),
        "base_url": "https://www.rodgersandbrowne.co.uk",
        "cms": CMSFamily.PROPERTYPAL_BLUECUBES,
        "strategy": ScrapeStrategy.REQUESTS,
        "index_type": IndexType.URL_MAP,
        "download_images": True,
        "link_pattern": "/property/",
        "fresh_flag": True,
        "rent_only": False,
        "parallel_group": 1,
        "quick_type": "smart",
        "quick_script": os.path.join(ROOT, "scrapers", "rb_full_scrape.py"),
        "listing_page": lambda n: (
            "https://www.rodgersandbrowne.co.uk/property-for-sale"
            if n == 1
            else f"https://www.rodgersandbrowne.co.uk/property-for-sale/page{n}/"
        ),
        "pagination_format": PaginationFormat.PATH_SLASH_PAGE_N,
    },
    "tr": {
        "label": "Templeton Robinson",
        "module": "tr_full_scrape",
        "props_dir": os.path.join(PROPERTIES_DIR, "tr"),
        "base_url": "https://www.templetonrobinson.com",
        "cms": CMSFamily.PROPERTYPAL_BLUECUBES,
        "strategy": ScrapeStrategy.SELENIUM,
        "index_type": IndexType.PROPERTY_INDEX,
        "download_images": False,
        "link_pattern": "/property/",
        "fresh_flag": True,
        "parallel_group": 2,
        "quick_type": "legacy",
        "quick_list_urls": [
            ("https://www.templeton-robinson.co.uk/property-for-sale",
             "https://www.templeton-robinson.co.uk/property-for-sale/page{N}/"),
        ],
        "listing_page": lambda n: f"https://www.templetonrobinson.com/property-for-sale/page{n}/",
        "pagination_format": PaginationFormat.PATH_SLASH_PAGE_N,
    },
    "mm": {
        "label": "McMillan McClure",
        "module": "mm_full_scrape",
        "props_dir": os.path.join(PROPERTIES_DIR, "mm"),
        "base_url": "https://www.mcmillanmcclure.com",
        "cms": CMSFamily.PROPERTYPAL_MODERN,
        "strategy": ScrapeStrategy.REQUESTS,
        "index_type": IndexType.URL_MAP,
        "download_images": False,
        # Property URLs follow /<slug>/<numeric-id> (e.g. /12-hillview-drive-newtownabbey/1095700)
        # — NOT /property/ like most PropertyPal sites. Match with regex.
        "link_pattern": r"^/[a-z0-9-]+/\d{6,}/?$",
        "link_pattern_is_regex": True,
        "fresh_flag": True,
        "rent_only": False,
        "parallel_group": 1,
        "quick_type": "smart",
        "quick_script": os.path.join(ROOT, "scrapers", "mm_full_scrape.py"),
        # Listing pages are server-rendered plain HTML — no JS needed.
        # Pagination is /property-for-sale/page-{n} (1-indexed, page 1 = no suffix).
        # We deliberately do NOT request sta=saleAgreed / sta=sold — those
        # statuses shouldn't enter the DB in the first place (they're filtered
        # out of the browse feed anyway).
        "listing_page": lambda n: (
            "https://www.mcmillanmcclure.com/property-for-sale"
            if n <= 1
            else f"https://www.mcmillanmcclure.com/property-for-sale/page-{n}"
        ),
        "pagination_format": PaginationFormat.PATH_PAGE_DASH_N,
    },
    "ce": {
        "label": "Country Estates",
        "module": "ce_full_scrape",
        "props_dir": os.path.join(PROPERTIES_DIR, "ce"),
        "base_url": "https://www.country-estates.com",
        "cms": CMSFamily.PROPERTYPAL_MODERN,
        "strategy": ScrapeStrategy.SELENIUM,
        "index_type": IndexType.URL_MAP,
        "download_images": False,
        "link_pattern": "/property/",
        "fresh_flag": True,
        "rent_only": False,
        "parallel_group": 2,
        "quick_type": "smart",
        "quick_script": os.path.join(ROOT, "scrapers", "ce_full_scrape.py"),
        "listing_page": lambda n: (
            "https://www.country-estates.com/property-for-sale"
            if n == 1
            else f"https://www.country-estates.com/property-for-sale/page-{n}"
        ),
        "pagination_format": PaginationFormat.PATH_PAGE_DASH_N,
    },
    "gm": {
        "label": "Gareth Mills Est. Agents",
        "module": "gm_full_scrape",
        "props_dir": os.path.join(PROPERTIES_DIR, "gm"),
        "base_url": "https://www.garethmillesstateagents.co.uk",
        "cms": CMSFamily.PROPERTYPAL_MODERN,
        "strategy": ScrapeStrategy.SELENIUM,
        "index_type": IndexType.URL_MAP,
        "download_images": False,
        "link_pattern": "/property/",
        "fresh_flag": True,
        "rent_only": False,
        "parallel_group": 2,
        "quick_type": "smart",
        "quick_script": os.path.join(ROOT, "scrapers", "gm_full_scrape.py"),
        "listing_page": lambda n: (
            "https://www.garethmillesstateagents.co.uk/property-for-sale"
            if n == 1
            else f"https://www.garethmillesstateagents.co.uk/property-for-sale/page-{n}"
        ),
        "pagination_format": PaginationFormat.PATH_PAGE_DASH_N,
    },
    # ── New agents (batch 2025-04) ───────────────────────────────────────────
    "mc": {
        "label": "Michael Chandler",
        "module": "mc_full_scrape",
        "props_dir": os.path.join(PROPERTIES_DIR, "mc"),
        "base_url": "https://www.michael-chandler.co.uk",
        "cms": CMSFamily.PROPERTYPAL_CLASSIC,
        "strategy": ScrapeStrategy.REQUESTS,
        "index_type": IndexType.PROPERTY_INDEX,
        "download_images": False,
        "link_pattern": "/property/",
        "fresh_flag": True,
        "parallel_group": 1,
        "listing_page": lambda n: (
            "https://www.michael-chandler.co.uk/property-for-sale"
            if n == 1
            else f"https://www.michael-chandler.co.uk/property-for-sale?page={n}"
        ),
        "pagination_format": PaginationFormat.QUERY_PAGE,
    },
    "ft": {
        "label": "Fetherstons",
        "module": "ft_full_scrape",
        "props_dir": os.path.join(PROPERTIES_DIR, "ft"),
        "base_url": "https://www.fetherstons.com",
        "cms": CMSFamily.PROPERTYPAL_CLASSIC,
        "strategy": ScrapeStrategy.REQUESTS,
        "index_type": IndexType.PROPERTY_INDEX,
        "download_images": False,
        "link_pattern": "/property/",
        "fresh_flag": True,
        "parallel_group": 1,
        "listing_page": lambda n: (
            "https://www.fetherstons.com/property-for-sale"
            if n == 1
            else f"https://www.fetherstons.com/property-for-sale?page={n}"
        ),
        "pagination_format": PaginationFormat.QUERY_PAGE,
    },
    "pr": {
        "label": "Peter Rodgers",
        "module": "pr_full_scrape",
        "props_dir": os.path.join(PROPERTIES_DIR, "pr"),
        "base_url": "https://www.peterrogersestateagents.com",
        "cms": CMSFamily.PROPERTYPAL_CLASSIC,
        "strategy": ScrapeStrategy.REQUESTS,
        "index_type": IndexType.PROPERTY_INDEX,
        "download_images": False,
        "link_pattern": "/property/",
        "fresh_flag": True,
        "parallel_group": 1,
        "listing_page": lambda n: (
            "https://www.peterrogersestateagents.com/property-for-sale"
            if n == 1
            else f"https://www.peterrogersestateagents.com/property-for-sale?page={n}"
        ),
        "pagination_format": PaginationFormat.QUERY_PAGE,
    },
    "cps": {
        "label": "CPS",
        "module": "cps_full_scrape",
        "props_dir": os.path.join(PROPERTIES_DIR, "cps"),
        "base_url": "https://cps-property.com",
        "cms": CMSFamily.PROPERTYPAL_CLASSIC,
        "strategy": ScrapeStrategy.REQUESTS,
        "index_type": IndexType.PROPERTY_INDEX,
        "download_images": False,
        "link_pattern": "/property/",
        "fresh_flag": True,
        "parallel_group": 1,
        "listing_page": lambda n: (
            "https://cps-property.com/property-for-sale"
            if n == 1
            else f"https://cps-property.com/property-for-sale?page={n}"
        ),
        "pagination_format": PaginationFormat.QUERY_PAGE,
    },
    "hn": {
        "label": "Hannath",
        "module": "hn_full_scrape",
        "props_dir": os.path.join(PROPERTIES_DIR, "hn"),
        "base_url": "https://www.hannath.com",
        "cms": CMSFamily.PROPERTYPAL_CLASSIC,
        "strategy": ScrapeStrategy.REQUESTS,
        "index_type": IndexType.PROPERTY_INDEX,
        "download_images": False,
        "link_pattern": "/property/",
        "fresh_flag": True,
        "parallel_group": 1,
        "listing_page": lambda n: (
            "https://www.hannath.com/all-sale-properties"
            if n == 1
            else f"https://www.hannath.com/all-sale-properties?page={n}"
        ),
        "pagination_format": PaginationFormat.QUERY_PAGE,
    },
    "bt": {
        "label": "Brian Todd",
        "module": "bt_full_scrape",
        "props_dir": os.path.join(PROPERTIES_DIR, "bt"),
        "base_url": "https://www.briantodd.co.uk",
        "cms": CMSFamily.PROPERTYPAL_CLASSIC,
        "strategy": ScrapeStrategy.REQUESTS,
        "index_type": IndexType.PROPERTY_INDEX,
        "download_images": False,
        "link_pattern": "/property/",
        "fresh_flag": True,
        "parallel_group": 1,
        "listing_page": lambda n: (
            "https://www.briantodd.co.uk/sale"
            if n == 1
            else f"https://www.briantodd.co.uk/search/1/Page{n}/"
        ),
        "pagination_format": PaginationFormat.SEARCH_SLASH_PAGE_N,
    },
    "rr": {
        "label": "Reeds Rains",
        "module": "rr_full_scrape",
        "props_dir": os.path.join(PROPERTIES_DIR, "rr"),
        "base_url": "https://www.reedsrains.co.uk",
        "cms": CMSFamily.REEDS_RAINS,
        "strategy": ScrapeStrategy.REQUESTS,
        "index_type": IndexType.BOTH,
        "download_images": False,
        "link_pattern": "/property/",
        "fresh_flag": True,
        "parallel_group": 1,
        "listing_page": lambda n: (
            "https://www.reedsrains.co.uk/properties-for-sale/northern-ireland"
            if n == 1
            else f"https://www.reedsrains.co.uk/properties-for-sale/northern-ireland/!/page/{n}"
        ),
        "pagination_format": PaginationFormat.BANG_SLASH_PAGE,
    },
    "ee": {
        "label": "Edmonton Estates",
        "module": "ee_full_scrape",
        "props_dir": os.path.join(PROPERTIES_DIR, "ee"),
        "base_url": "https://www.edmondsonestates.co.uk",
        "cms": CMSFamily.PROPERTYPAL_CLASSIC,
        "strategy": ScrapeStrategy.REQUESTS,
        "index_type": IndexType.PROPERTY_INDEX,
        "download_images": False,
        "link_pattern": "/property/",
        "fresh_flag": True,
        "parallel_group": 1,
        "listing_page": lambda n: (
            "https://www.edmondsonestates.co.uk/property-for-sale"
            if n == 1
            else f"https://www.edmondsonestates.co.uk/property-for-sale?page={n}"
        ),
        "pagination_format": PaginationFormat.QUERY_PAGE,
    },
    "ag": {
        "label": "Armstrong Gordon",
        "module": "ag_full_scrape",
        "props_dir": os.path.join(PROPERTIES_DIR, "ag"),
        "base_url": "https://www.armstronggordon.com",
        "cms": CMSFamily.PROPERTYPAL_CLASSIC,
        "strategy": ScrapeStrategy.REQUESTS,
        "index_type": IndexType.PROPERTY_INDEX,
        "download_images": False,
        "link_pattern": "/property/",
        "fresh_flag": True,
        "parallel_group": 1,
        "listing_page": lambda n: (
            "https://www.armstronggordon.com/property-for-sale"
            if n == 1
            else f"https://www.armstronggordon.com/property-for-sale?page={n}"
        ),
        "pagination_format": PaginationFormat.QUERY_PAGE,
    },
    "ta": {
        "label": "The Agent",
        "module": "ta_full_scrape",
        "props_dir": os.path.join(PROPERTIES_DIR, "ta"),
        "base_url": "https://www.theagentni.com",
        "cms": CMSFamily.PROPERTYPAL_CLASSIC,
        "strategy": ScrapeStrategy.REQUESTS,
        "index_type": IndexType.PROPERTY_INDEX,
        "download_images": False,
        "link_pattern": "/property/",
        "fresh_flag": True,
        "parallel_group": 1,
        "listing_page": lambda n: (
            "https://www.theagentni.com/property-for-sale"
            if n == 1
            else f"https://www.theagentni.com/property-for-sale?page={n}"
        ),
        "pagination_format": PaginationFormat.QUERY_PAGE,
    },
    "abc": {
        "label": "A Barton Company",
        "module": "abc_full_scrape",
        "props_dir": os.path.join(PROPERTIES_DIR, "abc"),
        "base_url": "https://www.abartoncompany.co.uk",
        "cms": CMSFamily.PROPERTYPAL_CLASSIC,
        "strategy": ScrapeStrategy.REQUESTS,
        "index_type": IndexType.PROPERTY_INDEX,
        "download_images": False,
        "link_pattern": "/property/",
        "fresh_flag": True,
        "parallel_group": 1,
        "listing_page": lambda n: (
            "https://www.abartoncompany.co.uk/property-for-sale"
            if n == 1
            else f"https://www.abartoncompany.co.uk/property-for-sale?page={n}"
        ),
        "pagination_format": PaginationFormat.QUERY_PAGE,
    },
    "hg": {
        "label": "Henry Graham",
        "module": "hg_full_scrape",
        "props_dir": os.path.join(PROPERTIES_DIR, "hg"),
        "base_url": "https://www.hgraham.co.uk",
        "cms": CMSFamily.PROPERTYPAL_CLASSIC,
        "strategy": ScrapeStrategy.REQUESTS,
        "index_type": IndexType.PROPERTY_INDEX,
        "download_images": False,
        "link_pattern": "/property/",
        "fresh_flag": True,
        "parallel_group": 1,
        "listing_page": lambda n: (
            "https://www.hgraham.co.uk/residential-sales"
            if n == 1
            else f"https://www.hgraham.co.uk/residential-sales?page={n}"
        ),
        "pagination_format": PaginationFormat.QUERY_PAGE,
    },
    "le": {
        "label": "Lennon Estates",
        "module": "le_full_scrape",
        "props_dir": os.path.join(PROPERTIES_DIR, "le"),
        "base_url": "https://lennon-estates.com",
        "cms": CMSFamily.PROPERTYPAL_CLASSIC,
        "strategy": ScrapeStrategy.REQUESTS,
        "index_type": IndexType.PROPERTY_INDEX,
        "download_images": False,
        "link_pattern": "/properties/",
        "fresh_flag": True,
        "parallel_group": 1,
        "listing_page": lambda n: (
            "https://lennon-estates.com/properties/for-sale/"
            if n == 1
            else f"https://lennon-estates.com/properties/for-sale/page/{n}/"
        ),
        "pagination_format": PaginationFormat.PATH_PAGE_N,
    },
    "amd": {
        "label": "Agar Murdoch and Deane",
        "module": "amd_full_scrape",
        "props_dir": os.path.join(PROPERTIES_DIR, "amd"),
        "base_url": "https://www.agarmurdochdeane.com",
        "cms": CMSFamily.PROPERTYPAL_CLASSIC,
        "strategy": ScrapeStrategy.REQUESTS,
        "index_type": IndexType.PROPERTY_INDEX,
        "download_images": False,
        "link_pattern": "/property/",
        "fresh_flag": True,
        "parallel_group": 1,
        "listing_page": lambda n: (
            "https://www.agarmurdochdeane.com/property-for-sale"
            if n == 1
            else f"https://www.agarmurdochdeane.com/property-for-sale?page={n}"
        ),
        "pagination_format": PaginationFormat.QUERY_PAGE,
    },
    "tm": {
        "label": "Tim Martin",
        "module": "tm_full_scrape",
        "props_dir": os.path.join(PROPERTIES_DIR, "tm"),
        "base_url": "https://www.timmartin.co.uk",
        "cms": CMSFamily.PROPERTYPAL_CLASSIC,
        "strategy": ScrapeStrategy.REQUESTS,
        "index_type": IndexType.PROPERTY_INDEX,
        "download_images": False,
        "link_pattern": "/properties/",
        "fresh_flag": True,
        "parallel_group": 1,
        "listing_page": lambda n: (
            "https://www.timmartin.co.uk/properties-for-sale"
            if n == 1
            else f"https://www.timmartin.co.uk/properties-for-sale?page={n}"
        ),
        "pagination_format": PaginationFormat.QUERY_PAGE,
    },
    "ma": {
        "label": "McAllister",
        "module": "ma_full_scrape",
        "props_dir": os.path.join(PROPERTIES_DIR, "ma"),
        "base_url": "https://www.mc-allister.co.uk",
        "cms": CMSFamily.PROPERTYPAL_CLASSIC,
        "strategy": ScrapeStrategy.REQUESTS,
        "index_type": IndexType.PROPERTY_INDEX,
        "download_images": False,
        "link_pattern": "/property/",
        "fresh_flag": True,
        "parallel_group": 1,
        "listing_page": lambda n: (
            "https://www.mc-allister.co.uk/property-for-sale"
            if n == 1
            else f"https://www.mc-allister.co.uk/property-for-sale?page={n}"
        ),
        "pagination_format": PaginationFormat.QUERY_PAGE,
    },
    "dl": {
        "label": "Dallas",
        "module": "dl_full_scrape",
        "props_dir": os.path.join(PROPERTIES_DIR, "dl"),
        "base_url": "https://www.dallasre.co.uk",
        "cms": CMSFamily.PROPERTYPAL_CLASSIC,
        "strategy": ScrapeStrategy.REQUESTS,
        "index_type": IndexType.PROPERTY_INDEX,
        "download_images": False,
        "link_pattern": "/property/",
        "fresh_flag": True,
        "parallel_group": 1,
        "listing_page": lambda n: (
            "https://www.dallasre.co.uk/homes-for-sale.php"
            if n == 1
            else f"https://www.dallasre.co.uk/homes-for-sale.php?p={n}"
        ),
        "pagination_format": PaginationFormat.QUERY_PAGE,
    },
    "bmc": {
        "label": "Bill McCann",
        "module": "bmc_full_scrape",
        "props_dir": os.path.join(PROPERTIES_DIR, "bmc"),
        "base_url": "https://billmccann.com",
        "cms": CMSFamily.WORDPRESS,
        "strategy": ScrapeStrategy.REQUESTS,
        "index_type": IndexType.PROPERTY_INDEX,
        "download_images": False,
        "link_pattern": "/property/",
        "fresh_flag": True,
        "parallel_group": 1,
        "listing_page": lambda n: (
            "https://billmccann.com/for-sale/"
            if n == 1
            else f"https://billmccann.com/for-sale/page/{n}/"
        ),
        "pagination_format": PaginationFormat.PATH_PAGE_N,
    },
    "ag2": {
        "label": "Andrews & Gregg",
        "module": "ag2_full_scrape",
        "props_dir": os.path.join(PROPERTIES_DIR, "ag2"),
        "base_url": "https://www.andrewsandgregg.com",
        "cms": CMSFamily.PROPERTYPAL_CLASSIC,
        "strategy": ScrapeStrategy.REQUESTS,
        "index_type": IndexType.PROPERTY_INDEX,
        "download_images": False,
        "link_pattern": "/property/",
        "fresh_flag": True,
        "parallel_group": 1,
        "listing_page": lambda n: (
            "https://www.andrewsandgregg.com/properties.aspx?mode=0&showsearch=1&commercial=0&menuID=30"
            if n == 1
            else f"https://www.andrewsandgregg.com/properties.aspx?mode=0&showsearch=1&commercial=0&menuID=30&page={n}"
        ),
        "pagination_format": PaginationFormat.QUERY_PAGE,
    },
    "ipe": {
        "label": "Independent Property Estates",
        "module": "ipe_full_scrape",
        "props_dir": os.path.join(PROPERTIES_DIR, "ipe"),
        "base_url": "https://ipestates.co.uk",
        "cms": CMSFamily.PROPERTYPAL_CLASSIC,
        "strategy": ScrapeStrategy.REQUESTS,
        "index_type": IndexType.PROPERTY_INDEX,
        "download_images": False,
        "link_pattern": "/property/",
        "fresh_flag": True,
        "parallel_group": 1,
        "listing_page": lambda n: (
            "https://ipestates.co.uk/property-for-sale"
            if n == 1
            else f"https://ipestates.co.uk/property-for-sale?page={n}"
        ),
        "pagination_format": PaginationFormat.QUERY_PAGE,
    },
    "mmc": {
        "label": "Montgomery & McCleary",
        "module": "mmc_full_scrape",
        "props_dir": os.path.join(PROPERTIES_DIR, "mmc"),
        "base_url": "https://www.montgomerymccleery.com",
        "cms": CMSFamily.PROPERTYPAL_CLASSIC,
        "strategy": ScrapeStrategy.REQUESTS,
        "index_type": IndexType.PROPERTY_INDEX,
        "download_images": False,
        "link_pattern": "/property/",
        "fresh_flag": True,
        "parallel_group": 1,
        "listing_page": lambda n: (
            "https://www.montgomerymccleery.com/property-for-sale"
            if n == 1
            else f"https://www.montgomerymccleery.com/property-for-sale?page={n}"
        ),
        "pagination_format": PaginationFormat.QUERY_PAGE,
    },
    "pe": {
        "label": "Pauline Elliott",
        "module": "pe_full_scrape",
        "props_dir": os.path.join(PROPERTIES_DIR, "pe"),
        "base_url": "https://www.paulineelliottestateagents.com",
        "cms": CMSFamily.PROPERTYPAL_CLASSIC,
        "strategy": ScrapeStrategy.REQUESTS,
        "index_type": IndexType.PROPERTY_INDEX,
        "download_images": False,
        "link_pattern": "/property/",
        "fresh_flag": True,
        "parallel_group": 1,
        "listing_page": lambda n: (
            "https://www.paulineelliottestateagents.com/property-for-sale"
            if n == 1
            else f"https://www.paulineelliottestateagents.com/property-for-sale?page={n}"
        ),
        "pagination_format": PaginationFormat.QUERY_PAGE,
    },
    "nest": {
        "label": "Nest Estate Agents",
        "module": "nest_full_scrape",
        "props_dir": os.path.join(PROPERTIES_DIR, "nest"),
        "base_url": "https://nestestateagents.com",
        "cms": CMSFamily.WORDPRESS,
        "strategy": ScrapeStrategy.REQUESTS,
        "index_type": IndexType.PROPERTY_INDEX,
        "download_images": False,
        "link_pattern": "/property/",
        "fresh_flag": True,
        "parallel_group": 1,
        "listing_page": lambda n: (
            "https://nestestateagents.com/sales/"
            if n == 1
            else f"https://nestestateagents.com/sales/page/{n}/"
        ),
        "pagination_format": PaginationFormat.PATH_PAGE_N,
    },
}


# ── Parallel groups (used by full_scrape.py) ──────────────────────────────────

PARALLEL_GROUPS: list[list[str]] = [
    [
        k for k, v in SOURCES.items() if v.get("parallel_group") == 1
    ],
    [
        k for k, v in SOURCES.items() if v.get("parallel_group") == 2
    ],
]


# ── Sources with numbered image suffixes (for image_sort_utils) ───────────────

NUMBERED_IMAGE_SOURCES = ["hc", "jm", "tr", "ups", "pp", "dh"]


# ── Sale-only sources (for rental filtering) ──────────────────────────────────

# Sources that only have sale listings (no rental counterpart on the site).
# Sources that DO have rental listings set the flag to False so they get
# filtered more carefully (rental URLs are excluded during listing walks).
def sale_only_sources() -> list[str]:
    """Return source keys that are sale-only (no rental listings on site).

    Defaults to True — most sources only have sale listings.  Sources that
    also list rentals (mm, ce, gm, pinp, rb, dh) have the flag set to False
    so rental URLs can be filtered out.
    """
    return [k for k, v in SOURCES.items() if v.get("rent_only", True)]


def all_sale_keys() -> list[str]:
    """Return all source keys, excluding any _rent suffixed ones."""
    return [k for k in SOURCES if not k.endswith("_rent")]


# ── Source keys that geocode.py handles (includes rental variants) ────────────

GEOCODE_SOURCES: dict[str, str] = {
    k: os.path.relpath(v["props_dir"], ROOT)
    for k, v in SOURCES.items()
}


# ── Source keys that migrate_data.py handles ──────────────────────────────────

MIGRATE_SOURCES: dict[str, str] = {
    k: os.path.relpath(v["props_dir"], ROOT)
    for k, v in SOURCES.items()
}
