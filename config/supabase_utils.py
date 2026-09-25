"""
config/supabase_utils.py  –  Shared Supabase helpers: row building, normalization, etc.

Import this from migrate_data.py, property_update.py, or any script that needs to
push property data to Supabase.
"""

from __future__ import annotations

import html
import json
import os
import re
import sys
from typing import Any

# ── Supabase client (lazy) ────────────────────────────────────────────────────

_supabase_client: Any = None

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _load_env() -> None:
    """Load .env into os.environ (fallback, only if not already set)."""
    env_path = os.path.join(ROOT, ".env")
    if not os.path.exists(env_path):
        return
    with open(env_path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, val = line.partition("=")
            key = key.strip()
            val = val.strip().strip('"').strip("'")
            if key and not os.environ.get(key):
                os.environ[key] = val


_load_env()


def get_supabase() -> Any:
    """Lazily create and return a Supabase client (service_role key)."""
    global _supabase_client
    if _supabase_client is not None:
        return _supabase_client
    try:
        from supabase import create_client
    except ImportError:
        sys.exit(
            "supabase package not installed. Run: pip install supabase"
        )
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_SERVICE_KEY")
    if not url or not key:
        sys.exit(
            "Set SUPABASE_URL and SUPABASE_SERVICE_KEY in .env or environment"
        )
    _supabase_client = create_client(url, key)
    return _supabase_client


# ── Field normalizers ────────────────────────────────────────────────────────


def parse_price_value(price_str: str | int | None) -> int | None:
    """Extract numeric pound value from a price string."""
    if not price_str:
        return None
    m = re.search(r"£([\d,]+)", str(price_str))
    if m:
        try:
            return int(m.group(1).replace(",", ""))
        except (ValueError, TypeError):
            pass
    try:
        return int(str(price_str))
    except (ValueError, TypeError):
        return None


def normalise_status(raw: Any) -> str:
    """Normalise status string to a canonical form.

    Canonical set: "For Sale", "Sale Agreed", "Under Offer", "Sold",
    "Let", "Let Agreed".

    Handles three classes of input:
    - Exact values ("For Sale", "sale agreed", etc.)
    - Concatenated blobs from listing summary rows ("sale agreed 3 bedrooms
      2 receptions semi-detached") via substring matching
    - Tenure / marketing strings that a parser mistakenly fed in as status
      ("Leasehold", "Freehold", "Chain Free", "New Home", "Land Purchase",
      "Potential Gross Yield...", etc.) — these are not statuses, return
      the safe fallback "For Sale".
    """
    s = (str(raw) if raw else "").strip().lower()
    if not s:
        return "For Sale"

    # Tenure / marketing strings that are NOT sale statuses. These occasionally
    # leak in via the .ListingPage-status element which can show tenure info
    # alongside the actual status. Map to the safe default.
    TENURE_JUNK = (
        "leasehold", "freehold", "chain free", "new home", "land purchase",
        "potential gross yield", "for rent",
    )
    if s in TENURE_JUNK:
        return "For Sale"

    # Exact matches
    if s in ("for sale", "forsale", "sale"):
        return "For Sale"
    if s in ("sale agreed", "saleagreed", "agreed", "status agreed"):
        return "Sale Agreed"
    if s in ("under offer", "underoffer"):
        return "Under Offer"
    if s == "sold":
        return "Sold"
    if s in ("let agreed", "letagreed"):
        return "Let Agreed"
    if s in ("let", "rent"):
        return "Let"

    # Substring matches for concatenated blobs. Order matters because
    # "let agreed" and "sale agreed" both contain "agreed".
    if "sale agreed" in s or "sale  agreed" in s or s.startswith("agreed "):
        return "Sale Agreed"
    if "under offer" in s:
        return "Under Offer"
    if "let agreed" in s or "let  agreed" in s:
        return "Let Agreed"
    if "sold" in s:
        return "Sold"
    if "for sale" in s or s.startswith("sale "):
        return "For Sale"
    # A leading "Agreed N ..." pattern (where N is a digit) is the classic
    # ".SingleListingPage-topEle" blob leak — treat as Sale Agreed.
    if re.match(r"^agreed\s+\d", s):
        return "Sale Agreed"
    if re.match(r"^sale\s+\d", s):
        return "For Sale"

    # Unknown: return the cleaned string so a human can spot it in analytics
    return (str(raw) if raw else "").strip()


def normalise_bedrooms(raw: Any) -> str | None:
    """Normalise bedrooms to a plain integer string."""
    if not raw:
        return None
    m = re.search(r"(\d+)", str(raw))
    return m.group(1) if m else str(raw)


def decode_html_entities(text: Any) -> str:
    """Decode HTML entities in scraped text. Estate agents often leave raw
    entities like & &#39; &nbsp; in their markup; without decoding, those
    render literally in the mobile/web apps (e.g. "O&#39;Brien")."""
    if not isinstance(text, str):
        return ""
    # html.unescape turns &nbsp; into U+00A0 (non-breaking space); convert to
    # a regular space so it's visually identical everywhere and URL-friendly.
    return html.unescape(text).replace("\xa0", " ")


def clean_text_list(items: list[Any]) -> list[str]:
    """Clean a list of features strings: decode entities and drop empties."""
    out: list[str] = []
    for item in items or []:
        s = decode_html_entities(item).strip()
        if s:
            out.append(s)
    return out


# ── Description cleanup ──────────────────────────────────────────────────────

# Legal/compliance boilerplate agents routinely append to property
# descriptions (AML identity-check legalese, Estate Agency Act disclosures,
# etc.). Once any of these phrases appears, everything from the start of that
# sentence onward is boilerplate — marketing copy never follows it.
_LEGAL_MARKERS = re.compile(
    r"(?i)customer\s+due\s+diligence"
    r"|money\s+laundering"
    r"|anti[-\s]?money\s+laundering"
    r"|estate\s+agency\s+act"
    r"|personal\s+interest(?:[^\n]{0,60})?estate\s+agency"
    r"|\baml\s+(check|notice|procedure|requirement)s?\b"
    r"|proof\s+of\s+(?:id|identity)(?:\s+and\s+address)?\s+(?:will\s+be\s+)?required"
    # agent self-marketing pitches tacked onto property blurbs
    # (e.g. Michael Chandler's "To arrange a viewing ... call ... / Thinking of
    # selling ... FREE VALUATION / Mortgage advice is also available ...")
    r"|to\s+arrange\s+(?:an?\s+|your\s+)?viewing[^.\n]{0,80}(?:call|contact|visit)"
    r"|thinking\s+of\s+(?:selling|letting|moving)"
    r"|free\s+(?:no[ -]obligation\s+)?valuation"
    r"|mortgage\s+advice\s+is\s+(?:also\s+)?available"
)


def clean_description(desc: Any) -> str:
    """Strip legal boilerplate tails from a free-text description.

    Agents concatenate compliance notices onto the end of the real blurb —
    sometimes mid-paragraph ("…a superb home. As part of our obligations under
    the Money Laundering …") and sometimes as their own block. Cut at the
    start of the sentence that introduces the legal text, then trim trailing
    whitespace/blank lines.
    """
    if not isinstance(desc, str) or not desc:
        return ""
    m = _LEGAL_MARKERS.search(desc)
    if not m:
        return desc.strip()
    # Find where the containing sentence started: end of previous paragraph,
    # else end of previous sentence, else start of text.
    para_break = desc.rfind("\n\n", 0, m.start())
    sent_break = desc.rfind(". ", 0, m.start())
    cut = max(para_break, sent_break)
    if cut < 0:
        cut = 0  # description IS boilerplate only
    elif sent_break > para_break:
        cut = sent_break + 1  # keep terminating dot of the good sentence
    return desc[:cut].strip()


# ── Row builder ───────────────────────────────────────────────────────────────


def build_property_row(
    source: str,
    source_id: str,
    data: dict[str, Any],
    geocache: dict[str, dict[str, float]] | None = None,
) -> dict[str, Any]:
    """Convert raw scraper JSON data into a Supabase properties row.

    Args:
        source: Source key (e.g. 'sb', 'ups').
        source_id: Local identifier (e.g. 'property_42').
        data: Raw property data dict from the scraper JSON.
        geocache: Optional address → {lat, lng} lookup.

    Returns:
        Dict ready for Supabase upsert.
    """
    geocache = geocache or {}

    address = (data.get("address") or data.get("title") or "").strip()
    # Authoritative coords embedded in the agent's page win over the geocache —
    # geocoders regularly pick a same-named street miles away.
    coords = None
    if data.get("lat") and data.get("lng"):
        coords = {"lat": data["lat"], "lng": data["lng"]}
    if coords is None:
        coords = geocache.get(address)

    # Status from multiple possible locations
    raw_status = (
        data.get("status")
        or (data.get("property_info") or {}).get("Status")
        or (data.get("property_info") or {}).get("status")
        or ""
    )

    prop_type = (
        data.get("property_type")
        or data.get("type")
        or (data.get("property_info") or {}).get("Style")
        or (data.get("property_info") or {}).get("Type")
        or ""
    )

    raw_beds = (
        data.get("bedrooms")
        or (data.get("property_info") or {}).get("Bedrooms")
        or (data.get("property_info") or {}).get("bedrooms")
        or ""
    )

    # Standardize price: prefer price_str, fall back to price
    price_str = data.get("price_str") or data.get("price") or ""
    if isinstance(price_str, int):
        price_str = f"£{price_str:,}"

    # Guard: unpriced "coming soon" listings sometimes carry a placeholder
    # number (rr embeds <!--property-price:"1"-->). Nothing for sale in NI
    # costs under £10k — treat as unpriced so the app renders POA instead of
    # "£1".
    price_val_tmp = parse_price_value(price_str)
    if price_val_tmp is not None and price_val_tmp < 10_000 and not source.endswith("_rent"):
        price_str = ""

    # Standardize image URLs: prefer image_urls, fall back to images
    image_urls = data.get("image_urls") or data.get("images") or []

    listing_type = "rent" if source.endswith("_rent") else "sale"

    row: dict[str, Any] = {
        "source": source,
        "source_id": source_id,
        # Normalise: strip trailing slash so the same listing can't exist
        # twice with /url and /url/ variants.
        "url": (data.get("url") or "").rstrip("/"),
        "address": address,
        "title": data.get("title") or address,
        "price": str(price_str) if price_str else None,
        "price_value": parse_price_value(price_str),
        "status": normalise_status(raw_status),
        "listing_type": listing_type,
        "property_type": prop_type or None,
        "bedrooms": normalise_bedrooms(raw_beds),
        "bathrooms": data.get("bathrooms") or None,
        "receptions": data.get("receptions") or None,
        "epc_rating": decode_html_entities(data.get("epc_rating")) or None,
        "description": clean_description(decode_html_entities(data.get("description")))
        or None,
        "key_features": clean_text_list(data.get("key_features") or []),
        "rooms": data.get("rooms") or [],
        "image_urls": image_urls,
    }

    if coords:
        row["lat"] = coords["lat"]
        row["lng"] = coords["lng"]

    return row


# ── Geocache helpers ─────────────────────────────────────────────────────────


def load_geocache() -> dict[str, dict[str, float]]:
    """Load geocache.json from the project root."""
    geocache_path = os.path.join(ROOT, "geocache.json")
    if os.path.exists(geocache_path):
        try:
            return json.load(open(geocache_path, encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            pass
    return {}


# ── Supabase batch operations ─────────────────────────────────────────────────

BATCH_SIZE = 100


def upsert_batch(rows: list[dict[str, Any]]) -> int:
    """Batch-upsert rows into Supabase properties table. Returns count upserted.

    Conflict target is (source, url) — the URL is the stable identity of a
    listing across scrapes (unlike source_id, which restarts at property_1
    on every fresh scrape). Requires the idx_properties_source_url_unique
    index from migration 004.
    """
    if not rows:
        return 0
    supabase = get_supabase()
    total = 0
    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i : i + BATCH_SIZE]
        try:
            supabase.table("properties").upsert(
                batch, on_conflict="source,url"
            ).execute()
            total += len(batch)
        except Exception as e:
            print(f"  Supabase upsert error (batch {i}–{i + BATCH_SIZE}): {e}")
    return total


def delete_batch(source: str, urls: set[str]) -> int:
    """Delete Supabase rows for a source whose URL is in *urls*. Returns count."""
    if not urls:
        return 0
    supabase = get_supabase()
    url_list = list(urls)
    total = 0
    DELETE_BATCH = 50
    for i in range(0, len(url_list), DELETE_BATCH):
        batch = url_list[i : i + DELETE_BATCH]
        try:
            supabase.table("properties").delete().eq(
                "source", source
            ).in_("url", batch).execute()
            total += len(batch)
        except Exception as e:
            print(f"  Supabase delete error: {e}")
    return total
