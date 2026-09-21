from config.sources import (
    SOURCES,
    PROPERTIES_DIR,
    CMSFamily,
    ScrapeStrategy,
    IndexType,
    PaginationFormat,
    PARALLEL_GROUPS,
    NUMBERED_IMAGE_SOURCES,
    GEOCODE_SOURCES,
    MIGRATE_SOURCES,
    HEADERS,
    all_sale_keys,
    sale_only_sources,
)
from config.logging_config import setup_logging, get_logger
from config.supabase_utils import (
    get_supabase,
    build_property_row,
    parse_price_value,
    normalise_status,
    normalise_bedrooms,
    load_geocache,
    upsert_batch,
    delete_batch,
)


# ── Delisting redirect helper ────────────────────────────────────────────────


def redirected_off_page(orig_url: str, resp) -> bool:
    """True if a GET of *orig_url* was redirected to a shallow landing/index
    page — the usual signal that a listing was removed.

    Examples caught: root redirects (michael-chandler, ee) and index-page
    redirects (nest → /search-results). Detail URLs are always deep
    (/property/… or /<slug>/<id>), so a path that *drops depth* to ≤1 segment
    means we were bounced off the listing entirely.
    """
    try:
        if not getattr(resp, "history", None):
            return False
        from urllib.parse import urlparse

        orig = [s for s in urlparse(orig_url).path.split("/") if s]
        final = [s for s in urlparse(resp.url).path.split("/") if s]
        return len(final) < len(orig) and len(final) <= 1
    except Exception:
        return False
