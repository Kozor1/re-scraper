"""
decode_existing_entities.py — Backfill: decode HTML entities already stored in Supabase.

The scraper now decodes entities before upserting (via build_property_row), but
rows scraped BEFORE that fix still contain `&` `&#39;` `&nbsp;` etc in their
description / key_features columns. This script walks the properties table,
decodes those fields, and upserts cleaned rows in batches.

Run once after deploying the scraper change:
    python3 decode_existing_entities.py
"""

from __future__ import annotations

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config.supabase_utils import get_supabase, decode_html_entities


def fetch_all_in_batches() -> list[dict]:
    """Fetch all properties using paginated queries (Supabase caps at 1000 by default)."""
    supabase = get_supabase()
    all_rows: list[dict] = []
    batch_size = 1000
    offset = 0
    while True:
        r = (
            supabase.table("properties")
            .select("id, description, key_features")
            .range(offset, offset + batch_size - 1)
            .execute()
        )
        rows = r.data or []
        if not rows:
            break
        all_rows.extend(rows)
        if len(rows) < batch_size:
            break
        offset += batch_size
    return all_rows


def main() -> None:
    supabase = get_supabase()
    print("Fetching all properties…")
    rows = fetch_all_in_batches()
    print(f"  {len(rows)} rows loaded")

    def needs_cleaning(row: dict) -> bool:
        d = row.get("description") or ""
        feats = row.get("key_features") or []
        return ("&" in d) or any("&" in str(f) for f in feats)

    dirty = [r for r in rows if needs_cleaning(r)]
    print(f"  {len(dirty)} rows contain HTML entities to decode")

    if not dirty:
        print("Nothing to do.")
        return

    # The `id` column is GENERATED ALWAYS AS IDENTITY, which means Supabase
    # refuses upserts that include `id` in the payload. We must UPDATE by id
    # instead, which also avoids accidentally inserting zero-row placeholders.
    updated = 0
    errors  = 0
    for idx, r in enumerate(dirty, 1):
        new_desc = decode_html_entities(r["description"]) if r.get("description") else r["description"]
        new_feats = [decode_html_entities(str(f)) for f in (r.get("key_features") or [])]
        try:
            supabase.table("properties").update(
                {"description": new_desc, "key_features": new_feats},
            ).eq("id", r["id"]).execute()
            updated += 1
        except Exception as e:
            errors += 1
            print(f"  Error on id={r['id']}: {e}")
        if idx % 200 == 0 or idx == len(dirty):
            print(f"  Progress: {idx}/{len(dirty)} (updated={updated}, errors={errors})")

    print(f"Done. Decoded {updated} properties.")


if __name__ == "__main__":
    main()
