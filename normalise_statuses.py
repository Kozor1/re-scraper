"""
normalise_statuses.py — One-off backfill for corrupt `status` values.

The scraper used to concatenated the entire listing summary ("Sale Agreed 3
bedrooms 2 receptions semi-detached") into the `status` column, because the
fallback selector (`.SingleListingPage-topEle`) wraps the whole summary block
and `normalise_status` only did exact matching.

The underlying parsers in `scrapers/base.py` and the `normalise_status`
function in `config/supabase_utils.py` are now both substring-aware. This
script re-normalises existing rows so the database is clean without running
a full scrape.

Usage:
    python3 normalise_statuses.py
"""

from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config.supabase_utils import get_supabase, normalise_status

# Anything whose stored status doesn't match one of these canonical values
# is considered corrupt and will be passed through normalise_status().
CANONICAL = {
    "For Sale",
    "Sale Agreed",
    "Under Offer",
    "Sold",
    "Let",
    "Let Agreed",
}

BATCH = 1000


def fetch_all_noncanonical() -> list[dict]:
    supabase = get_supabase()
    out: list[dict] = []
    offset = 0
    while True:
        r = (
            supabase.table("properties")
            .select("id,status")
            .range(offset, offset + BATCH - 1)
            .execute()
        )
        rows = r.data or []
        if not rows:
            break
        for row in rows:
            status = row.get("status") or ""
            if status not in CANONICAL:
                out.append(row)
        if len(rows) < BATCH:
            break
        offset += BATCH
    return out


def main() -> None:
    supabase = get_supabase()
    print("Scanning properties for non-canonical status values…")
    dirty = fetch_all_noncanonical()
    print(f"  {len(dirty)} rows need re-normalising")

    updated = 0
    errors = 0
    for idx, row in enumerate(dirty, 1):
        new_status = normalise_status(row.get("status"))
        if new_status == row.get("status"):
            continue
        try:
            supabase.table("properties").update(
                {"status": new_status}, returning="minimal"
            ).eq("id", row["id"]).execute()
            updated += 1
        except Exception as e:
            errors += 1
            print(f"  Error id={row['id']}: {e}")
        if idx % 200 == 0 or idx == len(dirty):
            print(f"  Progress: {idx}/{len(dirty)} (updated={updated}, errors={errors})")

    print(f"Done. Normalised {updated} statuses.")


if __name__ == "__main__":
    main()
