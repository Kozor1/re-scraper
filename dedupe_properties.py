#!/usr/bin/env python3
"""
dedupe_properties.py — One-off cleanup of duplicate `properties` rows.

Background:
  - The `properties` table has a uniqueness constraint on (source, source_id).
  - Scrapers assign incrementing `source_id`s (property_1, property_2, ...) which
    restart at 1 on every fresh scrape — so the same physical listing gets a new
    source_id each time, defeating the upsert and producing duplicates.
  - The true stable key for a property is (source, url).

This script:
  1. Fetches all rows.
  2. Groups by (source, url).
  3. For each group with >1 rows, picks a "best" row using:
       - prefer rows with price_value not null
       - then prefer longer description
       - then prefer most recently updated_at
  4. Deletes the other rows in batches of 200 ids.

Run with --dry-run first to see what would happen.
"""

from __future__ import annotations

import argparse
import os
import sys
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config.supabase_utils import get_supabase


def fetch_all() -> list[dict]:
    sb = get_supabase()
    all_rows: list[dict] = []
    offset = 0
    while True:
        r = (
            sb.table("properties")
            .select(
                "id, source, source_id, url, price, price_value, description,"
                "image_urls, key_features, status, updated_at"
            )
            .range(offset, offset + 999)
            .execute()
        )
        rows = r.data or []
        if not rows:
            break
        all_rows.extend(rows)
        if len(rows) < 1000:
            break
        offset += 1000
    return all_rows


def score(row: dict) -> tuple:
    """Higher is better."""
    has_price = 1 if row.get("price_value") is not None else 0
    desc_len = len(row.get("description") or "")
    n_imgs = len(row.get("image_urls") or [])
    n_feats = len(row.get("key_features") or [])
    updated = row.get("updated_at") or ""
    return (has_price, desc_len, n_imgs, n_feats, updated)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true",
                        help="Just report; don't delete anything.")
    args = parser.parse_args()

    print("Fetching all properties...")
    rows = fetch_all()
    print(f"  {len(rows)} total rows")

    by_key: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for r in rows:
        if not r.get("url"):
            continue
        by_key[(r["source"], r["url"])].append(r)

    dupes = {k: v for k, v in by_key.items() if len(v) > 1}
    print(f"  {len(dupes)} (source,url) pairs have duplicates")

    to_delete: list[int] = []
    for key, group in dupes.items():
        # Sort descending by score; keep the first (best)
        group.sort(key=score, reverse=True)
        for stale in group[1:]:
            to_delete.append(stale["id"])

    print(f"  Marking {len(to_delete)} stale rows for deletion.")

    if args.dry_run:
        print("Dry run — no deletions performed.")
        return

    sb = get_supabase()
    BATCH = 200
    total = 0
    for i in range(0, len(to_delete), BATCH):
        batch = to_delete[i : i + BATCH]
        try:
            sb.table("properties").delete().in_("id", batch).execute()
            total += len(batch)
            print(f"  Deleted {total}/{len(to_delete)}")
        except Exception as e:
            print(f"  ERROR deleting batch starting at {i}: {e}")

    print(f"\nDone. {total} duplicate rows deleted.")


if __name__ == "__main__":
    main()
