#!/usr/bin/env python3
"""
Migrate property metadata from JSON files → Supabase.

Includes image_urls (sourced from data['image_urls'] or data['images']).
Safe to re-run — uses upsert on (source, source_id).

Usage:
    python3 supabase/migrate_data.py                          # migrate all + prune (default)
    python3 supabase/migrate_data.py --source sb              # one source only (migrate + prune)
    python3 supabase/migrate_data.py --source sb --new-only   # only properties not yet in DB
    python3 supabase/migrate_data.py --no-prune               # migrate without pruning
    python3 supabase/migrate_data.py --prune-only             # only delete stale DB rows

Requires SUPABASE_URL and SUPABASE_SERVICE_KEY in .env or environment.
"""

from __future__ import annotations

import os
import sys
import json
import argparse
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import (
    MIGRATE_SOURCES as SOURCES,
    get_supabase,
    build_property_row,
    load_geocache,
)

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
supabase = get_supabase()
geocache = load_geocache()

print(f"Geocache: {len(geocache)} addresses")


# ── Helpers ───────────────────────────────────────────────────────────────────


def get_existing_ids(source: str) -> set[str]:
    """Fetch all source_ids already in Supabase for a given source."""
    existing: set[str] = set()
    offset = 0
    while True:
        rows = (
            supabase.table("properties")
            .select("source_id")
            .eq("source", source)
            .range(offset, offset + 999)
            .execute()
        ).data
        if not rows:
            break
        for r in rows:
            existing.add(r["source_id"])
        offset += 1000
        if len(rows) < 1000:
            break
    return existing


def collect_properties(
    only_source: str | None = None, new_only: bool = False
):
    """Yield (source, source_id, data) for every property JSON."""
    sources_to_run = {
        k: v for k, v in SOURCES.items() if not only_source or k == only_source
    }

    for source, rel_dir in sources_to_run.items():
        src_dir = os.path.join(ROOT, rel_dir)
        if not os.path.isdir(src_dir):
            print(f"  Skipping {source}: directory not found")
            continue

        existing: set[str] = set()
        if new_only:
            existing = get_existing_ids(source)
            print(f"  {source}: {len(existing)} already in DB — will skip these")

        dirs = sorted(
            [
                d
                for d in os.listdir(src_dir)
                if d.startswith("property_")
                and os.path.isdir(os.path.join(src_dir, d))
            ],
            key=lambda x: int(x.replace("property_", ""))
            if x.replace("property_", "").isdigit()
            else 0,
        )

        # Authoritative url -> id map from the source's index.  Long-running
        # sources historically accumulated duplicate property folders (same
        # URL under several property_N ids); only one folder per URL may
        # migrate — the one the index references, else the newest scrape.
        index_id_by_url: dict[str, str] = {}
        index_path = os.path.join(src_dir, "property_index.json")
        if os.path.isfile(index_path):
            try:
                with open(index_path, encoding="utf-8") as f:
                    for e in json.load(f).get("properties", []):
                        u = (e.get("url") or "").rstrip("/")
                        if u:
                            index_id_by_url[u] = e.get("id", "")
            except (json.JSONDecodeError, OSError):
                pass

        by_url: dict[str, list[tuple[str, dict]]] = {}
        for d in dirs:
            if new_only and d in existing:
                continue
            jpath = os.path.join(src_dir, d, f"{d}.json")
            if not os.path.exists(jpath):
                continue
            try:
                data = json.load(open(jpath, encoding="utf-8"))
            except (json.JSONDecodeError, OSError) as e:
                print(f"  WARN: could not read {jpath}: {e}")
                continue
            url_key = (data.get("url") or d).rstrip("/")
            by_url.setdefault(url_key, []).append((d, data))

        skipped_dupes = 0
        for url_key in sorted(by_url):
            group = by_url[url_key]
            if len(group) == 1:
                chosen = group[0]
            else:
                pref = index_id_by_url.get(url_key)
                chosen = (
                    next((g for g in group if g[0] == pref), None)
                    if pref
                    else None
                )
                if chosen is None:
                    chosen = max(
                        group, key=lambda t: t[1].get("scraped_at", "")
                    )
                skipped_dupes += len(group) - 1
            yield source, chosen[0], chosen[1]

        if skipped_dupes:
            print(f"  {source}: skipped {skipped_dupes} duplicate folder(s)")


# ── Migration ────────────────────────────────────────────────────────────────


def migrate(
    only_source: str | None = None, new_only: bool = False
) -> None:
    all_props = list(
        collect_properties(only_source=only_source, new_only=new_only)
    )
    total = len(all_props)
    if total == 0:
        print("Nothing to migrate — all properties already in DB.")
        return
    print(f"\nMigrating {total} properties to Supabase...\n")

    BATCH = 100
    inserted = 0
    errors = 0

    for i in range(0, total, BATCH):
        batch = all_props[i : i + BATCH]
        rows = []
        for source, source_id, data in batch:
            try:
                rows.append(
                    build_property_row(source, source_id, data, geocache)
                )
            except Exception as e:
                print(f"  WARN build_row {source}/{source_id}: {e}")
                errors += 1

        try:
            # Conflict target: (source, url). The URL is the stable identity
            # of a listing — source_id restarts at property_1 on every fresh
            # scrape, which previously caused duplicates. Requires migration
            # 004 (idx_properties_source_url_unique).
            supabase.table("properties").upsert(
                rows, on_conflict="source,url"
            ).execute()
            inserted += len(rows)
            pct = (i + len(batch)) / total * 100
            print(
                f"  [{i + len(batch)}/{total}]  {pct:.0f}%  "
                f"(batch ok, {len(rows)} rows)"
            )
        except Exception as e:
            print(f"  ERROR batch {i}–{i + BATCH}: {e}")
            errors += len(rows)

    print(f"\n{'=' * 55}")
    print(f"Done: {inserted} inserted/updated, {errors} errors")


# ── Prune ─────────────────────────────────────────────────────────────────────


def prune(only_source: str | None = None) -> None:
    """Delete DB rows for properties whose JSON files no longer exist (delisted)."""
    sources_to_prune = {
        k: v
        for k, v in SOURCES.items()
        if not only_source or k == only_source
    }

    total_deleted = 0
    for source, rel_dir in sources_to_prune.items():
        src_dir = os.path.join(ROOT, rel_dir)
        if not os.path.isdir(src_dir):
            print(f"  Skipping {source}: directory not found")
            continue

        on_disk = {
            d
            for d in os.listdir(src_dir)
            if d.startswith("property_")
            and os.path.isdir(os.path.join(src_dir, d))
            and os.path.exists(os.path.join(src_dir, d, f"{d}.json"))
        }

        in_db_map: dict[str, int] = {}
        offset = 0
        while True:
            rows = (
                supabase.table("properties")
                .select("id, source_id")
                .eq("source", source)
                .range(offset, offset + 999)
                .execute()
            ).data
            if not rows:
                break
            for r in rows:
                in_db_map[r["source_id"]] = r["id"]
            offset += 1000
            if len(rows) < 1000:
                break

        stale = [sid for sid in in_db_map if sid not in on_disk]
        if not stale:
            print(
                f"  {source}: nothing to prune "
                f"({len(in_db_map)} in DB, {len(on_disk)} on disk)"
            )
            continue

        print(
            f"  {source}: pruning {len(stale)} stale properties "
            f"(DB has {len(in_db_map)}, disk has {len(on_disk)})"
        )
        for sid in stale:
            print(f"    deleting {source}/{sid}")

        db_ids = [in_db_map[sid] for sid in stale]
        for i in range(0, len(db_ids), 100):
            batch = db_ids[i : i + 100]
            try:
                (
                    supabase.table("properties")
                    .delete()
                    .in_("id", batch)
                    .execute()
                )
                total_deleted += len(batch)
            except Exception as e:
                print(f"  ERROR deleting batch: {e}")

    print(f"\nPrune complete. Total deleted: {total_deleted}")


# ── Main ──────────────────────────────────────────────────────────────────────


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Migrate property JSON files to Supabase and prune delisted properties."
    )
    parser.add_argument(
        "--source",
        help="Only process one source (e.g. sb, hc, dh)",
    )
    parser.add_argument(
        "--new-only",
        action="store_true",
        help="Skip properties already in Supabase (faster for incremental updates)",
    )
    parser.add_argument(
        "--no-prune",
        action="store_true",
        help="Skip the automatic prune step after migration",
    )
    parser.add_argument(
        "--prune-only",
        action="store_true",
        help="Only run prune (delete stale DB rows), skip migration",
    )
    args = parser.parse_args()

    if args.prune_only:
        prune(only_source=args.source)
    else:
        migrate(only_source=args.source, new_only=args.new_only)
        if not args.no_prune:
            print()
            print("Running prune to remove any delisted properties...")
            prune(only_source=args.source)
