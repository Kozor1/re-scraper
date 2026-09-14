"""Archive duplicate/orphan property folders for the given sources.

A folder under properties/<site>/ is archived when its id is not referenced by
property_index.json:
  - DUPE:   its URL is in the index under a different id (superseded scrape)
  - ORPHAN: its URL is not in the index at all (or its JSON is unreadable)

Folders are MOVED (never deleted) to properties/_archive_dupes/<site>/ so the
operation is fully reversible. Safe to re-run.

Usage: python3 archive_orphans.py sb ups hc jm pp tr
"""
import json
import os
import shutil
import sys

sys.path.insert(0, ".")
from config import SOURCES

ARCHIVE_ROOT = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "properties", "_archive_dupes"
)

def main(sites):
    for site in sites:
        d = SOURCES[site]["props_dir"]
        idx_path = os.path.join(d, "property_index.json")
        if not os.path.isfile(idx_path):
            print(f"{site}: no index — skipped")
            continue
        idx = json.load(open(idx_path))
        id_by_url = {
            (e.get("url") or "").rstrip("/"): e.get("id")
            for e in idx.get("properties", [])
            if e.get("url")
        }
        index_ids = set(id_by_url.values())

        moved = dupe_n = orphan_n = 0
        for fid in sorted(os.listdir(d)):
            if not fid.startswith("property_"):
                continue
            folder = os.path.join(d, fid)
            if not os.path.isdir(folder):
                continue
            if fid in index_ids:
                continue
            jpath = os.path.join(folder, f"{fid}.json")
            kind = "orphan"
            if os.path.isfile(jpath):
                try:
                    url = (json.load(open(jpath)).get("url") or "").rstrip("/")
                    if url in id_by_url:
                        kind = "dupe"
                except Exception:
                    pass
            dest_dir = os.path.join(ARCHIVE_ROOT, site)
            os.makedirs(dest_dir, exist_ok=True)
            dest = os.path.join(dest_dir, fid)
            if os.path.exists(dest):
                dest = os.path.join(dest_dir, f"{fid}_b{os.path.getmtime(folder):.0f}")
            shutil.move(folder, dest)
            moved += 1
            dupe_n += kind == "dupe"
            orphan_n += kind == "orphan"
        print(f"{site}: archived {moved} folders (dupe={dupe_n} orphan={orphan_n})")

if __name__ == "__main__":
    sites = [s for s in sys.argv[1:] if s in SOURCES]
    if not sites:
        print("usage: python3 archive_orphans.py <source> [<source> ...]")
        sys.exit(1)
    main(sites)
