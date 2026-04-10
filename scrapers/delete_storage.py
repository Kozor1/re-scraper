#!/usr/bin/env python3
"""
delete_storage.py  –  Delete all files from the Supabase Storage
'property-images' bucket and clear the property_images table.

Run AFTER backfill_image_urls.py and BEFORE re-running migrate_images.py.

Usage:
    python3 scrapers/delete_storage.py            # full wipe (with confirmation)
    python3 scrapers/delete_storage.py --yes      # skip confirmation prompt
    python3 scrapers/delete_storage.py --dry-run  # list what would be deleted
"""

import os
import sys
import argparse

try:
    from supabase import create_client
except ImportError:
    print("ERROR: Run: pip install supabase")
    sys.exit(1)

# ── Supabase setup ────────────────────────────────────────────────────────────

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

env_path = os.path.join(ROOT, '.env')
if os.path.exists(env_path):
    for line in open(env_path):
        line = line.strip()
        if line and not line.startswith('#') and '=' in line:
            k, v = line.split('=', 1)
            os.environ.setdefault(k.strip(), v.strip())

url = os.environ.get('SUPABASE_URL')
key = os.environ.get('SUPABASE_SERVICE_KEY')
if not url or not key:
    print("ERROR: Set SUPABASE_URL and SUPABASE_SERVICE_KEY in .env or environment")
    sys.exit(1)

sb     = create_client(url, key)
BUCKET = 'property-images'

# ── Helpers ───────────────────────────────────────────────────────────────────

def list_all_files():
    """Return list of all file paths in the bucket."""
    files = []
    offset = 0
    while True:
        batch = sb.storage.from_(BUCKET).list(
            path='',
            options={'limit': 1000, 'offset': offset}
        )
        if not batch:
            break
        # Each item may be a folder (no size) or a file
        for item in batch:
            name = item.get('name', '')
            if item.get('id'):  # files have an id; folders don't
                files.append(name)
            else:
                # It's a folder prefix — list recursively
                sub = sb.storage.from_(BUCKET).list(path=name, options={'limit': 1000})
                for sub_item in (sub or []):
                    if sub_item.get('id'):
                        files.append(f"{name}/{sub_item['name']}")
        offset += len(batch)
        if len(batch) < 1000:
            break
    return files


def delete_storage_files(files, dry_run=False):
    """Delete files from the bucket in batches of 100."""
    if not files:
        print("  No files to delete.")
        return
    if dry_run:
        print(f"  [dry-run] Would delete {len(files)} files from Storage")
        for f in files[:10]:
            print(f"    {f}")
        if len(files) > 10:
            print(f"    … and {len(files) - 10} more")
        return

    BATCH = 100
    deleted = 0
    for i in range(0, len(files), BATCH):
        batch = files[i:i + BATCH]
        try:
            sb.storage.from_(BUCKET).remove(batch)
            deleted += len(batch)
            print(f"  Deleted {deleted}/{len(files)} files")
        except Exception as e:
            print(f"  ERROR deleting batch {i}–{i+BATCH}: {e}")

    print(f"\n  Storage: {deleted} files deleted from '{BUCKET}'")


def clear_property_images_table(dry_run=False):
    """Delete all rows from the property_images table."""
    if dry_run:
        count = sb.table('property_images').select('id', count='exact').execute()
        n = count.count if hasattr(count, 'count') else '?'
        print(f"  [dry-run] Would delete all rows from property_images table ({n} rows)")
        return

    try:
        # Delete in batches to avoid timeouts on large tables
        while True:
            rows = sb.table('property_images').select('id').limit(1000).execute().data
            if not rows:
                break
            ids = [r['id'] for r in rows]
            sb.table('property_images').delete().in_('id', ids).execute()
            print(f"  Deleted {len(ids)} rows from property_images…")
        print("  property_images table cleared.")
    except Exception as e:
        print(f"  ERROR clearing property_images: {e}")

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description=(
            'Delete all files from Supabase Storage property-images bucket '
            'and clear the property_images table.'
        )
    )
    parser.add_argument('--yes',     action='store_true', help='Skip confirmation prompt')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be deleted')
    args = parser.parse_args()

    print(f"Supabase project: {url}")
    print(f"Bucket: {BUCKET}\n")

    if args.dry_run:
        print("=== DRY RUN ===")
        print("\nListing Storage files…")
        files = list_all_files()
        print(f"Found {len(files)} files in Storage bucket")
        delete_storage_files(files, dry_run=True)
        print()
        clear_property_images_table(dry_run=True)
        print("\nRun without --dry-run to apply.")
        return

    if not args.yes:
        print("This will:")
        print(f"  • Delete ALL files from the '{BUCKET}' Supabase Storage bucket")
        print(f"  • Delete ALL rows from the property_images table")
        print()
        print("You should run backfill_image_urls.py first to save source URLs to JSON.")
        print("After this, run migrate_images.py to re-populate with source URLs.")
        print()
        confirm = input("Type 'yes' to continue: ").strip().lower()
        if confirm != 'yes':
            print("Aborted.")
            return

    print("\nListing Storage files…")
    files = list_all_files()
    print(f"Found {len(files)} files")
    delete_storage_files(files)

    print("\nClearing property_images table…")
    clear_property_images_table()

    print(
        "\nDone! Next step:\n"
        "  python3 supabase/migrate_images.py\n"
        "  (re-populates property_images with source URLs — no Storage upload)"
    )


if __name__ == '__main__':
    main()
