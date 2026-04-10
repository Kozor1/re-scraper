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

import os, json, re, glob, sys, argparse
from datetime import datetime

try:
    from supabase import create_client
except ImportError:
    print("Run: pip3 install supabase python-dotenv")
    sys.exit(1)

# ── Config ────────────────────────────────────────────────────────────────────

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Load .env if present
env_path = os.path.join(ROOT, '.env')
if os.path.exists(env_path):
    for line in open(env_path):
        line = line.strip()
        if line and not line.startswith('#') and '=' in line:
            k, v = line.split('=', 1)
            os.environ.setdefault(k.strip(), v.strip())

SUPABASE_URL = os.environ.get('SUPABASE_URL')
SUPABASE_KEY = os.environ.get('SUPABASE_SERVICE_KEY')

if not SUPABASE_URL or not SUPABASE_KEY:
    print("ERROR: Set SUPABASE_URL and SUPABASE_SERVICE_KEY in .env or environment")
    sys.exit(1)

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

SOURCES = {
    # ── Sale listings ─────────────────────────────────────────
    'sb':        'properties/sb',
    'ups':       'properties/ups',
    'hc':        'properties/hc',
    'jm':        'properties/jm',
    'pp':        'properties/pp',
    'tr':        'properties/tr',
    'dh':        'properties/dh',
    'mm':        'properties/mm',
    'ce':        'properties/ce',
    'gm':        'properties/gm',
    'pinp':      'properties/pinp',
    'rb':        'properties/rb',
    # ── Rental listings (source key ends with _rent) ──────────
    'sb_rent':   'properties/sb_rent',
    'ups_rent':  'properties/ups_rent',
    'hc_rent':   'properties/hc_rent',
    'jm_rent':   'properties/jm_rent',
    'pp_rent':   'properties/pp_rent',
    'tr_rent':   'properties/tr_rent',
    'dh_rent':   'properties/dh_rent',
    'mm_rent':   'properties/mm_rent',
    'ce_rent':   'properties/ce_rent',
    'gm_rent':   'properties/gm_rent',
    'rb_rent':   'properties/rb_rent',
}

# Load geocache for coords
geocache_path = os.path.join(ROOT, 'geocache.json')
geocache = {}
if os.path.exists(geocache_path):
    geocache = json.load(open(geocache_path))
print(f"Geocache: {len(geocache)} addresses")


# ── Helpers ───────────────────────────────────────────────────────────────────

def parse_price_value(price_str):
    """Extract numeric value from price string."""
    if not price_str:
        return None
    m = re.search(r'£([\d,]+)', str(price_str))
    if m:
        try:
            return int(m.group(1).replace(',', ''))
        except:
            pass
    # Try plain integer
    try:
        return int(price_str)
    except:
        return None

def normalise_status(raw):
    s = (raw or '').strip().lower()
    if s in ('for sale', 'forsale'):        return 'For Sale'
    if s in ('sale agreed', 'saleagreed', 'agreed'): return 'Sale Agreed'
    if s == 'sold':                          return 'Sold'
    return (raw or '').strip() or 'For Sale'

def normalise_bedrooms(raw):
    """Normalise bedrooms to a plain integer string e.g. '3'."""
    if not raw:
        return None
    m = re.search(r'(\d+)', str(raw))
    return m.group(1) if m else str(raw)


# ── Main migration ────────────────────────────────────────────────────────────

def get_existing_ids(source):
    """Fetch all source_ids already in Supabase for a given source."""
    existing = set()
    offset = 0
    while True:
        rows = (
            supabase.table('properties')
            .select('source_id')
            .eq('source', source)
            .range(offset, offset + 999)
            .execute()
        ).data
        if not rows:
            break
        for r in rows:
            existing.add(r['source_id'])
        offset += 1000
        if len(rows) < 1000:
            break
    return existing


def collect_properties(only_source=None, new_only=False):
    """Yield (source, source_id, data) for every property JSON."""
    sources_to_run = {k: v for k, v in SOURCES.items()
                      if not only_source or k == only_source}

    for source, rel_dir in sources_to_run.items():
        src_dir = os.path.join(ROOT, rel_dir)
        if not os.path.isdir(src_dir):
            print(f"  Skipping {source}: directory not found")
            continue

        existing = set()
        if new_only:
            existing = get_existing_ids(source)
            print(f"  {source}: {len(existing)} already in DB — will skip these")

        dirs = sorted(
            [d for d in os.listdir(src_dir)
             if d.startswith('property_') and os.path.isdir(os.path.join(src_dir, d))],
            key=lambda x: int(x.replace('property_', ''))
        )
        for d in dirs:
            if new_only and d in existing:
                continue
            jpath = os.path.join(src_dir, d, f'{d}.json')
            if not os.path.exists(jpath):
                continue
            try:
                data = json.load(open(jpath))
                yield source, d, data
            except Exception as e:
                print(f"  WARN: could not read {jpath}: {e}")


def build_row(source, source_id, data):
    """Convert raw JSON data into a properties table row."""
    address = (data.get('address') or data.get('title') or '').strip().rstrip(',').strip()
    coords  = geocache.get(address)

    # Normalise status — handle sources that store it in property_info
    raw_status = (
        data.get('status') or
        (data.get('property_info') or {}).get('Status') or
        (data.get('property_info') or {}).get('status') or
        ''
    )

    # Normalise type — may be in property_info['Style']
    prop_type = (
        data.get('type') or
        (data.get('property_info') or {}).get('Style') or
        (data.get('property_info') or {}).get('Type') or
        ''
    )

    # Normalise bedrooms
    raw_beds = (
        data.get('bedrooms') or
        (data.get('property_info') or {}).get('Bedrooms') or
        (data.get('property_info') or {}).get('bedrooms') or
        ''
    )

    price_str = data.get('price_str') or data.get('price') or ''
    if isinstance(price_str, int):
        price_str = f'£{price_str:,}'

    # Infer listing type from source key suffix
    listing_type = 'rent' if source.endswith('_rent') else 'sale'

    row = {
        'source':        source,
        'source_id':     source_id,
        'url':           data.get('url') or '',
        'address':       address,
        'title':         data.get('title') or address,
        'price':         str(price_str) if price_str else None,
        'price_value':   parse_price_value(price_str),
        'status':        normalise_status(raw_status),
        'listing_type':  listing_type,
        'property_type': prop_type or None,
        'bedrooms':      normalise_bedrooms(raw_beds),
        'bathrooms':     data.get('bathrooms') or None,
        'receptions':    data.get('receptions') or None,
        'epc_rating':    data.get('epc_rating') or None,
        'description':   data.get('description') or None,
        'key_features':  data.get('key_features') or [],
        'rooms':         data.get('rooms') or [],
        'image_urls':    data.get('image_urls') or data.get('images') or [],
    }

    if coords:
        row['lat'] = coords['lat']
        row['lng'] = coords['lng']

    return row


def migrate(only_source=None, new_only=False):
    all_props = list(collect_properties(only_source=only_source, new_only=new_only))
    total = len(all_props)
    if total == 0:
        print("Nothing to migrate — all properties already in DB.")
        return
    print(f"\nMigrating {total} properties to Supabase...\n")

    BATCH = 100
    inserted = 0
    errors   = 0

    for i in range(0, total, BATCH):
        batch = all_props[i:i + BATCH]
        rows  = []
        for source, source_id, data in batch:
            try:
                rows.append(build_row(source, source_id, data))
            except Exception as e:
                print(f"  WARN build_row {source}/{source_id}: {e}")
                errors += 1

        try:
            result = (
                supabase.table('properties')
                .upsert(rows, on_conflict='source,source_id')
                .execute()
            )
            inserted += len(rows)
            pct = (i + len(batch)) / total * 100
            print(f"  [{i + len(batch)}/{total}]  {pct:.0f}%  "
                  f"(batch ok, {len(rows)} rows)")
        except Exception as e:
            print(f"  ERROR batch {i}–{i+BATCH}: {e}")
            errors += len(rows)

    print(f"\n{'='*55}")
    print(f"Done: {inserted} inserted/updated, {errors} errors")


def prune(only_source=None):
    """Delete DB rows for properties whose JSON files no longer exist (delisted).

    Safe: only deletes rows where the source_id is no longer present in the
    local JSON directory for that source.  Run after a full scrape to remove
    properties that have been taken off the market.
    """
    sources_to_prune = {k: v for k, v in SOURCES.items()
                        if not only_source or k == only_source}

    total_deleted = 0
    for source, rel_dir in sources_to_prune.items():
        src_dir = os.path.join(ROOT, rel_dir)
        if not os.path.isdir(src_dir):
            print(f"  Skipping {source}: directory not found")
            continue

        # source_ids on disk
        on_disk = set(
            d for d in os.listdir(src_dir)
            if d.startswith('property_') and
            os.path.isdir(os.path.join(src_dir, d)) and
            os.path.exists(os.path.join(src_dir, d, f'{d}.json'))
        )

        # source_ids in DB
        in_db_map = {}   # source_id → numeric DB row id
        offset = 0
        while True:
            rows = (
                supabase.table('properties')
                .select('id, source_id')
                .eq('source', source)
                .range(offset, offset + 999)
                .execute()
            ).data
            if not rows:
                break
            for r in rows:
                in_db_map[r['source_id']] = r['id']
            offset += 1000
            if len(rows) < 1000:
                break

        stale = [sid for sid in in_db_map if sid not in on_disk]
        if not stale:
            print(f"  {source}: nothing to prune "
                  f"({len(in_db_map)} in DB, {len(on_disk)} on disk)")
            continue

        print(f"  {source}: pruning {len(stale)} stale properties "
              f"(DB has {len(in_db_map)}, disk has {len(on_disk)})")
        for sid in stale:
            print(f"    deleting {source}/{sid}")

        db_ids = [in_db_map[sid] for sid in stale]
        for i in range(0, len(db_ids), 100):
            batch = db_ids[i:i + 100]
            try:
                supabase.table('properties').delete().in_('id', batch).execute()
                total_deleted += len(batch)
            except Exception as e:
                print(f"  ERROR deleting batch: {e}")

    print(f"\nPrune complete. Total deleted: {total_deleted}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Migrate property JSON files to Supabase and prune delisted properties.'
    )
    parser.add_argument('--source',     help='Only process one source (e.g. sb, hc, dh)')
    parser.add_argument('--new-only',   action='store_true',
                        help='Skip properties already in Supabase (faster for incremental updates)')
    parser.add_argument('--no-prune',   action='store_true',
                        help='Skip the automatic prune step after migration')
    parser.add_argument('--prune-only', action='store_true',
                        help='Only run prune (delete stale DB rows), skip migration')
    args = parser.parse_args()

    if args.prune_only:
        prune(only_source=args.source)
    else:
        migrate(only_source=args.source, new_only=args.new_only)
        if not args.no_prune:
            print()
            print("Running prune to remove any delisted properties...")
            prune(only_source=args.source)
