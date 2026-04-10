"""
fix_tr_images.py  –  Fix image ordering and duplicates in existing TR JSON files.

TR's gallery carousel puts the last few images at the front of the HTML list
for seamless looping (e.g. -25.jpg, -26.jpg, -1.jpg, -2.jpg…).  This script:
  1. Sorts image_urls by their trailing numeric suffix  → -1.jpg first
  2. Removes duplicate URLs                             → no repeated images

Run once to fix all existing TR JSONs on disk, then re-run migrate_data.py
to push the corrected data to Supabase.

Usage:
    python3 scrapers/fix_tr_images.py
    python3 scrapers/fix_tr_images.py --dry-run   # preview changes, no writes
"""

import os, re, json, argparse

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
TR_DIR     = os.path.join(SCRIPT_DIR, 'properties', 'tr')


def _sort_and_dedup(urls):
    def _num(url):
        m = re.search(r'-(\d+)\.(jpg|jpeg|png|webp)$', url, re.IGNORECASE)
        return int(m.group(1)) if m else 999999
    seen, deduped = set(), []
    for url in urls:
        if url not in seen:
            seen.add(url)
            deduped.append(url)
    deduped.sort(key=_num)
    return deduped


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dry-run', action='store_true',
                        help='Print changes without writing files')
    args = parser.parse_args()

    if not os.path.isdir(TR_DIR):
        print(f"ERROR: TR directory not found: {TR_DIR}")
        return

    updated = 0
    unchanged = 0
    errors = 0

    for prop in sorted(os.listdir(TR_DIR),
                       key=lambda x: int(x.replace('property_', '')) if x.replace('property_', '').isdigit() else 0):
        jpath = os.path.join(TR_DIR, prop, f'{prop}.json')
        if not os.path.isfile(jpath):
            continue
        try:
            with open(jpath, encoding='utf-8') as f:
                data = json.load(f)
        except Exception as e:
            print(f"  WARN: could not read {jpath}: {e}")
            errors += 1
            continue

        original = data.get('image_urls', [])
        if not original:
            unchanged += 1
            continue

        fixed = _sort_and_dedup(original)

        if fixed == original:
            unchanged += 1
            continue

        first_before = original[0].split('/')[-1] if original else '-'
        first_after  = fixed[0].split('/')[-1]    if fixed    else '-'
        removed      = len(original) - len(fixed)

        print(f"  {prop}: {len(original)} → {len(fixed)} images  "
              f"(-{removed} dupes)  first: {first_before} → {first_after}")

        if not args.dry_run:
            data['image_urls']   = fixed
            data['image_count']  = len(fixed)
            with open(jpath, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)

        updated += 1

    print()
    print(f"{'DRY RUN — ' if args.dry_run else ''}Done.")
    print(f"  Updated:   {updated}")
    print(f"  Unchanged: {unchanged}")
    print(f"  Errors:    {errors}")

    if not args.dry_run and updated > 0:
        print()
        print("Now re-run the migration to push the fixes to Supabase:")
        print("  python3 supabase/migrate_data.py --source tr")


if __name__ == '__main__':
    main()
