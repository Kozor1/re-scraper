"""
fix_ups_status.py  –  Re-check and fix the 'status' field for all UPS properties
                      whose JSON currently says 'For Sale' by fetching the live page
                      and looking for the sale-agr badge.

Run once after discovering the sale-agr/SVG pattern bug.  Then re-run migrate_data.py
to push the corrected statuses to Supabase.

Usage:
    python3 fix_ups_status.py              # fix all For-Sale properties
    python3 fix_ups_status.py --dry-run    # preview only, no writes
"""

import os, re, json, time, argparse, random
import requests
from bs4 import BeautifulSoup

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
UPS_DIR    = os.path.join(SCRIPT_DIR, 'properties', 'ups')

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                  '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
}


def detect_status(soup):
    """Return 'Sale Agreed', 'Sold', or 'For Sale' from a BeautifulSoup page."""
    # Primary: <div class="sale-agr"><img src="/images/saleagreed.svg" alt="Agreed">
    sale_agr = soup.find('div', {'class': 'sale-agr'})
    if sale_agr:
        img = sale_agr.find('img')
        src = (img.get('src') or '').lower() if img else ''
        alt = (img.get('alt') or '').lower() if img else ''
        if 'saleagreed' in src or 'sale-agreed' in src or 'agreed' in alt:
            return 'Sale Agreed'
        if 'sold' in src or 'sold' in alt:
            return 'Sold'

    # Secondary: any img inside prop-det-status-outer
    status_outer = soup.find('div', {'class': 'prop-det-status-outer'})
    if status_outer:
        img = status_outer.find('img')
        if img:
            src = (img.get('src') or '').lower()
            alt = (img.get('alt') or '').lower()
            if 'saleagreed' in src or 'agreed' in alt:
                return 'Sale Agreed'
            if 'sold' in src or 'sold' in alt:
                return 'Sold'

    return 'For Sale'


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dry-run', action='store_true',
                        help='Print changes without writing files')
    args = parser.parse_args()

    if not os.path.isdir(UPS_DIR):
        print(f"ERROR: UPS directory not found: {UPS_DIR}")
        return

    # Collect all property folders
    folders = sorted(
        [d for d in os.listdir(UPS_DIR)
         if d.startswith('property_') and os.path.isdir(os.path.join(UPS_DIR, d))],
        key=lambda x: int(x.replace('property_', '')) if x.replace('property_', '').isdigit() else 0
    )

    updated   = 0
    unchanged = 0
    errors    = 0
    checked   = 0

    for prop in folders:
        jpath = os.path.join(UPS_DIR, prop, f'{prop}.json')
        if not os.path.isfile(jpath):
            continue
        try:
            data = json.load(open(jpath, encoding='utf-8'))
        except Exception as e:
            print(f"  WARN: could not read {jpath}: {e}")
            errors += 1
            continue

        current_status = data.get('status', 'For Sale')
        # Only re-check properties currently marked For Sale
        if current_status != 'For Sale':
            unchanged += 1
            continue

        url = data.get('url', '')
        if not url:
            print(f"  SKIP {prop}: no URL in JSON")
            unchanged += 1
            continue

        checked += 1
        try:
            resp = requests.get(url, headers=HEADERS, timeout=20)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.content, 'html.parser')
            new_status = detect_status(soup)
        except Exception as e:
            print(f"  ERROR {prop}: {e}")
            errors += 1
            time.sleep(2)
            continue

        if new_status != current_status:
            print(f"  {prop}: '{current_status}' → '{new_status}'  ({url})")
            if not args.dry_run:
                data['status'] = new_status
                with open(jpath, 'w', encoding='utf-8') as f:
                    json.dump(data, f, indent=2, ensure_ascii=False)
            updated += 1
        else:
            unchanged += 1

        # Polite delay
        time.sleep(random.uniform(0.5, 1.5))

    print()
    print(f"{'DRY RUN — ' if args.dry_run else ''}Done.")
    print(f"  Checked (For Sale only): {checked}")
    print(f"  Updated:                 {updated}")
    print(f"  Unchanged/skipped:       {unchanged}")
    print(f"  Errors:                  {errors}")

    if not args.dry_run and updated > 0:
        print()
        print("Now re-run the migration to push the fixes to Supabase:")
        print("  python3 supabase/migrate_data.py --source ups")


if __name__ == '__main__':
    main()
