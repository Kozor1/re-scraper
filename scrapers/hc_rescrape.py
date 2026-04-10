#!/usr/bin/env python3
"""
Hunter Campbell re-scraper — fills in missing fields for all existing HC properties.

The original scraper used wrong CSS selectors, leaving description, key_features,
type, bedrooms, receptions, and status blank.  HC uses the same PropertyPal CMS
as Templeton Robinson, so selectors are identical.

Safe to re-run — skips any property that already has a description.

Usage:
    python3 scrapers/hc_rescrape.py              # all 115 properties
    python3 scrapers/hc_rescrape.py --limit 5    # test on first 5
    python3 scrapers/hc_rescrape.py --id property_3   # single property

Run from the re_app/ directory.
"""

import os, json, re, time, sys, argparse
from datetime import datetime

try:
    import requests
    from bs4 import BeautifulSoup
except ImportError:
    print("Run: pip3 install requests beautifulsoup4")
    sys.exit(1)

# ── Config ─────────────────────────────────────────────────────────────────────

ROOT    = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
HC_DIR  = os.path.join(ROOT, 'properties', 'hc')

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
                  'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xhtml+xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-GB,en;q=0.9',
}

DELAY = 1.5   # seconds between requests — be polite to HC's server

# ── Fetch ──────────────────────────────────────────────────────────────────────

def fetch(url, retries=3):
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=HEADERS, timeout=20)
            r.raise_for_status()
            return r.text
        except Exception as e:
            print(f"    attempt {attempt+1} failed: {e}")
            if attempt < retries - 1:
                time.sleep(3 * (attempt + 1))
    return None

# ── Parse detail page ──────────────────────────────────────────────────────────

def parse_detail(html):
    """
    Returns dict with: description, key_features, type, bedrooms,
                       receptions, status (may be empty strings / []).
    """
    soup = BeautifulSoup(html, 'html.parser')
    result = {}

    # ── Key-value table (Price, Style, Bedrooms, Receptions, Heating, Status) ──
    for li in soup.select('ul.dettbl li'):
        key_el = li.find(class_='dt1')
        val_el = li.find(class_='dt2')
        if not key_el or not val_el:
            continue
        key = key_el.get_text(strip=True).lower()
        val = val_el.get_text(strip=True)
        if 'style' in key or 'type' in key:
            result['type'] = val
        elif 'bedroom' in key:
            result['bedrooms'] = val
        elif 'reception' in key:
            result['receptions'] = val
        elif 'status' in key:
            result['status'] = val
        elif 'heating' in key:
            result['heating'] = val

    # Fallback: parse status from the top "dtsm" bar
    if not result.get('status'):
        dtsm = soup.select_one('div.dtsm')
        if dtsm:
            text = dtsm.get_text(separator=' ', strip=True).lower()
            if 'agreed' in text:
                result['status'] = 'Sale Agreed'
            elif 'for sale' in text or 'available' in text:
                result['status'] = 'For Sale'

    # Fallback: bedrooms from dtsm
    if not result.get('bedrooms'):
        dtsm = soup.select_one('div.dtsm')
        if dtsm:
            m = re.search(r'(\d+)\s+bedroom', dtsm.get_text(separator=' ', strip=True), re.I)
            if m:
                result['bedrooms'] = m.group(1)

    # ── Key features (ul.feats) ────────────────────────────────────────────────
    features = []
    for li in soup.select('ul.feats li'):
        t = li.get_text(strip=True)
        if t:
            features.append(t)
    result['key_features'] = features

    # ── Description + rooms (div.textblock div.textbp) ────────────────────────
    description_parts = []
    rooms = []

    for container in soup.select('div.textblock div.textbp'):
        # textbp holds either:
        #   - introductory description paragraphs
        #   - room entries: <b>ROOM NAME: dimensions</b> followed by text
        items = list(container.children)
        current_room_name = None
        current_room_desc = []

        for node in items:
            if hasattr(node, 'name') and node.name == 'b':
                # Save any accumulated room
                if current_room_name is not None:
                    rooms.append({
                        'name': current_room_name,
                        'description': ' '.join(current_room_desc).strip(),
                    })
                    current_room_name = None
                    current_room_desc = []

                bold_text = node.get_text(strip=True)
                # Room headers look like "KITCHEN: - 3.45m (11'4\") x 2.74m (9'0\")"
                # or "ENTRANCE HALL:" or simply "LOUNGE - 3.93m x 3.79m"
                if bold_text and (bold_text.isupper() or re.search(r'[\d.]+m', bold_text)):
                    current_room_name = bold_text
                else:
                    # Bold text that's not a room (intro bold, etc.)
                    if bold_text:
                        description_parts.append(bold_text)

            elif hasattr(node, 'name') and node.name in ('p', 'span', 'div'):
                t = node.get_text(separator=' ', strip=True)
                if t:
                    if current_room_name is not None:
                        current_room_desc.append(t)
                    else:
                        description_parts.append(t)

            elif hasattr(node, 'name') and node.name is None:
                # NavigableString (plain text node)
                t = str(node).strip()
                if t:
                    if current_room_name is not None:
                        current_room_desc.append(t)
                    else:
                        description_parts.append(t)
            else:
                # Any other tag — grab text
                if hasattr(node, 'get_text'):
                    t = node.get_text(separator=' ', strip=True)
                    if t:
                        if current_room_name is not None:
                            current_room_desc.append(t)
                        else:
                            description_parts.append(t)

        # Flush last room
        if current_room_name is not None:
            rooms.append({
                'name': current_room_name,
                'description': ' '.join(current_room_desc).strip(),
            })

    result['description'] = ' '.join(description_parts).strip()
    result['rooms']       = rooms

    return result

# ── Normalise ──────────────────────────────────────────────────────────────────

def normalise_status(raw):
    s = (raw or '').strip().lower()
    if 'agreed' in s: return 'Sale Agreed'
    if 'sold'   in s: return 'Sold'
    return 'For Sale'

# ── Main ───────────────────────────────────────────────────────────────────────

def rescrape(only_id=None, limit=0):
    dirs = sorted(
        [d for d in os.listdir(HC_DIR)
         if d.startswith('property_') and os.path.isdir(os.path.join(HC_DIR, d))],
        key=lambda x: int(x.replace('property_', ''))
    )

    if only_id:
        dirs = [d for d in dirs if d == only_id]

    processed = skipped = errors = 0

    for i, d in enumerate(dirs):
        if limit and processed >= limit:
            print(f"\nReached --limit {limit}")
            break

        jpath = os.path.join(HC_DIR, d, f'{d}.json')
        if not os.path.exists(jpath):
            continue

        data = json.load(open(jpath))

        # Skip if already has description
        if data.get('description') and not only_id:
            skipped += 1
            continue

        url = data.get('url')
        if not url:
            print(f"  SKIP {d}: no URL")
            skipped += 1
            continue

        print(f"[{i+1}/{len(dirs)}] {d}  {url}")

        html = fetch(url)
        if not html:
            print(f"  ERROR: fetch failed")
            errors += 1
            continue

        parsed = parse_detail(html)

        # Merge into existing JSON — don't overwrite fields that already have good values
        changed = False
        for field in ('description', 'key_features', 'rooms', 'type', 'bedrooms',
                      'receptions', 'heating'):
            new_val = parsed.get(field)
            # Only update if we got something useful and the field is missing/wrong
            if field == 'bedrooms':
                # Existing bedrooms field contains room dimensions — always replace
                if new_val:
                    data['bedrooms'] = new_val
                    changed = True
                    # Clear the incorrect property_info.bedrooms
                    if 'property_info' in data and 'bedrooms' in data['property_info']:
                        del data['property_info']['bedrooms']
                        if not data['property_info']:
                            del data['property_info']
            elif field in ('key_features', 'rooms'):
                if new_val:   # non-empty list
                    data[field] = new_val
                    changed = True
            else:
                if new_val and not data.get(field):
                    data[field] = new_val
                    changed = True

        # Status: always normalise
        raw_status = parsed.get('status') or data.get('status') or ''
        norm = normalise_status(raw_status)
        if data.get('status') != norm:
            data['status'] = norm
            changed = True

        if changed:
            data['rescraped_at'] = datetime.now().isoformat()
            with open(jpath, 'w') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            beds = data.get('bedrooms', '?')
            status = data.get('status', '?')
            feat_n = len(data.get('key_features', []))
            room_n = len(data.get('rooms', []))
            desc_len = len(data.get('description', ''))
            print(f"  ✓  beds={beds}  status={status}  "
                  f"features={feat_n}  rooms={room_n}  desc={desc_len}ch")
        else:
            print(f"  — no new data extracted")

        processed += 1
        time.sleep(DELAY)

    print(f"\n{'='*55}")
    print(f"Done: {processed} processed, {skipped} skipped, {errors} errors")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--id',    help='Rescrape a single property (e.g. property_3)')
    parser.add_argument('--limit', type=int, default=0, help='Max properties to process')
    args = parser.parse_args()
    rescrape(only_id=args.id, limit=args.limit)
