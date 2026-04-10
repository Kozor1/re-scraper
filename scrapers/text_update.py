"""
text_update.py  –  Daily text-only update for all 6 NI estate agent sources.

Re-fetches property pages to detect price drops, status changes, and description
updates. No images are downloaded. Intended to run once per day.

Usage:
    python3 text_update.py                  # update all sources
    python3 text_update.py sb ups           # update specific sources
    python3 text_update.py jm --limit 10    # update first 10 properties only
    python3 text_update.py hc --workers 4   # parallel workers (default 1)

For each property already on disk (identified by its stored URL), this script:
  1. Re-fetches the property page (text only – no images)
  2. Extracts all text fields using comprehensive, site-aware selectors with fallbacks
  3. Detects changes vs the stored JSON (price, status, title, description, …)
  4. Updates the JSON with fresh data (old price/status preserved as _prev_price/_prev_status)
  5. Appends any changes to properties/{source}/{source}_changes.json

Images are NEVER downloaded – this script is text-only.
"""

import requests
from bs4 import BeautifulSoup
import time
import random
import os
import json
import logging
import argparse
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# ──────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────

SOURCES = {
    'sb':  'properties/sb',
    'ups': 'properties/ups',
    'hc':  'properties/hc',
    'jm':  'properties/jm',
    'pp':  'properties/pp',
    'tr':  'properties/tr',
}

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                  '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-GB,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
    'Cache-Control': 'max-age=0',
}

# Fields that count as "meaningful changes" to report
TRACKED_FIELDS = ['price', 'status', 'title', 'address', 'description']

# ──────────────────────────────────────────────
# Logging
# ──────────────────────────────────────────────

os.makedirs('logs', exist_ok=True)
log_filename = f"logs/text_rescrape_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename, encoding='utf-8'),
        logging.StreamHandler(),
    ]
)
logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────
# HTTP helpers
# ──────────────────────────────────────────────

def fetch(url, max_retries=3):
    """GET with retry and exponential back-off."""
    for attempt in range(max_retries):
        try:
            r = requests.get(url, headers=HEADERS, timeout=30)
            r.raise_for_status()
            return r
        except requests.exceptions.RequestException as e:
            logger.warning(f"[attempt {attempt+1}] {url} – {e}")
            if attempt < max_retries - 1:
                time.sleep((2 ** attempt) * random.uniform(1.5, 3.5))
            else:
                logger.error(f"Giving up on {url}")
                return None

# ──────────────────────────────────────────────
# Extraction helpers  (shared across sites)
# ──────────────────────────────────────────────

def _text(el):
    return el.get_text(separator=' ', strip=True) if el else ''

def _first(*candidates):
    """Return the first non-empty text from a list of BeautifulSoup elements."""
    for el in candidates:
        t = _text(el)
        if t:
            return t
    return ''

def extract_address(soup, site):
    """Extract the property address."""
    # SB / UPS style – two address divs
    a1 = soup.find('div', class_='prop-det-address-one')
    a2 = soup.find('div', class_='prop-det-address-two')
    if a1:
        parts = [_text(a1)]
        if a2:
            parts.append(_text(a2))
        return ', '.join(p for p in parts if p)

    # JM / HC / PP / TR – h1 is the full address
    h1 = soup.find('h1')
    if h1:
        return _text(h1)

    # Fallback: page title stripped of agency name
    title_tag = soup.find('title')
    if title_tag:
        t = _text(title_tag)
        for suffix in [
            ' for sale with Hunter Campbell',
            ' for sale with John Minnis',
            ' for sale with Simon Brien',
            ' for sale | Property People',
            ' | Templeton Robinson',
            ' | Hunter Campbell',
            ' – Hunter Campbell',
            ' - Hunter Campbell',
            ' | John Minnis',
            ' - John Minnis',
            ' | Simon Brien',
            ' | Ulster Property Sales',
            ' - Ulster Property Sales',
            ' | Templeton Robinson',
            ' - Templeton Robinson',
        ]:
            if suffix.lower() in t.lower():
                idx = t.lower().index(suffix.lower())
                return t[:idx].strip()
        return t

    return ''


def extract_price(soup, site):
    """Extract the asking price."""
    # TR style: span.dpt (qualifier) + span.dpp (amount)
    dpt = soup.find('span', class_='dpt')
    dpp = soup.find('span', class_='dpp')
    if dpp:
        qualifier = _text(dpt)
        amount = _text(dpp)
        return f"{qualifier} {amount}".strip() if qualifier else amount

    # SB / UPS / JM / HC / PP (prop-det pattern)
    pa = soup.find('span', class_='prop-det-price-amount')
    if pa:
        pt = soup.find('span', class_='prop-det-price-text')
        qualifier = _text(pt)
        amount = _text(pa)
        return f"{qualifier} {amount}".strip() if qualifier else amount

    # JM alternative
    pa2 = soup.find('span', class_='price-amount')
    if pa2:
        return _text(pa2)

    # Generic CSS names
    for tag in ['span', 'div', 'p', 'strong']:
        for cls in ['price', 'property-price', 'listing-price', 'sale-price']:
            el = soup.find(tag, class_=cls)
            if el:
                t = _text(el)
                if '£' in t or 'POA' in t.upper() or 'price' in t.lower():
                    return t

    # Last resort: find the first text node that looks like a price
    for el in soup.find_all(string=True):
        s = el.strip()
        if s.startswith('£') and len(s) < 30:
            return s

    return ''


def extract_info_rows(soup):
    """
    Extract key:value property info rows.
    Works for SB/UPS/JM/HC/PP/TR that use div.prop-det-info-row pattern.
    Returns a dict.
    """
    info = {}
    rows = soup.find_all('div', class_='prop-det-info-row')
    for row in rows:
        left = row.find('span', class_='prop-det-info-left')
        right = row.find('span', class_='prop-det-info-right')
        if left and right:
            label = _text(left)
            value = _text(right)
            # Strip FontAwesome private-use unicode characters
            label = ''.join(c for c in label if ord(c) < 0xE000 or ord(c) > 0xF8FF)
            label = label.strip().rstrip(':').strip()
            if label and value:
                info[label] = value
    return info


def extract_status_bedrooms(soup, site, info_rows):
    """
    Try to pull Status and Bedroom count from whatever is available.
    Returns (status, bedrooms) strings (may be empty).
    """
    status = info_rows.get('Status', info_rows.get('status', ''))
    bedrooms = info_rows.get('Bedrooms', info_rows.get('bedrooms', info_rows.get('Beds', '')))

    # TR summary stats: div.dtsm > li  e.g. "Sale"/"Agreed", "3 Bedrooms", "Semi-Detached"
    if not status or not bedrooms:
        dtsm = soup.find('div', class_='dtsm')
        if dtsm:
            for li in dtsm.find_all('li'):
                t = _text(li)
                tl = t.lower()
                if not status and tl in ('sale', 'for sale', 'let agreed', 'sold', 'under offer'):
                    status = t
                elif not status and tl == 'agreed':
                    status = 'Sale Agreed'   # TR uses "Agreed" for Sale Agreed
                if not bedrooms and 'bedroom' in tl:
                    bedrooms = t

    return status, bedrooms


def extract_key_features(soup, site):
    """
    Extract bullet-point key features.
    Returns a list of strings.
    """
    # SB / UPS / JM / HC (if they share the same CMS)
    feats_div = soup.find('div', class_='prop-det-feats')
    if feats_div:
        items = [_text(f) for f in feats_div.find_all('div', class_='feat')]
        if items:
            return items

    # PP / generic  –  ul.features or div.features
    for container_args in [
        {'name': 'ul', 'class_': 'features'},
        {'name': 'div', 'class_': 'features'},
        {'name': 'ul', 'class_': 'key-features'},
        {'name': 'div', 'class_': 'key-features'},
        {'name': 'ul', 'class_': 'property-features'},
        {'name': 'div', 'class_': 'property-features'},
    ]:
        container = soup.find(**container_args)
        if container:
            items = [_text(li) for li in container.find_all('li') if _text(li)]
            if items:
                return items

    return []


def extract_description(soup, site):
    """Extract the main property description text."""
    # SB / UPS / JM / HC / PP / TR (prop-det-text)
    desc_div = soup.find('div', class_='prop-det-text')
    if desc_div:
        inner = desc_div.find('div', class_='text')
        return _text(inner) if inner else _text(desc_div)

    # Generic fallbacks
    for args in [
        {'name': 'div', 'class_': 'description'},
        {'name': 'div', 'class_': 'property-description'},
        {'name': 'div', 'class_': 'prop-desc'},
        {'name': 'div', 'class_': 'prop-description'},
        {'name': 'section', 'class_': 'description'},
        {'name': 'div', 'id': 'description'},
        {'name': 'div', 'class_': 'overview'},
    ]:
        el = soup.find(**args)
        if el:
            t = _text(el)
            if len(t) > 100:   # avoid catching tiny snippets
                return t

    return ''


def extract_rooms(soup, site):
    """
    Extract room-by-room breakdown.
    Returns list of {name, dimensions, description} dicts.
    """
    rooms = []

    # SB / UPS / JM / HC / PP / TR (prop-det-rooms)
    rooms_div = soup.find('div', class_='prop-det-rooms')
    if rooms_div:
        for row in rooms_div.find_all('div', class_='room-row'):
            room = {'name': '', 'dimensions': '', 'description': ''}

            name_span = row.find('span', class_='room-name')
            if name_span:
                # Dimensions are usually in a nested <span>
                dim_span = name_span.find('span')
                if dim_span:
                    room['dimensions'] = _text(dim_span)
                    room['name'] = _text(name_span).replace(room['dimensions'], '').strip()
                else:
                    room['name'] = _text(name_span)

            desc_span = row.find('span', class_='room-desc')
            if desc_span:
                inner = desc_span.find('span')
                room['description'] = _text(inner) if inner else _text(desc_span)

            if room['name'] or room['description']:
                rooms.append(room)

    return rooms


# ──────────────────────────────────────────────
# Main scrape function for a single property
# ──────────────────────────────────────────────

def scrape_property_text(url, site):
    """
    Fetch and parse a property page, returning a dict of text fields.
    No images are downloaded.
    """
    r = fetch(url)
    if not r:
        return None

    soup = BeautifulSoup(r.content, 'html.parser')

    address = extract_address(soup, site)
    price   = extract_price(soup, site)
    info    = extract_info_rows(soup)
    status, bedrooms = extract_status_bedrooms(soup, site, info)
    features    = extract_key_features(soup, site)
    description = extract_description(soup, site)
    rooms       = extract_rooms(soup, site)

    # Title: prefer h1 or first h2
    h1 = soup.find('h1')
    h2 = soup.find('h2')
    title = _text(h1) or _text(h2) or address

    data = {
        'url': url,
        'title': title,
        'address': address,
        'price': price,
        'status': status,
        'property_info': info,
        'key_features': features,
        'description': description,
        'rooms': rooms,
        'rescraped_at': datetime.now().isoformat(),
    }

    # Remove empty values to keep JSON clean
    return {k: v for k, v in data.items() if v not in ('', [], {}, None)}


# ──────────────────────────────────────────────
# Change detection
# ──────────────────────────────────────────────

def detect_changes(old, new):
    """
    Compare old and new dicts for TRACKED_FIELDS.
    Returns list of {field, old, new} dicts.
    """
    changes = []
    for field in TRACKED_FIELDS:
        old_val = old.get(field, '')
        new_val = new.get(field, '')
        if old_val != new_val and new_val:
            changes.append({'field': field, 'old': old_val, 'new': new_val})
    return changes


# ──────────────────────────────────────────────
# Per-source processing
# ──────────────────────────────────────────────

def load_property_jsons(source_key):
    """
    Read all existing property JSON files for a source.
    Returns list of (json_path, data_dict).
    """
    folder = SOURCES[source_key]
    if not os.path.isdir(folder):
        logger.warning(f"Folder not found: {folder}")
        return []

    results = []
    entries = sorted(
        [p for p in os.listdir(folder) if p.startswith('property_')],
        key=lambda x: int(x.split('_')[1]) if x.split('_')[1].isdigit() else 0
    )
    for prop_dir in entries:
        json_path = os.path.join(folder, prop_dir, f"{prop_dir}.json")
        if not os.path.isfile(json_path):
            continue
        try:
            with open(json_path, encoding='utf-8') as f:
                data = json.load(f)
            if data.get('url'):
                results.append((json_path, data))
        except Exception as e:
            logger.warning(f"Could not load {json_path}: {e}")

    return results


def rescrape_one(args):
    """
    Worker function: re-scrape one property.
    args = (source_key, json_path, existing_data, rate_lock)
    Returns (json_path, changes_list_or_None, error_str_or_None)
    """
    source_key, json_path, existing, rate_event = args

    url = existing.get('url', '')
    if not url:
        return (json_path, None, 'No URL in stored JSON')

    # Rate-limit: wait for token from caller
    rate_event.wait()

    new_data = scrape_property_text(url, source_key)
    if not new_data:
        return (json_path, None, f'Failed to fetch {url}')

    changes = detect_changes(existing, new_data)

    # Build the updated JSON:
    # Keep id, scraped_at (first scrape), image metadata etc. from existing
    updated = dict(existing)
    for k, v in new_data.items():
        if k == 'rescraped_at':
            updated['rescraped_at'] = v
        elif k not in ('url', 'id', 'scraped_at'):
            # Track previous values for changed tracked fields
            if k in TRACKED_FIELDS and k in existing and existing[k] != v and v:
                updated[f'_prev_{k}'] = existing[k]
            updated[k] = v

    # Write back
    try:
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(updated, f, indent=2, ensure_ascii=False)
    except Exception as e:
        return (json_path, changes, f'Write error: {e}')

    return (json_path, changes, None)


# ──────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description='Text-only property re-scraper')
    parser.add_argument('sources', nargs='*', default=list(SOURCES.keys()),
                        help='Source codes to re-scrape (default: all)')
    parser.add_argument('--limit', type=int, default=0,
                        help='Max properties per source (0 = unlimited)')
    parser.add_argument('--workers', type=int, default=1,
                        help='Parallel workers (be gentle: default 1)')
    parser.add_argument('--delay', type=float, default=2.0,
                        help='Seconds between requests (default 2.0)')
    args = parser.parse_args()

    sources_to_run = [s for s in args.sources if s in SOURCES]
    if not sources_to_run:
        logger.error(f"Unknown sources. Valid: {list(SOURCES.keys())}")
        return

    logger.info(f"Starting text-only re-scrape. Sources: {sources_to_run}")
    logger.info(f"Workers: {args.workers}, Delay: {args.delay}s, Limit: {args.limit or 'unlimited'}")

    overall_changes = {}

    for source_key in sources_to_run:
        logger.info(f"{'='*60}")
        logger.info(f"Source: {source_key.upper()}")

        properties = load_property_jsons(source_key)
        if not properties:
            logger.warning(f"No properties found for {source_key}")
            continue

        if args.limit:
            properties = properties[:args.limit]

        logger.info(f"  Properties to re-scrape: {len(properties)}")

        source_changes = []
        total = len(properties)

        import threading

        if args.workers == 1:
            # Sequential with simple sleep
            for idx, (json_path, existing) in enumerate(properties, 1):
                logger.info(f"  [{idx}/{total}] {existing.get('url', json_path)}")

                dummy_event = threading.Event()
                dummy_event.set()
                _, changes, error = rescrape_one((source_key, json_path, existing, dummy_event))

                if error:
                    logger.error(f"    Error: {error}")
                elif changes:
                    logger.info(f"    CHANGES: {changes}")
                    source_changes.append({
                        'url': existing.get('url'),
                        'id': existing.get('id'),
                        'changes': changes,
                        'changed_at': datetime.now().isoformat(),
                    })
                else:
                    logger.info(f"    No changes detected")

                if idx < total:
                    time.sleep(args.delay + random.uniform(0, 1))

        else:
            # Parallel workers with rate limiting via events
            # We stagger the initial requests and use a semaphore-like pattern
            import threading

            lock = threading.Lock()
            semaphore = threading.Semaphore(args.workers)

            def rate_limited_fetch(task_args):
                with semaphore:
                    result = rescrape_one(task_args)
                    time.sleep(args.delay)
                    return result

            tasks = []
            for json_path, existing in properties:
                dummy_event = threading.Event()
                dummy_event.set()
                tasks.append((source_key, json_path, existing, dummy_event))

            with ThreadPoolExecutor(max_workers=args.workers) as executor:
                futures = {executor.submit(rate_limited_fetch, t): t for t in tasks}
                completed = 0
                for future in as_completed(futures):
                    completed += 1
                    json_path, changes, error = future.result()
                    task = futures[future]
                    existing = task[2]

                    if error:
                        logger.error(f"  [{completed}/{total}] Error: {error}")
                    elif changes:
                        logger.info(f"  [{completed}/{total}] CHANGES in {existing.get('id')}: {changes}")
                        source_changes.append({
                            'url': existing.get('url'),
                            'id': existing.get('id'),
                            'changes': changes,
                            'changed_at': datetime.now().isoformat(),
                        })
                    else:
                        logger.info(f"  [{completed}/{total}] No changes: {existing.get('id')}")

        # Save per-source changes report
        changes_path = os.path.join(SOURCES[source_key], f"{source_key}_changes.json")
        try:
            # Merge with any existing changes log
            existing_changes = []
            if os.path.isfile(changes_path):
                with open(changes_path, encoding='utf-8') as f:
                    existing_changes = json.load(f)
            existing_changes.extend(source_changes)
            with open(changes_path, 'w', encoding='utf-8') as f:
                json.dump(existing_changes, f, indent=2, ensure_ascii=False)
            logger.info(f"  Changes log: {changes_path} ({len(source_changes)} new entries)")
        except Exception as e:
            logger.error(f"  Could not save changes log: {e}")

        overall_changes[source_key] = len(source_changes)

    logger.info(f"{'='*60}")
    logger.info("Re-scrape complete!")
    for s, count in overall_changes.items():
        logger.info(f"  {s.upper()}: {count} properties with changes")
    logger.info(f"Log file: {log_filename}")


if __name__ == '__main__':
    main()
