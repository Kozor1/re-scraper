#!/usr/bin/env python3
"""
Pinpoint Property full scraper with smart-update logic.
SOURCE KEY: pinp
SALE:  https://pinpointproperty.com/property-for-sale
NOTE:  Pinpoint does not appear to have a lettings section.

Uses the PropertyPal/BlueCubes CMS common to JM, PP, SB, UPS, HC and R&B.

Smart update behaviour:
  - New listings  → scrape full detail page and save as property_N
  - Existing      → skip (use --rescrape to force re-check)
  - Delisted      → delete local folder and url_map entry (skipped in --quick mode)

Usage (run from re_app/ directory):
    python3 scrapers/pinp_full_scrape.py              # full update
    python3 scrapers/pinp_full_scrape.py --quick      # fast scan: new only, early stop
    python3 scrapers/pinp_full_scrape.py --limit 5    # stop after 5 new scrapes
    python3 scrapers/pinp_full_scrape.py --rescrape   # re-scrape existing too
    python3 scrapers/pinp_full_scrape.py --test       # scrape first 1 new property only

Requires: pip3 install requests beautifulsoup4
"""

import os, sys, json, re, time, random, argparse, shutil, logging
from datetime import datetime
from urllib.parse import urljoin, urlparse

try:
    import requests
    from bs4 import BeautifulSoup
except ImportError:
    print("Run: pip3 install requests beautifulsoup4")
    sys.exit(1)

# ── Config ─────────────────────────────────────────────────────────────────────

SOURCE_KEY = 'pinp'
BASE_URL   = 'https://pinpointproperty.com'
LIST_URL   = 'https://pinpointproperty.com/property-for-sale'
# Pagination: /property-for-sale/page2/, /property-for-sale/page3/, …

ROOT     = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PROP_DIR = os.path.join(ROOT, 'properties', SOURCE_KEY)
MAP_PATH = os.path.join(PROP_DIR, 'url_map.json')
LOGS_DIR = os.path.join(ROOT, 'logs')

DELAY            = 1.5
MAX_RETRIES      = 3
QUICK_STOP_AFTER = 5   # consecutive known URLs before stopping pagination in --quick mode

# ── Logging ────────────────────────────────────────────────────────────────────

os.makedirs(PROP_DIR, exist_ok=True)
os.makedirs(LOGS_DIR, exist_ok=True)

log_file = os.path.join(
    LOGS_DIR,
    f"pinp_scraper_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[
        logging.FileHandler(log_file, encoding='utf-8'),
        logging.StreamHandler(),
    ]
)
logger = logging.getLogger(__name__)

HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
        'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    ),
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-GB,en;q=0.9',
}

# ── HTTP helpers ───────────────────────────────────────────────────────────────

def fetch(url, retries=MAX_RETRIES):
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=HEADERS, timeout=25)
            r.raise_for_status()
            return r
        except Exception as e:
            logger.warning(f"  attempt {attempt+1} failed for {url}: {e}")
            if attempt < retries - 1:
                time.sleep(3 * (attempt + 1))
    return None

# ── URL map ────────────────────────────────────────────────────────────────────

def load_url_map():
    if os.path.exists(MAP_PATH):
        try:
            return json.load(open(MAP_PATH, encoding='utf-8'))
        except Exception:
            pass
    return {}

def save_url_map(url_map):
    with open(MAP_PATH, 'w', encoding='utf-8') as f:
        json.dump(url_map, f, indent=2, ensure_ascii=False)

def next_property_id(url_map):
    existing = [
        int(v.replace('property_', ''))
        for v in url_map.values()
        if re.fullmatch(r'property_\d+', v)
    ]
    for d in os.listdir(PROP_DIR):
        m = re.fullmatch(r'property_(\d+)', d)
        if m:
            existing.append(int(m.group(1)))
    return max(existing, default=0) + 1

# ── Listing page: collect live URLs ───────────────────────────────────────────

def is_property_url(href):
    """
    Pinpoint uses PropertyPal-style URLs:
      /property/{area}/{pq-id}/{address-slug}/
    e.g. /property/antrim-road/pq152858/2-innisfayle-drive/
    """
    return bool(re.search(r'/property/[^/]+/[a-z]{1,4}\d{4,}/', href))

def collect_live_urls(url_map, quick=False):
    live_urls = {}
    page = 1
    consecutive_known = 0

    while True:
        url = LIST_URL if page == 1 else f"{LIST_URL}/page{page}/"
        logger.info(f"  Listing page {page}: {url}")
        r = fetch(url)
        if not r:
            logger.warning(f"  Could not fetch listing page {page} — stopping")
            break

        soup = BeautifulSoup(r.content, 'html.parser')
        new_this_page = 0

        for a in soup.find_all('a', href=True):
            href = a['href']
            if not is_property_url(href):
                continue
            canonical = urljoin(BASE_URL, href.split('?')[0].rstrip('/'))
            if canonical not in live_urls:
                addr  = ''
                price = ''
                parent = a
                for _ in range(5):
                    parent = parent.parent
                    if parent is None:
                        break
                    h_el = parent.find(['h2', 'h3', 'h4'])
                    if h_el and h_el.get_text(strip=True):
                        addr = h_el.get_text(strip=True)
                    p_el = parent.find(class_=re.compile(r'price', re.I))
                    if p_el and p_el.get_text(strip=True):
                        price = p_el.get_text(strip=True)
                    if addr:
                        break

                live_urls[canonical] = {'address': addr, 'price_str': price}
                new_this_page += 1

                if quick and canonical in url_map:
                    consecutive_known += 1
                else:
                    consecutive_known = 0

                if quick and consecutive_known >= QUICK_STOP_AFTER:
                    logger.info(f"  [quick] {QUICK_STOP_AFTER} consecutive known — stopping")
                    break

        logger.info(f"  → {new_this_page} new URLs (total: {len(live_urls)})")

        if quick and consecutive_known >= QUICK_STOP_AFTER:
            break

        if new_this_page == 0:
            logger.info("  No new URLs — end of pagination")
            break

        page += 1
        time.sleep(DELAY + random.uniform(0, 0.5))

    return live_urls

# ── Detail page parser ─────────────────────────────────────────────────────────

def normalise_status(raw):
    s = (raw or '').strip().lower()
    if 'agreed' in s: return 'Sale Agreed'
    if 'sold'   in s: return 'Sold'
    return 'For Sale'

def extract_property_id(url):
    """Extract the property ID slug from the URL (e.g. 'pq152858')."""
    m = re.search(r'/property/[^/]+/([^/]+)/', url)
    return m.group(1) if m else ''

def extract_image_urls(soup, url):
    seen = set()
    urls = []

    def add(src):
        if src:
            full = urljoin(url, src) if not src.startswith('http') else src
            if full not in seen and full.startswith('http'):
                seen.add(full)
                urls.append(full)

    # Primary: ul#gallery or div#gallery — full-size <a href> links
    gallery = soup.find('ul', id='gallery') or soup.find('div', id='gallery')
    if gallery:
        real_links = [
            a for a in gallery.find_all('a', href=True)
            if 'slick-cloned' not in (a.get('class') or [])
        ]
        for a in real_links:
            add(a['href'])
        if urls:
            return urls

    # Fallback: construct image URLs from known pattern
    prop_id = extract_property_id(url)
    if prop_id:
        base_img = f'/images/property/1/{prop_id}/'
        for a in soup.find_all('a', href=True):
            href = a['href']
            if base_img in href or (prop_id in href and
                    any(href.lower().endswith(ext) for ext in ('.jpg', '.jpeg', '.png', '.webp'))):
                add(href)
        if urls:
            return urls

    # Last fallback: any img with /images/property/ path
    for img in soup.find_all('img'):
        src = img.get('src') or img.get('data-src') or img.get('data-lazy-src')
        if src and '/images/property/' in src:
            if not any(x in src.lower() for x in ('logo', 'office', 'icon', 'favicon')):
                add(src)

    return urls

def parse_detail(html, url):
    soup = BeautifulSoup(html, 'html.parser')
    data = {'url': url, 'scraped_at': datetime.now().isoformat()}

    # Address
    h1 = soup.find('h1')
    if h1:
        data['address'] = h1.get_text(separator=' ', strip=True)
    if not data.get('address'):
        title_tag = soup.find('title')
        if title_tag:
            t = title_tag.get_text(strip=True)
            for suffix in [
                ' for sale with Pinpoint', ' | Pinpoint Property',
                ' - Pinpoint Property', ' | Pinpoint', ' - Pinpoint'
            ]:
                t = t.replace(suffix, '')
            data['address'] = t.strip()
    data.setdefault('address', '')
    data['title'] = data['address']

    # Price — BlueCubes CMS style: span.prop-det-price-amount + span.prop-det-price-text
    amount_el = soup.select_one('span.prop-det-price-amount')
    if amount_el:
        amount = amount_el.get_text(strip=True)
        qualifier_el = soup.select_one('span.prop-det-price-text')
        qualifier = (qualifier_el.get_text(strip=True) + ' ') if qualifier_el else ''
        price = (qualifier + amount).strip()
        # Ensure a space before £ — some sites concatenate qualifier+amount without one
        data['price_str'] = re.sub(r'([A-Za-z])(£)', r'\1 \2', price)
    if not data.get('price_str'):
        # og:description often starts with £NNN,NNN
        og_desc = soup.find('meta', property='og:description')
        if og_desc and og_desc.get('content'):
            m = re.search(r'(£[\d,]+(?:pm|pcm)?|POA)', og_desc['content'], re.I)
            if m:
                data['price_str'] = m.group(1)

    # Metadata rows — BlueCubes CMS style: div.prop-det-info-row
    for row in soup.select('div.prop-det-info-row'):
        left  = row.find(class_='prop-det-info-left')
        right = row.find(class_='prop-det-info-right')
        if not left or not right:
            continue
        key = left.get_text(strip=True).lower()
        val = right.get_text(strip=True)
        if 'status' in key:
            data['status'] = normalise_status(val)
        elif 'style' in key or 'type' in key:
            data['type'] = val
        elif 'bedroom' in key:
            data['bedrooms'] = val
        elif 'reception' in key:
            data['receptions'] = val
        elif 'bathroom' in key:
            data['bathrooms'] = val
        elif 'price' in key:
            data.setdefault('price_str', val)

    # Status fallback
    if not data.get('status'):
        data['status'] = normalise_status('')

    # Key features
    feats = []
    for sel in ['ul.feats li', 'div.prop-det-feats .feat',
                '.DescriptionBox--bullets li', '.DescriptionBox--bullets p',
                'ul.features li', '.key-features li']:
        feats = [el.get_text(strip=True) for el in soup.select(sel) if el.get_text(strip=True)]
        if feats:
            break
    data['key_features'] = feats

    # Description
    desc = ''
    for sel in ['div.textbp', 'div.prop-det-text .text', '.ListingDescr-text',
                'div.description', '.property-description']:
        el = soup.select_one(sel)
        if el:
            t = el.get_text(separator=' ', strip=True)
            if len(t) > len(desc):
                desc = t
    data['description'] = desc

    # Rooms
    rooms = []
    for room_row in soup.select('div.prop-det-rooms div.room-row, div.room-row'):
        room_name = room_row.find('span', class_='room-name')
        room_desc = room_row.find('span', class_='room-desc')
        room_data = {'name': '', 'dimensions': '', 'description': ''}
        if room_name:
            dim_span = room_name.find('span')
            if dim_span:
                room_data['dimensions'] = dim_span.get_text(strip=True)
                room_data['name'] = room_name.get_text(strip=True).replace(
                    dim_span.get_text(strip=True), '').strip()
            else:
                room_data['name'] = room_name.get_text(strip=True)
        if room_desc:
            desc_span = room_desc.find('span')
            room_data['description'] = (
                desc_span.get_text(strip=True) if desc_span else room_desc.get_text(strip=True)
            )
        if room_data['name']:
            rooms.append(room_data)
    data['rooms'] = rooms

    # Images
    data['image_urls'] = extract_image_urls(soup, url)

    return data

# ── Scrape one property and save ───────────────────────────────────────────────

def scrape_and_save(url, folder_name, listing_data=None):
    r = fetch(url)
    if not r:
        logger.error(f"    fetch failed for {url}")
        return None

    data = parse_detail(r.text, url)

    # Apply listing-page fallbacks for fields the detail page failed to parse
    if listing_data:
        if not data.get('price_str') and listing_data.get('price_str'):
            data['price_str'] = listing_data['price_str']
            logger.debug(f"    price from listing page: {data['price_str']}")

    prop_dir = os.path.join(PROP_DIR, folder_name)
    os.makedirs(prop_dir, exist_ok=True)

    # Download images
    image_count = 0
    for i, img_url in enumerate(data.get('image_urls', []), 1):
        try:
            img_r = fetch(img_url)
            if img_r:
                ext = os.path.splitext(urlparse(img_url).path)[1] or '.jpg'
                img_path = os.path.join(prop_dir, f'img{i}{ext}')
                with open(img_path, 'wb') as f:
                    f.write(img_r.content)
                image_count += 1
        except Exception as e:
            logger.debug(f"    image download failed: {img_url}: {e}")

    jpath = os.path.join(prop_dir, f'{folder_name}.json')
    with open(jpath, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    beds     = data.get('bedrooms', '?')
    status   = data.get('status', '?')
    imgs     = len(data.get('image_urls', []))
    desc_len = len(data.get('description', ''))
    logger.info(
        f"    ✓ saved: beds={beds}  status={status}  "
        f"images={imgs}  desc={desc_len}ch"
    )
    return data

def delete_property(folder_name):
    prop_dir = os.path.join(PROP_DIR, folder_name)
    if os.path.isdir(prop_dir):
        shutil.rmtree(prop_dir)
        logger.info(f"    🗑  deleted {folder_name}/")

# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description='Pinpoint Property smart scraper')
    parser.add_argument('--limit',    type=int, default=0,
                        help='Max new properties to scrape (0 = all)')
    parser.add_argument('--rescrape', action='store_true',
                        help='Re-scrape all existing properties')
    parser.add_argument('--test',     action='store_true',
                        help='Scrape only the first 1 new property')
    parser.add_argument('--quick',    action='store_true',
                        help='Quick mode: early stop on consecutive known URLs, '
                             'skip stale detection')
    parser.add_argument('--fresh',    action='store_true',
                        help='Clear all data and start from scratch')
    args = parser.parse_args()

    if args.test:
        args.limit = 1

    if args.fresh:
        if os.path.exists(PROP_DIR):
            logger.info(f"--fresh: clearing {PROP_DIR}/")
            shutil.rmtree(PROP_DIR)
        os.makedirs(PROP_DIR, exist_ok=True)

    logger.info('=' * 60)
    logger.info(f"Pinpoint Property scraper — {datetime.now().isoformat()}")
    logger.info(f"Properties dir: {PROP_DIR}")
    logger.info(f"Options: limit={args.limit or 'none'}  rescrape={args.rescrape}  "
                f"quick={args.quick}")

    url_map = load_url_map()

    logger.info("\n[1/3] Collecting live property URLs…")
    live_urls = collect_live_urls(url_map, quick=args.quick)
    logger.info(f"  Found {len(live_urls)} live properties on website")

    if len(live_urls) == 0:
        logger.error("ABORT: 0 live URLs — site may be unreachable.")
        return

    live_set      = set(live_urls.keys())
    local_set     = set(url_map.keys())
    new_urls      = sorted(live_set - local_set)
    deleted_urls  = sorted(local_set - live_set)
    existing_urls = sorted(live_set & local_set)

    logger.info(f"\n[2/3] Diff: {len(new_urls)} new, "
                f"{len(deleted_urls)} delisted, "
                f"{len(existing_urls)} unchanged")

    added = removed = updated = errors = 0

    # Delete stale (skip in quick mode)
    if deleted_urls and not args.quick:
        logger.info(f"\nDeleting {len(deleted_urls)} delisted properties…")
        for url in deleted_urls:
            folder = url_map.pop(url)
            logger.info(f"  {folder}  {url}")
            delete_property(folder)
            removed += 1
        save_url_map(url_map)
    elif deleted_urls and args.quick:
        logger.info(f"  [quick] Skipping stale deletion of {len(deleted_urls)} properties")

    # Scrape new
    if new_urls:
        limit     = args.limit if args.limit else len(new_urls)
        to_scrape = new_urls[:limit]
        logger.info(f"\n[3/3] Scraping {len(to_scrape)} new properties…")
        next_id = next_property_id(url_map)

        for i, url in enumerate(to_scrape, 1):
            folder = f"property_{next_id}"
            next_id += 1
            logger.info(f"  [{i}/{len(to_scrape)}] NEW {folder}: {url}")
            data = scrape_and_save(url, folder, listing_data=live_urls.get(url))
            if data:
                url_map[url] = folder
                save_url_map(url_map)
                added += 1
            else:
                errors += 1
            time.sleep(DELAY + random.uniform(0, 0.5))

    # Re-scrape existing (optional)
    if args.rescrape and existing_urls:
        logger.info(f"\nRe-scraping {len(existing_urls)} existing properties…")
        for i, url in enumerate(existing_urls, 1):
            folder = url_map[url]
            logger.info(f"  [{i}/{len(existing_urls)}] UPDATE {folder}: {url}")
            data = scrape_and_save(url, folder, listing_data=live_urls.get(url))
            if data:
                updated += 1
            else:
                errors += 1
            time.sleep(DELAY + random.uniform(0, 0.5))

    logger.info(f"\n{'='*60}")
    logger.info(
        f"Done — added={added}  removed={removed}  "
        f"updated={updated}  errors={errors}"
    )
    logger.info(f"Log: {log_file}")


if __name__ == '__main__':
    main()
