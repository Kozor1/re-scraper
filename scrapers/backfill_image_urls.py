#!/usr/bin/env python3
"""
backfill_image_urls.py  –  One-time script to populate image_urls in all
existing property JSON files by re-fetching each property page.

Once this has run, migrate_images.py will store those source URLs directly
in Supabase instead of re-uploading files to Supabase Storage.

Run order:
    1.  python3 scrapers/backfill_image_urls.py        # populate image_urls
    2.  python3 scrapers/delete_storage.py             # wipe Supabase Storage bucket
    3.  python3 supabase/migrate_images.py --new-only  # re-insert as source URLs

Usage:
    python3 scrapers/backfill_image_urls.py              # all sources
    python3 scrapers/backfill_image_urls.py sb tr        # specific sources
    python3 scrapers/backfill_image_urls.py --limit 10   # test on first 10 per source
    python3 scrapers/backfill_image_urls.py --force      # re-fetch even if image_urls exists
"""

import os
import re
import sys
import json
import time
import random
import logging
import argparse
from datetime import datetime
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup


def _sort_and_dedup_image_urls_sb(urls):
    """Sort SB image URLs by their _NN suffix and remove duplicates."""
    def _img_num(url):
        m = re.search(r'_(\d+)\.(jpg|jpeg|png|webp)$', url, re.IGNORECASE)
        return int(m.group(1)) if m else 999999
    seen = set(); deduped = []
    for url in urls:
        if url not in seen:
            seen.add(url); deduped.append(url)
    deduped.sort(key=_img_num)
    return deduped

# ── Config ────────────────────────────────────────────────────────────────────

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT       = os.path.dirname(SCRIPT_DIR)

SOURCES = {
    'sb':  {'dir': os.path.join(ROOT, 'properties', 'sb'),  'style': 'sb'},
    'ups': {'dir': os.path.join(ROOT, 'properties', 'ups'), 'style': 'gallery-img'},
    'hc':  {'dir': os.path.join(ROOT, 'properties', 'hc'),  'style': 'gallery-img'},
    'jm':  {'dir': os.path.join(ROOT, 'properties', 'jm'),  'style': 'gallery-img'},
    'pp':  {'dir': os.path.join(ROOT, 'properties', 'pp'),  'style': 'gallery-img'},
    'dh':  {'dir': os.path.join(ROOT, 'properties', 'dh'),  'style': 'dh-copy'},
    'mm':  {'dir': os.path.join(ROOT, 'properties', 'mm'),  'style': 'gallery-img'},
    'ce':  {'dir': os.path.join(ROOT, 'properties', 'ce'),  'style': 'gallery-img'},
    # TR is JavaScript-rendered — handled separately via selenium (see backfill_source_tr_selenium)
    'tr':  {'dir': os.path.join(ROOT, 'properties', 'tr'),  'style': 'selenium'},
}

HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
        '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    ),
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-GB,en;q=0.9',
}

DELAY = 1.5   # seconds between requests

# ── Logging ───────────────────────────────────────────────────────────────────

os.makedirs(os.path.join(ROOT, 'logs'), exist_ok=True)
log_filename = os.path.join(
    ROOT, 'logs',
    f"backfill_image_urls_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename, encoding='utf-8'),
        logging.StreamHandler(),
    ]
)
logger = logging.getLogger(__name__)

# ── HTTP fetch ────────────────────────────────────────────────────────────────

def fetch(url, retries=3):
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=HEADERS, timeout=25)
            r.raise_for_status()
            return r
        except requests.exceptions.RequestException as e:
            logger.warning(f"  [attempt {attempt+1}] {url}: {e}")
            if attempt < retries - 1:
                time.sleep((2 ** attempt) * random.uniform(1.5, 3.0))
    return None

# ── Image URL extraction (per-source) ────────────────────────────────────────

def extract_image_urls_sb(soup, page_url):
    """
    Simon Brien: gallery uses <ul id='gallery'> with <a href='...'>
    The href on each link is the full-size image URL.
    URLs are sorted by their _NN suffix to correct carousel-loop ordering.
    """
    urls = []
    gallery = soup.find('ul', id='gallery')
    if gallery:
        for a in gallery.find_all('a', href=True):
            full = urljoin(page_url, a['href'])
            urls.append(full)
    return _sort_and_dedup_image_urls_sb(urls)


def extract_image_urls_gallery_img(soup, page_url):
    """
    UPS / HC / JM / PP / MM / CE and similar PropertyPal CMS sites.

    Selector priority:
      1. ul#pphoto  — native PropertyPal platform gallery (<a href> = full-size)
      2. ul#gallery / div#gallery / div.gallery  — common licensed CMS variant
      3. div#propphoto / div.propphoto
      4. Inline <script> JSON blobs  — JS-rendered galleries
      5. Any <img> whose src path looks like a property photo
    """
    import re as _re

    urls = []

    def add(src):
        if src:
            full = urljoin(page_url, src)
            if full not in urls and full.startswith('http'):
                urls.append(full)

    # ── 1. ul#pphoto (native PropertyPal) ─────────────────────────────────────
    pphoto = soup.find('ul', id='pphoto')
    if pphoto:
        for a in pphoto.find_all('a', href=True):
            href = a['href']
            if any(ext in href.lower() for ext in ('.jpg', '.jpeg', '.png', '.webp')):
                add(href)
        if not urls:   # hrefs may be lightbox anchors — fall back to img src
            for img in pphoto.find_all('img'):
                add(img.get('src') or img.get('data-src') or img.get('data-lazy-src'))
        if urls:
            return urls

    # ── 2. ul#gallery / div#gallery / div.gallery ─────────────────────────────
    gallery = (
        soup.find('ul',  id='gallery') or
        soup.find('div', id='gallery') or
        soup.find('div', class_='gallery')
    )
    if gallery:
        for a in gallery.find_all('a', href=True):
            href = a['href']
            if any(ext in href.lower() for ext in ('.jpg', '.jpeg', '.png', '.webp')):
                add(href)
        for img in gallery.find_all('img'):
            add(img.get('src') or img.get('data-src') or img.get('data-lazy-src'))
        if urls:
            return urls

    # ── 3. div#propphoto / div.propphoto ──────────────────────────────────────
    propphoto = soup.find(id='propphoto') or soup.find(class_='propphoto')
    if propphoto:
        for a in propphoto.find_all('a', href=True):
            add(a['href'])
        for img in propphoto.find_all('img'):
            add(img.get('src') or img.get('data-src'))
        if urls:
            return urls

    # ── 4. JSON in <script> tags (JS-rendered galleries) ──────────────────────
    for script in soup.find_all('script'):
        text = script.string or ''
        found = _re.findall(
            r'["\']('
            r'(?:https?://[^"\']+)?'
            r'/(?:images?|photos?|property-images?|uploads?)/[^"\']+\.(?:jpe?g|png|webp)'
            r')["\']',
            text, _re.I
        )
        for f in found:
            add(f if f.startswith('http') else urljoin(page_url, f))
        if urls:
            return urls

    # ── 5. Fallback: any img whose URL path looks like a property photo ────────
    for img in soup.find_all('img'):
        src = img.get('src') or img.get('data-src') or img.get('data-lazy-src')
        if not src:
            continue
        full = urljoin(page_url, src)
        if (any(p in full for p in ('/images/property/', '/property-images/',
                                     '/property_images/', '/uploads/property/'))
                and not any(x in full.lower() for x in ('logo', 'office', 'icon', 'favicon'))):
            add(full)

    return urls


def extract_image_urls_dh(soup, page_url):
    """
    Daniel Henry: images are served from media.propertypal.com/sd/<width>/...
    The <img> src contains the base (small) size; srcset lists larger widths.
    We prefer the largest available size (900w or 1024w), falling back to src.

    Note: DH stores images under data['images'], not data['image_urls'], but
    this function just returns the URL list — the caller writes to image_urls.
    """
    seen = set()
    urls = []

    for img in soup.select('img[src*="media.propertypal.com/sd/"]'):
        src = img.get('src', '').strip()
        if not src or src in seen:
            continue
        seen.add(src)

        best = src
        best_width = 0

        srcset = img.get('srcset', '')
        if srcset:
            for part in srcset.split(','):
                part = part.strip()
                if not part:
                    continue
                pieces = part.split()
                candidate_url = pieces[0]
                width = 0
                if len(pieces) >= 2:
                    try:
                        width = int(pieces[1].rstrip('w'))
                    except ValueError:
                        pass
                if width > best_width:
                    best_width = width
                    best = candidate_url

        if best not in urls:
            urls.append(best)

    return urls


def extract_image_urls(soup, page_url, style):
    if style == 'sb':
        return extract_image_urls_sb(soup, page_url)
    elif style == 'dh':
        return extract_image_urls_dh(soup, page_url)
    else:
        return extract_image_urls_gallery_img(soup, page_url)

# ── TR selenium backfill (TR is JS-rendered; static requests can't see gallery) ───

def backfill_source_tr_selenium(force=False, limit=0):
    """
    Use headless Chrome (via tr_full_scrape.py's helpers) to extract image URLs
    for existing TR property JSON files that are missing them.
    """
    try:
        sys.path.insert(0, SCRIPT_DIR)
        from tr_full_scrape import make_driver, load_page
        from tr_full_scrape import extract_image_urls as tr_extract_image_urls
    except ImportError as e:
        logger.error(f"Cannot import TR selenium helpers: {e}")
        logger.error("Install dependencies: pip install selenium webdriver-manager")
        return 0, 0, 0

    props_dir = SOURCES['tr']['dir']
    if not os.path.isdir(props_dir):
        logger.warning(f"  TR: directory not found ({props_dir})")
        return 0, 0, 0

    dirs = sorted(
        [d for d in os.listdir(props_dir)
         if d.startswith('property_') and os.path.isdir(os.path.join(props_dir, d))],
        key=lambda x: int(x.replace('property_', ''))
    )

    total = len(dirs)
    updated = 0
    skipped = 0
    errors = 0

    # Build the list of properties that still need processing
    to_process = []
    for prop_dir_name in dirs:
        json_path = os.path.join(props_dir, prop_dir_name, f'{prop_dir_name}.json')
        if not os.path.isfile(json_path):
            continue
        try:
            with open(json_path, encoding='utf-8') as f:
                data = json.load(f)
        except Exception:
            errors += 1
            continue
        if data.get('image_urls') and not force:
            skipped += 1
            continue
        url = data.get('url')
        if not url:
            skipped += 1
            continue
        to_process.append((prop_dir_name, json_path, data, url))
        if limit and len(to_process) >= limit:
            break

    logger.info(
        f"  TR: {total} total, {len(to_process)} need selenium image backfill, "
        f"{skipped} already done"
    )

    if not to_process:
        return total, 0, 0

    driver = make_driver()
    try:
        for idx, (prop_dir_name, json_path, data, url) in enumerate(to_process, 1):
            logger.info(f"  [{idx}/{len(to_process)}] {prop_dir_name}: {url}")

            # Restart driver every 50 properties to free memory
            if idx > 1 and (idx - 1) % 50 == 0:
                logger.info("  Restarting Chrome to free memory…")
                try:
                    driver.quit()
                except Exception:
                    pass
                driver = make_driver()

            ok = load_page(driver, url)
            if not ok:
                logger.error(f"  Failed to load page for {prop_dir_name}")
                errors += 1
                continue

            img_urls = tr_extract_image_urls(driver, url)
            if img_urls:
                data['image_urls'] = img_urls
                try:
                    with open(json_path, 'w', encoding='utf-8') as f:
                        json.dump(data, f, indent=2, ensure_ascii=False)
                    logger.info(f"    Saved {len(img_urls)} image URLs")
                    updated += 1
                except Exception as e:
                    logger.error(f"    Write error: {e}")
                    errors += 1
            else:
                logger.warning(f"    No image URLs found for {prop_dir_name}")
                updated += 1  # Count as processed even if no images found

            time.sleep(random.uniform(1.5, 2.5))
    finally:
        try:
            driver.quit()
        except Exception:
            pass

    return total, updated, errors


# ── Per-source backfill ───────────────────────────────────────────────────────

def backfill_source(source_key, force=False, limit=0):
    cfg = SOURCES[source_key]
    props_dir = cfg['dir']
    style     = cfg['style']

    if not os.path.isdir(props_dir):
        logger.warning(f"  {source_key.upper()}: directory not found ({props_dir})")
        return 0, 0, 0

    # Collect all property directories
    dirs = sorted(
        [d for d in os.listdir(props_dir)
         if d.startswith('property_') and os.path.isdir(os.path.join(props_dir, d))],
        key=lambda x: int(x.replace('property_', ''))
    )

    total     = len(dirs)
    updated   = 0
    skipped   = 0
    errors    = 0

    logger.info(f"  {source_key.upper()}: {total} properties")

    for idx, prop_dir_name in enumerate(dirs, 1):
        if limit and updated + skipped >= limit:
            logger.info(f"  Reached --limit {limit}")
            break

        json_path = os.path.join(props_dir, prop_dir_name, f'{prop_dir_name}.json')
        if not os.path.isfile(json_path):
            continue

        try:
            with open(json_path, encoding='utf-8') as f:
                data = json.load(f)
        except Exception as e:
            logger.error(f"  Could not read {json_path}: {e}")
            errors += 1
            continue

        # Skip if already done (unless --force)
        if data.get('image_urls') and not force:
            skipped += 1
            continue

        # ── DH-copy: images were already scraped by dh_scrape.py under 'images' key ──
        # dh_scrape.py uses Selenium and stores results in data['images'], not
        # data['image_urls'].  No re-fetch needed — just copy the key over.
        if style == 'dh-copy':
            img_urls = data.get('images', [])
            if img_urls:
                data['image_urls'] = img_urls
                try:
                    with open(json_path, 'w', encoding='utf-8') as f:
                        json.dump(data, f, indent=2, ensure_ascii=False)
                    logger.info(f"  [{idx}/{total}] {prop_dir_name}: copied {len(img_urls)} images → image_urls")
                    updated += 1
                except Exception as e:
                    logger.error(f"  {prop_dir_name}: write error: {e}")
                    errors += 1
            else:
                logger.warning(f"  [{idx}/{total}] {prop_dir_name}: no images in JSON (dh_scrape may need re-run)")
                skipped += 1
            continue   # never hits the HTTP fetch below

        url = data.get('url')
        if not url:
            logger.warning(f"  {prop_dir_name}: no URL in JSON, skipping")
            skipped += 1
            continue

        logger.info(f"  [{idx}/{total}] {prop_dir_name}: {url}")

        r = fetch(url)
        if not r:
            logger.error(f"  {prop_dir_name}: fetch failed")
            errors += 1
            time.sleep(DELAY)
            continue

        soup = BeautifulSoup(r.content, 'html.parser')
        img_urls = extract_image_urls(soup, url, style)

        if img_urls:
            data['image_urls'] = img_urls
            try:
                with open(json_path, 'w', encoding='utf-8') as f:
                    json.dump(data, f, indent=2, ensure_ascii=False)
                logger.info(f"    Saved {len(img_urls)} image URLs")
                updated += 1
            except Exception as e:
                logger.error(f"    Write error: {e}")
                errors += 1
        else:
            logger.warning(f"    No image URLs found")
            updated += 1   # Still count as processed (property may have no images)

        time.sleep(DELAY + random.uniform(0, 0.5))

    return total, updated, errors

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description=(
            'Backfill image_urls into existing property JSON files by '
            're-fetching each property page.'
        )
    )
    parser.add_argument('sources', nargs='*', default=list(SOURCES.keys()),
                        help='Sources to process (default: all)')
    parser.add_argument('--force', action='store_true',
                        help='Re-fetch even if image_urls already exists in JSON')
    parser.add_argument('--limit', type=int, default=0,
                        help='Max properties per source to process (0 = all)')
    args = parser.parse_args()

    sources_to_run = [s for s in args.sources if s in SOURCES]
    unknown = [s for s in args.sources if s not in SOURCES]
    if unknown:
        logger.warning(f"Unknown sources (ignored): {unknown}")
    if not sources_to_run:
        logger.error("No valid sources.")
        sys.exit(1)

    logger.info(f"backfill_image_urls starting. Sources: {sources_to_run}")
    logger.info(f"Log: {log_filename}")

    overall = {}
    for source_key in sources_to_run:
        logger.info(f"{'='*60}")
        logger.info(f"Source: {source_key.upper()}")
        if SOURCES[source_key]['style'] == 'selenium':
            # TR requires headless Chrome because its pages are JavaScript-rendered
            total, updated, errors = backfill_source_tr_selenium(
                force=args.force, limit=args.limit
            )
        else:
            total, updated, errors = backfill_source(source_key, force=args.force, limit=args.limit)
        overall[source_key] = {'total': total, 'updated': updated, 'errors': errors}

    logger.info(f"{'='*60}")
    logger.info("backfill_image_urls complete:")
    for source_key, s in overall.items():
        logger.info(
            f"  {source_key.upper():4s}  total={s['total']}  "
            f"updated={s['updated']}  errors={s['errors']}"
        )
    logger.info(
        "\nNext steps:\n"
        "  1. python3 scrapers/delete_storage.py          (wipe Supabase Storage)\n"
        "  2. python3 supabase/migrate_images.py           (re-insert as source URLs)"
    )


if __name__ == '__main__':
    main()
