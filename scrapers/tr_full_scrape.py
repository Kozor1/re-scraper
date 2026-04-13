"""
tr_full_scrape.py  –  Full scraper for Templeton Robinson properties.

Uses headless Chrome (via Selenium) to load each property detail page so that
JavaScript-rendered content (description, key features, rooms) is available.
Listing pages and image downloads still use plain requests — only the property
detail page requires a browser.

Usage:
    python3 tr_full_scrape.py          # scrape all properties
    python3 tr_full_scrape.py --test   # test on one property, verbose output
    python3 tr_full_scrape.py --limit 10  # cap to first 10 new properties
"""

import requests
from bs4 import BeautifulSoup
import time
import random
import os
import re
import sys
import json
import shutil
import logging
import argparse
from datetime import datetime
from urllib.parse import urljoin, urlparse

# ── Selenium imports (required) ───────────────────────────────────────────────
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

# ── Config ────────────────────────────────────────────────────────────────────

SCRAPERS_DIR = os.path.dirname(os.path.abspath(__file__))   # re_app/scrapers/
SCRIPT_DIR   = os.path.dirname(SCRAPERS_DIR)                 # re_app/
TR_DIR       = os.path.join(SCRIPT_DIR, 'properties', 'tr')
INDEX_PATH   = os.path.join(TR_DIR, 'property_index.json')
SUMMARY_PATH = os.path.join(TR_DIR, 'summary.json')

BASE_URL = 'https://www.templetonrobinson.com/property-for-sale/page{page}/'

# Restart Chrome every N properties to prevent memory build-up
RESTART_EVERY = 50

os.makedirs(TR_DIR, exist_ok=True)
os.makedirs(os.path.join(SCRAPERS_DIR, 'logs'), exist_ok=True)

# ── Logging ───────────────────────────────────────────────────────────────────

log_filename = os.path.join(
    SCRAPERS_DIR, 'logs',
    f"tr_scraper_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
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

# ── HTTP helpers (for listing pages and image downloads) ──────────────────────

HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
        '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    ),
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-GB,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
    'Cache-Control': 'max-age=0',
}


def fetch(url, max_retries=3):
    """GET with retry and exponential back-off."""
    for attempt in range(max_retries):
        try:
            r = requests.get(url, headers=HEADERS, timeout=30)
            r.raise_for_status()
            return r
        except requests.exceptions.RequestException as e:
            logger.warning(f"[attempt {attempt+1}] {url} — {e}")
            if attempt < max_retries - 1:
                time.sleep((2 ** attempt) * random.uniform(2, 4))
            else:
                logger.error(f"Giving up on {url}")
                return None


def download_image(img_url, folder, img_num):
    """Download a single image and save it to disk. Returns saved path or None."""
    try:
        r = fetch(img_url)
        if r and r.status_code == 200:
            ext = os.path.splitext(urlparse(img_url).path)[1] or '.jpg'
            path = os.path.join(folder, f'img{img_num}{ext}')
            with open(path, 'wb') as f:
                f.write(r.content)
            logger.info(f"    Downloaded: {path}")
            return path
    except Exception as e:
        logger.error(f"    Error downloading image {img_url}: {e}")
    return None

# ── Listing-page link extraction (static HTML — no JS needed) ─────────────────

def extract_property_links(soup, page_url):
    """Extract individual property detail URLs from a listing page."""
    links = []
    for a in soup.find_all('a', href=True):
        href = a['href']
        if '/property/' in href and '/property-for-sale/' not in href:
            full = urljoin(page_url, href).split('?')[0].rstrip('/')
            if full not in links:
                links.append(full)
    return links

# ── Selenium driver ───────────────────────────────────────────────────────────

def make_driver():
    """Create a headless Chrome WebDriver."""
    opts = Options()
    opts.add_argument('--headless=new')
    opts.add_argument('--no-sandbox')
    opts.add_argument('--disable-dev-shm-usage')
    opts.add_argument('--disable-gpu')
    opts.add_argument('--window-size=1280,900')
    opts.add_argument(
        '--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
        'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    )
    opts.add_experimental_option('excludeSwitches', ['enable-logging'])
    service = Service(ChromeDriverManager().install())
    driver  = webdriver.Chrome(service=service, options=opts)
    driver.set_page_load_timeout(30)
    return driver


def load_page(driver, url):
    """
    Load a TR property page and wait for meaningful content to appear.
    Returns True on success.
    """
    try:
        driver.get(url)
    except Exception as e:
        logger.error(f"  Page load error: {e}")
        return False

    try:
        WebDriverWait(driver, 12).until(
            lambda d: any(
                d.find_elements(By.CSS_SELECTOR, s)
                for s in ['div.textblock', 'ul.feats', 'div.dtsm', 'h1', 'span.dpp']
            )
        )
    except Exception:
        pass  # Continue even if sentinel not found

    time.sleep(1.5)  # Let lazy-loaded content settle
    return True

# ── Text extraction (from rendered DOM via selenium) ──────────────────────────

# Description selectors in priority order (first match > 100 chars wins)
DESCRIPTION_SELECTORS = [
    'div.textblock div.textbp',   # TR confirmed selector
    'div.prop-det-text div.text',
    'div.prop-det-text',
    'div.property-description',
    'div.prop-description',
    'div.description',
    'div#description',
    'div.overview',
    'section.description',
    'div.prop-desc',
]

FEATURES_SELECTORS = [
    'ul.feats > li',              # TR confirmed selector
    'div.prop-det-feats div.feat',
    'ul.features > li',
    'div.features > li',
    'ul.key-features > li',
    'div.key-features > li',
]


def _selenium_text(driver, css):
    """Return stripped text of the first matching element, or ''."""
    try:
        return driver.find_element(By.CSS_SELECTOR, css).text.strip()
    except Exception:
        return ''


def extract_address(driver, soup):
    h1 = soup.find('h1')
    return h1.get_text(strip=True) if h1 else _selenium_text(driver, 'h1')


def extract_price(soup):
    dpt = soup.find('span', class_='dpt')
    dpp = soup.find('span', class_='dpp')
    if dpp:
        qualifier = dpt.get_text(strip=True) if dpt else ''
        amount = dpp.get_text(strip=True)
        return f"{qualifier} {amount}".strip() if qualifier else amount
    return ''


def extract_status_bedrooms_type(soup):
    """Parse the TR summary bar (div.dtsm) for status, bedrooms, property type."""
    status = bedrooms = prop_type = ''
    dtsm = soup.find('div', class_='dtsm')
    if dtsm:
        for li in dtsm.find_all('li'):
            t = li.get_text(strip=True)
            tl = t.lower()
            if tl in ('sale', 'for sale'):
                status = 'For Sale'
            elif tl in ('agreed', 'sale agreed'):
                status = 'Sale Agreed'
            elif tl in ('sold', 'let agreed', 'under offer'):
                status = t
            elif 'bedroom' in tl and not bedrooms:
                bedrooms = t
            elif re.search(r'reception', tl):
                pass  # skip reception count
            elif t and not prop_type:
                prop_type = t
    return status, bedrooms, prop_type


def extract_description(driver):
    for sel in DESCRIPTION_SELECTORS:
        try:
            els = driver.find_elements(By.CSS_SELECTOR, sel)
            for el in els:
                t = el.text.strip()
                if len(t) > 100:
                    return t
        except Exception:
            pass
    return ''


def extract_features(driver):
    for sel in FEATURES_SELECTORS:
        try:
            els = driver.find_elements(By.CSS_SELECTOR, sel)
            items = [e.text.strip() for e in els if e.text.strip()]
            if items:
                return items
        except Exception:
            pass
    return []


def extract_rooms(driver):
    """
    Extract room-by-room breakdown from TR's ul.rooms list.
    Structure: ul.rooms > li > h3 (name + dimensions) + div.textbp (description)
    """
    rooms = []
    try:
        for li in driver.find_elements(By.CSS_SELECTOR, 'ul.rooms > li'):
            room = {'name': '', 'dimensions': '', 'description': ''}
            try:
                h3_text = li.find_element(By.CSS_SELECTOR, 'h3').text.strip()
                if ':' in h3_text:
                    name, _, dims = h3_text.partition(':')
                    room['name']       = name.strip()
                    room['dimensions'] = dims.strip()
                else:
                    room['name'] = h3_text
            except Exception:
                pass
            try:
                room['description'] = li.find_element(
                    By.CSS_SELECTOR, 'div.textbp'
                ).text.strip()
            except Exception:
                pass
            if room['name'] or room['description']:
                rooms.append(room)
    except Exception:
        pass
    return rooms


sys.path.insert(0, SCRAPERS_DIR)
from image_sort_utils import sort_and_dedup as _sort_and_dedup_image_urls


def extract_image_urls(driver, property_url):
    """
    Extract property image URLs from the rendered page.
    Tries the gallery element first, falls back to any property-looking image.
    Results are sorted by numeric suffix and deduplicated (TR's carousel HTML
    repeats the last few images at the front for looping).
    """
    soup = BeautifulSoup(driver.page_source, 'html.parser')

    # Primary: ul#gallery (TR's main image carousel)
    gallery = soup.find('ul', id='gallery')
    if gallery:
        urls = []
        for img in gallery.find_all('img'):
            src = img.get('src') or img.get('data-src') or img.get('data-lazy-src')
            if src:
                urls.append(urljoin(property_url, src))
        if urls:
            return _sort_and_dedup_image_urls(urls)

    # Fallback: any img whose URL looks like a property photo
    urls = []
    for img in soup.find_all('img'):
        src = img.get('src') or img.get('data-src')
        if src and '/images/property/' in src and 'logo' not in src.lower():
            full = urljoin(property_url, src)
            if full not in urls:
                urls.append(full)
    return _sort_and_dedup_image_urls(urls)

# ── Core per-property scrape function ─────────────────────────────────────────

def scrape_property_page(property_url, property_id, driver):
    """
    Scrape a single TR property page using a shared selenium driver.
    Downloads images via plain requests. Saves JSON to disk.
    Returns the property data dict, or None on failure.
    """
    logger.info(f"Scraping: {property_url}")

    # Load the page in headless Chrome
    ok = load_page(driver, property_url)
    if not ok:
        logger.error(f"  Failed to load page — skipping {property_id}")
        return None

    # Parse the rendered HTML with BeautifulSoup for fast static field extraction
    soup = BeautifulSoup(driver.page_source, 'html.parser')

    property_data = {
        'url':        property_url,
        'id':         property_id,
        'scraped_at': datetime.now().isoformat(),
    }

    # ── Text fields ───────────────────────────────────────────────────────────

    address = extract_address(driver, soup)
    if address:
        property_data['address'] = address
        property_data['title']   = address   # TR uses address as title

    price = extract_price(soup)
    if price:
        property_data['price'] = price

    status, bedrooms, prop_type = extract_status_bedrooms_type(soup)
    if status:
        property_data['status'] = status
    if bedrooms:
        property_data['bedrooms'] = bedrooms
    if prop_type:
        property_data['type'] = prop_type

    description = extract_description(driver)
    if description:
        property_data['description'] = description

    features = extract_features(driver)
    if features:
        property_data['key_features'] = features

    rooms = extract_rooms(driver)
    if rooms:
        property_data['rooms'] = rooms

    # ── Images ────────────────────────────────────────────────────────────────

    property_folder = os.path.join(TR_DIR, property_id)
    os.makedirs(property_folder, exist_ok=True)

    img_urls = extract_image_urls(driver, property_url)
    logger.info(f"  Found {len(img_urls)} image(s)")

    # Save source URLs so migrate_images.py can store them directly
    # without uploading to Supabase Storage
    if img_urls:
        property_data['image_urls'] = img_urls

    image_count = 0
    for i, img_url in enumerate(img_urls, 1):
        if download_image(img_url, property_folder, i):
            image_count += 1

    property_data['image_count'] = image_count

    # ── Save JSON ─────────────────────────────────────────────────────────────

    json_path = os.path.join(property_folder, f'{property_id}.json')
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(property_data, f, indent=2, ensure_ascii=False)

    logger.info(
        f"  Saved: {json_path}  |  "
        f"desc={len(description)}ch  feats={len(features)}  "
        f"rooms={len(rooms)}  imgs={image_count}"
    )
    return property_data

# ── Index helpers ─────────────────────────────────────────────────────────────

def load_property_index():
    if os.path.isfile(INDEX_PATH):
        try:
            with open(INDEX_PATH, encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Error loading property index: {e}")
    return {'properties': [], 'last_updated': None}


def save_property_index(index):
    with open(INDEX_PATH, 'w', encoding='utf-8') as f:
        json.dump(index, f, indent=2, ensure_ascii=False)
    logger.info(f"Property index saved: {INDEX_PATH}")

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description='Templeton Robinson full scraper')
    parser.add_argument('--test',  action='store_true',
                        help='Scrape one property only with verbose output')
    parser.add_argument('--limit', type=int, default=0,
                        help='Maximum number of new properties to scrape (0 = unlimited)')
    parser.add_argument('--max-pages', type=int, default=1000,
                        help='Maximum listing pages to walk (default: 1000)')
    args = parser.parse_args()

    # Clear output directory for a fresh full scrape
    if not args.test:
        if os.path.exists(TR_DIR):
            logger.info(f"Clearing {TR_DIR}/ for a fresh full scrape...")
            shutil.rmtree(TR_DIR)
        os.makedirs(TR_DIR, exist_ok=True)
        os.makedirs(os.path.join(SCRIPT_DIR, 'logs'), exist_ok=True)

    logger.info("Starting Templeton Robinson scraper (selenium mode)…")
    logger.info(f"Log: {log_filename}")

    # Load existing index to skip already-scraped URLs
    property_index     = load_property_index()
    existing_entries   = property_index.get('properties', [])
    known_urls         = {e['url'].rstrip('/') for e in existing_entries if e.get('url')}
    next_id            = len(existing_entries) + 1

    logger.info(f"Known properties: {len(known_urls)}")

    # ── Walk listing pages to collect new URLs ────────────────────────────────

    all_new_urls = []
    pages_to_check = 1 if args.test else args.max_pages

    for page_num in range(1, pages_to_check + 1):
        page_url = BASE_URL.format(page=page_num)
        logger.info(f"Listing page {page_num}: {page_url}")

        r = fetch(page_url)
        if not r:
            logger.warning(f"Failed to fetch listing page {page_num}, stopping.")
            break

        soup = BeautifulSoup(r.content, 'html.parser')
        links = extract_property_links(soup, page_url)

        if not links:
            logger.info("No more property links found — end of listings.")
            break

        page_new = [l for l in links if l not in known_urls]
        all_new_urls.extend(page_new)
        logger.info(f"  {len(links)} listings on page, {len(page_new)} new")

        # If an entire page has no new properties, we've caught up
        if not page_new and page_num > 1:
            logger.info("Page fully known — stopping pagination.")
            break

        time.sleep(random.uniform(1, 2))

    logger.info(f"New properties to scrape: {len(all_new_urls)}")

    if args.test:
        all_new_urls = all_new_urls[:1]
        logger.info("TEST MODE: capping to 1 property")

    if args.limit:
        all_new_urls = all_new_urls[:args.limit]
        logger.info(f"--limit {args.limit}: capped to {len(all_new_urls)} properties")

    if not all_new_urls:
        logger.info("Nothing to scrape.")
        return

    # ── Scrape each new property with a shared selenium driver ────────────────

    driver    = make_driver()
    scraped   = []
    errors    = 0

    try:
        for idx, url in enumerate(all_new_urls, 1):
            logger.info(f"{'='*60}")
            logger.info(f"[{idx}/{len(all_new_urls)}]")

            # Periodic driver restart to free memory
            if idx > 1 and (idx - 1) % RESTART_EVERY == 0:
                logger.info(f"Restarting Chrome to free memory (every {RESTART_EVERY} properties)…")
                try:
                    driver.quit()
                except Exception:
                    pass
                driver = make_driver()

            prop_id = f'property_{next_id + idx - 1}'

            data = scrape_property_page(url, prop_id, driver)
            if data:
                scraped.append(data)
            else:
                errors += 1

            if idx < len(all_new_urls):
                delay = random.uniform(1.5, 3.0)
                logger.info(f"Waiting {delay:.1f}s…")
                time.sleep(delay)

    finally:
        try:
            driver.quit()
        except Exception:
            pass

    # ── Update index ──────────────────────────────────────────────────────────

    for d in scraped:
        existing_entries.append({
            'id':         d['id'],
            'url':        d['url'],
            'address':    d.get('address', ''),
            'title':      d.get('title', ''),
            'scraped_at': d['scraped_at'],
        })

    save_property_index({
        'properties':   existing_entries,
        'last_updated': datetime.now().isoformat(),
    })

    # ── Save summary ──────────────────────────────────────────────────────────

    summary = {
        'new_properties_found':   len(all_new_urls),
        'properties_scraped':     len(scraped),
        'errors':                 errors,
        'scraper_mode':           'selenium',
        'scraped_at':             datetime.now().isoformat(),
        'log_file':               log_filename,
    }
    with open(SUMMARY_PATH, 'w', encoding='utf-8') as f:
        json.dump(summary, f, indent=2)

    logger.info(f"{'='*60}")
    logger.info("Scrape complete!")
    logger.info(f"  Scraped: {len(scraped)}  Errors: {errors}")
    logger.info(f"  Saved to: {TR_DIR}")
    logger.info(f"  Log:      {log_filename}")


if __name__ == '__main__':
    main()
