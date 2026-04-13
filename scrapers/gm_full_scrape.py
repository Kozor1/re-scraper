#!/usr/bin/env python3
"""
Gareth Mills Estate Agents full scraper with smart-update logic.
SOURCE KEY: gm
URL:  https://www.garethmillsestateagents.com/property-for-sale

Uses the same PropertyPal CMS selectors as McMillan McClure / Country Estates.

Smart update behaviour:
  - New listings    → scrape full detail page and save as property_N
  - Existing        → skip (use --rescrape to force re-check price/status)
  - Delisted        → delete local folder and url_map entry (skipped in --quick mode)

Usage (run from swome-scraper/ directory):
    python3 scrapers/gm_full_scrape.py              # sale listings
    python3 scrapers/gm_full_scrape.py --quick      # fast scan: new only, early stop on known
    python3 scrapers/gm_full_scrape.py --limit 5    # stop after 5 new scrapes
    python3 scrapers/gm_full_scrape.py --rescrape   # re-scrape all existing too
    python3 scrapers/gm_full_scrape.py --test       # scrape first 1 new property only

Requires: pip3 install requests beautifulsoup4 selenium webdriver-manager
"""

import os, sys, json, re, time, random, argparse, shutil, logging
from datetime import datetime
from urllib.parse import urljoin

try:
    import requests
    from bs4 import BeautifulSoup
except ImportError:
    print("Run: pip3 install requests beautifulsoup4")
    sys.exit(1)

# ── Config ─────────────────────────────────────────────────────────────────────

SOURCE_KEY = 'gm'
BASE_URL   = 'https://www.garethmillsestateagents.com'
LIST_URL   = 'https://www.garethmillsestateagents.com/property-for-sale'
# Pagination: /property-for-sale/page-2, /property-for-sale/page-3, …

ROOT     = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PROP_DIR = os.path.join(ROOT, 'properties', SOURCE_KEY)
MAP_PATH = os.path.join(PROP_DIR, 'url_map.json')
LOGS_DIR = os.path.join(ROOT, 'logs')

DELAY            = 1.5
MAX_RETRIES      = 3
QUICK_STOP_AFTER = 5   # stop pagination after this many consecutive known URLs (--quick mode)

# ── Logging ────────────────────────────────────────────────────────────────────

os.makedirs(PROP_DIR, exist_ok=True)
os.makedirs(LOGS_DIR, exist_ok=True)

log_file = os.path.join(
    LOGS_DIR,
    f"gm_scraper_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
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

# ── Selenium helpers (detail pages — JS-rendered on PropertyPal modern CMS) ────

_driver = None

def make_driver():
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.chrome.service import Service
    from webdriver_manager.chrome import ChromeDriverManager
    opts = Options()
    opts.add_argument('--headless')
    opts.add_argument('--no-sandbox')
    opts.add_argument('--disable-dev-shm-usage')
    opts.add_argument('--disable-gpu')
    opts.add_argument('--window-size=1280,1024')
    opts.add_argument(f'--user-agent={HEADERS["User-Agent"]}')
    return webdriver.Chrome(
        service=Service(ChromeDriverManager().install()), options=opts
    )

def get_driver():
    global _driver
    if _driver is None:
        logger.info('  Starting Selenium ChromeDriver…')
        _driver = make_driver()
    return _driver

def restart_driver():
    global _driver
    if _driver:
        try:
            _driver.quit()
        except Exception:
            pass
    _driver = None
    time.sleep(8)
    return get_driver()

def fetch_detail(url, retries=2):
    for attempt in range(retries):
        try:
            drv = get_driver()
            drv.get(url)
            time.sleep(5)
            return drv.page_source
        except Exception as e:
            logger.warning(f"  Selenium error [{attempt+1}] {url}: {e} — restarting driver")
            restart_driver()
    return None

def collect_gallery_images_selenium():
    drv = get_driver()
    COLLECT_JS = """
    var seen = new Set();
    document.querySelectorAll('img[src*="media.propertypal.com/sd/"]').forEach(function(img) {
        var best = img.src, bestW = 0;
        if (img.srcset) {
            img.srcset.split(',').forEach(function(p) {
                var parts = p.trim().split(/\\s+/);
                if (parts.length >= 2) {
                    var w = parseInt(parts[1]);
                    if (w > bestW) { bestW = w; best = parts[0]; }
                }
            });
        }
        seen.add(best);
    });
    return Array.from(seen);
    """
    all_urls_seen = set()
    all_urls_list = []

    def _add_url(u):
        if u and u not in all_urls_seen:
            all_urls_seen.add(u)
            all_urls_list.append(u)

    try:
        for u in (drv.execute_script(COLLECT_JS) or []):
            _add_url(u)
    except Exception:
        return all_urls_list

    from selenium.webdriver.common.by import By
    next_selectors = [
        '[class*="next" i]', '[class*="Next"]',
        '[aria-label*="next" i]', '[aria-label*="Next"]',
        '.slick-next', '.swiper-button-next',
    ]
    next_btn = None
    for sel in next_selectors:
        try:
            candidates = drv.find_elements(By.CSS_SELECTOR, sel)
            for c in candidates:
                if c.is_displayed():
                    next_btn = c
                    break
            if next_btn:
                break
        except Exception:
            continue

    if next_btn:
        no_new = 0
        for _ in range(50):
            prev_count = len(all_urls_list)
            try:
                next_btn.click()
                time.sleep(0.4)
            except Exception:
                break
            try:
                for u in (drv.execute_script(COLLECT_JS) or []):
                    _add_url(u)
            except Exception:
                break
            if len(all_urls_list) == prev_count:
                no_new += 1
                if no_new >= 3:
                    break
            else:
                no_new = 0

    return all_urls_list

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

# ── Listing page: collect live property URLs ───────────────────────────────────

def collect_live_urls(url_map, quick=False):
    """
    Paginate the listing pages and return a dict of {canonical_url: {address, price_str}}.
    In quick mode, stops pagination early when QUICK_STOP_AFTER consecutive known
    URLs are encountered (URLs already in url_map).
    """
    live_urls = {}
    page = 1
    consecutive_known = 0

    while True:
        url = LIST_URL if page == 1 else f"{LIST_URL}/page-{page}"
        logger.info(f"  Listing page {page}: {url}")
        r = fetch(url)
        if not r:
            logger.warning(f"  Could not fetch listing page {page} — stopping")
            break

        soup = BeautifulSoup(r.content, 'html.parser')
        new_this_page = 0

        for a in soup.find_all('a', href=True):
            href = a['href']
            # PropertyPal CMS: detail URLs end with a numeric property ID
            if not re.search(r'/\d{5,}(?:\?|$)', href):
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
                    h2 = parent.find(['h2', 'h3'])
                    if h2 and h2.get_text(strip=True):
                        addr = h2.get_text(strip=True)
                    pspan = parent.find(class_=re.compile(r'price', re.I))
                    if pspan and pspan.get_text(strip=True):
                        price = pspan.get_text(strip=True)
                    if addr:
                        break
                live_urls[canonical] = {'address': addr, 'price_str': price}
                new_this_page += 1

                if quick and canonical in url_map:
                    consecutive_known += 1
                else:
                    consecutive_known = 0

                if quick and consecutive_known >= QUICK_STOP_AFTER:
                    logger.info(f"  [quick] {QUICK_STOP_AFTER} consecutive known URLs — stopping pagination")
                    break

        logger.info(f"  → {new_this_page} new URLs (total: {len(live_urls)})")

        if quick and consecutive_known >= QUICK_STOP_AFTER:
            break

        if new_this_page == 0:
            logger.info("  No new URLs on this page — end of pagination")
            break

        page += 1
        time.sleep(DELAY + random.uniform(0, 0.5))

    return live_urls

# ── Detail page parser (PropertyPal CMS — same as MM / CE) ────────────────────

def normalise_status(raw):
    s = (raw or '').strip().lower()
    if 'agreed' in s: return 'Sale Agreed'
    if 'sold'   in s: return 'Sold'
    if 'let'    in s: return 'Let'
    return 'For Sale'

def extract_image_urls(soup, page_url):
    seen = set()
    urls = []

    def add(src):
        if src:
            full = urljoin(page_url, src) if not src.startswith('http') else src
            if full not in seen and full.startswith('http'):
                seen.add(full)
                urls.append(full)

    pphoto = soup.find('ul', id='pphoto')
    if pphoto:
        for a in pphoto.find_all('a', href=True):
            href = a['href']
            if any(ext in href.lower() for ext in ('.jpg', '.jpeg', '.png', '.webp')):
                add(href)
        if not urls:
            for img in pphoto.find_all('img'):
                add(img.get('src') or img.get('data-src') or img.get('data-lazy-src'))
        if urls:
            return urls

    for a in soup.find_all('a', href=True):
        href = a['href']
        if 'media.propertypal.com' in href:
            add(href)
    if urls:
        return urls

    for img in soup.select('img[src*="media.propertypal.com/sd/"]'):
        src = img.get('src', '').strip()
        if not src or src in seen:
            continue
        best = src
        best_width = 0
        srcset = img.get('srcset', '')
        if srcset:
            for part in srcset.split(','):
                part = part.strip()
                pieces = part.split()
                if len(pieces) >= 2:
                    try:
                        w = int(pieces[1].rstrip('w'))
                        if w > best_width:
                            best_width = w
                            best = pieces[0]
                    except ValueError:
                        pass
        add(best)

    if urls:
        return urls

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

    for script in soup.find_all('script'):
        text = script.string or ''
        found = re.findall(
            r'["\']('
            r'(?:https?://[^"\']+)?'
            r'/(?:images?|photos?|property-images?|uploads?)/[^"\']+\.(?:jpe?g|png|webp)'
            r')["\']',
            text, re.I
        )
        for f in found:
            add(f if f.startswith('http') else urljoin(page_url, f))
        if urls:
            return urls

    return urls

def parse_detail(html, url):
    soup = BeautifulSoup(html, 'html.parser')
    data = {'url': url, 'scraped_at': datetime.now().isoformat()}

    # Address
    h1 = soup.select_one('h1')
    if h1:
        data['address'] = h1.get_text(separator=' ', strip=True)
    if not data.get('address'):
        og = soup.find('meta', property='og:title')
        if og and og.get('content'):
            raw = og['content']
            for sep in [' | ', ' – ', ' - ']:
                if sep in raw:
                    raw = raw[:raw.rfind(sep)]
            data['address'] = raw.strip()
    data.setdefault('address', '')
    data['title'] = data['address']

    # Price
    price_val = soup.select_one('.Price-priceValue')
    if price_val:
        qualifier = soup.select_one('.Price-priceOffers')
        q = (qualifier.get_text(strip=True) + ' ') if qualifier else ''
        data['price_str'] = (q + price_val.get_text(strip=True)).strip()
    if not data.get('price_str'):
        og_desc = soup.find('meta', property='og:description')
        if og_desc and og_desc.get('content'):
            m = re.search(r'(£[\d,]+(?:pm|pcm)?|POA)', og_desc['content'], re.I)
            if m:
                data['price_str'] = m.group(1)

    # Type + Bedrooms + Bathrooms + Receptions (PropertyPal CMS)
    attr_items = soup.select('.SingleListingPage-attributes li')
    for i, li in enumerate(attr_items):
        text = li.get_text(strip=True)
        if not text:
            continue
        if i == 0:
            data['type'] = text
        elif i == 1:
            try:
                n = int(text)
                data['bedrooms'] = f"{n} Bedroom{'s' if n != 1 else ''}"
            except ValueError:
                pass
        elif i == 2:
            data['bathrooms'] = text
        elif i == 3:
            data['receptions'] = text

    # Status
    top = soup.select_one('.SingleListingPage-topEle')
    if top:
        data['status'] = normalise_status(top.get_text(separator=' ', strip=True))
    else:
        data.setdefault('status', normalise_status(''))

    # Key features
    bullets = soup.select('.DescriptionBox--bullets li')
    if not bullets:
        bullets = soup.select('.DescriptionBox--bullets p')
    data['key_features'] = [
        el.get_text(strip=True) for el in bullets if el.get_text(strip=True)
    ]

    # Description
    desc_els = soup.select('.ListingDescr-text')
    full_desc = ''
    for el in desc_els:
        t = el.get_text(separator=' ', strip=True)
        if len(t) > len(full_desc):
            full_desc = t
    data['description'] = full_desc
    data['rooms'] = []

    # Images
    data['image_urls'] = extract_image_urls(soup, url)

    return data

# ── Scrape one property and save ───────────────────────────────────────────────

def scrape_and_save(url, folder_name):
    html = fetch_detail(url)
    if not html:
        logger.error(f"    fetch failed for {url}")
        return None

    data = parse_detail(html, url)

    if len(data.get('image_urls', [])) <= 4:
        try:
            gallery_urls = collect_gallery_images_selenium()
            if len(gallery_urls) > len(data.get('image_urls', [])):
                data['image_urls'] = gallery_urls
        except Exception as e:
            logger.debug(f"    gallery click-through failed: {e}")

    prop_dir = os.path.join(PROP_DIR, folder_name)
    os.makedirs(prop_dir, exist_ok=True)

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
    parser = argparse.ArgumentParser(description='Gareth Mills Estate Agents smart scraper')
    parser.add_argument('--limit',    type=int, default=0,
                        help='Max new properties to scrape (0 = all)')
    parser.add_argument('--rescrape', action='store_true',
                        help='Re-scrape all existing properties')
    parser.add_argument('--test',     action='store_true',
                        help='Scrape only the first 1 new property and exit')
    parser.add_argument('--quick',    action='store_true',
                        help='Quick mode: stop pagination early on consecutive known URLs, '
                             'skip stale detection')
    args = parser.parse_args()

    if args.test:
        args.limit = 1

    if args.fresh:
        if os.path.exists(PROP_DIR):
            logger.info(f"--fresh: clearing {PROP_DIR}/")
            shutil.rmtree(PROP_DIR)
        os.makedirs(PROP_DIR, exist_ok=True)

    logger.info('=' * 60)
    logger.info(f"Gareth Mills Estate Agents scraper — {datetime.now().isoformat()}")
    logger.info(f"Properties dir: {PROP_DIR}")
    logger.info(f"Options: limit={args.limit or 'none'}  rescrape={args.rescrape}  "
                f"quick={args.quick}")

    url_map = load_url_map()

    # Step 1: collect live URLs
    logger.info("\n[1/3] Collecting live property URLs…")
    live_urls = collect_live_urls(url_map, quick=args.quick)
    logger.info(f"  Found {len(live_urls)} live properties on website")

    if len(live_urls) == 0:
        logger.error(
            "ABORT: listing pages returned 0 live URLs — site may be unreachable. "
            "No deletions or updates will be performed."
        )
        return

    live_set     = set(live_urls.keys())
    local_set    = set(url_map.keys())
    new_urls     = sorted(live_set - local_set)
    deleted_urls = sorted(local_set - live_set)
    existing_urls = sorted(live_set & local_set)

    logger.info(f"\n[2/3] Diff: {len(new_urls)} new, "
                f"{len(deleted_urls)} delisted, "
                f"{len(existing_urls)} unchanged")

    added = removed = updated = errors = 0

    # Step 3a: delete stale (skip in quick mode)
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

    # Step 3b: scrape new
    if new_urls:
        limit     = args.limit if args.limit else len(new_urls)
        to_scrape = new_urls[:limit]
        logger.info(f"\nScraping {len(to_scrape)} new properties…")

        next_id = next_property_id(url_map)

        for i, url in enumerate(to_scrape, 1):
            folder = f"property_{next_id}"
            next_id += 1
            logger.info(f"  [{i}/{len(to_scrape)}] NEW {folder}: {url}")
            if i > 1 and (i - 1) % 50 == 0:
                logger.info('  Restarting Chrome to free memory…')
                restart_driver()
            data = scrape_and_save(url, folder)
            if data:
                url_map[url] = folder
                save_url_map(url_map)
                added += 1
            else:
                errors += 1
            time.sleep(DELAY + random.uniform(0, 0.5))

    # Step 3c: re-scrape existing (optional)
    if args.rescrape and existing_urls:
        logger.info(f"\nRe-scraping {len(existing_urls)} existing properties…")
        for i, url in enumerate(existing_urls, 1):
            folder = url_map[url]
            logger.info(f"  [{i}/{len(existing_urls)}] UPDATE {folder}: {url}")
            if i > 1 and (i - 1) % 50 == 0:
                restart_driver()
            data = scrape_and_save(url, folder)
            if data:
                updated += 1
            else:
                errors += 1
            time.sleep(DELAY + random.uniform(0, 0.5))

    if _driver:
        try:
            _driver.quit()
        except Exception:
            pass

    logger.info(f"\n{'='*60}")
    logger.info(
        f"Done — added={added}  removed={removed}  "
        f"updated={updated}  errors={errors}"
    )
    logger.info(f"Log: {log_file}")


if __name__ == '__main__':
    main()
