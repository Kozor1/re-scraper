#!/usr/bin/env python3
"""
Daniel Henry (danielhenry.co.uk) property scraper.

Scrapes all residential sale listings and saves to properties/dh/property_N/.

Usage:
    python3 scrapers/dh_scrape.py              # scrape all
    python3 scrapers/dh_scrape.py --limit 10   # test with first 10 properties
    python3 scrapers/dh_scrape.py --delay 1.5  # custom delay (default 1.2s)
    python3 scrapers/dh_scrape.py --all        # re-scrape even complete properties
    python3 scrapers/dh_scrape.py --selenium   # force Selenium from the start
"""

import os, re, json, time, sys, argparse, shutil
import requests
from bs4 import BeautifulSoup

BASE_URL  = 'https://www.danielhenry.co.uk'
LIST_URL  = BASE_URL + '/search?sta=forSale&sta=saleAgreed&sta=sold&st=sale&currency=GBP&pt=residential'
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUT_DIR   = os.path.join(SCRIPT_DIR, '..', 'properties', 'dh')

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 '
                  '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xhtml+xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-GB,en;q=0.9',
    'Referer': BASE_URL + '/',
}

# Non-room section labels in the ListingDescr dl
NON_ROOM_LABELS = {
    'description', 'location', 'directions', 'accommodation to include',
    'general', 'general information', 'notes', 'viewing', 'outside',
    'exterior features', 'external features', 'additional information',
}

# ── HTTP / Selenium helpers ───────────────────────────────────────────────────

_session = None
_driver  = None
_use_selenium = False

def get_session():
    global _session
    if _session is None:
        _session = requests.Session()
        _session.headers.update(HEADERS)
    return _session

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
    return webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)

def get_driver():
    global _driver
    if _driver is None:
        print('  Starting Selenium ChromeDriver...')
        _driver = make_driver()
    return _driver

def restart_driver():
    global _driver
    if _driver:
        try: _driver.quit()
        except: pass
    _driver = None
    time.sleep(2)
    return get_driver()

def fetch_requests(url, retries=2):
    sess = get_session()
    for attempt in range(retries + 1):
        try:
            r = sess.get(url, timeout=20)
            if r.status_code == 200:
                return r.text
            print(f'    HTTP {r.status_code}')
        except Exception as e:
            print(f'    Request error: {e}')
        if attempt < retries:
            time.sleep(2)
    return None

def fetch_selenium(url, wait=3, retries=1):
    for attempt in range(retries + 1):
        try:
            drv = get_driver()
            drv.get(url)
            time.sleep(wait)
            return drv.page_source
        except Exception as e:
            print(f'    Selenium error: {e} — restarting driver')
            restart_driver()
    return None

def fetch(url, delay=1.2):
    """Fetch with requests, fall back to Selenium if needed."""
    global _use_selenium
    if not _use_selenium:
        html = fetch_requests(url)
        # Confirm we actually got property content (not a blank/redirect/bot page)
        if html and '.single-property' in html:
            time.sleep(delay)
            return html
        if html and 'search-results' in html and '.single-property' not in html:
            # Empty results page is still valid (end of pagination)
            time.sleep(delay)
            return html
        print('    requests got no property content — switching to Selenium')
        _use_selenium = True
    html = fetch_selenium(url)
    time.sleep(delay)
    return html

# ── Parsing helpers ───────────────────────────────────────────────────────────

def parse_price(price_str):
    """'£150,000' → 150000, or None."""
    s = re.sub(r'[£,\s]', '', price_str or '')
    try:
        return int(s)
    except:
        return None

def normalise_status(raw):
    s = (raw or '').strip().lower()
    if s in ('for sale', 'forsale'):       return 'For Sale'
    if s in ('sale agreed', 'saleagreed'): return 'Sale Agreed'
    if s == 'sold':                        return 'Sold'
    return (raw or '').strip().title()

def parse_brief(brief):
    """'3 Bed Semi-detached Bungalow' → ('3 Bedrooms', 'Semi-detached Bungalow')."""
    m = re.match(r'^(\d+)\s*[Bb]ed\b\s*(.*)', brief)
    if m:
        n, typ = int(m.group(1)), m.group(2).strip()
        return f"{n} Bedroom{'s' if n != 1 else ''}", typ
    return None, brief

def ki_val(soup, cls):
    """Extract value cell from a KeyInfo table row."""
    row = soup.select_one(f'.KeyInfo-{cls}')
    if not row:
        return None
    cells = row.select('td.KeyInfo-cell, th.KeyInfo-cell')
    # The td (second cell) holds the value
    tds = [c for c in cells if c.name == 'td']
    return tds[0].get_text(strip=True) if tds else None

# ── Listing page scraper ──────────────────────────────────────────────────────

def scrape_listings(html):
    """Parse listing page HTML → list of basic property dicts."""
    soup = BeautifulSoup(html, 'html.parser')
    results = []
    for card in soup.select('.single-property'):
        # Main link (not the shortlist button)
        a = card.select_one('a[href]:not(.shortlist-btn):not(.shortlist)')
        if not a:
            continue
        href = a['href']
        dh_id = href.rstrip('/').split('/')[-1]
        if not dh_id.isdigit():
            continue

        url = (BASE_URL + href) if href.startswith('/') else href

        status_el  = card.select_one('.status-overlay span')
        line1_el   = card.select_one('.Address-addressLine1')
        town_el    = card.select_one('.Address-addressTown')
        out_el     = card.select_one('.Address-addressOutcode')
        in_el      = card.select_one('.Address-addressIncode')
        brief_el   = card.select_one('.brief-text')
        price_el   = card.select_one('.Price-priceValue')

        line1    = (line1_el.get_text(strip=True) if line1_el else '').rstrip(',')
        town     = (town_el.get_text(strip=True)  if town_el  else '').rstrip(',')
        outcode  = out_el.get_text(strip=True) if out_el else ''
        incode   = in_el.get_text(strip=True)  if in_el  else ''
        postcode = f'{outcode} {incode}'.strip()
        address  = ', '.join(filter(None, [line1, town, postcode]))

        brief              = brief_el.get_text(strip=True) if brief_el else ''
        bedrooms, prop_type = parse_brief(brief)
        price_str          = price_el.get_text(strip=True) if price_el else ''

        results.append({
            'dh_id':    dh_id,
            'url':      url,
            'address':  address,
            'status':   normalise_status(status_el.get_text(strip=True) if status_el else ''),
            'bedrooms': bedrooms,
            'type':     prop_type,
            'price_str': price_str,
            'price':    parse_price(price_str),
        })
    return results

def max_page(html):
    """Return the highest page= value linked in pagination (0 = first page only)."""
    soup = BeautifulSoup(html, 'html.parser')
    hi = 0
    for a in soup.select('[href*="page="]'):
        m = re.search(r'page=(\d+)', a.get('href', ''))
        if m:
            hi = max(hi, int(m.group(1)))
    return hi

# ── Detail page scraper ───────────────────────────────────────────────────────

def scrape_detail(html, basic):
    """Parse a property detail page HTML. Returns full property dict."""
    soup = BeautifulSoup(html, 'html.parser')
    data = dict(basic)

    # --- Key Information table ---
    style_val = ki_val(soup, 'style')
    if style_val:
        data['type'] = style_val

    beds_val = ki_val(soup, 'bedrooms')
    if beds_val:
        try:
            n = int(beds_val)
            data['bedrooms'] = f"{n} Bedroom{'s' if n != 1 else ''}"
        except:
            data['bedrooms'] = beds_val

    data['receptions'] = ki_val(soup, 'receptionrooms') or ''
    data['bathrooms']  = ki_val(soup, 'bathrooms')      or ''

    status_val = ki_val(soup, 'status')
    if status_val:
        data['status'] = normalise_status(status_val)

    price_el = soup.select_one('.KeyInfo-price .price-text, .KeyInfo-price td')
    if price_el:
        ps = price_el.get_text(strip=True)
        # Normalise "Offers around£620,000" → "Offers around £620,000"
        ps = re.sub(r'([A-Za-z])(£)', r'\1 \2', ps)
        if '£' in ps:
            data['price_str'] = ps
            data['price']     = parse_price(ps)

    # Refine address from KeyInfo (more structured than listing card)
    line1   = soup.select_one('.KeyInfo-address .Address-addressLine1')
    town    = soup.select_one('.KeyInfo-address .Address-addressTown')
    outcode = soup.select_one('.KeyInfo-address .Address-addressOutcode')
    incode  = soup.select_one('.KeyInfo-address .Address-addressIncode')
    if line1:
        l1  = line1.get_text(strip=True).rstrip(',')
        tw  = (town.get_text(strip=True) if town else '').rstrip(',')
        pc  = ' '.join(filter(None, [
            outcode.get_text(strip=True) if outcode else '',
            incode.get_text(strip=True)  if incode  else '',
        ]))
        data['address'] = ', '.join(filter(None, [l1, tw, pc]))

    data['title'] = data['address']

    # --- Summary bullets (key features) ---
    data['key_features'] = [
        el.get_text(strip=True)
        for el in soup.select('.ListingBullets-item span')
        if el.get_text(strip=True)
    ]

    # --- Description + rooms from ListingDescr dl ---
    description = ''
    rooms       = []

    descr_section = soup.select_one('.ListingDescr-text')
    if descr_section:
        dl = descr_section.find('dl')
        if dl:
            elems = dl.find_all(['dt', 'dd'])
            i = 0
            while i < len(elems):
                el = elems[i]
                if el.name == 'dt':
                    strong  = el.find('strong')
                    label   = (strong.get_text(strip=True) if strong else el.get_text(strip=True)).rstrip(':').strip()
                    # Dimensions: text in dt after the <strong>
                    dt_full = el.get_text(strip=True)
                    strong_text = strong.get_text(strip=True) if strong else ''
                    dims = dt_full[len(strong_text):].strip().lstrip(':').strip()

                    dd_text = ''
                    if i + 1 < len(elems) and elems[i + 1].name == 'dd':
                        dd_text = elems[i + 1].get_text(strip=True)
                        i += 1

                    label_lower = label.lower()
                    if label_lower == 'description':
                        description = dd_text
                    elif label_lower not in NON_ROOM_LABELS and (dd_text or dims):
                        rooms.append({
                            'name':        label,
                            'dimensions':  dims,
                            'description': dd_text,
                        })
                i += 1

    # If no explicit "Description" label was found, use the longest NON_ROOM_LABELS
    # dd_text — DH often omits the label or uses "Accommodation to Include" as header.
    if not description and descr_section:
        dl = descr_section.find('dl')
        if dl:
            elems = dl.find_all(['dt', 'dd'])
            i = 0
            while i < len(elems):
                el = elems[i]
                if el.name == 'dt':
                    strong = el.find('strong')
                    label  = (strong.get_text(strip=True) if strong
                              else el.get_text(strip=True)).rstrip(':').strip().lower()
                    dd_text = ''
                    if i + 1 < len(elems) and elems[i + 1].name == 'dd':
                        dd_text = elems[i + 1].get_text(strip=True)
                        i += 1
                    if label in NON_ROOM_LABELS and len(dd_text) > len(description):
                        description = dd_text
                i += 1
        # Final fallback: plain text from the section (no dl)
        if not description:
            description = descr_section.get_text(separator=' ', strip=True)

    data['description'] = description
    data['rooms']       = rooms

    # --- Images ---
    # ul#pphoto on DH pages uses JavaScript lightbox <a> tags whose href is
    # "javascript:void(0)" or "#" — NOT direct image URLs.  The actual image URL
    # is stored in data attributes (data-href, data-src, data-full) on the <a> tag,
    # or in the <img> src/data-src inside it.  Check all of these before falling
    # back to the 4 lazy-loaded CDN img thumbnails.
    images = []
    seen   = set()
    from urllib.parse import urljoin as _urljoin

    def _add_img(src):
        if not src:
            return
        full = _urljoin(url, src) if not src.startswith('http') else src
        if full.startswith('http') and full not in seen:
            seen.add(full)
            images.append(full)

    # DH uses a SlideshowCarousel — no ul#pphoto.  Collect whatever CDN images
    # are in the initial DOM; the gallery click-through in main() will add the rest.
    # Note: DH uses TWO CDN URL patterns:
    #   media.propertypal.com/sd/{hash}/p/{id}/{img}.jpg  (standard)
    #   media.propertypal.com/{token}/p/{id}/{img}.jpg    (alt token, no /sd/)
    # So we match on 'media.propertypal.com' without requiring '/sd/'.
    for img in soup.select('img[src*="media.propertypal.com"]'):
        src = img.get('src', '')
        if not src or src in seen:
            continue
        # Prefer the largest srcset variant
        srcset = img.get('srcset', '')
        best = src
        best_w = 0
        for part in srcset.split(','):
            pieces = part.strip().split()
            if len(pieces) >= 2:
                try:
                    w = int(pieces[1].rstrip('w'))
                    if w > best_w:
                        best_w = w
                        best = pieces[0]
                except ValueError:
                    pass
        _add_img(best)

    data['images'] = images

    data['coords'] = None
    return data

# ── File I/O ──────────────────────────────────────────────────────────────────

def next_property_num():
    """Return the next sequential property number for properties/dh/."""
    os.makedirs(OUT_DIR, exist_ok=True)
    existing = [
        d for d in os.listdir(OUT_DIR)
        if d.startswith('property_') and os.path.isdir(os.path.join(OUT_DIR, d))
    ]
    if not existing:
        return 1
    nums = []
    for d in existing:
        try:
            nums.append(int(d.replace('property_', '')))
        except:
            pass
    return max(nums) + 1 if nums else 1

def load_dh_id_map():
    """Return dict mapping dh_id → property_N dir name (for dedup)."""
    mapping = {}
    if not os.path.isdir(OUT_DIR):
        return mapping
    for d in os.listdir(OUT_DIR):
        if not d.startswith('property_'):
            continue
        jpath = os.path.join(OUT_DIR, d, f'{d}.json')
        if os.path.exists(jpath):
            try:
                data = json.load(open(jpath))
                if data.get('dh_id'):
                    mapping[data['dh_id']] = d
            except:
                pass
    return mapping

def save_property(data, prop_dir_name):
    dir_path = os.path.join(OUT_DIR, prop_dir_name)
    os.makedirs(dir_path, exist_ok=True)
    jpath = os.path.join(dir_path, f'{prop_dir_name}.json')
    with open(jpath, 'w') as f:
        json.dump(data, f, indent=2)

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    global _use_selenium

    parser = argparse.ArgumentParser(description='Scrape Daniel Henry properties')
    parser.add_argument('--rent',     action='store_true',      help='Scrape rental properties')
    parser.add_argument('--limit',    type=int,   default=0,    help='Max properties to scrape')
    parser.add_argument('--delay',    type=float, default=1.2,  help='Delay between requests (s)')
    parser.add_argument('--all',      action='store_true',      help='Re-scrape even complete properties')
    parser.add_argument('--selenium', action='store_true',      help='Use Selenium from the start')
    parser.add_argument('--fresh',    action='store_true',      help='Clear all existing data and start from scratch')
    args = parser.parse_args()

    if args.rent:
        global LIST_URL, OUT_DIR
        LIST_URL = BASE_URL + '/search?sta=toLet&sta=tenancyCurrentUnavailable&st=let&currency=GBP&pt=residential'
        OUT_DIR  = os.path.join(SCRIPT_DIR, '..', 'properties', 'dh_rent')

    if args.selenium:
        _use_selenium = True

    if args.fresh and os.path.exists(OUT_DIR):
        print(f"--fresh: clearing {OUT_DIR}/ for a fresh full scrape...")
        shutil.rmtree(OUT_DIR)

    os.makedirs(OUT_DIR, exist_ok=True)

    # ── Phase 1: collect all property stubs from listing pages ────────────────
    print('Phase 1: collecting property listings...')
    all_listings = []
    page = 0   # 0 = first page (no page param), 1+ = &page=N

    while True:
        url = LIST_URL if page == 0 else f'{LIST_URL}&page={page}'
        print(f'  Page {page + 1}: {url}')

        html = fetch(url, delay=args.delay)
        if not html:
            print(f'  Failed to fetch page {page + 1} — stopping')
            break

        cards = scrape_listings(html)
        if not cards:
            print(f'  No property cards on page {page + 1} — done')
            break

        all_listings.extend(cards)
        top_page = max_page(html)
        print(f'  → {len(cards)} cards  (total: {len(all_listings)},  max page param: {top_page})')

        if args.limit and len(all_listings) >= args.limit:
            all_listings = all_listings[:args.limit]
            print(f'  Reached --limit {args.limit}')
            break

        if page < top_page:
            page += 1
        else:
            break

    print(f'\nCollected {len(all_listings)} listings\n')

    # ── Phase 2: fetch each detail page ──────────────────────────────────────
    print('Phase 2: fetching property detail pages...')

    dh_id_map  = load_dh_id_map()   # dh_id → "property_N"
    next_num   = next_property_num()
    saved = skipped = errors = 0

    for idx, basic in enumerate(all_listings):
        dh_id     = basic['dh_id']
        prop_name = dh_id_map.get(dh_id)   # existing dir name if already scraped

        # If already exists and --all not set, check if complete
        if prop_name and not args.all:
            jpath = os.path.join(OUT_DIR, prop_name, f'{prop_name}.json')
            if os.path.exists(jpath):
                try:
                    existing = json.load(open(jpath))
                    imgs = len(existing.get('images', []))
                    if (existing.get('description') and existing.get('key_features')
                            and imgs > 5):
                        skipped += 1
                        continue
                except:
                    pass

        # Restart Chrome every 50 properties to prevent memory-related crashes
        if _use_selenium and idx > 0 and idx % 50 == 0:
            print(f'  Restarting Chrome to free memory (property {idx + 1})…')
            restart_driver()

        print(f'[{idx + 1}/{len(all_listings)}] {basic["address"]}  ({basic["status"]})')

        html = fetch(basic['url'], delay=args.delay)
        if not html:
            print('  ERROR: failed to fetch detail page')
            errors += 1
            continue

        try:
            data = scrape_detail(html, basic)
        except Exception as e:
            import traceback
            print(f'  ERROR parsing: {e}')
            traceback.print_exc()
            errors += 1
            continue

        # If we still have few images and Selenium is running, click through
        # the SlideshowCarousel to reveal all gallery images.
        # Key findings from debug_dh_html.py:
        #  - next button: span.SlideshowCarousel-next  (hidden by CSS, needs JS click)
        #  - images use BOTH /sd/ and non-/sd/ CDN URLs — match on media.propertypal.com
        if len(data.get('images', [])) <= 5 and _use_selenium and _driver:
            try:
                from selenium.webdriver.common.by import By
                # Collect ALL propertypal CDN images (both /sd/ and alt token paths).
                # Returns URLs in document (DOM) order so insertion order is preserved.
                COLLECT_JS = """
                var result = [];
                document.querySelectorAll('img[src*="media.propertypal.com"]').forEach(function(img) {
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
                    if (best && best.indexOf('media.propertypal.com') !== -1) result.push(best);
                });
                return result;
                """

                # Seed with already-found static images to preserve their order.
                # Use a list for ordered insertion and a set for O(1) dedup.
                gallery_urls = list(data.get('images', []))
                gallery_seen = set(gallery_urls)

                def _add_gallery(url_list):
                    for u in (url_list or []):
                        if u not in gallery_seen:
                            gallery_seen.add(u)
                            gallery_urls.append(u)

                _add_gallery(_driver.execute_script(COLLECT_JS) or [])

                # Find the SlideshowCarousel-next button — it's CSS-hidden so we
                # must use JS click rather than Selenium's regular .click()
                next_btn = None
                for sel in ['.SlideshowCarousel-next', '[class*="SlideshowCarousel-next"]',
                             '[class*="next" i]', '[class*="Next"]']:
                    try:
                        candidates = _driver.find_elements(By.CSS_SELECTOR, sel)
                        if candidates:
                            next_btn = candidates[0]
                            break
                    except Exception:
                        continue

                if next_btn:
                    no_new = 0
                    for _ in range(60):   # up to 60 slides
                        prev = len(gallery_urls)
                        try:
                            # JS click bypasses CSS visibility/pointer-events restrictions
                            _driver.execute_script("arguments[0].click()", next_btn)
                            time.sleep(0.3)
                            _add_gallery(_driver.execute_script(COLLECT_JS) or [])
                        except Exception:
                            break
                        if len(gallery_urls) == prev:
                            no_new += 1
                            if no_new >= 3:
                                break
                        else:
                            no_new = 0

                if len(gallery_urls) > len(data.get('images', [])):
                    data['images'] = gallery_urls
            except Exception:
                pass  # gallery click-through failed — keep whatever images we have

        # Assign or reuse property_N name
        if not prop_name:
            prop_name = f'property_{next_num}'
            dh_id_map[dh_id] = prop_name
            next_num += 1

        data['id']    = prop_name
        data['dh_id'] = dh_id    # preserve original DH numeric ID

        save_property(data, prop_name)
        saved += 1
        print(f'  ✓ {prop_name}  desc:{bool(data["description"])}  '
              f'feats:{len(data["key_features"])}  '
              f'rooms:{len(data["rooms"])}  '
              f'imgs:{len(data["images"])}')

    # Cleanup Selenium if it was used
    if _driver:
        try: _driver.quit()
        except: pass

    print(f'\n{"="*55}')
    print(f'Done: {saved} saved, {skipped} skipped, {errors} errors')
    print(f'Output: {os.path.abspath(OUT_DIR)}')

if __name__ == '__main__':
    main()
