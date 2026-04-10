#!/usr/bin/env python3
"""
quick_update_all.py  –  Fast intra-day scan for new property listings.

Designed to run 3-4 times per day.  Finds NEW listings only; does NOT
remove stale/delisted properties (that is handled by the nightly full scrape).

Strategy per source type:
  smart  — Run the full scraper with --quick flag.  These scrapers already
            skip known properties and stop pagination early once they hit
            QUICK_STOP_AFTER consecutive known URLs.
  legacy — Use built-in url-map-based quick scan: read all existing JSON
            files to build a known-URL set, then paginate from newest,
            stopping after CONSECUTIVE_KNOWN_THRESHOLD consecutive known
            entries.  New properties are scraped inline using shared
            PropertyPal CMS parser logic.

Pipeline:
  1. All quick scans run in parallel (smart scrapers as subprocesses,
     legacy sources via thread pool).
  2. geocode.py   — geocode any new addresses
  3. migrate_data.py — push new records to Supabase

Sources
  smart:  mm, ce, gm, pinp, rb
  legacy: sb, ups, hc, jm, pp, tr, dh

Usage (run from re_app/ directory):
    python3 quick_update_all.py                        # all sources
    python3 quick_update_all.py --only mm gm pinp      # specific sources
    python3 quick_update_all.py --skip tr dh           # exclude sources
    python3 quick_update_all.py --no-migrate           # skip migrate step
    python3 quick_update_all.py --dry-run              # print plan only

Requires: pip3 install requests beautifulsoup4
"""

import os, sys, json, re, time, random, shutil, argparse, logging, subprocess
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urljoin, urlparse

try:
    import requests
    from bs4 import BeautifulSoup
except ImportError:
    print("Run: pip3 install requests beautifulsoup4")
    sys.exit(1)

# ── Paths ─────────────────────────────────────────────────────────────────────

ROOT     = os.path.dirname(os.path.abspath(__file__))
SCRAPERS = os.path.join(ROOT, 'scrapers')
SUPABASE = os.path.join(ROOT, 'supabase')
PYTHON   = sys.executable

GEOCODE_SCRIPT = os.path.join(ROOT, 'geocode.py')
MIGRATE_SCRIPT = os.path.join(SUPABASE, 'migrate_data.py')

# ── Logging ───────────────────────────────────────────────────────────────────

os.makedirs(os.path.join(ROOT, 'logs'), exist_ok=True)
log_file = os.path.join(
    ROOT, 'logs',
    f"quick_update_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
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

DELAY             = 1.5
MAX_RETRIES       = 3
CONSEC_THRESHOLD  = 5    # stop after this many consecutive known URLs

# ── Source registry ───────────────────────────────────────────────────────────
#
# smart  → run full scraper with --quick (already incremental, url_map based)
# legacy → run inline quick scan (url-map derived from existing JSON files)
#
# For legacy sources, list_urls is a list of (page1_url, page_N_template).
# The template must contain {N} for the page number (N >= 2).
#
# link_pattern: regex that matches a property detail href on the listing page.

ALL_SOURCES = {

    # ── Smart scrapers ────────────────────────────────────────────────────────
    'mm': {
        'type':   'smart',
        'script': os.path.join(SCRAPERS, 'mm_full_scrape.py'),
    },
    'ce': {
        'type':   'smart',
        'script': os.path.join(SCRAPERS, 'ce_full_scrape.py'),
    },
    'gm': {
        'type':   'smart',
        'script': os.path.join(SCRAPERS, 'gm_full_scrape.py'),
    },
    'pinp': {
        'type':   'smart',
        'script': os.path.join(SCRAPERS, 'pinp_full_scrape.py'),
    },
    'rb': {
        'type':   'smart',
        'script': os.path.join(SCRAPERS, 'rb_full_scrape.py'),
    },

    # ── Legacy scrapers ───────────────────────────────────────────────────────
    'sb': {
        'type':        'legacy',
        'base_url':    'https://www.stuartbaillie.com',
        'props_dir':   os.path.join(ROOT, 'properties', 'sb'),
        'cms':         'pp',    # CSS selector family: pp = PropertyPal CMS (JM/PP/SB/UPS/HC/TR/DH)
        'list_urls': [
            ('https://www.stuartbaillie.com/property-for-sale',
             'https://www.stuartbaillie.com/property-for-sale/page{N}/'),
            ('https://www.stuartbaillie.com/property-to-rent',
             'https://www.stuartbaillie.com/property-to-rent/page{N}/'),
        ],
        'link_pattern': r'/property/[^/]+/\d{4,}',
    },
    'ups': {
        'type':        'legacy',
        'base_url':    'https://www.ulsterpropertysales.co.uk',
        'props_dir':   os.path.join(ROOT, 'properties', 'ups'),
        'cms':         'pp',
        'list_urls': [
            ('https://www.ulsterpropertysales.co.uk/property-for-sale',
             'https://www.ulsterpropertysales.co.uk/property-for-sale/page{N}/'),
            ('https://www.ulsterpropertysales.co.uk/property-to-rent',
             'https://www.ulsterpropertysales.co.uk/property-to-rent/page{N}/'),
        ],
        'link_pattern': r'/property/[^/]+/\d{4,}',
    },
    'hc': {
        'type':        'legacy',
        'base_url':    'https://www.huntercampbell.co.uk',
        'props_dir':   os.path.join(ROOT, 'properties', 'hc'),
        'cms':         'pp',
        'list_urls': [
            ('https://www.huntercampbell.co.uk/property-for-sale',
             'https://www.huntercampbell.co.uk/property-for-sale/page{N}/'),
            ('https://www.huntercampbell.co.uk/property-to-rent',
             'https://www.huntercampbell.co.uk/property-to-rent/page{N}/'),
        ],
        'link_pattern': r'/property/[^/]+/\d{4,}',
    },
    'jm': {
        'type':        'legacy',
        'base_url':    'https://www.johnminnis.co.uk',
        'props_dir':   os.path.join(ROOT, 'properties', 'jm'),
        'cms':         'pp',
        'list_urls': [
            ('https://www.johnminnis.co.uk/search/906207/page1/',
             'https://www.johnminnis.co.uk/search/906207/page{N}/'),
        ],
        'link_pattern': r'/property/[^/]+/\d{4,}',
    },
    'pp': {
        'type':        'legacy',
        'base_url':    'https://www.propertypeopleni.com',
        'props_dir':   os.path.join(ROOT, 'properties', 'pp'),
        'cms':         'pp',
        'list_urls': [
            ('https://www.propertypeopleni.com/property-for-sale/page1/',
             'https://www.propertypeopleni.com/property-for-sale/page{N}/'),
            ('https://www.propertypeopleni.com/property-to-rent/page1/',
             'https://www.propertypeopleni.com/property-to-rent/page{N}/'),
        ],
        'link_pattern': r'/property/[^/]+/\d{4,}',
    },
    'tr': {
        'type':        'legacy',
        'base_url':    'https://www.templeton-robinson.co.uk',
        'props_dir':   os.path.join(ROOT, 'properties', 'tr'),
        'cms':         'pp',
        'list_urls': [
            ('https://www.templeton-robinson.co.uk/property-for-sale',
             'https://www.templeton-robinson.co.uk/property-for-sale/page{N}/'),
            ('https://www.templeton-robinson.co.uk/property-to-rent',
             'https://www.templeton-robinson.co.uk/property-to-rent/page{N}/'),
        ],
        'link_pattern': r'/property/[^/]+/\d{4,}',
    },
    'dh': {
        'type':        'legacy',
        'base_url':    'https://www.dhhomes.co.uk',
        'props_dir':   os.path.join(ROOT, 'properties', 'dh'),
        'cms':         'pp',
        'list_urls': [
            ('https://www.dhhomes.co.uk/property-for-sale',
             'https://www.dhhomes.co.uk/property-for-sale/page{N}/'),
            ('https://www.dhhomes.co.uk/property-to-rent',
             'https://www.dhhomes.co.uk/property-to-rent/page{N}/'),
        ],
        'link_pattern': r'/property/[^/]+/\d{4,}',
    },
}

# ── HTTP helpers ───────────────────────────────────────────────────────────────

def http_get(url, retries=MAX_RETRIES):
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=HEADERS, timeout=25)
            r.raise_for_status()
            return r
        except Exception as e:
            if attempt < retries - 1:
                time.sleep(3 * (attempt + 1))
    return None

# ── Legacy source helpers ─────────────────────────────────────────────────────

def build_known_urls(props_dir):
    """
    Read all existing property JSON files and return a set of known URLs.
    Also tries url_map.json first (faster, more reliable).
    """
    known = set()

    # url_map.json (smart-style legacy): url → folder_name
    map_path = os.path.join(props_dir, 'url_map.json')
    if os.path.isfile(map_path):
        try:
            m = json.load(open(map_path, encoding='utf-8'))
            known.update(m.keys())
            return known
        except Exception:
            pass

    # Fall back to scanning individual JSON files
    if not os.path.isdir(props_dir):
        return known
    for d in os.listdir(props_dir):
        if not re.fullmatch(r'property_\d+', d):
            continue
        jpath = os.path.join(props_dir, d, f'{d}.json')
        if os.path.isfile(jpath):
            try:
                with open(jpath, encoding='utf-8') as f:
                    data = json.load(f)
                url = data.get('url', '').strip()
                if url:
                    known.add(url)
            except Exception:
                pass
    return known

def load_or_create_url_map(props_dir):
    map_path = os.path.join(props_dir, 'url_map.json')
    if os.path.isfile(map_path):
        try:
            return json.load(open(map_path, encoding='utf-8'))
        except Exception:
            pass
    # Build from existing JSON files
    url_map = {}
    if os.path.isdir(props_dir):
        for d in os.listdir(props_dir):
            if not re.fullmatch(r'property_\d+', d):
                continue
            jpath = os.path.join(props_dir, d, f'{d}.json')
            if os.path.isfile(jpath):
                try:
                    with open(jpath, encoding='utf-8') as f:
                        data = json.load(f)
                    url = data.get('url', '').strip()
                    if url:
                        url_map[url] = d
                except Exception:
                    pass
    return url_map

def save_url_map(props_dir, url_map):
    map_path = os.path.join(props_dir, 'url_map.json')
    with open(map_path, 'w', encoding='utf-8') as f:
        json.dump(url_map, f, indent=2, ensure_ascii=False)

def next_prop_id(props_dir, url_map):
    existing = [
        int(v.replace('property_', ''))
        for v in url_map.values()
        if re.fullmatch(r'property_\d+', v)
    ]
    if os.path.isdir(props_dir):
        for d in os.listdir(props_dir):
            m = re.fullmatch(r'property_(\d+)', d)
            if m:
                existing.append(int(m.group(1)))
    return max(existing, default=0) + 1

# ── Shared PropertyPal CMS detail page parser (covers all legacy sources) ─────

def normalise_status(raw):
    s = (raw or '').strip().lower()
    if 'agreed' in s: return 'Sale Agreed'
    if 'sold'   in s: return 'Sold'
    if 'let'    in s: return 'Let'
    return 'For Sale'

def extract_image_urls_pp(soup, url):
    """Extract images from a PropertyPal CMS property page."""
    seen = set()
    urls = []

    def add(src):
        if src:
            full = urljoin(url, src) if not src.startswith('http') else src
            if full not in seen and full.startswith('http'):
                seen.add(full)
                urls.append(full)

    gallery = soup.find('ul', id='gallery') or soup.find('div', id='gallery')
    if gallery:
        for a in gallery.find_all('a', href=True):
            if 'slick-cloned' not in (a.get('class') or []):
                add(a['href'])
        if urls:
            return urls

    # Fallback: property ID pattern from URL
    m = re.search(r'/property/[^/]+/([^/]+)/', url)
    if m:
        prop_id = m.group(1)
        base_img = f'/images/property/1/{prop_id}/'
        for a in soup.find_all('a', href=True):
            href = a['href']
            if base_img in href or (prop_id in href and
                    any(href.lower().endswith(ext) for ext in ('.jpg', '.jpeg', '.png', '.webp'))):
                add(href)
        if urls:
            return urls

    for img in soup.find_all('img'):
        src = img.get('src') or img.get('data-src') or img.get('data-lazy-src')
        if src and '/images/property/' in src:
            if not any(x in src.lower() for x in ('logo', 'office', 'icon', 'favicon')):
                add(src)

    return urls

def parse_pp_detail(html, url):
    """Parse a PropertyPal CMS property detail page (covers JM, PP, SB, UPS, HC, TR, DH)."""
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
            # Strip common site-name suffixes
            for sep in [' | ', ' - ', ' — ']:
                if sep in t:
                    t = t[:t.rfind(sep)]
            data['address'] = t.strip()
    data.setdefault('address', '')
    data['title'] = data['address']

    # Price
    for sel in ['span.price-amount', '.Price-priceValue', '[class*="price-value"]']:
        el = soup.select_one(sel)
        if el:
            t = el.get_text(strip=True)
            if t:
                qualifier = soup.select_one('.Price-priceOffers, [class*="priceOffers"]')
                q = (qualifier.get_text(strip=True) + ' ') if qualifier else ''
                data['price_str'] = (q + t).strip()
                break
    if not data.get('price_str'):
        og_desc = soup.find('meta', property='og:description')
        if og_desc and og_desc.get('content'):
            m = re.search(r'(£[\d,]+(?:pm|pcm)?|POA)', og_desc['content'], re.I)
            if m:
                data['price_str'] = m.group(1)

    # Metadata (ul.dettbl style)
    for li in soup.select('ul.dettbl li'):
        dt1 = li.find(class_='dt1')
        dt2 = li.find(class_='dt2')
        if not dt1 or not dt2:
            continue
        key = dt1.get_text(strip=True).lower()
        val = dt2.get_text(strip=True)
        if 'price' in key:
            data.setdefault('price_str', val)
        elif 'style' in key or 'type' in key:
            data['type'] = val
        elif 'bedroom' in key:
            data['bedrooms'] = val
        elif 'reception' in key:
            data['receptions'] = val
        elif 'bathroom' in key:
            data['bathrooms'] = val
        elif 'status' in key:
            data['status'] = val
        elif 'heating' in key:
            data['heating'] = val

    # SingleListingPage CMS attributes (MM/CE/GM style)
    if not data.get('type'):
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

    # Status fallback
    if not data.get('status'):
        for sel in ['.SingleListingPage-topEle', 'div.dtsm', '.status']:
            el = soup.select_one(sel)
            if el:
                data['status'] = normalise_status(el.get_text(separator=' ', strip=True))
                break
        data.setdefault('status', normalise_status(''))

    # Key features
    feats = []
    for sel in ['ul.feats li', 'div.prop-det-feats .feat',
                '.DescriptionBox--bullets li', '.DescriptionBox--bullets p',
                'ul.features li']:
        feats = [el.get_text(strip=True) for el in soup.select(sel) if el.get_text(strip=True)]
        if feats:
            break
    data['key_features'] = feats

    # Description
    desc = ''
    for sel in ['div.textbp', 'div.prop-det-text .text', '.ListingDescr-text',
                'div.description']:
        el = soup.select_one(sel)
        if el:
            t = el.get_text(separator=' ', strip=True)
            if len(t) > len(desc):
                desc = t
    data['description'] = desc

    # Rooms
    rooms = []
    for room_row in soup.select('div.prop-det-rooms div.room-row'):
        room_name = room_row.find('span', class_='room-name')
        room_desc = room_row.find('span', class_='room-desc')
        rd = {'name': '', 'dimensions': '', 'description': ''}
        if room_name:
            dim_span = room_name.find('span')
            if dim_span:
                rd['dimensions'] = dim_span.get_text(strip=True)
                rd['name'] = room_name.get_text(strip=True).replace(
                    dim_span.get_text(strip=True), '').strip()
            else:
                rd['name'] = room_name.get_text(strip=True)
        if room_desc:
            ds = room_desc.find('span')
            rd['description'] = ds.get_text(strip=True) if ds else room_desc.get_text(strip=True)
        if rd['name']:
            rooms.append(rd)
    data['rooms'] = rooms

    data['image_urls'] = extract_image_urls_pp(soup, url)
    return data

def scrape_and_save_legacy(url, folder_name, props_dir):
    """Fetch and save one property for a legacy source."""
    r = http_get(url)
    if not r:
        return None

    data = parse_pp_detail(r.text, url)

    prop_dir = os.path.join(props_dir, folder_name)
    os.makedirs(prop_dir, exist_ok=True)

    for i, img_url in enumerate(data.get('image_urls', []), 1):
        try:
            img_r = http_get(img_url)
            if img_r:
                ext = os.path.splitext(urlparse(img_url).path)[1] or '.jpg'
                with open(os.path.join(prop_dir, f'img{i}{ext}'), 'wb') as f:
                    f.write(img_r.content)
        except Exception:
            pass

    jpath = os.path.join(prop_dir, f'{folder_name}.json')
    with open(jpath, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    return data

# ── Quick scan for one legacy source ─────────────────────────────────────────

def quick_scan_legacy(key, cfg, label=None):
    label = label or key.upper()
    props_dir = cfg['props_dir']
    os.makedirs(props_dir, exist_ok=True)

    logger.info(f"  [{label}] Loading known URLs…")
    url_map = load_or_create_url_map(props_dir)
    known   = set(url_map.keys())
    logger.info(f"  [{label}] {len(known)} known properties locally")

    new_urls   = []
    base_url   = cfg['base_url']
    link_pat   = re.compile(cfg['link_pattern'])

    for page1_url, page_n_tmpl in cfg['list_urls']:
        page = 1
        consec_known = 0

        while True:
            url = page1_url if page == 1 else page_n_tmpl.format(N=page)
            r = http_get(url)
            if not r:
                logger.warning(f"  [{label}] Failed to fetch {url}")
                break

            soup = BeautifulSoup(r.content, 'html.parser')
            new_this_page = 0

            for a in soup.find_all('a', href=True):
                href = a['href']
                if not link_pat.search(href):
                    continue
                canonical = urljoin(base_url, href.split('?')[0].rstrip('/'))
                if canonical not in known and canonical not in new_urls:
                    new_urls.append(canonical)
                    consec_known = 0
                    new_this_page += 1
                else:
                    consec_known += 1
                    if consec_known >= CONSEC_THRESHOLD:
                        break

            logger.info(f"  [{label}] page {page}: {new_this_page} new "
                        f"(consec_known={consec_known})")

            if consec_known >= CONSEC_THRESHOLD or new_this_page == 0:
                break

            page += 1
            time.sleep(DELAY + random.uniform(0, 0.5))

    logger.info(f"  [{label}] {len(new_urls)} new properties to scrape")

    if not new_urls:
        return 0

    next_id = next_prop_id(props_dir, url_map)
    added = 0

    for i, prop_url in enumerate(new_urls, 1):
        folder = f"property_{next_id}"
        next_id += 1
        logger.info(f"  [{label}] [{i}/{len(new_urls)}] NEW {folder}: {prop_url}")
        data = scrape_and_save_legacy(prop_url, folder, props_dir)
        if data:
            url_map[prop_url] = folder
            save_url_map(props_dir, url_map)
            added += 1
        time.sleep(DELAY + random.uniform(0, 0.5))

    logger.info(f"  [{label}] Done — {added} new properties added")
    return added

# ── Run a smart scraper as subprocess ─────────────────────────────────────────

def run_smart_scraper(script_path, label):
    cmd = [PYTHON, script_path, '--quick']
    logger.info(f"  [{label}] → {os.path.basename(script_path)} --quick")
    t0 = time.monotonic()
    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            cwd=ROOT,
            bufsize=1,
        )
        for line in proc.stdout:
            line = line.rstrip('\n')
            if line:
                logger.info(f"    [{label}] {line}")
        proc.wait()
    except Exception as e:
        logger.error(f"  [{label}] failed to start: {e}")
        return -1
    elapsed = time.monotonic() - t0
    status  = 'OK' if proc.returncode == 0 else f'FAILED (rc={proc.returncode})'
    logger.info(f"  [{label}] {status} ({elapsed:.0f}s)")
    return proc.returncode

# ── Run a single script (geocode / migrate) ───────────────────────────────────

def run_script(script_path, extra_args=None, label=None):
    label = label or os.path.basename(script_path).replace('.py', '')
    cmd   = [PYTHON, script_path] + (extra_args or [])
    logger.info(f"  [{label}] → {os.path.basename(script_path)}")
    t0 = time.monotonic()
    try:
        proc = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, cwd=ROOT, bufsize=1,
        )
        for line in proc.stdout:
            line = line.rstrip('\n')
            if line:
                logger.info(f"    [{label}] {line}")
        proc.wait()
    except Exception as e:
        logger.error(f"  [{label}] failed to start: {e}")
        return -1
    elapsed = time.monotonic() - t0
    status  = 'OK' if proc.returncode == 0 else f'FAILED (rc={proc.returncode})'
    logger.info(f"  [{label}] {status} ({elapsed:.0f}s)")
    return proc.returncode

# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description='Quick intra-day scan for new property listings across all agents.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            'Examples:\n'
            '  python3 quick_update_all.py                # all sources\n'
            '  python3 quick_update_all.py --only mm gm   # only McMillan + Gareth Mills\n'
            '  python3 quick_update_all.py --skip tr dh   # all except TR and DH\n'
            '  python3 quick_update_all.py --dry-run      # print plan only\n'
        )
    )
    parser.add_argument('--only', nargs='+', metavar='SOURCE',
                        help=f"Run only these sources ({', '.join(ALL_SOURCES)})")
    parser.add_argument('--skip', nargs='+', metavar='SOURCE',
                        help='Skip these sources')
    parser.add_argument('--no-migrate', action='store_true',
                        help='Skip migrate_data.py at the end')
    parser.add_argument('--dry-run',    action='store_true',
                        help='Print plan but do not execute')
    args = parser.parse_args()

    # Resolve active sources
    if args.only:
        unknown = [s for s in args.only if s not in ALL_SOURCES]
        if unknown:
            parser.error(f"Unknown source(s): {', '.join(unknown)}")
        active = {k: v for k, v in ALL_SOURCES.items() if k in args.only}
    else:
        active = dict(ALL_SOURCES)

    if args.skip:
        for s in args.skip:
            active.pop(s, None)

    if not active:
        parser.error("No sources selected.")

    smart_sources  = {k: v for k, v in active.items() if v['type'] == 'smart'}
    legacy_sources = {k: v for k, v in active.items() if v['type'] == 'legacy'}

    logger.info('=' * 60)
    logger.info(f"quick_update_all.py — {datetime.now().isoformat()}")
    logger.info(f"Smart  sources: {', '.join(smart_sources) or '(none)'}")
    logger.info(f"Legacy sources: {', '.join(legacy_sources) or '(none)'}")
    logger.info(f"Options: no_migrate={args.no_migrate}  dry_run={args.dry_run}")
    logger.info(f"Log: {log_file}")

    if args.dry_run:
        logger.info('\nPlan:')
        for k in smart_sources:
            logger.info(f"  [smart]  {k} → {os.path.basename(ALL_SOURCES[k]['script'])} --quick")
        for k in legacy_sources:
            logger.info(f"  [legacy] {k} → inline quick scan")
        logger.info('\n--dry-run: nothing executed.')
        return

    overall_start = time.monotonic()
    results = {}

    # ── Step 1: run all scans in parallel ────────────────────────────────────
    logger.info(f"\n{'='*60}")
    logger.info("▶  Step 1: Quick scans (all sources in parallel)")

    def run_source(key, cfg):
        if cfg['type'] == 'smart':
            rc = run_smart_scraper(cfg['script'], label=key)
            return key, rc
        else:
            try:
                added = quick_scan_legacy(key, cfg, label=key)
                return key, 0 if added >= 0 else 1
            except Exception as e:
                logger.error(f"  [{key}] exception: {e}")
                return key, 1

    with ThreadPoolExecutor(max_workers=len(active)) as pool:
        futures = {pool.submit(run_source, k, v): k for k, v in active.items()}
        for future in as_completed(futures):
            key, rc = future.result()
            results[key] = rc
            if rc != 0:
                logger.warning(f"  ⚠  {key} reported an error (rc={rc})")

    # ── Step 2: geocode new addresses ────────────────────────────────────────
    logger.info(f"\n{'='*60}")
    logger.info("▶  Step 2: Geocode new addresses")
    if os.path.exists(GEOCODE_SCRIPT):
        run_script(GEOCODE_SCRIPT, ['--no-nominatim'], label='geocode')
    else:
        logger.warning(f"  geocode.py not found at {GEOCODE_SCRIPT} — skipping")

    # ── Step 3: migrate to Supabase ──────────────────────────────────────────
    if not args.no_migrate:
        logger.info(f"\n{'='*60}")
        logger.info("▶  Step 3: Migrate to Supabase")
        if os.path.exists(MIGRATE_SCRIPT):
            run_script(MIGRATE_SCRIPT, label='migrate_data')
        else:
            logger.warning(f"  migrate_data.py not found — skipping")

    # ── Summary ───────────────────────────────────────────────────────────────
    total_time = time.monotonic() - overall_start
    had_failures = any(rc != 0 for rc in results.values())

    logger.info(f"\n{'='*60}")
    logger.info(f"Quick update complete — {total_time:.0f}s total")
    for key, rc in results.items():
        icon = '✓' if rc == 0 else '✗'
        logger.info(f"  {icon}  {key}  (rc={rc})")

    if had_failures:
        logger.warning('\nSome sources reported errors — review the log above.')
        sys.exit(1)
    else:
        logger.info('\nAll sources completed successfully.')

    logger.info(f"\nLog saved to: {log_file}")


if __name__ == '__main__':
    main()
