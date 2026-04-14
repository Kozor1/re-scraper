#!/usr/bin/env python3
"""
daily_sync.py  –  Daily property sync for all 6 NI estate agent sources.

For each source this script:
  1. Walks ALL listing pages to get the complete set of currently live property URLs
  2. Scrapes + uploads any NEW properties (text + images)
  3. Deletes DELISTED properties from Supabase and the local index
  4. Re-scrapes text fields (price, status, description) for EXISTING properties
     and pushes any changes to Supabase
  5. Runs the TR selenium backfill for any TR properties still missing descriptions

Intended to run once per day, e.g. via cron:
    0 3 * * *  cd ~/Desktop/re_app && source venv/bin/activate && python3 daily_sync.py

Usage:
    python3 daily_sync.py                     # sync all 6 sources
    python3 daily_sync.py sb tr               # sync specific sources only
    python3 daily_sync.py --dry-run           # report changes without applying them
    python3 daily_sync.py --no-text-update    # skip text re-scrape for existing properties
    python3 daily_sync.py --no-selenium       # skip TR selenium backfill
    python3 daily_sync.py --max-pages 20      # cap listing pages checked per source
"""

import os
import sys
import re
import json
import time
import random
import logging
import argparse
import importlib
import traceback
import subprocess
from datetime import datetime
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

# Scraper modules live in scrapers/ — add to path so importlib can find them
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'scrapers'))

# ── Paths ─────────────────────────────────────────────────────────────────────

ROOT = os.path.dirname(os.path.abspath(__file__))
GEOCODE_SCRIPT = os.path.join(ROOT, 'geocode.py')

# Load .env (needed for Supabase credentials)
env_path = os.path.join(ROOT, '.env')
if os.path.exists(env_path):
    for line in open(env_path):
        line = line.strip()
        if line and not line.startswith('#') and '=' in line:
            k, v = line.split('=', 1)
            os.environ.setdefault(k.strip(), v.strip())

# ── Supabase (lazy — only initialised when first needed) ──────────────────────

_supabase_client = None

def get_supabase():
    global _supabase_client
    if _supabase_client is not None:
        return _supabase_client
    try:
        from supabase import create_client
    except ImportError:
        logger.error("supabase package not installed. Run: pip install supabase")
        sys.exit(1)
    url = os.environ.get('SUPABASE_URL')
    key = os.environ.get('SUPABASE_SERVICE_KEY')
    if not url or not key:
        logger.error("Set SUPABASE_URL and SUPABASE_SERVICE_KEY in .env or environment")
        sys.exit(1)
    _supabase_client = create_client(url, key)
    return _supabase_client


# ── Load geocache ─────────────────────────────────────────────────────────────

geocache_path = os.path.join(ROOT, 'geocache.json')
geocache = {}
if os.path.exists(geocache_path):
    geocache = json.load(open(geocache_path))

# ── Source config ─────────────────────────────────────────────────────────────

SOURCES = {
    'sb': {
        'label':        'Simon Brien',
        'module':       'sb_full_scrape',
        'props_dir':    os.path.join(ROOT, 'properties', 'sb'),
        'link_pattern': '/buy/',
        'scrape_style': 'sb',
        'listing_page': lambda n: (
            'https://www.simonbrien.com/property-for-sale'
            if n == 1
            else f'https://www.simonbrien.com/property-for-sale/page{n}/?orderBy='
        ),
    },
    'ups': {
        'label':        'Ulster Property Sales',
        'module':       'ups_full_scrape',
        'props_dir':    os.path.join(ROOT, 'properties', 'ups'),
        'link_pattern': '/property/',
        'scrape_style': 'standard',
        'listing_page': lambda n: f'https://www.ulsterpropertysales.co.uk/property-for-sale/page{n}/',
    },
    'hc': {
        'label':        'Hunter Campbell',
        'module':       'hc_full_scrape',
        'props_dir':    os.path.join(ROOT, 'properties', 'hc'),
        'link_pattern': '/property/',
        'scrape_style': 'standard',
        'listing_page': lambda n: (
            'https://www.huntercampbell.co.uk/residential-sales'
            if n == 1
            else f'https://www.huntercampbell.co.uk/residential-sales?page={n}'
        ),
    },
    'jm': {
        'label':        'John Minnis',
        'module':       'jm_full_scrape',
        'props_dir':    os.path.join(ROOT, 'properties', 'jm'),
        'link_pattern': '/property/',
        'scrape_style': 'standard',
        'listing_page': lambda n: f'https://www.johnminnis.co.uk/search/906207/page{n}/',
    },
    'pp': {
        'label':        'Property People NI',
        'module':       'pp_full_scrape',
        'props_dir':    os.path.join(ROOT, 'properties', 'pp'),
        'link_pattern': '/property/',
        'scrape_style': 'standard',
        'listing_page': lambda n: f'https://www.propertypeopleni.com/property-for-sale/page{n}/',
    },
    'tr': {
        'label':        'Templeton Robinson',
        'module':       'tr_full_scrape',
        'props_dir':    os.path.join(ROOT, 'properties', 'tr'),
        'link_pattern': '/property/',
        'scrape_style': 'standard',
        'listing_page': lambda n: f'https://www.templetonrobinson.com/property-for-sale/page{n}/',
    },
    # New agents (batch 2025-04)
    'mc': {
        'label':        'Michael Chandler',
        'module':       'mc_full_scrape',
        'props_dir':    os.path.join(ROOT, 'properties', 'mc'),
        'link_pattern': '/property/',
        'scrape_style': 'standard',
        'listing_page': lambda n: (
            'https://www.michael-chandler.co.uk/property-for-sale'
            if n == 1
            else f'https://www.michael-chandler.co.uk/property-for-sale?page={n}'
        ),
    },
    'ft': {
        'label':        'Fetherstons',
        'module':       'ft_full_scrape',
        'props_dir':    os.path.join(ROOT, 'properties', 'ft'),
        'link_pattern': '/property/',
        'scrape_style': 'standard',
        'listing_page': lambda n: (
            'https://www.fetherstons.com/property-for-sale'
            if n == 1
            else f'https://www.fetherstons.com/property-for-sale?page={n}'
        ),
    },
    'pr': {
        'label':        'Peter Rodgers',
        'module':       'pr_full_scrape',
        'props_dir':    os.path.join(ROOT, 'properties', 'pr'),
        'link_pattern': '/property/',
        'scrape_style': 'standard',
        'listing_page': lambda n: (
            'https://www.peterrogersestateagents.com/property-for-sale'
            if n == 1
            else f'https://www.peterrogersestateagents.com/property-for-sale?page={n}'
        ),
    },
    'cps': {
        'label':        'CPS',
        'module':       'cps_full_scrape',
        'props_dir':    os.path.join(ROOT, 'properties', 'cps'),
        'link_pattern': '/property/',
        'scrape_style': 'standard',
        'listing_page': lambda n: (
            'https://cps-property.com/property-for-sale'
            if n == 1
            else f'https://cps-property.com/property-for-sale?page={n}'
        ),
    },
    'hn': {
        'label':        'Hannath',
        'module':       'hn_full_scrape',
        'props_dir':    os.path.join(ROOT, 'properties', 'hn'),
        'link_pattern': '/property/',
        'scrape_style': 'standard',
        'listing_page': lambda n: (
            'https://www.hannath.com/all-sale-properties'
            if n == 1
            else f'https://www.hannath.com/all-sale-properties?page={n}'
        ),
    },
    'bt': {
        'label':        'Brian Todd',
        'module':       'bt_full_scrape',
        'props_dir':    os.path.join(ROOT, 'properties', 'bt'),
        'link_pattern': '/property/',
        'scrape_style': 'standard',
        'listing_page': lambda n: (
            'https://www.briantodd.co.uk/sale'
            if n == 1
            else f'https://www.briantodd.co.uk/sale?page={n}'
        ),
    },
    'rr': {
        'label':        'Reeds Rains',
        'module':       'rr_full_scrape',
        'props_dir':    os.path.join(ROOT, 'properties', 'rr'),
        'link_pattern': '/property/',
        'scrape_style': 'standard',
        'listing_page': lambda n: (
            'https://www.reedsrains.co.uk/properties-for-sale/northern-ireland'
            if n == 1
            else f'https://www.reedsrains.co.uk/properties-for-sale/northern-ireland?page={n}'
        ),
    },
    'ee': {
        'label':        'Edmonton Estates',
        'module':       'ee_full_scrape',
        'props_dir':    os.path.join(ROOT, 'properties', 'ee'),
        'link_pattern': '/property/',
        'scrape_style': 'standard',
        'listing_page': lambda n: (
            'https://www.edmondsonestates.co.uk/property-for-sale'
            if n == 1
            else f'https://www.edmondsonestates.co.uk/property-for-sale?page={n}'
        ),
    },
    'ag': {
        'label':        'Armstrong Gordon',
        'module':       'ag_full_scrape',
        'props_dir':    os.path.join(ROOT, 'properties', 'ag'),
        'link_pattern': '/property/',
        'scrape_style': 'standard',
        'listing_page': lambda n: (
            'https://www.armstronggordon.com/property-for-sale'
            if n == 1
            else f'https://www.armstronggordon.com/property-for-sale?page={n}'
        ),
    },
    'ta': {
        'label':        'The Agent',
        'module':       'ta_full_scrape',
        'props_dir':    os.path.join(ROOT, 'properties', 'ta'),
        'link_pattern': '/property/',
        'scrape_style': 'standard',
        'listing_page': lambda n: (
            'https://www.theagentni.com/property-for-sale'
            if n == 1
            else f'https://www.theagentni.com/property-for-sale?page={n}'
        ),
    },
    'abc': {
        'label':        'A Barton Company',
        'module':       'abc_full_scrape',
        'props_dir':    os.path.join(ROOT, 'properties', 'abc'),
        'link_pattern': '/property/',
        'scrape_style': 'standard',
        'listing_page': lambda n: (
            'https://www.abartoncompany.co.uk/property-for-sale'
            if n == 1
            else f'https://www.abartoncompany.co.uk/property-for-sale?page={n}'
        ),
    },
    'hg': {
        'label':        'Henry Graham',
        'module':       'hg_full_scrape',
        'props_dir':    os.path.join(ROOT, 'properties', 'hg'),
        'link_pattern': '/property/',
        'scrape_style': 'standard',
        'listing_page': lambda n: (
            'https://www.hgraham.co.uk/residential-sales'
            if n == 1
            else f'https://www.hgraham.co.uk/residential-sales?page={n}'
        ),
    },
    'le': {
        'label':        'Lennon Estates',
        'module':       'le_full_scrape',
        'props_dir':    os.path.join(ROOT, 'properties', 'le'),
        'link_pattern': '/properties/',
        'scrape_style': 'standard',
        'listing_page': lambda n: (
            'https://lennon-estates.com/properties/for-sale/'
            if n == 1
            else f'https://lennon-estates.com/properties/for-sale/page/{n}/'
        ),
    },
    'amd': {
        'label':        'Agar Murdoch and Deane',
        'module':       'amd_full_scrape',
        'props_dir':    os.path.join(ROOT, 'properties', 'amd'),
        'link_pattern': '/property/',
        'scrape_style': 'standard',
        'listing_page': lambda n: (
            'https://www.agarmurdochdeane.com/property-for-sale'
            if n == 1
            else f'https://www.agarmurdochdeane.com/property-for-sale?page={n}'
        ),
    },
    'tm': {
        'label':        'Tim Martin',
        'module':       'tm_full_scrape',
        'props_dir':    os.path.join(ROOT, 'properties', 'tm'),
        'link_pattern': '/properties/',
        'scrape_style': 'standard',
        'listing_page': lambda n: (
            'https://www.timmartin.co.uk/properties-for-sale'
            if n == 1
            else f'https://www.timmartin.co.uk/properties-for-sale?page={n}'
        ),
    },
    'ma': {
        'label':        'McAllister',
        'module':       'ma_full_scrape',
        'props_dir':    os.path.join(ROOT, 'properties', 'ma'),
        'link_pattern': '/property/',
        'scrape_style': 'standard',
        'listing_page': lambda n: (
            'https://www.mc-allister.co.uk/property-for-sale'
            if n == 1
            else f'https://www.mc-allister.co.uk/property-for-sale?page={n}'
        ),
    },
    'dl': {
        'label':        'Dallas',
        'module':       'dl_full_scrape',
        'props_dir':    os.path.join(ROOT, 'properties', 'dl'),
        'link_pattern': '/property/',
        'scrape_style': 'standard',
        'listing_page': lambda n: (
            'https://www.dallasre.co.uk/homes-for-sale.php'
            if n == 1
            else f'https://www.dallasre.co.uk/homes-for-sale.php?p={n}'
        ),
    },
    'bmc': {
        'label':        'Bill McCann',
        'module':       'bmc_full_scrape',
        'props_dir':    os.path.join(ROOT, 'properties', 'bmc'),
        'link_pattern': '/property/',
        'scrape_style': 'standard',
        'listing_page': lambda n: (
            'https://billmccann.com/for-sale/'
            if n == 1
            else f'https://billmccann.com/for-sale/page/{n}/'
        ),
    },
    'ag2': {
        'label':        'Andrews & Gregg',
        'module':       'ag2_full_scrape',
        'props_dir':    os.path.join(ROOT, 'properties', 'ag2'),
        'link_pattern': '/property/',
        'scrape_style': 'standard',
        'listing_page': lambda n: (
            'https://www.andrewsandgregg.com/properties.aspx?mode=0&showsearch=1&commercial=0&menuID=30'
            if n == 1
            else f'https://www.andrewsandgregg.com/properties.aspx?mode=0&showsearch=1&commercial=0&menuID=30&page={n}'
        ),
    },
    'ipe': {
        'label':        'Independent Property Estates',
        'module':       'ipe_full_scrape',
        'props_dir':    os.path.join(ROOT, 'properties', 'ipe'),
        'link_pattern': '/property/',
        'scrape_style': 'standard',
        'listing_page': lambda n: (
            'https://ipestates.co.uk/property-for-sale'
            if n == 1
            else f'https://ipestates.co.uk/property-for-sale?page={n}'
        ),
    },
    'mmc': {
        'label':        'Montgomery & McCleary',
        'module':       'mmc_full_scrape',
        'props_dir':    os.path.join(ROOT, 'properties', 'mmc'),
        'link_pattern': '/property/',
        'scrape_style': 'standard',
        'listing_page': lambda n: (
            'https://www.montgomerymccleery.com/property-for-sale'
            if n == 1
            else f'https://www.montgomerymccleery.com/property-for-sale?page={n}'
        ),
    },
    'pe': {
        'label':        'Pauline Elliott',
        'module':       'pe_full_scrape',
        'props_dir':    os.path.join(ROOT, 'properties', 'pe'),
        'link_pattern': '/property/',
        'scrape_style': 'standard',
        'listing_page': lambda n: (
            'https://www.paulineelliottestateagents.com/property-for-sale'
            if n == 1
            else f'https://www.paulineelliottestateagents.com/property-for-sale?page={n}'
        ),
    },
    # Missing agents from full_scrape.py
    'dh': {
        'label':        'Daniel Henry',
        'module':       'dh_scrape',
        'props_dir':    os.path.join(ROOT, 'properties', 'dh'),
        'link_pattern': '/property/',
        'scrape_style': 'standard',
        'listing_page': lambda n: (
            'https://www.danielhenry.co.uk/property-for-sale'
            if n == 1
            else f'https://www.danielhenry.co.uk/property-for-sale?page={n}'
        ),
    },
    'mm': {
        'label':        'McMillan McClure',
        'module':       'mm_full_scrape',
        'props_dir':    os.path.join(ROOT, 'properties', 'mm'),
        'link_pattern': '/property/',
        'scrape_style': 'standard',
        'listing_page': lambda n: (
            'https://www.mcmillanmccullough.com/property-for-sale'
            if n == 1
            else f'https://www.mcmillanmccullough.com/property-for-sale?page={n}'
        ),
    },
    'ce': {
        'label':        'Country Estates',
        'module':       'ce_full_scrape',
        'props_dir':    os.path.join(ROOT, 'properties', 'ce'),
        'link_pattern': '/property/',
        'scrape_style': 'standard',
        'listing_page': lambda n: (
            'https://www.countryestates.net/property-for-sale'
            if n == 1
            else f'https://www.countryestates.net/property-for-sale?page={n}'
        ),
    },
    'gm': {
        'label':        'Gareth Mills Est. Agents',
        'module':       'gm_full_scrape',
        'props_dir':    os.path.join(ROOT, 'properties', 'gm'),
        'link_pattern': '/property/',
        'scrape_style': 'standard',
        'listing_page': lambda n: (
            'https://www.garethmillesstateagents.co.uk/property-for-sale'
            if n == 1
            else f'https://www.garethmillesstateagents.co.uk/property-for-sale?page={n}'
        ),
    },
    'pinp': {
        'label':        'Pinpoint Property',
        'module':       'pinp_full_scrape',
        'props_dir':    os.path.join(ROOT, 'properties', 'pinp'),
        'link_pattern': '/property/',
        'scrape_style': 'standard',
        'listing_page': lambda n: (
            'https://www.pinpointproperty.co.uk/property-for-sale'
            if n == 1
            else f'https://www.pinpointproperty.co.uk/property-for-sale?page={n}'
        ),
    },
    'rb': {
        'label':        'Rodgers & Browne',
        'module':       'rb_full_scrape',
        'props_dir':    os.path.join(ROOT, 'properties', 'rb'),
        'link_pattern': '/property/',
        'scrape_style': 'standard',
        'listing_page': lambda n: (
            'https://www.rodgersandbrowne.co.uk/property-for-sale'
            if n == 1
            else f'https://www.rodgersandbrowne.co.uk/property-for-sale?page={n}'
        ),
    },
}

HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
        '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    ),
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-GB,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
}

# ── Logging ───────────────────────────────────────────────────────────────────

os.makedirs(os.path.join(ROOT, 'logs'), exist_ok=True)
log_filename = os.path.join(
    ROOT, 'logs',
    f"daily_sync_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
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

# ── HTTP helpers ──────────────────────────────────────────────────────────────

def fetch(url, retries=3):
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=HEADERS, timeout=30)
            r.raise_for_status()
            return r
        except requests.exceptions.RequestException as e:
            logger.warning(f"  [attempt {attempt+1}] {url}: {e}")
            if attempt < retries - 1:
                time.sleep((2 ** attempt) * random.uniform(1, 2.5))
    return None

# ── Listing-page link extraction ──────────────────────────────────────────────

def extract_links(soup, page_url, link_pattern):
    links = set()
    for a in soup.find_all('a', href=True):
        href = a['href']
        if link_pattern in href:
            full = urljoin(page_url, href).split('?')[0].split('#')[0].rstrip('/')
            links.add(full)
    return links


def get_all_live_urls(source_key, max_pages=200):
    """
    Walk every listing page for a source until no more property links appear.
    Returns the complete set of currently live property URLs.
    """
    cfg = SOURCES[source_key]
    live_urls = set()

    for page_num in range(1, max_pages + 1):
        page_url = cfg['listing_page'](page_num)
        logger.info(f"  Listing page {page_num}: {page_url}")

        r = fetch(page_url)
        if not r:
            logger.warning(f"  Failed to fetch page {page_num}, stopping.")
            break

        soup = BeautifulSoup(r.content, 'html.parser')
        links = extract_links(soup, page_url, cfg['link_pattern'])

        if not links:
            logger.info(f"  No property links on page {page_num} — end of listings.")
            break

        live_urls.update(links)
        logger.info(f"  Page {page_num}: {len(links)} listings (running total: {len(live_urls)})")
        time.sleep(random.uniform(1.5, 2.5))

    logger.info(f"  Total live URLs for {source_key.upper()}: {len(live_urls)}")
    return live_urls

# ── Local index helpers ───────────────────────────────────────────────────────

def load_index(source_key):
    index_path = os.path.join(SOURCES[source_key]['props_dir'], 'property_index.json')
    if os.path.isfile(index_path):
        try:
            with open(index_path, encoding='utf-8') as f:
                data = json.load(f)
            if isinstance(data, dict) and 'properties' in data:
                return data
        except Exception as e:
            logger.warning(f"  Could not load index for {source_key}: {e}")
    # Build from files as fallback
    return _build_index_from_files(source_key)


def _build_index_from_files(source_key):
    props_dir = SOURCES[source_key]['props_dir']
    entries = []
    if os.path.isdir(props_dir):
        for entry in os.listdir(props_dir):
            if not entry.startswith('property_'):
                continue
            json_path = os.path.join(props_dir, entry, f'{entry}.json')
            if not os.path.isfile(json_path):
                continue
            try:
                with open(json_path, encoding='utf-8') as f:
                    d = json.load(f)
                if d.get('url'):
                    entries.append({
                        'id': d.get('id', entry),
                        'url': d['url'],
                        'address': d.get('address', ''),
                        'title': d.get('title', ''),
                        'scraped_at': d.get('scraped_at', ''),
                    })
            except Exception:
                pass
    return {'properties': entries, 'last_updated': None}


def save_index(source_key, index):
    index_path = os.path.join(SOURCES[source_key]['props_dir'], 'property_index.json')
    index['last_updated'] = datetime.now().isoformat()
    with open(index_path, 'w', encoding='utf-8') as f:
        json.dump(index, f, indent=2, ensure_ascii=False)


def get_known_url_map(index):
    """Return {normalised_url: source_id} from an index."""
    return {
        e['url'].rstrip('/'): e['id']
        for e in index.get('properties', [])
        if e.get('url')
    }


def get_next_id(source_key):
    props_dir = SOURCES[source_key]['props_dir']
    if not os.path.isdir(props_dir):
        return 1
    nums = [
        int(d.replace('property_', ''))
        for d in os.listdir(props_dir)
        if d.startswith('property_') and d.replace('property_', '').isdigit()
    ]
    return max(nums, default=0) + 1

# ── Supabase helpers ──────────────────────────────────────────────────────────

def parse_price_value(price_str):
    if not price_str:
        return None
    m = re.search(r'£([\d,]+)', str(price_str))
    if m:
        try:
            return int(m.group(1).replace(',', ''))
        except Exception:
            pass
    try:
        return int(price_str)
    except Exception:
        return None


def normalise_status(raw):
    s = (raw or '').strip().lower()
    if s in ('for sale', 'forsale'):
        return 'For Sale'
    if s in ('sale agreed', 'saleagreed', 'agreed'):
        return 'Sale Agreed'
    if s == 'sold':
        return 'Sold'
    return (raw or '').strip() or 'For Sale'


def normalise_bedrooms(raw):
    if not raw:
        return None
    m = re.search(r'(\d+)', str(raw))
    return m.group(1) if m else str(raw)


def build_row(source, source_id, data):
    address = data.get('address') or data.get('title') or ''
    coords  = geocache.get(address)
    raw_status = (
        data.get('status') or
        (data.get('property_info') or {}).get('Status') or
        (data.get('property_info') or {}).get('status') or ''
    )
    prop_type = (
        data.get('type') or
        (data.get('property_info') or {}).get('Style') or
        (data.get('property_info') or {}).get('Type') or ''
    )
    raw_beds = (
        data.get('bedrooms') or
        (data.get('property_info') or {}).get('Bedrooms') or
        (data.get('property_info') or {}).get('bedrooms') or ''
    )
    price_str = data.get('price_str') or data.get('price') or ''
    if isinstance(price_str, int):
        price_str = f'£{price_str:,}'

    row = {
        'source':        source,
        'source_id':     source_id,
        'url':           data.get('url') or '',
        'address':       address,
        'title':         data.get('title') or address,
        'price':         str(price_str) if price_str else None,
        'price_value':   parse_price_value(price_str),
        'status':        normalise_status(raw_status),
        'property_type': prop_type or None,
        'bedrooms':      normalise_bedrooms(raw_beds),
        'bathrooms':     data.get('bathrooms') or None,
        'receptions':    data.get('receptions') or None,
        'epc_rating':    data.get('epc_rating') or None,
        'description':   data.get('description') or None,
        'key_features':  data.get('key_features') or [],
        'rooms':         data.get('rooms') or [],
        'image_urls':    data.get('image_urls') or data.get('images') or [],
        'scraped_at':    data.get('rescraped_at') or data.get('scraped_at') or None,
    }
    if coords:
        row['lat'] = coords['lat']
        row['lng'] = coords['lng']
    return row


def upsert_to_supabase(rows, dry_run=False):
    """Batch upsert rows into Supabase properties table."""
    if not rows:
        return 0
    if dry_run:
        logger.info(f"  [dry-run] Would upsert {len(rows)} rows to Supabase")
        return len(rows)
    BATCH = 100
    total_ok = 0
    for i in range(0, len(rows), BATCH):
        batch = rows[i:i + BATCH]
        try:
            get_supabase().table('properties') \
                .upsert(batch, on_conflict='source,source_id') \
                .execute()
            total_ok += len(batch)
            logger.info(f"  Upserted {total_ok}/{len(rows)} rows")
        except Exception as e:
            logger.error(f"  Supabase upsert error (batch {i}–{i+BATCH}): {e}")
    return total_ok


def delete_from_supabase(source, dead_urls, dry_run=False):
    """
    Delete all Supabase rows for a given source whose URL is in dead_urls.
    Works in batches of 50 to stay within URL length limits.
    Returns count of deleted rows.
    """
    if not dead_urls:
        return 0

    dead_list = list(dead_urls)
    total_deleted = 0
    BATCH = 50

    if dry_run:
        logger.info(f"  [dry-run] Would delete {len(dead_list)} delisted properties from Supabase")
        return len(dead_list)

    for i in range(0, len(dead_list), BATCH):
        batch = dead_list[i:i + BATCH]
        try:
            get_supabase().table('properties') \
                .delete() \
                .eq('source', source) \
                .in_('url', batch) \
                .execute()
            total_deleted += len(batch)
            logger.info(f"  Deleted {total_deleted}/{len(dead_list)} delisted properties from Supabase")
        except Exception as e:
            logger.error(f"  Supabase delete error: {e}")

    return total_deleted

# ── Geocoding ─────────────────────────────────────────────────────────────────

def run_geocoding():
    """
    Run geocode.py to geocode any new addresses.
    Returns bool indicating success.
    """
    if not os.path.isfile(GEOCODE_SCRIPT):
        logger.error(f"Geocode script not found: {GEOCODE_SCRIPT}")
        return False

    cmd = [sys.executable, GEOCODE_SCRIPT]
    logger.info(f"[GEOCODE] {' '.join(cmd)}")
    start = time.time()
    try:
        result = subprocess.run(
            cmd,
            cwd=ROOT,
            capture_output=False,
            text=True,
        )
        elapsed = time.time() - start
        if result.returncode == 0:
            logger.info(f"[GEOCODE] ✓ Done in {elapsed:.0f}s")
            return True
        else:
            logger.error(f"[GEOCODE] ✗ Exit {result.returncode} after {elapsed:.0f}s")
            return False
    except Exception as exc:
        logger.error(f"[GEOCODE] ✗ Exception: {exc}")
        return False


# ── New property scraping ─────────────────────────────────────────────────────

def scrape_new_properties(source_key, new_urls):
    """
    Scrape new properties using the source's own module.
    Returns list of (source_id, data) tuples for uploading to Supabase.
    """
    cfg = SOURCES[source_key]
    module_name = cfg['module']
    scrape_style = cfg.get('scrape_style', 'standard')
    props_dir = cfg['props_dir']

    try:
        if module_name in sys.modules:
            del sys.modules[module_name]
        mod = importlib.import_module(module_name)
    except Exception as e:
        logger.error(f"  Could not import {module_name}: {e}")
        return []

    next_id = get_next_id(source_key)
    results = []

    for idx, url in enumerate(sorted(new_urls), 1):
        prop_id = f'property_{next_id + idx - 1}'
        logger.info(f"  Scraping ({idx}/{len(new_urls)}) {prop_id}: {url}")
        try:
            if scrape_style == 'sb':
                prop_folder = os.path.join(props_dir, prop_id)
                os.makedirs(prop_folder, exist_ok=True)
                data = mod.scrape_property_details(url)
                if data:
                    data['id'] = prop_id
                    image_count = mod.scrape_property_images(url, prop_folder)
                    data['image_count'] = image_count
                    data['scraped_at'] = datetime.now().isoformat()
                    json_path = os.path.join(prop_folder, f'{prop_id}.json')
                    with open(json_path, 'w', encoding='utf-8') as f:
                        json.dump(data, f, indent=2, ensure_ascii=False)
                    results.append((prop_id, data))
            else:
                # Standard: mod.scrape_property_page handles folder, JSON, images
                # For TR this now uses selenium internally
                if scrape_style == 'standard' and hasattr(mod, 'scrape_property_page'):
                    # TR's new scraper expects (url, prop_id, driver) — check signature
                    import inspect
                    sig = inspect.signature(mod.scrape_property_page)
                    if 'driver' in sig.parameters:
                        # TR selenium scraper: need a driver
                        if not hasattr(scrape_new_properties, '_tr_driver'):
                            scrape_new_properties._tr_driver = mod.make_driver()
                        data = mod.scrape_property_page(
                            url, prop_id, scrape_new_properties._tr_driver
                        )
                    else:
                        data = mod.scrape_property_page(url, prop_id)

                    if data:
                        results.append((prop_id, data))
                    else:
                        raise ValueError("scrape_property_page returned None")

        except Exception as e:
            logger.error(f"  Failed {prop_id}: {e}")

        if idx < len(new_urls):
            time.sleep(random.uniform(1.5, 3.0))

    # Clean up TR selenium driver if we created one
    tr_driver = getattr(scrape_new_properties, '_tr_driver', None)
    if tr_driver:
        try:
            tr_driver.quit()
        except Exception:
            pass
        del scrape_new_properties._tr_driver

    return results

# ── Text update for existing properties ──────────────────────────────────────

def text_update_source(source_key, known_url_map, dry_run=False):
    """
    Re-scrape text fields for all existing properties of a source.
    Returns list of (source_id, data) tuples where changes were detected.
    """
    from scrapers.text_update import scrape_property_text, detect_changes

    props_dir = SOURCES[source_key]['props_dir']
    updated = []

    items = list(known_url_map.items())   # [(url, source_id), ...]
    total = len(items)

    for idx, (url, source_id) in enumerate(items, 1):
        json_path = os.path.join(props_dir, source_id, f'{source_id}.json')
        if not os.path.isfile(json_path):
            continue

        try:
            with open(json_path, encoding='utf-8') as f:
                existing = json.load(f)
        except Exception:
            continue

        logger.info(f"  Text update [{idx}/{total}] {source_id}")
        new_data = scrape_property_text(url, source_key)
        if not new_data:
            continue

        changes = detect_changes(existing, new_data)
        if changes:
            logger.info(f"    Changes: {[c['field'] for c in changes]}")

        # Merge updates into existing data
        merged = dict(existing)
        for k, v in new_data.items():
            if k in ('url', 'id', 'scraped_at'):
                continue
            from scrapers.text_update import TRACKED_FIELDS
            if k in TRACKED_FIELDS and k in existing and existing[k] != v and v:
                merged[f'_prev_{k}'] = existing[k]
            merged[k] = v
        merged['rescraped_at'] = datetime.now().isoformat()

        if not dry_run:
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(merged, f, indent=2, ensure_ascii=False)

        updated.append((source_id, merged))
        time.sleep(random.uniform(1.0, 2.0))

    return updated

# ── TR selenium backfill ──────────────────────────────────────────────────────

def run_tr_selenium_backfill(dry_run=False):
    """Run tr_selenium_scrape.py as a subprocess to fill any missing TR descriptions."""
    if dry_run:
        logger.info("  [dry-run] Would run TR selenium backfill")
        return True

    script = os.path.join(ROOT, 'scrapers', 'tr_selenium_scrape.py')
    if not os.path.isfile(script):
        logger.error(f"TR selenium scraper not found: {script}")
        return False

    logger.info("Launching TR selenium backfill…")
    try:
        result = subprocess.run(
            [sys.executable, script],
            cwd=ROOT,
            timeout=3600,
        )
        if result.returncode == 0:
            logger.info("TR selenium backfill complete.")
            return True
        else:
            logger.error(f"TR selenium backfill exited with code {result.returncode}")
            return False
    except subprocess.TimeoutExpired:
        logger.error("TR selenium backfill timed out after 1 hour.")
        return False
    except Exception as e:
        logger.error(f"TR selenium backfill error: {e}")
        return False

# ── Per-source sync ───────────────────────────────────────────────────────────

def sync_source(source_key, args):
    cfg = SOURCES[source_key]
    logger.info(f"{'='*60}")
    logger.info(f"Source: {cfg['label']} ({source_key.upper()})")

    # ── 1. Get complete set of currently live URLs ────────────────────────────
    logger.info("Step 1: Walking listing pages for live URLs…")
    live_urls = get_all_live_urls(source_key, max_pages=args.max_pages)
    live_urls_norm = {u.rstrip('/') for u in live_urls}

    # ── 2. Compare against local index ───────────────────────────────────────
    logger.info("Step 2: Comparing with local index…")
    index = load_index(source_key)
    known_url_map = get_known_url_map(index)  # {url: source_id}

    new_urls    = live_urls_norm - set(known_url_map.keys())
    dead_urls   = set(known_url_map.keys()) - live_urls_norm
    extant_urls = live_urls_norm & set(known_url_map.keys())

    logger.info(f"  Live: {len(live_urls_norm)}  Known: {len(known_url_map)}")
    logger.info(f"  → New: {len(new_urls)}  Delisted: {len(dead_urls)}  Existing: {len(extant_urls)}")

    stats = {
        'live':     len(live_urls_norm),
        'new':      len(new_urls),
        'delisted': len(dead_urls),
        'updated':  0,
        'errors':   0,
    }

    # ── 3. Scrape and upload new properties ───────────────────────────────────
    if new_urls:
        logger.info(f"Step 3: Scraping {len(new_urls)} new properties…")
        if not args.dry_run:
            scraped = scrape_new_properties(source_key, new_urls)
            logger.info(f"  Scraped {len(scraped)} of {len(new_urls)} new properties")

            # Upload to Supabase
            rows = []
            for source_id, data in scraped:
                try:
                    rows.append(build_row(source_key, source_id, data))
                except Exception as e:
                    logger.error(f"  build_row error for {source_id}: {e}")
                    stats['errors'] += 1
            upsert_to_supabase(rows)

            # Add to local index
            for source_id, data in scraped:
                index['properties'].append({
                    'id':         source_id,
                    'url':        data.get('url', ''),
                    'address':    data.get('address', ''),
                    'title':      data.get('title', ''),
                    'scraped_at': data.get('scraped_at', ''),
                })
            save_index(source_key, index)
        else:
            logger.info(f"  [dry-run] Would scrape and upload {len(new_urls)} new properties:")
            for url in sorted(new_urls):
                logger.info(f"    + {url}")
    else:
        logger.info("Step 3: No new properties.")

    # ── 4. Remove delisted properties ─────────────────────────────────────────
    if dead_urls:
        logger.info(f"Step 4: Removing {len(dead_urls)} delisted properties…")
        if args.dry_run:
            for url in sorted(dead_urls):
                source_id = known_url_map.get(url, '?')
                logger.info(f"  [dry-run] Would delete {source_id}: {url}")
        else:
            # Delete from Supabase
            delete_from_supabase(source_key, dead_urls)

            # Remove from local index
            dead_set = dead_urls  # already normalised
            index['properties'] = [
                e for e in index['properties']
                if e.get('url', '').rstrip('/') not in dead_set
            ]
            save_index(source_key, index)

            # Archive local JSON files to a delisted/ subfolder
            delisted_dir = os.path.join(cfg['props_dir'], 'delisted')
            os.makedirs(delisted_dir, exist_ok=True)
            for url in dead_urls:
                source_id = known_url_map.get(url)
                if source_id:
                    src = os.path.join(cfg['props_dir'], source_id)
                    dst = os.path.join(delisted_dir, source_id)
                    if os.path.isdir(src) and not os.path.exists(dst):
                        import shutil
                        shutil.move(src, dst)
                        logger.info(f"  Archived {source_id} → delisted/")
    else:
        logger.info("Step 4: No delisted properties.")

    # ── 5. Text update for existing properties ────────────────────────────────
    if not args.no_text_update and extant_urls:
        logger.info(f"Step 5: Text re-scrape for {len(extant_urls)} existing properties…")
        # Build url→source_id map for only the extant URLs
        extant_map = {u: known_url_map[u] for u in extant_urls if u in known_url_map}
        if not args.dry_run:
            updated = text_update_source(source_key, extant_map, dry_run=False)
            stats['updated'] = len(updated)

            # Upload changes to Supabase
            rows = []
            for source_id, data in updated:
                try:
                    rows.append(build_row(source_key, source_id, data))
                except Exception as e:
                    logger.error(f"  build_row error for {source_id}: {e}")
            if rows:
                upsert_to_supabase(rows)
                logger.info(f"  Pushed {len(rows)} text updates to Supabase")
        else:
            logger.info(f"  [dry-run] Would text-update {len(extant_urls)} existing properties")
    else:
        if args.no_text_update:
            logger.info("Step 5: Skipped (--no-text-update)")
        else:
            logger.info("Step 5: No existing properties to update.")

    return stats

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description='Daily property sync — finds new listings, removes delisted, updates prices.'
    )
    parser.add_argument('sources', nargs='*', default=list(SOURCES.keys()),
                        help='Sources to sync (default: all)')
    parser.add_argument('--dry-run', action='store_true',
                        help='Report changes without scraping, uploading, or deleting anything')
    parser.add_argument('--no-text-update', action='store_true',
                        help='Skip text re-scrape for existing properties (faster)')
    parser.add_argument('--no-selenium', action='store_true',
                        help='Skip TR selenium backfill (use if Chrome/selenium not installed)')
    parser.add_argument('--no-geocode', action='store_true',
                        help='Skip the geocoding step (skip calling geocode.py)')
    parser.add_argument('--max-pages', type=int, default=200,
                        help='Max listing pages to walk per source (default: 200)')
    args = parser.parse_args()

    sources_to_run = [s for s in args.sources if s in SOURCES]
    unknown = [s for s in args.sources if s not in SOURCES]
    if unknown:
        logger.warning(f"Unknown sources (ignored): {unknown}")
    if not sources_to_run:
        logger.error("No valid sources to sync.")
        sys.exit(1)

    mode = 'DRY RUN' if args.dry_run else 'LIVE'
    logger.info(f"daily_sync starting [{mode}]. Sources: {sources_to_run}")
    logger.info(f"Log: {log_filename}")
    overall_start = time.time()

    all_stats = {}

    for source_key in sources_to_run:
        try:
            stats = sync_source(source_key, args)
            all_stats[source_key] = stats
        except Exception as e:
            logger.error(f"Error syncing {source_key}: {e}")
            logger.error(traceback.format_exc())
            all_stats[source_key] = {'error': str(e)}

    # ── TR selenium backfill ──────────────────────────────────────────────────
    if 'tr' in sources_to_run and not args.no_selenium:
        logger.info(f"{'='*60}")
        logger.info("TR selenium backfill (fills any missing descriptions)…")
        run_tr_selenium_backfill(dry_run=args.dry_run)

    # ── Geocoding ────────────────────────────────────────────────────────────────
    geocode_ok = False
    if not args.no_geocode:
        if not args.dry_run:
            logger.info(f"{'='*60}")
            logger.info("Geocoding new addresses…")
            geocode_ok = run_geocoding()
        else:
            logger.info(f"{'='*60}")
            logger.info("[dry-run] Would run geocoding")
    else:
        logger.info("\n--no-geocode: skipping geocoding step.")

    # ── Summary ───────────────────────────────────────────────────────────────
    elapsed = time.time() - overall_start
    logger.info(f"{'='*60}")
    logger.info(f"daily_sync complete in {elapsed/60:.1f} min")

    # Geocode status
    if args.no_geocode:
        geocode_status = 'skipped'
    elif args.dry_run:
        geocode_status = 'dry-run'
    else:
        geocode_status = '✓' if geocode_ok else '✗'
    logger.info(f"Geocode: {geocode_status}")

    logger.info(f"{'Source':<6}  {'Label':<30}  {'New':>4}  {'Deleted':>7}  {'Updated':>7}")
    logger.info(f"{'-'*60}")
    for source_key, stats in all_stats.items():
        if 'error' in stats:
            logger.info(
                f"{source_key.upper():<6}  {SOURCES[source_key]['label']:<30}  ERROR: {stats['error']}"
            )
        else:
            logger.info(
                f"{source_key.upper():<6}  {SOURCES[source_key]['label']:<30}  "
                f"{stats.get('new', 0):>4}  "
                f"{stats.get('delisted', 0):>7}  "
                f"{stats.get('updated', 0):>7}"
            )
    logger.info(f"Log: {log_filename}")


if __name__ == '__main__':
    main()
