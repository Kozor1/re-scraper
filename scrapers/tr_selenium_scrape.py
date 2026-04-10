#!/usr/bin/env python3
"""
tr_selenium_scrape.py
─────────────────────
Uses headless Chrome (via Selenium + webdriver-manager) to fetch the
JavaScript-rendered description and key features for all Templeton Robinson
properties that are currently missing them.

Setup (run once):
    pip install selenium webdriver-manager

Usage:
    python3 tr_selenium_scrape.py               # update all missing TR properties
    python3 tr_selenium_scrape.py --test         # test on first property only, verbose output
    python3 tr_selenium_scrape.py --limit 20     # cap to 20 properties per run
    python3 tr_selenium_scrape.py --delay 3.0    # seconds between requests (default 2.0)
    python3 tr_selenium_scrape.py --all          # re-scrape even properties that already have data
"""

import argparse
import json
import logging
import os
import random
import time
from datetime import datetime

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

# ── Config ───────────────────────────────────────────────────────────────────

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
TR_DIR     = os.path.join(SCRIPT_DIR, 'properties', 'tr')

os.makedirs(os.path.join(SCRIPT_DIR, 'logs'), exist_ok=True)
log_filename = os.path.join(
    SCRIPT_DIR, 'logs',
    f"tr_selenium_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s  %(levelname)s  %(message)s',
    handlers=[
        logging.FileHandler(log_filename, encoding='utf-8'),
        logging.StreamHandler(),
    ]
)
logger = logging.getLogger(__name__)

# ── Selector config ───────────────────────────────────────────────────────────
# Description: try in order, first match with >100 chars wins.
# TR uses: div.textblock > div.textbp  (the textblock wrapper also holds <h2>Description</h2>)
DESCRIPTION_SELECTORS = [
    'div.textblock div.textbp',     # Templeton Robinson (confirmed from HTML dump)
    'div.prop-det-text div.text',   # SB / UPS / JM / HC shared CMS
    'div.prop-det-text',
    'div.property-description',
    'div.prop-description',
    'div.description',
    'div#description',
    'div.overview',
    'div.property-overview',
    'section.description',
    'div.prop-desc',
    'div.pdp-description',
    'div.desc',
]

# Key features: try in order, first match that yields ≥1 item wins.
# TR uses: ul.feats > li  (font-icon <span><i></i></span> carries no text, so .text is clean)
FEATURES_SELECTORS = [
    ('ul.feats > li',               False),  # Templeton Robinson (confirmed from HTML dump)
    ('div.prop-det-feats div.feat', False),  # SB / UPS / JM / HC shared CMS
    ('div.prop-det-feats li',       False),
    ('ul.features > li',            False),
    ('div.features > li',           False),
    ('ul.key-features > li',        False),
    ('div.key-features > li',       False),
    ('ul.property-features > li',   False),
    ('div.property-features > li',  False),
    ('div.pdp-features li',         False),
    ('ul.pdp-features li',          False),
]

# ── Driver setup ─────────────────────────────────────────────────────────────

def make_driver():
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
    # Suppress "DevTools listening …" noise
    opts.add_experimental_option('excludeSwitches', ['enable-logging'])

    service = Service(ChromeDriverManager().install())
    driver  = webdriver.Chrome(service=service, options=opts)
    driver.set_page_load_timeout(30)
    return driver


# ── Extraction helpers ────────────────────────────────────────────────────────

def get_text(driver, css):
    """Return stripped text of first matching element, or ''."""
    try:
        el = driver.find_element(By.CSS_SELECTOR, css)
        return el.text.strip()
    except Exception:
        return ''


def extract_description(driver, test_mode=False):
    for sel in DESCRIPTION_SELECTORS:
        try:
            els = driver.find_elements(By.CSS_SELECTOR, sel)
            for el in els:
                t = el.text.strip()
                if len(t) > 100:
                    if test_mode:
                        logger.info(f'  ✅ Description found via: {sel!r} ({len(t)} chars)')
                    return t
            if test_mode and els:
                logger.info(f'  · tried {sel!r} — matched {len(els)} element(s) but text too short')
        except Exception as e:
            if test_mode:
                logger.info(f'  · tried {sel!r} — error: {e}')
    if test_mode:
        logger.warning('  ❌ No description found with any selector')
    return ''


def extract_features(driver, test_mode=False):
    for sel, _ in FEATURES_SELECTORS:
        try:
            els = driver.find_elements(By.CSS_SELECTOR, sel)
            items = [e.text.strip() for e in els if e.text.strip()]
            if items:
                if test_mode:
                    logger.info(f'  ✅ Features found via: {sel!r} — {len(items)} items: {items[:5]}')
                return items
            if test_mode and els:
                logger.info(f'  · tried {sel!r} — matched elements but all text empty')
        except Exception as e:
            if test_mode:
                logger.info(f'  · tried {sel!r} — error: {e}')
    if test_mode:
        logger.warning('  ❌ No key features found with any selector')
    return []


def extract_rooms(driver, test_mode=False):
    """
    Extract room-by-room breakdown from TR's ul.rooms list.
    Returns a list of {name, dimensions, description} dicts.
    TR structure:
        <ul class="rooms">
          <li>
            <h3><span>ROOM NAME:</span> 12' x 10' (3.6m x 3.0m)</h3>
            <div class="textbp">Room description text…</div>
          </li>
        </ul>
    """
    rooms = []
    try:
        items = driver.find_elements(By.CSS_SELECTOR, 'ul.rooms > li')
        for li in items:
            room = {'name': '', 'dimensions': '', 'description': ''}
            try:
                h3 = li.find_element(By.CSS_SELECTOR, 'h3')
                h3_text = h3.text.strip()
                # h3 contains "ROOM NAME: 12' x 10'" — split on first colon
                if ':' in h3_text:
                    name_part, _, dim_part = h3_text.partition(':')
                    room['name']       = name_part.strip()
                    room['dimensions'] = dim_part.strip()
                else:
                    room['name'] = h3_text
            except Exception:
                pass
            try:
                desc_el = li.find_element(By.CSS_SELECTOR, 'div.textbp')
                room['description'] = desc_el.text.strip()
            except Exception:
                pass
            if room['name'] or room['description']:
                rooms.append(room)
    except Exception as e:
        if test_mode:
            logger.info(f'  · rooms extraction error: {e}')

    if test_mode:
        if rooms:
            logger.info(f'  ✅ Rooms found: {len(rooms)} — first: {rooms[0]}')
        else:
            logger.info('  · No rooms found (may not be present on all listings)')
    return rooms


# ── Page loading ──────────────────────────────────────────────────────────────

def load_page(driver, url, test_mode=False):
    """
    Load a property page and wait for meaningful content to appear.
    Returns True on success.
    """
    try:
        driver.get(url)
    except Exception as e:
        logger.error(f'  Page load error: {e}')
        return False

    # Wait up to 12 s for TR-specific content to appear
    try:
        WebDriverWait(driver, 12).until(
            lambda d: any(
                d.find_elements(By.CSS_SELECTOR, s)
                for s in ['div.textblock', 'ul.feats', 'div.dtsm',
                           'ul.rooms', 'h1', 'span.dpp']
            )
        )
    except Exception:
        # Page loaded but none of the sentinel elements appeared — continue anyway
        pass

    # Small extra pause to let lazy-loaded content settle
    time.sleep(1.5)

    if test_mode:
        logger.info(f'  Page title: {driver.title!r}')
        logger.info(f'  URL after load: {driver.current_url}')

    return True


# ── Main loop ─────────────────────────────────────────────────────────────────

def collect_todo(rescrape_all):
    """Return list of (json_path, data) for properties that need updating."""
    todo = []
    for prop_dir in sorted(os.listdir(TR_DIR)):
        jf = os.path.join(TR_DIR, prop_dir, f'{prop_dir}.json')
        if not os.path.isfile(jf):
            continue
        try:
            with open(jf, encoding='utf-8') as f:
                data = json.load(f)
        except Exception:
            continue

        if not data.get('url'):
            continue

        has_desc  = bool(data.get('description'))
        has_feats = bool(data.get('key_features'))

        if rescrape_all or not has_desc or not has_feats:
            todo.append((jf, data))

    return todo


# Restart Chrome every this many properties to prevent memory build-up
RESTART_EVERY = 50


def run(args):
    todo = collect_todo(args.all)

    if args.limit:
        todo = todo[:args.limit]

    logger.info(f'Properties to process: {len(todo)}')
    if not todo:
        logger.info('Nothing to do.')
        return

    if args.test:
        todo = todo[:1]
        logger.info('--- TEST MODE: processing first property only ---')

    driver = make_driver()
    updated = skipped = errors = 0

    try:
        for i, (jf, data) in enumerate(todo, 1):
            url = data['url']
            prop_id = data.get('id', os.path.basename(os.path.dirname(jf)))
            logger.info(f'[{i}/{len(todo)}] {prop_id}  {url}')

            # Periodic restart to prevent Chrome memory build-up
            if not args.test and i > 1 and (i - 1) % RESTART_EVERY == 0:
                logger.info(f'  ↻ Restarting Chrome to free memory (every {RESTART_EVERY} properties)…')
                try:
                    driver.quit()
                except Exception:
                    pass
                driver = make_driver()

            ok = load_page(driver, url, test_mode=args.test)

            # If the session died, restart and retry once
            if not ok:
                logger.warning('  Session lost — restarting Chrome and retrying…')
                try:
                    driver.quit()
                except Exception:
                    pass
                try:
                    driver = make_driver()
                    ok = load_page(driver, url, test_mode=args.test)
                except Exception as e:
                    logger.error(f'  Could not restart driver: {e}')

            if not ok:
                errors += 1
                continue

            description = extract_description(driver, test_mode=args.test)
            features    = extract_features(driver, test_mode=args.test)
            rooms       = extract_rooms(driver, test_mode=args.test)

            if args.test:
                logger.info(f'\n=== TEST RESULT for {prop_id} ===')
                logger.info(f'Description ({len(description)} chars):\n  {description[:400]!r}')
                logger.info(f'Key features ({len(features)} items): {features}')
                logger.info(f'Rooms ({len(rooms)} found): {rooms[:2]}')

                # Save full page HTML so we can find the right selectors
                dump_path = os.path.join(SCRIPT_DIR, 'tr_page_dump.html')
                with open(dump_path, 'w', encoding='utf-8') as fh:
                    fh.write(driver.page_source)
                logger.info(f'\nFull page HTML saved to: {dump_path}')

                # Print all CSS classes on the page that look property-related
                elements = driver.find_elements(By.CSS_SELECTOR, '[class]')
                classes = set()
                for el in elements:
                    for c in (el.get_attribute('class') or '').split():
                        classes.add(c)
                keywords = ('prop', 'desc', 'feat', 'detail', 'text', 'info',
                            'overview', 'content', 'summary', 'spec', 'room', 'key')
                relevant = sorted(c for c in classes
                                  if any(k in c.lower() for k in keywords))
                logger.info(f'\nRelevant CSS classes found on page:\n  {relevant}')
                logger.info('=== END TEST ===\n')
                break   # only process one in test mode

            if not description and not features and not rooms:
                logger.warning(f'  No data found — skipping (page may have changed)')
                skipped += 1
                continue

            changed = False
            # Only fill fields not already present (unless --all)
            for field, value in [('description', description),
                                  ('key_features', features),
                                  ('rooms', rooms)]:
                if value and (args.all or not data.get(field)):
                    data[field] = value
                    changed = True

            if changed:
                data['selenium_scraped_at'] = datetime.now().isoformat()
                with open(jf, 'w', encoding='utf-8') as f:
                    json.dump(data, f, indent=2, ensure_ascii=False)
                logger.info(f'  ✅ Saved — desc:{bool(description)} feats:{len(features)} rooms:{len(rooms)}')
                updated += 1
            else:
                skipped += 1

            # Polite delay between requests
            if i < len(todo):
                delay = args.delay + random.uniform(0, 1)
                time.sleep(delay)

    except KeyboardInterrupt:
        logger.info('\nInterrupted by user.')
    finally:
        driver.quit()

    logger.info(
        f'\nDone.  Updated: {updated}  Skipped/empty: {skipped}  Errors: {errors}'
    )


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Selenium scraper for TR descriptions/features')
    parser.add_argument('--test',   action='store_true', help='Dry-run on first property, print results')
    parser.add_argument('--all',    action='store_true', help='Re-scrape even properties that already have data')
    parser.add_argument('--limit',  type=int, default=0, help='Max properties to process (0 = unlimited)')
    parser.add_argument('--delay',  type=float, default=2.0, help='Base seconds between requests (default 2.0)')
    args = parser.parse_args()
    run(args)
