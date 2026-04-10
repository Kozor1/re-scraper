#!/usr/bin/env python3
"""
Daniel Henry website inspector.
Run this on your Mac to dump the HTML structure so we can build the scraper.

Usage:
    python3 scrapers/dh_inspect.py
"""

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import time, os, re, json

LIST_URL = 'https://www.danielhenry.co.uk/search?sta=forSale&sta=saleAgreed&sta=sold&st=sale&pt=residential'
DUMP_DIR = 'dh_dumps'
os.makedirs(DUMP_DIR, exist_ok=True)

def make_driver():
    opts = Options()
    opts.add_argument('--headless')
    opts.add_argument('--no-sandbox')
    opts.add_argument('--disable-dev-shm-usage')
    opts.add_argument('--disable-gpu')
    opts.add_argument('--window-size=1280,1024')
    opts.add_argument('--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
    return webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)

def analyse_page(html, label):
    soup = BeautifulSoup(html, 'html.parser')

    print(f"\n{'='*60}")
    print(f"PAGE: {label}")
    print(f"{'='*60}")
    print(f"Title: {soup.title and soup.title.text.strip()}")
    print(f"HTML length: {len(html):,}")

    # Property count
    print("\n--- Count / results text ---")
    for tag in soup.find_all(string=lambda t: t and re.search(r'\d+\s*(propert|result|listing)', t, re.I) and len(t.strip()) < 120):
        print(f"  {tag.strip()!r}")

    # All unique classes
    all_classes = set()
    for el in soup.find_all(True):
        for c in el.get('class', []):
            all_classes.add(c)

    # Property-looking classes
    print("\n--- Property-related CSS classes ---")
    prop_words = ['property', 'listing', 'result', 'card', 'item', 'prop', 'search', 'address',
                  'price', 'bed', 'bath', 'status', 'agent', 'image', 'photo', 'thumb']
    for c in sorted(all_classes):
        if any(w in c.lower() for w in prop_words):
            print(f"  .{c}")

    # Links that look like individual property pages
    print("\n--- Property page links (sample) ---")
    links = set()
    for a in soup.find_all('a', href=True):
        href = a['href']
        if re.search(r'/(property|properties|listing|sale|residential)/', href, re.I):
            links.add(href)
    for l in sorted(links)[:20]:
        print(f"  {l}")

    # Pagination
    print("\n--- Pagination ---")
    for el in soup.find_all(True):
        cls = ' '.join(el.get('class', []))
        if 'pag' in cls.lower() or el.get('aria-label', '').lower() in ('pagination', 'next page', 'previous page'):
            print(f"  {el.name}.{cls}: {el.get_text(strip=True)[:80]}")

    # Try common property card selectors
    print("\n--- Common property card selectors ---")
    selectors = [
        'li.property', 'div.property', 'article.property',
        'li.listing', 'div.listing', 'article.listing',
        'li.result', 'div.result',
        '[class*="property"]', '[class*="PropertyCard"]',
        'ul.properties > li', 'ul.results > li',
        '.properties-list li', '.search-results li',
    ]
    for sel in selectors:
        try:
            matches = soup.select(sel)
            if matches:
                print(f"  {sel!r}: {len(matches)} matches")
        except Exception:
            pass

    # JSON-LD or window.__data type scripts
    print("\n--- Scripts with embedded data ---")
    for script in soup.find_all('script'):
        src = script.get('src', '')
        text = script.string or ''
        if script.get('type') == 'application/ld+json':
            print(f"  JSON-LD: {text[:200]}")
        elif 'window.' in text and len(text) > 100:
            print(f"  window.* script ({len(text)} chars): {text[:150]}")

print("Starting browser...")
driver = make_driver()

try:
    print(f"Fetching: {LIST_URL}")
    driver.get(LIST_URL)

    # Wait up to 10s for something to appear
    time.sleep(5)

    # Save listing page
    html = driver.page_source
    dump_path = os.path.join(DUMP_DIR, 'listings_page1.html')
    with open(dump_path, 'w') as f:
        f.write(html)
    print(f"Saved: {dump_path}")

    analyse_page(html, 'Listings page 1')

    # ---- Now try to find and open the first property link ----
    soup = BeautifulSoup(html, 'html.parser')
    prop_links = []
    for a in soup.find_all('a', href=True):
        href = a['href']
        if re.search(r'/(property|properties|listing|sale|residential)/', href, re.I):
            full = href if href.startswith('http') else 'https://www.danielhenry.co.uk' + href
            prop_links.append(full)
    prop_links = list(dict.fromkeys(prop_links))  # dedupe, preserve order

    if prop_links:
        first_prop = prop_links[0]
        print(f"\n\nFetching first property page: {first_prop}")
        driver.get(first_prop)
        time.sleep(4)
        prop_html = driver.page_source
        prop_dump = os.path.join(DUMP_DIR, 'property_detail.html')
        with open(prop_dump, 'w') as f:
            f.write(prop_html)
        print(f"Saved: {prop_dump}")

        prop_soup = BeautifulSoup(prop_html, 'html.parser')
        print(f"\n{'='*60}")
        print("PROPERTY DETAIL PAGE")
        print(f"{'='*60}")
        print(f"Title: {prop_soup.title and prop_soup.title.text.strip()}")
        print(f"URL: {first_prop}")

        # All unique classes on detail page
        detail_classes = set()
        for el in prop_soup.find_all(True):
            for c in el.get('class', []):
                detail_classes.add(c)

        print("\n--- All CSS classes on detail page ---")
        for c in sorted(detail_classes):
            print(f"  .{c}")

        # Look for price, address, description, features
        print("\n--- Text in likely data fields ---")
        for sel in ['h1', 'h2', '.price', '.address', '.description', '.features',
                    '[class*="price"]', '[class*="address"]', '[class*="desc"]',
                    '[class*="feature"]', '[class*="bed"]', '[class*="status"]',
                    'ul li']:
            try:
                els = prop_soup.select(sel)[:3]
                for el in els:
                    t = el.get_text(strip=True)
                    if t:
                        print(f"  {sel}: {t[:100]}")
            except Exception:
                pass
    else:
        print("\nNo property links found on listing page — site may be heavily JS-rendered")
        print("All links on the page:")
        for a in soup.find_all('a', href=True)[:30]:
            print(f"  {a['href']}")

finally:
    driver.quit()

print("\n\nDone! HTML dumps saved to:", os.path.abspath(DUMP_DIR))
print("Share the output above (and optionally the HTML files) to proceed with the scraper.")
