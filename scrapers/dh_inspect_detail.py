#!/usr/bin/env python3
"""Fetches one Daniel Henry property detail page and dumps its structure."""

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import time, os

URL = 'https://www.danielhenry.co.uk/80-meadowvale-park-limavady/1063030'

opts = Options()
opts.add_argument('--headless')
opts.add_argument('--no-sandbox')
opts.add_argument('--disable-dev-shm-usage')
opts.add_argument('--disable-gpu')
opts.add_argument('--window-size=1280,1024')

driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)
try:
    print(f"Fetching: {URL}")
    driver.get(URL)
    time.sleep(4)
    html = driver.page_source
    dump = 'dh_dumps/property_detail.html'
    os.makedirs('dh_dumps', exist_ok=True)
    with open(dump, 'w') as f:
        f.write(html)
    print(f"Saved to {dump} ({len(html):,} bytes)")

    soup = BeautifulSoup(html, 'html.parser')
    print(f"\nTitle: {soup.title and soup.title.text.strip()}")

    # All unique classes
    all_classes = set()
    for el in soup.find_all(True):
        for c in el.get('class', []):
            all_classes.add(c)
    print(f"\n--- All CSS classes ({len(all_classes)}) ---")
    for c in sorted(all_classes):
        print(f"  .{c}")

    # Key text content by selector
    print("\n--- h1/h2 ---")
    for el in soup.select('h1, h2'):
        print(f"  {el.name}: {el.get_text(strip=True)[:100]}")

    print("\n--- Price ---")
    for sel in ['.Price-priceValue', '[class*="price"]', '[class*="Price"]']:
        for el in soup.select(sel)[:2]:
            print(f"  {sel}: {el.get_text(strip=True)[:80]}")

    print("\n--- Description-like content ---")
    for sel in ['.description', '.textblock', '.content', '[class*="desc"]', '[class*="text"]']:
        for el in soup.select(sel)[:2]:
            t = el.get_text(strip=True)[:200]
            if len(t) > 30:
                print(f"  {sel}: {t}")

    print("\n--- Lists (features / rooms / details) ---")
    for ul in soup.find_all('ul'):
        cls = ' '.join(ul.get('class', []))
        items = [li.get_text(strip=True) for li in ul.find_all('li')]
        if items and len(' '.join(items)) > 10:
            print(f"  ul.{cls}: {items[:5]}")
finally:
    driver.quit()
