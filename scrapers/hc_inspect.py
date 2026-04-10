#!/usr/bin/env python3
"""
Hunter Campbell website inspector.
Run this from re_app/ to dump one property detail page so we can build a proper re-scraper.

Usage:
    python3 scrapers/hc_inspect.py
"""

import requests
from bs4 import BeautifulSoup
import os, re, json

os.makedirs('hc_dumps', exist_ok=True)

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 '
                  '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xhtml+xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-GB,en;q=0.9',
}

# Load an existing HC property URL from saved JSON
hc_dir = 'properties/hc'
sample_url = None
for d in sorted(os.listdir(hc_dir)):
    if not d.startswith('property_'): continue
    j = os.path.join(hc_dir, d, f'{d}.json')
    if os.path.exists(j):
        data = json.load(open(j))
        if data.get('url'):
            sample_url = data['url']
            print(f"Using: {sample_url}")
            break

if not sample_url:
    sample_url = 'https://www.huntercampbell.co.uk/residential-sales'
    print(f"No saved URL found, using listing page: {sample_url}")

r = requests.get(sample_url, headers=HEADERS, timeout=20)
print(f"Status: {r.status_code}  Length: {len(r.text):,}")

with open('hc_dumps/property_detail.html', 'w') as f:
    f.write(r.text)
print("Saved to hc_dumps/property_detail.html")

soup = BeautifulSoup(r.text, 'html.parser')
print(f"Title: {soup.title and soup.title.text.strip()}")

# All unique classes
all_classes = set()
for el in soup.find_all(True):
    for c in el.get('class', []):
        all_classes.add(c)
print(f"\n--- All CSS classes ({len(all_classes)}) ---")
for c in sorted(all_classes):
    print(f"  .{c}")

print("\n--- h1/h2/h3 ---")
for el in soup.select('h1, h2, h3'):
    print(f"  {el.name}.{'|'.join(el.get('class',['']))}: {el.get_text(strip=True)[:100]}")

print("\n--- All <ul> lists ---")
for ul in soup.find_all('ul'):
    cls = ' '.join(ul.get('class', []))
    items = [li.get_text(strip=True) for li in ul.find_all('li') if li.get_text(strip=True)]
    if 2 <= len(items) <= 20:
        print(f"  ul.{cls or '(no class)'}:")
        for it in items[:6]:
            print(f"    - {it[:100]}")

print("\n--- Paragraphs > 50 chars ---")
seen = set()
for p in soup.find_all('p'):
    t = p.get_text(strip=True)
    if len(t) > 50 and t not in seen:
        seen.add(t)
        cls = ' '.join(p.get('class', []))
        print(f"  p.{cls}: {t[:150]}")

print("\n--- All <dl> / <table> content ---")
for dl in soup.find_all('dl'):
    cls = ' '.join(dl.get('class', []))
    print(f"  dl.{cls}:")
    for dt in dl.find_all('dt'):
        dd = dt.find_next_sibling('dd')
        print(f"    {dt.get_text(strip=True)}: {dd.get_text(strip=True) if dd else ''}")

for table in soup.find_all('table'):
    cls = ' '.join(table.get('class', []))
    print(f"  table.{cls}:")
    for row in table.find_all('tr')[:6]:
        cells = [td.get_text(strip=True) for td in row.find_all(['td','th'])]
        if cells: print(f"    {cells}")
