#!/usr/bin/env python3
"""
Michael Chandler Full Property Scraper
Scrapes all properties from Michael Chandler website.
"""

import re
import argparse
import requests
from bs4 import BeautifulSoup
import time
import random
import os
import shutil
import sys
from urllib.parse import urljoin, urlparse
import json
import logging
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from image_sort_utils import sort_and_dedup as _sort_and_dedup_image_urls

# Headers to mimic a real browser
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'none',
    'Cache-Control': 'max-age=0',
}

# Base URL for the property listings
base_url = 'https://www.michael-chandler.co.uk/property-for-sale'
OUTPUT_DIR = 'properties/mc'

# Create directories for downloads
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs('logs', exist_ok=True)

# Setup logging
log_filename = f"logs/mc_scraper_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def get_with_retry(url, max_retries=3):
    """Make a GET request with retry logic and rate limiting"""
    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            error_msg = f"Attempt {attempt + 1} failed: {e}"
            logger.warning(f"{url} - {error_msg}")

            if attempt < max_retries - 1:
                sleep_time = (2 ** attempt) * random.uniform(2, 5)
                logger.info(f"Retrying in {sleep_time:.2f} seconds...")
                time.sleep(sleep_time)
            else:
                logger.error(f"Failed to fetch {url} after {max_retries} attempts")
                return None

def extract_property_links(soup, page_url):
    """Extract individual property listing links from a listing page"""
    property_links = []

    for link in soup.find_all('a', href=True):
        href = link['href']
        if '/property/' in href:
            full_url = urljoin(page_url, href)
            if full_url not in property_links:
                property_links.append(full_url)

    return property_links

def download_image(img_url, property_folder, img_num):
    """Download an image and save it to disk"""
    try:
        response = get_with_retry(img_url)
        if response and response.status_code == 200:
            parsed_url = urlparse(img_url)
            ext = os.path.splitext(parsed_url.path)[1] or '.jpg'
            filename = f"{property_folder}/img{img_num}{ext}"

            with open(filename, 'wb') as f:
                f.write(response.content)

            logger.info(f"Downloaded: {filename}")
            return filename
    except Exception as e:
        error_msg = f"Error downloading image {img_url}: {e}"
        logger.error(error_msg)
    return None

def scrape_property_page(property_url, property_id):
    """Scrape an individual property page"""
    logger.info(f"Scraping property: {property_url}")

    response = get_with_retry(property_url)
    if not response:
        return None

    soup = BeautifulSoup(response.content, 'html.parser')

    property_folder = f"{OUTPUT_DIR}/{property_id}"
    os.makedirs(property_folder, exist_ok=True)

    property_data = {
        'url': property_url,
        'id': property_id,
        'scraped_at': datetime.now().isoformat(),
    }

    # Extract address/title
    h1 = soup.find('h1')
    if h1:
        property_data['title'] = h1.get_text(strip=True)
        property_data['address'] = h1.get_text(strip=True)
    else:
        title_tag = soup.find('title')
        if title_tag:
            t = title_tag.get_text(strip=True)
            for suffix in [' for sale with Michael Chandler', ' | Michael Chandler', ' - Michael Chandler']:
                t = t.replace(suffix, '')
            property_data['address'] = t.strip()
            property_data['title'] = property_data['address']

    # Extract metadata from property details
    for li in soup.select('ul.dettbl li'):
        key_el = li.find(class_='dt1')
        val_el = li.find(class_='dt2')
        if not key_el or not val_el:
            continue
        key = key_el.get_text(strip=True).lower()
        val = val_el.get_text(strip=True)
        if 'price' in key:
            property_data['price'] = val
        elif 'style' in key or 'type' in key:
            property_data['type'] = val
        elif 'bedroom' in key:
            property_data['bedrooms'] = val
        elif 'reception' in key:
            property_data['receptions'] = val
        elif 'status' in key:
            property_data['status'] = val

    # Fallback status
    if not property_data.get('status'):
        dtsm = soup.select_one('div.dtsm')
        if dtsm:
            t = dtsm.get_text(separator=' ', strip=True).lower()
            if 'agreed' in t:
                property_data['status'] = 'Sale Agreed'
            elif 'for sale' in t or 'available' in t:
                property_data['status'] = 'For Sale'

    # Key features
    key_features = [li.get_text(strip=True) for li in soup.select('ul.feats li') if li.get_text(strip=True)]
    if key_features:
        property_data['key_features'] = key_features

    # Description
    description_parts = []
    for container in soup.select('div.textbp'):
        import copy as _copy
        c = _copy.copy(container)
        for tag in c.find_all(['p', 'br']):
            if tag.name == 'br':
                tag.replace_with('\n')
            else:
                tag.insert_before('\n\n')
        text = c.get_text(separator='', strip=False).strip()
        if text:
            description_parts.append(text)
    if description_parts:
        raw = '\n\n'.join(description_parts)
        raw = re.sub(r'\n{3,}', '\n\n', raw).strip()
        property_data['description'] = raw

    # Images
    _img_href_re = re.compile(r'\.(jpg|jpeg|png|webp|gif)(\?.*)?$', re.IGNORECASE)
    image_count = 0
    image_urls = []
    gallery = soup.find('ul', id='gallery')
    if gallery:
        real_links = [
            a for a in gallery.find_all('a', href=True)
            if 'slick-cloned' not in (a.parent.get('class') or [])
            and _img_href_re.search(a['href'])
        ]
        seen = set()
        for a in real_links:
            full_url = urljoin(property_url, a['href'])
            if full_url not in seen:
                seen.add(full_url)
                image_urls.append(full_url)
        logger.info(f"Found {len(image_urls)} images in ul#gallery")
    else:
        logger.warning("ul#gallery not found — trying fallback")
        for img in soup.find_all('img'):
            src = img.get('src') or img.get('data-src')
            if src and '/images/property/' in src:
                full_url = urljoin(property_url, src)
                if full_url not in image_urls:
                    image_urls.append(full_url)

    property_data['image_urls'] = image_urls

    for i, img_url in enumerate(image_urls, 1):
        if download_image(img_url, property_folder, i):
            image_count += 1

    # Save property data
    data_filename = f"{property_folder}/{property_id}.json"
    with open(data_filename, 'w', encoding='utf-8') as f:
        json.dump(property_data, f, indent=2, ensure_ascii=False)

    logger.info(f"Saved data to: {data_filename}")
    logger.info(f"Downloaded {image_count} images")

    return property_data

def load_property_index():
    """Load the property index file if it exists"""
    index_path = f'{OUTPUT_DIR}/property_index.json'
    if os.path.exists(index_path):
        try:
            with open(index_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Error loading property index: {e}")
    return {'properties': [], 'last_updated': None}

def save_property_index(index):
    """Save the property index file"""
    index_path = f'{OUTPUT_DIR}/property_index.json'
    with open(index_path, 'w', encoding='utf-8') as f:
        json.dump(index, f, indent=2, ensure_ascii=False)
    logger.info(f"Property index saved to: {index_path}")

def main():
    parser = argparse.ArgumentParser(description='Michael Chandler property scraper')
    parser.add_argument('--limit', type=int, default=0, help='Max properties to scrape (0 = unlimited)')
    parser.add_argument('--fresh', action='store_true', help='Clear existing data before scraping')
    args = parser.parse_args()

    logger.info("Starting Michael Chandler scraper...")

    max_pages = 1000
    max_properties = args.limit if args.limit > 0 else 100000
    test_mode = False

    if args.fresh or not test_mode:
        if os.path.exists(OUTPUT_DIR):
            logger.info(f"Clearing {OUTPUT_DIR}/ for a fresh full scrape...")
            shutil.rmtree(OUTPUT_DIR)
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        os.makedirs('logs', exist_ok=True)

    pages_to_fetch = 1 if test_mode else max_pages

    property_index = load_property_index()
    existing_properties = property_index.get('properties', [])
    next_property_id = len(existing_properties) + 1

    logger.info(f"Existing properties in index: {len(existing_properties)}")

    all_property_links = []
    seen_urls = set()  # Track URLs to detect duplicates/end of pagination

    for page_num in range(1, pages_to_fetch + 1):
        if page_num == 1:
            page_url = base_url
        else:
            page_url = f"{base_url}?page={page_num}"

        logger.info(f"Fetching listing page {page_num}: {page_url}")

        response = get_with_retry(page_url)
        if not response:
            logger.warning(f"Failed to fetch page {page_num}, stopping pagination")
            break

        soup = BeautifulSoup(response.content, 'html.parser')

        # Extract property links from this page
        property_links = extract_property_links(soup, page_url)

        # Filter out URLs we've already seen (prevents infinite loops when sites repeat content)
        new_links = [url for url in property_links if url not in seen_urls]

        logger.info(f"Found {len(property_links)} property links on page {page_num} ({len(new_links)} new)")

        if not new_links:
            logger.info(f"No new properties on page {page_num}, stopping pagination")
            break

        all_property_links.extend(new_links)
        seen_urls.update(new_links)

        if len(all_property_links) >= max_properties:
            all_property_links = all_property_links[:max_properties]
            logger.info(f"Reached maximum of {max_properties} properties")
            break

        time.sleep(random.uniform(1, 2))

    logger.info(f"Total property links found: {len(all_property_links)}")

    if not all_property_links:
        logger.warning("No property links found. The website structure may have changed.")
        return

    properties_to_scrape = all_property_links[:1] if test_mode else all_property_links

    logger.info(f"Testing mode: {test_mode}")
    logger.info(f"Scraping {len(properties_to_scrape)} property(ies)")

    all_properties = []
    for idx, property_url in enumerate(properties_to_scrape, 1):
        logger.info(f"{'='*60}")
        logger.info(f"Processing property {idx}/{len(properties_to_scrape)}")

        property_id = f"property_{next_property_id + idx - 1}"

        try:
            property_data = scrape_property_page(property_url, property_id)
        except Exception as exc:
            logger.error(f"Unhandled exception scraping {property_url}: {exc}", exc_info=True)
            property_data = None
        if property_data:
            all_properties.append(property_data)

        if idx < len(properties_to_scrape):
            delay = random.uniform(1, 3)
            logger.info(f"Waiting {delay:.2f} seconds before next request...")
            time.sleep(delay)

        if idx % 10 == 0:
            logger.info(f"Progress: {idx}/{len(properties_to_scrape)} properties processed ({idx/len(properties_to_scrape)*100:.1f}%)")

    summary = {
        'total_properties_found': len(all_property_links),
        'properties_scraped': len(all_properties),
        'test_mode': test_mode,
        'max_pages': max_pages,
        'max_properties': max_properties,
        'scraped_at': datetime.now().isoformat(),
        'log_file': log_filename,
    }

    with open(f'{OUTPUT_DIR}/summary.json', 'w', encoding='utf-8') as f:
        json.dump(summary, f, indent=2)

    for property_data in all_properties:
        property_entry = {
            'id': property_data['id'],
            'url': property_data['url'],
            'address': property_data.get('address', ''),
            'title': property_data.get('title', ''),
            'scraped_at': property_data['scraped_at']
        }
        existing_properties.append(property_entry)

    property_index = {
        'properties': existing_properties,
        'last_updated': datetime.now().isoformat()
    }
    save_property_index(property_index)

    logger.info(f"{'='*60}")
    logger.info("Scraping complete!")
    logger.info(f"Total properties found: {len(all_property_links)}")
    logger.info(f"Properties scraped: {len(all_properties)}")
    logger.info(f"Properties saved to: {OUTPUT_DIR}/")
    logger.info(f"Log file: {log_filename}")

if __name__ == "__main__":
    main()
