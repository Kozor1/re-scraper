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
base_url = 'https://ipestates.co.uk/property-for-sale'
OUTPUT_DIR = 'properties/ipe'

# Create directories for downloads
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs('logs', exist_ok=True)

# Setup logging
log_filename = f"logs/ipe_scraper_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename, encoding='utf-8'),
        logging.StreamHandler()  # Also print to console
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
                # Exponential backoff: 2^attempt seconds
                sleep_time = (2 ** attempt) * random.uniform(2, 5)
                logger.info(f"Retrying in {sleep_time:.2f} seconds...")
                time.sleep(sleep_time)
            else:
                logger.error(f"Failed to fetch {url} after {max_retries} attempts")
                return None

def extract_property_links(soup, page_url):
    """Extract individual property listing links from a listing page"""
    property_links = []

    # Look for property listing links
    # IPE typically has property pages with URLs containing the property slug
    for link in soup.find_all('a', href=True):
        href = link['href']
        # Look for links that go to individual property pages
        # Properties are usually under /property/ or have a pattern like /property-name-id/
        if href.startswith('/property/') or ('/property-' in href and '/property-for-sale' not in href):
            full_url = urljoin(page_url, href)
            # Avoid duplicates
            if full_url not in property_links:
                property_links.append(full_url)

    return property_links

def download_image(img_url, property_folder, img_num):
    """Download an image and save it to disk"""
    try:
        response = get_with_retry(img_url)
        if response and response.status_code == 200:
            # Get file extension from URL
            parsed_url = urlparse(img_url)
            ext = os.path.splitext(parsed_url.path)[1] or '.jpg'

            # Create filename
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

    # Create property folder
    property_folder = f"{OUTPUT_DIR}/{property_id}"
    os.makedirs(property_folder, exist_ok=True)

    # Extract property data
    property_data = {
        'url': property_url,
        'id': property_id,
        'scraped_at': datetime.now().isoformat(),
    }

    # ── Address ────────────────────────────────────────────────────────────────
    h1 = soup.find('h1')
    if h1:
        property_data['title'] = h1.get_text(strip=True)
        property_data['address'] = h1.get_text(strip=True)
    else:
        title_tag = soup.find('title')
        if title_tag:
            t = title_tag.get_text(strip=True)
            for suffix in [' for sale with IPE', ' | IPE', ' - IPE', ' | Independent Property Estates']:
                t = t.replace(suffix, '')
            property_data['address'] = t.strip()
            property_data['title'] = property_data['address']

    # ── Price ───────────────────────────────────────────────────────────────────
    price_selectors = ['.property-price', '.price', '[class*="price"]', '.listing-price', 'h2']
    for selector in price_selectors:
        price_elem = soup.select_one(selector)
        if price_elem:
            price_text = price_elem.get_text(strip=True)
            if '£' in price_text or 'POA' in price_text or 'price' in price_text.lower():
                property_data['price'] = price_text
                break

    # ── Bedrooms ─────────────────────────────────────────────────────────────────
    bedroom_selectors = ['.bedrooms', '[class*="bedroom"]', '.property-meta', '.property-details']
    for selector in bedroom_selectors:
        bedroom_elem = soup.select_one(selector)
        if bedroom_elem:
            text = bedroom_elem.get_text(strip=True).lower()
            match = re.search(r'(\d+)\s*bedroom', text)
            if match:
                property_data['bedrooms'] = match.group(1)
                break

    # ── Property Type ────────────────────────────────────────────────────────────
    type_selectors = ['.property-type', '[class*="type"]', '.property-style']
    for selector in type_selectors:
        type_elem = soup.select_one(selector)
        if type_elem:
            property_data['type'] = type_elem.get_text(strip=True)
            break

    # ── Status ───────────────────────────────────────────────────────────────────
    status_selectors = ['.status', '[class*="status"]', '.sold-stamp', '.available']
    for selector in status_selectors:
        status_elem = soup.select_one(selector)
        if status_elem:
            status_text = status_elem.get_text(strip=True).lower()
            if 'agreed' in status_text or 'sold' in status_text:
                property_data['status'] = 'Sale Agreed'
            elif 'for sale' in status_text:
                property_data['status'] = 'For Sale'
            break

    # ── Description ─────────────────────────────────────────────────────────────
    desc_selectors = ['.property-description', '.description', '[class*="description"]', '.entry-content', '#description']
    for selector in desc_selectors:
        desc_elem = soup.select_one(selector)
        if desc_elem:
            property_data['description'] = desc_elem.get_text(separator='\n', strip=True)
            break

    # ── Key Features ─────────────────────────────────────────────────────────────
    feature_selectors = ['.key-features', '.features', '[class*="feature"] ul', '.property-features', '.bullets']
    for selector in feature_selectors:
        features_elem = soup.select(selector)
        if features_elem:
            key_features = []
            for elem in features_elem:
                for li in elem.find_all('li'):
                    text = li.get_text(strip=True)
                    if text:
                        key_features.append(text)
            if key_features:
                property_data['key_features'] = key_features
                break

    # ── Images ───────────────────────────────────────────────────────────────────
    image_urls = []
    img_selectors = ['.property-gallery img', '.gallery img', '[class*="gallery"] img', '.slick-slide img', '.wp-block-image img']
    for selector in img_selectors:
        images = soup.select(selector)
        for img in images:
            src = img.get('src') or img.get('data-src') or img.get('data-lazy')
            if src:
                full_url = urljoin(property_url, src)
                if full_url not in image_urls:
                    image_urls.append(full_url)
        if image_urls:
            break

    # Also check for image links in lightbox/fancybox
    if not image_urls:
        for link in soup.find_all('a', href=True):
            href = link.get('href', '')
            if any(ext in href.lower() for ext in ['.jpg', '.jpeg', '.png', '.webp']):
                full_url = urljoin(property_url, href)
                if full_url not in image_urls:
                    image_urls.append(full_url)

    property_data['image_urls'] = image_urls
    logger.info(f"Found {len(image_urls)} images")

    image_count = 0
    for i, img_url in enumerate(image_urls, 1):
        if download_image(img_url, property_folder, i):
            image_count += 1

    # Save property data to JSON file in property folder
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
    parser = argparse.ArgumentParser(description='Independent Property Estates scraper')
    parser.add_argument('--limit', type=int, default=0, help='Max properties to scrape (0 = unlimited)')
    parser.add_argument('--fresh', action='store_true', help='Clear output directory for fresh scrape')
    args = parser.parse_args()

    logger.info("Starting Independent Property Estates scraper...")

    # Configuration
    max_pages = 100  # Adjust based on how many pages you want to scrape
    max_properties = args.limit if args.limit > 0 else 100000

    # Clear output directory for a fresh scrape
    if args.fresh:
        if os.path.exists(OUTPUT_DIR):
            logger.info(f"Clearing {OUTPUT_DIR}/ for a fresh scrape...")
            shutil.rmtree(OUTPUT_DIR)
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        os.makedirs('logs', exist_ok=True)

    # Load existing property index
    property_index = load_property_index()
    existing_properties = property_index.get('properties', [])
    next_property_id = len(existing_properties) + 1

    logger.info(f"Existing properties in index: {len(existing_properties)}")

    # Collect all property links from multiple pages
    all_property_links = []

    for page_num in range(1, max_pages + 1):
        # IPE may use query parameters for pagination
        if page_num == 1:
            page_url = base_url
        else:
            page_url = f"{base_url}/page/{page_num}/"

        logger.info(f"Fetching listing page {page_num}: {page_url}")

        response = get_with_retry(page_url)
        if not response:
            logger.warning(f"Failed to fetch page {page_num}, stopping pagination")
            break

        soup = BeautifulSoup(response.content, 'html.parser')

        # Extract property links from this page
        property_links = extract_property_links(soup, page_url)
        logger.info(f"Found {len(property_links)} property links on page {page_num}")

        if not property_links:
            logger.info(f"No more properties found on page {page_num}, stopping pagination")
            break

        all_property_links.extend(property_links)

        # Stop if we've reached the maximum number of properties
        if len(all_property_links) >= max_properties:
            all_property_links = all_property_links[:max_properties]
            logger.info(f"Reached maximum of {max_properties} properties")
            break

        # Small delay between page requests
        time.sleep(random.uniform(1, 2))

    logger.info(f"Total property links found: {len(all_property_links)}")

    if not all_property_links:
        logger.warning("No property links found. The website structure may have changed.")
        return

    logger.info(f"Scraping {len(all_property_links)} property(ies)")

    # Scrape each property
    all_properties = []
    for idx, property_url in enumerate(all_property_links, 1):
        logger.info(f"{'='*60}")
        logger.info(f"Processing property {idx}/{len(all_property_links)}")

        # Create a unique ID for this property
        property_id = f"property_{next_property_id + idx - 1}"

        # Scrape the property page
        try:
            property_data = scrape_property_page(property_url, property_id)
        except Exception as exc:
            logger.error(f"Unhandled exception scraping {property_url}: {exc}", exc_info=True)
            property_data = None
        if property_data:
            all_properties.append(property_data)

        # Rate limiting: wait between requests
        if idx < len(all_property_links):
            # Random delay between 1-3 seconds to be respectful to the server
            delay = random.uniform(1, 3)
            logger.info(f"Waiting {delay:.2f} seconds before next request...")
            time.sleep(delay)

        # Progress update every 10 properties
        if idx % 10 == 0:
            logger.info(f"Progress: {idx}/{len(all_property_links)} properties processed ({idx/len(all_property_links)*100:.1f}%)")

    # Save summary
    summary = {
        'total_properties_found': len(all_property_links),
        'properties_scraped': len(all_properties),
        'max_pages': max_pages,
        'max_properties': max_properties,
        'scraped_at': datetime.now().isoformat(),
        'log_file': log_filename,
    }

    with open(f'{OUTPUT_DIR}/summary.json', 'w', encoding='utf-8') as f:
        json.dump(summary, f, indent=2)

    # Update property index
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
