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
base_url = 'https://billmccann.com/for-sale/'
OUTPUT_DIR = 'properties/bmc'

# Create directories for downloads
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs('logs', exist_ok=True)

# Setup logging
log_filename = f"logs/bmc_scraper_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
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

    # Bill McCann is a WordPress site with property listings
    # Look for property cards/containers with links

    # Try different selectors for property cards
    selectors = [
        'article.property-card',
        'div.property-card',
        '.property-listing-item',
        '.property-item',
        '[data-post-type="property"]',
        '.facetwp-template article',
        '.property',
    ]

    for selector in selectors:
        for elem in soup.select(selector):
            link = elem.find('a', href=True)
            if link:
                href = link['href']
                full_url = urljoin(page_url, href)
                if full_url not in property_links:
                    property_links.append(full_url)

    # If no selectors matched, look for any links containing property-related terms
    if not property_links:
        for link in soup.find_all('a', href=re.compile(r'property|listing|sale', re.IGNORECASE)):
            href = link['href']
            if '/for-sale/' in href or '/property/' in href:
                full_url = urljoin(page_url, href)
                if full_url not in property_links and full_url != page_url:
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
            for suffix in [' | Bill McCann', ' - Bill McCann Estate Agency']:
                t = t.replace(suffix, '')
            property_data['address'] = t.strip()
            property_data['title'] = property_data['address']

    # ── Metadata (Price, Bedrooms, etc.) ──
    # Look for property meta information
    meta_selectors = [
        '.property-price',
        '.price',
        '.property-details',
        '.property-meta',
        '.property-info',
    ]

    for selector in meta_selectors:
        elem = soup.select_one(selector)
        if elem:
            text = elem.get_text(strip=True)
            # Extract price
            price_match = re.search(r'\u00a3[\d,]+(?:\.\d{2})?', text)
            if price_match:
                property_data['price'] = price_match.group(0)
            break

    # Look for bedrooms, bathrooms in the content
    content = soup.get_text()
    bed_match = re.search(r'(\d+)\s*bedroom', content, re.IGNORECASE)
    if bed_match:
        property_data['bedrooms'] = bed_match.group(1)

    bath_match = re.search(r'(\d+)\s*bathroom', content, re.IGNORECASE)
    if bath_match:
        property_data['bathrooms'] = bath_match.group(1)

    # ── Description ─────────────────────────────────────────────────────────────
    # Look for main content area
    content_selectors = [
        '.entry-content',
        '.property-content',
        '.property-description',
        '.description',
        'article .content',
        'main',
    ]

    for selector in content_selectors:
        elem = soup.select_one(selector)
        if elem:
            property_data['description'] = elem.get_text(separator='\n', strip=True)
            break

    # ── Images ─────────────────────────────────────────────────────────────────
    image_urls = []

    # Look for images in the page
    for img in soup.find_all('img'):
        src = img.get('src') or img.get('data-src')
        if src:
            # Skip small images and icons
            if 'wp-content' in src or 'uploads' in src:
                full_url = urljoin(property_url, src)
                if full_url not in image_urls:
                    image_urls.append(full_url)

    # Look for gallery images
    gallery_selectors = [
        '.gallery img',
        '.property-gallery img',
        '.slick-slide img',
        '.swiper-slide img',
        '.property-images img',
    ]

    for selector in gallery_selectors:
        for img in soup.select(selector):
            src = img.get('src') or img.get('data-src') or img.get('data-lazy')
            if src:
                full_url = urljoin(property_url, src)
                if full_url not in image_urls:
                    image_urls.append(full_url)

    property_data['image_urls'] = image_urls
    logger.info(f"Found {len(image_urls)} images")

    # Download images
    image_count = 0
    for i, img_url in enumerate(image_urls, 1):
        if download_image(img_url, property_folder, i):
            image_count += 1

    # Save property data to JSON file
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
    parser = argparse.ArgumentParser(description='Bill McCann property scraper')
    parser.add_argument('--limit', type=int, default=0, help='Max properties to scrape (0 = unlimited)')
    parser.add_argument('--fresh', action='store_true', help='Clear output directory for a fresh scrape')
    args = parser.parse_args()

    logger.info("Starting Bill McCann scraper...")

    # Configuration
    max_pages = 50
    max_properties = args.limit if args.limit > 0 else 100000

    # Clear output directory for a fresh full scrape
    if args.fresh:
        if os.path.exists(OUTPUT_DIR):
            logger.info(f"Clearing {OUTPUT_DIR}/ for a fresh full scrape...")
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
    seen_urls = set()  # Track URLs to detect duplicates/end of pagination

    # WordPress sites often use /page/2/ format
    for page_num in range(1, max_pages + 1):
        if page_num == 1:
            page_url = base_url
        else:
            page_url = f"{base_url}page/{page_num}/"

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
