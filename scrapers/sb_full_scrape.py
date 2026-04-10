#!/usr/bin/env python3
"""
Simon Brien Full Property Scraper
Scrapes all properties from Simon Brien website and saves them to sb_properties folder.
"""

import re
import argparse
import requests
from bs4 import BeautifulSoup
import json
import os
import shutil
import sys
import time
import random
from datetime import datetime
from urllib.parse import urljoin

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from image_sort_utils import sort_and_dedup as _sort_and_dedup_image_urls

# Configuration
BASE_URL = "https://www.simonbrien.com/property-for-sale"
BASE_DOMAIN = "https://www.simonbrien.com"
OUTPUT_DIR = "properties/sb"
MAX_PAGES = 1000  # Maximum number of listing pages to scrape
MAX_PROPERTIES = 100000  # Maximum number of properties to scrape
TEST_MODE = False  # Set to False to scrape all properties
REQUEST_DELAY_MIN = 1  # Minimum delay between requests (seconds)
REQUEST_DELAY_MAX = 3  # Maximum delay between requests (seconds)
MAX_RETRIES = 3  # Maximum number of retries for failed requests
RETRY_DELAY = 5  # Initial delay between retries (seconds)

# Headers to make requests look like they're from a real browser
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'none',
    'Cache-Control': 'max-age=0'
}

# Set up logging
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
log_filename = f"sb_scraper_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
log_path = os.path.join(log_dir, log_filename)

import logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_path),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def get_with_retry(url, max_retries=MAX_RETRIES):
    """Make HTTP request with retry logic and exponential backoff"""
    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=HEADERS, timeout=30)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            if attempt < max_retries - 1:
                wait_time = RETRY_DELAY * (2 ** attempt)
                logger.warning(f"Request failed (attempt {attempt + 1}/{max_retries}): {e}")
                logger.info(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                logger.error(f"Request failed after {max_retries} attempts: {e}")
                return None


def download_image(img_url, property_folder, img_num):
    """Download an image and save it to the property folder"""
    try:
        response = get_with_retry(img_url)
        if not response:
            return None
        
        # Determine file extension from URL
        if '.webp' in img_url:
            ext = '.webp'
        elif '.jpg' in img_url or '.jpeg' in img_url:
            ext = '.jpg'
        elif '.png' in img_url:
            ext = '.png'
        else:
            ext = '.jpg'  # Default
        
        filename = f"img{img_num}{ext}"
        filepath = os.path.join(property_folder, filename)
        
        with open(filepath, 'wb') as f:
            f.write(response.content)
        
        logger.info(f"Downloaded image {img_num}: {filename}")
        return filepath
    except Exception as e:
        logger.error(f"Error downloading image {img_url}: {e}")
        return None


_IMAGE_HREF_RE = re.compile(r'\.(jpg|jpeg|png|webp|gif)(\?.*)?$', re.IGNORECASE)


def scrape_property_images(property_url, property_folder):
    """Scrape all images from a property detail page.

    Returns (image_count, image_urls) where image_urls preserves the DOM
    display order after filtering Slick clones and non-image links (e.g.
    YouTube videos).

    Why DOM order instead of numeric sort:
      Numeric sort was originally used to counteract misplaced clone slides.
      Now that clones are filtered by their parent <li class="slick-cloned">
      attribute, the remaining slides are already in the intended display
      order.  SB sometimes assigns a high numeric suffix to the hero/exterior
      shot (e.g. _11) because their CMS reserves lower slots for a video
      embed.  Sorting by suffix would push the hero shot to position 11;
      DOM order keeps it first (immediately after the filtered-out video).
    """
    response = get_with_retry(property_url)
    if not response:
        return 0, []

    soup = BeautifulSoup(response.content, 'html.parser')

    # Target the gallery element specifically
    gallery = soup.find('ul', id='gallery')
    if not gallery:
        logger.warning(f"No gallery found for {property_url}")
        return 0, []

    # Collect real image slides only:
    #   1. Skip Slick-cloned elements (class is on the parent <li>).
    #   2. Skip non-image hrefs (YouTube embeds, virtual-tour links, etc.).
    all_links = gallery.find_all('a')
    gallery_links = [
        a for a in all_links
        if 'slick-cloned' not in (a.parent.get('class') or [])
        and _IMAGE_HREF_RE.search(a.get('href', ''))
    ]
    logger.info(
        f"Found {len(all_links)} gallery links "
        f"({len(all_links) - len(gallery_links)} excluded: clones / non-image)"
    )

    # Deduplicate while preserving DOM order
    seen: set[str] = set()
    image_urls: list[str] = []
    for link in gallery_links:
        full_url = urljoin(BASE_DOMAIN, link['href'])
        if full_url not in seen:
            seen.add(full_url)
            image_urls.append(full_url)

    # Download images in display order
    image_count = 0
    for i, full_img_url in enumerate(image_urls, 1):
        downloaded_path = download_image(full_img_url, property_folder, i)
        if downloaded_path:
            image_count += 1

    return image_count, image_urls


def scrape_property_details(property_url):
    """Scrape property details from a property page"""
    response = get_with_retry(property_url)
    if not response:
        return None
    
    soup = BeautifulSoup(response.content, 'html.parser')
    
    # Extract property details
    property_data = {
        'url': property_url,
        'scraped_at': datetime.now().isoformat()
    }
    
    # Extract address (Simon Brien specific - detail page)
    address_one = soup.find('h1', class_='prop-det-address-one')
    address_two = soup.find('h2', class_='prop-det-address-two')
    
    if address_one and address_two:
        property_data['address'] = f"{address_one.get_text(strip=True)} {address_two.get_text(strip=True)}"
    elif address_one:
        property_data['address'] = address_one.get_text(strip=True)
    else:
        # Fallback: try to get title
        title = soup.find('h1') or soup.find('h2')
        property_data['title'] = title.get_text(strip=True) if title else 'Unknown'
    
    # Extract price
    price = soup.find('span', class_='prop-det-price-amount')
    if price:
        property_data['price'] = price.get_text(strip=True)
    
    # Extract property details from prop-det-info rows
    info_rows = soup.find_all('div', class_='prop-det-info-row')
    for row in info_rows:
        left = row.find('span', class_='prop-det-info-left')
        right = row.find('span', class_='prop-det-info-right')
        if left and right:
            label = left.get_text(strip=True).lower()
            value = right.get_text(strip=True)
            if label == 'style':
                property_data['type'] = value
            elif label == 'bedrooms':
                property_data['bedrooms'] = value
            elif label == 'status':
                property_data['status'] = value
            elif label == 'epc rating':
                property_data['epc_rating'] = value
    
    # Extract Key Features
    key_features_title = soup.find('h2', class_='prop-det-title', string='Key Features')
    if key_features_title:
        key_features_div = key_features_title.find_next_sibling('div', class_='prop-det-feats')
        if key_features_div:
            features = []
            feat_divs = key_features_div.find_all('div', class_='feat')
            for feat_div in feat_divs:
                # Remove the icon and get the text
                icon = feat_div.find('i', class_='fa')
                if icon:
                    icon.decompose()
                feature_text = feat_div.get_text(strip=True)
                if feature_text:
                    features.append(feature_text)
            property_data['key_features'] = features
    
    # Extract Description
    description_title = soup.find('h2', class_='prop-det-title', string='Description')
    if description_title:
        description_div = description_title.find_next_sibling('div', class_='prop-det-text')
        if description_div:
            # Preserve paragraph/line-break structure before extracting text
            for tag in description_div.find_all(['p', 'br', 'div']):
                if tag.name == 'br':
                    tag.replace_with('\n')
                else:
                    tag.insert_before('\n\n')
            raw = description_div.get_text(separator='', strip=False)
            # Collapse excessive blank lines to a maximum of two
            import re as _re
            raw = _re.sub(r'\n{3,}', '\n\n', raw).strip()
            property_data['description'] = raw
    
    # Extract Rooms
    rooms_title = soup.find('h2', class_='prop-det-title', string='Rooms')
    if rooms_title:
        rooms_div = rooms_title.find_next_sibling('div', class_='prop-det-rooms')
        if rooms_div:
            rooms = []
            room_rows = rooms_div.find_all('div', class_='room-row')
            for room_row in room_rows:
                room_name = room_row.find('span', class_='room-name')
                room_desc = room_row.find('span', class_='room-desc')
                room_info = {}
                if room_name:
                    room_info['name'] = room_name.get_text(strip=True)
                if room_desc:
                    room_info['description'] = room_desc.get_text(strip=True)
                if room_info:
                    rooms.append(room_info)
            property_data['rooms'] = rooms
    
    return property_data


def get_property_links_from_page(url):
    """Extract all property links from a listing page"""
    response = get_with_retry(url)
    if not response:
        return []
    
    soup = BeautifulSoup(response.content, 'html.parser')
    
    # Find all property cards (Simon Brien specific)
    property_cards = soup.find_all('a', class_='prop-card')
    
    property_links = []
    for card in property_cards:
        href = card.get('href')
        if href:
            full_url = urljoin(BASE_DOMAIN, href)
            property_links.append(full_url)
    
    logger.info(f"Found {len(property_links)} properties on page: {url}")
    return property_links


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
    """Main scraping function"""
    parser = argparse.ArgumentParser(description='Simon Brien property scraper')
    parser.add_argument('--rent', action='store_true', help='Scrape rental properties')
    parser.add_argument('--limit', type=int, default=0, help='Max properties to scrape (0 = unlimited)')
    args = parser.parse_args()

    if args.rent:
        global BASE_URL, OUTPUT_DIR
        BASE_URL   = 'https://www.simonbrien.com/property-for-rent'
        OUTPUT_DIR = 'properties/sb_rent'

    logger.info("=" * 60)
    logger.info("Simon Brien Full Property Scraper")
    logger.info("=" * 60)

    # Create output directory
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Clear output directory for a fresh full scrape
    if not TEST_MODE:
        if os.path.exists(OUTPUT_DIR):
            logger.info(f"Clearing {OUTPUT_DIR}/ for a fresh full scrape...")
            shutil.rmtree(OUTPUT_DIR)
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        os.makedirs('logs', exist_ok=True)

    # Load existing property index
    property_index = load_property_index()
    existing_properties = property_index.get('properties', [])
    next_property_id = len(existing_properties) + 1

    logger.info(f"Starting with property ID: {next_property_id}")

    # Configure for test mode / limit
    pages_to_fetch = 1 if TEST_MODE else MAX_PAGES
    if TEST_MODE:
        max_props = 1
    elif args.limit > 0:
        max_props = args.limit
    else:
        max_props = MAX_PROPERTIES
    
    logger.info(f"Test mode: {TEST_MODE}")
    logger.info(f"Pages to fetch: {pages_to_fetch}")
    logger.info(f"Max properties: {max_props}")
    
    all_properties = []
    total_images_downloaded = 0
    
    # Iterate through listing pages
    for page_num in range(1, pages_to_fetch + 1):
        if len(all_properties) >= max_props:
            logger.info(f"Reached maximum property limit ({max_props})")
            break
        
        # Construct page URL
        if page_num == 1:
            page_url = BASE_URL
        else:
            page_url = f"{BASE_URL}/page{page_num}/?orderBy="
        
        logger.info(f"Fetching page {page_num}: {page_url}")
        
        # Get property links from this page
        property_links = get_property_links_from_page(page_url)
        
        if not property_links:
            logger.warning(f"No properties found on page {page_num}, stopping")
            break
        
        # Scrape each property
        for idx, property_url in enumerate(property_links):
            if len(all_properties) >= max_props:
                logger.info(f"Reached maximum property limit ({max_props})")
                break
            
            property_id = f"property_{next_property_id}"
            property_folder = os.path.join(OUTPUT_DIR, property_id)
            os.makedirs(property_folder, exist_ok=True)
            
            logger.info(f"Scraping property {property_id}: {property_url}")
            
            # Scrape property details
            property_data = scrape_property_details(property_url)
            if property_data:
                property_data['id'] = property_id
                
                # Scrape images (returns sorted, deduplicated URL list)
                image_count, image_urls = scrape_property_images(property_url, property_folder)
                property_data['image_count'] = image_count
                property_data['image_urls'] = image_urls
                total_images_downloaded += image_count
                
                # Save property data to JSON file
                json_path = os.path.join(property_folder, f"{property_id}.json")
                with open(json_path, 'w', encoding='utf-8') as f:
                    json.dump(property_data, f, indent=2, ensure_ascii=False)
                
                logger.info(f"Saved property data to: {json_path}")
                logger.info(f"Downloaded {image_count} images")
                
                all_properties.append(property_data)
                
                # Progress update
                if len(all_properties) % 10 == 0:
                    logger.info(f"Progress: {len(all_properties)} properties scraped, {total_images_downloaded} images downloaded")
                
                # Increment property ID for next property
                next_property_id += 1
            
            # Random delay between property scrapes
            delay = random.uniform(REQUEST_DELAY_MIN, REQUEST_DELAY_MAX)
            time.sleep(delay)
        
        # Random delay between page scrapes
        delay = random.uniform(REQUEST_DELAY_MIN, REQUEST_DELAY_MAX)
        time.sleep(delay)
    
    # Save summary
    summary = {
        'total_properties': len(all_properties),
        'total_images': total_images_downloaded,
        'scraped_at': datetime.now().isoformat(),
        'test_mode': TEST_MODE
    }
    
    summary_path = os.path.join(OUTPUT_DIR, 'summary.json')
    with open(summary_path, 'w', encoding='utf-8') as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)
    
    logger.info("=" * 60)
    logger.info("Scraping Complete!")
    logger.info(f"Total properties scraped: {len(all_properties)}")
    logger.info(f"Total images downloaded: {total_images_downloaded}")
    logger.info(f"Summary saved to: {summary_path}")
    logger.info(f"Log file: {log_path}")
    logger.info("=" * 60)
    
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


if __name__ == "__main__":
    main()
