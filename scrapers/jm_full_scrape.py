import argparse
import re
import requests
from bs4 import BeautifulSoup
import time
import random
import os
import sys
import shutil
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
base_url_template = 'https://www.johnminnis.co.uk/search/906207/page{page_num}/'
OUTPUT_DIR = 'properties/jm'

# Create directories for downloads
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs('logs', exist_ok=True)

# Setup logging
log_filename = f"logs/jm_scraper_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
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
    # These are typically links that go to individual property pages
    for link in soup.find_all('a', href=True):
        href = link['href']
        # Look for links that contain property-related patterns
        # Individual property pages usually have URLs like /property/location/id/address/
        if '/property/' in href and '/property-for-sale/' not in href and '/property-for-rent/' not in href:
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
    
    # Extract title (look for common patterns)
    title = soup.find('h1') or soup.find('h2')
    if title:
        property_data['title'] = title.get_text(strip=True)
    
    # Extract address from title or h1 tag
    # John Minnis uses the title tag for the full address
    title_tag = soup.find('title')
    if title_tag:
        title_text = title_tag.get_text(strip=True)
        # Remove "for sale with John Minnis" suffix if present
        if ' for sale with John Minnis' in title_text:
            address = title_text.replace(' for sale with John Minnis', '')
        else:
            address = title_text
        property_data['address'] = address
    
    # Extract price (John Minnis specific)
    price_amount = soup.find('span', class_='price-amount')
    if price_amount:
        property_data['price'] = price_amount.get_text(strip=True)
    
    # Extract property info (John Minnis specific)
    property_info = {}
    info_rows = soup.find_all('div', class_='prop-det-info-row')
    for row in info_rows:
        left_span = row.find('span', class_='prop-det-info-left')
        right_span = row.find('span', class_='prop-det-info-right')
        if left_span and right_span:
            label = left_span.get_text(strip=True)
            value = right_span.get_text(strip=True)
            # Remove icon characters
            label = ''.join(c for c in label if not c in ['\uf015', '\uf002', '\uf3ed', '\uf236', '\uf4d8', '\uf06d', '\uf080'])
            property_info[label] = value
    
    if property_info:
        property_data['property_info'] = property_info
    
    # Extract key features (John Minnis specific)
    key_features = []
    feats_div = soup.find('div', class_='prop-det-feats')
    if feats_div:
        for feat in feats_div.find_all('div', class_='feat'):
            feat_text = feat.get_text(strip=True)
            key_features.append(feat_text)
    if key_features:
        property_data['key_features'] = key_features
    
    # Extract description (John Minnis specific)
    desc_div = soup.find('div', class_='prop-det-text')
    if desc_div:
        text_div = desc_div.find('div', class_='text')
        if text_div:
            property_data['description'] = text_div.get_text(strip=True)
        else:
            property_data['description'] = desc_div.get_text(strip=True)
    
    # Extract rooms (John Minnis specific)
    rooms = []
    rooms_div = soup.find('div', class_='prop-det-rooms')
    if rooms_div:
        for room_row in rooms_div.find_all('div', class_='room-row'):
            room_name = room_row.find('span', class_='room-name')
            room_desc = room_row.find('span', class_='room-desc')
            
            room_data = {'name': '', 'dimensions': '', 'description': ''}
            
            if room_name:
                # Extract name and dimensions
                name_text = room_name.get_text(strip=True)
                # Dimensions are in a span within room_name
                dim_span = room_name.find('span')
                if dim_span:
                    room_data['dimensions'] = dim_span.get_text(strip=True)
                    room_data['name'] = name_text.replace(dim_span.get_text(strip=True), '').strip()
                else:
                    room_data['name'] = name_text
            
            if room_desc:
                desc_span = room_desc.find('span')
                if desc_span:
                    room_data['description'] = desc_span.get_text(strip=True)
                else:
                    room_data['description'] = room_desc.get_text(strip=True)
            
            rooms.append(room_data)
    if rooms:
        property_data['rooms'] = rooms
    
    # Extract and download images from the property page.
    # JM: div#gallery contains <a href="full-size-url"> links in display order.
    # DOM order is preserved; non-image hrefs (e.g. YouTube) are filtered out.
    _img_href_re = re.compile(r'\.(jpg|jpeg|png|webp|gif)(\?.*)?$', re.IGNORECASE)
    image_count = 0
    image_urls = []

    gallery = soup.find('div', id='gallery')
    if gallery:
        # Collect full-size URLs from <a> hrefs (not thumbnail <img> srcs),
        # skipping any links that don't point to an image file.
        for a in gallery.find_all('a', href=True):
            if not _img_href_re.search(a['href']):
                continue
            full_url = urljoin(property_url, a['href'])
            if full_url not in image_urls:
                image_urls.append(full_url)
        logger.info(f"Found {len(image_urls)} image links in gallery")
    else:
        logger.warning("Gallery element not found, falling back to /images/property/ img scan")
        for img in soup.find_all('img'):
            src = img.get('src') or img.get('data-src')
            if src:
                full_url = urljoin(property_url, src)
                if '/images/property/' in full_url and not any(
                    x in full_url.lower() for x in ('logo', 'office', 'icon')
                ):
                    if full_url not in image_urls:
                        image_urls.append(full_url)

    property_data['image_urls'] = image_urls

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
    parser = argparse.ArgumentParser(description='John Minnis property scraper')
    parser.add_argument('--limit', type=int, default=0, help='Max properties to scrape (0 = unlimited)')
    args = parser.parse_args()

    logger.info("Starting John Minnis scraper...")

    # Configuration
    max_pages = 1000  # Adjust based on how many pages you want to scrape
    max_properties = args.limit if args.limit > 0 else 100000
    test_mode = False  # Set to True to scrape only 1 property for testing

    # Clear output directory for a fresh full scrape
    if not test_mode:
        if os.path.exists(OUTPUT_DIR):
            logger.info(f"Clearing {OUTPUT_DIR}/ for a fresh full scrape...")
            shutil.rmtree(OUTPUT_DIR)
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        os.makedirs('logs', exist_ok=True)

    # In test mode, only fetch 1 page
    pages_to_fetch = 1 if test_mode else max_pages

    # Load existing property index
    property_index = load_property_index()
    existing_properties = property_index.get('properties', [])
    next_property_id = len(existing_properties) + 1
    
    logger.info(f"Existing properties in index: {len(existing_properties)}")
    
    # Collect all property links from multiple pages
    all_property_links = []
    seen_urls = set()  # Track URLs to detect duplicates/end of pagination

    for page_num in range(1, pages_to_fetch + 1):
        page_url = base_url_template.format(page_num=page_num)
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
    
    # For testing, only scrape the first property
    properties_to_scrape = all_property_links[:1] if test_mode else all_property_links
    
    logger.info(f"Testing mode: {test_mode}")
    logger.info(f"Scraping {len(properties_to_scrape)} property(ies)")
    
    # Scrape each property
    all_properties = []
    for idx, property_url in enumerate(properties_to_scrape, 1):
        logger.info(f"{'='*60}")
        logger.info(f"Processing property {idx}/{len(properties_to_scrape)}")

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
        if idx < len(properties_to_scrape):
            # Random delay between 1-3 seconds to be respectful to the server
            delay = random.uniform(1, 3)
            logger.info(f"Waiting {delay:.2f} seconds before next request...")
            time.sleep(delay)
        
        # Progress update every 10 properties
        if idx % 10 == 0:
            logger.info(f"Progress: {idx}/{len(properties_to_scrape)} properties processed ({idx/len(properties_to_scrape)*100:.1f}%)")
    
    # Save summary
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
