import re
import argparse
import sys
import shutil
import requests
from bs4 import BeautifulSoup
import time
import random
import os
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
base_url_template = 'https://www.ulsterpropertysales.co.uk/property-for-sale/page{page_num}/'
OUTPUT_DIR = 'properties/ups'

# Create directories for downloads
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs('logs', exist_ok=True)

# Setup logging
log_filename = f"logs/scraper_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
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
        if '/property/' in href and '/property-for-sale/' not in href:
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
    
    # Extract address — prefer the desktop section (prop-det-top-right) which has the
    # full single-line address. The mobile section splits it across two elements and
    # the first h1 found by soup.find() would give a truncated result.
    top_right = soup.find('div', class_='prop-det-top-right')
    address_one = (
        (top_right.find('h1', class_='prop-det-address-one') if top_right else None) or
        soup.find('h1', class_='prop-det-address-one')
    )
    if address_one:
        full_address = address_one.get_text(strip=True).rstrip(',').strip()
        property_data['address'] = full_address
        property_data['title'] = full_address
    else:
        # Fallback: piece together from mobile two-line format
        address_two = (
            soup.find('h2', class_='prop-det-address-two') or
            soup.find('p', class_='prop-det-address-two') or
            soup.find('span', class_='prop-det-address-two') or
            soup.find('div', class_='prop-det-address-two')
        )
        fallback_one = soup.find('h1', class_='prop-det-address-one')
        if fallback_one:
            line1 = fallback_one.get_text(strip=True).rstrip(',').strip()
            line2 = address_two.get_text(strip=True).rstrip(',').strip() if address_two else ''
            combined = f"{line1}, {line2}".rstrip(', ') if line2 else line1
            property_data['address'] = combined
            property_data['title'] = combined
        else:
            # Last resort: use page <title> tag, strip agency suffix
            page_title = soup.find('title')
            if page_title:
                t = page_title.get_text(strip=True)
                for suffix in [' for sale with UPS', ' for sale with Ulster Property Sales', ' - Ulster Property Sales']:
                    t = t.replace(suffix, '')
                property_data['title'] = t.strip()

    # Extract structured property facts (bedrooms, bathrooms, receptions, type, EPC)
    # UPS uses <ul class="icons2"> inside the desktop prop-det-top-right panel,
    # with <li> items like "3 bedrooms", "2 Receptions", "Semi-Detached" etc.
    facts_list = (
        (top_right.find('ul', class_='icons2') if top_right else None) or
        soup.find('ul', class_='icons2')
    )
    KNOWN_TYPES = {
        'detached': 'Detached',
        'semi-detached': 'Semi-Detached',
        'semi detached': 'Semi-Detached',
        'terraced': 'Terraced',
        'end terrace': 'End Terrace',
        'end-terrace': 'End Terrace',
        'apartment': 'Apartment',
        'flat': 'Flat',
        'bungalow': 'Bungalow',
        'cottage': 'Cottage',
        'townhouse': 'Townhouse',
        'town house': 'Townhouse',
        'villa': 'Villa',
        'link detached': 'Link Detached',
        'link-detached': 'Link Detached',
        'maisonette': 'Maisonette',
    }

    if facts_list:
        for item in facts_list.find_all('li'):
            text = item.get_text(strip=True)
            tl = text.lower()
            m = re.match(r'(\d+)\s*bedroom', tl)
            if m:
                n = int(m.group(1))
                property_data['bedrooms'] = f"{n} Bedroom{'s' if n != 1 else ''}"
                continue
            m = re.match(r'(\d+)\s*bathroom', tl)
            if m:
                property_data['bathrooms'] = m.group(1)
                continue
            m = re.match(r'(\d+)\s*reception', tl)
            if m:
                property_data['receptions'] = m.group(1)
                continue
            m = re.search(r'epc[^\d]*([a-g]\d+)', tl)
            if m:
                property_data['epc_rating'] = text
                continue
            # Property type detection
            if 'property_type' not in property_data:
                for key, label in KNOWN_TYPES.items():
                    if key in tl:
                        property_data['property_type'] = label
                        break
    
    # Extract price (UPS specific)
    price_text = soup.find('span', class_='prop-det-price-text')
    price_amount = soup.find('span', class_='prop-det-price-amount')
    if price_text and price_amount:
        qualifier = price_text.get_text(strip=True)
        amount = price_amount.get_text(strip=True)
        # For rentals, strip "Monthly" qualifier and "pm" suffix - web app adds "/mo"
        if '-rent/' in property_url.lower():
            qualifier = ''
            amount = re.sub(r'\s*pm$', '', amount, flags=re.IGNORECASE).strip()
        property_data['price'] = f"{qualifier} {amount}".strip() if qualifier else amount
    elif price_amount:
        amount = price_amount.get_text(strip=True)
        # For rentals, strip "pm" suffix
        if '-rent/' in property_url.lower():
            amount = re.sub(r'\s*pm$', '', amount, flags=re.IGNORECASE).strip()
        property_data['price'] = amount
    
    # Extract status (UPS specific).
    # UPS renders the status as an SVG image inside <div class="sale-agr"> (not text),
    # so we check for that container and its img src/alt.  Also keep the older
    # text-based selectors as a fallback for any future site changes.
    status = 'For Sale'

    # Primary: check for the SVG-image status badge (class="sale-agr")
    sale_agr_div = soup.find('div', {'class': 'sale-agr'})
    if sale_agr_div:
        img = sale_agr_div.find('img')
        src = (img.get('src') or '').lower() if img else ''
        alt = (img.get('alt') or '').lower() if img else ''
        if 'saleagreed' in src or 'sale-agreed' in src or 'agreed' in alt:
            status = 'Sale Agreed'
        elif 'sold' in src or 'sold' in alt:
            status = 'Sold'

    # Secondary: check any img inside prop-det-status-outer for sold.svg etc.
    if status == 'For Sale':
        status_outer = soup.find('div', {'class': 'prop-det-status-outer'})
        if status_outer:
            img = status_outer.find('img')
            if img:
                src = (img.get('src') or '').lower()
                alt = (img.get('alt') or '').lower()
                if 'saleagreed' in src or 'agreed' in alt:
                    status = 'Sale Agreed'
                elif 'sold' in src or 'sold' in alt:
                    status = 'Sold'

    # Tertiary: older text-based selectors
    if status == 'For Sale':
        for selector in [
            ('div', {'class': 'sale-agreed'}),
            ('span', {'class': 'sale-agreed'}),
            ('div', {'class': 'prop-det-status'}),
            ('span', {'class': 'prop-det-status'}),
            ('div', {'class': 'status-banner'}),
        ]:
            el = soup.find(selector[0], selector[1])
            if el:
                text = el.get_text(strip=True).lower()
                if 'agreed' in text:
                    status = 'Sale Agreed'
                elif 'sold' in text:
                    status = 'Sold'
                break

    property_data['status'] = status

    # Extract key features (UPS specific)
    key_features = []
    feats_div = soup.find('div', class_='prop-det-feats')
    if feats_div:
        for feat in feats_div.find_all('div', class_='feat'):
            feat_text = feat.get_text(strip=True)
            # Remove the check icon if present
            if feat_text.startswith('✓') or feat_text.startswith('✓ '):
                feat_text = feat_text[1:].strip()
            key_features.append(feat_text)
    if key_features:
        property_data['key_features'] = key_features
    
    # Extract description (UPS specific)
    desc_div = soup.find('div', class_='prop-det-text')
    if desc_div:
        property_data['description'] = desc_div.get_text(strip=True)
    
    # Extract rooms (UPS specific)
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
                room_data['description'] = room_desc.get_text(strip=True)
            
            rooms.append(room_data)
    if rooms:
        property_data['rooms'] = rooms
    
    # Extract and download images from the property page
    image_count = 0
    image_urls = []  # Collect original source URLs for Supabase

    # Target the gallery element specifically
    gallery = soup.find('ul', id='gallery')
    if gallery:
        # Find all img tags within the gallery
        gallery_imgs = gallery.find_all('img')
        logger.info(f"Found {len(gallery_imgs)} images in gallery")

        for img in gallery_imgs:
            img_url = img.get('src') or img.get('data-src')
            if img_url:
                full_img_url = urljoin(property_url, img_url)
                image_urls.append(full_img_url)
    else:
        # Fallback: try to find images with property-specific patterns
        logger.warning("Gallery element not found, using fallback method")
        img_tags = soup.find_all('img')
        for img in img_tags:
            img_url = img.get('src') or img.get('data-src')
            if img_url:
                full_img_url = urljoin(property_url, img_url)
                # Only download images that are property photos (not logos, office images, etc.)
                # Property images follow pattern: /images/property/{folder}/{property_id}/{property_id}-{num}.jpg
                if '/images/property/' in full_img_url and 'office' not in full_img_url.lower() and 'logo' not in full_img_url.lower():
                    image_urls.append(full_img_url)

    # UPS gallery carousels put the last image at position 0 for seamless looping,
    # just like TR.  Sort by the trailing numeric suffix (e.g. -1.jpg before -28.jpg)
    # and deduplicate to restore the correct display order.
    image_urls = _sort_and_dedup_image_urls(image_urls)

    # Download in correct order
    for i, url in enumerate(image_urls, 1):
        downloaded_path = download_image(url, property_folder, i)
        if downloaded_path:
            image_count += 1

    property_data['image_urls'] = image_urls
    
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
    parser = argparse.ArgumentParser(description='Ulster Property Sales scraper')
    parser.add_argument('--rent',  action='store_true', help='Scrape rental properties')
    parser.add_argument('--limit', type=int, default=0, help='Max properties to scrape (0 = unlimited)')
    args = parser.parse_args()

    if args.rent:
        global base_url_template, OUTPUT_DIR
        base_url_template = 'https://www.ulsterpropertysales.co.uk/property-for-rent/page{page_num}/'
        OUTPUT_DIR = 'properties/ups_rent'

    logger.info("Starting Ulster Property Sales scraper v2...")

    # Configuration
    max_pages = 1000  # Adjust based on how many pages you want to scrape
    max_properties = args.limit if args.limit > 0 else 100000
    test_mode = False  # Set to True to scrape only 1 property for testing

    # Clear the output folder before a full scrape so stale data doesn't linger.
    # In test mode we leave the folder intact so you can inspect previous runs.
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
    # Build a URL → property_id map so re-runs update in-place rather than duplicating
    url_to_id = {p['url']: p['id'] for p in existing_properties if p.get('url')}
    # Next ID is one past the highest numeric suffix seen (or count+1 as fallback)
    existing_nums = []
    for p in existing_properties:
        m = re.match(r'property_(\d+)', p.get('id', ''))
        if m:
            existing_nums.append(int(m.group(1)))
    next_property_id = (max(existing_nums) + 1) if existing_nums else 1

    logger.info(f"Existing properties in index: {len(existing_properties)}")
    
    # Collect all property links from multiple pages
    all_property_links = []
    
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
    
    # For testing, only scrape the first property
    properties_to_scrape = all_property_links[:1] if test_mode else all_property_links
    
    logger.info(f"Testing mode: {test_mode}")
    logger.info(f"Scraping {len(properties_to_scrape)} property(ies)")
    
    # Scrape each property
    all_properties = []
    new_id_counter = next_property_id
    for idx, property_url in enumerate(properties_to_scrape, 1):
        logger.info(f"{'='*60}")
        logger.info(f"Processing property {idx}/{len(properties_to_scrape)}")

        # Reuse existing ID if this URL was scraped before, otherwise allocate a new one
        if property_url in url_to_id:
            property_id = url_to_id[property_url]
            logger.info(f"Re-scraping existing property: {property_id}")
        else:
            property_id = f"property_{new_id_counter}"
            new_id_counter += 1
        
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
    
    # Update property index — update in place for re-scraped properties, append for new ones
    existing_by_id = {p['id']: p for p in existing_properties}
    for property_data in all_properties:
        entry = {
            'id': property_data['id'],
            'url': property_data['url'],
            'address': property_data.get('address', ''),
            'title': property_data.get('title', ''),
            'scraped_at': property_data['scraped_at'],
        }
        existing_by_id[entry['id']] = entry  # overwrites if existing, adds if new
    existing_properties = list(existing_by_id.values())
    
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
