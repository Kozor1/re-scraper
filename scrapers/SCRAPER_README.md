# Ulster Property Sales Scraper

## Overview

This scraper consists of two Python scripts for scraping property data from Ulster Property Sales website:

1. **`ups_full_scrape.py`** - Initial full scrape to establish a baseline
2. **`ups_update_scrape.py`** - Update scraper to find and add new properties

## File Structure

```
ups_properties/
├── property_index.json          # Index of all scraped properties
├── summary.json                 # Summary of scraping operations
└── property_1/
    ├── property_1.json          # Property data (without images array)
    ├── img1.jpg
    ├── img2.jpg
    └── ...
```

## Property Index File

The `property_index.json` file tracks all scraped properties and is used by the update scraper to identify duplicates. It contains:

```json
{
  "properties": [
    {
      "id": "property_1",
      "url": "https://www.ulsterpropertysales.co.uk/property/...",
      "address": "16 Ashburn, Ballynahinch",
      "title": "Property Title",
      "scraped_at": "2026-03-05T21:21:01.419978"
    }
  ],
  "last_updated": "2026-03-05T21:21:01.419978"
}
```

## Usage

### Initial Full Scrape

Run this first to establish a baseline of properties:

```bash
python3 ups_full_scrape.py
```

Configuration options in `ups_full_scrape.py`:
- `max_pages = 1` - Number of listing pages to scrape
- `max_properties = 6` - Maximum total properties to scrape
- `test_mode = False` - Set to True to scrape only 1 property for testing

### Update Scrape

Run this periodically to find and add new properties:

```bash
python3 ups_update_scrape.py
```

Configuration options in `ups_update_scrape.py`:
- `max_pages = 50` - Maximum pages to check
- `consecutive_duplicates_threshold = 3` - Stop after this many consecutive duplicates
- `test_mode = False` - Set to True to test with limited properties

## How It Works

### Full Scrape

1. Iterates through listing pages (ordered by recency)
2. Extracts property links from each page
3. Scrapes each property page for:
   - Property details (address, price, features, description, rooms)
   - Property images (from the gallery element)
4. Saves property data to JSON file in property folder
5. Downloads images to property folder
6. Updates `property_index.json` with new properties
7. Creates `summary.json` with scraping statistics

### Update Scrape

1. Loads `property_index.json` to get existing properties
2. Creates a set of normalized addresses for fast lookup
3. Iterates through listing pages (ordered by recency)
4. For each property:
   - Fetches the property page to extract address
   - Normalizes the address (lowercase, removes extra spaces)
   - Checks if address exists in the index
   - If duplicate: increments consecutive duplicate counter
   - If new: adds to list of new properties to scrape
5. Stops when `consecutive_duplicates_threshold` is reached
6. Scrapes only new properties
7. Updates `property_index.json` with new properties
8. Updates `summary.json` with new statistics

## Key Features

### Property Index

- **Efficient**: Single file lookup instead of scanning entire folder
- **Fast**: Set-based address comparison for O(1) lookup
- **Reliable**: Tracks property ID, URL, address, title, and scrape timestamp

### Consecutive Duplicate Detection

- **Robust**: Continues past first duplicate (handles typos/variations)
- **Configurable**: Threshold can be adjusted (default: 3)
- **Smart**: Resets counter when new property is found

### Rate Limiting

- **Respectful**: Random delays between requests (1-3 seconds)
- **Retry Logic**: Exponential backoff for failed requests
- **Configurable**: Can be adjusted as needed

## Property Data Structure

Each property JSON file contains:

```json
{
  "url": "https://www.ulsterpropertysales.co.uk/property/...",
  "id": "property_1",
  "scraped_at": "2026-03-05T21:21:01.419978",
  "title": "Property Title",
  "address": "16 Ashburn, Ballynahinch",
  "price": "£150,000",
  "key_features": [
    "Feature 1",
    "Feature 2"
  ],
  "description": "Property description...",
  "rooms": [
    {
      "name": "Living Room",
      "dimensions": "4.2m x 3.8m",
      "description": "Room description..."
    }
  ]
}
```

Note: Images are saved as separate files in the property folder, not included in the JSON.

## Logging

All scraping operations are logged to:
- `logs/scraper_YYYYMMDD_HHMMSS.log`
- Logs include timestamps, property URLs, download status, and errors

## Configuration

### Full Scrape Configuration

Edit these values in `ups_full_scrape.py`:

```python
max_pages = 1              # Number of listing pages to scrape
max_properties = 6         # Maximum total properties to scrape
test_mode = False         # Set to True to scrape only 1 property for testing
```

### Update Scrape Configuration

Edit these values in `ups_update_scrape.py`:

```python
max_pages = 50                                    # Maximum pages to check
consecutive_duplicates_threshold = 3              # Stop after this many consecutive duplicates
test_mode = False                                # Set to True to test with limited properties
```

## Troubleshooting

### No properties found

- Check if the website structure has changed
- Verify the base URL template is correct
- Check the log file for errors

### Duplicate properties

- The scraper uses normalized addresses for comparison
- Adjust `consecutive_duplicates_threshold` if needed
- Check the `property_index.json` file for existing properties

### Rate limiting issues

- Increase the delay between requests
- Adjust the retry logic parameters
- Check the website's terms of service

## Notes

- The website lists properties by recency (newest first)
- The update scraper works forwards through listings
- Images are extracted from the `<ul id="gallery">` element
- Property IDs are sequential (property_1, property_2, etc.)
- All property data is saved without the images array in the JSON file
