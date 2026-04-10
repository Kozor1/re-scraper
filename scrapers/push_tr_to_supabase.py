"""
push_tr_to_supabase.py  –  Push all TR property data to Supabase using raw HTTP.

Uses only the `requests` library (no supabase package needed).
Safe to re-run — upserts on (source, source_id).

Usage:
    python3 scrapers/push_tr_to_supabase.py
"""

import os, re, json, sys, time
import requests

# ── Config ────────────────────────────────────────────────────────────────────

ROOT       = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TR_DIR     = os.path.join(ROOT, 'scrapers', 'properties', 'tr')
ENV_PATH   = os.path.join(ROOT, '.env')

# Load .env
env = {}
if os.path.exists(ENV_PATH):
    for line in open(ENV_PATH):
        line = line.strip()
        if line and not line.startswith('#') and '=' in line:
            k, v = line.split('=', 1)
            env[k.strip()] = v.strip()

SUPABASE_URL = env.get('SUPABASE_URL') or os.environ.get('SUPABASE_URL', '')
SUPABASE_KEY = env.get('SUPABASE_SERVICE_KEY') or os.environ.get('SUPABASE_SERVICE_KEY', '')

if not SUPABASE_URL or not SUPABASE_KEY:
    print("ERROR: Missing SUPABASE_URL or SUPABASE_SERVICE_KEY in .env")
    sys.exit(1)

REST_URL = SUPABASE_URL.rstrip('/') + '/rest/v1/properties'
HEADERS  = {
    'apikey':        SUPABASE_KEY,
    'Authorization': f'Bearer {SUPABASE_KEY}',
    'Content-Type':  'application/json',
    'Prefer':        'resolution=merge-duplicates',
}

# ── Geocache ──────────────────────────────────────────────────────────────────

geocache_path = os.path.join(ROOT, 'geocache.json')
geocache = {}
if os.path.exists(geocache_path):
    geocache = json.load(open(geocache_path))
print(f"Geocache: {len(geocache)} addresses")

# ── Helpers ───────────────────────────────────────────────────────────────────

def parse_price_value(price_str):
    if not price_str:
        return None
    m = re.search(r'£([\d,]+)', str(price_str))
    if m:
        try:
            return int(m.group(1).replace(',', ''))
        except Exception:
            pass
    try:
        return int(price_str)
    except Exception:
        return None

def normalise_status(raw):
    s = (raw or '').strip().lower()
    if s in ('for sale', 'forsale'):              return 'For Sale'
    if s in ('sale agreed', 'saleagreed', 'agreed'): return 'Sale Agreed'
    if s == 'sold':                               return 'Sold'
    return (raw or '').strip() or 'For Sale'

def normalise_bedrooms(raw):
    if not raw:
        return None
    m = re.search(r'(\d+)', str(raw))
    return m.group(1) if m else str(raw)

def build_row(source_id, data):
    address   = (data.get('address') or data.get('title') or '').strip().rstrip(',').strip()
    coords    = geocache.get(address)
    price_str = data.get('price_str') or data.get('price') or ''
    if isinstance(price_str, int):
        price_str = f'£{price_str:,}'

    row = {
        'source':        'tr',
        'source_id':     source_id,
        'url':           data.get('url') or '',
        'address':       address,
        'title':         data.get('title') or address,
        'price':         str(price_str) if price_str else None,
        'price_value':   parse_price_value(price_str),
        'status':        normalise_status(data.get('status') or ''),
        'property_type': data.get('type') or None,
        'bedrooms':      normalise_bedrooms(data.get('bedrooms') or ''),
        'bathrooms':     data.get('bathrooms') or None,
        'receptions':    data.get('receptions') or None,
        'epc_rating':    data.get('epc_rating') or None,
        'description':   data.get('description') or None,
        'key_features':  data.get('key_features') or [],
        'rooms':         data.get('rooms') or [],
        'image_urls':    data.get('image_urls') or [],
    }
    if coords:
        row['lat'] = coords['lat']
        row['lng'] = coords['lng']
    return row

# ── Load all TR properties ────────────────────────────────────────────────────

props = []
for name in sorted(
    (d for d in os.listdir(TR_DIR) if d.startswith('property_') and os.path.isdir(os.path.join(TR_DIR, d))),
    key=lambda x: int(x.replace('property_', '')) if x.replace('property_', '').isdigit() else 0
):
    jpath = os.path.join(TR_DIR, name, f'{name}.json')
    if not os.path.exists(jpath):
        continue
    try:
        data = json.load(open(jpath, encoding='utf-8'))
        props.append((name, data))
    except Exception as e:
        print(f"  WARN: could not read {jpath}: {e}")

print(f"Properties to push: {len(props)}")

# ── Upsert in batches of 50 ───────────────────────────────────────────────────

BATCH = 50
inserted = 0
errors   = 0

for i in range(0, len(props), BATCH):
    batch = props[i:i+BATCH]
    rows  = []
    for source_id, data in batch:
        try:
            rows.append(build_row(source_id, data))
        except Exception as e:
            print(f"  WARN build_row {source_id}: {e}")
            errors += 1

    for attempt in range(3):
        try:
            resp = requests.post(REST_URL, headers=HEADERS, json=rows, timeout=30)
            if resp.status_code in (200, 201):
                inserted += len(rows)
                pct = (i + len(batch)) / len(props) * 100
                print(f"  [{i+len(batch)}/{len(props)}]  {pct:.0f}%  ok")
                break
            else:
                print(f"  ERROR batch {i}: HTTP {resp.status_code} — {resp.text[:200]}")
                if attempt < 2:
                    time.sleep(2 ** attempt)
                else:
                    errors += len(rows)
        except Exception as e:
            print(f"  ERROR batch {i}: {e}")
            if attempt < 2:
                time.sleep(2 ** attempt)
            else:
                errors += len(rows)

print(f"\n{'='*50}")
print(f"Done: {inserted} upserted, {errors} errors")
