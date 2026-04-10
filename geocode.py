#!/usr/bin/env python3
"""
geocode.py  –  Geocode all property addresses using Google Geocoding API.

Reads every property JSON in properties/{source}/ and geocodes any address not
already in geocache.json.  Results are saved incrementally so the script can be
interrupted and resumed safely.

Primary:  Google Geocoding API  (requires GOOGLE_GEOCODING_API_KEY in .env)
Fallback: OpenStreetMap Nominatim  (free, used if Google returns no result or
          no API key is configured)

Google API key setup:
  1. Go to https://console.cloud.google.com/ → Geocoding API → Create credentials
  2. Add to re_app/.env:   GOOGLE_GEOCODING_API_KEY=AIza...
  Free tier: 40,000 requests/month (far more than needed).

Strategy (tried in order for each address):
  1. Google:  address + town-from-URL + "Northern Ireland, UK"
  2. Google:  address + short-town + "Northern Ireland, UK"
  3. Google:  full address + "Northern Ireland, UK"
  4. Google:  prefix-stripped address + town + "Northern Ireland, UK"
  5. Google:  postcode only (if BT postcode found)
  6. Google:  town only (fallback placement)
  (Then Nominatim with the same candidates if Google still fails)

Usage:
    python3 geocode.py                    # geocode all missing addresses
    python3 geocode.py --limit 100        # stop after 100 new lookups
    python3 geocode.py --source sb        # only process one source
    python3 geocode.py --dry-run          # print candidates without calling API
    python3 geocode.py --retry-failed     # retry addresses previously marked as not-found
    python3 geocode.py --no-nominatim     # skip Nominatim fallback (Google-only)

Run from the re_app/ directory.
After running:
    python3 supabase/migrate_data.py      # push lat/lng to Supabase
"""

import os, json, sys, time, argparse, re
from urllib.request import urlopen, Request
from urllib.parse import urlencode, urlparse, quote_plus
from urllib.error import URLError

# ── Config ─────────────────────────────────────────────────────────────────────

ROOT          = os.path.dirname(os.path.abspath(__file__))
GEOCACHE_PATH = os.path.join(ROOT, 'geocache.json')
NOMINATIM_DELAY = 1.1   # seconds between Nominatim requests (policy: max 1/sec)
GOOGLE_DELAY    = 0.05  # 50 ms between Google requests (well within free-tier limits)

SOURCES = {
    'sb':       'properties/sb',
    'ups':      'properties/ups',
    'hc':       'properties/hc',
    'jm':       'properties/jm',
    'pp':       'properties/pp',
    'tr':       'properties/tr',
    'dh':       'properties/dh',
    'mm':       'properties/mm',
    'ce':       'properties/ce',
    'gm':       'properties/gm',
    'pinp':     'properties/pinp',
    'rb':       'properties/rb',
    # Rental feeds — same geocache is shared so addresses already found for
    # sale properties are free; only genuinely new rental addresses need a lookup.
    'sb_rent':  'properties/sb_rent',
    'ups_rent': 'properties/ups_rent',
    'hc_rent':  'properties/hc_rent',
    'jm_rent':  'properties/jm_rent',
    'pp_rent':  'properties/pp_rent',
    'tr_rent':  'properties/tr_rent',
    'dh_rent':  'properties/dh_rent',
    'mm_rent':  'properties/mm_rent',
    'ce_rent':  'properties/ce_rent',
    'gm_rent':  'properties/gm_rent',
    'rb_rent':  'properties/rb_rent',
}

# User-Agent required by Nominatim's usage policy
NOMINATIM_UA = 'PropertySwipe/1.0 (Northern Ireland property app; r.m.lavery@hotmail.co.uk)'


def _load_env():
    """Load key=value pairs from .env file into os.environ (if not already set)."""
    env_path = os.path.join(ROOT, '.env')
    if not os.path.exists(env_path):
        return
    with open(env_path, encoding='utf-8') as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith('#') or '=' not in line:
                continue
            key, _, val = line.partition('=')
            key = key.strip()
            val = val.strip().strip('"').strip("'")
            if key and key not in os.environ:
                os.environ[key] = val


_load_env()
GOOGLE_API_KEY = os.environ.get('GOOGLE_GEOCODING_API_KEY', '')

# NI towns/cities for fast matching in address slugs
NI_PLACES = [
    'belfast', 'lisburn', 'londonderry', 'derry', 'newtownabbey', 'bangor', 'castlereagh',
    'ballymena', 'newtownards', 'antrim', 'omagh', 'enniskillen', 'larne', 'holywood',
    'carrickfergus', 'ballyclare', 'dungannon', 'coleraine', 'limavady', 'strabane',
    'cookstown', 'armagh', 'portadown', 'lurgan', 'newry', 'downpatrick', 'banbridge',
    'craigavon', 'maghera', 'magherafelt', 'portstewart', 'portrush', 'ballycastle',
    'ballymoney', 'comber', 'carryduff', 'dundonald', 'dunmurry', 'finaghy', 'stranmillis',
    'whitehead', 'greenisland', 'islandmagee', 'glengormley', 'templepatrick', 'hillsborough',
    'crumlin', 'randalstown', 'ahoghill', 'broughshane', 'cullybackey', 'cushendall',
    'carnlough', 'glenarm', 'ballygally', 'parkgate', 'kells', 'whitehouse', 'donaghadee',
    'jordanstown', 'monkstown', 'doagh', 'ballynure', 'muckamore', 'toomebridge',
    'carnmoney', 'lisburn', 'dromore', 'moira', 'dromara', 'saintfield', 'bangor',
    'ballynahinch', 'crossgar', 'killyleagh', 'ardglass', 'strangford', 'portaferry',
    'warrenpoint', 'rostrevor', 'kilkeel', 'newcastle', 'castlewellan', 'clough',
    'castlederg', 'fivemiletown', 'maguiresbridge', 'irvinestown', 'lisnaskea',
    'ligoniel', 'andersonstown', 'ballysillan', 'lisnaharragh', 'newtownbreda',
]

# ── Geocache ───────────────────────────────────────────────────────────────────

def load_geocache():
    if os.path.exists(GEOCACHE_PATH):
        try:
            return json.load(open(GEOCACHE_PATH, encoding='utf-8'))
        except Exception as e:
            print(f"WARN: could not read geocache: {e}")
    return {}

def save_geocache(cache):
    with open(GEOCACHE_PATH, 'w', encoding='utf-8') as f:
        json.dump(cache, f, indent=2, ensure_ascii=False)

# ── Address collection ──────────────────────────────────────────────────────────

def collect_properties(only_source=None):
    """
    Return a list of (address, url, source_key) for every property JSON.
    Deduplicates by address: each unique address appears only once.
    """
    seen = set()
    results = []
    sources_to_run = {k: v for k, v in SOURCES.items()
                      if not only_source or k == only_source}

    for source, rel_dir in sources_to_run.items():
        src_dir = os.path.join(ROOT, rel_dir)
        if not os.path.isdir(src_dir):
            continue
        dirs = sorted(
            [d for d in os.listdir(src_dir)
             if d.startswith('property_') and os.path.isdir(os.path.join(src_dir, d))],
            key=lambda x: int(x.replace('property_', ''))
        )
        for d in dirs:
            jpath = os.path.join(src_dir, d, f'{d}.json')
            if not os.path.exists(jpath):
                continue
            try:
                data = json.load(open(jpath, encoding='utf-8'))
            except Exception:
                continue
            addr = (data.get('address') or data.get('title') or '').strip()
            url  = data.get('url') or ''
            if addr and addr not in seen:
                seen.add(addr)
                results.append((addr, url, source))

    return results

# ── URL town extraction ─────────────────────────────────────────────────────────

def town_from_url(url):
    """
    Extract a human-readable town name from an NI estate agent property URL.

    Supported patterns:
      Simon Brien:      /buy/{type}/{town-slug}/{address-slug}
      John Minnis:      /property/{town-slug}/{id}/{address-slug}
      Hunter Campbell:  /property/{town-slug}/{id}/{address-slug}
      PropertyPal:      /property-for-sale/{id}/{address-slug}   (no town segment)
      TR:               /property/{type-id}/{address-slug}        (no town segment)
    """
    if not url:
        return ''
    try:
        path = urlparse(url).path.rstrip('/')
        segments = [s for s in path.split('/') if s]
        host = urlparse(url).netloc.lower()

        town_slug = ''

        if 'simonbrien.com' in host:
            # /buy/{type}/{town}/{addr}  → index 2 relative to /buy/
            if len(segments) >= 4 and segments[0] == 'buy':
                town_slug = segments[2]

        elif ('johnminnis.co.uk' in host or 'huntercampbell.co.uk' in host
              or 'ulsterpropertysales.co.uk' in host):
            # /property/{town}/{id}/{addr}  → index 1 relative to /property/
            if len(segments) >= 2 and segments[0] == 'property':
                town_slug = segments[1]

        if town_slug:
            # Fix known URL slug typos before converting to title-case
            SLUG_FIXES = {
                'hilsborough':     'hillsborough',
                'newtonabbey':     'newtownabbey',
                'portruch':        'portrush',
                'portrusch':       'portrush',
            }
            town_slug = SLUG_FIXES.get(town_slug.lower(), town_slug)
            # Convert slug to title-case town name
            # e.g. "belfast-city-centre" → "Belfast City Centre"
            # e.g. "hillsborough" → "Hillsborough"
            town = town_slug.replace('-', ' ').title()
            return town

    except Exception:
        pass
    return ''

# ── Address candidate building ─────────────────────────────────────────────────

_POSTCODE_RE = re.compile(r'\bBT\d{1,2}\s*\d[A-Z]{2}\b', re.IGNORECASE)

def extract_postcode(address):
    """Return a BT postcode if one appears in the address string."""
    m = _POSTCODE_RE.search(address)
    return m.group(0).upper() if m else None

def strip_prefix(address):
    """
    Remove new-build / apartment prefixes that Nominatim can't resolve:
      "Site 27, The Rose Rushfield"  → "The Rose Rushfield"
      "Apartment 9.12, The Arc"      → "The Arc"
      "Flat 6, Brent Lodge"          → "Brent Lodge"
      "Plot 3 Beech Hill"            → "Beech Hill"
    Uses [,\\s]+ so it handles both "Apartment 7 X" and "Apartment 7, X".
    """
    cleaned = re.sub(
        r'^(site\s+\d+[,\s]+|apartment\s+[\d.]+[,\s]+|apt\s+[\d.]+[,\s]+'
        r'|flat\s+[\d.]+[,\s]+|plot\s+\d+[,\s]+|unit\s+\d+[,\s]+)',
        '', address, flags=re.IGNORECASE
    ).strip().strip(',').strip()
    return cleaned if cleaned != address else ''


def _short_town(town):
    """
    Return just the first meaningful word(s) of a compound town/area name.
    "Belfast City Centre"          → "Belfast"
    "Lisburn City Centre"          → "Lisburn"
    "Lisburn Road Area"            → "Lisburn"
    "Newcastle Newry Mourne & Down"→ "Newcastle"
    "Ballyhackmore"                → "Belfast"  (mapped Belfast suburb)
    Single-word real towns return unchanged.
    """
    # Belfast suburbs/areas that Nominatim won't resolve — map to "Belfast"
    BELFAST_AREAS = {
        'ballyhackmore', 'finaghy', 'stranmillis', 'botanic', 'ormeau',
        'malone', 'andersonstown', 'falls', 'shankill', 'newtownbreda',
        'castlereagh', 'knock', 'stormont', 'belmont', 'bloomfield',
        'sydenham', 'dundonald',  # Dundonald borders east Belfast
        'upper ormeau', 'lower ormeau',
    }
    town_lower = town.lower()
    for area in BELFAST_AREAS:
        if area in town_lower:
            return 'Belfast'

    # Strip trailing location qualifiers that make Nominatim fail
    stop_words = {
        'city', 'centre', 'center', 'town', 'district',
        'road', 'area', 'north', 'south', 'east', 'west',
        'newry', 'mourne', 'down', '&',
    }
    words = town.split()
    kept = []
    for w in words:
        if w.lower() in stop_words:
            break
        kept.append(w)
    return ' '.join(kept) if kept else town


def strip_house_number(address):
    """
    Strip a leading house number from a street address so Nominatim can still
    match the street name:  "2 Coopers Mill Gardens" → "Coopers Mill Gardens"
    Returns empty string if address doesn't start with a number.
    """
    m = re.match(r'^\d+[a-zA-Z]?\s+(.*)', address)
    return m.group(1).strip() if m else ''


def town_from_address(address):
    """
    If a town name is already embedded as the last comma-separated segment,
    return it.  E.g.  "7 Muskett Court, Saintfield Road, Carryduff"  → "Carryduff"
    Only returns something if the last segment is a recognised NI place.
    """
    parts = [p.strip().strip(',').strip() for p in address.split(',')]
    # Try last two segments (sometimes "Saintfield Road, Carryduff")
    for seg in reversed(parts[-2:]):
        if not seg:
            continue
        seg_lower = seg.lower()
        # Match against known NI places (substring match to handle "Carryduff BT8")
        for place in NI_PLACES:
            if place in seg_lower:
                return seg.title()
    return ''


def build_candidates(address, url):
    """
    Build a list of geocoding query strings to try in order (most to least specific).
    """
    # Normalise: strip trailing commas/whitespace that are scraping artefacts
    address = re.sub(r'\s+', ' ', address).strip().strip(',').strip()

    town     = town_from_url(url) or town_from_address(address)
    short_t  = _short_town(town) if town else ''
    postcode = extract_postcode(address)
    stripped = strip_prefix(address)          # removes "Site X," / "Apartment X.Y" etc.
    no_num   = strip_house_number(address)    # removes leading house number

    ni = 'Northern Ireland, UK'
    candidates = []

    # 1. Full address + full town
    if town:
        candidates.append(f"{address}, {town}, {ni}")

    # 2. Full address + short town (e.g. "Lisburn" instead of "Lisburn City Centre")
    if short_t and short_t != town:
        candidates.append(f"{address}, {short_t}, {ni}")

    # 3. Full address + NI only
    candidates.append(f"{address}, {ni}")

    # 4. House-number-stripped + full/short town
    if no_num:
        if town:
            candidates.append(f"{no_num}, {town}, {ni}")
        if short_t and short_t != town:
            candidates.append(f"{no_num}, {short_t}, {ni}")
        candidates.append(f"{no_num}, {ni}")

    # 5. Prefix-stripped (removes "Site X,") + full/short town
    if stripped and stripped != address:
        if town:
            candidates.append(f"{stripped}, {town}, {ni}")
        if short_t and short_t != town:
            candidates.append(f"{stripped}, {short_t}, {ni}")
        candidates.append(f"{stripped}, {ni}")

    # 6. Postcode only — very reliable for UK if present
    if postcode:
        candidates.append(f"{postcode}, {ni}")

    # 7. Short town only — last resort so at least the map shows the right area
    if short_t:
        candidates.append(f"{short_t}, {ni}")
    elif town:
        candidates.append(f"{town}, {ni}")

    # 8. Absolute fallback: if URL contains "belfast" and we still have nothing,
    #    place the property in Belfast rather than leaving it unmapped.
    if 'belfast' in url.lower():
        candidates.append(f"Belfast, {ni}")

    # Deduplicate while preserving order
    seen = set()
    unique = []
    for c in candidates:
        key = re.sub(r'\s+', ' ', c).lower()
        if key not in seen:
            seen.add(key)
            unique.append(c)
    return unique

# ── Google Geocoding API ────────────────────────────────────────────────────────

def _google_query(query_string, retries=2):
    """
    Single Google Geocoding API query.
    Returns {'lat', 'lng'} or None.
    Only called when GOOGLE_API_KEY is set.
    """
    params = urlencode({
        'address':    query_string,
        'key':        GOOGLE_API_KEY,
        'region':     'GB',
        'components': 'country:GB',
    })
    url = f"https://maps.googleapis.com/maps/api/geocode/json?{params}"
    req = Request(url)

    for attempt in range(retries):
        try:
            with urlopen(req, timeout=15) as resp:
                data = json.loads(resp.read())
            if data.get('status') == 'OK' and data.get('results'):
                loc = data['results'][0]['geometry']['location']
                return {'lat': float(loc['lat']), 'lng': float(loc['lng'])}
            if data.get('status') == 'REQUEST_DENIED':
                print(f"  WARN: Google API key rejected — check GOOGLE_GEOCODING_API_KEY in .env")
                return None
            if data.get('status') == 'OVER_QUERY_LIMIT':
                print(f"  WARN: Google quota exceeded — switching to Nominatim for remainder")
                return None
            return None  # ZERO_RESULTS or other
        except URLError as e:
            if attempt < retries - 1:
                time.sleep(3)
        except Exception as e:
            if attempt < retries - 1:
                time.sleep(2)
    return None


# ── Nominatim lookup ───────────────────────────────────────────────────────────

def _nominatim_query(query_string, retries=2):
    """Single Nominatim query. Returns {'lat', 'lng'} or None."""
    params = urlencode({
        'q':             query_string,
        'format':        'json',
        'addressdetails': 0,
        'limit':         1,
        'countrycodes':  'gb',
    })
    url = f"https://nominatim.openstreetmap.org/search?{params}"
    req = Request(url, headers={'User-Agent': NOMINATIM_UA})

    for attempt in range(retries):
        try:
            with urlopen(req, timeout=15) as resp:
                data = json.loads(resp.read())
            if data:
                return {'lat': float(data[0]['lat']), 'lng': float(data[0]['lon'])}
            return None
        except URLError as e:
            if attempt < retries - 1:
                time.sleep(3)
        except Exception as e:
            if attempt < retries - 1:
                time.sleep(2)
    return None


def geocode_with_fallbacks(address, url, use_nominatim=True):
    """
    Try each candidate query string using Google (primary) then Nominatim (fallback).
    Returns (coords_or_None, query_used, provider_name).
    """
    candidates = build_candidates(address, url)

    # ── Google pass ────────────────────────────────────────────────────────────
    if GOOGLE_API_KEY:
        for i, query in enumerate(candidates):
            if i > 0:
                time.sleep(GOOGLE_DELAY)
            coords = _google_query(query)
            if coords:
                return coords, query, 'google'

    # ── Nominatim fallback ─────────────────────────────────────────────────────
    if use_nominatim:
        for i, query in enumerate(candidates):
            if i > 0:
                time.sleep(NOMINATIM_DELAY)
            else:
                # First Nominatim request: always sleep to respect their policy
                # (we may have just been hammering Google, not Nominatim)
                time.sleep(NOMINATIM_DELAY)
            coords = _nominatim_query(query)
            if coords:
                return coords, query, 'nominatim'

    return None, None, None


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description='Geocode NI property addresses via Google Geocoding API + Nominatim fallback.'
    )
    parser.add_argument('--source', default=None,
                        help='Only process a single source (sb, ups, hc, jm, pp, tr)')
    parser.add_argument('--limit', type=int, default=0,
                        help='Stop after this many new geocode requests (0 = unlimited)')
    parser.add_argument('--dry-run', action='store_true',
                        help='Print candidates without calling the API')
    parser.add_argument('--retry-failed', action='store_true',
                        help='Retry addresses previously saved as None (not-found)')
    parser.add_argument('--no-nominatim', action='store_true',
                        help='Skip Nominatim fallback — use Google only')
    args = parser.parse_args()

    if args.source and args.source not in SOURCES:
        print(f"Unknown source '{args.source}'. Valid: {list(SOURCES)}")
        sys.exit(1)

    if GOOGLE_API_KEY:
        print(f"Google Geocoding API: ✓ key loaded ({GOOGLE_API_KEY[:8]}…)")
    else:
        print("Google Geocoding API: ✗ no key — add GOOGLE_GEOCODING_API_KEY to .env")
        print("  Falling back to Nominatim only (slower, lower UK accuracy)")

    use_nominatim = not args.no_nominatim

    print("Loading geocache…")
    geocache = load_geocache()
    valid   = sum(1 for v in geocache.values() if v)
    failed  = sum(1 for v in geocache.values() if v is None)
    print(f"  {len(geocache):,} cached  ({valid:,} coords, {failed:,} marked not-found)")

    print("Collecting addresses from JSON files…")
    all_props = collect_properties(only_source=args.source)
    print(f"  {len(all_props):,} unique addresses found")

    # Filter: skip already-found; also skip already-failed unless --retry-failed
    to_geocode = []
    for addr, url, src in all_props:
        if addr not in geocache:
            to_geocode.append((addr, url, src))
        elif geocache[addr] is None and args.retry_failed:
            to_geocode.append((addr, url, src))

    print(f"  {len(to_geocode):,} need geocoding")

    if not to_geocode:
        print("Nothing to do — all addresses already processed.")
        print("Tip: use --retry-failed to retry addresses that previously returned no result.")
        return

    if args.dry_run:
        print("\n[DRY RUN] Candidates per address:")
        for addr, url, src in to_geocode[:10]:
            town = town_from_url(url)
            candidates = build_candidates(addr, url)
            print(f"  [{src}] {addr!r}")
            if town:
                print(f"    town from URL: {town!r}")
            for j, c in enumerate(candidates):
                print(f"    {j+1}. {c}")
        if len(to_geocode) > 10:
            print(f"  ... and {len(to_geocode) - 10} more")
        return

    found = not_found = 0
    google_hits = nominatim_hits = 0
    limit = args.limit or len(to_geocode)

    print(f"\nGeocoding up to {min(limit, len(to_geocode)):,} addresses…")
    print("(safe to interrupt — progress is saved after each address)\n")

    for i, (addr, url, src) in enumerate(to_geocode[:limit], 1):
        town = town_from_url(url)
        town_hint = f" [{town}]" if town else ""
        print(f"[{i}/{min(limit, len(to_geocode))}] {addr}{town_hint}")

        coords, query_used, provider = geocode_with_fallbacks(addr, url, use_nominatim=use_nominatim)

        if coords:
            geocache[addr] = coords
            print(f"  ✓  lat={coords['lat']:.5f}  lng={coords['lng']:.5f}  [{provider}] {query_used!r}")
            found += 1
            if provider == 'google':
                google_hits += 1
            else:
                nominatim_hits += 1
        else:
            geocache[addr] = None   # sentinel: don't retry next run (use --retry-failed)
            print(f"  ✗  not found after all fallbacks")
            not_found += 1

        save_geocache(geocache)

        # Small delay between addresses when using Google only
        # (Nominatim delay is handled inside geocode_with_fallbacks)
        if GOOGLE_API_KEY and use_nominatim is False and i < min(limit, len(to_geocode)):
            time.sleep(GOOGLE_DELAY)

    print(f"\n{'='*55}")
    print(f"Done.  found={found}  (google={google_hits}, nominatim={nominatim_hits})  not_found={not_found}")
    valid_now = sum(1 for v in geocache.values() if v)
    print(f"geocache.json now has {valid_now:,} valid coordinates")
    if not_found:
        print(f"  {not_found} addresses couldn't be found — use --retry-failed to retry them")
    print(f"\nNext step: python3 supabase/migrate_data.py   (push lat/lng to Supabase)")


if __name__ == '__main__':
    main()
