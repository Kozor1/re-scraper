"""Geocode + patch DB rows that are pinned at centroid hotspots but whose
addresses never made it into the geocache."""
import json
import re
import sys
import collections

sys.path.insert(0, ".")
from config import get_supabase
from geocode import geocode_with_fallbacks

cache = json.load(open("geocache.json"))
counts = collections.Counter((v["lat"], v["lng"]) for v in cache.values() if v)
hotspots = {pt for pt, c in counts.items() if c >= 8}


def norm(a):
    return re.sub(r",[ ]*", " ", (a or "").strip()).strip()


sb = get_supabase()
rows, off = [], 0
while True:
    r = (
        sb.table("properties")
        .select("id,address,lat,lng")
        .not_.is_("lat", "null")
        .order("id")
        .range(off, off + 999)
        .execute()
    )
    rows += r.data
    if len(r.data) < 1000:
        break
    off += 1000

hot = [x for x in rows if (x["lat"], x["lng"]) in hotspots]
print(len(hot), "rows at hotspots", flush=True)
patched = 0
for i, x in enumerate(hot, 1):
    k = norm(x["address"])
    coords, q, prov, precise = geocode_with_fallbacks(k, "https://x", use_nominatim=False)
    if coords and precise:
        cache[k] = coords
        sb.table("properties").update({"lat": coords["lat"], "lng": coords["lng"]}).eq(
            "id", x["id"]
        ).execute()
        patched += 1
    if i % 50 == 0:
        print(f"  {i}/{len(hot)} patched={patched}", flush=True)
        json.dump(cache, open("geocache.json", "w"), indent=1)

json.dump(cache, open("geocache.json", "w"), indent=1)
print("patched", patched, flush=True)
