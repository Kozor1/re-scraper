"""Repair centroid geocodes: cache entries snapped to a town-centre point.

Only entries whose address contains richer detail (a house number or BT
postcode) are retried; if the fresh geocode isn't precise the old value stays.
Then patches DB rows that currently sit at those old centroid points.
"""
import json
import sys
import re
import collections

sys.path.insert(0, ".")
from config import get_supabase
from geocode import geocode_with_fallbacks

cache = json.load(open("geocache.json"))

counts = collections.Counter((v["lat"], v["lng"]) for v in cache.values() if v)
hotspots = {pt for pt, c in counts.items() if c >= 8}
print("cluster points (>=8 entries):", {pt: counts[pt] for pt in hotspots})

# keys at hotspots with a digit or postcode in the address → worth re-geocoding
cand = [
    (k, v)
    for k, v in cache.items()
    if v and (v["lat"], v["lng"]) in hotspots and re.search(r"\d", k)
]
print(f"re-geocoding {len(cand)} centroid-suspect addresses…")

fixed = {}
kept = 0
for i, (addr, old) in enumerate(cand, 1):
    coords, query, provider, precise = geocode_with_fallbacks(addr, "https://x", use_nominatim=False)
    if coords and precise:
        fixed[addr] = coords
        cache[addr] = coords
    else:
        kept += 1
    if i % 100 == 0:
        print(f"  {i}/{len(cand)} ({len(fixed)} fixed)")
        json.dump(cache, open("geocache.json", "w"), indent=1)

json.dump(cache, open("geocache.json", "w"), indent=1)
print(f"fixed {len(fixed)}, kept-at-centroid {kept}")

# Patch DB rows whose address got a fixed geocode (and whose current coords
# were null or at a hotspot centroid).
sb = get_supabase()

rows, off = [], 0
while True:
    r = (
        sb.table("properties")
        .select("id,address,lat,lng")
        .order("id")
        .range(off, off + 999)
        .execute()
    )
    rows += r.data
    if len(r.data) < 1000:
        break
    off += 1000

patched = 0
for x in rows:
    addr = (x.get("address") or "").strip().rstrip(",")
    if addr in fixed:
        lat, lng = x.get("lat"), x.get("lng")
        if lat is None or (lat, lng) in hotspots or abs(lat - fixed[addr]["lat"]) > 1e-5:
            sb.table("properties").update(
                {"lat": fixed[addr]["lat"], "lng": fixed[addr]["lng"]}
            ).eq("id", x["id"]).execute()
            patched += 1
print("DB rows patched:", patched)
