"""Apply repaired geocache coords to DB rows still pinned at centroid points."""
import json
import re
import sys
import collections

sys.path.insert(0, ".")
from config import get_supabase

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
        .order("id")
        .range(off, off + 999)
        .execute()
    )
    rows += r.data
    if len(r.data) < 1000:
        break
    off += 1000

patched = skipped = 0
by_source = collections.Counter()
for x in rows:
    lat, lng = x.get("lat"), x.get("lng")
    if lat is None or (lat, lng) not in hotspots:
        continue
    v = cache.get(norm(x.get("address")))
    if not v:
        skipped += 1
        continue
    sb.table("properties").update({"lat": v["lat"], "lng": v["lng"]}).eq(
        "id", x["id"]
    ).execute()
    patched += 1

print(f"patched {patched}, no-cache-key {skipped}")
