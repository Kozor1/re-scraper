"""One-off DB backfill: re-clean descriptions still carrying legal boilerplate,
using the same clean_description() applied at migrate time going forward."""
import sys

sys.path.insert(0, ".")
from config import get_supabase
from config.supabase_utils import clean_description, _LEGAL_MARKERS

sb = get_supabase()
rows, off = [], 0
while True:
    r = (
        sb.table("properties")
        .select("id,source,description")
        .not_.is_("description", "null")
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
    desc = x["description"]
    if not _LEGAL_MARKERS.search(desc):
        continue
    cleaned = clean_description(desc)
    sb.table("properties").update({"description": cleaned or None}).eq(
        "id", x["id"]
    ).execute()
    patched += 1

print(f"patched {patched} descriptions")
