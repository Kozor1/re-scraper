#!/usr/bin/env python3
"""Backfill UPS properties stuck with POA because Sale Agreed pages didn't
surface price through the previous selector. Reuses parse_pp_bluecubes_detail
which now includes the prop-det-price-amount fallback."""

from __future__ import annotations

import os, sys, time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config.supabase_utils import get_supabase
from scrapers.ups_full_scrape import UpsScraper

def main():
    supabase = get_supabase()
    rows = supabase.table("properties")\
        .select("id,url")\
        .eq("source", "ups")\
        .is_("price_value", None)\
        .execute().data or []
    print(f"Found {len(rows)} UPS rows missing price_value")
    scraper = UpsScraper("ups")
    updated = 0
    import re
    def parse_num(s: str) -> int | None:
        m = re.search(r"([\d,]+)", s.replace("&pound;", "£").replace("£", ""))
        return int(m.group(1).replace(",", "")) if m else None

    for i, row in enumerate(rows, 1):
        url = row["url"]
        try:
            r = scraper.fetch(url)
            if not r: continue
            data = scraper.scrape_detail_page(r.text, url)
            new_price = (data or {}).get("price_str") or (data or {}).get("price")
            if new_price:
                supabase.table("properties").update({
                    "price": new_price,
                    "price_value": parse_num(new_price),
                }).eq("id", row["id"]).execute()
                updated += 1
        except Exception as e:
            print(f"  error {url}: {e}")
        if i % 20 == 0:
            print(f"  {i}/{len(rows)}... {updated} updated")
        time.sleep(0.8)
    print(f"Done. Updated {updated}/{len(rows)}")

if __name__ == "__main__":
    main()
