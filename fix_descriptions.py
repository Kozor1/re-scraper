#!/usr/bin/env python3
"""
fix_descriptions.py
===================
Post-processing script to repair run-on descriptions in the Supabase
properties table.

The root cause: BeautifulSoup's get_text(strip=True) collapses all HTML
whitespace and paragraph breaks into a single continuous string, so text that
was in separate <p> tags ends up concatenated without any separator.

This script applies a set of heuristic regex rules to reinsert paragraph /
line breaks where sentence boundaries were lost, then pushes the fixed
descriptions back to Supabase.

Usage
-----
  cd swome-scraper
  python fix_descriptions.py [--dry-run] [--limit N]

Flags
-----
  --dry-run   Print proposed changes without writing to the database.
  --limit N   Only process the first N properties (useful for spot-checking).

Requirements
------------
  pip install supabase python-dotenv --break-system-packages
"""

import argparse
import os
import re
import sys
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
    sys.exit("ERROR: SUPABASE_URL and SUPABASE_SERVICE_KEY must be set in .env")

from supabase import create_client, Client  # noqa: E402  (import after env check)

supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)


# ─────────────────────────────────────────────────────────────────────────────
# Text-fixing logic
# ─────────────────────────────────────────────────────────────────────────────

def fix_description(text: str) -> str:
    """
    Apply heuristic rules to reinsert paragraph breaks in a property
    description that was stripped of its HTML structure.

    Rules applied (in order):
    1. Sentence-end + immediate capital letter  →  .\n\n
       e.g. "...great location.The property..."  →  "...great location.\n\nThe property..."
    2. Exclamation / question mark + capital    →  !\n\n  or  ?\n\n
    3. Colon + capital letter (room headings)   →  :\n\n
       e.g. "...en-suite bathroom:Bedroom 2..."  →  "...\n\nBedroom 2..."
    4. All-uppercase room labels (e.g. KITCHEN, ENTRANCE HALL) that are
       immediately preceded by lower-case text  →  \n\n before label
    5. Collapse 3+ consecutive newlines to 2.
    6. Strip leading/trailing whitespace.
    """
    if not text:
        return text

    # Rule 1: period followed immediately by a capital letter (no space)
    # Avoid matching abbreviations like "sq.ft." by requiring the char before
    # the period to NOT be a single uppercase letter (e.g. "Mr.Smith").
    # Lookahead for capital or digit at start of new sentence.
    text = re.sub(r'(?<=[a-z0-9,\'"])\.(?=[A-Z])', '.\n\n', text)

    # Rule 2: exclamation/question mark immediately before capital letter
    text = re.sub(r'([!?])(?=[A-Z])', r'\1\n\n', text)

    # Rule 3: colon immediately before capital letter (room headings)
    # e.g. "...bathroom:Bedroom Two..." → "...bathroom:\n\nBedroom Two..."
    text = re.sub(r':(?=[A-Z][a-z])', ':\n\n', text)

    # Rule 4: ALL-CAPS word of 4+ chars preceded by lowercase text
    # e.g. "...lovely home.LOUNGE..." → "...lovely home.\n\nLOUNGE..."
    text = re.sub(r'(?<=[a-z.!?])([A-Z]{4,}(?:\s+[A-Z]+)*)', r'\n\n\1', text)

    # Rule 5: Collapse excessive blank lines
    text = re.sub(r'\n{3,}', '\n\n', text)

    return text.strip()


def descriptions_differ(original: str, fixed: str) -> bool:
    """Return True only if the fix makes a meaningful change."""
    return original.strip() != fixed.strip()


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description='Fix run-on property descriptions in Supabase')
    parser.add_argument('--dry-run', action='store_true',
                        help='Print changes without writing to the database')
    parser.add_argument('--limit', type=int, default=None,
                        help='Only process the first N properties')
    args = parser.parse_args()

    print("Fetching properties from Supabase...")

    # Fetch in batches to avoid the default 1,000-row limit
    batch_size = 1000
    offset = 0
    all_properties = []

    while True:
        resp = (
            supabase.table('properties')
            .select('id, description')
            .not_.is_('description', 'null')
            .range(offset, offset + batch_size - 1)
            .execute()
        )
        batch = resp.data or []
        all_properties.extend(batch)
        if len(batch) < batch_size:
            break
        offset += batch_size

    print(f"Fetched {len(all_properties)} properties with descriptions.")

    if args.limit:
        all_properties = all_properties[:args.limit]
        print(f"Limiting to first {args.limit} properties.")

    changed = 0
    skipped = 0
    errors = 0

    for prop in all_properties:
        prop_id = prop['id']
        original = prop.get('description', '') or ''

        fixed = fix_description(original)

        if not descriptions_differ(original, fixed):
            skipped += 1
            continue

        changed += 1

        if args.dry_run:
            print(f"\n{'='*60}")
            print(f"Property ID: {prop_id}")
            print(f"--- BEFORE ---")
            print(repr(original[:300]))
            print(f"--- AFTER  ---")
            print(repr(fixed[:300]))
        else:
            try:
                supabase.table('properties').update(
                    {'description': fixed}
                ).eq('id', prop_id).execute()
            except Exception as e:
                print(f"ERROR updating property {prop_id}: {e}")
                errors += 1

    mode = "DRY RUN" if args.dry_run else "LIVE"
    print(f"\n[{mode}] Done. {changed} updated, {skipped} unchanged, {errors} errors.")


if __name__ == '__main__':
    main()
