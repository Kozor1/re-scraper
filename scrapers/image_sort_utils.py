"""
image_sort_utils.py
-------------------
Shared utility for sorting and deduplicating property image URL lists.

Every broker that uses a JS carousel (SB, HC, TR, UPS, JM, PP) places a few
images at the wrong position in the gallery HTML for seamless infinite looping.
Sorting by the trailing numeric suffix restores the intended display order.

URL patterns handled:
  Simon Brien  : …/NBF260058_01.webp      →  _(\d+)\.(ext)
  Hunter Campbell: …/hclc2_…_img_00.jpg  →  _(\d+)\.(ext)
  John Minnis  : …/19055535-1.jpg         →  -(\d+)\.(ext)
  Templeton R. : …/trltrl102720-1.jpg     →  -(\d+)\.(ext)
  UPS          : …/34557628-1.jpg         →  -(\d+)\.(ext)
  Property People: …/pq7755-1.png         →  [_-](\d+)\.(ext)

MM and CE use PropertyPal CDN hash filenames (e.g. 40019546.jpg) — no numeric
suffix is available, so DOM insertion order is relied on instead (handled in
those scrapers directly).

CLI usage
---------
Re-sort image_urls in every JSON file under a source directory:

    python3 image_sort_utils.py --source sb
    python3 image_sort_utils.py --source hc jm tr ups pp
    python3 image_sort_utils.py --all            # every source with numbered images
"""

import re
import json
import glob
import os
import logging
import argparse

logger = logging.getLogger(__name__)

# Sources whose images carry a reliable numeric suffix.
# NOTE: 'sb' is intentionally excluded — Simon Brien's CMS reserves low numeric
# slots for video embeds and assigns a high suffix (e.g. _11) to the hero/exterior
# shot.  Sorting by suffix would push the hero image to the end of the gallery.
# The SB scraper preserves DOM insertion order instead, which keeps the hero first.
NUMBERED_SOURCES = ['hc', 'jm', 'tr', 'ups', 'pp', 'dh']

# Default properties root (relative to this file's directory, i.e. re_app/scrapers/../properties)
_SCRAPERS_DIR = os.path.dirname(os.path.abspath(__file__))
_PROPERTIES_DIR = os.path.join(_SCRAPERS_DIR, '..', 'properties')


def sort_and_dedup(urls: list[str]) -> list[str]:
    """Return a deduplicated, numerically-sorted copy of *urls*.

    URLs that contain no recognisable numeric suffix are placed at the end in
    their original relative order so nothing is ever lost.
    """
    def _img_num(url: str) -> int:
        m = re.search(r'[_-](\d+)\.(jpg|jpeg|png|webp)$', url, re.IGNORECASE)
        return int(m.group(1)) if m else 999999

    seen: set[str] = set()
    deduped: list[str] = []
    for url in urls:
        if url not in seen:
            seen.add(url)
            deduped.append(url)

    deduped.sort(key=_img_num)
    return deduped


# ---------------------------------------------------------------------------
# Batch fix helpers (CLI / post-scrape use)
# ---------------------------------------------------------------------------

def fix_property_file(json_path: str) -> bool:
    """Re-sort image_urls in a single property JSON file.

    Returns True if the file was changed, False if it was already correct or
    had no image_urls.
    """
    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        logger.warning(f"Could not read {json_path}: {e}")
        return False

    original = data.get('image_urls') or []
    if not original:
        return False

    sorted_urls = sort_and_dedup(original)
    if sorted_urls == original:
        return False  # already correct

    data['image_urls'] = sorted_urls
    try:
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        return True
    except Exception as e:
        logger.warning(f"Could not write {json_path}: {e}")
        return False


def fix_source_dir(source_key: str, properties_root: str = _PROPERTIES_DIR) -> dict:
    """Re-sort image_urls in every JSON file under *source_key*'s directory.

    Returns a dict with keys 'fixed', 'unchanged', 'total'.
    """
    source_dir = os.path.join(properties_root, source_key)
    if not os.path.isdir(source_dir):
        raise FileNotFoundError(f"Source directory not found: {source_dir}")

    json_files = sorted(glob.glob(os.path.join(source_dir, '**', '*.json'), recursive=True))
    fixed = unchanged = 0
    for path in json_files:
        if fix_property_file(path):
            fixed += 1
        else:
            unchanged += 1

    return {'fixed': fixed, 'unchanged': unchanged, 'total': fixed + unchanged}


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main():
    logging.basicConfig(level=logging.INFO, format='%(levelname)s  %(message)s')

    parser = argparse.ArgumentParser(
        description='Re-sort image_urls in property JSON files by numeric suffix.'
    )
    parser.add_argument(
        '--source', nargs='+', metavar='KEY',
        help=f'One or more source keys to fix (e.g. sb hc jm). '
             f'Numbered sources: {", ".join(NUMBERED_SOURCES)}'
    )
    parser.add_argument(
        '--all', action='store_true',
        help=f'Fix all numbered sources: {", ".join(NUMBERED_SOURCES)}'
    )
    parser.add_argument(
        '--properties-dir', default=_PROPERTIES_DIR, metavar='DIR',
        help='Root properties directory (default: ../properties relative to this script)'
    )
    args = parser.parse_args()

    if not args.source and not args.all:
        parser.print_help()
        return

    sources = NUMBERED_SOURCES if args.all else args.source

    grand_fixed = grand_total = 0
    for key in sources:
        try:
            result = fix_source_dir(key, args.properties_dir)
        except FileNotFoundError as e:
            logger.warning(str(e))
            continue
        logger.info(
            f"  {key.upper():<6}  fixed={result['fixed']}  "
            f"unchanged={result['unchanged']}  total={result['total']}"
        )
        grand_fixed += result['fixed']
        grand_total  += result['total']

    logger.info(f"\nDone — fixed {grand_fixed}/{grand_total} JSON files.")


if __name__ == '__main__':
    main()
