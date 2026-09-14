#!/usr/bin/env python3
"""
Test script to verify all scrapers are working correctly.
Tests each scraper by scraping 1 property and checking for valid output.

Uses the shared SOURCES config from config/sources.py.
"""

from __future__ import annotations

import os
import sys
import json
import subprocess
import tempfile
from datetime import datetime

ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, ROOT)
PYTHON_ENV = {**os.environ, "PYTHONPATH": ROOT}

from config import SOURCES, all_sale_keys

# Build scraper list from shared config.
# Expected fields: address, price_str (standardized), image_urls (standardized)
EXPECTED_FIELDS = ["address", "image_urls"]

SCRAPERS: list[tuple[str, str]] = []
for key in all_sale_keys():
    cfg = SOURCES[key]
    module = cfg["module"]
    script_name = f"{module}.py"
    # Only include if the scraper file exists
    script_path = os.path.join(ROOT, "scrapers", script_name)
    if os.path.isfile(script_path):
        SCRAPERS.append((script_name, key))

print(f"Found {len(SCRAPERS)} scrapers to test")


def test_scraper(
    scraper_file: str, source_key: str, timeout: int = 120
) -> bool:
    """Test a single scraper"""
    scraper_name = source_key.upper()
    scraper_path = os.path.join(ROOT, "scrapers", scraper_file)

    print(f"\n{'=' * 60}")
    print(f"Testing {scraper_name}...")
    print(f"{'=' * 60}")

    original_dir = os.getcwd()
    try:
        result = subprocess.run(
            [sys.executable, scraper_path, "--limit", "1", "--fresh"],
            capture_output=True,
            text=True,
            timeout=timeout,
            cwd=ROOT,
            env=PYTHON_ENV,
        )

        if result.returncode != 0:
            print(f"  Status: FAILED (exit code {result.returncode})")
            print(f"  Error: {result.stderr[:200]}")
            return False

        # Find the property directory output
        props_dir = SOURCES[source_key]["props_dir"]
        if not os.path.isdir(props_dir):
            print(f"  Status: FAILED - No output dir: {props_dir}")
            return False

        prop_dirs = sorted(
            [
                d
                for d in os.listdir(props_dir)
                if d.startswith("property_")
                and os.path.isdir(os.path.join(props_dir, d))
            ],
            key=lambda x: int(x.replace("property_", "")),
        )
        if not prop_dirs:
            print("  Status: FAILED - No property directory created")
            return False

        prop_dir = os.path.join(props_dir, prop_dirs[0])
        json_files = [
            f for f in os.listdir(prop_dir) if f.endswith(".json")
        ]

        if not json_files:
            print("  Status: FAILED - No JSON file created")
            return False

        json_path = os.path.join(prop_dir, json_files[0])
        with open(json_path, encoding="utf-8") as f:
            data = json.load(f)

        # Check expected fields
        missing = [f for f in EXPECTED_FIELDS if f not in data]
        empty = [
            f
            for f in EXPECTED_FIELDS
            if f in data
            and not data[f]
            and (f != "image_urls" or len(data[f]) == 0)
        ]

        if missing:
            print(f"  Status: FAILED - Missing fields: {missing}")
            return False

        if empty:
            print(f"  Status: WARNING - Empty fields: {empty}")

        print("  Status: SUCCESS")
        print(
            f"  Address: {str(data.get('address', 'N/A'))[:50]}..."
        )
        print(
            f"  Price: {data.get('price_str', data.get('price', 'N/A'))}"
        )
        print(f"  Bedrooms: {data.get('bedrooms', 'N/A')}")
        print(f"  Images: {len(data.get('image_urls', data.get('images', [])))}")

        return True

    except subprocess.TimeoutExpired:
        print(f"  Status: FAILED - Timeout after {timeout}s")
        return False
    except Exception as e:
        print(f"  Status: FAILED - {e}")
        return False


def main() -> None:
    print("\n" + "=" * 60)
    print("SCRAPER VERIFICATION TEST")
    print("Testing all scrapers with --limit 1")
    print("=" * 60)

    results: dict[str, bool] = {}
    for scraper_file, source_key in SCRAPERS:
        success = test_scraper(scraper_file, source_key)
        results[scraper_file] = success

    passed = sum(1 for v in results.values() if v)
    failed = len(results) - passed

    print("\n" + "=" * 60)
    print("TEST SUMMARY")
    print("=" * 60)
    print(f"Total: {len(results)}")
    print(f"Passed: {passed}")
    print(f"Failed: {failed}")

    if failed > 0:
        print("\nFailed scrapers:")
        for scraper, success in results.items():
            if not success:
                print(f"  - {scraper}")

    print("\n" + "=" * 60)
    sys.exit(0 if failed == 0 else 1)


if __name__ == "__main__":
    main()
