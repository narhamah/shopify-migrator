#!/usr/bin/env python3
"""Crawl the live Arabic storefront, find untranslated English strings,
match them to Shopify theme translation keys, and translate only what's visible.

This solves the ~3,400 key limit by only translating strings that actually
appear on the site, instead of blindly translating all theme keys.

Pipeline:
  1. Crawl the /ar site with Playwright → collect all visible English text
  2. Fetch theme translation keys from Shopify API
  3. Match scraped strings to theme keys (fuzzy + exact)
  4. Translate only matched keys → upload to Shopify

Prerequisites:
    pip install playwright
    playwright install chromium

Usage:
    # Step 1: Crawl + match + translate (full pipeline)
    python crawl_and_translate.py

    # Crawl only — save scraped strings to JSON
    python crawl_and_translate.py --crawl-only

    # Skip crawl — use previously saved scrape data
    python crawl_and_translate.py --skip-crawl

    # Dry run — match and show what would be translated, no uploads
    python crawl_and_translate.py --dry-run

    # Include checkout pages (requires a product in cart)
    python crawl_and_translate.py --include-checkout

    # Custom base URL
    python crawl_and_translate.py --base-url https://sa.taraformula.com

    # Control crawl depth
    python crawl_and_translate.py --max-pages 200

    # Override translation model
    python crawl_and_translate.py --model gpt-5-mini
"""
import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "src"))

from tara_migrate.tools.crawl_and_translate import main

if __name__ == "__main__":
    main()
