#!/usr/bin/env python3
"""Build the Saudi Shopify store end-to-end after data export + scraping.

Single entry point that orchestrates ALL build steps: translate → import →
images → configure. Supports building English only, Arabic only, or both.

Prerequisites:
    1. export_spain.py      — export source data
    2. scrape_kuwait.py     — scrape EN/AR from Magento

Usage:
    python build_site.py                         # Build everything (EN + AR)
    python build_site.py --lang en               # English only
    python build_site.py --lang ar               # Arabic only (assumes EN already imported)
    python build_site.py --dry-run               # Preview all phases
    python build_site.py --phase 3               # Run only phase 3
    python build_site.py --phase 2,3,5           # Run specific phases
    python build_site.py --skip 1,7              # Run all except phases 1 and 7
    python build_site.py --from 4                # Run phases 4 onwards
    python build_site.py --lang en --from 3      # English, starting from import
"""

import argparse
import os
import subprocess
import sys
import time

from dotenv import load_dotenv


# Phase registry: number → (name, function, description, langs)
# langs: which --lang values include this phase ("en", "ar", "all")
PHASES = {}


def phase(num, name, description="", langs=("en", "ar", "all")):
    """Decorator to register a build phase."""
    def decorator(func):
        PHASES[num] = (name, func, description, set(langs))
        return func
    return decorator


# =========================================================================
# Phase 1: Translate to English
# =========================================================================

@phase(1, "Translate Spanish → English",
       "Scrape-first translation of Spain export to English (TOON batched)",
       langs=("en", "all"))
def phase_translate_english(dry_run=False, **kw):
    print("\n" + "=" * 60)
    print("PHASE 1: Translate Spanish → English")
    print("=" * 60)

    from translate_gaps import SPAIN_DIR, EN_DIR, translate_with_gaps

    translate_with_gaps(
        source_dir=SPAIN_DIR,
        output_dir=EN_DIR,
        source_lang="Spanish",
        target_lang="English",
        lang_code="en",
        dry=dry_run,
    )


# =========================================================================
# Phase 2: Fix SAR Prices
# =========================================================================

@phase(2, "Fix SAR Prices",
       "Fetch correct SAR prices from Magento and update local data + Shopify",
       langs=("en", "all"))
def phase_fix_prices(dry_run=False, **kw):
    print("\n" + "=" * 60)
    print("PHASE 2: Fix SAR Prices")
    print("=" * 60)

    try:
        from fix_prices import fetch_sar_prices, update_product_files, update_shopify_products
        from utils import save_json, EN_DIR, AR_DIR

        prices = fetch_sar_prices("https://taraformula.com", "sa-en")
        if not prices:
            print("  WARNING: No prices fetched from Magento")
            return

        save_json(prices, "data/sar_prices.json")
        print(f"  Fetched SAR prices for {len(prices)} SKUs")

        dirs = [EN_DIR]
        if os.path.exists(os.path.join(AR_DIR, "products.json")):
            dirs.append(AR_DIR)
        updated = update_product_files(prices, dirs)
        print(f"  Updated {updated} prices in local data files")

        if not dry_run:
            shopify_updated = update_shopify_products(prices)
            print(f"  Updated {shopify_updated} prices on Shopify")
        else:
            print("  DRY RUN: would update Shopify product prices")

    except Exception as e:
        print(f"  ERROR: {e}")
        print("  Continuing with remaining phases...")


# =========================================================================
# Phase 3: Import English Content
# =========================================================================

@phase(3, "Import English Content",
       "Create all resources (products, collections, pages, metaobjects) in Saudi store",
       langs=("en", "all"))
def phase_import_english(dry_run=False, **kw):
    print("\n" + "=" * 60)
    print("PHASE 3: Import English Content")
    print("=" * 60)

    cmd = [sys.executable, "import_english.py"]
    if dry_run:
        cmd.append("--dry-run")
    subprocess.run(cmd, check=False)


# =========================================================================
# Phase 4: Translate to Arabic
# =========================================================================

@phase(4, "Translate English → Arabic",
       "Scrape-first translation of English content to Arabic (TOON batched)",
       langs=("ar", "all"))
def phase_translate_arabic(dry_run=False, **kw):
    print("\n" + "=" * 60)
    print("PHASE 4: Translate English → Arabic")
    print("=" * 60)

    from translate_gaps import EN_DIR, AR_DIR, translate_with_gaps

    translate_with_gaps(
        source_dir=EN_DIR,
        output_dir=AR_DIR,
        source_lang="English",
        target_lang="Arabic",
        lang_code="ar",
        dry=dry_run,
    )


# =========================================================================
# Phase 5: Import Arabic Translations
# =========================================================================

@phase(5, "Import Arabic Translations",
       "Register Arabic as secondary locale on all Saudi store resources",
       langs=("ar", "all"))
def phase_import_arabic(dry_run=False, **kw):
    print("\n" + "=" * 60)
    print("PHASE 5: Import Arabic Translations")
    print("=" * 60)

    cmd = [sys.executable, "import_arabic.py"]
    if dry_run:
        cmd.append("--dry-run")
    subprocess.run(cmd, check=False)


# =========================================================================
# Phase 6: Migrate All Images
# =========================================================================

@phase(6, "Migrate All Images",
       "Upload product, collection, homepage, metaobject, and article images",
       langs=("en", "all"))
def phase_migrate_images(saudi, spain, dry_run=False, **kw):
    print("\n" + "=" * 60)
    print("PHASE 6: Migrate All Images")
    print("=" * 60)

    from migrate_all_images import (
        phase1_product_images, phase2_collection_images,
        phase3_homepage_images, phase4_metaobject_files,
        phase5_article_files, phase6_verify,
    )
    from utils import load_json, save_json, FILE_MAP_FILE

    id_map = load_json("data/id_map.json") if os.path.exists("data/id_map.json") else {}
    file_map = load_json(FILE_MAP_FILE) if os.path.exists(FILE_MAP_FILE) else {}
    if not isinstance(file_map, dict):
        file_map = {}

    image_phases = [
        phase1_product_images,
        phase2_collection_images,
        phase3_homepage_images,
        phase4_metaobject_files,
        phase5_article_files,
        phase6_verify,
    ]

    for img_phase in image_phases:
        try:
            img_phase(spain, saudi, id_map, file_map, dry_run=dry_run)
        except Exception as e:
            print(f"  ERROR in {img_phase.__name__}: {e}")
            print("  Continuing with next image phase...")

    save_json(file_map, FILE_MAP_FILE)


# =========================================================================
# Phase 7: Resolve Metaobject Diffs
# =========================================================================

@phase(7, "Resolve Metaobject Diffs",
       "Fix missing definitions, entries, and broken cross-references",
       langs=("en", "all"))
def phase_resolve_diffs(dry_run=False, **kw):
    print("\n" + "=" * 60)
    print("PHASE 7: Resolve Metaobject Diffs")
    print("=" * 60)

    cmd = [sys.executable, "resolve_metaobject_diffs.py"]
    if dry_run:
        cmd.append("--inspect")
    subprocess.run(cmd, check=False)


# =========================================================================
# Phase 8: Post-Migration Setup
# =========================================================================

@phase(8, "Post-Migration Setup",
       "Locale, collections, menus, SEO, redirects, inventory, publish, activate",
       langs=("en", "all"))
def phase_post_migration(dry_run=False, **kw):
    print("\n" + "=" * 60)
    print("PHASE 8: Post-Migration Setup (11 sub-steps)")
    print("=" * 60)

    cmd = [sys.executable, "post_migration.py"]
    if dry_run:
        cmd.append("--dry-run")
    subprocess.run(cmd, check=False)


# =========================================================================
# Main
# =========================================================================

def main():
    load_dotenv()

    parser = argparse.ArgumentParser(
        description="Build Saudi Shopify store end-to-end",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Phases (in execution order):
  1  Translate ES → EN    [en]   Scrape-first translation (TOON batched)
  2  Fix SAR Prices       [en]   Fetch Magento prices, update data + Shopify
  3  Import English       [en]   Create resources in Saudi store
  4  Translate EN → AR    [ar]   Scrape-first translation (TOON batched)
  5  Import Arabic        [ar]   Register translations via Translations API
  6  Migrate All Images   [en]   Product, collection, homepage, metaobject, article
  7  Resolve MO Diffs     [en]   Fix schema mismatches and broken references
  8  Post-Migration Setup [en]   Locale, collections, menus, SEO, redirects, publish

  [en] = runs with --lang en or --lang all
  [ar] = runs with --lang ar or --lang all

Examples:
  python build_site.py                       # Full build (EN + AR)
  python build_site.py --lang en             # English only (phases 1-3, 6-8)
  python build_site.py --lang ar             # Arabic only (phases 4-5)
  python build_site.py --lang en --from 3    # English, starting from import
  python build_site.py --dry-run             # Preview everything
  python build_site.py --phase 6             # Just migrate images
  python build_site.py --skip 2              # Skip price fix
""")
    parser.add_argument("--lang", choices=["en", "ar", "all"], default="all",
                        help="Language to build: en, ar, or all (default: all)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview all changes without executing")
    parser.add_argument("--phase", type=str, default=None,
                        help="Run specific phases only (e.g., '4' or '2,3,5')")
    parser.add_argument("--skip", type=str, default=None,
                        help="Skip specific phases (e.g., '1,7')")
    parser.add_argument("--from", type=int, default=None, dest="from_phase",
                        help="Start from this phase number (inclusive)")
    args = parser.parse_args()

    lang = args.lang

    # Determine which phases to run based on --lang
    all_phases = sorted(PHASES.keys())
    lang_phases = [p for p in all_phases if lang in PHASES[p][3]]

    if args.phase:
        phases_to_run = [int(p.strip()) for p in args.phase.split(",")]
    elif args.from_phase:
        phases_to_run = [p for p in lang_phases if p >= args.from_phase]
    else:
        phases_to_run = list(lang_phases)

    if args.skip:
        skip = {int(p.strip()) for p in args.skip.split(",")}
        phases_to_run = [p for p in phases_to_run if p not in skip]

    # Validate
    for p in phases_to_run:
        if p not in PHASES:
            print(f"ERROR: Unknown phase {p}. Valid phases: {list(PHASES.keys())}")
            sys.exit(1)

    # Connect to stores
    saudi_url = os.environ.get("SAUDI_SHOP_URL")
    saudi_token = os.environ.get("SAUDI_ACCESS_TOKEN")
    spain_url = os.environ.get("SPAIN_SHOP_URL")
    spain_token = os.environ.get("SPAIN_ACCESS_TOKEN")

    if not all([saudi_url, saudi_token, spain_url, spain_token]):
        print("ERROR: Set SPAIN_SHOP_URL, SPAIN_ACCESS_TOKEN, SAUDI_SHOP_URL, SAUDI_ACCESS_TOKEN in .env")
        sys.exit(1)

    from shopify_client import ShopifyClient
    saudi = ShopifyClient(saudi_url, saudi_token)
    spain = ShopifyClient(spain_url, spain_token)

    print("=" * 60)
    print(f"BUILD SITE: Saudi Shopify Store ({lang.upper()})")
    print("=" * 60)
    print(f"  Language: {lang}")
    print(f"  Mode:     {'DRY RUN' if args.dry_run else 'LIVE'}")
    print(f"  Phases:   {phases_to_run}")
    for p in phases_to_run:
        name, _, desc, _ = PHASES[p]
        print(f"    {p}. {name} — {desc}")
    print()

    start_time = time.time()

    for phase_num in phases_to_run:
        name, func, desc, _ = PHASES[phase_num]
        phase_start = time.time()

        try:
            func(saudi=saudi, spain=spain, dry_run=args.dry_run, lang=lang)
        except KeyboardInterrupt:
            print(f"\n  Interrupted during phase {phase_num} ({name})")
            print(f"  Re-run with --lang {lang} --from {phase_num} to resume")
            sys.exit(1)
        except Exception as e:
            print(f"\n  ERROR in phase {phase_num} ({name}): {e}")
            print(f"  Continuing... (re-run with --phase {phase_num} to retry)")

        elapsed = time.time() - phase_start
        print(f"\n  Phase {phase_num} completed in {elapsed:.0f}s")

    total = time.time() - start_time
    print(f"\n{'=' * 60}")
    print(f"BUILD COMPLETE — {lang.upper()} ({total:.0f}s)")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
