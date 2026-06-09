#!/usr/bin/env python3
"""Build the Saudi Shopify store end-to-end after data export + scraping.

Single entry point that orchestrates ALL build steps: translate → import →
images → configure. Supports building English only, Arabic only, or both.

Prerequisites:
    1. export_source.py      — export source data
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
from datetime import datetime, timezone

from dotenv import load_dotenv

from tara_migrate.client import ShopifyClient
from tara_migrate.core import config
from tara_migrate.core.config import ConfigError
from tara_migrate.core.logging import add_run_log_file, get_logger
from tara_migrate.core.preflight import PreflightError, run_preflight
from tara_migrate.core.run_manifest import RunManifest, hash_paths, hash_values

logger = get_logger(__name__)


class PhaseError(Exception):
    """Raised when a build phase (including a subprocess phase) fails."""


def run_subprocess(cmd, name):
    """Run a subprocess phase, failing loudly on a non-zero exit code.

    Replaces the old ``subprocess.run(cmd, check=False)`` calls that discarded
    exit codes and let a crashed phase masquerade as success.
    """
    result = subprocess.run(cmd)
    if result.returncode != 0:
        raise PhaseError(f"{name} failed with exit code {result.returncode}")


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

    from tara_migrate.translation.translate_gaps import EN_DIR, SOURCE_DIR, translate_with_gaps

    translate_with_gaps(
        source_dir=SOURCE_DIR,
        output_dir=EN_DIR,
        source_lang="Spanish",
        target_lang="English",
        lang_code="en",
        dry=dry_run,
    )


# =========================================================================
# Phase 2: Fix SAR Prices
# =========================================================================

@phase(2, "Fix Magento Prices",
       "Fetch correct prices from Magento and update local data + Shopify",
       langs=("en", "all"))
def phase_fix_prices(dry_run=False, **kw):
    print("\n" + "=" * 60)
    print("PHASE 2: Fix Magento Prices")
    print("=" * 60)

    from tara_migrate.core import AR_DIR, EN_DIR, save_json
    from tara_migrate.fixers.fix_prices import fetch_magento_prices, update_product_files, update_shopify_products

    site_url = config.get_magento_site_url()
    store_code = config.get_magento_store_code()
    prices = fetch_magento_prices(site_url, store_code)
    if not prices:
        # Nothing to do is not a failure, but surface it loudly.
        print("  WARNING: No prices fetched from Magento — skipping price update")
        return

    save_json(prices, config.get_progress_file("magento_prices.json"))
    print(f"  Fetched prices for {len(prices)} SKUs")

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


# =========================================================================
# Phase 3: Import English Content
# =========================================================================

@phase(3, "Import English Content",
       "Create all resources (products, collections, pages, metaobjects) in destination store",
       langs=("en", "all"))
def phase_import_english(dry_run=False, **kw):
    print("\n" + "=" * 60)
    print("PHASE 3: Import English Content")
    print("=" * 60)

    cmd = [sys.executable, "import_english.py"]
    if dry_run:
        cmd.append("--dry-run")
    run_subprocess(cmd, "Import English")


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

    from tara_migrate.translation.translate_gaps import AR_DIR, EN_DIR, translate_with_gaps

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
       "Register Arabic as secondary locale on all destination store resources",
       langs=("ar", "all"))
def phase_import_arabic(dry_run=False, **kw):
    print("\n" + "=" * 60)
    print("PHASE 5: Import Arabic Translations")
    print("=" * 60)

    cmd = [sys.executable, "import_arabic.py"]
    if dry_run:
        cmd.append("--dry-run")
    run_subprocess(cmd, "Import Arabic")


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

    from tara_migrate.core import FILE_MAP_FILE, load_json, save_json
    from tara_migrate.pipeline.migrate_all_images import (
        phase1_product_images,
        phase2_collection_images,
        phase3_homepage_images,
        phase4_product_metafield_files,
        phase5_metaobject_files,
        phase6_article_files,
        phase7_verify,
    )

    id_map = load_json(config.get_id_map_file()) if os.path.exists(config.get_id_map_file()) else {}
    file_map = load_json(FILE_MAP_FILE) if os.path.exists(FILE_MAP_FILE) else {}
    if not isinstance(file_map, dict):
        file_map = {}

    image_phases = [
        phase1_product_images,
        phase2_collection_images,
        phase3_homepage_images,
        phase4_product_metafield_files,
        phase5_metaobject_files,
        phase6_article_files,
        phase7_verify,
    ]

    failures = []
    for img_phase in image_phases:
        try:
            img_phase(spain, saudi, id_map, file_map, dry_run=dry_run)
        except Exception as e:
            logger.error("  ERROR in %s: %s", img_phase.__name__, e)
            failures.append(f"{img_phase.__name__}: {e}")

    # Persist partial progress regardless, then fail the phase if any sub-phase broke.
    save_json(file_map, FILE_MAP_FILE)
    if failures:
        raise PhaseError("image sub-phases failed: " + "; ".join(failures))


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
    run_subprocess(cmd, "Resolve Metaobject Diffs")


# =========================================================================
# Phase 8: Post-Migration Setup
# =========================================================================

@phase(8, "Post-Migration Setup",
       "Locale, collections, menus, SEO, redirects, inventory, publish, handles, theme sync",
       langs=("en", "all"))
def phase_post_migration(dry_run=False, **kw):
    print("\n" + "=" * 60)
    print("PHASE 8: Post-Migration Setup (12 sub-steps)")
    print("=" * 60)

    cmd = [sys.executable, "post_migration.py"]
    lang = kw.get("lang")
    if lang:
        cmd.extend(["--lang", lang])
    if dry_run:
        cmd.append("--dry-run")
    run_subprocess(cmd, "Post-Migration Setup")


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
  3  Import English       [en]   Create resources in destination store
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
    parser.add_argument("--config", type=str, default=None,
                        help="Path to a declarative destination TOML "
                             "(destinations/<name>.toml). Sets SOURCE/DEST/MAGENTO/DEST_NAME.")
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
    parser.add_argument("--resume", action="store_true",
                        help="Skip phases already marked completed in the run manifest "
                             "(only when config + source export are unchanged)")
    parser.add_argument("--keep-going", action="store_true",
                        help="Continue to later phases even if one fails "
                             "(default: stop on the first failure)")
    parser.add_argument("--skip-preflight", action="store_true",
                        help="Skip config/scope/connectivity validation (not recommended)")
    args = parser.parse_args()

    # A declarative destination file populates the env the pipeline reads.
    if args.config:
        from tara_migrate.core.config_schema import apply_to_env, load_destination_config
        try:
            cfg = load_destination_config(args.config)
            apply_to_env(cfg)
            print(f"Loaded destination config: {cfg.name} ({args.config})")
        except Exception as e:
            print(f"ERROR: could not load --config {args.config}: {e}")
            sys.exit(1)

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

    # ---- Preflight: validate config, tokens, scopes, connectivity ----
    require_magento = 2 in phases_to_run
    if not args.skip_preflight:
        try:
            result = run_preflight(
                require_magento=require_magento,
                check_connectivity=not args.dry_run,
                check_scopes=not args.dry_run,
            )
            print(result.summary())
            for w in result.warnings:
                logger.warning("Preflight warning: %s", w)
        except (ConfigError, PreflightError) as e:
            print("\nPREFLIGHT FAILED:\n" + str(e))
            sys.exit(1)
    else:
        # Even with preflight skipped, we cannot run without connection config.
        missing = config.missing_required(require_magento=require_magento)
        if missing:
            print("ERROR: Missing required configuration:\n  - " + "\n  - ".join(missing))
            sys.exit(1)

    # Connect to stores
    saudi = ShopifyClient(config.get_dest_shop_url(), config.get_dest_access_token())
    spain = ShopifyClient(config.get_source_shop_url(), config.get_source_access_token())

    # ---- Run manifest (truthful status + resume) ----
    dest_name = config.get_dest_name() or "default"
    manifest = None
    manifest_path = config.get_progress_file("run_manifest.json")
    if not args.dry_run:
        config_hash = hash_values([
            config.get_dest_shop_url(), config.get_source_shop_url(),
            config.get_magento_store_code(), dest_name, lang,
        ])
        source_hash = hash_paths([config.SOURCE_DIR])
        manifest = RunManifest.load_or_create(
            manifest_path, destination=dest_name,
            config_hash=config_hash, source_export_hash=source_hash)
        build_id = datetime.now(timezone.utc).replace(microsecond=0).isoformat() + "-" + dest_name
        manifest.begin_run(build_id)
        add_run_log_file(config.get_progress_file("logs/build.log"))

    print("=" * 60)
    print(f"BUILD SITE: {dest_name} ({lang.upper()})")
    print("=" * 60)
    print(f"  Language: {lang}")
    print(f"  Mode:     {'DRY RUN' if args.dry_run else 'LIVE'}")
    print(f"  Resume:   {'on' if args.resume else 'off'}    Fail-fast: {'off' if args.keep_going else 'on'}")
    print(f"  Phases:   {phases_to_run}")
    for p in phases_to_run:
        name, _, desc, _ = PHASES[p]
        print(f"    {p}. {name} - {desc}")
    print()

    start_time = time.time()
    failed_phases = []

    for phase_num in phases_to_run:
        name, func, desc, _ = PHASES[phase_num]
        phase_key = f"phase_{phase_num}"

        if args.resume and manifest and manifest.is_completed(phase_key):
            print(f"\n  Phase {phase_num} ({name}) already completed - skipping (--resume)")
            continue

        phase_start = time.time()
        if manifest:
            manifest.start_phase(phase_key)

        try:
            result = func(saudi=saudi, spain=spain, dry_run=args.dry_run, lang=lang)
            counts = result if isinstance(result, dict) else None
            if manifest:
                manifest.complete_phase(phase_key, counts=counts)
            elapsed = time.time() - phase_start
            print(f"\n  Phase {phase_num} completed in {elapsed:.0f}s")
        except KeyboardInterrupt:
            if manifest:
                manifest.fail_phase(phase_key, "interrupted", checkpoint={"resumable": True})
            print(f"\n  Interrupted during phase {phase_num} ({name})")
            print("  Re-run with --resume to continue")
            sys.exit(130)
        except Exception as e:
            if manifest:
                manifest.fail_phase(phase_key, e)
            failed_phases.append((phase_num, name, e))
            print(f"\n  ERROR in phase {phase_num} ({name}): {e}")
            if not args.keep_going:
                print("  Stopping (fail-fast). Fix the issue and re-run with --resume.")
                break
            print("  --keep-going set: continuing (downstream phases may be incomplete)")

    total = time.time() - start_time
    if manifest:
        # Fold the guided manual-step checklist (written by post-migration) into the manifest.
        ms_path = config.get_progress_file("manual_steps.json")
        if os.path.exists(ms_path):
            from tara_migrate.core import load_json
            manifest.set_manual_steps(load_json(ms_path, default=[]))
        status = manifest.end_run()
    else:
        status = "failed" if failed_phases else "completed"

    print(f"\n{'=' * 60}")
    if status == "failed" or failed_phases:
        print(f"BUILD FAILED - {dest_name} ({lang.upper()}, {total:.0f}s)")
        for num, nm, err in failed_phases:
            print(f"  - phase {num} {nm}: {err}")
        if manifest:
            print(f"  Manifest: {manifest_path}")
        print(f"{'=' * 60}")
        sys.exit(1)

    print(f"BUILD COMPLETE - {dest_name} ({lang.upper()}, {total:.0f}s)")
    if manifest:
        print(f"  Manifest: {manifest_path}")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
