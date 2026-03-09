#!/usr/bin/env python3
"""Step 4: Translate English content to Arabic.

Uses TOON (Token-Oriented Object Notation) to batch translate all fields
in a single pipeline, reducing API calls by ~40x vs per-field translation.
Includes TARA tone of voice for brand-consistent translations.

Resumable: saves progress after each batch.

Usage:
    python translate_to_arabic.py              # Full translation
    python translate_to_arabic.py --dry        # Show what would be translated
    python translate_to_arabic.py --model o3   # Use a different model
"""

import argparse
import copy
import os
import sys
import time

from dotenv import load_dotenv
from openai import OpenAI

from translate_gaps import (
    EN_DIR,
    AR_DIR,
    BATCH_SIZE,
    TPM_LIMIT,
    TEXT_METAFIELD_TYPES,
    extract_product_fields,
    extract_collection_fields,
    extract_page_fields,
    extract_blog_fields,
    extract_article_fields,
    extract_metaobject_fields,
    apply_translations,
    adaptive_batch,
    translate_batch,
    load_json,
    save_json,
    _regenerate_metaobject_handles,
)


def main():
    load_dotenv()
    parser = argparse.ArgumentParser(description="Translate English content EN → AR using TOON batches")
    parser.add_argument("--dry", action="store_true", help="Dry run: show fields without calling API")
    parser.add_argument("--model", default="gpt-5-mini", help="OpenAI model (default: gpt-5-mini)")
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE, help=f"Fields per batch (default: {BATCH_SIZE})")
    parser.add_argument("--tpm", type=int, default=TPM_LIMIT, help=f"Tokens-per-minute budget (default: {TPM_LIMIT})")
    args = parser.parse_args()

    input_dir = EN_DIR
    output_dir = AR_DIR
    os.makedirs(output_dir, exist_ok=True)

    # Load English data
    products = load_json(os.path.join(input_dir, "products.json"))
    collections = load_json(os.path.join(input_dir, "collections.json"))
    pages = load_json(os.path.join(input_dir, "pages.json"))
    blogs = load_json(os.path.join(input_dir, "blogs.json"))
    articles = load_json(os.path.join(input_dir, "articles.json"))
    metaobjects = load_json(os.path.join(input_dir, "metaobjects.json"))

    if not products and not collections and not pages:
        print("ERROR: English data is empty. Run translate_to_english.py first.")
        sys.exit(1)

    print(f"{'=' * 60}")
    print(f"TRANSLATE ENGLISH → ARABIC (TOON batched)")
    print(f"{'=' * 60}")

    # ---- Extract ALL translatable fields ----
    all_fields = []

    for p in products:
        all_fields.extend(extract_product_fields(p, "prod"))
    for c in collections:
        all_fields.extend(extract_collection_fields(c, "coll"))
    for pg in pages:
        all_fields.extend(extract_page_fields(pg, "page"))
    for b in blogs:
        all_fields.extend(extract_blog_fields(b, "blog"))
    for a in articles:
        all_fields.extend(extract_article_fields(a, "art"))
    if isinstance(metaobjects, dict):
        all_fields.extend(extract_metaobject_fields(metaobjects, "mo"))

    # Filter out empty values
    all_fields = [f for f in all_fields if f.get("value") and f["value"].strip()]

    print(f"\n  Total fields to translate: {len(all_fields)}")

    # Breakdown
    field_types = {}
    for f in all_fields:
        category = f["id"].split(".")[0]
        field_types[category] = field_types.get(category, 0) + 1
    print(f"  Breakdown:")
    for cat, count in sorted(field_types.items()):
        print(f"    {cat}: {count} fields")

    if args.dry:
        print(f"\n  DRY RUN — no API calls made")
        print(f"\n  Sample fields (first 10):")
        for f in all_fields[:10]:
            val = f["value"][:80] + "..." if len(f["value"]) > 80 else f["value"]
            print(f"    {f['id']}: {val}")
        return

    if not all_fields:
        print("\n  Nothing to translate!")
        return

    # ---- Load progress ----
    progress_file = os.path.join(output_dir, "_translation_progress_ar.json")
    all_translations = {}
    if os.path.exists(progress_file):
        all_translations = load_json(progress_file)
        if isinstance(all_translations, dict):
            print(f"\n  Resuming: {len(all_translations)} fields already translated")
        else:
            all_translations = {}

    remaining = [f for f in all_fields if f["id"] not in all_translations]
    print(f"  Remaining: {len(remaining)} fields")

    if not remaining:
        print("  All fields already translated!")
    else:
        api_key = os.environ.get("OPENAI_API_KEY")
        if not api_key:
            print("ERROR: OPENAI_API_KEY not set. Add it to .env")
            sys.exit(1)

        client = OpenAI(api_key=api_key)

        max_batch_tokens = args.batch_size * 100
        batches = adaptive_batch(remaining, max_tokens=max_batch_tokens)
        total_batches = len(batches)
        batch_sizes = [len(b) for b in batches]

        print(f"\n  {total_batches} adaptive batches (sizes: {min(batch_sizes)}-{max(batch_sizes)} fields)")
        print(f"  TPM budget: {args.tpm:,}")

        window_start = time.time()
        window_tokens = 0
        failed_fields = []

        for i, batch in enumerate(batches):
            now = time.time()
            elapsed = now - window_start
            if elapsed >= 60:
                window_start = now
                window_tokens = 0
            elif window_tokens >= args.tpm * 0.85:
                wait = 60 - elapsed + 2
                print(f"    TPM throttle: {window_tokens:,} tokens used, waiting {wait:.0f}s...")
                time.sleep(wait)
                window_start = time.time()
                window_tokens = 0

            t_map, tokens_used = translate_batch(
                client, args.model, batch,
                "English", "Arabic",
                i + 1, total_batches,
            )
            window_tokens += tokens_used

            if t_map:
                all_translations.update(t_map)
                save_json(all_translations, progress_file)
                batch_ids = {f["id"] for f in batch}
                missing_from_batch = batch_ids - set(t_map.keys())
                if missing_from_batch:
                    failed_fields.extend([f for f in batch if f["id"] in missing_from_batch])
            else:
                failed_fields.extend(batch)

        if failed_fields:
            print(f"\n  WARNING: {len(failed_fields)} fields failed translation")
            print(f"  Re-run to retry (progress is saved)")
            save_json([f["id"] for f in failed_fields], os.path.join(output_dir, "_failed_fields_ar.json"))

    # ---- Build output data ----
    print(f"\n  Merging {len(all_translations)} translations into output files...")

    output_products = [copy.deepcopy(p) for p in products]
    output_collections = [copy.deepcopy(c) for c in collections]
    output_pages = [copy.deepcopy(pg) for pg in pages]
    output_blogs = [copy.deepcopy(b) for b in blogs]
    output_articles = [copy.deepcopy(a) for a in articles]
    output_metaobjects = copy.deepcopy(metaobjects) if isinstance(metaobjects, dict) else {}

    apply_translations(
        all_translations,
        output_products, output_collections, output_pages,
        output_articles, output_metaobjects, blogs=output_blogs,
    )

    # ---- Save ----
    save_json(output_products, os.path.join(output_dir, "products.json"))
    save_json(output_collections, os.path.join(output_dir, "collections.json"))
    save_json(output_pages, os.path.join(output_dir, "pages.json"))
    save_json(output_blogs, os.path.join(output_dir, "blogs.json"))
    save_json(output_articles, os.path.join(output_dir, "articles.json"))
    save_json(output_metaobjects, os.path.join(output_dir, "metaobjects.json"))

    # Copy non-translatable files
    for fname in ["metaobject_definitions.json"]:
        src = os.path.join(input_dir, fname)
        if os.path.exists(src):
            save_json(load_json(src), os.path.join(output_dir, fname))

    print(f"\n{'=' * 60}")
    print(f"TRANSLATION COMPLETE → ARABIC")
    print(f"{'=' * 60}")
    print(f"  Products:    {len(output_products)}")
    print(f"  Collections: {len(output_collections)}")
    print(f"  Pages:       {len(output_pages)}")
    print(f"  Blogs:       {len(output_blogs)}")
    print(f"  Articles:    {len(output_articles)}")
    if output_metaobjects:
        mo_total = sum(len(v.get("objects", [])) for v in output_metaobjects.values())
        print(f"  Metaobjects: {mo_total}")
    print(f"  Output:      {output_dir}/")

    translated_count = len(all_translations)
    total_needed = len(all_fields)
    completeness = (translated_count / total_needed * 100) if total_needed else 100
    print(f"\n  Completeness: {translated_count}/{total_needed} fields ({completeness:.1f}%)")
    if completeness < 100:
        print(f"  Re-run: python translate_to_arabic.py")
    else:
        print(f"\n  Next: python import_arabic.py")


if __name__ == "__main__":
    main()
