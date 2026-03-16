#!/usr/bin/env python3
"""Prepare the English import directory from source export data.

For cross-store migration (e.g. Saudi → Kuwait) where the source store
content is already in English, this script copies the exported data into
the destination's English import directory — skipping the translation step
entirely.

For the original Spain → Saudi pipeline (where source is Spanish), use
``translate_gaps.py --lang en`` instead.

Usage:
    # Copy source export → english dir (auto-detects DEST_NAME)
    python prepare_import.py

    # Preview what would be copied
    python prepare_import.py --dry-run

    # Explicit destination
    DEST_NAME=kuwait python prepare_import.py
"""

import argparse
import json
import os
import shutil

from dotenv import load_dotenv

from tara_migrate.core import config


# Files to copy from source_export → english dir
CONTENT_FILES = [
    "products.json",
    "collections.json",
    "pages.json",
    "blogs.json",
    "articles.json",
    "metaobjects.json",
    "metaobject_definitions.json",
    "collects.json",
]


def main():
    parser = argparse.ArgumentParser(
        description="Prepare English import directory from source export")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would be copied without doing it")
    args = parser.parse_args()

    load_dotenv()

    source_dir = config.SOURCE_DIR
    en_dir = config.get_en_dir()
    dest_name = config.get_dest_name()

    print(f"Source directory: {source_dir}")
    print(f"English directory: {en_dir}")
    if dest_name:
        print(f"Destination: {dest_name}")
    print()

    if not os.path.exists(source_dir):
        print(f"ERROR: Source directory not found: {source_dir}")
        print("Run export_source.py first to export from the source store.")
        return

    if not args.dry_run:
        os.makedirs(en_dir, exist_ok=True)

    copied = 0
    skipped = 0

    for filename in CONTENT_FILES:
        src_path = os.path.join(source_dir, filename)
        dst_path = os.path.join(en_dir, filename)

        if not os.path.exists(src_path):
            print(f"  SKIP: {filename} (not in source export)")
            skipped += 1
            continue

        if os.path.exists(dst_path):
            # Check if content is identical
            with open(src_path) as f:
                src_data = json.load(f)
            with open(dst_path) as f:
                dst_data = json.load(f)
            if src_data == dst_data:
                print(f"  SKIP: {filename} (already up to date)")
                skipped += 1
                continue

        if args.dry_run:
            src_size = os.path.getsize(src_path)
            print(f"  COPY: {filename} ({src_size:,} bytes)")
        else:
            shutil.copy2(src_path, dst_path)
            print(f"  COPIED: {filename}")
        copied += 1

    print(f"\n{'Would copy' if args.dry_run else 'Copied'}: {copied} files, Skipped: {skipped}")

    if not args.dry_run and copied > 0:
        print(f"\nEnglish import data ready at: {en_dir}/")
        print("Next step: python import_english.py")


if __name__ == "__main__":
    main()
