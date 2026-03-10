#!/usr/bin/env python3
"""Convert font files to woff2 format recursively.

Usage:
    python generate_woff2.py [--folder FOLDER]

Requires: fonttools[woff] - install with `pip install fonttools[woff]`
"""

import argparse
import sys
from pathlib import Path

FONT_EXTENSIONS = {".ttf", ".otf", ".woff", ".eot", ".svg"}


def convert_to_woff2(input_path: Path, output_path: Path) -> bool:
    try:
        from fontTools.ttLib import TTFont
    except ImportError:
        print("Error: fonttools not installed. Run: pip install 'fonttools[woff]'")
        sys.exit(1)

    try:
        font = TTFont(input_path)
        font.flavor = "woff2"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        font.save(output_path)
        return True
    except Exception as e:
        print(f"  Failed to convert {input_path}: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description="Convert fonts to woff2 format")
    parser.add_argument("--folder", default="fonts", help="Root folder to scan for fonts (default: fonts)")
    args = parser.parse_args()

    root = Path(args.folder)
    if not root.exists():
        print(f"Error: folder '{root}' does not exist")
        sys.exit(1)

    font_files = [f for f in root.rglob("*") if f.suffix.lower() in FONT_EXTENSIONS]

    if not font_files:
        print(f"No font files found in '{root}'")
        return

    print(f"Found {len(font_files)} font(s) to convert")

    woff2_dir = root / "woff2"
    converted = 0
    skipped = 0

    for font_file in font_files:
        # Skip files already inside a woff2 output folder
        if "woff2" in font_file.parts:
            skipped += 1
            continue

        relative = font_file.relative_to(root)
        output_file = woff2_dir / relative.with_suffix(".woff2")

        if output_file.exists():
            print(f"  Skipping (exists): {output_file}")
            skipped += 1
            continue

        print(f"  Converting: {font_file} -> {output_file}")
        if convert_to_woff2(font_file, output_file):
            converted += 1

    print(f"\nDone: {converted} converted, {skipped} skipped")


if __name__ == "__main__":
    main()
