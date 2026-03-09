#!/usr/bin/env python3
"""Map homepage section blocks to images by reading the source site and updating theme assets.

Reads the current Shopify theme's homepage template (templates/index.json),
identifies sections/blocks with empty image settings, and populates them
by uploading images from the source Magento site.

Usage:
    # Show current homepage sections and their image settings
    python setup_homepage.py --inspect

    # Map images from a JSON config file
    python setup_homepage.py --config homepage_images.json

    # Upload an image and set it on a specific section setting
    python setup_homepage.py --set "section_id.setting_key" --image-url "https://..."
"""

import argparse
import json
import os

from dotenv import load_dotenv

from tara_migrate.client import ShopifyClient
from tara_migrate.core import load_json, save_json


def inspect_homepage(client):
    """Read and display the homepage template structure with all image settings."""
    theme_id = client.get_main_theme_id()
    if not theme_id:
        print("ERROR: No main theme found")
        return None

    print(f"Theme ID: {theme_id}")

    # Get homepage template
    try:
        asset = client.get_asset(theme_id, "templates/index.json")
    except Exception as e:
        print(f"ERROR: Could not read templates/index.json: {e}")
        return None

    template_json = json.loads(asset.get("value", "{}"))
    print(f"\nHomepage template: {len(template_json.get('sections', {}))} sections")
    print(f"Section order: {template_json.get('order', [])}")

    sections = template_json.get("sections", {})
    section_schemas = {}

    for section_id, section in sections.items():
        section_type = section.get("type", "unknown")
        settings = section.get("settings", {})
        blocks = section.get("blocks", {})
        block_order = section.get("block_order", [])

        print(f"\n{'='*60}")
        print(f"Section: {section_id}")
        print(f"  Type: {section_type}")

        # Show image-related settings
        image_settings = {k: v for k, v in settings.items() if _is_image_setting(k, v)}
        if image_settings:
            print("  Image settings:")
            for k, v in image_settings.items():
                status = "SET" if v else "EMPTY"
                print(f"    {k}: {v or '(empty)'} [{status}]")

        # Show other settings for context
        text_settings = {k: v for k, v in settings.items()
                        if k not in image_settings and v and isinstance(v, str) and len(str(v)) < 100}
        if text_settings:
            print("  Text settings:")
            for k, v in list(text_settings.items())[:5]:
                print(f"    {k}: {v}")

        # Show blocks with their image settings
        if blocks:
            print(f"  Blocks ({len(blocks)}):")
            for bid in block_order or blocks.keys():
                block = blocks.get(bid, {})
                btype = block.get("type", "unknown")
                bsettings = block.get("settings", {})
                block_images = {k: v for k, v in bsettings.items() if _is_image_setting(k, v)}
                block_text = {k: v for k, v in bsettings.items()
                             if k not in block_images and v and isinstance(v, str) and len(str(v)) < 80}

                print(f"    Block: {bid} (type: {btype})")
                if block_images:
                    for k, v in block_images.items():
                        status = "SET" if v else "EMPTY"
                        print(f"      IMAGE {k}: {v or '(empty)'} [{status}]")
                if block_text:
                    for k, v in list(block_text.items())[:3]:
                        print(f"      {k}: {v[:60]}")

        # Try to read the section's schema for more info
        try:
            section_asset = client.get_asset(theme_id, f"sections/{section_type}.liquid")
            section_schemas[section_id] = section_asset.get("value", "")
        except Exception:
            pass

    return template_json, theme_id


def _is_image_setting(key, value):
    """Check if a setting key looks like an image field."""
    image_keywords = ["image", "img", "background", "banner", "hero", "icon", "logo", "photo", "picture", "thumbnail"]
    key_lower = key.lower()
    return any(kw in key_lower for kw in image_keywords)


def apply_config(client, config_file, dry_run=False):
    """Apply image mappings from a JSON config file.

    Config format:
    {
        "section_id": {
            "settings": {
                "image": "shopify://shop_images/filename.jpg"
            },
            "blocks": {
                "block_id": {
                    "image": "shopify://shop_images/filename.jpg"
                }
            }
        }
    }

    Image values can be:
    - "shopify://shop_images/filename.jpg" (already uploaded to Files)
    - "https://..." (will be uploaded first)
    """
    config = load_json(config_file)
    if not config:
        print(f"ERROR: Empty or missing config file: {config_file}")
        return

    theme_id = client.get_main_theme_id()
    if not theme_id:
        print("ERROR: No main theme found")
        return

    asset = client.get_asset(theme_id, "templates/index.json")
    template = json.loads(asset.get("value", "{}"))
    sections = template.get("sections", {})

    updated = 0
    for section_id, section_config in config.items():
        if section_id not in sections:
            print(f"  WARNING: Section '{section_id}' not found in homepage template")
            continue

        section = sections[section_id]

        # Update section-level settings
        if "settings" in section_config:
            for key, value in section_config["settings"].items():
                value = _resolve_image_value(client, value, dry_run)
                if value is not None:
                    old = section.get("settings", {}).get(key)
                    section.setdefault("settings", {})[key] = value
                    print(f"  SET {section_id}.settings.{key}: {old} → {value}")
                    updated += 1

        # Update block-level settings
        if "blocks" in section_config:
            blocks = section.get("blocks", {})
            for block_id, block_settings in section_config["blocks"].items():
                if block_id not in blocks:
                    print(f"  WARNING: Block '{block_id}' not found in section '{section_id}'")
                    continue
                for key, value in block_settings.items():
                    value = _resolve_image_value(client, value, dry_run)
                    if value is not None:
                        old = blocks[block_id].get("settings", {}).get(key)
                        blocks[block_id].setdefault("settings", {})[key] = value
                        print(f"  SET {section_id}.blocks.{block_id}.{key}: {old} → {value}")
                        updated += 1

    if updated == 0:
        print("  No changes to apply")
        return

    if dry_run:
        print(f"\n  DRY RUN: Would update {updated} image settings")
        return

    # Write back the updated template
    template_str = json.dumps(template, ensure_ascii=False, indent=2)
    client.put_asset(theme_id, "templates/index.json", template_str)
    print(f"\n  Updated {updated} image settings in homepage template")


def set_single(client, path, image_url, dry_run=False):
    """Set a single image on a section/block setting.

    Path format: "section_id.setting_key" or "section_id.blocks.block_id.setting_key"
    """
    theme_id = client.get_main_theme_id()
    if not theme_id:
        print("ERROR: No main theme found")
        return

    asset = client.get_asset(theme_id, "templates/index.json")
    template = json.loads(asset.get("value", "{}"))
    sections = template.get("sections", {})

    parts = path.split(".")
    if len(parts) == 2:
        section_id, key = parts
        if section_id not in sections:
            print(f"ERROR: Section '{section_id}' not found")
            return
        value = _resolve_image_value(client, image_url, dry_run)
        if value is None:
            return
        sections[section_id].setdefault("settings", {})[key] = value
        print(f"  SET {section_id}.settings.{key} = {value}")
    elif len(parts) == 4 and parts[1] == "blocks":
        section_id, _, block_id, key = parts
        if section_id not in sections:
            print(f"ERROR: Section '{section_id}' not found")
            return
        blocks = sections[section_id].get("blocks", {})
        if block_id not in blocks:
            print(f"ERROR: Block '{block_id}' not found in section '{section_id}'")
            return
        value = _resolve_image_value(client, image_url, dry_run)
        if value is None:
            return
        blocks[block_id].setdefault("settings", {})[key] = value
        print(f"  SET {section_id}.blocks.{block_id}.{key} = {value}")
    else:
        print(f"ERROR: Invalid path format: {path}")
        print("  Use: section_id.setting_key OR section_id.blocks.block_id.setting_key")
        return

    if dry_run:
        print("  DRY RUN: Would update template")
        return

    template_str = json.dumps(template, ensure_ascii=False, indent=2)
    client.put_asset(theme_id, "templates/index.json", template_str)
    print("  Template updated")


def _resolve_image_value(client, value, dry_run=False):
    """Resolve an image value — upload if URL, pass through if shopify:// reference."""
    if not value:
        return None
    if value.startswith("shopify://"):
        return value
    if value.startswith("http://") or value.startswith("https://"):
        if dry_run:
            print(f"    Would upload: {value}")
            return f"shopify://shop_images/uploaded_{os.path.basename(value)}"
        try:
            result = client.upload_file_from_url(value, optimize=True)
            if result:
                # File reference for theme settings uses shopify://shop_images/filename
                filename = result.get("alt", "") or os.path.basename(value)
                # Get the actual filename from the uploaded file
                file_url = result.get("url", "")
                if file_url:
                    # Extract just the filename from the CDN URL
                    fname = file_url.split("/")[-1].split("?")[0]
                    return f"shopify://shop_images/{fname}"
            print(f"    WARNING: Upload returned no result for {value}")
            return None
        except Exception as e:
            print(f"    ERROR uploading {value}: {e}")
            return None
    return value


def main():
    load_dotenv()
    parser = argparse.ArgumentParser(description="Map homepage blocks to images")
    parser.add_argument("--inspect", action="store_true",
                        help="Show homepage sections and their image settings")
    parser.add_argument("--config", type=str,
                        help="JSON config file mapping sections/blocks to images")
    parser.add_argument("--set", type=str, dest="set_path",
                        help="Set a single image: section_id.setting_key")
    parser.add_argument("--image-url", type=str,
                        help="Image URL for --set (https:// or shopify://)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would be done without making changes")
    args = parser.parse_args()

    shop_url = os.environ.get("SAUDI_SHOP_URL")
    access_token = os.environ.get("SAUDI_ACCESS_TOKEN")
    if not shop_url or not access_token:
        print("ERROR: SAUDI_SHOP_URL and SAUDI_ACCESS_TOKEN must be set in .env")
        return

    client = ShopifyClient(shop_url, access_token)

    if args.inspect:
        result = inspect_homepage(client)
        if result:
            template, theme_id = result
            save_json(template, "data/homepage_template.json")
            print("\n  Saved homepage template to data/homepage_template.json")
            print("\n  To update images, create a config file and run:")
            print("    python setup_homepage.py --config homepage_images.json")
        return

    if args.set_path:
        if not args.image_url:
            print("ERROR: --image-url is required with --set")
            return
        set_single(client, args.set_path, args.image_url, dry_run=args.dry_run)
        return

    if args.config:
        apply_config(client, args.config, dry_run=args.dry_run)
        return

    # Default: inspect
    print("No action specified. Use --inspect, --config, or --set.")
    print("\nUsage examples:")
    print("  python setup_homepage.py --inspect")
    print("  python setup_homepage.py --config homepage_images.json")
    print("  python setup_homepage.py --set 'hero.image' --image-url 'https://example.com/hero.jpg'")


if __name__ == "__main__":
    main()
