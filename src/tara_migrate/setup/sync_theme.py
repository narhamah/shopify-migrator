#!/usr/bin/env python3
"""Sync the active source theme asset set to the active destination theme.

This is the missing piece for "exact replica" migrations where the destination
store must inherit the same templates, sections, settings, assets, and locale
files as the source storefront.
"""

from __future__ import annotations

import argparse
import os

from dotenv import load_dotenv

from tara_migrate.client import ShopifyClient
from tara_migrate.core import config, save_json


def _classify_asset_payload(asset: dict) -> dict | None:
    if asset.get("value") is not None:
        return {"value": asset["value"]}
    if asset.get("attachment") is not None:
        return {"attachment": asset["attachment"]}
    if asset.get("public_url"):
        return {"src": asset["public_url"]}
    return None


def sync_active_theme(
    source_client: ShopifyClient,
    dest_client: ShopifyClient,
    dry_run: bool = False,
    delete_extras: bool = True,
) -> dict:
    source_theme_id = source_client.get_main_theme_id()
    dest_theme_id = dest_client.get_main_theme_id()
    if not source_theme_id or not dest_theme_id:
        raise RuntimeError("Could not find the active theme on one or both stores")

    source_assets = {asset["key"]: asset for asset in source_client.list_assets(source_theme_id)}
    dest_assets = {asset["key"]: asset for asset in dest_client.list_assets(dest_theme_id)}

    missing = sorted(set(source_assets) - set(dest_assets))
    extra = sorted(set(dest_assets) - set(source_assets))
    changed = sorted(
        key
        for key in (set(source_assets) & set(dest_assets))
        if (
            source_assets[key].get("checksum") != dest_assets[key].get("checksum")
            or source_assets[key].get("size") != dest_assets[key].get("size")
        )
    )

    report = {
        "source_theme_id": source_theme_id,
        "dest_theme_id": dest_theme_id,
        "missing_assets": missing,
        "changed_assets": changed,
        "extra_assets": extra,
        "uploaded": [],
        "updated": [],
        "deleted": [],
        "skipped": [],
        "errors": [],
    }

    print("\n=== Theme Sync ===")
    print(f"  Source theme: {source_theme_id}")
    print(f"  Destination theme: {dest_theme_id}")
    print(f"  Missing assets: {len(missing)}")
    print(f"  Changed assets: {len(changed)}")
    print(f"  Extra assets: {len(extra)}")

    for key in missing + changed:
        action = "upload" if key in missing else "update"
        try:
            asset = source_client.get_asset(source_theme_id, key)
            payload = _classify_asset_payload(asset)
            if not payload:
                report["skipped"].append({"key": key, "reason": "no transferable payload"})
                print(f"  SKIP {key}: no transferable payload")
                continue

            if dry_run:
                print(f"  WOULD {action.upper()} {key}")
                continue

            dest_client.put_asset(dest_theme_id, key, **payload)
            report["uploaded" if action == "upload" else "updated"].append(key)
            print(f"  {action.upper()} {key}")
        except Exception as e:
            report["errors"].append({"key": key, "error": str(e)})
            print(f"  ERROR {action.upper()} {key}: {e}")

    if delete_extras:
        for key in extra:
            try:
                if dry_run:
                    print(f"  WOULD DELETE {key}")
                    continue
                dest_client.delete_asset(dest_theme_id, key)
                report["deleted"].append(key)
                print(f"  DELETE {key}")
            except Exception as e:
                report["errors"].append({"key": key, "error": str(e)})
                print(f"  ERROR DELETE {key}: {e}")

    output_path = config.get_progress_file("theme_sync_report.json")
    save_json(report, output_path)
    print(f"\n  Theme sync report saved to {output_path}")
    return report


def main():
    load_dotenv()
    parser = argparse.ArgumentParser(description="Sync the active source theme to the destination theme")
    parser.add_argument("--dry-run", action="store_true", help="Preview changes without writing assets")
    parser.add_argument("--keep-extra", action="store_true", help="Do not delete destination-only assets")
    args = parser.parse_args()

    source = ShopifyClient(config.get_source_shop_url(), config.get_source_access_token())
    dest = ShopifyClient(config.get_dest_shop_url(), config.get_dest_access_token())
    sync_active_theme(source, dest, dry_run=args.dry_run, delete_extras=not args.keep_extra)


if __name__ == "__main__":
    main()
