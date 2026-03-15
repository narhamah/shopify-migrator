#!/usr/bin/env python3
"""Fix existing redirect targets on the destination store.

If redirects were already created with Spanish handles as targets,
this script fetches all redirects from the destination store, remaps their
targets using the old→new handle mapping, and updates them.

Usage:
    python fix_redirects.py --dry-run    # preview
    python fix_redirects.py               # fix all
"""

import argparse
import os

from dotenv import load_dotenv

from tara_migrate.client import ShopifyClient
from tara_migrate.core import config
from tara_migrate.pipeline.post_migration import _build_handle_remap, _remap_redirect_target


def main():
    load_dotenv()
    parser = argparse.ArgumentParser(description="Fix redirect targets on destination store")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be done")
    args = parser.parse_args()

    shop_url = config.get_dest_shop_url()
    access_token = config.get_dest_access_token()
    if not shop_url or not access_token:
        print("ERROR: DEST_SHOP_URL and DEST_ACCESS_TOKEN must be set in .env")
        return

    client = ShopifyClient(shop_url, access_token)

    # Build handle remap table
    remap = _build_handle_remap()
    print(f"Handle remap table: {len(remap)} entries")
    if not remap:
        print("No handle changes detected — nothing to fix.")
        return

    # Fetch all existing redirects from destination store
    print("Fetching existing redirects from destination store...")
    redirects = client.get_redirects()
    print(f"Found {len(redirects)} redirects")

    fixed = 0
    for redir in redirects:
        rid = redir.get("id")
        path = redir.get("path", "")
        target = redir.get("target", "")

        new_target = _remap_redirect_target(target, remap)
        if new_target == target:
            continue

        if args.dry_run:
            print(f"  Would fix: {path} → {new_target}  (was: {target})")
        else:
            try:
                client.update_redirect(rid, target=new_target)
                print(f"  Fixed: {path} → {new_target}  (was: {target})")
            except Exception as e:
                print(f"  Error fixing redirect {rid} ({path}): {e}")
        fixed += 1

    print(f"\n{'Would fix' if args.dry_run else 'Fixed'} {fixed} redirects")


if __name__ == "__main__":
    main()
