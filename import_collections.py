#!/usr/bin/env python3
"""Import ALL collections from Spain to Saudi store.

Creates both smart (automatic) and custom (manual) collections,
remaps smart collection rules (metafield definition GIDs and metaobject
GIDs), and links products to custom collections via collects.

Usage:
    python import_collections.py --dry-run    # Preview
    python import_collections.py              # Run live
"""

import argparse
import json
import os
import time

from dotenv import load_dotenv

from shopify_client import ShopifyClient
from utils import load_json, save_json, ID_MAP_FILE


def deduplicate_collections(collections):
    """Remove duplicate entries (same ID appears twice in export)."""
    seen = set()
    unique = []
    for c in collections:
        cid = c.get("id")
        if cid not in seen:
            seen.add(cid)
            unique.append(c)
    return unique


def build_metafield_def_remap(spain, saudi):
    """Build Spain MetafieldDefinition GID → Saudi GID mapping.

    Smart collection rules reference MetafieldDefinition GIDs (numeric
    condition_object_id) that are store-specific.  We remap by matching
    (namespace, key) between the two stores.

    Also loads the exported definitions file as a fallback source of
    Spain GIDs.
    """
    remap = {}  # Spain numeric ID → Saudi numeric ID

    spain_defs_file = "data/spain_export/product_metafield_definitions.json"
    spain_defs = load_json(spain_defs_file) if os.path.exists(spain_defs_file) else []

    if not spain_defs:
        # Fetch live from Spain
        try:
            spain_defs_raw = spain.get_metafield_definitions("PRODUCT")
            spain_defs = spain_defs_raw
        except Exception as e:
            print(f"  Warning: Could not fetch Spain metafield definitions: {e}")
            return remap

    # Fetch Saudi definitions
    try:
        saudi_defs = saudi.get_metafield_definitions("PRODUCT")
    except Exception as e:
        print(f"  Warning: Could not fetch Saudi metafield definitions: {e}")
        return remap

    saudi_by_nk = {}
    for d in saudi_defs:
        nk = (d["namespace"], d["key"])
        gid = d["id"]
        numeric = int(gid.split("/")[-1]) if "/" in gid else gid
        saudi_by_nk[nk] = numeric

    for sd in spain_defs:
        nk = (sd["namespace"], sd["key"])
        spain_gid = sd["id"]
        spain_numeric = int(spain_gid.split("/")[-1]) if isinstance(spain_gid, str) and "/" in spain_gid else spain_gid

        if nk in saudi_by_nk:
            remap[spain_numeric] = saudi_by_nk[nk]

    return remap


def remap_rules(rules, metafield_def_remap, metaobject_id_map):
    """Remap smart collection rules from Spain GIDs to Saudi GIDs.

    Returns (remapped_rules, skip_reason) where skip_reason is None on success.
    """
    remapped = []
    for rule in rules:
        rule = dict(rule)  # shallow copy

        # 1. Remap condition_object_id (MetafieldDefinition numeric ID)
        coid = rule.get("condition_object_id")
        if coid and isinstance(coid, int):
            dest_coid = metafield_def_remap.get(coid)
            if dest_coid:
                rule["condition_object_id"] = dest_coid
            elif rule.get("column") == "product_metafield_definition":
                return None, f"no matching metafield definition for condition_object_id {coid}"
        elif coid and isinstance(coid, str) and coid.startswith("gid://"):
            numeric = int(coid.split("/")[-1])
            dest_numeric = metafield_def_remap.get(numeric)
            if dest_numeric:
                rule["condition_object_id"] = dest_numeric
            elif rule.get("column") == "product_metafield_definition":
                return None, f"no matching metafield definition for {coid}"

        # 2. Remap condition (metaobject GID)
        cond = rule.get("condition", "")
        if isinstance(cond, str) and cond.startswith("gid://shopify/Metaobject/"):
            dest_cond = metaobject_id_map.get(cond)
            if dest_cond:
                rule["condition"] = dest_cond
            # If not mapped, keep original — the ingredient might exist on
            # Saudi with the same GID (unlikely) or we skip

        remapped.append(rule)

    return remapped, None


def main():
    load_dotenv()
    parser = argparse.ArgumentParser(
        description="Import all collections from Spain to Saudi store")
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview without making changes")
    args = parser.parse_args()

    spain_url = os.environ.get("SPAIN_SHOP_URL")
    spain_token = os.environ.get("SPAIN_ACCESS_TOKEN")
    saudi_url = os.environ.get("SAUDI_SHOP_URL")
    saudi_token = os.environ.get("SAUDI_ACCESS_TOKEN")

    if not all([spain_url, spain_token, saudi_url, saudi_token]):
        print("ERROR: Set SPAIN_SHOP_URL, SPAIN_ACCESS_TOKEN, SAUDI_SHOP_URL, SAUDI_ACCESS_TOKEN in .env")
        return

    spain = ShopifyClient(spain_url, spain_token)
    saudi = ShopifyClient(saudi_url, saudi_token)

    id_map = load_json(ID_MAP_FILE) if os.path.exists(ID_MAP_FILE) else {}

    # Use English collections (translated titles/descriptions)
    collections = load_json("data/english/collections.json")
    if not collections:
        print("ERROR: No collections found in data/english/collections.json")
        return

    collections = deduplicate_collections(collections)

    print("=" * 60)
    print("COLLECTION IMPORT: Spain → Saudi")
    print("=" * 60)

    smart = [c for c in collections if c.get("rules") is not None]
    custom = [c for c in collections if c.get("rules") is None]
    print(f"  Total: {len(collections)} ({len(smart)} smart, {len(custom)} custom)")
    print(f"  Mode: {'DRY RUN' if args.dry_run else 'LIVE'}")

    # --- Build GID remappings ---
    print("\n--- Building GID remappings ---")

    # Metafield definition remap (for smart collection rules)
    if not args.dry_run:
        metafield_def_remap = build_metafield_def_remap(spain, saudi)
        print(f"  Mapped {len(metafield_def_remap)} metafield definition GIDs")
    else:
        metafield_def_remap = {}

    # Metaobject GID remap (for smart collection conditions)
    all_metaobject_id_map = {}
    for map_key, mapping in id_map.items():
        if map_key.startswith("metaobjects_"):
            all_metaobject_id_map.update(mapping)
    print(f"  Metaobject GID mappings: {len(all_metaobject_id_map)}")

    # --- Phase 1: Create custom collections ---
    print(f"\n--- Phase 1: Custom Collections ({len(custom)}) ---")
    created_custom = 0
    skipped = 0

    for i, coll in enumerate(custom):
        source_id = str(coll["id"])
        handle = coll.get("handle", "")
        title = coll.get("title", "")[:50]
        label = f"[{i+1}/{len(custom)}] {title}"

        if source_id in id_map.get("collections", {}):
            skipped += 1
            continue

        if args.dry_run:
            print(f"  {label} — would create custom (handle: {handle})")
            created_custom += 1
            continue

        # Check if already exists by handle
        existing = saudi.get_collections_by_handle(handle)
        if existing:
            dest_id = existing[0]["id"]
            print(f"  {label} — exists (id: {dest_id}), mapping")
            id_map.setdefault("collections", {})[source_id] = str(dest_id)
            save_json(id_map, ID_MAP_FILE)
            continue

        coll_data = {
            "title": coll.get("title", ""),
            "body_html": coll.get("body_html", ""),
            "handle": handle,
        }
        if coll.get("sort_order"):
            coll_data["sort_order"] = coll["sort_order"]
        if coll.get("image", {}).get("src"):
            coll_data["image"] = {"src": coll["image"]["src"]}

        try:
            created = saudi.create_custom_collection(coll_data)
            dest_id = created.get("id")
            if dest_id:
                print(f"  {label} — created (id: {dest_id})")
                id_map.setdefault("collections", {})[source_id] = str(dest_id)
                save_json(id_map, ID_MAP_FILE)
                created_custom += 1
                time.sleep(0.3)
        except Exception as e:
            print(f"  {label} — ERROR: {e}")

    print(f"  Created {created_custom} custom collections, skipped {skipped}")

    # --- Phase 2: Create smart collections ---
    print(f"\n--- Phase 2: Smart Collections ({len(smart)}) ---")
    created_smart = 0
    skipped_smart = 0
    failed_smart = 0

    for i, coll in enumerate(smart):
        source_id = str(coll["id"])
        handle = coll.get("handle", "")
        title = coll.get("title", "")[:50]
        label = f"[{i+1}/{len(smart)}] {title}"

        if source_id in id_map.get("collections", {}):
            skipped_smart += 1
            continue

        # Remap rules
        rules = coll.get("rules", [])
        if args.dry_run:
            rule_types = set(r.get("column", "") for r in rules)
            print(f"  {label} — would create smart (handle: {handle}, rules: {len(rules)}, types: {rule_types})")
            created_smart += 1
            continue

        # Check if already exists by handle on Saudi
        # For smart collections, get_collections_by_handle only checks custom,
        # so also try fetching all smart collections
        existing = saudi.get_collections_by_handle(handle)
        if existing:
            dest_id = existing[0]["id"]
            print(f"  {label} — exists (id: {dest_id}), mapping")
            id_map.setdefault("collections", {})[source_id] = str(dest_id)
            save_json(id_map, ID_MAP_FILE)
            continue

        remapped_rules, skip_reason = remap_rules(rules, metafield_def_remap, all_metaobject_id_map)
        if skip_reason:
            print(f"  {label} — SKIPPED: {skip_reason}")
            failed_smart += 1
            continue

        coll_data = {
            "title": coll.get("title", ""),
            "body_html": coll.get("body_html", ""),
            "handle": handle,
            "rules": remapped_rules,
            "disjunctive": coll.get("disjunctive", False),
        }
        if coll.get("sort_order"):
            coll_data["sort_order"] = coll["sort_order"]
        if coll.get("image", {}).get("src"):
            coll_data["image"] = {"src": coll["image"]["src"]}

        try:
            created = saudi.create_smart_collection(coll_data)
            dest_id = created.get("id")
            if dest_id:
                print(f"  {label} — created (id: {dest_id})")
                id_map.setdefault("collections", {})[source_id] = str(dest_id)
                save_json(id_map, ID_MAP_FILE)
                created_smart += 1
                time.sleep(0.3)
        except Exception as e:
            err_msg = str(e)
            if hasattr(e, 'response') and e.response is not None:
                try:
                    err_msg = json.dumps(e.response.json(), indent=2)
                except Exception:
                    pass
            print(f"  {label} — ERROR: {err_msg}")
            failed_smart += 1

    print(f"  Created {created_smart} smart collections, skipped {skipped_smart}, failed {failed_smart}")

    # --- Phase 3: Link products to custom collections ---
    print(f"\n--- Phase 3: Product-Collection Links ---")

    collects = load_json("data/spain_export/collects.json")
    if not collects:
        print("  No collects data found — skipping product links")
    else:
        product_map = id_map.get("products", {})
        collection_map = id_map.get("collections", {})
        progress = load_json("data/collects_progress.json") if os.path.exists("data/collects_progress.json") else {}
        if not isinstance(progress, dict):
            progress = {}

        linked = 0
        link_skipped = 0
        link_errors = 0

        for collect in collects:
            source_pid = str(collect.get("product_id", ""))
            source_cid = str(collect.get("collection_id", ""))
            key = f"{source_pid}_{source_cid}"

            if key in progress:
                link_skipped += 1
                continue

            dest_pid = product_map.get(source_pid)
            dest_cid = collection_map.get(source_cid)

            if not dest_pid or not dest_cid:
                link_skipped += 1
                continue

            if args.dry_run:
                linked += 1
                continue

            try:
                saudi.create_collect(dest_pid, dest_cid)
                linked += 1
                progress[key] = True
                if linked % 10 == 0:
                    save_json(progress, "data/collects_progress.json")
            except Exception as e:
                err = str(e)
                if "already" in err.lower() or "422" in err:
                    progress[key] = True
                    link_skipped += 1
                else:
                    print(f"  Error linking product {dest_pid} → collection {dest_cid}: {e}")
                    link_errors += 1

        if not args.dry_run:
            save_json(progress, "data/collects_progress.json")
        print(f"  Linked {linked} products, skipped {link_skipped}, errors {link_errors}")

    # --- Summary ---
    save_json(id_map, ID_MAP_FILE)

    print("\n" + "=" * 60)
    print("COLLECTION IMPORT COMPLETE")
    print("=" * 60)
    coll_map = id_map.get("collections", {})
    print(f"  Total collection mappings: {len(coll_map)}")
    print(f"  Custom created: {created_custom}")
    print(f"  Smart created: {created_smart}")
    if failed_smart:
        print(f"  Smart failed: {failed_smart}")


if __name__ == "__main__":
    main()
