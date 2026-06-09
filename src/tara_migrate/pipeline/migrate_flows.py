#!/usr/bin/env python3
"""Rebuild owned automation patterns that Shopify Flow has no CRUD API for.

Shopify exposes no API to read/import Flow *workflow definitions*, so arbitrary
flows stay manual. But the common owned patterns — "auto-collect products into a
collection by rule, then publish" — are just smart collections and can be
rebuilt deterministically from a declarative spec.

Spec file (default data/{dest}/flow_patterns.json):
    [
      {
        "type": "smart_collection",
        "title": "Shampoos",
        "handle": "shampoos",
        "disjunctive": false,
        "rules": [{"column": "type", "relation": "equals", "condition": "Shampoo"}],
        "publish": true
      }
    ]

Unrecognized pattern types are reported as manual follow-ups (with no silent skip).

Usage:
    python migrate_flows.py --dry-run
    python migrate_flows.py --spec data/kuwait/flow_patterns.json
"""

import argparse
import os

from dotenv import load_dotenv

from tara_migrate.client import ShopifyClient
from tara_migrate.core import config, load_json
from tara_migrate.core.logging import get_logger

logger = get_logger(__name__)

SUPPORTED = {"smart_collection"}


def rebuild_patterns(client, patterns, dry_run=False):
    """Rebuild supported flow patterns; return (created, published, skipped)."""
    created, published, skipped = 0, 0, []
    existing = {c.get("handle"): c for c in client.get_collections()}
    publications = []
    if not dry_run:
        try:
            publications = client.get_publications()
        except Exception as e:
            logger.warning("Could not fetch publications (won't auto-publish): %s", e)

    for pattern in patterns:
        ptype = pattern.get("type")
        if ptype not in SUPPORTED:
            skipped.append(pattern)
            print(f"  SKIP (manual): unsupported pattern type '{ptype}' — {pattern.get('title', '')}")
            continue

        handle = pattern.get("handle")
        title = pattern.get("title", handle)
        if handle in existing:
            print(f"  '{title}' already exists — skipping create")
            continue

        if dry_run:
            print(f"  Would create smart collection '{title}' with {len(pattern.get('rules', []))} rule(s)"
                  + (" + publish" if pattern.get("publish") else ""))
            continue

        collection_data = {
            "title": title,
            "handle": handle,
            "disjunctive": pattern.get("disjunctive", False),
            "rules": pattern.get("rules", []),
        }
        result = client.create_smart_collection(collection_data)
        coll_id = result.get("id")
        created += 1
        print(f"  Created smart collection '{title}' (id {coll_id})")

        if pattern.get("publish") and coll_id and publications:
            gid = f"gid://shopify/Collection/{coll_id}"
            try:
                client.publish_resource(gid, [p["id"] for p in publications])
                published += 1
                print(f"    Published '{title}' to {len(publications)} channel(s)")
            except Exception as e:
                logger.warning("    Could not publish '%s': %s", title, e)

    return created, published, skipped


def main():
    parser = argparse.ArgumentParser(description="Rebuild owned smart-collection flow patterns")
    parser.add_argument("--spec", default=None, help="Path to flow_patterns.json")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    load_dotenv()
    spec_path = args.spec or config.get_progress_file("flow_patterns.json")
    if not os.path.exists(spec_path):
        print(f"No flow pattern spec at {spec_path}. Create one to rebuild owned patterns.")
        print("Arbitrary Shopify Flows have no API and must be exported/imported via the Flow UI.")
        return

    patterns = load_json(spec_path, default=[])
    client = ShopifyClient(config.get_dest_shop_url(), config.get_dest_access_token())

    print(f"=== Rebuilding {len(patterns)} flow pattern(s) ({'DRY RUN' if args.dry_run else 'LIVE'}) ===")
    created, published, skipped = rebuild_patterns(client, patterns, dry_run=args.dry_run)
    print(f"\n  Created: {created}, Published: {published}, Manual follow-ups: {len(skipped)}")


if __name__ == "__main__":
    main()
