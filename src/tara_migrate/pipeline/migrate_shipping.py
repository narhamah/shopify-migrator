#!/usr/bin/env python3
"""Migrate shipping zones + flat rates from the source store to the destination.

Automates "Settings > Shipping and delivery" for the common case: copy the
source store's delivery zones (countries + flat-rate method definitions) onto
the destination's default delivery profile.

Scope: requires write_shipping on the destination token. Flat/price-based rates
are migrated; carrier-calculated (real-time) rates need a hosted callback +
Advanced plan and are reported as a manual follow-up.

Usage:
    python migrate_shipping.py --dry-run
    python migrate_shipping.py
    python migrate_shipping.py --currency KWD     # override rate currency
"""

import argparse

from dotenv import load_dotenv

from tara_migrate.client import ShopifyClient
from tara_migrate.core import config
from tara_migrate.core.logging import get_logger

logger = get_logger(__name__)


def _extract_zones(profile, currency_override=None):
    """Flatten a delivery profile into [{name, countries:[code], rates:[{name,amount,currency}]}]."""
    zones = []
    for group in profile.get("profileLocationGroups", []):
        for edge in group.get("locationGroupZones", {}).get("edges", []):
            node = edge["node"]
            zone = node.get("zone", {})
            countries = [c["code"]["countryCode"] for c in zone.get("countries", [])
                         if c.get("code", {}).get("countryCode")]
            rates = []
            for m_edge in node.get("methodDefinitions", {}).get("edges", []):
                m = m_edge["node"]
                provider = m.get("rateProvider", {})
                price = provider.get("price")
                if price:  # flat / definition rate (skip carrier-calculated)
                    rates.append({
                        "name": m.get("name", "Standard"),
                        "amount": price["amount"],
                        "currency": currency_override or price["currencyCode"],
                    })
            if countries:
                zones.append({"name": zone.get("name", "Zone"), "countries": countries, "rates": rates})
    return zones


def build_profile_input(location_group_id, zones):
    """Build a DeliveryProfileInput that adds *zones* to a location group."""
    zones_to_create = []
    for z in zones:
        zones_to_create.append({
            "name": z["name"],
            "countries": [{"code": c} for c in z["countries"]],
            "methodDefinitionsToCreate": [
                {
                    "name": r["name"],
                    "rateDefinition": {"price": {"amount": r["amount"], "currencyCode": r["currency"]}},
                }
                for r in z["rates"]
            ],
        })
    return {"locationGroupsToUpdate": [{"id": location_group_id, "zonesToCreate": zones_to_create}]}


def migrate_shipping(source_client, dest_client, currency_override=None, dry_run=False):
    source_profiles = source_client.get_delivery_profiles()
    source_default = next((p for p in source_profiles if p.get("default")), None)
    if not source_default:
        print("  No default delivery profile on source — nothing to migrate")
        return

    zones = _extract_zones(source_default, currency_override=currency_override)
    if not zones:
        print("  Source default profile has no flat-rate zones to migrate")
        return
    print(f"  Found {len(zones)} source zone(s): " + ", ".join(z["name"] for z in zones))

    dest_profiles = dest_client.get_delivery_profiles()
    dest_default = next((p for p in dest_profiles if p.get("default")), None)
    if not dest_default:
        print("  No default delivery profile on destination — cannot migrate")
        return

    groups = dest_default.get("profileLocationGroups", [])
    if not groups or not groups[0].get("locationGroup", {}).get("id"):
        print("  Destination profile has no location group — assign a location first")
        return
    location_group_id = groups[0]["locationGroup"]["id"]

    profile_input = build_profile_input(location_group_id, zones)
    if dry_run:
        print(f"  Would add {len(zones)} zone(s) to dest profile {dest_default['id']}:")
        for z in zones:
            rate_str = ", ".join(f"{r['name']} {r['amount']} {r['currency']}" for r in z["rates"]) or "no flat rates"
            print(f"    - {z['name']} {z['countries']}: {rate_str}")
        return

    dest_client.update_delivery_profile(dest_default["id"], profile_input)
    print(f"  Added {len(zones)} zone(s) to destination default profile")


def main():
    parser = argparse.ArgumentParser(description="Migrate shipping zones/rates source -> destination")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--currency", default=None, help="Override the rate currency code (e.g. KWD)")
    args = parser.parse_args()

    load_dotenv()
    source_client = ShopifyClient(config.get_source_shop_url(), config.get_source_access_token())
    dest_client = ShopifyClient(config.get_dest_shop_url(), config.get_dest_access_token())

    print(f"=== Migrating shipping ({'DRY RUN' if args.dry_run else 'LIVE'}) ===")
    migrate_shipping(source_client, dest_client, currency_override=args.currency, dry_run=args.dry_run)
    print("  NOTE: carrier-calculated (real-time) rates are not migrated — set those up per courier.")
    print("=== Shipping migration complete ===")


if __name__ == "__main__":
    main()
