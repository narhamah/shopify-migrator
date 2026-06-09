#!/usr/bin/env python3
"""Set up a Shopify Market for the destination from a declarative config.

Automates the previously-manual "Settings > Markets" work: creates the market,
its country regions, base currency, and a web presence. With the subfolder URL
strategy this needs ZERO DNS work (e.g. /kw on the primary domain).

Idempotent: an existing market with the same name is reused; regions/currency/
web-presence are only created when missing.

Usage:
    python setup_markets.py --config destinations/kuwait.toml
    python setup_markets.py --config destinations/kuwait.toml --dry-run
"""

import argparse

from dotenv import load_dotenv

from tara_migrate.client import ShopifyClient
from tara_migrate.core import config
from tara_migrate.core.config_schema import apply_to_env, load_destination_config
from tara_migrate.core.logging import get_logger

logger = get_logger(__name__)


def setup_market(client, name, countries, currency=None, locales=None,
                 default_locale="en", subfolder_suffix=None, domain_id=None,
                 dry_run=False):
    """Create/reuse a market and configure regions, currency, and web presence."""
    locales = locales or [default_locale]
    alternate = [l for l in locales if l != default_locale]

    existing = {m["name"]: m for m in client.get_markets()}
    market = existing.get(name)

    if market:
        print(f"  Market '{name}' already exists (id {market['id']}) — reusing")
    elif dry_run:
        print(f"  Would create market '{name}' covering {countries}")
        return None
    else:
        market = client.create_market(name, countries)
        print(f"  Created market '{name}' (id {market['id']}) covering {countries}")

    market_id = market["id"] if market else None

    if dry_run:
        print(f"  Would set currency={currency}, locales={locales} (default {default_locale}),"
              f" web presence subfolder={subfolder_suffix or domain_id or 'default'}")
        return market_id

    if currency:
        try:
            client.market_update_currency(market_id, currency)
            print(f"  Set base currency: {currency}")
        except Exception as e:
            logger.warning("  Could not set currency %s: %s", currency, e)

    has_web_presence = bool(market.get("webPresence")) if market else False
    if not has_web_presence:
        try:
            client.market_create_web_presence(
                market_id, default_locale, alternate_locales=alternate,
                subfolder_suffix=subfolder_suffix, domain_id=domain_id)
            print(f"  Created web presence (default {default_locale}, alternates {alternate})")
        except Exception as e:
            logger.warning("  Could not create web presence: %s", e)
    else:
        print("  Web presence already configured — skipping")

    return market_id


def main():
    parser = argparse.ArgumentParser(description="Set up a Shopify Market for the destination")
    parser.add_argument("--config", help="Destination TOML (destinations/<name>.toml)")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    load_dotenv()

    name = config.get_dest_name() or "Destination"
    countries, currency, locales, default_locale, subfolder = [], None, ["en"], "en", None
    if args.config:
        cfg = load_destination_config(args.config)
        apply_to_env(cfg)
        name = cfg.name
        countries = cfg.market.countries
        currency = cfg.market.currency
        locales = cfg.market.locales
        default_locale = cfg.market.default_locale
        subfolder = cfg.market.subfolder_suffix

    if not countries:
        print("ERROR: no market countries configured (set [market].countries in the config)")
        raise SystemExit(1)

    client = ShopifyClient(config.get_dest_shop_url(), config.get_dest_access_token())
    print(f"=== Setting up market '{name}' ({'DRY RUN' if args.dry_run else 'LIVE'}) ===")
    setup_market(client, name, countries, currency=currency, locales=locales,
                 default_locale=default_locale, subfolder_suffix=subfolder, dry_run=args.dry_run)
    print("=== Market setup complete ===")


if __name__ == "__main__":
    main()
