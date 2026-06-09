#!/usr/bin/env python3
"""Auto-configure Klaviyo after the merchant installs the app.

Installing Klaviyo itself needs merchant OAuth (no API for that). But once the
app is connected and a KLAVIYO_API_KEY (private key) is available, the
post-install config — marketing lists, etc. — IS automatable. This makes the
third-party-app step "one OAuth click + automatic config" instead of fully
manual.

Usage:
    KLAVIYO_API_KEY=pk_xxx python setup_klaviyo.py --lists Newsletter,"Back in stock" --dry-run
    KLAVIYO_API_KEY=pk_xxx python setup_klaviyo.py --lists Newsletter
"""

import argparse
import os

import requests

from dotenv import load_dotenv

from tara_migrate.core.logging import get_logger

logger = get_logger(__name__)

KLAVIYO_BASE = "https://a.klaviyo.com/api"
KLAVIYO_REVISION = "2024-10-15"


class KlaviyoClient:
    def __init__(self, api_key, session=None):
        self.api_key = api_key
        self.session = session or requests.Session()
        self.session.headers.update({
            "Authorization": f"Klaviyo-API-Key {api_key}",
            "revision": KLAVIYO_REVISION,
            "accept": "application/json",
            "content-type": "application/json",
        })

    def get_lists(self):
        resp = self.session.get(f"{KLAVIYO_BASE}/lists/")
        resp.raise_for_status()
        return resp.json().get("data", [])

    def create_list(self, name):
        payload = {"data": {"type": "list", "attributes": {"name": name}}}
        resp = self.session.post(f"{KLAVIYO_BASE}/lists/", json=payload)
        resp.raise_for_status()
        return resp.json().get("data", {})


def ensure_lists(client, names, dry_run=False):
    """Create any of *names* that don't already exist. Returns count created."""
    existing = {item.get("attributes", {}).get("name") for item in client.get_lists()}
    created = 0
    for name in names:
        if name in existing:
            print(f"  List '{name}' already exists — skipping")
            continue
        if dry_run:
            print(f"  Would create list '{name}'")
            continue
        client.create_list(name)
        created += 1
        print(f"  Created list '{name}'")
    return created


def main():
    parser = argparse.ArgumentParser(description="Auto-configure Klaviyo (post-install)")
    parser.add_argument("--lists", default="Newsletter",
                        help="Comma-separated marketing list names to ensure exist")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    load_dotenv()
    api_key = os.environ.get("KLAVIYO_API_KEY")
    if not api_key:
        print("ERROR: KLAVIYO_API_KEY not set. Install Klaviyo (merchant OAuth) and add its private key.")
        raise SystemExit(1)

    names = [n.strip() for n in args.lists.split(",") if n.strip()]
    client = KlaviyoClient(api_key)
    print(f"=== Configuring Klaviyo ({'DRY RUN' if args.dry_run else 'LIVE'}) ===")
    created = ensure_lists(client, names, dry_run=args.dry_run)
    print(f"=== Klaviyo config complete: {created} list(s) created ===")


if __name__ == "__main__":
    main()
