#!/usr/bin/env python3
"""Import customers from a Magento CSV export into a Shopify store.

Filters by country and handles Arabic names/addresses properly.

Usage:
    python import_customers.py --input Export_Customers.csv --country "Saudi Arabia"
    python import_customers.py --input Export_Customers.csv --country "United States" --dry-run
    python import_customers.py --input Export_Customers.csv --country "Saudi Arabia,United Arab Emirates"
"""

import argparse
import csv
import json
import os
import re
import time

from dotenv import load_dotenv

from tara_migrate.client import ShopifyClient
from tara_migrate.core import config, save_json


def parse_phone(phone_str):
    """Normalize phone number (strip spaces, ensure + prefix)."""
    if not phone_str:
        return ""
    phone = phone_str.strip()
    # Remove leading spaces and normalize
    phone = re.sub(r"\s+", "", phone)
    # Convert 00 prefix to +
    if phone.startswith("00"):
        phone = "+" + phone[2:]
    # Ensure + prefix
    if phone and not phone.startswith("+"):
        phone = "+" + phone
    return phone


def parse_address(address_str):
    """Parse a multiline Magento address string into components."""
    if not address_str:
        return {}
    lines = [l.strip() for l in address_str.strip().split("\n") if l.strip()]
    if not lines:
        return {}
    # Magento format: street lines, then "city state zip"
    return {"lines": lines}


def split_name(name_str):
    """Split a full name into first and last name, handling Arabic names."""
    if not name_str:
        return "", ""
    name = name_str.strip()
    parts = name.split(None, 1)
    if len(parts) == 2:
        return parts[0], parts[1]
    return name, ""


def magento_row_to_shopify_customer(row):
    """Convert a Magento CSV row to a Shopify customer dict.

    Handles Arabic names and addresses with proper encoding.
    """
    # Prefer Billing Firstname/Lastname, fall back to Name field
    first_name = (row.get("Billing Firstname") or "").strip()
    last_name = (row.get("Billing Lastname") or "").strip()
    if not first_name and not last_name:
        first_name, last_name = split_name(row.get("Name", ""))

    email = (row.get("Email") or "").strip().lower()
    phone = parse_phone(row.get("Phone", ""))

    customer = {
        "first_name": first_name,
        "last_name": last_name,
        "email": email,
        "phone": phone,
        "verified_email": True,
        "send_email_invite": False,
        "tags": f"magento-import,magento-id:{row.get('ID', '')}",
    }

    # Build address from billing info
    street = (row.get("Street Address") or "").strip()
    city = (row.get("City") or "").strip()
    province = (row.get("State/Province") or "").strip()
    zipcode = (row.get("ZIP") or "").strip()
    country = (row.get("Country") or "").strip()
    company = (row.get("Company") or "").strip()

    if street or city:
        address = {
            "first_name": first_name,
            "last_name": last_name,
            "address1": street,
            "city": city,
            "province": province,
            "zip": zipcode,
            "country": country,
            "phone": phone,
        }
        if company:
            address["company"] = company
        customer["addresses"] = [address]

    return customer


def load_customers_csv(csv_path, countries=None):
    """Load customers from Magento CSV, optionally filtering by country.

    Args:
        csv_path: Path to the Magento customer export CSV
        countries: Set of country names to include (None = all)

    Returns:
        List of customer dicts ready for Shopify import
    """
    customers = []
    seen_emails = set()
    skipped_dup = 0
    skipped_no_email = 0

    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Filter by country
            row_country = (row.get("Country") or "").strip()
            if countries and row_country not in countries:
                continue

            email = (row.get("Email") or "").strip().lower()
            if not email:
                skipped_no_email += 1
                continue

            # Dedup by email
            if email in seen_emails:
                skipped_dup += 1
                continue
            seen_emails.add(email)

            customer = magento_row_to_shopify_customer(row)
            customers.append(customer)

    if skipped_dup:
        print(f"  Skipped {skipped_dup} duplicate emails")
    if skipped_no_email:
        print(f"  Skipped {skipped_no_email} rows with no email")

    return customers


def import_customers(client, customers, dry_run=False, batch_delay=0.5):
    """Import customers to Shopify, skipping existing ones by email.

    Returns (created, skipped, errors) counts.
    """
    created = 0
    skipped = 0
    errors = 0

    for i, customer in enumerate(customers):
        email = customer.get("email", "")
        label = f"  [{i+1}/{len(customers)}] {email}"

        if dry_run:
            print(f"{label} — would create ({customer['first_name']} {customer['last_name']})")
            created += 1
            continue

        # Check if customer already exists
        try:
            existing = client.search_customers(f"email:{email}")
            if existing:
                print(f"{label} — already exists (id: {existing[0]['id']})")
                skipped += 1
                continue
        except Exception:
            pass  # Search may not be available, proceed with create

        try:
            result = client.create_customer(customer)
            cid = result.get("id", "?")
            print(f"{label} — created (id: {cid})")
            created += 1
        except Exception as e:
            err = str(e)
            if "has already been taken" in err or "already exists" in err.lower():
                print(f"{label} — already exists")
                skipped += 1
            else:
                print(f"{label} — ERROR: {err[:100]}")
                errors += 1

        time.sleep(batch_delay)

    return created, skipped, errors


def main():
    load_dotenv()
    parser = argparse.ArgumentParser(
        description="Import customers from Magento CSV into Shopify"
    )
    parser.add_argument("--input", required=True, help="Path to Magento customer CSV")
    parser.add_argument(
        "--country",
        help="Filter by country name (comma-separated for multiple, e.g. 'Saudi Arabia,Kuwait')",
    )
    parser.add_argument("--shop", help="Destination Shopify store URL (overrides DEST_SHOP_URL)")
    parser.add_argument("--token", help="Destination access token (overrides DEST_ACCESS_TOKEN)")
    parser.add_argument("--dry-run", action="store_true", help="Preview without creating")
    parser.add_argument("--save-json", help="Save filtered customers to JSON file (no Shopify import)")
    parser.add_argument("--delay", type=float, default=0.5, help="Delay between API calls (default: 0.5s)")
    args = parser.parse_args()

    if not os.path.exists(args.input):
        print(f"ERROR: File not found: {args.input}")
        return

    # Parse country filter
    countries = None
    if args.country:
        countries = {c.strip() for c in args.country.split(",")}
        print(f"Filtering by countries: {', '.join(sorted(countries))}")

    # Load and filter customers
    print(f"Loading customers from {args.input}...")
    customers = load_customers_csv(args.input, countries)
    print(f"  {len(customers)} customers to import")

    if not customers:
        print("No customers matched the filter.")
        return

    # Show sample
    print("\nSample customers:")
    for c in customers[:5]:
        addr = c.get("addresses", [{}])[0] if c.get("addresses") else {}
        print(f"  {c['first_name']} {c['last_name']} <{c['email']}> "
              f"phone={c.get('phone', '')} city={addr.get('city', '')}")

    # Save to JSON only
    if args.save_json:
        save_json(customers, args.save_json)
        print(f"\nSaved {len(customers)} customers to {args.save_json}")
        return

    # Import to Shopify
    shop_url = args.shop or config.get_dest_shop_url()
    access_token = args.token or config.get_dest_access_token()

    if args.dry_run:
        print("\n=== DRY RUN ===")
    else:
        print(f"\nImporting to {shop_url}...")

    client = None if args.dry_run else ShopifyClient(shop_url, access_token)

    created, skipped, errors = import_customers(
        client, customers, dry_run=args.dry_run, batch_delay=args.delay
    )

    print(f"\n--- Import Summary ---")
    print(f"  Created:  {created}")
    print(f"  Skipped:  {skipped}")
    print(f"  Errors:   {errors}")


if __name__ == "__main__":
    main()
