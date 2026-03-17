#!/usr/bin/env python3
"""Export a normalized Tara quiz product catalog from Shopify.

Fetches live product data from Shopify and produces a structured JSON file
that the quiz frontend and Cloudflare Worker can consume for recommendation
handle matching and bundle rendering.

Output: data/shopify_quiz_catalog.json (or data/{dest}/shopify_quiz_catalog.json)

Usage:
    python export_quiz_catalog.py [--dry-run] [--output PATH]
"""

import argparse
import sys
from typing import Any

from dotenv import load_dotenv

from tara_migrate.client import ShopifyClient
from tara_migrate.core import config
from tara_migrate.core.logging import get_logger
from tara_migrate.core.utils import save_json

logger = get_logger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Quiz-relevant product handles
# ─────────────────────────────────────────────────────────────────────────────

BUNDLE_HANDLES = [
    "nurture-system",
    "hair-wellness-system",
    "scalp-hair-revival-system",
    "hair-density-system",
    "age-well-system",
    "hair-strength-system",
    "hair-stimulation-system",
]

INDIVIDUAL_HANDLES = [
    "cactus-red-seaweed-scalp-serum",
    "ghassoul-avocado-smoothing-conditioner",
    "charcoal-salicylic-exfoliating-shampoo",
    "rejuvenating-scalp-serum",
    "hydrating-conditioner",
    "nourishing-shampoo",
    "nurture-leave-in-conditioner",
    "nurture-conditioner",
    "nurture-shampoo",
    "follicle-boost-serum",
    "strand-thicken-conditioner",
    "scalp-prep-shampoo",
    "follicle-stimulating-scalp-serum",
    "thickening-conditioner",
    "volumizing-shampoo",
    "scalp-support-serum",
    "replenishing-conditioner",
    "revitalizing-shampoo",
]

ALL_QUIZ_HANDLES = set(BUNDLE_HANDLES + INDIVIDUAL_HANDLES)

# ─────────────────────────────────────────────────────────────────────────────
# Bundle → component mappings (source of truth for known bundles)
# ─────────────────────────────────────────────────────────────────────────────

BUNDLE_COMPONENTS: dict[str, list[str]] = {
    "nurture-system": [
        "nurture-shampoo",
        "nurture-conditioner",
        "nurture-leave-in-conditioner",
    ],
    "hair-wellness-system": [
        "nourishing-shampoo",
        "hydrating-conditioner",
        "rejuvenating-scalp-serum",
    ],
    "scalp-hair-revival-system": [
        "charcoal-salicylic-exfoliating-shampoo",
        "ghassoul-avocado-smoothing-conditioner",
        "cactus-red-seaweed-scalp-serum",
    ],
    "hair-density-system": [
        "scalp-prep-shampoo",
        "strand-thicken-conditioner",
        "follicle-boost-serum",
    ],
    "age-well-system": [
        "revitalizing-shampoo",
        "replenishing-conditioner",
        "scalp-support-serum",
    ],
    "hair-stimulation-system": [
        "volumizing-shampoo",
        "thickening-conditioner",
        "follicle-stimulating-scalp-serum",
    ],
    # hair-strength-system: components derived from Shopify at runtime
}

# ─────────────────────────────────────────────────────────────────────────────
# Quiz family mappings (bundle handle → quiz collection identity)
# ─────────────────────────────────────────────────────────────────────────────

QUIZ_FAMILIES: dict[str, dict[str, str]] = {
    "nurture-system": {
        "quiz_collection_id": "nurture",
        "quiz_collection_name": "Nurture",
    },
    "hair-wellness-system": {
        "quiz_collection_id": "hair-wellness",
        "quiz_collection_name": "Hair Wellness",
    },
    "scalp-hair-revival-system": {
        "quiz_collection_id": "scalp-hair-revival",
        "quiz_collection_name": "Scalp & Hair Revival",
    },
    "hair-density-system": {
        "quiz_collection_id": "hair-density",
        "quiz_collection_name": "Hair Density",
    },
    "age-well-system": {
        "quiz_collection_id": "age-well",
        "quiz_collection_name": "Age Well",
    },
    "hair-strength-system": {
        "quiz_collection_id": "hair-strength",
        "quiz_collection_name": "Hair Strength",
    },
    "hair-stimulation-system": {
        "quiz_collection_id": "hair-stimulation",
        "quiz_collection_name": "Hair Stimulation",
    },
}

# ─────────────────────────────────────────────────────────────────────────────
# Product type derivation
# ─────────────────────────────────────────────────────────────────────────────

_TYPE_KEYWORDS = [
    ("leave-in", "leave-in"),
    ("shampoo", "shampoo"),
    ("conditioner", "conditioner"),
    ("serum", "serum"),
    ("mask", "mask"),
    ("oil", "oil"),
]


def derive_product_type(handle: str, title: str) -> str:
    """Derive quiz product_type from handle/title deterministically."""
    if handle in BUNDLE_COMPONENTS:
        return "bundle"
    combined = f"{handle} {title}".lower()
    for keyword, ptype in _TYPE_KEYWORDS:
        if keyword in combined:
            return ptype
    return "other"


def derive_quiz_roles(product_type: str, is_bundle: bool) -> list[str]:
    """Derive quiz_roles from product_type."""
    roles = []
    if is_bundle:
        roles.append("bundle")
    if product_type != "bundle":
        roles.append(product_type)
    return roles or ["product"]


def _find_family_for_handle(handle: str) -> str | None:
    """Find which bundle family a product handle belongs to."""
    if handle in BUNDLE_COMPONENTS:
        return handle
    for bundle_handle, components in BUNDLE_COMPONENTS.items():
        if handle in components:
            return bundle_handle
    return None


# ─────────────────────────────────────────────────────────────────────────────
# Shopify data fetching
# ─────────────────────────────────────────────────────────────────────────────

def fetch_quiz_products(client: ShopifyClient) -> dict[str, dict[str, Any]]:
    """Fetch all products from Shopify and filter to quiz-relevant handles."""
    logger.info("Fetching all products from Shopify...")
    all_products = client.get_products()
    logger.info("  Fetched %d total products", len(all_products))

    by_handle: dict[str, dict[str, Any]] = {}
    for p in all_products:
        handle = p.get("handle", "")
        if handle in ALL_QUIZ_HANDLES:
            by_handle[handle] = p

    logger.info("  Matched %d quiz-relevant products", len(by_handle))
    return by_handle


def fetch_collection_memberships(
    client: ShopifyClient,
    product_ids: set[int],
) -> dict[int, list[dict[str, Any]]]:
    """Fetch collection memberships for given product IDs."""
    logger.info("Fetching collections...")
    collections = client.get_collections()
    logger.info("  Found %d collections", len(collections))

    # Build product_id → list of {id, title} mappings
    memberships: dict[int, list[dict[str, Any]]] = {pid: [] for pid in product_ids}

    for coll in collections:
        coll_id = coll["id"]
        coll_title = coll.get("title", "")
        try:
            collects = client.get_collects(coll_id)
        except Exception:
            continue
        for c in collects:
            pid = c.get("product_id")
            if pid in memberships:
                memberships[pid].append({"id": coll_id, "title": coll_title})

    return memberships


def resolve_hair_strength_components(
    products_by_handle: dict[str, dict[str, Any]],
) -> list[str]:
    """Try to derive hair-strength-system components from Shopify product data.

    Looks at the product's body_html or tags for component references.
    Falls back to empty list if components cannot be determined.
    """
    bundle = products_by_handle.get("hair-strength-system")
    if not bundle:
        return []

    # Check tags for component handles
    tags = [t.strip().lower() for t in (bundle.get("tags", "") or "").split(",") if t.strip()]

    # Check if any individual handles appear in tags
    candidates = []
    for handle in INDIVIDUAL_HANDLES:
        if handle in tags:
            candidates.append(handle)
    if candidates:
        return candidates

    # Check body_html for individual product handle references
    body = (bundle.get("body_html") or "").lower()
    for handle in INDIVIDUAL_HANDLES:
        if handle in body and handle not in candidates:
            candidates.append(handle)
    if candidates:
        return candidates

    logger.warning("  Could not derive hair-strength-system components from Shopify data")
    return []


# ─────────────────────────────────────────────────────────────────────────────
# Normalization
# ─────────────────────────────────────────────────────────────────────────────

def normalize_product(
    product: dict[str, Any],
    collection_memberships: list[dict[str, Any]],
    bundle_map: dict[str, list[str]],
    shop_domain: str,
) -> dict[str, Any]:
    """Normalize a Shopify product into the quiz catalog schema."""
    handle = product["handle"]
    title = product.get("title", "")
    product_id = product["id"]

    is_bundle = handle in bundle_map
    product_type = derive_product_type(handle, title)
    family_handle = _find_family_for_handle(handle)

    # Quiz family metadata
    family_meta = QUIZ_FAMILIES.get(family_handle or "", {})
    quiz_collection_id = family_meta.get("quiz_collection_id", "")
    quiz_collection_name = family_meta.get("quiz_collection_name", "")

    # Variants
    variants = product.get("variants", [])
    prices = [float(v.get("price", 0)) for v in variants if v.get("price")]
    variant_ids = [v["id"] for v in variants]
    variant_skus = [v.get("sku", "") for v in variants if v.get("sku")]

    # Image
    image = product.get("image")
    featured_image = image["src"] if image else None

    # Tags
    raw_tags = product.get("tags", "") or ""
    tags = [t.strip() for t in raw_tags.split(",") if t.strip()]

    # Availability
    available = any(
        v.get("inventory_quantity", 0) > 0 or v.get("inventory_policy") == "continue"
        for v in variants
    )
    # If no inventory tracking, consider available if status is active
    if not variants:
        available = product.get("status") == "active"

    # Online store URL
    online_store_url = f"https://{shop_domain}/products/{handle}"

    # Currency from first variant's presentment prices or default
    currency = "SAR"

    # Subtitle from metafields (tagline) — best-effort from product data
    subtitle = ""
    metafields = product.get("metafields", [])
    if isinstance(metafields, list):
        for mf in metafields:
            if isinstance(mf, dict) and mf.get("key") == "tagline":
                subtitle = mf.get("value", "")
                break

    return {
        "id": product_id,
        "admin_graphql_api_id": f"gid://shopify/Product/{product_id}",
        "handle": handle,
        "title": title,
        "subtitle": subtitle,
        "vendor": product.get("vendor", "TARA"),
        "product_type": product_type,
        "status": product.get("status", "active"),
        "online_store_url": online_store_url,
        "featured_image": featured_image,
        "price_min": min(prices) if prices else 0.0,
        "price_max": max(prices) if prices else 0.0,
        "currency": currency,
        "available_for_sale": available,
        "tags": tags,
        "collection_ids": [c["id"] for c in collection_memberships],
        "collection_titles": [c["title"] for c in collection_memberships],
        "quiz_collection_id": quiz_collection_id,
        "quiz_collection_name": quiz_collection_name,
        "quiz_roles": derive_quiz_roles(product_type, is_bundle),
        "keywords": tags,
        "is_bundle": is_bundle,
        "bundle_handle": handle if is_bundle else (family_handle or ""),
        "bundle_components": bundle_map.get(handle, []),
        "variant_ids": variant_ids,
        "variant_skus": variant_skus,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Validation
# ─────────────────────────────────────────────────────────────────────────────

def validate_catalog(
    catalog: list[dict[str, Any]],
    bundle_map: dict[str, list[str]],
) -> list[str]:
    """Validate the catalog. Returns a list of error messages (empty = OK)."""
    errors: list[str] = []

    handles_in_catalog = {p["handle"] for p in catalog}

    # Check for duplicate handles
    seen: set[str] = set()
    for p in catalog:
        h = p["handle"]
        if h in seen:
            errors.append(f"Duplicate handle: {h}")
        seen.add(h)

    # Check required bundle handles
    for bh in BUNDLE_HANDLES:
        if bh not in handles_in_catalog:
            errors.append(f"Missing required bundle handle: {bh}")

    # Check required individual handles
    for ih in INDIVIDUAL_HANDLES:
        if ih not in handles_in_catalog:
            errors.append(f"Missing required individual handle: {ih}")

    # Check bundle component references
    for bundle_handle, components in bundle_map.items():
        if bundle_handle not in handles_in_catalog:
            continue
        for comp in components:
            if comp not in handles_in_catalog:
                errors.append(
                    f"Bundle {bundle_handle} references missing component: {comp}"
                )

    return errors


def print_summary(
    catalog: list[dict[str, Any]],
    errors: list[str],
    bundle_map: dict[str, list[str]],
) -> None:
    """Print a sync summary."""
    bundles = [p for p in catalog if p["is_bundle"]]
    individuals = [p for p in catalog if not p["is_bundle"]]
    handles_found = {p["handle"] for p in catalog}
    expected = ALL_QUIZ_HANDLES
    missing = expected - handles_found

    print("\n" + "=" * 60)
    print("QUIZ CATALOG SYNC SUMMARY")
    print("=" * 60)
    print(f"  Total products exported:  {len(catalog)}")
    print(f"  Bundles:                  {len(bundles)}")
    print(f"  Individual products:      {len(individuals)}")
    print(f"  Bundle handles found:     {', '.join(p['handle'] for p in bundles)}")

    if missing:
        print(f"\n  MISSING expected handles ({len(missing)}):")
        for h in sorted(missing):
            print(f"    - {h}")

    if errors:
        print(f"\n  VALIDATION ERRORS ({len(errors)}):")
        for e in errors:
            print(f"    - {e}")
    else:
        print("\n  Validation: PASSED")

    # Bundle membership summary
    print("\n  Bundle compositions:")
    for bh, comps in bundle_map.items():
        status = "OK" if bh in handles_found else "MISSING"
        print(f"    {bh} [{status}]: {', '.join(comps) if comps else '(no components resolved)'}")

    print("=" * 60 + "\n")


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def get_output_path(override: str | None = None) -> str:
    """Determine the output path for the quiz catalog."""
    if override:
        return override
    return config.get_progress_file("shopify_quiz_catalog.json")


def main() -> None:
    load_dotenv()

    parser = argparse.ArgumentParser(description="Export Tara quiz product catalog from Shopify")
    parser.add_argument("--dry-run", action="store_true", help="Fetch and validate but do not write file")
    parser.add_argument("--output", "-o", type=str, default=None, help="Override output file path")
    parser.add_argument("--skip-collections", action="store_true", help="Skip fetching collection memberships (faster)")
    args = parser.parse_args()

    # Connect to destination store
    shop_url = config.get_dest_shop_url()
    access_token = config.get_dest_access_token()
    client = ShopifyClient(shop_url, access_token)

    # Extract clean domain for URLs
    shop_info = client.get_shop()
    shop_domain = shop_info.get("domain", shop_url.replace("https://", "").replace(".myshopify.com", ".com"))

    # Fetch products
    products_by_handle = fetch_quiz_products(client)

    # Resolve hair-strength-system components dynamically
    bundle_map = dict(BUNDLE_COMPONENTS)
    if "hair-strength-system" in products_by_handle and "hair-strength-system" not in bundle_map:
        resolved = resolve_hair_strength_components(products_by_handle)
        if resolved:
            bundle_map["hair-strength-system"] = resolved
            logger.info("  Resolved hair-strength-system components: %s", resolved)
        else:
            bundle_map["hair-strength-system"] = []

    # Fetch collection memberships
    memberships: dict[int, list[dict[str, Any]]] = {}
    if not args.skip_collections:
        product_ids = {p["id"] for p in products_by_handle.values()}
        memberships = fetch_collection_memberships(client, product_ids)

    # Normalize
    catalog: list[dict[str, Any]] = []
    for handle in sorted(products_by_handle.keys()):
        product = products_by_handle[handle]
        pid = product["id"]
        coll_list = memberships.get(pid, [])
        normalized = normalize_product(product, coll_list, bundle_map, shop_domain)
        catalog.append(normalized)

    # Validate
    errors = validate_catalog(catalog, bundle_map)

    # Summary
    print_summary(catalog, errors, bundle_map)

    if errors:
        logger.error("Validation failed with %d error(s). See summary above.", len(errors))
        # Still write the file (with warnings) — don't block on non-critical gaps
        # But exit with error code if bundles or components are missing
        critical = [e for e in errors if "Missing required bundle" in e or "references missing component" in e]
        if critical:
            logger.error("Critical errors found — catalog may be incomplete.")
            if not args.dry_run:
                logger.warning("Writing catalog anyway — review errors above.")

    if args.dry_run:
        print("Dry run — no file written.")
        logger.info("Would write %d products to %s", len(catalog), get_output_path(args.output))
    else:
        output_path = get_output_path(args.output)
        save_json(catalog, output_path)
        logger.info("Wrote quiz catalog (%d products) to %s", len(catalog), output_path)
        print(f"Catalog written to: {output_path}")

    if errors:
        sys.exit(1)


if __name__ == "__main__":
    main()
