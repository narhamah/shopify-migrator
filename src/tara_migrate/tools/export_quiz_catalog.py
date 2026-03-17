#!/usr/bin/env python3
"""Export a normalized Tara quiz product catalog from Shopify.

Fetches live product data from Shopify via GraphQL (by handle) and produces
a structured JSON file that the quiz frontend and Cloudflare Worker can
consume for recommendation handle matching and bundle rendering.

Bundle component relationships are queried live from Shopify's bundle API,
not hardcoded.

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
# Quiz-relevant product handles (expected in the store)
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
    "invigorating-shampoo",
    "repairing-hair-mask",
    "strengthening-scalp-serum",
]

ALL_QUIZ_HANDLES = set(BUNDLE_HANDLES + INDIVIDUAL_HANDLES)

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


def derive_product_type(handle: str, title: str, is_bundle: bool) -> str:
    """Derive quiz product_type from handle/title deterministically."""
    if is_bundle:
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


def _find_family_for_handle(handle: str, bundle_map: dict[str, list[str]]) -> str | None:
    """Find which bundle family a product handle belongs to."""
    if handle in bundle_map:
        return handle
    for bundle_handle, components in bundle_map.items():
        if handle in components:
            return bundle_handle
    return None


# ─────────────────────────────────────────────────────────────────────────────
# GraphQL queries — fetch products by handle with bundle components
# ─────────────────────────────────────────────────────────────────────────────

PRODUCT_BY_HANDLE_QUERY = """
query productByHandle($handle: String!) {
  productByHandle(handle: $handle) {
    id
    handle
    title
    vendor
    status
    tags
    onlineStoreUrl
    featuredMedia {
      preview {
        image {
          url
        }
      }
    }
    priceRangeV2 {
      minVariantPrice {
        amount
        currencyCode
      }
      maxVariantPrice {
        amount
        currencyCode
      }
    }
    totalInventory
    hasOnlyDefaultVariant
    variants(first: 100) {
      edges {
        node {
          id
          sku
          price
          availableForSale
          inventoryQuantity
        }
      }
    }
    bundleComponents(first: 20) {
      edges {
        node {
          componentProduct {
            id
            handle
            title
          }
          quantity
        }
      }
    }
    metafield(namespace: "custom", key: "tagline") {
      value
    }
    collections(first: 50) {
      edges {
        node {
          id
          title
        }
      }
    }
  }
}
"""


def _gid_to_numeric(gid: str) -> int:
    """Extract numeric ID from a Shopify GID string."""
    return int(gid.split("/")[-1])


# ─────────────────────────────────────────────────────────────────────────────
# Shopify data fetching
# ─────────────────────────────────────────────────────────────────────────────

def fetch_product_by_handle(client: ShopifyClient, handle: str) -> dict[str, Any] | None:
    """Fetch a single product by handle via GraphQL, including bundle components."""
    data = client._graphql(PRODUCT_BY_HANDLE_QUERY, {"handle": handle})
    return data.get("productByHandle")


def fetch_quiz_products(client: ShopifyClient) -> dict[str, dict[str, Any]]:
    """Fetch all quiz-relevant products by handle from Shopify."""
    by_handle: dict[str, dict[str, Any]] = {}
    missing: list[str] = []

    for handle in sorted(ALL_QUIZ_HANDLES):
        logger.info("  Fetching %s...", handle)
        product = fetch_product_by_handle(client, handle)
        if product:
            by_handle[handle] = product
        else:
            missing.append(handle)
            logger.warning("  Product not found: %s", handle)

    logger.info("Fetched %d quiz products (%d missing)", len(by_handle), len(missing))
    return by_handle


# ─────────────────────────────────────────────────────────────────────────────
# Bundle component extraction from GraphQL response
# ─────────────────────────────────────────────────────────────────────────────

def extract_bundle_map(products_by_handle: dict[str, dict[str, Any]]) -> dict[str, list[str]]:
    """Build bundle → component handle map from live Shopify bundle data."""
    bundle_map: dict[str, list[str]] = {}

    for handle in BUNDLE_HANDLES:
        product = products_by_handle.get(handle)
        if not product:
            continue

        components_edges = (product.get("bundleComponents") or {}).get("edges", [])
        component_handles = []
        for edge in components_edges:
            node = edge.get("node", {})
            comp_product = node.get("componentProduct", {})
            comp_handle = comp_product.get("handle")
            if comp_handle:
                component_handles.append(comp_handle)

        bundle_map[handle] = component_handles

    return bundle_map


# ─────────────────────────────────────────────────────────────────────────────
# Normalization
# ─────────────────────────────────────────────────────────────────────────────

def normalize_product(
    product: dict[str, Any],
    bundle_map: dict[str, list[str]],
    shop_domain: str,
) -> dict[str, Any]:
    """Normalize a Shopify GraphQL product into the quiz catalog schema."""
    handle = product["handle"]
    title = product.get("title", "")
    gid = product["id"]
    product_id = _gid_to_numeric(gid)

    is_bundle = handle in bundle_map and len(bundle_map[handle]) > 0
    product_type = derive_product_type(handle, title, is_bundle)
    family_handle = _find_family_for_handle(handle, bundle_map)

    # Quiz family metadata
    family_meta = QUIZ_FAMILIES.get(family_handle or "", {})
    quiz_collection_id = family_meta.get("quiz_collection_id", "")
    quiz_collection_name = family_meta.get("quiz_collection_name", "")

    # Variants
    variant_edges = (product.get("variants") or {}).get("edges", [])
    variants = [e["node"] for e in variant_edges]
    prices = [float(v.get("price", 0)) for v in variants if v.get("price")]
    variant_ids = [_gid_to_numeric(v["id"]) for v in variants]
    variant_skus = [v.get("sku", "") for v in variants if v.get("sku")]

    # Price range (prefer priceRangeV2 for accuracy)
    price_range = product.get("priceRangeV2") or {}
    min_price = price_range.get("minVariantPrice", {})
    max_price = price_range.get("maxVariantPrice", {})
    price_min = float(min_price.get("amount", 0)) if min_price else (min(prices) if prices else 0.0)
    price_max = float(max_price.get("amount", 0)) if max_price else (max(prices) if prices else 0.0)
    currency = min_price.get("currencyCode", "SAR") if min_price else "SAR"

    # Image
    featured_media = product.get("featuredMedia") or {}
    preview = featured_media.get("preview") or {}
    image_data = preview.get("image") or {}
    featured_image = image_data.get("url")

    # Tags
    tags = product.get("tags") or []

    # Availability
    available = any(v.get("availableForSale", False) for v in variants)

    # Online store URL
    online_store_url = product.get("onlineStoreUrl") or f"https://{shop_domain}/products/{handle}"

    # Subtitle from tagline metafield
    tagline_mf = product.get("metafield") or {}
    subtitle = tagline_mf.get("value", "")

    # Collections
    collection_edges = (product.get("collections") or {}).get("edges", [])
    collections = [e["node"] for e in collection_edges]
    collection_ids = [_gid_to_numeric(c["id"]) for c in collections]
    collection_titles = [c.get("title", "") for c in collections]

    # Status
    status = (product.get("status") or "ACTIVE").lower()

    return {
        "id": product_id,
        "admin_graphql_api_id": gid,
        "handle": handle,
        "title": title,
        "subtitle": subtitle,
        "vendor": product.get("vendor", "TARA"),
        "product_type": product_type,
        "status": status,
        "online_store_url": online_store_url,
        "featured_image": featured_image,
        "price_min": price_min,
        "price_max": price_max,
        "currency": currency,
        "available_for_sale": available,
        "tags": tags,
        "collection_ids": collection_ids,
        "collection_titles": collection_titles,
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
        if not components:
            errors.append(f"Bundle {bundle_handle} has no components")
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
    print("\n  Bundle compositions (from Shopify):")
    for bh in BUNDLE_HANDLES:
        comps = bundle_map.get(bh, [])
        status = "OK" if bh in handles_found else "MISSING"
        comp_str = ", ".join(comps) if comps else "(no components found)"
        print(f"    {bh} [{status}]: {comp_str}")

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
    args = parser.parse_args()

    # Connect to destination store
    shop_url = config.get_dest_shop_url()
    access_token = config.get_dest_access_token()
    client = ShopifyClient(shop_url, access_token)

    # Extract clean domain for URLs
    shop_info = client.get_shop()
    shop_domain = shop_info.get("domain", shop_url.replace("https://", "").replace(".myshopify.com", ".com"))

    # Fetch each quiz product by handle via GraphQL (includes bundle components)
    products_by_handle = fetch_quiz_products(client)

    # Extract bundle composition from live Shopify data
    bundle_map = extract_bundle_map(products_by_handle)
    logger.info("Bundle map from Shopify: %s",
                {k: v for k, v in bundle_map.items()})

    # Any component handles found that we didn't already expect?
    all_component_handles = {h for comps in bundle_map.values() for h in comps}
    unexpected = all_component_handles - ALL_QUIZ_HANDLES
    if unexpected:
        logger.info("Discovered %d additional component handles from Shopify bundles: %s",
                     len(unexpected), unexpected)
        # Fetch any unexpected component products we don't have yet
        for handle in sorted(unexpected):
            if handle not in products_by_handle:
                logger.info("  Fetching discovered component: %s", handle)
                product = fetch_product_by_handle(client, handle)
                if product:
                    products_by_handle[handle] = product

    # Normalize
    catalog: list[dict[str, Any]] = []
    for handle in sorted(products_by_handle.keys()):
        product = products_by_handle[handle]
        normalized = normalize_product(product, bundle_map, shop_domain)
        catalog.append(normalized)

    # Validate
    errors = validate_catalog(catalog, bundle_map)

    # Summary
    print_summary(catalog, errors, bundle_map)

    if errors:
        logger.error("Validation failed with %d error(s). See summary above.", len(errors))
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
