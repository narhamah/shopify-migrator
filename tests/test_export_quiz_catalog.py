"""Tests for the quiz catalog exporter."""
import pytest

from tara_migrate.tools.export_quiz_catalog import (
    ALL_QUIZ_HANDLES,
    BUNDLE_HANDLES,
    INDIVIDUAL_HANDLES,
    derive_product_type,
    derive_quiz_roles,
    extract_bundle_map,
    normalize_product,
    validate_catalog,
)


# ---------------------------------------------------------------------------
# Helpers — build GraphQL-shaped product dicts
# ---------------------------------------------------------------------------

def _gql_product(handle, title="Test Product", price="149.00", pid=1001,
                 bundle_components=None, tags=None, status="ACTIVE",
                 available=True, tagline=None):
    """Build a product dict matching the GraphQL response shape."""
    gid = f"gid://shopify/Product/{pid}"
    variant_gid = f"gid://shopify/ProductVariant/{pid + 9000}"
    comps = bundle_components or []
    return {
        "id": gid,
        "handle": handle,
        "title": title,
        "vendor": "TARA",
        "status": status,
        "tags": tags or [],
        "onlineStoreUrl": f"https://sa.taraformula.com/products/{handle}",
        "featuredMedia": {
            "preview": {
                "image": {"url": f"https://cdn.shopify.com/{handle}.jpg"}
            }
        },
        "priceRangeV2": {
            "minVariantPrice": {"amount": price, "currencyCode": "SAR"},
            "maxVariantPrice": {"amount": price, "currencyCode": "SAR"},
        },
        "totalInventory": 10,
        "hasOnlyDefaultVariant": True,
        "variants": {
            "edges": [{
                "node": {
                    "id": variant_gid,
                    "sku": f"SKU-{handle.upper()[:10]}",
                    "price": price,
                    "availableForSale": available,
                    "inventoryQuantity": 10,
                }
            }]
        },
        "bundleComponents": {
            "edges": [
                {
                    "node": {
                        "componentProduct": {
                            "id": f"gid://shopify/Product/{2000 + i}",
                            "handle": ch,
                            "title": ch.replace("-", " ").title(),
                        },
                        "quantity": 1,
                    }
                }
                for i, ch in enumerate(comps)
            ]
        },
        "metafield": {"value": tagline} if tagline else None,
        "collections": {
            "edges": [
                {"node": {"id": "gid://shopify/Collection/3001", "title": "All Products"}}
            ]
        },
    }


# ---------------------------------------------------------------------------
# derive_product_type
# ---------------------------------------------------------------------------

class TestDeriveProductType:
    def test_bundle(self):
        assert derive_product_type("nurture-system", "Nurture System", True) == "bundle"

    def test_hair_strength_bundle(self):
        assert derive_product_type("hair-strength-system", "Hair Strength System", True) == "bundle"

    def test_shampoo(self):
        assert derive_product_type("nurture-shampoo", "Nurture Shampoo", False) == "shampoo"

    def test_conditioner(self):
        assert derive_product_type("hydrating-conditioner", "Hydrating Conditioner", False) == "conditioner"

    def test_serum(self):
        assert derive_product_type("rejuvenating-scalp-serum", "Rejuvenating Scalp Serum", False) == "serum"

    def test_leave_in(self):
        assert derive_product_type("nurture-leave-in-conditioner", "Nurture Leave-In Conditioner", False) == "leave-in"

    def test_mask(self):
        assert derive_product_type("repairing-hair-mask", "Repairing Hair Mask", False) == "mask"

    def test_unknown(self):
        assert derive_product_type("mystery-product", "Mystery Product", False) == "other"


# ---------------------------------------------------------------------------
# derive_quiz_roles
# ---------------------------------------------------------------------------

class TestDeriveQuizRoles:
    def test_bundle(self):
        assert derive_quiz_roles("bundle", True) == ["bundle"]

    def test_shampoo_not_bundle(self):
        assert derive_quiz_roles("shampoo", False) == ["shampoo"]

    def test_serum_not_bundle(self):
        assert derive_quiz_roles("serum", False) == ["serum"]

    def test_other_no_bundle(self):
        assert derive_quiz_roles("other", False) == ["other"]


# ---------------------------------------------------------------------------
# extract_bundle_map
# ---------------------------------------------------------------------------

class TestExtractBundleMap:
    def test_extracts_components(self):
        products = {
            "nurture-system": _gql_product(
                "nurture-system", "Nurture System",
                bundle_components=["nurture-shampoo", "nurture-conditioner", "nurture-leave-in-conditioner"],
            ),
        }
        bundle_map = extract_bundle_map(products)
        assert bundle_map["nurture-system"] == [
            "nurture-shampoo", "nurture-conditioner", "nurture-leave-in-conditioner"
        ]

    def test_empty_when_no_components(self):
        products = {
            "nurture-system": _gql_product("nurture-system", "Nurture System"),
        }
        bundle_map = extract_bundle_map(products)
        assert bundle_map["nurture-system"] == []

    def test_skips_missing_handles(self):
        products = {}
        bundle_map = extract_bundle_map(products)
        for bh in BUNDLE_HANDLES:
            assert bh not in bundle_map


# ---------------------------------------------------------------------------
# normalize_product
# ---------------------------------------------------------------------------

class TestNormalizeProduct:
    def test_basic_fields(self):
        product = _gql_product("nurture-shampoo", "Nurture Shampoo", price="149.00", pid=1001)
        bundle_map = {"nurture-system": ["nurture-shampoo", "nurture-conditioner", "nurture-leave-in-conditioner"]}
        result = normalize_product(product, bundle_map, "sa.taraformula.com")

        assert result["handle"] == "nurture-shampoo"
        assert result["title"] == "Nurture Shampoo"
        assert result["id"] == 1001
        assert result["admin_graphql_api_id"] == "gid://shopify/Product/1001"
        assert result["product_type"] == "shampoo"
        assert result["is_bundle"] is False
        assert result["available_for_sale"] is True
        assert result["online_store_url"] == "https://sa.taraformula.com/products/nurture-shampoo"

    def test_bundle_product(self):
        product = _gql_product(
            "nurture-system", "Nurture System", price="379.00",
            bundle_components=["nurture-shampoo", "nurture-conditioner", "nurture-leave-in-conditioner"],
        )
        bundle_map = extract_bundle_map({"nurture-system": product})
        result = normalize_product(product, bundle_map, "sa.taraformula.com")

        assert result["is_bundle"] is True
        assert result["product_type"] == "bundle"
        assert result["bundle_handle"] == "nurture-system"
        assert result["bundle_components"] == [
            "nurture-shampoo", "nurture-conditioner", "nurture-leave-in-conditioner"
        ]
        assert "bundle" in result["quiz_roles"]

    def test_collection_memberships_from_graphql(self):
        product = _gql_product("nurture-shampoo", "Nurture Shampoo")
        result = normalize_product(product, {}, "sa.taraformula.com")

        assert result["collection_ids"] == [3001]
        assert result["collection_titles"] == ["All Products"]

    def test_prices_from_price_range(self):
        product = _gql_product("nurture-shampoo", "Nurture Shampoo", price="149.00")
        result = normalize_product(product, {}, "sa.taraformula.com")

        assert result["price_min"] == 149.0
        assert result["price_max"] == 149.0
        assert result["currency"] == "SAR"

    def test_quiz_family_for_bundle(self):
        product = _gql_product("nurture-system", "Nurture System")
        bundle_map = {"nurture-system": ["nurture-shampoo"]}
        result = normalize_product(product, bundle_map, "sa.taraformula.com")

        assert result["quiz_collection_id"] == "nurture"
        assert result["quiz_collection_name"] == "Nurture"

    def test_quiz_family_for_individual(self):
        product = _gql_product("nourishing-shampoo", "Nourishing Shampoo")
        bundle_map = {"hair-wellness-system": ["nourishing-shampoo", "hydrating-conditioner", "rejuvenating-scalp-serum"]}
        result = normalize_product(product, bundle_map, "sa.taraformula.com")

        assert result["quiz_collection_id"] == "hair-wellness"
        assert result["quiz_collection_name"] == "Hair Wellness"
        assert result["bundle_handle"] == "hair-wellness-system"
        assert result["is_bundle"] is False

    def test_subtitle_from_metafield(self):
        product = _gql_product("nurture-shampoo", "Nurture Shampoo", tagline="Luxury scalp care")
        result = normalize_product(product, {}, "sa.taraformula.com")
        assert result["subtitle"] == "Luxury scalp care"

    def test_variant_ids_and_skus(self):
        product = _gql_product("nurture-shampoo", "Nurture Shampoo", pid=1001)
        result = normalize_product(product, {}, "sa.taraformula.com")
        assert result["variant_ids"] == [10001]
        assert len(result["variant_skus"]) == 1


# ---------------------------------------------------------------------------
# validate_catalog
# ---------------------------------------------------------------------------

class TestValidateCatalog:
    def _build_catalog(self, handles):
        return [{"handle": h, "is_bundle": h in BUNDLE_HANDLES} for h in handles]

    def test_valid_catalog(self):
        catalog = self._build_catalog(ALL_QUIZ_HANDLES)
        bundle_map = {bh: ["comp1"] for bh in BUNDLE_HANDLES}
        errors = validate_catalog(catalog, bundle_map)
        # No missing-handle or duplicate errors
        handle_errors = [e for e in errors if "Duplicate" in e or "Missing required" in e]
        assert handle_errors == []

    def test_missing_bundle_handle(self):
        handles = ALL_QUIZ_HANDLES - {"nurture-system"}
        catalog = self._build_catalog(handles)
        errors = validate_catalog(catalog, {})
        assert any("Missing required bundle handle: nurture-system" in e for e in errors)

    def test_missing_individual_handle(self):
        handles = ALL_QUIZ_HANDLES - {"nurture-shampoo"}
        catalog = self._build_catalog(handles)
        errors = validate_catalog(catalog, {})
        assert any("Missing required individual handle: nurture-shampoo" in e for e in errors)

    def test_duplicate_handle(self):
        catalog = [{"handle": "nurture-shampoo", "is_bundle": False}] * 2
        errors = validate_catalog(catalog, {})
        assert any("Duplicate handle: nurture-shampoo" in e for e in errors)

    def test_missing_component(self):
        catalog = self._build_catalog({"nurture-system"})
        bundle_map = {"nurture-system": ["nurture-shampoo"]}
        errors = validate_catalog(catalog, bundle_map)
        assert any("references missing component: nurture-shampoo" in e for e in errors)

    def test_empty_bundle_components(self):
        catalog = self._build_catalog({"nurture-system"})
        bundle_map = {"nurture-system": []}
        errors = validate_catalog(catalog, bundle_map)
        assert any("has no components" in e for e in errors)


# ---------------------------------------------------------------------------
# Handle completeness
# ---------------------------------------------------------------------------

class TestHandleCompleteness:
    def test_all_bundle_handles_in_quiz_handles(self):
        for bh in BUNDLE_HANDLES:
            assert bh in ALL_QUIZ_HANDLES

    def test_all_individual_handles_in_quiz_handles(self):
        for ih in INDIVIDUAL_HANDLES:
            assert ih in ALL_QUIZ_HANDLES

    def test_no_overlap_bundles_and_individuals(self):
        overlap = set(BUNDLE_HANDLES) & set(INDIVIDUAL_HANDLES)
        assert overlap == set(), f"Handles in both BUNDLE and INDIVIDUAL: {overlap}"
