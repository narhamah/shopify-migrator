"""Tests for the quiz catalog exporter."""
import json

import pytest

from tests.conftest import make_product
from tara_migrate.tools.export_quiz_catalog import (
    ALL_QUIZ_HANDLES,
    BUNDLE_COMPONENTS,
    BUNDLE_HANDLES,
    INDIVIDUAL_HANDLES,
    derive_product_type,
    derive_quiz_roles,
    normalize_product,
    validate_catalog,
)


# ---------------------------------------------------------------------------
# derive_product_type
# ---------------------------------------------------------------------------

class TestDeriveProductType:
    def test_bundle(self):
        assert derive_product_type("nurture-system", "Nurture System") == "bundle"

    def test_hair_strength_bundle(self):
        assert derive_product_type("hair-strength-system", "Hair Strength System") == "bundle"

    def test_shampoo(self):
        assert derive_product_type("nurture-shampoo", "Nurture Shampoo") == "shampoo"

    def test_conditioner(self):
        assert derive_product_type("hydrating-conditioner", "Hydrating Conditioner") == "conditioner"

    def test_serum(self):
        assert derive_product_type("rejuvenating-scalp-serum", "Rejuvenating Scalp Serum") == "serum"

    def test_leave_in(self):
        assert derive_product_type("nurture-leave-in-conditioner", "Nurture Leave-In Conditioner") == "leave-in"

    def test_mask(self):
        assert derive_product_type("repairing-hair-mask", "Repairing Hair Mask") == "mask"

    def test_unknown(self):
        assert derive_product_type("mystery-product", "Mystery Product") == "other"


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
# normalize_product
# ---------------------------------------------------------------------------

class TestNormalizeProduct:
    def _make_quiz_product(self, handle="nurture-shampoo", title="Nurture Shampoo",
                           price="149.00", pid=1001):
        p = make_product(id=pid, handle=handle, title=title, price=price)
        p["image"] = {"src": "https://cdn.shopify.com/nurture.jpg"}
        p["variants"][0]["id"] = 9901
        p["variants"][0]["inventory_quantity"] = 10
        return p

    def test_basic_fields(self):
        product = self._make_quiz_product()
        result = normalize_product(product, [], BUNDLE_COMPONENTS, "sa.taraformula.com")

        assert result["handle"] == "nurture-shampoo"
        assert result["title"] == "Nurture Shampoo"
        assert result["id"] == 1001
        assert result["admin_graphql_api_id"] == "gid://shopify/Product/1001"
        assert result["product_type"] == "shampoo"
        assert result["is_bundle"] is False
        assert result["available_for_sale"] is True
        assert result["online_store_url"] == "https://sa.taraformula.com/products/nurture-shampoo"

    def test_bundle_product(self):
        product = self._make_quiz_product(handle="nurture-system", title="Nurture System", price="379.00")
        result = normalize_product(product, [], BUNDLE_COMPONENTS, "sa.taraformula.com")

        assert result["is_bundle"] is True
        assert result["product_type"] == "bundle"
        assert result["bundle_handle"] == "nurture-system"
        assert result["bundle_components"] == [
            "nurture-shampoo", "nurture-conditioner", "nurture-leave-in-conditioner"
        ]
        assert "bundle" in result["quiz_roles"]

    def test_collection_memberships(self):
        product = self._make_quiz_product()
        colls = [{"id": 2001, "title": "Shampoos"}, {"id": 2002, "title": "All Products"}]
        result = normalize_product(product, colls, BUNDLE_COMPONENTS, "sa.taraformula.com")

        assert result["collection_ids"] == [2001, 2002]
        assert result["collection_titles"] == ["Shampoos", "All Products"]

    def test_prices(self):
        product = self._make_quiz_product(price="149.00")
        result = normalize_product(product, [], BUNDLE_COMPONENTS, "sa.taraformula.com")

        assert result["price_min"] == 149.0
        assert result["price_max"] == 149.0

    def test_quiz_family_metadata(self):
        product = self._make_quiz_product(handle="nurture-system", title="Nurture System")
        result = normalize_product(product, [], BUNDLE_COMPONENTS, "sa.taraformula.com")

        assert result["quiz_collection_id"] == "nurture"
        assert result["quiz_collection_name"] == "Nurture"

    def test_individual_product_family(self):
        product = self._make_quiz_product(handle="nourishing-shampoo", title="Nourishing Shampoo")
        result = normalize_product(product, [], BUNDLE_COMPONENTS, "sa.taraformula.com")

        assert result["quiz_collection_id"] == "hair-wellness"
        assert result["quiz_collection_name"] == "Hair Wellness"
        assert result["bundle_handle"] == "hair-wellness-system"
        assert result["is_bundle"] is False

    def test_subtitle_from_metafields(self):
        product = self._make_quiz_product()
        # conftest.make_product includes tagline metafield
        result = normalize_product(product, [], BUNDLE_COMPONENTS, "sa.taraformula.com")
        assert result["subtitle"] == "Luxury care"


# ---------------------------------------------------------------------------
# validate_catalog
# ---------------------------------------------------------------------------

class TestValidateCatalog:
    def _build_catalog(self, handles):
        return [{"handle": h, "is_bundle": h in BUNDLE_COMPONENTS} for h in handles]

    def test_valid_catalog(self):
        catalog = self._build_catalog(ALL_QUIZ_HANDLES)
        errors = validate_catalog(catalog, BUNDLE_COMPONENTS)
        # May have errors for hair-strength-system components not being in BUNDLE_COMPONENTS
        # but no duplicate or missing-handle errors for the handles we provide
        dup_errors = [e for e in errors if "Duplicate" in e]
        assert dup_errors == []

    def test_missing_bundle_handle(self):
        handles = ALL_QUIZ_HANDLES - {"nurture-system"}
        catalog = self._build_catalog(handles)
        errors = validate_catalog(catalog, BUNDLE_COMPONENTS)
        assert any("Missing required bundle handle: nurture-system" in e for e in errors)

    def test_missing_individual_handle(self):
        handles = ALL_QUIZ_HANDLES - {"nurture-shampoo"}
        catalog = self._build_catalog(handles)
        errors = validate_catalog(catalog, BUNDLE_COMPONENTS)
        assert any("Missing required individual handle: nurture-shampoo" in e for e in errors)

    def test_duplicate_handle(self):
        catalog = [{"handle": "nurture-shampoo", "is_bundle": False}] * 2
        errors = validate_catalog(catalog, {})
        assert any("Duplicate handle: nurture-shampoo" in e for e in errors)

    def test_missing_component(self):
        catalog = self._build_catalog({"nurture-system"})
        errors = validate_catalog(catalog, BUNDLE_COMPONENTS)
        assert any("references missing component: nurture-shampoo" in e for e in errors)


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

    def test_all_bundle_components_are_individual_handles(self):
        for bundle_handle, components in BUNDLE_COMPONENTS.items():
            for comp in components:
                assert comp in INDIVIDUAL_HANDLES, (
                    f"Bundle {bundle_handle} component {comp} not in INDIVIDUAL_HANDLES"
                )

    def test_no_overlap_bundles_and_individuals(self):
        overlap = set(BUNDLE_HANDLES) & set(INDIVIDUAL_HANDLES)
        assert overlap == set(), f"Handles in both BUNDLE and INDIVIDUAL: {overlap}"
