"""Tests for setup_store.py."""
import json
import os
from unittest.mock import MagicMock, patch, call

import pytest

from tara_migrate.setup.setup_store import (
    DEFAULT_METAOBJECT_DEFINITIONS as METAOBJECT_DEFINITIONS,
    DEFAULT_PRODUCT_METAFIELD_DEFINITIONS as PRODUCT_METAFIELD_DEFINITIONS,
    DEFAULT_ARTICLE_METAFIELD_DEFINITIONS as ARTICLE_METAFIELD_DEFINITIONS,
    _normalize_metafield_definition,
    _normalize_metaobject_field_definition,
    main,
)


# ---------------------------------------------------------------------------
# Constants sanity checks
# ---------------------------------------------------------------------------

class TestDefinitionConstants:
    def test_metaobject_definitions_count(self):
        assert len(METAOBJECT_DEFINITIONS) == 4

    def test_metaobject_definition_types(self):
        types = [d["type"] for d in METAOBJECT_DEFINITIONS]
        assert types == ["benefit", "faq_entry", "blog_author", "ingredient"]

    def test_benefit_fields(self):
        benefit = METAOBJECT_DEFINITIONS[0]
        keys = [f["key"] for f in benefit["fieldDefinitions"]]
        assert set(keys) == {"title", "description", "category", "icon_label"}

    def test_ingredient_has_resolve_placeholders(self):
        ingredient = METAOBJECT_DEFINITIONS[3]
        benefits_field = [f for f in ingredient["fieldDefinitions"] if f["key"] == "benefits"][0]
        assert any(v["value"].startswith("RESOLVE:") for v in benefits_field["validations"])

    def test_product_metafield_definitions_count(self):
        assert len(PRODUCT_METAFIELD_DEFINITIONS) == 19

    def test_article_metafield_definitions_count(self):
        assert len(ARTICLE_METAFIELD_DEFINITIONS) == 12

    def test_product_reference_metafields_have_resolve(self):
        ref_defs = [d for d in PRODUCT_METAFIELD_DEFINITIONS if d.get("validations")]
        assert len(ref_defs) == 2  # ingredients and faqs
        for d in ref_defs:
            assert d["validations"][0]["value"].startswith("RESOLVE:")

    def test_article_reference_metafields_have_resolve(self):
        ref_defs = [d for d in ARTICLE_METAFIELD_DEFINITIONS if d.get("validations")]
        assert len(ref_defs) == 2  # author and ingredients


# ---------------------------------------------------------------------------
# RESOLVE: placeholder resolution (via the normalize helpers)
# ---------------------------------------------------------------------------

class TestResolveMetaobjectDefinitionIds:
    """The standalone resolver was refactored into the normalize helpers.

    RESOLVE:<type> placeholders are rewritten to the destination metaobject
    definition GID; references whose target type is missing from the
    destination are dropped (not blanked) so Shopify never receives a
    dangling validation.
    """

    def _field(self, value):
        return {
            "key": "benefits",
            "type": "list.metaobject_reference",
            "validations": [{"name": "metaobject_definition_id", "value": value}],
        }

    def test_resolves_placeholder_to_dest_gid(self):
        dest = {"benefit": {"id": "gid://123"}}
        out = _normalize_metaobject_field_definition(self._field("RESOLVE:benefit"), {}, dest)
        assert out["validations"][0]["value"] == "gid://123"

    def test_unresolvable_placeholder_is_dropped(self):
        out = _normalize_metaobject_field_definition(self._field("RESOLVE:unknown"), {}, {})
        # The dangling validation is dropped entirely, so no validations remain.
        assert "validations" not in out

    def test_metafield_resolves_placeholder(self):
        definition = {
            "name": "Ingredient refs",
            "namespace": "custom",
            "key": "ingredient_refs",
            "type": "list.metaobject_reference",
            "validations": [{"name": "metaobject_definition_id", "value": "RESOLVE:ingredient"}],
        }
        dest = {"ingredient": {"id": "gid://mo/9"}}
        out = _normalize_metafield_definition(definition, "PRODUCT", {}, dest)
        assert out["validations"][0]["value"] == "gid://mo/9"

    def test_non_resolve_value_unchanged(self):
        field = {
            "key": "x",
            "type": "number_integer",
            "validations": [{"name": "min", "value": "1"}],
        }
        out = _normalize_metaobject_field_definition(field, {}, {})
        assert out["validations"][0]["value"] == "1"

    def test_no_validations(self):
        out = _normalize_metaobject_field_definition({"key": "x", "type": "single_line_text_field"}, {}, {})
        assert "validations" not in out


# ---------------------------------------------------------------------------
# main()
# ---------------------------------------------------------------------------

class TestMain:
    @patch("tara_migrate.setup.setup_store.load_dotenv")
    @patch("tara_migrate.setup.setup_store.ShopifyClient")
    @patch("sys.argv", ["tara_migrate.setup.setup_store.py"])
    def test_main_creates_all_definitions(self, MockClient, mock_dotenv):
        mock_client = MagicMock()
        MockClient.return_value = mock_client

        # No existing definitions
        mock_client.get_metaobject_definitions.return_value = []
        mock_client.get_metafield_definitions.return_value = []

        # All creates succeed
        mock_client.create_metaobject_definition.return_value = {"id": "gid://mo/1", "type": "benefit"}
        mock_client.create_metafield_definition.return_value = {"id": "gid://mf/1", "namespace": "custom", "key": "k", "name": "K"}

        os.environ["DEST_SHOP_URL"] = "test.myshopify.com"
        os.environ["DEST_ACCESS_TOKEN"] = "tok"
        try:
            main()
        finally:
            del os.environ["DEST_SHOP_URL"]
            del os.environ["DEST_ACCESS_TOKEN"]

        assert mock_client.create_metaobject_definition.call_count == 4
        # 19 product + 12 article = 31 metafield definitions
        assert mock_client.create_metafield_definition.call_count == 31

    @patch("tara_migrate.setup.setup_store.load_dotenv")
    @patch("sys.argv", ["tara_migrate.setup.setup_store.py", "--dry-run"])
    def test_main_dry_run(self, mock_dotenv, capsys):
        main()
        captured = capsys.readouterr()
        assert "DRY RUN" in captured.out
        assert "would create" in captured.out

    @patch("tara_migrate.setup.setup_store.load_dotenv")
    @patch("tara_migrate.setup.setup_store.ShopifyClient")
    @patch("sys.argv", ["tara_migrate.setup.setup_store.py"])
    def test_main_skips_existing(self, MockClient, mock_dotenv):
        mock_client = MagicMock()
        MockClient.return_value = mock_client

        # All 4 metaobject defs already exist
        mock_client.get_metaobject_definitions.return_value = [
            {"type": "benefit", "id": "gid://1"},
            {"type": "faq_entry", "id": "gid://2"},
            {"type": "blog_author", "id": "gid://3"},
            {"type": "ingredient", "id": "gid://4"},
        ]
        # All metafield defs already exist
        existing_mfs = []
        for d in PRODUCT_METAFIELD_DEFINITIONS:
            existing_mfs.append({"namespace": d["namespace"], "key": d["key"]})
        for d in ARTICLE_METAFIELD_DEFINITIONS:
            existing_mfs.append({"namespace": d["namespace"], "key": d["key"]})
        mock_client.get_metafield_definitions.return_value = existing_mfs

        os.environ["DEST_SHOP_URL"] = "test.myshopify.com"
        os.environ["DEST_ACCESS_TOKEN"] = "tok"
        try:
            main()
        finally:
            del os.environ["DEST_SHOP_URL"]
            del os.environ["DEST_ACCESS_TOKEN"]

        mock_client.create_metaobject_definition.assert_not_called()
        mock_client.create_metafield_definition.assert_not_called()

    @patch("tara_migrate.setup.setup_store.load_dotenv")
    @patch("tara_migrate.setup.setup_store.ShopifyClient")
    @patch("sys.argv", ["tara_migrate.setup.setup_store.py"])
    def test_main_handles_creation_error(self, MockClient, mock_dotenv, capsys):
        mock_client = MagicMock()
        MockClient.return_value = mock_client
        mock_client.get_metaobject_definitions.return_value = []
        mock_client.get_metafield_definitions.return_value = []
        mock_client.create_metaobject_definition.side_effect = Exception("API error")
        mock_client.create_metafield_definition.return_value = {"id": "gid://1", "namespace": "custom", "key": "k", "name": "K"}

        os.environ["DEST_SHOP_URL"] = "test.myshopify.com"
        os.environ["DEST_ACCESS_TOKEN"] = "tok"
        try:
            main()
        finally:
            del os.environ["DEST_SHOP_URL"]
            del os.environ["DEST_ACCESS_TOKEN"]

        captured = capsys.readouterr()
        assert "error" in captured.out.lower()

    @patch("tara_migrate.setup.setup_store.load_dotenv")
    @patch("tara_migrate.setup.setup_store.ShopifyClient")
    @patch("sys.argv", ["tara_migrate.setup.setup_store.py"])
    def test_main_already_exists_via_api(self, MockClient, mock_dotenv):
        mock_client = MagicMock()
        MockClient.return_value = mock_client
        mock_client.get_metaobject_definitions.return_value = []
        mock_client.get_metafield_definitions.return_value = []
        # Return None (already exists via API)
        mock_client.create_metaobject_definition.return_value = None
        mock_client.create_metafield_definition.return_value = None

        os.environ["DEST_SHOP_URL"] = "test.myshopify.com"
        os.environ["DEST_ACCESS_TOKEN"] = "tok"
        try:
            main()
        finally:
            del os.environ["DEST_SHOP_URL"]
            del os.environ["DEST_ACCESS_TOKEN"]
