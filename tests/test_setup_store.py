"""Tests for setup_store.py."""
import json
import os
from unittest.mock import MagicMock, patch, call

import pytest

from tara_migrate.setup.setup_store import (
    METAOBJECT_DEFINITIONS,
    PRODUCT_METAFIELD_DEFINITIONS,
    ARTICLE_METAFIELD_DEFINITIONS,
    resolve_metaobject_definition_ids,
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
# resolve_metaobject_definition_ids
# ---------------------------------------------------------------------------

class TestResolveMetaobjectDefinitionIds:
    def test_resolves_placeholders(self):
        definitions = [
            {"key": "benefits", "validations": [{"name": "metaobject_definition_id", "value": "RESOLVE:benefit"}]},
        ]
        existing = {"benefit": {"id": "gid://123"}}
        resolve_metaobject_definition_ids(definitions, existing)
        assert definitions[0]["validations"][0]["value"] == "gid://123"

    def test_unresolvable_placeholder(self, capsys):
        definitions = [
            {"key": "x", "validations": [{"name": "metaobject_definition_id", "value": "RESOLVE:unknown"}]},
        ]
        resolve_metaobject_definition_ids(definitions, {})
        assert definitions[0]["validations"][0]["value"] == ""
        captured = capsys.readouterr()
        assert "WARNING" in captured.out

    def test_non_resolve_value_unchanged(self):
        definitions = [
            {"key": "x", "validations": [{"name": "min", "value": "1"}]},
        ]
        resolve_metaobject_definition_ids(definitions, {})
        assert definitions[0]["validations"][0]["value"] == "1"

    def test_no_validations(self):
        definitions = [{"key": "x"}]
        resolve_metaobject_definition_ids(definitions, {})
        assert "validations" not in definitions[0]


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

        os.environ["SAUDI_SHOP_URL"] = "test.myshopify.com"
        os.environ["SAUDI_ACCESS_TOKEN"] = "tok"
        try:
            main()
        finally:
            del os.environ["SAUDI_SHOP_URL"]
            del os.environ["SAUDI_ACCESS_TOKEN"]

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

        os.environ["SAUDI_SHOP_URL"] = "test.myshopify.com"
        os.environ["SAUDI_ACCESS_TOKEN"] = "tok"
        try:
            main()
        finally:
            del os.environ["SAUDI_SHOP_URL"]
            del os.environ["SAUDI_ACCESS_TOKEN"]

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

        os.environ["SAUDI_SHOP_URL"] = "test.myshopify.com"
        os.environ["SAUDI_ACCESS_TOKEN"] = "tok"
        try:
            main()
        finally:
            del os.environ["SAUDI_SHOP_URL"]
            del os.environ["SAUDI_ACCESS_TOKEN"]

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

        os.environ["SAUDI_SHOP_URL"] = "test.myshopify.com"
        os.environ["SAUDI_ACCESS_TOKEN"] = "tok"
        try:
            main()
        finally:
            del os.environ["SAUDI_SHOP_URL"]
            del os.environ["SAUDI_ACCESS_TOKEN"]
