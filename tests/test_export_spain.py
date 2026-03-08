"""Tests for export_spain.py."""
import json
import os
from unittest.mock import MagicMock, patch

import pytest

from export_spain import ensure_dir, save_json, main


class TestEnsureDir:
    def test_creates_directory(self, tmp_path):
        new_dir = tmp_path / "subdir"
        ensure_dir(str(new_dir))
        assert new_dir.is_dir()

    def test_existing_directory_ok(self, tmp_path):
        ensure_dir(str(tmp_path))
        assert tmp_path.is_dir()


class TestSaveJson:
    def test_writes_json(self, tmp_path):
        filepath = str(tmp_path / "out.json")
        save_json({"key": "value"}, filepath)
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)
        assert data["key"] == "value"

    def test_utf8_encoding(self, tmp_path):
        filepath = str(tmp_path / "out.json")
        save_json({"name": "TARA عربي"}, filepath)
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)
        assert "عربي" in data["name"]

    def test_pretty_printed(self, tmp_path):
        filepath = str(tmp_path / "out.json")
        save_json({"a": 1}, filepath)
        with open(filepath, "r") as f:
            text = f.read()
        assert "\n" in text  # Indented


class TestExportMain:
    @patch("export_spain.load_dotenv")
    @patch("export_spain.ShopifyClient")
    def test_full_export(self, MockClient, mock_dotenv, tmp_path):
        mock_client = MagicMock()
        MockClient.return_value = mock_client

        mock_client.get_shop.return_value = {"name": "Test Shop"}
        mock_client.get_products.return_value = [
            {"id": 1, "title": "Product 1"},
            {"id": 2, "title": "Product 2"},
        ]
        mock_client.get_metafields.return_value = [{"namespace": "custom", "key": "k", "value": "v"}]
        mock_client.get_collections.return_value = [{"id": 10, "title": "Coll"}]
        mock_client.get_pages.return_value = [{"id": 20, "title": "Page"}]
        mock_client.get_blogs.return_value = [{"id": 30, "title": "Blog", "handle": "blog"}]
        mock_client.get_articles.return_value = [{"id": 40, "title": "Art"}]
        mock_client.get_metaobject_definitions.return_value = [
            {"type": "benefit", "name": "Benefit"}
        ]
        mock_client.get_metaobjects.return_value = [
            {"id": "gid://1", "handle": "h1", "type": "benefit", "fields": []}
        ]
        mock_client.get_collects.return_value = [{"id": 1, "product_id": 1, "collection_id": 10}]
        mock_client.get_redirects.return_value = []
        mock_client.get_policies.return_value = []

        os.environ["SPAIN_SHOP_URL"] = "spain.myshopify.com"
        os.environ["SPAIN_ACCESS_TOKEN"] = "tok"

        # Patch the output dir to use tmp_path
        with patch("export_spain.os.makedirs"):
            with patch("builtins.open", create=True) as mock_open:
                # We can't easily redirect file writes without more complex patching.
                # Instead, test the function calls to verify correctness.
                main()

        # Verify products got metafields attached
        # 2 products + 1 article + 1 collection + 1 page = 5
        assert mock_client.get_metafields.call_count == 5

        # Verify articles got _blog_id attached
        mock_client.get_articles.assert_called_once_with(30)

        del os.environ["SPAIN_SHOP_URL"]
        del os.environ["SPAIN_ACCESS_TOKEN"]

    @patch("export_spain.load_dotenv")
    @patch("export_spain.ShopifyClient")
    def test_export_empty_store(self, MockClient, mock_dotenv):
        mock_client = MagicMock()
        MockClient.return_value = mock_client

        mock_client.get_shop.return_value = {"name": "Empty Shop"}
        mock_client.get_products.return_value = []
        mock_client.get_collections.return_value = []
        mock_client.get_pages.return_value = []
        mock_client.get_blogs.return_value = []
        mock_client.get_metaobject_definitions.return_value = []
        mock_client.get_collects.return_value = []
        mock_client.get_redirects.return_value = []
        mock_client.get_policies.return_value = []

        os.environ["SPAIN_SHOP_URL"] = "spain.myshopify.com"
        os.environ["SPAIN_ACCESS_TOKEN"] = "tok"
        main()
        del os.environ["SPAIN_SHOP_URL"]
        del os.environ["SPAIN_ACCESS_TOKEN"]

        mock_client.get_metafields.assert_not_called()
        mock_client.get_articles.assert_not_called()
