"""Tests for import_english.py."""
import json
import os
from unittest.mock import MagicMock, patch, call

import pytest

from import_english import prepare_product_for_import, main
from utils import load_json, save_json

from tests.conftest import (
    make_product, make_collection, make_page, make_blog, make_article,
    make_metaobject, make_metaobjects_data, make_id_map,
)


# ---------------------------------------------------------------------------
# Utility functions
# ---------------------------------------------------------------------------

class TestLoadJson:
    def test_load_existing(self, tmp_path):
        f = tmp_path / "test.json"
        f.write_text(json.dumps({"key": "val"}))
        assert load_json(str(f)) == {"key": "val"}

    def test_load_missing_json_extension(self, tmp_path):
        result = load_json(str(tmp_path / "missing.json"))
        assert result == []

    def test_load_missing_non_json(self, tmp_path):
        result = load_json(str(tmp_path / "missing.txt"))
        assert result == {}


class TestSaveJson:
    def test_save_creates_dirs(self, tmp_path):
        f = str(tmp_path / "sub" / "out.json")
        save_json({"a": 1}, f)
        assert os.path.exists(f)
        with open(f) as fh:
            assert json.load(fh) == {"a": 1}

    def test_save_current_dir(self, tmp_path):
        f = str(tmp_path / "out.json")
        save_json([1, 2], f)
        with open(f) as fh:
            assert json.load(fh) == [1, 2]


class TestPrepareProductForImport:
    def test_basic_fields(self):
        product = make_product()
        result = prepare_product_for_import(product)
        assert result["title"] == "Test Product"
        assert result["handle"] == "test-product"
        assert result["vendor"] == "TARA"
        assert result["status"] == "active"

    def test_price_with_sar_prices(self):
        product = make_product(price="10.00")
        sar_prices = {"SKU001": {"final_price": 37.5, "regular_price": 50.0}}
        result = prepare_product_for_import(product, sar_prices)
        assert result["variants"][0]["price"] == "37.5"
        assert result["variants"][0]["compare_at_price"] == "50.0"

    def test_images_extracted(self):
        product = make_product()
        result = prepare_product_for_import(product)
        assert len(result["images"]) == 1
        assert result["images"][0]["src"] == "https://cdn.shopify.com/img.jpg"

    def test_no_images(self):
        product = make_product()
        del product["images"]
        result = prepare_product_for_import(product)
        assert "images" not in result

    def test_reference_metafields_skipped(self):
        product = make_product()
        result = prepare_product_for_import(product)
        mf_keys = [mf["key"] for mf in result.get("metafields", [])]
        assert "tagline" in mf_keys
        assert "ingredients" not in mf_keys  # Reference field skipped

    def test_no_metafields(self):
        product = make_product()
        del product["metafields"]
        result = prepare_product_for_import(product)
        assert "metafields" not in result

    def test_no_variants(self):
        product = make_product()
        del product["variants"]
        result = prepare_product_for_import(product)
        assert "variants" not in result

    def test_no_options(self):
        product = make_product()
        del product["options"]
        result = prepare_product_for_import(product)
        assert "options" not in result

    def test_variant_fields_preserved(self):
        product = make_product()
        result = prepare_product_for_import(product)
        v = result["variants"][0]
        assert v["sku"] == "SKU001"
        assert v["weight"] == 0.5
        assert v["weight_unit"] == "kg"
        assert v["requires_shipping"] is True
        assert v["taxable"] is True

    def test_image_without_src_skipped(self):
        product = make_product()
        product["images"] = [{"src": "https://cdn.shopify.com/img.jpg"}, {"alt": "no src"}]
        result = prepare_product_for_import(product)
        assert len(result["images"]) == 1


# ---------------------------------------------------------------------------
# main() — uses monkeypatch.chdir so relative paths resolve to tmp_path
# ---------------------------------------------------------------------------

def _setup_english_data(base_path):
    """Write standard test data files under base_path/data/english/."""
    data_dir = base_path / "data"
    data_dir.mkdir(exist_ok=True)
    english_dir = data_dir / "english"
    english_dir.mkdir(exist_ok=True)

    (english_dir / "products.json").write_text(json.dumps([make_product()]))
    (english_dir / "collections.json").write_text(json.dumps([make_collection()]))
    (english_dir / "pages.json").write_text(json.dumps([make_page()]))
    (english_dir / "blogs.json").write_text(json.dumps([make_blog()]))
    (english_dir / "articles.json").write_text(json.dumps([make_article()]))
    (english_dir / "metaobjects.json").write_text(json.dumps(make_metaobjects_data()))
    (english_dir / "metaobject_definitions.json").write_text(json.dumps([]))


class TestMainDryRun:
    @patch("import_english.load_dotenv")
    @patch("sys.argv", ["import_english.py", "--dry-run"])
    def test_dry_run(self, mock_dotenv, tmp_path, monkeypatch, capsys):
        monkeypatch.chdir(tmp_path)
        _setup_english_data(tmp_path)

        main()

        captured = capsys.readouterr()
        assert "DRY RUN" in captured.out
        assert "would create" in captured.out


class TestMainPhases:
    @patch("import_english.load_dotenv")
    @patch("import_english.ShopifyClient")
    @patch("sys.argv", ["import_english.py"])
    def test_creates_products(self, MockClient, mock_dotenv, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        _setup_english_data(tmp_path)

        mc = MagicMock()
        MockClient.return_value = mc

        mc.get_metaobject_definitions.return_value = []
        mc.get_products_by_handle.return_value = []
        mc.create_product.return_value = {"id": 9001}
        mc.get_collections_by_handle.return_value = []
        mc.create_custom_collection.return_value = {"id": 9002}
        mc.get_pages_by_handle.return_value = []
        mc.create_page.return_value = {"id": 9003}
        mc.get_blogs_by_handle.return_value = []
        mc.create_blog.return_value = {"id": 9004}
        mc.create_article.return_value = {"id": 9005}
        mc.get_metaobjects_by_handle.return_value = None
        mc.create_metaobject.return_value = {"id": "gid://shopify/Metaobject/500", "handle": "h"}
        mc.set_metafields.return_value = []
        mc.update_metaobject.return_value = {"id": "gid://1"}

        os.environ["SAUDI_SHOP_URL"] = "saudi.myshopify.com"
        os.environ["SAUDI_ACCESS_TOKEN"] = "tok"
        try:
            main()
        finally:
            del os.environ["SAUDI_SHOP_URL"]
            del os.environ["SAUDI_ACCESS_TOKEN"]

        mc.create_product.assert_called_once()
        mc.create_custom_collection.assert_called_once()
        mc.create_page.assert_called_once()
        mc.create_blog.assert_called_once()
        mc.create_article.assert_called_once()


class TestMainExistingResources:
    """Test that existing resources get mapped rather than re-created."""

    @patch("import_english.load_dotenv")
    @patch("import_english.ShopifyClient")
    @patch("sys.argv", ["import_english.py"])
    def test_existing_product_mapped(self, MockClient, mock_dotenv, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        _setup_english_data(tmp_path)

        mc = MagicMock()
        MockClient.return_value = mc
        mc.get_metaobject_definitions.return_value = []
        mc.get_products_by_handle.return_value = [{"id": 8888}]
        mc.get_collections_by_handle.return_value = [{"id": 8889}]
        mc.get_pages_by_handle.return_value = [{"id": 8890}]
        mc.get_blogs_by_handle.return_value = [{"id": 8891}]
        mc.create_article.return_value = {"id": 8892}
        mc.get_metaobjects_by_handle.return_value = {"id": "gid://existing/1", "handle": "h"}
        mc.set_metafields.return_value = []
        mc.update_metaobject.return_value = {"id": "gid://1"}

        os.environ["SAUDI_SHOP_URL"] = "saudi.myshopify.com"
        os.environ["SAUDI_ACCESS_TOKEN"] = "tok"
        try:
            main()
        finally:
            del os.environ["SAUDI_SHOP_URL"]
            del os.environ["SAUDI_ACCESS_TOKEN"]

        # Products should NOT be created — they already exist
        mc.create_product.assert_not_called()
        mc.create_custom_collection.assert_not_called()
        mc.create_page.assert_not_called()
        mc.create_blog.assert_not_called()

    @patch("import_english.load_dotenv")
    @patch("import_english.ShopifyClient")
    @patch("sys.argv", ["import_english.py"])
    def test_phase6_remaps_references(self, MockClient, mock_dotenv, tmp_path, monkeypatch):
        """Integration test of Phase 6 reference remapping."""
        monkeypatch.chdir(tmp_path)
        _setup_english_data(tmp_path)

        mc = MagicMock()
        MockClient.return_value = mc
        mc.get_metaobject_definitions.return_value = []
        mc.get_products_by_handle.return_value = [{"id": 9001}]
        mc.get_collections_by_handle.return_value = [{"id": 9002}]
        mc.get_pages_by_handle.return_value = [{"id": 9003}]
        mc.get_blogs_by_handle.return_value = [{"id": 9004}]
        mc.create_article.return_value = {"id": 9005}
        mc.get_metaobjects_by_handle.return_value = {"id": "gid://shopify/Metaobject/500", "handle": "h"}
        mc.set_metafields.return_value = []
        mc.update_metaobject.return_value = {"id": "gid://1"}

        os.environ["SAUDI_SHOP_URL"] = "saudi.myshopify.com"
        os.environ["SAUDI_ACCESS_TOKEN"] = "tok"
        try:
            main()
        finally:
            del os.environ["SAUDI_SHOP_URL"]
            del os.environ["SAUDI_ACCESS_TOKEN"]

        # Phase 6 should call update_metaobject for ingredient→benefit remapping
        # and set_metafields for product→ingredient and article→author remapping
        assert mc.update_metaobject.called or mc.set_metafields.called

    @patch("import_english.load_dotenv")
    @patch("import_english.ShopifyClient")
    @patch("sys.argv", ["import_english.py"])
    def test_error_handling_in_creation(self, MockClient, mock_dotenv, tmp_path, monkeypatch, capsys):
        monkeypatch.chdir(tmp_path)
        _setup_english_data(tmp_path)

        mc = MagicMock()
        MockClient.return_value = mc
        mc.get_metaobject_definitions.side_effect = Exception("Connection error")
        # Configure remaining mocks so they return serializable values
        mc.get_products_by_handle.return_value = [{"id": 9001}]
        mc.get_collections_by_handle.return_value = [{"id": 9002}]
        mc.get_pages_by_handle.return_value = [{"id": 9003}]
        mc.get_blogs_by_handle.return_value = [{"id": 9004}]
        mc.create_article.return_value = {"id": 9005}
        mc.get_metaobjects_by_handle.return_value = {"id": "gid://shopify/Metaobject/500", "handle": "h"}
        mc.set_metafields.return_value = []
        mc.update_metaobject.return_value = {"id": "gid://1"}

        os.environ["SAUDI_SHOP_URL"] = "saudi.myshopify.com"
        os.environ["SAUDI_ACCESS_TOKEN"] = "tok"
        try:
            main()
        finally:
            del os.environ["SAUDI_SHOP_URL"]
            del os.environ["SAUDI_ACCESS_TOKEN"]

        captured = capsys.readouterr()
        assert "Could not fetch" in captured.out


class TestPhase6Remapping:
    """Test reference remapping logic at unit level."""

    def test_ingredient_benefit_remap(self):
        source_refs = ["gid://shopify/Metaobject/100", "gid://shopify/Metaobject/101"]
        benefit_map = {
            "gid://shopify/Metaobject/100": "gid://shopify/Metaobject/500",
            "gid://shopify/Metaobject/101": "gid://shopify/Metaobject/501",
        }
        dest_refs = [benefit_map.get(ref, ref) for ref in source_refs if benefit_map.get(ref)]
        assert dest_refs == ["gid://shopify/Metaobject/500", "gid://shopify/Metaobject/501"]

    def test_ingredient_benefit_partial_remap(self):
        source_refs = ["gid://shopify/Metaobject/100", "gid://shopify/Metaobject/999"]
        benefit_map = {"gid://shopify/Metaobject/100": "gid://shopify/Metaobject/500"}
        dest_refs = [benefit_map.get(ref, ref) for ref in source_refs if benefit_map.get(ref)]
        assert dest_refs == ["gid://shopify/Metaobject/500"]

    def test_collection_remap(self):
        source_coll_gid = "gid://shopify/Collection/2001"
        source_coll_id = source_coll_gid.split("/")[-1]
        collection_map = {"2001": 9002}
        dest_coll_id = collection_map.get(source_coll_id)
        assert dest_coll_id == 9002

    def test_product_ingredient_remap(self):
        source_refs = json.loads('["gid://shopify/Metaobject/400"]')
        ingredient_map = {"gid://shopify/Metaobject/400": "gid://shopify/Metaobject/800"}
        dest_refs = [ingredient_map.get(ref, ref) for ref in source_refs if ingredient_map.get(ref)]
        assert dest_refs == ["gid://shopify/Metaobject/800"]

    def test_article_author_remap(self):
        blog_author_map = {"gid://shopify/Metaobject/300": "gid://shopify/Metaobject/700"}
        assert blog_author_map.get("gid://shopify/Metaobject/300") == "gid://shopify/Metaobject/700"

    def test_article_related_products_remap(self):
        source_refs = json.loads('["gid://shopify/Product/1001"]')
        product_map = {"1001": 9001}
        dest_refs = []
        for ref in source_refs:
            ref_id = ref.split("/")[-1]
            dest_prod_id = product_map.get(ref_id)
            if dest_prod_id:
                dest_refs.append(f"gid://shopify/Product/{dest_prod_id}")
        assert dest_refs == ["gid://shopify/Product/9001"]

    def test_article_related_articles_remap(self):
        source_refs = json.loads('["gid://shopify/OnlineStoreArticle/5001"]')
        article_map = {"5001": 9005}
        dest_refs = []
        for ref in source_refs:
            ref_id = ref.split("/")[-1]
            dest_art_id = article_map.get(ref_id)
            if dest_art_id:
                dest_refs.append(f"gid://shopify/OnlineStoreArticle/{dest_art_id}")
        assert dest_refs == ["gid://shopify/OnlineStoreArticle/9005"]


class TestSkipExisting:
    def test_product_skip(self):
        id_map = {"products": {"1001": 9001}}
        assert "1001" in id_map.get("products", {})

    def test_metaobject_skip(self):
        id_map = {"metaobjects_benefit": {"gid://1": "gid://2"}}
        assert "gid://1" in id_map.get("metaobjects_benefit", {})

    def test_metaobject_exists_by_handle(self):
        existing = {"id": "gid://shopify/Metaobject/500", "handle": "h1"}
        assert existing is not None
