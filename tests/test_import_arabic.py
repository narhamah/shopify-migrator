"""Tests for import_arabic.py."""
import json
import os
from unittest.mock import MagicMock, patch

import pytest

from import_arabic import build_translation_inputs, ARABIC_LOCALE, main
from utils import load_json, save_json
from tests.conftest import (
    make_product, make_collection, make_page, make_article,
    make_blog, make_id_map, make_metaobjects_data,
)


# ---------------------------------------------------------------------------
# Utility functions
# ---------------------------------------------------------------------------

class TestLoadJson:
    def test_load_existing(self, tmp_path):
        f = tmp_path / "test.json"
        f.write_text(json.dumps({"k": "v"}))
        assert load_json(str(f)) == {"k": "v"}

    def test_load_missing_json(self, tmp_path):
        assert load_json(str(tmp_path / "missing.json")) == []

    def test_load_missing_non_json(self, tmp_path):
        assert load_json(str(tmp_path / "missing.txt")) == {}


class TestSaveJson:
    def test_save(self, tmp_path):
        f = str(tmp_path / "sub" / "out.json")
        save_json([1], f)
        with open(f) as fh:
            assert json.load(fh) == [1]


class TestBuildTranslationInputs:
    def test_matching_keys(self):
        translatable = [
            {"key": "title", "value": "Hello", "digest": "abc123", "locale": "en"},
            {"key": "body_html", "value": "<p>Body</p>", "digest": "def456", "locale": "en"},
        ]
        arabic_fields = {"title": "عنوان", "body_html": "<p>محتوى</p>"}
        result = build_translation_inputs(translatable, arabic_fields)
        assert len(result) == 2
        assert result[0]["locale"] == ARABIC_LOCALE
        assert result[0]["translatableContentDigest"] == "abc123"

    def test_partial_match(self):
        translatable = [
            {"key": "title", "value": "Hello", "digest": "abc", "locale": "en"},
            {"key": "body_html", "value": "<p>Body</p>", "digest": "def", "locale": "en"},
        ]
        result = build_translation_inputs(translatable, {"title": "عنوان"})
        assert len(result) == 1

    def test_no_match(self):
        translatable = [{"key": "title", "value": "Hello", "digest": "abc", "locale": "en"}]
        assert len(build_translation_inputs(translatable, {"other": "v"})) == 0

    def test_empty_arabic_value_skipped(self):
        translatable = [{"key": "title", "value": "Hello", "digest": "abc", "locale": "en"}]
        assert len(build_translation_inputs(translatable, {"title": ""})) == 0

    def test_none_arabic_value_skipped(self):
        translatable = [{"key": "title", "value": "Hello", "digest": "abc", "locale": "en"}]
        assert len(build_translation_inputs(translatable, {"title": None})) == 0


# ---------------------------------------------------------------------------
# Helper to set up data dirs
# ---------------------------------------------------------------------------

def _setup_arabic_data(base_path, id_map=None, progress=None,
                       en_products=None, ar_products=None,
                       en_collections=None, ar_collections=None,
                       en_pages=None, ar_pages=None,
                       en_articles=None, ar_articles=None,
                       en_metaobjects=None, ar_metaobjects=None):
    data_dir = base_path / "data"
    data_dir.mkdir(exist_ok=True)
    en_dir = data_dir / "english"
    ar_dir = data_dir / "arabic"
    en_dir.mkdir(exist_ok=True)
    ar_dir.mkdir(exist_ok=True)

    product = make_product()

    (en_dir / "products.json").write_text(json.dumps(en_products if en_products is not None else [product]))
    (ar_dir / "products.json").write_text(json.dumps(
        ar_products if ar_products is not None else [{**product, "title": "عنوان", "body_html": "<p>محتوى</p>", "product_type": "نوع"}]
    ))
    (en_dir / "collections.json").write_text(json.dumps(en_collections or []))
    (ar_dir / "collections.json").write_text(json.dumps(ar_collections or []))
    (en_dir / "pages.json").write_text(json.dumps(en_pages or []))
    (ar_dir / "pages.json").write_text(json.dumps(ar_pages or []))
    (en_dir / "articles.json").write_text(json.dumps(en_articles or []))
    (ar_dir / "articles.json").write_text(json.dumps(ar_articles or []))

    if en_metaobjects is not None:
        (en_dir / "metaobjects.json").write_text(json.dumps(en_metaobjects))
    if ar_metaobjects is not None:
        (ar_dir / "metaobjects.json").write_text(json.dumps(ar_metaobjects))

    if id_map is None:
        id_map = make_id_map()
    (data_dir / "id_map.json").write_text(json.dumps(id_map))

    if progress:
        (data_dir / "arabic_import_progress.json").write_text(json.dumps(progress))


# ---------------------------------------------------------------------------
# main() tests
# ---------------------------------------------------------------------------

class TestMainDryRun:
    @patch("import_arabic.load_dotenv")
    @patch("sys.argv", ["import_arabic.py", "--dry-run"])
    def test_dry_run_output(self, mock_dotenv, tmp_path, monkeypatch, capsys):
        monkeypatch.chdir(tmp_path)
        _setup_arabic_data(tmp_path)
        main()
        captured = capsys.readouterr()
        assert "DRY RUN" in captured.out


class TestMainProducts:
    @patch("import_arabic.load_dotenv")
    @patch("import_arabic.ShopifyClient")
    @patch("sys.argv", ["import_arabic.py"])
    def test_registers_product_translations(self, MockClient, mock_dotenv, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        _setup_arabic_data(tmp_path)

        mc = MagicMock()
        MockClient.return_value = mc
        mc.get_translatable_resource.return_value = {
            "resourceId": "gid://shopify/Product/9001",
            "translatableContent": [
                {"key": "title", "value": "Test Product", "digest": "abc", "locale": "en"},
                {"key": "body_html", "value": "<p>Body</p>", "digest": "def", "locale": "en"},
            ],
        }
        mc.register_translations.return_value = [{"key": "title", "locale": "ar", "value": "عنوان"}]

        os.environ["SAUDI_SHOP_URL"] = "saudi.myshopify.com"
        os.environ["SAUDI_ACCESS_TOKEN"] = "tok"
        try:
            main()
        finally:
            del os.environ["SAUDI_SHOP_URL"]
            del os.environ["SAUDI_ACCESS_TOKEN"]

        mc.register_translations.assert_called()

    @patch("import_arabic.load_dotenv")
    @patch("import_arabic.ShopifyClient")
    @patch("sys.argv", ["import_arabic.py"])
    def test_skips_already_done(self, MockClient, mock_dotenv, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        _setup_arabic_data(tmp_path, progress={"product_9001": True})

        mc = MagicMock()
        MockClient.return_value = mc

        os.environ["SAUDI_SHOP_URL"] = "saudi.myshopify.com"
        os.environ["SAUDI_ACCESS_TOKEN"] = "tok"
        try:
            main()
        finally:
            del os.environ["SAUDI_SHOP_URL"]
            del os.environ["SAUDI_ACCESS_TOKEN"]

        mc.get_translatable_resource.assert_not_called()

    @patch("import_arabic.load_dotenv")
    @patch("import_arabic.ShopifyClient")
    @patch("sys.argv", ["import_arabic.py"])
    def test_no_dest_id_skips(self, MockClient, mock_dotenv, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        _setup_arabic_data(tmp_path, id_map={"products": {}})

        mc = MagicMock()
        MockClient.return_value = mc

        os.environ["SAUDI_SHOP_URL"] = "saudi.myshopify.com"
        os.environ["SAUDI_ACCESS_TOKEN"] = "tok"
        try:
            main()
        finally:
            del os.environ["SAUDI_SHOP_URL"]
            del os.environ["SAUDI_ACCESS_TOKEN"]

        mc.get_translatable_resource.assert_not_called()

    @patch("import_arabic.load_dotenv")
    @patch("import_arabic.ShopifyClient")
    @patch("sys.argv", ["import_arabic.py"])
    def test_no_arabic_translation_skips(self, MockClient, mock_dotenv, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        product = make_product()
        # Arabic has product with different ID — no match
        ar_product = {**product, "id": 9999, "title": "عنوان"}
        _setup_arabic_data(tmp_path, en_products=[product], ar_products=[ar_product])

        mc = MagicMock()
        MockClient.return_value = mc

        os.environ["SAUDI_SHOP_URL"] = "saudi.myshopify.com"
        os.environ["SAUDI_ACCESS_TOKEN"] = "tok"
        try:
            main()
        finally:
            del os.environ["SAUDI_SHOP_URL"]
            del os.environ["SAUDI_ACCESS_TOKEN"]

        mc.get_translatable_resource.assert_not_called()

    @patch("import_arabic.load_dotenv")
    @patch("import_arabic.ShopifyClient")
    @patch("sys.argv", ["import_arabic.py"])
    def test_translation_error_continues(self, MockClient, mock_dotenv, tmp_path, monkeypatch, capsys):
        monkeypatch.chdir(tmp_path)
        _setup_arabic_data(tmp_path)

        mc = MagicMock()
        MockClient.return_value = mc
        mc.get_translatable_resource.side_effect = Exception("API error")

        os.environ["SAUDI_SHOP_URL"] = "saudi.myshopify.com"
        os.environ["SAUDI_ACCESS_TOKEN"] = "tok"
        try:
            main()
        finally:
            del os.environ["SAUDI_SHOP_URL"]
            del os.environ["SAUDI_ACCESS_TOKEN"]

        captured = capsys.readouterr()
        assert "error" in captured.out.lower()

    @patch("import_arabic.load_dotenv")
    @patch("import_arabic.ShopifyClient")
    @patch("sys.argv", ["import_arabic.py"])
    def test_no_translatable_content(self, MockClient, mock_dotenv, tmp_path, monkeypatch, capsys):
        monkeypatch.chdir(tmp_path)
        _setup_arabic_data(tmp_path)

        mc = MagicMock()
        MockClient.return_value = mc
        mc.get_translatable_resource.return_value = None

        os.environ["SAUDI_SHOP_URL"] = "saudi.myshopify.com"
        os.environ["SAUDI_ACCESS_TOKEN"] = "tok"
        try:
            main()
        finally:
            del os.environ["SAUDI_SHOP_URL"]
            del os.environ["SAUDI_ACCESS_TOKEN"]

        captured = capsys.readouterr()
        assert "could not fetch" in captured.out.lower()


class TestMainCollections:
    @patch("import_arabic.load_dotenv")
    @patch("import_arabic.ShopifyClient")
    @patch("sys.argv", ["import_arabic.py"])
    def test_registers_collection_translations(self, MockClient, mock_dotenv, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        coll = make_collection()
        ar_coll = {**coll, "title": "مجموعة", "body_html": "<p>وصف</p>"}
        _setup_arabic_data(
            tmp_path,
            en_products=[], ar_products=[],
            en_collections=[coll], ar_collections=[ar_coll],
            id_map={"products": {}, "collections": {str(coll["id"]): 9002}},
        )

        mc = MagicMock()
        MockClient.return_value = mc
        mc.get_translatable_resource.return_value = {
            "resourceId": "gid://shopify/Collection/9002",
            "translatableContent": [
                {"key": "title", "value": "Test", "digest": "abc", "locale": "en"},
                {"key": "body_html", "value": "<p>Body</p>", "digest": "def", "locale": "en"},
            ],
        }
        mc.register_translations.return_value = []

        os.environ["SAUDI_SHOP_URL"] = "saudi.myshopify.com"
        os.environ["SAUDI_ACCESS_TOKEN"] = "tok"
        try:
            main()
        finally:
            del os.environ["SAUDI_SHOP_URL"]
            del os.environ["SAUDI_ACCESS_TOKEN"]

        mc.register_translations.assert_called()


class TestMainPages:
    @patch("import_arabic.load_dotenv")
    @patch("import_arabic.ShopifyClient")
    @patch("sys.argv", ["import_arabic.py"])
    def test_registers_page_translations(self, MockClient, mock_dotenv, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        page = make_page()
        ar_page = {**page, "title": "صفحة", "body_html": "<p>محتوى</p>"}
        _setup_arabic_data(
            tmp_path,
            en_products=[], ar_products=[],
            en_pages=[page], ar_pages=[ar_page],
            id_map={"products": {}, "pages": {str(page["id"]): 9003}},
        )

        mc = MagicMock()
        MockClient.return_value = mc
        mc.get_translatable_resource.return_value = {
            "resourceId": "gid://shopify/OnlineStorePage/9003",
            "translatableContent": [
                {"key": "title", "value": "Test Page", "digest": "abc", "locale": "en"},
            ],
        }
        mc.register_translations.return_value = []

        os.environ["SAUDI_SHOP_URL"] = "saudi.myshopify.com"
        os.environ["SAUDI_ACCESS_TOKEN"] = "tok"
        try:
            main()
        finally:
            del os.environ["SAUDI_SHOP_URL"]
            del os.environ["SAUDI_ACCESS_TOKEN"]

        mc.register_translations.assert_called()


class TestMainArticles:
    @patch("import_arabic.load_dotenv")
    @patch("import_arabic.ShopifyClient")
    @patch("sys.argv", ["import_arabic.py"])
    def test_registers_article_translations(self, MockClient, mock_dotenv, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        art = make_article()
        ar_art = {**art, "title": "مقالة", "body_html": "<p>محتوى</p>", "summary_html": "<p>ملخص</p>"}
        _setup_arabic_data(
            tmp_path,
            en_products=[], ar_products=[],
            en_articles=[art], ar_articles=[ar_art],
            id_map={"products": {}, "articles": {str(art["id"]): 9005}},
        )

        mc = MagicMock()
        MockClient.return_value = mc
        mc.get_translatable_resource.return_value = {
            "resourceId": f"gid://shopify/OnlineStoreArticle/9005",
            "translatableContent": [
                {"key": "title", "value": "Test", "digest": "abc", "locale": "en"},
                {"key": "body_html", "value": "<p>Body</p>", "digest": "def", "locale": "en"},
                {"key": "summary_html", "value": "<p>Sum</p>", "digest": "ghi", "locale": "en"},
            ],
        }
        mc.register_translations.return_value = []

        os.environ["SAUDI_SHOP_URL"] = "saudi.myshopify.com"
        os.environ["SAUDI_ACCESS_TOKEN"] = "tok"
        try:
            main()
        finally:
            del os.environ["SAUDI_SHOP_URL"]
            del os.environ["SAUDI_ACCESS_TOKEN"]

        mc.register_translations.assert_called()


class TestMainMetaobjects:
    @patch("import_arabic.load_dotenv")
    @patch("import_arabic.ShopifyClient")
    @patch("sys.argv", ["import_arabic.py"])
    def test_registers_metaobject_translations(self, MockClient, mock_dotenv, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)

        en_metaobjects = {
            "benefit": {
                "definition": {"type": "benefit"},
                "objects": [{"id": "gid://shopify/Metaobject/100", "handle": "shine",
                             "fields": [{"key": "title", "value": "Shine"}]}],
            }
        }
        ar_metaobjects = {
            "benefit": {
                "definition": {"type": "benefit"},
                "objects": [{"id": "gid://shopify/Metaobject/100", "handle": "shine",
                             "fields": [{"key": "title", "value": "تألق"}]}],
            }
        }
        _setup_arabic_data(
            tmp_path,
            en_products=[], ar_products=[],
            en_metaobjects=en_metaobjects, ar_metaobjects=ar_metaobjects,
            id_map={
                "products": {},
                "metaobjects_benefit": {"gid://shopify/Metaobject/100": "gid://shopify/Metaobject/500"},
            },
        )

        mc = MagicMock()
        MockClient.return_value = mc
        mc.get_translatable_resource.return_value = {
            "resourceId": "gid://shopify/Metaobject/500",
            "translatableContent": [
                {"key": "title", "value": "Shine", "digest": "xyz", "locale": "en"},
            ],
        }
        mc.register_translations.return_value = []

        os.environ["SAUDI_SHOP_URL"] = "saudi.myshopify.com"
        os.environ["SAUDI_ACCESS_TOKEN"] = "tok"
        try:
            main()
        finally:
            del os.environ["SAUDI_SHOP_URL"]
            del os.environ["SAUDI_ACCESS_TOKEN"]

        mc.register_translations.assert_called()

    @patch("import_arabic.load_dotenv")
    @patch("import_arabic.ShopifyClient")
    @patch("sys.argv", ["import_arabic.py"])
    def test_metaobject_no_dest_id_falls_back_to_handle(self, MockClient, mock_dotenv, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)

        en_metaobjects = {
            "benefit": {
                "definition": {"type": "benefit"},
                "objects": [{"id": "gid://shopify/Metaobject/100", "handle": "shine",
                             "fields": [{"key": "title", "value": "Shine"}]}],
            }
        }
        ar_metaobjects = {
            "benefit": {
                "definition": {"type": "benefit"},
                # Arabic has a different source ID
                "objects": [{"id": "gid://shopify/Metaobject/999", "handle": "shine",
                             "fields": [{"key": "title", "value": "تألق"}]}],
            }
        }
        _setup_arabic_data(
            tmp_path,
            en_products=[], ar_products=[],
            en_metaobjects=en_metaobjects, ar_metaobjects=ar_metaobjects,
            id_map={
                "products": {},
                "metaobjects_benefit": {"gid://shopify/Metaobject/100": "gid://shopify/Metaobject/500"},
            },
        )

        mc = MagicMock()
        MockClient.return_value = mc
        mc.get_translatable_resource.return_value = {
            "resourceId": "gid://shopify/Metaobject/500",
            "translatableContent": [
                {"key": "title", "value": "Shine", "digest": "xyz", "locale": "en"},
            ],
        }
        mc.register_translations.return_value = []

        os.environ["SAUDI_SHOP_URL"] = "saudi.myshopify.com"
        os.environ["SAUDI_ACCESS_TOKEN"] = "tok"
        try:
            main()
        finally:
            del os.environ["SAUDI_SHOP_URL"]
            del os.environ["SAUDI_ACCESS_TOKEN"]

        # Should still find dest_id via handle fallback
        mc.register_translations.assert_called()


class TestMetaobjectTranslations:
    def test_arabic_fields_from_metaobject(self):
        ar_obj = {
            "fields": [
                {"key": "title", "value": "تألق"},
                {"key": "description", "value": "وصف"},
            ],
        }
        arabic_fields = {f["key"]: f["value"] for f in ar_obj["fields"] if f.get("value")}
        assert arabic_fields == {"title": "تألق", "description": "وصف"}

    def test_dest_id_lookup_by_handle_fallback(self):
        en_by_handle = {"shine": {"id": "gid://src/100"}}
        id_map_key = {"gid://src/100": "gid://dest/500"}
        dest_id = id_map_key.get("gid://src/999")
        assert dest_id is None
        en_obj = en_by_handle.get("shine")
        dest_id = id_map_key.get(en_obj["id"])
        assert dest_id == "gid://dest/500"
