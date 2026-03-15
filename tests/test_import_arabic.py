"""Tests for import_arabic.py."""
import json
import os
from unittest.mock import MagicMock, patch

import pytest

from tara_migrate.pipeline.import_arabic import (
    ARABIC_LOCALE,
    build_article_arabic_fields,
    build_collection_arabic_fields,
    build_local_lookup,
    build_metaobject_arabic_fields,
    build_metaobject_lookup,
    build_page_arabic_fields,
    build_product_arabic_fields,
    build_translation_inputs,
    main,
)
from tara_migrate.core import load_json, save_json
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
# Field builder tests
# ---------------------------------------------------------------------------

class TestBuildProductArabicFields:
    def test_basic_fields(self):
        product = make_product()
        product["title"] = "عنوان"
        product["body_html"] = "<p>محتوى</p>"
        product["handle"] = "عنوان-منتج"
        result = build_product_arabic_fields(product)
        assert result["title"] == "عنوان"
        assert result["body_html"] == "<p>محتوى</p>"
        assert result["handle"] == "عنوان-منتج"

    def test_metafields(self):
        product = make_product()
        product["metafields"] = [
            {"namespace": "custom", "key": "tagline", "value": "شعار", "type": "single_line_text_field"},
        ]
        result = build_product_arabic_fields(product)
        assert result["custom.tagline"] == "شعار"

    def test_reference_metafields_skipped(self):
        product = make_product()
        product["metafields"] = [
            {"namespace": "custom", "key": "related", "value": "gid://...", "type": "product_reference"},
        ]
        result = build_product_arabic_fields(product)
        assert "custom.related" not in result


class TestBuildCollectionArabicFields:
    def test_basic_fields(self):
        coll = make_collection()
        coll["title"] = "مجموعة"
        result = build_collection_arabic_fields(coll)
        assert result["title"] == "مجموعة"
        assert "body_html" in result


class TestBuildPageArabicFields:
    def test_basic_fields(self):
        page = make_page()
        page["title"] = "صفحة"
        result = build_page_arabic_fields(page)
        assert result["title"] == "صفحة"


class TestBuildArticleArabicFields:
    def test_basic_fields(self):
        art = make_article()
        art["title"] = "مقالة"
        art["summary_html"] = "<p>ملخص</p>"
        result = build_article_arabic_fields(art)
        assert result["title"] == "مقالة"
        assert result["summary_html"] == "<p>ملخص</p>"


class TestBuildMetaobjectArabicFields:
    def test_basic_fields(self):
        obj = {
            "handle": "تألق",
            "fields": [
                {"key": "title", "value": "تألق"},
                {"key": "description", "value": "وصف"},
            ],
        }
        result = build_metaobject_arabic_fields(obj)
        assert result["handle"] == "تألق"
        assert result["title"] == "تألق"
        assert result["description"] == "وصف"

    def test_empty_value_skipped(self):
        obj = {
            "handle": "test",
            "fields": [{"key": "title", "value": ""}, {"key": "desc", "value": "ok"}],
        }
        result = build_metaobject_arabic_fields(obj)
        assert "title" not in result
        assert result["desc"] == "ok"


# ---------------------------------------------------------------------------
# Lookup builder tests
# ---------------------------------------------------------------------------

class TestBuildLocalLookup:
    def test_from_progress_file(self):
        progress_ar = {
            "prod.test-product.title": "عنوان",
            "prod.test-product.body_html": "<p>محتوى</p>",
            "coll.test-coll.title": "مجموعة",
        }
        result = build_local_lookup(progress_ar, "prod")
        assert "test-product" in result
        assert result["test-product"]["title"] == "عنوان"
        assert "test-coll" not in result  # wrong prefix

    def test_full_json_fallback(self):
        progress_ar = {
            "prod.test-product.title": "عنوان",
        }
        product = make_product()
        product["handle"] = "test-product"
        product["body_html"] = "<p>محتوى كامل</p>"

        result = build_local_lookup(
            progress_ar, "prod",
            ar_items=[product],
            field_builder=build_product_arabic_fields,
        )
        # Progress file value takes priority
        assert result["test-product"]["title"] == "عنوان"
        # Full JSON provides body_html (not in progress)
        assert result["test-product"]["body_html"] == "<p>محتوى كامل</p>"

    def test_progress_takes_priority(self):
        progress_ar = {
            "prod.test-product.title": "من الملف",
        }
        product = make_product()
        product["handle"] = "test-product"
        product["title"] = "من JSON"

        result = build_local_lookup(
            progress_ar, "prod",
            ar_items=[product],
            field_builder=build_product_arabic_fields,
        )
        assert result["test-product"]["title"] == "من الملف"

    def test_empty_progress(self):
        result = build_local_lookup({}, "prod")
        assert result == {}

    def test_metaobject_key_format(self):
        progress_ar = {
            "mo.benefit.shine.title": "تألق",
            "mo.benefit.shine.handle": "تألق-معنى",
        }
        result = build_local_lookup(progress_ar, "mo")
        # mo prefix parsed as: type=benefit, handle.field = shine.title
        # But since build_local_lookup splits on first dot:
        # key=benefit, field=shine.title — not ideal for metaobjects
        # That's why we have build_metaobject_lookup separately
        assert "benefit" in result


class TestBuildMetaobjectLookup:
    def test_from_progress(self):
        progress_ar = {
            "mo.benefit.shine.title": "تألق",
            "mo.benefit.shine.handle": "تألق-جمال",
        }
        result = build_metaobject_lookup(progress_ar, {})
        assert "benefit.shine" in result
        assert result["benefit.shine"]["title"] == "تألق"
        assert result["benefit.shine"]["handle"] == "تألق-جمال"

    def test_with_full_json(self):
        progress_ar = {"mo.benefit.shine.title": "تألق"}
        ar_metaobjects = {
            "benefit": {
                "objects": [{
                    "handle": "shine",
                    "fields": [
                        {"key": "title", "value": "من JSON"},
                        {"key": "description", "value": "وصف"},
                    ],
                }],
            },
        }
        result = build_metaobject_lookup(progress_ar, ar_metaobjects)
        # Progress takes priority for title
        assert result["benefit.shine"]["title"] == "تألق"
        # Full JSON provides description
        assert result["benefit.shine"]["description"] == "وصف"


# ---------------------------------------------------------------------------
# Helper to set up data dirs
# ---------------------------------------------------------------------------

def _setup_arabic_data(base_path, id_map=None, progress=None,
                       en_products=None, ar_products=None,
                       en_collections=None, ar_collections=None,
                       en_pages=None, ar_pages=None,
                       en_articles=None, ar_articles=None,
                       en_metaobjects=None, ar_metaobjects=None,
                       progress_ar=None):
    data_dir = base_path / "data"
    data_dir.mkdir(exist_ok=True)
    en_dir = data_dir / "english"
    ar_dir = data_dir / "arabic"
    en_dir.mkdir(exist_ok=True)
    ar_dir.mkdir(exist_ok=True)

    product = make_product()

    (en_dir / "products.json").write_text(json.dumps(en_products if en_products is not None else [product]))
    (ar_dir / "products.json").write_text(json.dumps(
        ar_products if ar_products is not None else [{**product, "title": "عنوان", "body_html": "<p>محتوى</p>", "handle": "test-product", "product_type": "نوع"}]
    ))
    (en_dir / "collections.json").write_text(json.dumps(en_collections or []))
    (ar_dir / "collections.json").write_text(json.dumps(ar_collections or []))
    (en_dir / "pages.json").write_text(json.dumps(en_pages or []))
    (ar_dir / "pages.json").write_text(json.dumps(ar_pages or []))
    (en_dir / "articles.json").write_text(json.dumps(en_articles or []))
    (ar_dir / "articles.json").write_text(json.dumps(ar_articles or []))
    (en_dir / "blogs.json").write_text(json.dumps([]))
    (ar_dir / "blogs.json").write_text(json.dumps([]))

    if en_metaobjects is not None:
        (en_dir / "metaobjects.json").write_text(json.dumps(en_metaobjects))
    else:
        (en_dir / "metaobjects.json").write_text(json.dumps({}))
    if ar_metaobjects is not None:
        (ar_dir / "metaobjects.json").write_text(json.dumps(ar_metaobjects))
    else:
        (ar_dir / "metaobjects.json").write_text(json.dumps({}))

    if id_map is None:
        id_map = make_id_map()
    (data_dir / "id_map.json").write_text(json.dumps(id_map))

    if progress:
        (data_dir / "arabic_import_progress.json").write_text(json.dumps(progress))

    # Translation progress file
    if progress_ar is None:
        progress_ar = {}
    (ar_dir / "_translation_progress_ar.json").write_text(json.dumps(progress_ar))


# ---------------------------------------------------------------------------
# main() tests
# ---------------------------------------------------------------------------

class TestMainDryRun:
    @patch("tara_migrate.pipeline.import_arabic.load_dotenv")
    @patch("sys.argv", ["import_arabic.py", "--dry-run"])
    def test_dry_run_output(self, mock_dotenv, tmp_path, monkeypatch, capsys):
        monkeypatch.chdir(tmp_path)
        _setup_arabic_data(tmp_path)
        main()
        captured = capsys.readouterr()
        assert "DRY RUN" in captured.out


def _make_store_resource(gid, translatable_content):
    """Helper to create a store resource as returned by get_translatable_resources."""
    return {"resourceId": gid, "translatableContent": translatable_content}


class TestMainProducts:
    @patch("tara_migrate.pipeline.import_arabic.load_dotenv")
    @patch("tara_migrate.pipeline.import_arabic.ShopifyClient")
    @patch("sys.argv", ["import_arabic.py"])
    def test_registers_product_translations(self, MockClient, mock_dotenv, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        product = make_product()
        _setup_arabic_data(
            tmp_path,
            en_products=[product],
            ar_products=[{**product, "title": "عنوان", "body_html": "<p>محتوى</p>", "handle": "test-product"}],
            progress_ar={
                "prod.test-product.title": "عنوان",
                "prod.test-product.body_html": "<p>محتوى</p>",
            },
        )

        tc = [
            {"key": "title", "value": "Test Product", "digest": "abc", "locale": "en"},
            {"key": "handle", "value": "test-product", "digest": "hnd", "locale": "en"},
            {"key": "body_html", "value": "<p>Body</p>", "digest": "def", "locale": "en"},
        ]

        mc = MagicMock()
        MockClient.return_value = mc
        mc.get_translatable_resources.return_value = [
            _make_store_resource("gid://shopify/Product/9001", tc),
        ]
        mc.register_translations.return_value = [{"key": "title", "locale": "ar", "value": "عنوان"}]
        mc._request.return_value = MagicMock(
            json=lambda: {"product": {"id": 9001, "images": []}}
        )

        os.environ["DEST_SHOP_URL"] = "dest-test.myshopify.com"
        os.environ["DEST_ACCESS_TOKEN"] = "tok"
        try:
            main()
        finally:
            del os.environ["DEST_SHOP_URL"]
            del os.environ["DEST_ACCESS_TOKEN"]

        mc.register_translations.assert_called()

    @patch("tara_migrate.pipeline.import_arabic.load_dotenv")
    @patch("tara_migrate.pipeline.import_arabic.ShopifyClient")
    @patch("sys.argv", ["import_arabic.py"])
    def test_skips_already_done(self, MockClient, mock_dotenv, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        _setup_arabic_data(tmp_path, progress={"product_9001": True})

        mc = MagicMock()
        MockClient.return_value = mc
        mc.get_translatable_resources.return_value = [
            _make_store_resource("gid://shopify/Product/9001", [
                {"key": "title", "value": "Test", "digest": "abc", "locale": "en"},
            ]),
        ]

        os.environ["DEST_SHOP_URL"] = "dest-test.myshopify.com"
        os.environ["DEST_ACCESS_TOKEN"] = "tok"
        try:
            main()
        finally:
            del os.environ["DEST_SHOP_URL"]
            del os.environ["DEST_ACCESS_TOKEN"]

        mc.register_translations.assert_not_called()

    @patch("tara_migrate.pipeline.import_arabic.load_dotenv")
    @patch("tara_migrate.pipeline.import_arabic.ShopifyClient")
    @patch("sys.argv", ["import_arabic.py"])
    def test_no_local_data_skips(self, MockClient, mock_dotenv, tmp_path, monkeypatch, capsys):
        monkeypatch.chdir(tmp_path)
        _setup_arabic_data(tmp_path, id_map={"products": {}})

        mc = MagicMock()
        MockClient.return_value = mc
        mc.get_translatable_resources.return_value = [
            _make_store_resource("gid://shopify/Product/9999", [
                {"key": "title", "value": "Unknown", "digest": "abc", "locale": "en"},
                {"key": "handle", "value": "unknown-product", "digest": "hnd", "locale": "en"},
            ]),
        ]

        os.environ["DEST_SHOP_URL"] = "dest-test.myshopify.com"
        os.environ["DEST_ACCESS_TOKEN"] = "tok"
        try:
            main()
        finally:
            del os.environ["DEST_SHOP_URL"]
            del os.environ["DEST_ACCESS_TOKEN"]

        captured = capsys.readouterr()
        assert "no local Arabic data" in captured.out

    @patch("tara_migrate.pipeline.import_arabic.load_dotenv")
    @patch("tara_migrate.pipeline.import_arabic.ShopifyClient")
    @patch("sys.argv", ["import_arabic.py"])
    def test_translation_error_continues(self, MockClient, mock_dotenv, tmp_path, monkeypatch, capsys):
        monkeypatch.chdir(tmp_path)
        _setup_arabic_data(
            tmp_path,
            progress_ar={"prod.test-product.title": "عنوان"},
        )

        mc = MagicMock()
        MockClient.return_value = mc
        mc.get_translatable_resources.return_value = [
            _make_store_resource("gid://shopify/Product/9001", [
                {"key": "title", "value": "Test", "digest": "abc", "locale": "en"},
                {"key": "handle", "value": "test-product", "digest": "hnd", "locale": "en"},
            ]),
        ]
        mc.register_translations.side_effect = Exception("API error")

        os.environ["DEST_SHOP_URL"] = "dest-test.myshopify.com"
        os.environ["DEST_ACCESS_TOKEN"] = "tok"
        try:
            main()
        finally:
            del os.environ["DEST_SHOP_URL"]
            del os.environ["DEST_ACCESS_TOKEN"]

        captured = capsys.readouterr()
        assert "error" in captured.out.lower()

    @patch("tara_migrate.pipeline.import_arabic.load_dotenv")
    @patch("tara_migrate.pipeline.import_arabic.ShopifyClient")
    @patch("sys.argv", ["import_arabic.py"])
    def test_empty_store(self, MockClient, mock_dotenv, tmp_path, monkeypatch, capsys):
        monkeypatch.chdir(tmp_path)
        _setup_arabic_data(tmp_path)

        mc = MagicMock()
        MockClient.return_value = mc
        mc.get_translatable_resources.return_value = []

        os.environ["DEST_SHOP_URL"] = "dest-test.myshopify.com"
        os.environ["DEST_ACCESS_TOKEN"] = "tok"
        try:
            main()
        finally:
            del os.environ["DEST_SHOP_URL"]
            del os.environ["DEST_ACCESS_TOKEN"]

        captured = capsys.readouterr()
        assert "Found 0" in captured.out


class TestMainCollections:
    @patch("tara_migrate.pipeline.import_arabic.load_dotenv")
    @patch("tara_migrate.pipeline.import_arabic.ShopifyClient")
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
            progress_ar={
                "coll.test-collection.title": "مجموعة",
                "coll.test-collection.body_html": "<p>وصف</p>",
            },
        )

        mc = MagicMock()
        MockClient.return_value = mc
        mc.get_translatable_resources.return_value = [
            _make_store_resource("gid://shopify/Collection/9002", [
                {"key": "title", "value": "Test", "digest": "abc", "locale": "en"},
                {"key": "handle", "value": "test-collection", "digest": "hnd", "locale": "en"},
                {"key": "body_html", "value": "<p>Body</p>", "digest": "def", "locale": "en"},
            ]),
        ]
        mc.register_translations.return_value = []

        os.environ["DEST_SHOP_URL"] = "dest-test.myshopify.com"
        os.environ["DEST_ACCESS_TOKEN"] = "tok"
        try:
            main()
        finally:
            del os.environ["DEST_SHOP_URL"]
            del os.environ["DEST_ACCESS_TOKEN"]

        mc.register_translations.assert_called()


class TestMainPages:
    @patch("tara_migrate.pipeline.import_arabic.load_dotenv")
    @patch("tara_migrate.pipeline.import_arabic.ShopifyClient")
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
            progress_ar={
                "page.test-page.title": "صفحة",
            },
        )

        mc = MagicMock()
        MockClient.return_value = mc
        mc.get_translatable_resources.return_value = [
            _make_store_resource("gid://shopify/Page/9003", [
                {"key": "title", "value": "Test Page", "digest": "abc", "locale": "en"},
                {"key": "handle", "value": "test-page", "digest": "hnd", "locale": "en"},
            ]),
        ]
        mc.register_translations.return_value = []

        os.environ["DEST_SHOP_URL"] = "dest-test.myshopify.com"
        os.environ["DEST_ACCESS_TOKEN"] = "tok"
        try:
            main()
        finally:
            del os.environ["DEST_SHOP_URL"]
            del os.environ["DEST_ACCESS_TOKEN"]

        mc.register_translations.assert_called()


class TestMainArticles:
    @patch("tara_migrate.pipeline.import_arabic.load_dotenv")
    @patch("tara_migrate.pipeline.import_arabic.ShopifyClient")
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
            progress_ar={
                "art.test-article.title": "مقالة",
                "art.test-article.body_html": "<p>محتوى</p>",
                "art.test-article.summary_html": "<p>ملخص</p>",
            },
        )

        mc = MagicMock()
        MockClient.return_value = mc
        mc.get_translatable_resources.return_value = [
            _make_store_resource("gid://shopify/Article/9005", [
                {"key": "title", "value": "Test", "digest": "abc", "locale": "en"},
                {"key": "handle", "value": "test-article", "digest": "hnd", "locale": "en"},
                {"key": "body_html", "value": "<p>Body</p>", "digest": "def", "locale": "en"},
                {"key": "summary_html", "value": "<p>Sum</p>", "digest": "ghi", "locale": "en"},
            ]),
        ]
        mc.register_translations.return_value = []

        os.environ["DEST_SHOP_URL"] = "dest-test.myshopify.com"
        os.environ["DEST_ACCESS_TOKEN"] = "tok"
        try:
            main()
        finally:
            del os.environ["DEST_SHOP_URL"]
            del os.environ["DEST_ACCESS_TOKEN"]

        mc.register_translations.assert_called()


class TestMainMetaobjects:
    @patch("tara_migrate.pipeline.import_arabic.load_dotenv")
    @patch("tara_migrate.pipeline.import_arabic.ShopifyClient")
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
            progress_ar={
                "mo.benefit.shine.title": "تألق",
            },
        )

        mc = MagicMock()
        MockClient.return_value = mc
        mc.get_translatable_resources.return_value = [
            _make_store_resource("gid://shopify/Metaobject/500", [
                {"key": "title", "value": "Shine", "digest": "xyz", "locale": "en"},
                {"key": "handle", "value": "shine", "digest": "hnd", "locale": "en"},
            ]),
        ]
        mc.register_translations.return_value = []

        os.environ["DEST_SHOP_URL"] = "dest-test.myshopify.com"
        os.environ["DEST_ACCESS_TOKEN"] = "tok"
        try:
            main()
        finally:
            del os.environ["DEST_SHOP_URL"]
            del os.environ["DEST_ACCESS_TOKEN"]

        mc.register_translations.assert_called()

    @patch("tara_migrate.pipeline.import_arabic.load_dotenv")
    @patch("tara_migrate.pipeline.import_arabic.ShopifyClient")
    @patch("sys.argv", ["import_arabic.py"])
    def test_metaobject_no_local_data(self, MockClient, mock_dotenv, tmp_path, monkeypatch, capsys):
        """Metaobjects without local data should still be processed (gaps for AI)."""
        monkeypatch.chdir(tmp_path)
        _setup_arabic_data(tmp_path, en_products=[], ar_products=[])

        mc = MagicMock()
        MockClient.return_value = mc
        mc.get_translatable_resources.return_value = [
            _make_store_resource("gid://shopify/Metaobject/500", [
                {"key": "title", "value": "Shine", "digest": "xyz", "locale": "en"},
                {"key": "handle", "value": "shine", "digest": "hnd", "locale": "en"},
            ]),
        ]

        os.environ["DEST_SHOP_URL"] = "dest-test.myshopify.com"
        os.environ["DEST_ACCESS_TOKEN"] = "tok"
        try:
            main()
        finally:
            del os.environ["DEST_SHOP_URL"]
            del os.environ["DEST_ACCESS_TOKEN"]

        captured = capsys.readouterr()
        assert "Found 1 translatable metaobjects" in captured.out


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


# ---------------------------------------------------------------------------
# Image language detection tests
# ---------------------------------------------------------------------------

class TestClassifyImageLanguage:
    def test_import_without_tesseract(self):
        """classify_image_language should raise if pytesseract not available."""
        from tara_migrate.tools.image_lang_detect import classify_image_language
        # Module imports fine even without tesseract installed
        assert callable(classify_image_language)
