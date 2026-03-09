"""Tests for translate_to_english.py (TOON batched translation)."""
import json
import os
from unittest.mock import MagicMock, patch

import pytest

from translate_gaps import (
    to_toon, from_toon, _toon_escape, _toon_unescape,
    extract_product_fields, extract_collection_fields,
    extract_page_fields, extract_blog_fields,
    extract_article_fields, extract_metaobject_fields,
    apply_translations, adaptive_batch, load_json, save_json,
)


class TestToonFormat:
    def test_roundtrip(self):
        entries = [
            {"id": "prod.1.title", "value": "Hello World"},
            {"id": "prod.2.body", "value": "Line1\nLine2"},
        ]
        toon = to_toon(entries)
        parsed = from_toon(toon)
        assert len(parsed) == 2
        assert parsed[0]["value"] == "Hello World"
        assert parsed[1]["value"] == "Line1\nLine2"

    def test_escape_pipe(self):
        entries = [{"id": "x", "value": "a|b"}]
        toon = to_toon(entries)
        assert "\\p" in toon
        parsed = from_toon(toon)
        assert parsed[0]["value"] == "a|b"

    def test_escape_backslash(self):
        entries = [{"id": "x", "value": "a\\b"}]
        parsed = from_toon(to_toon(entries))
        assert parsed[0]["value"] == "a\\b"

    def test_empty_input(self):
        assert from_toon("") == []
        assert to_toon([]) == ""


class TestFieldExtraction:
    def test_product_core_fields(self):
        product = {
            "handle": "shampoo",
            "title": "Shampoo",
            "body_html": "<p>Desc</p>",
            "product_type": "Hair Care",
            "vendor": "TARA",
            "tags": "tag1, tag2",
        }
        fields = extract_product_fields(product, "prod")
        ids = {f["id"] for f in fields}
        assert "prod.shampoo.handle" in ids
        assert "prod.shampoo.title" in ids
        assert "prod.shampoo.body_html" in ids
        assert "prod.shampoo.vendor" in ids
        assert "prod.shampoo.tags" in ids

    def test_product_image_alt(self):
        product = {
            "handle": "p1",
            "images": [{"src": "img.jpg", "alt": "Alt text"}],
        }
        fields = extract_product_fields(product, "prod")
        alts = [f for f in fields if "img0.alt" in f["id"]]
        assert len(alts) == 1
        assert alts[0]["value"] == "Alt text"

    def test_product_text_metafields(self):
        product = {
            "handle": "p1",
            "metafields": [
                {"namespace": "custom", "key": "tagline", "type": "single_line_text_field", "value": "Best shampoo"},
                {"namespace": "custom", "key": "ingredients_ref", "type": "list.metaobject_reference", "value": "gid://"},
            ],
        }
        fields = extract_product_fields(product, "prod")
        mf_fields = [f for f in fields if ".mf." in f["id"]]
        assert len(mf_fields) == 1
        assert mf_fields[0]["id"] == "prod.p1.mf.custom.tagline"

    def test_collection_image_alt(self):
        coll = {
            "handle": "c1",
            "title": "Coll",
            "image": {"alt": "Collection image"},
        }
        fields = extract_collection_fields(coll, "coll")
        alts = [f for f in fields if "image.alt" in f["id"]]
        assert len(alts) == 1

    def test_blog_fields(self):
        blog = {"handle": "news", "title": "News", "tags": "tag1, tag2"}
        fields = extract_blog_fields(blog, "blog")
        ids = {f["id"] for f in fields}
        assert "blog.news.title" in ids
        assert "blog.news.handle" in ids
        assert "blog.news.tags" in ids

    def test_article_full_fields(self):
        article = {
            "handle": "post-1",
            "title": "Post",
            "body_html": "<p>Body</p>",
            "summary_html": "<p>Summary</p>",
            "author": "Jane",
            "tags": "t1",
            "image": {"alt": "Article image"},
            "metafields": [
                {"namespace": "custom", "key": "blog_summary", "type": "single_line_text_field", "value": "Summary"},
            ],
        }
        fields = extract_article_fields(article, "art")
        ids = {f["id"] for f in fields}
        assert "art.post-1.handle" in ids
        assert "art.post-1.author" in ids
        assert "art.post-1.image.alt" in ids
        assert "art.post-1.mf.custom.blog_summary" in ids

    def test_metaobject_type_based(self):
        """Metaobject extraction is type-based, not whitelist-based."""
        mo_data = {
            "custom_type": {
                "definition": {},
                "objects": [{
                    "handle": "obj1",
                    "fields": [
                        {"key": "name", "type": "single_line_text_field", "value": "Name"},
                        {"key": "ref", "type": "metaobject_reference", "value": "gid://"},
                    ],
                }],
            },
        }
        fields = extract_metaobject_fields(mo_data, "mo")
        # Should extract the text field but not the reference
        text_fields = [f for f in fields if f["id"].endswith(".name")]
        ref_fields = [f for f in fields if f["id"].endswith(".ref")]
        assert len(text_fields) == 1
        assert len(ref_fields) == 0

    def test_metaobject_handle_extraction(self):
        mo_data = {
            "benefit": {
                "definition": {},
                "objects": [{"handle": "shine", "fields": []}],
            },
        }
        fields = extract_metaobject_fields(mo_data, "mo")
        handle_fields = [f for f in fields if f["id"].endswith(".handle")]
        assert len(handle_fields) == 1
        assert handle_fields[0]["value"] == "shine"


class TestApplyTranslations:
    def test_apply_product(self):
        products = [{"handle": "p1", "title": "Old", "vendor": "V"}]
        translations = {"prod.p1.title": "New", "prod.p1.vendor": "NewV"}
        apply_translations(translations, products, [], [], [], {})
        assert products[0]["title"] == "New"
        assert products[0]["vendor"] == "NewV"

    def test_apply_blog(self):
        blogs = [{"handle": "news", "title": "Old", "tags": "old"}]
        translations = {"blog.news.title": "New", "blog.news.tags": "new"}
        apply_translations({}, [], [], [], [], {}, blogs=blogs)
        # No translations for blogs in the main dict
        assert blogs[0]["title"] == "Old"
        # Now with blog translations
        apply_translations(translations, [], [], [], [], {}, blogs=blogs)
        assert blogs[0]["title"] == "New"
        assert blogs[0]["tags"] == "new"

    def test_apply_article_handle(self):
        articles = [{"handle": "old-handle", "title": "T"}]
        translations = {"art.old-handle.handle": "new handle"}
        apply_translations({}, [], [], [], articles, {})
        assert articles[0]["handle"] == "old-handle"
        apply_translations(translations, [], [], [], articles, {})
        assert articles[0]["handle"] == "new-handle"

    def test_apply_image_alt(self):
        products = [{"handle": "p1", "images": [{"src": "img.jpg", "alt": "Old"}]}]
        translations = {"prod.p1.img0.alt": "New alt"}
        apply_translations(translations, products, [], [], [], {})
        assert products[0]["images"][0]["alt"] == "New alt"


class TestAdaptiveBatch:
    def test_small_fields_packed(self):
        fields = [{"id": f"f{i}", "value": "short"} for i in range(10)]
        batches = adaptive_batch(fields, max_tokens=1000)
        assert len(batches) == 1

    def test_large_field_own_batch(self):
        fields = [
            {"id": "small", "value": "x"},
            {"id": "huge", "value": "y" * 50000},
            {"id": "small2", "value": "z"},
        ]
        batches = adaptive_batch(fields, max_tokens=100)
        assert len(batches) >= 2


class TestLoadSaveJson:
    def test_load_json(self, tmp_path):
        f = tmp_path / "data.json"
        f.write_text(json.dumps([1, 2, 3]))
        assert load_json(str(f)) == [1, 2, 3]

    def test_save_json(self, tmp_path):
        f = str(tmp_path / "out.json")
        save_json({"a": 1}, f)
        with open(f) as fh:
            assert json.load(fh) == {"a": 1}
