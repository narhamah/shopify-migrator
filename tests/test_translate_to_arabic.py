"""Tests for translate_to_arabic.py (TOON batched translation)."""
import json
import os
from unittest.mock import MagicMock, patch

import pytest

from translate_gaps import (
    to_toon, from_toon,
    extract_product_fields, extract_collection_fields,
    extract_page_fields, extract_blog_fields,
    extract_article_fields, extract_metaobject_fields,
    apply_translations, adaptive_batch, load_json, save_json,
    TEXT_METAFIELD_TYPES,
)


class TestResumeLogic:
    """Test the TOON progress-based resume at the unit level."""

    def test_progress_filters_remaining(self):
        all_fields = [
            {"id": "prod.p1.title", "value": "T1"},
            {"id": "prod.p2.title", "value": "T2"},
        ]
        already_done = {"prod.p1.title": "Translated"}
        remaining = [f for f in all_fields if f["id"] not in already_done]
        assert len(remaining) == 1
        assert remaining[0]["id"] == "prod.p2.title"


class TestTextMetafieldTypes:
    def test_text_types(self):
        assert "single_line_text_field" in TEXT_METAFIELD_TYPES
        assert "multi_line_text_field" in TEXT_METAFIELD_TYPES
        assert "rich_text_field" in TEXT_METAFIELD_TYPES

    def test_non_text_types_excluded(self):
        assert "metaobject_reference" not in TEXT_METAFIELD_TYPES
        assert "number_integer" not in TEXT_METAFIELD_TYPES
        assert "boolean" not in TEXT_METAFIELD_TYPES


class TestCollectionMetafields:
    def test_text_type_metafields_extracted(self):
        coll = {
            "handle": "c1",
            "title": "Coll",
            "metafields": [
                {"namespace": "global", "key": "title_tag", "type": "single_line_text_field", "value": "SEO Title"},
                {"namespace": "custom", "key": "ref", "type": "metaobject_reference", "value": "gid://"},
            ],
        }
        fields = extract_collection_fields(coll, "coll")
        mf_fields = [f for f in fields if ".mf." in f["id"]]
        assert len(mf_fields) == 1
        assert mf_fields[0]["id"] == "coll.c1.mf.global.title_tag"


class TestPageMetafields:
    def test_text_type_metafields_extracted(self):
        page = {
            "handle": "about",
            "title": "About",
            "metafields": [
                {"namespace": "custom", "key": "subtitle", "type": "multi_line_text_field", "value": "Sub"},
                {"namespace": "custom", "key": "color", "type": "color", "value": "#fff"},
            ],
        }
        fields = extract_page_fields(page, "page")
        mf_fields = [f for f in fields if ".mf." in f["id"]]
        assert len(mf_fields) == 1


class TestApplyTranslationsArabic:
    def test_apply_collection_image_alt(self):
        collections = [{"handle": "c1", "title": "C", "image": {"alt": "Old"}}]
        translations = {"coll.c1.image.alt": "New"}
        apply_translations(translations, [], collections, [], [], {})
        assert collections[0]["image"]["alt"] == "New"

    def test_apply_article_author(self):
        articles = [{"handle": "a1", "title": "T", "author": "Old"}]
        translations = {"art.a1.author": "New Author"}
        apply_translations(translations, [], [], [], articles, {})
        assert articles[0]["author"] == "New Author"

    def test_apply_metaobject_handle(self):
        metaobjects = {
            "benefit": {
                "definition": {},
                "objects": [{"handle": "old-handle", "fields": []}],
            },
        }
        translations = {"mo.benefit.old-handle.handle": "new handle text"}
        apply_translations(translations, [], [], [], [], metaobjects)
        assert metaobjects["benefit"]["objects"][0]["handle"] == "new-handle-text"

    def test_apply_page_metafields(self):
        pages = [{
            "handle": "about",
            "title": "About",
            "metafields": [
                {"namespace": "global", "key": "title_tag", "type": "single_line_text_field", "value": "Old SEO"},
            ],
        }]
        translations = {"page.about.mf.global.title_tag": "New SEO"}
        apply_translations(translations, [], [], pages, [], {})
        assert pages[0]["metafields"][0]["value"] == "New SEO"
