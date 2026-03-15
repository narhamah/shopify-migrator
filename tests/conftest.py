"""Shared fixtures for the shopify-migrator test suite."""
import json

import pytest


# ---------------------------------------------------------------------------
# Sample data factories
# ---------------------------------------------------------------------------

def make_product(id=1001, handle="test-product", title="Test Product", price="29.99"):
    return {
        "id": id,
        "handle": handle,
        "title": title,
        "body_html": "<p>Body</p>",
        "vendor": "TARA",
        "product_type": "Serum",
        "tags": "skincare, serum",
        "status": "active",
        "images": [{"src": "https://cdn.shopify.com/img.jpg"}],
        "variants": [
            {
                "title": "Default Title",
                "price": price,
                "compare_at_price": "39.99",
                "sku": "SKU001",
                "barcode": "",
                "weight": 0.5,
                "weight_unit": "kg",
                "inventory_management": "shopify",
                "option1": "50ml",
                "option2": None,
                "option3": None,
                "requires_shipping": True,
                "taxable": True,
            }
        ],
        "options": [{"name": "Size", "values": ["50ml"]}],
        "metafields": [
            {"namespace": "custom", "key": "tagline", "value": "Luxury care", "type": "single_line_text_field"},
            {"namespace": "custom", "key": "ingredients", "value": '["gid://shopify/Metaobject/1"]',
             "type": "list.metaobject_reference"},
        ],
    }


def make_collection(id=2001, handle="test-collection", title="Test Collection"):
    return {
        "id": id,
        "handle": handle,
        "title": title,
        "body_html": "<p>Collection body</p>",
        "image": {"src": "https://cdn.shopify.com/coll.jpg"},
    }


def make_page(id=3001, handle="test-page", title="Test Page"):
    return {
        "id": id,
        "handle": handle,
        "title": title,
        "body_html": "<p>Page body</p>",
        "published_at": "2024-01-01T00:00:00Z",
        "template_suffix": "",
    }


def make_blog(id=4001, handle="test-blog", title="Test Blog"):
    return {"id": id, "handle": handle, "title": title}


def make_article(id=5001, blog_id=4001, handle="test-article", title="Test Article"):
    return {
        "id": id,
        "_blog_id": blog_id,
        "_blog_handle": "test-blog",
        "handle": handle,
        "title": title,
        "body_html": "<p>Article body</p>",
        "summary_html": "<p>Summary</p>",
        "tags": "tag1, tag2",
        "published_at": "2024-01-01T00:00:00Z",
        "author": "Author Name",
        "image": {"src": "https://cdn.shopify.com/art.jpg"},
        "metafields": [
            {"namespace": "custom", "key": "blog_summary", "value": "Summary text",
             "type": "single_line_text_field"},
            {"namespace": "custom", "key": "author", "value": "gid://shopify/Metaobject/99",
             "type": "metaobject_reference"},
            {"namespace": "custom", "key": "listing_image", "value": "gid://shopify/MediaImage/77",
             "type": "file_reference"},
        ],
    }


def make_metaobject(type="benefit", handle="test-benefit", id="gid://shopify/Metaobject/100"):
    fields_by_type = {
        "benefit": [
            {"key": "title", "value": "Shine", "type": "single_line_text_field"},
            {"key": "description", "value": "Makes hair shine", "type": "single_line_text_field"},
            {"key": "category", "value": "Hair", "type": "single_line_text_field"},
            {"key": "icon_label", "value": "shine", "type": "single_line_text_field"},
        ],
        "ingredient": [
            {"key": "name", "value": "Argan Oil", "type": "single_line_text_field"},
            {"key": "image", "value": "gid://shopify/MediaImage/10", "type": "file_reference"},
            {"key": "icon", "value": "gid://shopify/MediaImage/11", "type": "file_reference"},
            {"key": "science_images", "value": '["gid://shopify/MediaImage/12"]', "type": "list.file_reference"},
            {"key": "benefits", "value": '["gid://shopify/Metaobject/100"]', "type": "list.metaobject_reference"},
            {"key": "collection", "value": "gid://shopify/Collection/2001", "type": "collection_reference"},
            {"key": "description", "value": '{"type":"root","children":[]}', "type": "rich_text_field"},
            {"key": "one_line_benefit", "value": "Nourishes", "type": "single_line_text_field"},
            {"key": "source", "value": "Morocco", "type": "single_line_text_field"},
            {"key": "origin", "value": "Organic", "type": "single_line_text_field"},
            {"key": "category", "value": "Oil", "type": "single_line_text_field"},
            {"key": "concern", "value": "Dryness", "type": "single_line_text_field"},
            {"key": "is_hero", "value": "true", "type": "boolean"},
            {"key": "sort_order", "value": "1", "type": "number_integer"},
        ],
        "blog_author": [
            {"key": "name", "value": "Jane", "type": "single_line_text_field"},
            {"key": "bio", "value": "Author bio", "type": "single_line_text_field"},
            {"key": "avatar", "value": "gid://shopify/MediaImage/20", "type": "file_reference"},
        ],
        "faq_entry": [
            {"key": "question", "value": "How?", "type": "single_line_text_field"},
            {"key": "answer", "value": '{"type":"root","children":[{"type":"text","value":"Like this"}]}',
             "type": "rich_text_field"},
        ],
    }
    return {
        "id": id,
        "handle": handle,
        "type": type,
        "fields": fields_by_type.get(type, []),
    }


def make_metaobjects_data():
    """Full metaobjects.json structure."""
    return {
        "benefit": {
            "definition": {"type": "benefit", "name": "Benefit", "fieldDefinitions": []},
            "objects": [make_metaobject("benefit", "shine", "gid://shopify/Metaobject/100")],
        },
        "faq_entry": {
            "definition": {"type": "faq_entry", "name": "FAQ Entry", "fieldDefinitions": []},
            "objects": [make_metaobject("faq_entry", "how-to", "gid://shopify/Metaobject/200")],
        },
        "blog_author": {
            "definition": {"type": "blog_author", "name": "Blog Author", "fieldDefinitions": []},
            "objects": [make_metaobject("blog_author", "jane", "gid://shopify/Metaobject/300")],
        },
        "ingredient": {
            "definition": {"type": "ingredient", "name": "Ingredient", "fieldDefinitions": []},
            "objects": [make_metaobject("ingredient", "argan-oil", "gid://shopify/Metaobject/400")],
        },
    }


def make_id_map():
    return {
        "products": {"1001": 9001},
        "collections": {"2001": 9002},
        "pages": {"3001": 9003},
        "blogs": {"4001": 9004},
        "articles": {"5001": 9005},
        "metaobjects_benefit": {"gid://shopify/Metaobject/100": "gid://shopify/Metaobject/500"},
        "metaobjects_faq_entry": {"gid://shopify/Metaobject/200": "gid://shopify/Metaobject/600"},
        "metaobjects_blog_author": {"gid://shopify/Metaobject/300": "gid://shopify/Metaobject/700"},
        "metaobjects_ingredient": {"gid://shopify/Metaobject/400": "gid://shopify/Metaobject/800"},
        "_ref_remapped": {},
    }


@pytest.fixture
def tmp_data_dir(tmp_path):
    """Create a tmp directory structure mimicking data/."""
    for subdir in ["source_export", "english", "arabic"]:
        (tmp_path / subdir).mkdir()
    return tmp_path
