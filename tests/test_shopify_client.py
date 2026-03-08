"""Tests for shopify_client.py — ShopifyClient class."""
import io
import json
from unittest.mock import MagicMock, patch, PropertyMock

import pytest
import requests

from shopify_client import ShopifyClient, API_VERSION


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------

class TestShopifyClientInit:
    def test_adds_https_prefix(self):
        c = ShopifyClient("mystore.myshopify.com", "tok")
        assert c.shop_url == "https://mystore.myshopify.com"

    def test_preserves_existing_https(self):
        c = ShopifyClient("https://mystore.myshopify.com", "tok")
        assert c.shop_url == "https://mystore.myshopify.com"

    def test_strips_trailing_slash(self):
        c = ShopifyClient("https://mystore.myshopify.com/", "tok")
        assert c.shop_url == "https://mystore.myshopify.com"

    def test_base_url(self):
        c = ShopifyClient("mystore.myshopify.com", "tok")
        assert c.base_url == f"https://mystore.myshopify.com/admin/api/{API_VERSION}"

    def test_graphql_url(self):
        c = ShopifyClient("mystore.myshopify.com", "tok")
        assert c.graphql_url.endswith("/graphql.json")

    def test_session_headers(self):
        c = ShopifyClient("mystore.myshopify.com", "tok123")
        assert c.session.headers["X-Shopify-Access-Token"] == "tok123"
        assert c.session.headers["Content-Type"] == "application/json"


# ---------------------------------------------------------------------------
# Low-level helpers
# ---------------------------------------------------------------------------

class TestRequest:
    def test_successful_request(self):
        c = ShopifyClient("shop.myshopify.com", "tok")
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        c.session.request = MagicMock(return_value=mock_resp)
        resp = c._request("GET", "shop.json")
        assert resp == mock_resp

    def test_rate_limit_retry(self):
        c = ShopifyClient("shop.myshopify.com", "tok")
        rate_resp = MagicMock()
        rate_resp.status_code = 429
        rate_resp.headers = {"Retry-After": "0"}
        ok_resp = MagicMock()
        ok_resp.status_code = 200
        c.session.request = MagicMock(side_effect=[rate_resp, ok_resp])
        resp = c._request("GET", "shop.json")
        assert resp == ok_resp
        assert c.session.request.call_count == 2

    def test_http_error_raised(self):
        c = ShopifyClient("shop.myshopify.com", "tok")
        mock_resp = MagicMock()
        mock_resp.status_code = 500
        mock_resp.raise_for_status.side_effect = requests.exceptions.HTTPError("500")
        c.session.request = MagicMock(return_value=mock_resp)
        with pytest.raises(requests.exceptions.HTTPError):
            c._request("GET", "shop.json")


class TestGetJson:
    def test_returns_json_and_headers(self):
        c = ShopifyClient("shop.myshopify.com", "tok")
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"shop": {"name": "Test"}}
        mock_resp.headers = {"X-Custom": "val"}
        c.session.request = MagicMock(return_value=mock_resp)
        data, headers = c._get_json("shop.json")
        assert data == {"shop": {"name": "Test"}}
        assert headers["X-Custom"] == "val"


class TestPaginate:
    def test_single_page(self):
        c = ShopifyClient("shop.myshopify.com", "tok")
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"products": [{"id": 1}, {"id": 2}]}
        mock_resp.headers = {"Link": ""}
        c.session.request = MagicMock(return_value=mock_resp)
        result = c._paginate("products.json", "products")
        assert len(result) == 2

    def test_multi_page(self):
        c = ShopifyClient("shop.myshopify.com", "tok")
        resp1 = MagicMock()
        resp1.status_code = 200
        resp1.json.return_value = {"products": [{"id": 1}]}
        resp1.headers = {"Link": '<https://shop.myshopify.com/next>; rel="next"'}
        resp2 = MagicMock()
        resp2.status_code = 200
        resp2.json.return_value = {"products": [{"id": 2}]}
        resp2.headers = {"Link": ""}
        c.session.request = MagicMock(side_effect=[resp1, resp2])
        result = c._paginate("products.json", "products")
        assert len(result) == 2

    def test_rate_limit_in_paginate(self):
        c = ShopifyClient("shop.myshopify.com", "tok")
        rate_resp = MagicMock()
        rate_resp.status_code = 429
        rate_resp.headers = {"Retry-After": "0"}
        ok_resp = MagicMock()
        ok_resp.status_code = 200
        ok_resp.json.return_value = {"products": [{"id": 1}]}
        ok_resp.headers = {"Link": ""}
        c.session.request = MagicMock(side_effect=[rate_resp, ok_resp])
        result = c._paginate("products.json", "products")
        assert len(result) == 1

    def test_empty_resource_key(self):
        c = ShopifyClient("shop.myshopify.com", "tok")
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"other": []}
        mock_resp.headers = {"Link": ""}
        c.session.request = MagicMock(return_value=mock_resp)
        result = c._paginate("products.json", "products")
        assert result == []

    def test_default_limit_param(self):
        c = ShopifyClient("shop.myshopify.com", "tok")
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"products": []}
        mock_resp.headers = {"Link": ""}
        c.session.request = MagicMock(return_value=mock_resp)
        c._paginate("products.json", "products")
        call_kwargs = c.session.request.call_args
        assert call_kwargs[1]["params"]["limit"] == 250


class TestGraphQL:
    def test_success(self):
        c = ShopifyClient("shop.myshopify.com", "tok")
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"data": {"shop": {"name": "Test"}}}
        c.session.post = MagicMock(return_value=mock_resp)
        result = c._graphql("{ shop { name } }")
        assert result == {"shop": {"name": "Test"}}

    def test_with_variables(self):
        c = ShopifyClient("shop.myshopify.com", "tok")
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"data": {"result": True}}
        c.session.post = MagicMock(return_value=mock_resp)
        result = c._graphql("mutation { ... }", {"id": "123"})
        call_args = c.session.post.call_args
        assert call_args[1]["json"]["variables"] == {"id": "123"}

    def test_graphql_errors_raise(self):
        c = ShopifyClient("shop.myshopify.com", "tok")
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"errors": [{"message": "bad query"}]}
        c.session.post = MagicMock(return_value=mock_resp)
        with pytest.raises(Exception, match="GraphQL errors"):
            c._graphql("{ bad }")

    def test_graphql_rate_limit(self):
        c = ShopifyClient("shop.myshopify.com", "tok")
        rate_resp = MagicMock()
        rate_resp.status_code = 429
        rate_resp.headers = {"Retry-After": "0"}
        ok_resp = MagicMock()
        ok_resp.status_code = 200
        ok_resp.json.return_value = {"data": {"ok": True}}
        c.session.post = MagicMock(side_effect=[rate_resp, ok_resp])
        result = c._graphql("{ ok }")
        assert result == {"ok": True}

    def test_no_data_key(self):
        c = ShopifyClient("shop.myshopify.com", "tok")
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {}
        c.session.post = MagicMock(return_value=mock_resp)
        result = c._graphql("{ ok }")
        assert result == {}


# ---------------------------------------------------------------------------
# REST read methods
# ---------------------------------------------------------------------------

class TestRESTReads:
    def setup_method(self):
        self.c = ShopifyClient("shop.myshopify.com", "tok")

    def _mock_get_json(self, data):
        self.c._get_json = MagicMock(return_value=(data, {}))

    def test_get_shop(self):
        self._mock_get_json({"shop": {"name": "Store"}})
        assert self.c.get_shop() == {"name": "Store"}

    def test_get_shop_empty(self):
        self._mock_get_json({})
        assert self.c.get_shop() == {}

    def test_get_products(self):
        self.c._paginate = MagicMock(return_value=[{"id": 1}])
        assert self.c.get_products() == [{"id": 1}]
        self.c._paginate.assert_called_once_with("products.json", "products")

    def test_get_collections(self):
        self.c._paginate = MagicMock(side_effect=[[{"id": 1}], [{"id": 2}]])
        result = self.c.get_collections()
        assert len(result) == 2

    def test_get_pages(self):
        self.c._paginate = MagicMock(return_value=[])
        assert self.c.get_pages() == []

    def test_get_blogs(self):
        self.c._paginate = MagicMock(return_value=[{"id": 1}])
        assert self.c.get_blogs() == [{"id": 1}]

    def test_get_articles(self):
        self.c._paginate = MagicMock(return_value=[{"id": 1}])
        result = self.c.get_articles(42)
        self.c._paginate.assert_called_once_with("blogs/42/articles.json", "articles")

    def test_get_metafields(self):
        self.c._paginate = MagicMock(return_value=[{"key": "k"}])
        result = self.c.get_metafields("products", 123)
        self.c._paginate.assert_called_once_with("products/123/metafields.json", "metafields")


# ---------------------------------------------------------------------------
# REST write methods
# ---------------------------------------------------------------------------

class TestRESTWrites:
    def setup_method(self):
        self.c = ShopifyClient("shop.myshopify.com", "tok")
        self.mock_resp = MagicMock()
        self.mock_resp.status_code = 201
        self.c._request = MagicMock(return_value=self.mock_resp)

    def test_create_product(self):
        self.mock_resp.json.return_value = {"product": {"id": 1}}
        result = self.c.create_product({"title": "P"})
        assert result == {"id": 1}
        self.c._request.assert_called_once_with("POST", "products.json", json={"product": {"title": "P"}})

    def test_update_product(self):
        self.mock_resp.json.return_value = {"product": {"id": 1}}
        result = self.c.update_product(1, {"title": "New"})
        assert result == {"id": 1}

    def test_create_custom_collection(self):
        self.mock_resp.json.return_value = {"custom_collection": {"id": 2}}
        result = self.c.create_custom_collection({"title": "C"})
        assert result == {"id": 2}

    def test_create_page(self):
        self.mock_resp.json.return_value = {"page": {"id": 3}}
        result = self.c.create_page({"title": "P"})
        assert result == {"id": 3}

    def test_create_blog(self):
        self.mock_resp.json.return_value = {"blog": {"id": 4}}
        result = self.c.create_blog({"title": "B"})
        assert result == {"id": 4}

    def test_create_article(self):
        self.mock_resp.json.return_value = {"article": {"id": 5}}
        result = self.c.create_article(4, {"title": "A"})
        assert result == {"id": 5}

    def test_create_metafield(self):
        self.mock_resp.json.return_value = {"metafield": {"id": 6}}
        result = self.c.create_metafield("products", 1, {"namespace": "custom", "key": "k"})
        assert result == {"id": 6}


# ---------------------------------------------------------------------------
# REST lookup by handle
# ---------------------------------------------------------------------------

class TestLookupByHandle:
    def setup_method(self):
        self.c = ShopifyClient("shop.myshopify.com", "tok")

    def test_get_products_by_handle(self):
        self.c._get_json = MagicMock(return_value=({"products": [{"id": 1}]}, {}))
        result = self.c.get_products_by_handle("my-product")
        assert result == [{"id": 1}]

    def test_get_pages_by_handle(self):
        self.c._get_json = MagicMock(return_value=({"pages": []}, {}))
        result = self.c.get_pages_by_handle("my-page")
        assert result == []

    def test_get_collections_by_handle(self):
        self.c._get_json = MagicMock(return_value=({"custom_collections": [{"id": 2}]}, {}))
        result = self.c.get_collections_by_handle("my-coll")
        assert result == [{"id": 2}]

    def test_get_blogs_by_handle(self):
        self.c._get_json = MagicMock(return_value=({"blogs": [{"id": 3}]}, {}))
        result = self.c.get_blogs_by_handle("my-blog")
        assert result == [{"id": 3}]


# ---------------------------------------------------------------------------
# GraphQL: Metaobjects
# ---------------------------------------------------------------------------

class TestMetaobjects:
    def setup_method(self):
        self.c = ShopifyClient("shop.myshopify.com", "tok")

    def test_get_metaobject_definitions(self):
        self.c._graphql = MagicMock(return_value={
            "metaobjectDefinitions": {
                "edges": [{"node": {"id": "gid://1", "type": "benefit", "name": "Benefit", "fieldDefinitions": []}}]
            }
        })
        result = self.c.get_metaobject_definitions()
        assert len(result) == 1
        assert result[0]["type"] == "benefit"

    def test_get_metaobjects_single_page(self):
        self.c._graphql = MagicMock(return_value={
            "metaobjects": {
                "edges": [{"cursor": "c1", "node": {"id": "gid://1", "handle": "h1", "type": "benefit", "fields": []}}],
                "pageInfo": {"hasNextPage": False},
            }
        })
        result = self.c.get_metaobjects("benefit")
        assert len(result) == 1

    def test_get_metaobjects_paginated(self):
        page1 = {
            "metaobjects": {
                "edges": [{"cursor": "c1", "node": {"id": "gid://1", "handle": "h1", "type": "benefit", "fields": []}}],
                "pageInfo": {"hasNextPage": True},
            }
        }
        page2 = {
            "metaobjects": {
                "edges": [{"cursor": "c2", "node": {"id": "gid://2", "handle": "h2", "type": "benefit", "fields": []}}],
                "pageInfo": {"hasNextPage": False},
            }
        }
        self.c._graphql = MagicMock(side_effect=[page1, page2])
        result = self.c.get_metaobjects("benefit")
        assert len(result) == 2

    def test_create_metaobject_definition_success(self):
        self.c._graphql = MagicMock(return_value={
            "metaobjectDefinitionCreate": {
                "metaobjectDefinition": {"id": "gid://1", "type": "benefit"},
                "userErrors": [],
            }
        })
        result = self.c.create_metaobject_definition({"type": "benefit"})
        assert result["id"] == "gid://1"

    def test_create_metaobject_definition_already_exists(self):
        self.c._graphql = MagicMock(return_value={
            "metaobjectDefinitionCreate": {
                "metaobjectDefinition": None,
                "userErrors": [{"field": "type", "message": "Type already exists"}],
            }
        })
        result = self.c.create_metaobject_definition({"type": "benefit"})
        assert result is None

    def test_create_metaobject_definition_error(self):
        self.c._graphql = MagicMock(return_value={
            "metaobjectDefinitionCreate": {
                "metaobjectDefinition": None,
                "userErrors": [{"field": "type", "message": "Invalid type"}],
            }
        })
        with pytest.raises(Exception, match="MetaobjectDefinitionCreate"):
            self.c.create_metaobject_definition({"type": "bad"})

    def test_create_metaobject_success(self):
        self.c._graphql = MagicMock(return_value={
            "metaobjectCreate": {
                "metaobject": {"id": "gid://1", "handle": "h1"},
                "userErrors": [],
            }
        })
        result = self.c.create_metaobject({"type": "benefit", "handle": "h1"})
        assert result["handle"] == "h1"

    def test_create_metaobject_already_exists(self):
        self.c._graphql = MagicMock(return_value={
            "metaobjectCreate": {
                "metaobject": None,
                "userErrors": [{"field": "handle", "message": "Handle already exists"}],
            }
        })
        result = self.c.create_metaobject({"type": "benefit", "handle": "h1"})
        assert result is None

    def test_create_metaobject_error(self):
        self.c._graphql = MagicMock(return_value={
            "metaobjectCreate": {
                "metaobject": None,
                "userErrors": [{"field": "handle", "message": "Something bad"}],
            }
        })
        with pytest.raises(Exception, match="MetaobjectCreate"):
            self.c.create_metaobject({"type": "bad"})

    def test_update_metaobject(self):
        self.c._graphql = MagicMock(return_value={
            "metaobjectUpdate": {
                "metaobject": {"id": "gid://1", "handle": "h1"},
                "userErrors": [],
            }
        })
        result = self.c.update_metaobject("gid://1", [{"key": "title", "value": "New"}])
        assert result["id"] == "gid://1"

    def test_update_metaobject_error(self):
        self.c._graphql = MagicMock(return_value={
            "metaobjectUpdate": {
                "metaobject": None,
                "userErrors": [{"field": "id", "message": "Not found"}],
            }
        })
        with pytest.raises(Exception, match="MetaobjectUpdate"):
            self.c.update_metaobject("gid://bad", [])

    def test_get_metaobjects_by_handle(self):
        self.c._graphql = MagicMock(return_value={
            "metaobjectByHandle": {"id": "gid://1", "handle": "h1", "type": "benefit", "fields": []}
        })
        result = self.c.get_metaobjects_by_handle("benefit", "h1")
        assert result["id"] == "gid://1"

    def test_get_metaobjects_by_handle_not_found(self):
        self.c._graphql = MagicMock(return_value={"metaobjectByHandle": None})
        result = self.c.get_metaobjects_by_handle("benefit", "nope")
        assert result is None


# ---------------------------------------------------------------------------
# GraphQL: Metafield Definitions
# ---------------------------------------------------------------------------

class TestMetafieldDefinitions:
    def setup_method(self):
        self.c = ShopifyClient("shop.myshopify.com", "tok")

    def test_get_metafield_definitions(self):
        self.c._graphql = MagicMock(return_value={
            "metafieldDefinitions": {
                "edges": [{"cursor": "c1", "node": {"id": "gid://1", "namespace": "custom", "key": "tagline",
                           "name": "Tagline", "type": {"name": "single_line_text_field"}, "ownerType": "PRODUCT"}}],
                "pageInfo": {"hasNextPage": False},
            }
        })
        result = self.c.get_metafield_definitions("PRODUCT")
        assert len(result) == 1

    def test_get_metafield_definitions_paginated(self):
        page1 = {"metafieldDefinitions": {
            "edges": [{"cursor": "c1", "node": {"id": "1"}}],
            "pageInfo": {"hasNextPage": True},
        }}
        page2 = {"metafieldDefinitions": {
            "edges": [{"cursor": "c2", "node": {"id": "2"}}],
            "pageInfo": {"hasNextPage": False},
        }}
        self.c._graphql = MagicMock(side_effect=[page1, page2])
        result = self.c.get_metafield_definitions("PRODUCT")
        assert len(result) == 2

    def test_create_metafield_definition_success(self):
        self.c._graphql = MagicMock(return_value={
            "metafieldDefinitionCreate": {
                "createdDefinition": {"id": "gid://1", "namespace": "custom", "key": "k", "name": "K"},
                "userErrors": [],
            }
        })
        result = self.c.create_metafield_definition({"namespace": "custom", "key": "k"})
        assert result["id"] == "gid://1"

    def test_create_metafield_definition_already_exists(self):
        self.c._graphql = MagicMock(return_value={
            "metafieldDefinitionCreate": {
                "createdDefinition": None,
                "userErrors": [{"field": "key", "message": "already exists"}],
            }
        })
        result = self.c.create_metafield_definition({"namespace": "custom", "key": "k"})
        assert result is None

    def test_create_metafield_definition_error(self):
        self.c._graphql = MagicMock(return_value={
            "metafieldDefinitionCreate": {
                "createdDefinition": None,
                "userErrors": [{"field": "key", "message": "Invalid"}],
            }
        })
        with pytest.raises(Exception, match="MetafieldDefinitionCreate"):
            self.c.create_metafield_definition({"namespace": "custom", "key": "k"})

    def test_set_metafields_success(self):
        self.c._graphql = MagicMock(return_value={
            "metafieldsSet": {
                "metafields": [{"id": "gid://1", "namespace": "custom", "key": "k"}],
                "userErrors": [],
            }
        })
        result = self.c.set_metafields([{"ownerId": "gid://x", "namespace": "custom", "key": "k", "value": "v", "type": "single_line_text_field"}])
        assert len(result) == 1

    def test_set_metafields_error(self):
        self.c._graphql = MagicMock(return_value={
            "metafieldsSet": {
                "metafields": [],
                "userErrors": [{"field": "key", "message": "Bad"}],
            }
        })
        with pytest.raises(Exception, match="MetafieldsSet"):
            self.c.set_metafields([])


# ---------------------------------------------------------------------------
# GraphQL: Staged Uploads / File Create
# ---------------------------------------------------------------------------

class TestFileOperations:
    def setup_method(self):
        self.c = ShopifyClient("shop.myshopify.com", "tok")

    def test_staged_uploads_create(self):
        self.c._graphql = MagicMock(return_value={
            "stagedUploadsCreate": {
                "stagedTargets": [{"url": "https://upload.example.com", "resourceUrl": "https://res.example.com",
                                   "parameters": [{"name": "key", "value": "val"}]}],
                "userErrors": [],
            }
        })
        result = self.c.staged_uploads_create([{"filename": "f.jpg", "mimeType": "image/jpeg"}])
        assert result[0]["url"] == "https://upload.example.com"

    def test_staged_uploads_error(self):
        self.c._graphql = MagicMock(return_value={
            "stagedUploadsCreate": {
                "stagedTargets": [],
                "userErrors": [{"field": "input", "message": "Bad"}],
            }
        })
        with pytest.raises(Exception, match="StagedUploadsCreate"):
            self.c.staged_uploads_create([])

    def test_file_create(self):
        self.c._graphql = MagicMock(return_value={
            "fileCreate": {
                "files": [{"id": "gid://shopify/MediaImage/1", "alt": "test"}],
                "userErrors": [],
            }
        })
        result = self.c.file_create([{"alt": "test", "contentType": "IMAGE", "originalSource": "https://x"}])
        assert result[0]["id"] == "gid://shopify/MediaImage/1"

    def test_file_create_error(self):
        self.c._graphql = MagicMock(return_value={
            "fileCreate": {
                "files": [],
                "userErrors": [{"field": "files", "message": "Bad"}],
            }
        })
        with pytest.raises(Exception, match="FileCreate"):
            self.c.file_create([])

    def test_get_file_by_id_media_image(self):
        self.c._graphql = MagicMock(return_value={
            "node": {"id": "gid://1", "alt": "test", "fileStatus": "READY", "image": {"url": "https://img.jpg"}}
        })
        result = self.c.get_file_by_id("gid://1")
        assert result["image"]["url"] == "https://img.jpg"

    def test_get_file_by_id_not_found(self):
        self.c._graphql = MagicMock(return_value={"node": None})
        result = self.c.get_file_by_id("gid://nope")
        assert result is None

    @patch("requests.post")
    def test_upload_file_from_url(self, mock_post):
        c = ShopifyClient("shop.myshopify.com", "tok")
        # Mock the download
        download_resp = MagicMock()
        download_resp.content = b"fake image bytes"
        download_resp.raise_for_status = MagicMock()
        c.session.get = MagicMock(return_value=download_resp)

        # Mock staged upload
        c.staged_uploads_create = MagicMock(return_value=[{
            "url": "https://upload.example.com",
            "resourceUrl": "https://res.example.com",
            "parameters": [{"name": "key", "value": "val"}],
        }])

        # Mock post upload
        mock_post.return_value = MagicMock(status_code=200)
        mock_post.return_value.raise_for_status = MagicMock()

        # Mock file create
        c.file_create = MagicMock(return_value=[{"id": "gid://shopify/MediaImage/99"}])

        result = c.upload_file_from_url("https://cdn.example.com/image.jpg")
        assert result == "gid://shopify/MediaImage/99"

    @patch("requests.post")
    def test_upload_file_from_url_no_filename(self, mock_post):
        c = ShopifyClient("shop.myshopify.com", "tok")
        download_resp = MagicMock()
        download_resp.content = b"data"
        download_resp.raise_for_status = MagicMock()
        c.session.get = MagicMock(return_value=download_resp)
        c.staged_uploads_create = MagicMock(return_value=[{
            "url": "https://u.com", "resourceUrl": "https://r.com",
            "parameters": [],
        }])
        mock_post.return_value = MagicMock(status_code=200)
        mock_post.return_value.raise_for_status = MagicMock()
        c.file_create = MagicMock(return_value=[{"id": "gid://1"}])
        result = c.upload_file_from_url("https://example.com/path/image.png", filename="custom.png", alt="alt text")
        assert result == "gid://1"

    @patch("requests.post")
    def test_upload_file_no_created_files(self, mock_post):
        c = ShopifyClient("shop.myshopify.com", "tok")
        download_resp = MagicMock()
        download_resp.content = b"data"
        download_resp.raise_for_status = MagicMock()
        c.session.get = MagicMock(return_value=download_resp)
        c.staged_uploads_create = MagicMock(return_value=[{
            "url": "https://u.com", "resourceUrl": "https://r.com",
            "parameters": [],
        }])
        mock_post.return_value = MagicMock(status_code=200)
        mock_post.return_value.raise_for_status = MagicMock()
        c.file_create = MagicMock(return_value=[])
        result = c.upload_file_from_url("https://example.com/data.pdf")
        assert result is None

    @patch("requests.post")
    def test_upload_file_generic_file_type(self, mock_post):
        c = ShopifyClient("shop.myshopify.com", "tok")
        download_resp = MagicMock()
        download_resp.content = b"data"
        download_resp.raise_for_status = MagicMock()
        c.session.get = MagicMock(return_value=download_resp)
        c.staged_uploads_create = MagicMock(return_value=[{
            "url": "https://u.com", "resourceUrl": "https://r.com", "parameters": [],
        }])
        mock_post.return_value = MagicMock(status_code=200)
        mock_post.return_value.raise_for_status = MagicMock()
        c.file_create = MagicMock(return_value=[{"id": "gid://1"}])
        # .xyz has no known mime type
        result = c.upload_file_from_url("https://example.com/file.xyz_unknown")
        assert result == "gid://1"

    @patch("requests.post")
    def test_upload_url_with_no_path(self, mock_post):
        """URL with no path should fallback to 'file' filename."""
        c = ShopifyClient("shop.myshopify.com", "tok")
        download_resp = MagicMock()
        download_resp.content = b"data"
        download_resp.raise_for_status = MagicMock()
        c.session.get = MagicMock(return_value=download_resp)
        c.staged_uploads_create = MagicMock(return_value=[{
            "url": "https://u.com", "resourceUrl": "https://r.com", "parameters": [],
        }])
        mock_post.return_value = MagicMock(status_code=200)
        mock_post.return_value.raise_for_status = MagicMock()
        c.file_create = MagicMock(return_value=[{"id": "gid://1"}])
        result = c.upload_file_from_url("https://example.com/")
        assert result == "gid://1"


# ---------------------------------------------------------------------------
# GraphQL: Translations API
# ---------------------------------------------------------------------------

class TestTranslationsAPI:
    def setup_method(self):
        self.c = ShopifyClient("shop.myshopify.com", "tok")

    def test_register_translations(self):
        self.c._graphql = MagicMock(return_value={
            "translationsRegister": {
                "translations": [{"key": "title", "locale": "ar", "value": "عنوان"}],
                "userErrors": [],
            }
        })
        result = self.c.register_translations("gid://Product/1", "ar", [{"key": "title", "value": "عنوان", "translatableContentDigest": "abc"}])
        assert result[0]["locale"] == "ar"

    def test_register_translations_error(self):
        self.c._graphql = MagicMock(return_value={
            "translationsRegister": {
                "translations": [],
                "userErrors": [{"field": "resourceId", "message": "Not found"}],
            }
        })
        with pytest.raises(Exception, match="TranslationsRegister"):
            self.c.register_translations("gid://bad", "ar", [])

    def test_get_translatable_resources(self):
        self.c._graphql = MagicMock(return_value={
            "translatableResources": {
                "edges": [{"cursor": "c1", "node": {"resourceId": "gid://1", "translatableContent": []}}],
                "pageInfo": {"hasNextPage": False},
            }
        })
        result = self.c.get_translatable_resources("PRODUCT")
        assert len(result) == 1

    def test_get_translatable_resources_paginated(self):
        page1 = {"translatableResources": {
            "edges": [{"cursor": "c1", "node": {"resourceId": "gid://1", "translatableContent": []}}],
            "pageInfo": {"hasNextPage": True},
        }}
        page2 = {"translatableResources": {
            "edges": [{"cursor": "c2", "node": {"resourceId": "gid://2", "translatableContent": []}}],
            "pageInfo": {"hasNextPage": False},
        }}
        self.c._graphql = MagicMock(side_effect=[page1, page2])
        result = self.c.get_translatable_resources("PRODUCT")
        assert len(result) == 2

    def test_get_translatable_resource(self):
        self.c._graphql = MagicMock(return_value={
            "translatableResource": {"resourceId": "gid://1", "translatableContent": [{"key": "title"}]}
        })
        result = self.c.get_translatable_resource("gid://1")
        assert result["resourceId"] == "gid://1"

    def test_get_translatable_resource_not_found(self):
        self.c._graphql = MagicMock(return_value={})
        result = self.c.get_translatable_resource("gid://nope")
        assert result is None


# ---------------------------------------------------------------------------
# REST: Collects
# ---------------------------------------------------------------------------

class TestCollects:
    def setup_method(self):
        self.c = ShopifyClient("shop.myshopify.com", "tok")

    def test_get_collects(self):
        self.c._paginate = MagicMock(return_value=[{"id": 1, "product_id": 10, "collection_id": 20}])
        result = self.c.get_collects()
        assert len(result) == 1
        self.c._paginate.assert_called_once_with("collects.json", "collects", params={})

    def test_get_collects_by_collection(self):
        self.c._paginate = MagicMock(return_value=[])
        self.c.get_collects(collection_id=42)
        self.c._paginate.assert_called_once_with("collects.json", "collects", params={"collection_id": 42})

    def test_create_collect(self):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"collect": {"id": 99, "product_id": 10, "collection_id": 20}}
        self.c._request = MagicMock(return_value=mock_resp)

        result = self.c.create_collect(10, 20)
        assert result["id"] == 99


# ---------------------------------------------------------------------------
# REST: Redirects
# ---------------------------------------------------------------------------

class TestRedirects:
    def setup_method(self):
        self.c = ShopifyClient("shop.myshopify.com", "tok")

    def test_get_redirects(self):
        self.c._paginate = MagicMock(return_value=[{"id": 1, "path": "/old", "target": "/new"}])
        result = self.c.get_redirects()
        assert len(result) == 1

    def test_create_redirect(self):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"redirect": {"id": 1, "path": "/old", "target": "/new"}}
        self.c._request = MagicMock(return_value=mock_resp)

        result = self.c.create_redirect("/old", "/new")
        assert result["path"] == "/old"


# ---------------------------------------------------------------------------
# REST: Locations & Policies
# ---------------------------------------------------------------------------

class TestLocationsAndPolicies:
    def setup_method(self):
        self.c = ShopifyClient("shop.myshopify.com", "tok")

    def test_get_locations(self):
        self.c._get_json = MagicMock(return_value=({"locations": [{"id": 1, "name": "HQ"}]}, {}))
        result = self.c.get_locations()
        assert result[0]["name"] == "HQ"

    def test_get_policies(self):
        self.c._get_json = MagicMock(return_value=({"policies": [{"title": "Privacy"}]}, {}))
        result = self.c.get_policies()
        assert result[0]["title"] == "Privacy"


# ---------------------------------------------------------------------------
# GraphQL: Locales
# ---------------------------------------------------------------------------

class TestLocales:
    def setup_method(self):
        self.c = ShopifyClient("shop.myshopify.com", "tok")

    def test_enable_locale(self):
        self.c._graphql = MagicMock(return_value={
            "shopLocaleEnable": {
                "shopLocale": {"locale": "ar", "published": True},
                "userErrors": [],
            }
        })
        result = self.c.enable_locale("ar")
        assert result["locale"] == "ar"

    def test_enable_locale_already_exists(self):
        self.c._graphql = MagicMock(return_value={
            "shopLocaleEnable": {
                "shopLocale": {"locale": "ar", "published": True},
                "userErrors": [{"field": "locale", "message": "Locale already enabled"}],
            }
        })
        result = self.c.enable_locale("ar")
        assert result["locale"] == "ar"

    def test_enable_locale_error(self):
        self.c._graphql = MagicMock(return_value={
            "shopLocaleEnable": {
                "shopLocale": None,
                "userErrors": [{"field": "locale", "message": "Invalid locale"}],
            }
        })
        with pytest.raises(Exception, match="shopLocaleEnable"):
            self.c.enable_locale("zz")

    def test_get_locales(self):
        self.c._graphql = MagicMock(return_value={
            "shopLocales": [
                {"locale": "en", "primary": True, "published": True},
                {"locale": "ar", "primary": False, "published": True},
            ]
        })
        result = self.c.get_locales()
        assert len(result) == 2


# ---------------------------------------------------------------------------
# GraphQL: Inventory
# ---------------------------------------------------------------------------

class TestInventory:
    def setup_method(self):
        self.c = ShopifyClient("shop.myshopify.com", "tok")

    def test_get_inventory_item_id(self):
        self.c._graphql = MagicMock(return_value={
            "productVariant": {"inventoryItem": {"id": "gid://shopify/InventoryItem/1"}}
        })
        result = self.c.get_inventory_item_id(123)
        assert result == "gid://shopify/InventoryItem/1"

    def test_get_inventory_item_id_not_found(self):
        self.c._graphql = MagicMock(return_value={"productVariant": None})
        result = self.c.get_inventory_item_id(999)
        assert result is None

    def test_set_inventory_quantity(self):
        self.c._graphql = MagicMock(return_value={
            "inventorySetOnHandQuantities": {
                "inventoryAdjustmentGroup": {"reason": "correction"},
                "userErrors": [],
            }
        })
        result = self.c.set_inventory_quantity("gid://ii/1", "gid://loc/1", 50)
        assert result is not None

    def test_set_inventory_quantity_error(self):
        self.c._graphql = MagicMock(return_value={
            "inventorySetOnHandQuantities": {
                "inventoryAdjustmentGroup": None,
                "userErrors": [{"field": "qty", "message": "Invalid"}],
            }
        })
        with pytest.raises(Exception, match="inventorySetOnHandQuantities"):
            self.c.set_inventory_quantity("gid://ii/1", "gid://loc/1", -1)


# ---------------------------------------------------------------------------
# GraphQL: Menus
# ---------------------------------------------------------------------------

class TestMenus:
    def setup_method(self):
        self.c = ShopifyClient("shop.myshopify.com", "tok")

    def test_create_menu(self):
        self.c._graphql = MagicMock(return_value={
            "menuCreate": {
                "menu": {"id": "gid://1", "title": "Main", "handle": "main-menu"},
                "userErrors": [],
            }
        })
        items = [{"title": "Shop", "url": "/collections/all"}]
        result = self.c.create_menu("Main", "main-menu", items)
        assert result["id"] == "gid://1"

    def test_create_menu_already_exists(self):
        self.c._graphql = MagicMock(return_value={
            "menuCreate": {
                "menu": None,
                "userErrors": [{"field": "handle", "message": "Handle already taken"}],
            }
        })
        result = self.c.create_menu("Main", "main-menu", [])
        assert result is None

    def test_create_menu_error(self):
        self.c._graphql = MagicMock(return_value={
            "menuCreate": {
                "menu": None,
                "userErrors": [{"field": "title", "message": "Title is required"}],
            }
        })
        with pytest.raises(Exception, match="menuCreate"):
            self.c.create_menu("", "bad", [])


# ---------------------------------------------------------------------------
# GraphQL: SEO
# ---------------------------------------------------------------------------

class TestSEO:
    def setup_method(self):
        self.c = ShopifyClient("shop.myshopify.com", "tok")

    def test_update_product_seo(self):
        self.c.set_metafields = MagicMock(return_value=[])
        self.c.update_product_seo(123, "Title", "Description")
        self.c.set_metafields.assert_called_once()
        args = self.c.set_metafields.call_args[0][0]
        assert len(args) == 2
        assert args[0]["key"] == "title_tag"
        assert args[1]["key"] == "description_tag"

    def test_update_product_seo_title_only(self):
        self.c.set_metafields = MagicMock(return_value=[])
        self.c.update_product_seo(123, "Title", None)
        args = self.c.set_metafields.call_args[0][0]
        assert len(args) == 1
        assert args[0]["key"] == "title_tag"

    def test_update_product_seo_no_tags(self):
        self.c.set_metafields = MagicMock(return_value=[])
        result = self.c.update_product_seo(123, None, None)
        assert result == []
        self.c.set_metafields.assert_not_called()


# ---------------------------------------------------------------------------
# REST: Smart Collections
# ---------------------------------------------------------------------------

class TestSmartCollections:
    def setup_method(self):
        self.c = ShopifyClient("shop.myshopify.com", "tok")

    def test_create_smart_collection(self):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"smart_collection": {"id": 42, "title": "Sale"}}
        self.c._request = MagicMock(return_value=mock_resp)

        result = self.c.create_smart_collection({"title": "Sale", "rules": [{"column": "tag", "relation": "equals", "condition": "sale"}]})
        assert result["id"] == 42


# ---------------------------------------------------------------------------
# REST: Price Rules & Discount Codes
# ---------------------------------------------------------------------------

class TestPriceRules:
    def setup_method(self):
        self.c = ShopifyClient("shop.myshopify.com", "tok")

    def test_get_price_rules(self):
        self.c._paginate = MagicMock(return_value=[{"id": 1, "title": "Sale"}])
        result = self.c.get_price_rules()
        assert len(result) == 1
        self.c._paginate.assert_called_once_with("price_rules.json", "price_rules")

    def test_get_discount_codes(self):
        self.c._paginate = MagicMock(return_value=[{"id": 1, "code": "SAVE10"}])
        result = self.c.get_discount_codes(42)
        assert result[0]["code"] == "SAVE10"
        self.c._paginate.assert_called_once_with("price_rules/42/discount_codes.json", "discount_codes")

    def test_create_price_rule(self):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"price_rule": {"id": 99, "title": "Rule"}}
        self.c._request = MagicMock(return_value=mock_resp)
        result = self.c.create_price_rule({"title": "Rule"})
        assert result["id"] == 99

    def test_create_discount_code(self):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"discount_code": {"id": 1, "code": "SAVE10"}}
        self.c._request = MagicMock(return_value=mock_resp)
        result = self.c.create_discount_code(42, "SAVE10")
        assert result["code"] == "SAVE10"


# ---------------------------------------------------------------------------
# GraphQL: Publications / Publishing
# ---------------------------------------------------------------------------

class TestPublications:
    def setup_method(self):
        self.c = ShopifyClient("shop.myshopify.com", "tok")

    def test_get_publications(self):
        self.c._graphql = MagicMock(return_value={
            "publications": {
                "edges": [
                    {"node": {"id": "gid://shopify/Publication/1", "name": "Online Store"}},
                    {"node": {"id": "gid://shopify/Publication/2", "name": "POS"}},
                ]
            }
        })
        result = self.c.get_publications()
        assert len(result) == 2
        assert result[0]["name"] == "Online Store"

    def test_publish_resource(self):
        self.c._graphql = MagicMock(return_value={
            "publishablePublish": {
                "publishable": {"availablePublicationsCount": {"count": 2}},
                "userErrors": [],
            }
        })
        result = self.c.publish_resource("gid://shopify/Product/1", ["gid://shopify/Publication/1"])
        assert result is not None

    def test_publish_resource_error(self):
        self.c._graphql = MagicMock(return_value={
            "publishablePublish": {
                "publishable": None,
                "userErrors": [{"field": "id", "message": "Not found"}],
            }
        })
        with pytest.raises(Exception, match="publishablePublish"):
            self.c.publish_resource("gid://bad", [])
