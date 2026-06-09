import json
import os
import random
import time
from typing import Any

import requests

from tara_migrate.core.logging import get_logger

logger = get_logger(__name__)

# API version is pinned but overridable via env so a destination can be migrated
# against a newer Shopify release without a code change.
API_VERSION = os.environ.get("SHOPIFY_API_VERSION", "2024-10")

# How many times to retry a cost-throttled GraphQL call before giving up.
MAX_THROTTLE_RETRIES = 12
# GraphQL extension codes that mean "back off and retry" vs "permission problem".
_THROTTLE_CODES = {"THROTTLED"}
_AUTH_CODES = {"ACCESS_DENIED", "UNAUTHORIZED"}


# ---------------------------------------------------------------------------
# Typed exception hierarchy
# ---------------------------------------------------------------------------
# These all subclass Exception so existing `except Exception` / pytest.raises(Exception)
# callers keep working, while new code can catch the specific failure mode.

class ShopifyError(Exception):
    """Base class for all ShopifyClient errors."""


class GraphQLThrottled(ShopifyError):
    """Raised when a GraphQL call stays cost-throttled after MAX_THROTTLE_RETRIES."""


class GraphQLUserError(ShopifyError):
    """Raised for top-level GraphQL query/schema errors (not mutation userErrors)."""


class GraphQLAuthError(ShopifyError):
    """Raised when the access token lacks scope / is denied."""


def _error_codes(errors: list[dict[str, Any]] | None) -> set[str]:
    """Collect the `extensions.code` values from a GraphQL errors array."""
    codes = set()
    for err in errors or []:
        code = (err.get("extensions") or {}).get("code")
        if code:
            codes.add(code)
    return codes


def _is_throttled(errors: list[dict[str, Any]] | None) -> bool:
    return bool(_error_codes(errors) & _THROTTLE_CODES)


def _is_auth_error(errors: list[dict[str, Any]] | None) -> bool:
    if _error_codes(errors) & _AUTH_CODES:
        return True
    # Some access-denied errors arrive without an extensions.code.
    for err in errors or []:
        msg = (err.get("message") or "").lower()
        if "access denied" in msg or "not approved" in msg or "requires merchant approval" in msg:
            return True
    return False


def _throttle_wait_seconds(body: dict[str, Any]) -> float:
    """Compute backoff from Shopify's leaky-bucket cost in extensions.cost.

    Shopify returns the bucket state on the *same* HTTP-200 response that
    carries the THROTTLED error: extensions.cost.throttleStatus = {
    maximumAvailable, currentlyAvailable, restoreRate }. We wait long enough
    for the bucket to refill to cover the requested cost.
    """
    try:
        cost = body["extensions"]["cost"]
        status = cost["throttleStatus"]
        available = float(status["currentlyAvailable"])
        restore = float(status["restoreRate"])
        requested = float(cost.get("requestedQueryCost") or 0)
        deficit = max(requested - available, 0.0)
        if restore > 0 and deficit > 0:
            return max(1.0, deficit / restore)
    except (KeyError, TypeError, ValueError, ZeroDivisionError):
        pass
    return 2.0


class ShopifyClient:
    def __init__(self, shop_url: str, access_token: str) -> None:
        self.shop_url = shop_url.rstrip("/")
        if not self.shop_url.startswith("https://"):
            self.shop_url = f"https://{self.shop_url}"
        self.base_url = f"{self.shop_url}/admin/api/{API_VERSION}"
        self.graphql_url = f"{self.base_url}/graphql.json"
        self.session = requests.Session()
        self.session.headers.update({
            "X-Shopify-Access-Token": access_token,
            "Content-Type": "application/json",
        })

    # --- Low-level helpers ---

    @staticmethod
    def _sleep(seconds: float) -> None:
        """Sleep with +/-10% jitter to avoid thundering-herd retry alignment."""
        time.sleep(max(0.0, seconds) * random.uniform(0.9, 1.1))

    def _request_raw(self, method: str, url: str, **kwargs: Any) -> requests.Response:
        """Send request to any URL with rate-limit and connection error retry."""
        conn_attempts = 0
        while True:
            try:
                resp = self.session.request(method, url, **kwargs)
            except (ConnectionError, OSError) as e:
                conn_attempts += 1
                if conn_attempts < 4:
                    wait = 2 ** conn_attempts
                    logger.warning("  Connection error (attempt %d/4), retrying in %ds: %s",
                                   conn_attempts, wait, e)
                    self._sleep(wait)
                    continue
                raise
            conn_attempts = 0  # reset on successful connection
            if resp.status_code == 429:
                retry_after = float(resp.headers.get("Retry-After", 2))
                logger.warning("  Rate limited. Retrying after %ss...", retry_after)
                self._sleep(retry_after)
                continue
            if resp.status_code == 422:
                # Include Shopify's validation errors in the exception
                try:
                    body = resp.json()
                    errors = body.get("errors", body)
                    raise requests.HTTPError(
                        f"422 Validation Error: {errors}", response=resp
                    )
                except (ValueError, KeyError):
                    pass
            resp.raise_for_status()
            return resp

    def _request(self, method: str, endpoint: str, **kwargs: Any) -> requests.Response:
        url = f"{self.base_url}/{endpoint}"
        return self._request_raw(method, url, **kwargs)

    def _get_json(self, endpoint: str, params: dict[str, Any] | None = None) -> tuple[dict[str, Any], Any]:
        resp = self._request("GET", endpoint, params=params)
        return resp.json(), resp.headers

    def _paginate(self, endpoint: str, resource_key: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        params = params or {}
        params.setdefault("limit", 250)
        all_items = []
        url = f"{self.base_url}/{endpoint}"
        while url:
            resp = self._request_raw("GET", url, params=params)
            data = resp.json()
            items = data.get(resource_key, [])
            all_items.extend(items)
            params = {}
            url = None
            link_header = resp.headers.get("Link", "")
            for part in link_header.split(","):
                if 'rel="next"' in part:
                    url = part.split("<")[1].split(">")[0]
                    break
        return all_items

    def _graphql(self, query: str, variables: dict[str, Any] | None = None) -> dict[str, Any]:
        """Execute a GraphQL query/mutation.

        Handles three distinct throttling/error modes:
          * HTTP 429 (rare for GraphQL) — honour Retry-After.
          * Cost-based THROTTLED — returned on HTTP 200 with the error code in
            ``errors[].extensions.code`` and the leaky-bucket state in
            ``extensions.cost``. Back off and retry up to MAX_THROTTLE_RETRIES.
          * Auth/permission errors — raised immediately as GraphQLAuthError.
        Other top-level errors raise GraphQLUserError (message preserved as
        "GraphQL errors: ..." for backwards compatibility).
        """
        payload: dict[str, Any] = {"query": query}
        if variables:
            payload["variables"] = variables

        conn_attempts = 0
        throttle_attempts = 0
        while True:
            try:
                resp = self.session.post(self.graphql_url, json=payload)
            except (ConnectionError, OSError) as e:
                conn_attempts += 1
                if conn_attempts < 4:
                    wait = 2 ** conn_attempts
                    logger.warning("  Connection error (attempt %d/4), retrying in %ds: %s",
                                   conn_attempts, wait, e)
                    self._sleep(wait)
                    continue
                raise
            conn_attempts = 0

            if resp.status_code == 429:
                retry_after = float(resp.headers.get("Retry-After", 2))
                logger.warning("  Rate limited (GraphQL). Retrying after %ss...", retry_after)
                self._sleep(retry_after)
                continue

            resp.raise_for_status()
            body = resp.json()
            errors = body.get("errors")
            if errors:
                if _is_throttled(errors):
                    throttle_attempts += 1
                    if throttle_attempts > MAX_THROTTLE_RETRIES:
                        raise GraphQLThrottled(
                            f"GraphQL still throttled after {MAX_THROTTLE_RETRIES} retries: {errors}"
                        )
                    wait = _throttle_wait_seconds(body)
                    logger.warning("  GraphQL THROTTLED (retry %d/%d), backing off %.1fs...",
                                   throttle_attempts, MAX_THROTTLE_RETRIES, wait)
                    self._sleep(wait)
                    continue
                if _is_auth_error(errors):
                    raise GraphQLAuthError(f"GraphQL access denied: {errors}")
                raise GraphQLUserError(f"GraphQL errors: {errors}")
            return body.get("data", {})

    # --- REST: Read methods ---

    def get_shop(self) -> dict[str, Any]:
        data, _ = self._get_json("shop.json")
        return data.get("shop", {})

    def get_access_scopes(self) -> list[str]:
        """Return the access-scope handles granted to this token.

        Uses the free `currentAppInstallation.accessScopes` query so a preflight
        can verify write permissions before any mutation runs.
        """
        query = "{ currentAppInstallation { accessScopes { handle } } }"
        data = self._graphql(query)
        installation = data.get("currentAppInstallation") or {}
        return [s["handle"] for s in installation.get("accessScopes", []) if s.get("handle")]

    def verify_token_scopes(self, required: list[str]) -> list[str]:
        """Return the subset of *required* scopes the token is missing (empty == OK)."""
        granted = set(self.get_access_scopes())
        return [scope for scope in required if scope not in granted]

    def get_products(self) -> list[dict[str, Any]]:
        return self._paginate("products.json", "products")

    def get_collections(self) -> list[dict[str, Any]]:
        custom = self._paginate("custom_collections.json", "custom_collections")
        smart = self._paginate("smart_collections.json", "smart_collections")
        return custom + smart

    def get_pages(self) -> list[dict[str, Any]]:
        return self._paginate("pages.json", "pages")

    def get_blogs(self) -> list[dict[str, Any]]:
        return self._paginate("blogs.json", "blogs")

    def get_articles(self, blog_id: int | str) -> list[dict[str, Any]]:
        return self._paginate(f"blogs/{blog_id}/articles.json", "articles")

    def get_metafields(self, resource: str, resource_id: int | str) -> list[dict[str, Any]]:
        return self._paginate(f"{resource}/{resource_id}/metafields.json", "metafields")

    # --- REST: Delete methods ---

    def delete_product(self, product_id: int | str) -> None:
        self._request("DELETE", f"products/{product_id}.json")

    def delete_custom_collection(self, collection_id: int | str) -> None:
        self._request("DELETE", f"custom_collections/{collection_id}.json")

    def delete_smart_collection(self, collection_id: int | str) -> None:
        self._request("DELETE", f"smart_collections/{collection_id}.json")

    def delete_page(self, page_id: int | str) -> None:
        self._request("DELETE", f"pages/{page_id}.json")

    def delete_blog(self, blog_id: int | str) -> None:
        self._request("DELETE", f"blogs/{blog_id}.json")

    def delete_article(self, blog_id: int | str, article_id: int | str) -> None:
        self._request("DELETE", f"blogs/{blog_id}/articles/{article_id}.json")

    def delete_price_rule(self, price_rule_id: int | str) -> None:
        self._request("DELETE", f"price_rules/{price_rule_id}.json")

    def delete_metaobject(self, metaobject_id: str) -> str:
        query = """
        mutation metaobjectDelete($id: ID!) {
          metaobjectDelete(id: $id) {
            deletedId
            userErrors { field message }
          }
        }
        """
        data = self._graphql(query, {"id": metaobject_id})
        result = data["metaobjectDelete"]
        if result["userErrors"]:
            raise Exception(f"metaobjectDelete errors: {result['userErrors']}")
        return result["deletedId"]

    def delete_metaobject_definition(self, definition_id: str) -> str:
        query = """
        mutation metaobjectDefinitionDelete($id: ID!) {
          metaobjectDefinitionDelete(id: $id) {
            deletedId
            userErrors { field message }
          }
        }
        """
        data = self._graphql(query, {"id": definition_id})
        result = data["metaobjectDefinitionDelete"]
        if result["userErrors"]:
            raise Exception(f"metaobjectDefinitionDelete errors: {result['userErrors']}")
        return result["deletedId"]

    def delete_file(self, file_id: str) -> list[str]:
        query = """
        mutation fileDelete($input: [ID!]!) {
          fileDelete(fileIds: $input) {
            deletedFileIds
            userErrors { field message }
          }
        }
        """
        data = self._graphql(query, {"input": [file_id]})
        result = data["fileDelete"]
        if result["userErrors"]:
            raise Exception(f"fileDelete errors: {result['userErrors']}")
        return result["deletedFileIds"]

    def get_files(self, first: int = 250) -> list[dict[str, Any]]:
        """Get all files (paginated)."""
        all_files = []
        cursor = None
        while True:
            after_clause = f', after: "{cursor}"' if cursor else ""
            query = f"""
            {{
              files(first: {first}{after_clause}) {{
                edges {{
                  cursor
                  node {{
                    ... on MediaImage {{
                      id
                      alt
                      image {{ url }}
                    }}
                    ... on GenericFile {{
                      id
                      alt
                      url
                    }}
                  }}
                }}
                pageInfo {{ hasNextPage }}
              }}
            }}
            """
            data = self._graphql(query)
            edges = data["files"]["edges"]
            for edge in edges:
                all_files.append(edge["node"])
                cursor = edge["cursor"]
            if not data["files"]["pageInfo"]["hasNextPage"]:
                break
        return all_files

    # --- REST: Write methods ---

    def create_product(self, product_data: dict[str, Any]) -> dict[str, Any]:
        resp = self._request("POST", "products.json", json={"product": product_data})
        return resp.json().get("product", {})

    def update_product(self, product_id: int | str, product_data: dict[str, Any]) -> dict[str, Any]:
        resp = self._request("PUT", f"products/{product_id}.json", json={"product": product_data})
        return resp.json().get("product", {})

    def create_custom_collection(self, collection_data: dict[str, Any]) -> dict[str, Any]:
        resp = self._request("POST", "custom_collections.json", json={"custom_collection": collection_data})
        return resp.json().get("custom_collection", {})

    def create_page(self, page_data: dict[str, Any]) -> dict[str, Any]:
        resp = self._request("POST", "pages.json", json={"page": page_data})
        return resp.json().get("page", {})

    def create_blog(self, blog_data: dict[str, Any]) -> dict[str, Any]:
        resp = self._request("POST", "blogs.json", json={"blog": blog_data})
        return resp.json().get("blog", {})

    def create_article(self, blog_id: int | str, article_data: dict[str, Any]) -> dict[str, Any]:
        resp = self._request("POST", f"blogs/{blog_id}/articles.json", json={"article": article_data})
        return resp.json().get("article", {})

    def create_metafield(self, resource: str, resource_id: int | str, metafield_data: dict[str, Any]) -> dict[str, Any]:
        resp = self._request(
            "POST",
            f"{resource}/{resource_id}/metafields.json",
            json={"metafield": metafield_data},
        )
        return resp.json().get("metafield", {})

    # --- REST: Lookup by handle ---

    def get_products_by_handle(self, handle: str) -> list[dict[str, Any]]:
        data, _ = self._get_json("products.json", params={"handle": handle})
        return data.get("products", [])

    def get_pages_by_handle(self, handle: str) -> list[dict[str, Any]]:
        data, _ = self._get_json("pages.json", params={"handle": handle})
        return data.get("pages", [])

    def get_collections_by_handle(self, handle: str) -> list[dict[str, Any]]:
        """Search both custom and smart collections by handle."""
        data, _ = self._get_json("custom_collections.json", params={"handle": handle})
        results = data.get("custom_collections", [])
        if not results:
            data, _ = self._get_json("smart_collections.json", params={"handle": handle})
            results = data.get("smart_collections", [])
        return results

    def get_blogs_by_handle(self, handle: str) -> list[dict[str, Any]]:
        data, _ = self._get_json("blogs.json", params={"handle": handle})
        return data.get("blogs", [])

    # --- GraphQL: Metaobjects ---

    def get_metaobject_definitions(self) -> list[dict[str, Any]]:
        """Get all metaobject type definitions."""
        query = """
        {
          metaobjectDefinitions(first: 250) {
            edges {
              node {
                id
                type
                name
                displayNameKey
                access {
                  admin
                  storefront
                }
                capabilities {
                  publishable {
                    enabled
                  }
                  renderable {
                    enabled
                    data {
                      metaTitleKey
                      metaDescriptionKey
                    }
                  }
                  translatable {
                    enabled
                  }
                  onlineStore {
                    enabled
                    data {
                      urlHandle
                    }
                  }
                }
                fieldDefinitions {
                  key
                  name
                  type { name }
                  validations { name value }
                }
              }
            }
          }
        }
        """
        data = self._graphql(query)
        return [edge["node"] for edge in data["metaobjectDefinitions"]["edges"]]

    def get_metaobjects(self, metaobject_type: str) -> list[dict[str, Any]]:
        """Get all metaobjects of a given type (paginated)."""
        all_objects = []
        cursor = None
        while True:
            after_clause = f', after: "{cursor}"' if cursor else ""
            query = f"""
            query getMetaobjects($type: String!) {{
              metaobjects(type: $type, first: 250{after_clause}) {{
                edges {{
                  cursor
                  node {{
                    id
                    handle
                    type
                    fields {{
                      key
                      value
                      type
                    }}
                  }}
                }}
                pageInfo {{ hasNextPage }}
              }}
            }}
            """
            data = self._graphql(query, {"type": metaobject_type})
            edges = data["metaobjects"]["edges"]
            for edge in edges:
                all_objects.append(edge["node"])
                cursor = edge["cursor"]
            if not data["metaobjects"]["pageInfo"]["hasNextPage"]:
                break
        return all_objects

    def create_metaobject_definition(self, definition_data: dict[str, Any]) -> dict[str, Any] | None:
        """Create a metaobject definition (type) in the destination store."""
        query = """
        mutation CreateMetaobjectDefinition($definition: MetaobjectDefinitionCreateInput!) {
          metaobjectDefinitionCreate(definition: $definition) {
            metaobjectDefinition {
              id
              type
            }
            userErrors {
              field
              message
            }
          }
        }
        """
        data = self._graphql(query, {"definition": definition_data})
        result = data["metaobjectDefinitionCreate"]
        if result["userErrors"]:
            errors = result["userErrors"]
            # If the type already exists, that's OK
            if any("already exists" in e["message"].lower() for e in errors):
                logger.info("    Definition already exists, continuing...")
                return None
            raise Exception(f"MetaobjectDefinitionCreate errors: {errors}")
        return result["metaobjectDefinition"]

    def enable_standard_metaobject_definition(self, metaobject_type: str) -> dict[str, Any]:
        """Enable a Shopify standard metaobject definition by type."""
        query = """
        mutation EnableStandardMetaobjectDefinition($type: String!) {
          standardMetaobjectDefinitionEnable(type: $type) {
            metaobjectDefinition {
              id
              type
              name
              fieldDefinitions {
                key
                name
                type { name }
                validations { name value }
              }
            }
            userErrors {
              field
              message
            }
          }
        }
        """
        data = self._graphql(query, {"type": metaobject_type})
        result = data["standardMetaobjectDefinitionEnable"]
        if result["userErrors"]:
            raise Exception(f"standardMetaobjectDefinitionEnable errors: {result['userErrors']}")
        return result["metaobjectDefinition"]

    def update_metaobject_definition(self, definition_id: str, update_data: dict[str, Any]) -> dict[str, Any]:
        """Update an existing metaobject definition (e.g. set displayNameKey)."""
        query = """
        mutation UpdateMetaobjectDefinition($id: ID!, $definition: MetaobjectDefinitionUpdateInput!) {
          metaobjectDefinitionUpdate(id: $id, definition: $definition) {
            metaobjectDefinition {
              id
              type
            }
            userErrors {
              field
              message
            }
          }
        }
        """
        data = self._graphql(query, {"id": definition_id, "definition": update_data})
        result = data["metaobjectDefinitionUpdate"]
        if result["userErrors"]:
            raise Exception(f"MetaobjectDefinitionUpdate errors: {result['userErrors']}")
        return result["metaobjectDefinition"]

    def create_metaobject(self, metaobject_data: dict[str, Any]) -> dict[str, Any] | None:
        """Create a metaobject instance."""
        query = """
        mutation CreateMetaobject($metaobject: MetaobjectCreateInput!) {
          metaobjectCreate(metaobject: $metaobject) {
            metaobject {
              id
              handle
            }
            userErrors {
              field
              message
            }
          }
        }
        """
        data = self._graphql(query, {"metaobject": metaobject_data})
        result = data["metaobjectCreate"]
        if result["userErrors"]:
            errors = result["userErrors"]
            if any("already exists" in e["message"].lower() for e in errors):
                return None
            raise Exception(f"MetaobjectCreate errors: {errors}")
        return result["metaobject"]

    def update_metaobject(self, metaobject_id: str, fields: list[dict[str, Any]],
                         handle: str | None = None) -> dict[str, Any]:
        """Update a metaobject's fields and/or handle."""
        query = """
        mutation UpdateMetaobject($id: ID!, $metaobject: MetaobjectUpdateInput!) {
          metaobjectUpdate(id: $id, metaobject: $metaobject) {
            metaobject {
              id
              handle
            }
            userErrors {
              field
              message
            }
          }
        }
        """
        metaobject_input: dict[str, Any] = {}
        if fields:
            metaobject_input["fields"] = fields
        if handle is not None:
            metaobject_input["handle"] = handle
        data = self._graphql(query, {"id": metaobject_id, "metaobject": metaobject_input})
        result = data["metaobjectUpdate"]
        if result["userErrors"]:
            raise Exception(f"MetaobjectUpdate errors: {result['userErrors']}")
        return result["metaobject"]

    def get_metaobjects_by_handle(self, metaobject_type: str, handle: str) -> dict[str, Any] | None:
        """Look up a metaobject by type and handle."""
        query = f"""
        {{
          metaobjectByHandle(handle: {{type: "{metaobject_type}", handle: "{handle}"}}) {{
            id
            handle
            type
            fields {{
              key
              value
              type
            }}
          }}
        }}
        """
        data = self._graphql(query)
        return data.get("metaobjectByHandle")

    # --- GraphQL: Metafield Definitions ---

    def get_metafield_definitions(self, owner_type: str) -> list[dict[str, Any]]:
        """Get all metafield definitions for a given owner type.

        Args:
            owner_type: "PRODUCT", "ARTICLE", "COLLECTION", etc.
        """
        all_defs = []
        cursor = None
        while True:
            after_clause = f', after: "{cursor}"' if cursor else ""
            query = f"""
            {{
              metafieldDefinitions(ownerType: {owner_type}, first: 250{after_clause}) {{
                edges {{
                  cursor
                    node {{
                      id
                      namespace
                      key
                      name
                      type {{ name }}
                      ownerType
                      capabilities {{
                        smartCollectionCondition {{
                          enabled
                        }}
                        adminFilterable {{
                          enabled
                        }}
                      }}
                      validations {{
                        name
                        value
                      }}
                    }}
                  }}
                  pageInfo {{ hasNextPage }}
                }}
              }}
            """
            data = self._graphql(query)
            edges = data["metafieldDefinitions"]["edges"]
            for edge in edges:
                all_defs.append(edge["node"])
                cursor = edge["cursor"]
            if not data["metafieldDefinitions"]["pageInfo"]["hasNextPage"]:
                break
        return all_defs

    def create_metafield_definition(self, definition: dict[str, Any]) -> dict[str, Any] | None:
        """Create a metafield definition via GraphQL.

        Args:
            definition: dict with name, namespace, key, type, ownerType,
                       and optionally validations
        """
        query = """
        mutation CreateMetafieldDefinition($definition: MetafieldDefinitionInput!) {
          metafieldDefinitionCreate(definition: $definition) {
            createdDefinition {
              id
              namespace
              key
              name
            }
            userErrors {
              field
              message
            }
          }
        }
        """
        data = self._graphql(query, {"definition": definition})
        result = data["metafieldDefinitionCreate"]
        if result["userErrors"]:
            errors = result["userErrors"]
            if any("already exists" in e.get("message", "").lower() for e in errors):
                return None
            raise Exception(f"MetafieldDefinitionCreate errors: {errors}")
        return result["createdDefinition"]

    def update_metafield_definition(self, definition: dict[str, Any]) -> dict[str, Any]:
        """Update a metafield definition via GraphQL.

        Args:
            definition: dict with id and the fields to update, such as
                        capabilities, validations, or name
        """
        query = """
        mutation UpdateMetafieldDefinition($definition: MetafieldDefinitionUpdateInput!) {
          metafieldDefinitionUpdate(definition: $definition) {
            updatedDefinition {
              id
              namespace
              key
              name
            }
            userErrors {
              field
              message
            }
          }
        }
        """
        data = self._graphql(query, {"definition": definition})
        result = data["metafieldDefinitionUpdate"]
        if result["userErrors"]:
            raise Exception(f"MetafieldDefinitionUpdate errors: {result['userErrors']}")
        return result["updatedDefinition"]

    def set_metafields(self, metafields: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Set metafields on resources via GraphQL metafieldsSet.

        Args:
            metafields: list of dicts with ownerId, namespace, key, value, type
        """
        query = """
        mutation SetMetafields($metafields: [MetafieldsSetInput!]!) {
          metafieldsSet(metafields: $metafields) {
            metafields {
              id
              namespace
              key
            }
            userErrors {
              field
              message
            }
          }
        }
        """
        data = self._graphql(query, {"metafields": metafields})
        result = data["metafieldsSet"]
        if result["userErrors"]:
            raise Exception(f"MetafieldsSet errors: {result['userErrors']}")
        return result["metafields"]

    # --- GraphQL: File / Asset uploads ---

    def staged_uploads_create(self, staged_inputs: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Create staged upload targets for files.

        Args:
            staged_inputs: list of dicts with filename, mimeType, resource,
                          httpMethod (POST or PUT), fileSize
        Returns:
            list of staged upload targets with url, parameters, resourceUrl
        """
        query = """
        mutation stagedUploadsCreate($input: [StagedUploadInput!]!) {
          stagedUploadsCreate(input: $input) {
            stagedTargets {
              url
              resourceUrl
              parameters {
                name
                value
              }
            }
            userErrors {
              field
              message
            }
          }
        }
        """
        data = self._graphql(query, {"input": staged_inputs})
        result = data["stagedUploadsCreate"]
        if result["userErrors"]:
            raise Exception(f"StagedUploadsCreate errors: {result['userErrors']}")
        return result["stagedTargets"]

    def file_create(self, files_input: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Create files in Shopify from staged uploads.

        Args:
            files_input: list of dicts with alt, contentType, originalSource
        Returns:
            list of created file dicts with id, alt, fileStatus
        """
        query = """
        mutation fileCreate($files: [FileCreateInput!]!) {
          fileCreate(files: $files) {
            files {
              id
              alt
              ... on MediaImage {
                id
                image {
                  url
                }
              }
              ... on GenericFile {
                id
                url
              }
            }
            userErrors {
              field
              message
            }
          }
        }
        """
        data = self._graphql(query, {"files": files_input})
        result = data["fileCreate"]
        if result["userErrors"]:
            raise Exception(f"FileCreate errors: {result['userErrors']}")
        return result["files"]

    def get_file_by_id(self, file_id: str) -> dict[str, Any] | None:
        """Get a file's details and status by GID."""
        query = """
        query getFile($id: ID!) {
          node(id: $id) {
            ... on MediaImage {
              id
              alt
              fileStatus
              image {
                url
              }
            }
            ... on GenericFile {
              id
              alt
              fileStatus
              url
            }
          }
        }
        """
        data = self._graphql(query, {"id": file_id})
        return data.get("node")

    def upload_file_from_url(self, source_url: str, filename: str | None = None, alt: str = "", optimize: bool = False) -> str | None:
        """Upload a file to Shopify from a public URL.

        Downloads from source_url, optionally optimizes to WebP, stages the
        upload, and creates the file. Returns the Shopify file GID.

        Args:
            source_url: Public URL to download from.
            filename: Override filename (optional).
            alt: Alt text for the file.
            optimize: If True, convert images to optimized WebP before uploading.
        """
        import os
        import urllib.parse

        if not filename:
            parsed = urllib.parse.urlparse(source_url)
            filename = os.path.basename(parsed.path) or "file"
            filename = filename.split("?")[0]

        # Shopify's CDN content-negotiates image URLs. When a source path ends
        # with .webp/.avif, a generic request may receive a JPEG derivative
        # instead of the original format, which changes the uploaded filename
        # registration and breaks `shopify://shop_images/...` references.
        ext = os.path.splitext(filename)[1].lower()
        headers: dict[str, str] = {}
        if ext == ".webp":
            headers["Accept"] = "image/webp,image/apng,image/*,*/*;q=0.8"
        elif ext == ".avif":
            headers["Accept"] = "image/avif,image/webp,image/*,*/*;q=0.8"

        # Download the file
        resp = self.session.get(source_url, stream=True, headers=headers)
        resp.raise_for_status()
        content = resp.content

        # Optionally optimize images to WebP
        if optimize:
            try:
                from optimize_images import optimize_image
                content, filename = optimize_image(content, filename)
            except ImportError:
                pass  # Pillow not installed, skip optimization

        return self.upload_file_bytes(content, filename, alt=alt)

    def upload_file_bytes(self, content: bytes, filename: str, alt: str = "") -> str | None:
        """Upload raw file bytes to Shopify via staged upload.

        Args:
            content: Raw file bytes.
            filename: Filename including extension.
            alt: Alt text for the file.

        Returns:
            Shopify file GID string, or None on failure.
        """
        import io
        import mimetypes

        # Windows often lacks WebP/AVIF mappings by default, which causes
        # image uploads to fall back to application/octet-stream and become
        # GenericFile records instead of MediaImage.
        mimetypes.add_type("image/webp", ".webp")
        mimetypes.add_type("image/avif", ".avif")

        mime_type, _ = mimetypes.guess_type(filename)
        if not mime_type:
            mime_type = "application/octet-stream"

        if mime_type.startswith("image/"):
            resource = "IMAGE"
        else:
            resource = "FILE"

        file_size = str(len(content))

        staged_input = [{
            "filename": filename,
            "mimeType": mime_type,
            "resource": resource,
            "httpMethod": "POST",
            "fileSize": file_size,
        }]
        targets = self.staged_uploads_create(staged_input)
        target = targets[0]

        form_data = {}
        for param in target["parameters"]:
            form_data[param["name"]] = param["value"]

        files_payload = {"file": (filename, io.BytesIO(content), mime_type)}

        upload_resp = requests.post(target["url"], data=form_data, files=files_payload)
        upload_resp.raise_for_status()

        file_input = [{
            "alt": alt,
            "contentType": resource,
            "originalSource": target["resourceUrl"],
        }]
        created_files = self.file_create(file_input)
        if created_files:
            return created_files[0]["id"]
        return None

    # --- GraphQL: Translations API ---

    def register_translations(self, resource_id: str, locale: str, translations: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Register translations for a resource using the Shopify Translations API.

        Args:
            resource_id: The GID of the resource (e.g., "gid://shopify/Product/123")
            locale: Target locale code (e.g., "ar")
            translations: List of dicts with keys: key, value, translatableContentDigest
        """
        query = """
        mutation translationsRegister($resourceId: ID!, $translations: [TranslationInput!]!) {
          translationsRegister(resourceId: $resourceId, translations: $translations) {
            translations {
              key
              locale
              value
            }
            userErrors {
              field
              message
            }
          }
        }
        """
        data = self._graphql(query, {
            "resourceId": resource_id,
            "translations": translations,
        })
        result = data["translationsRegister"]
        if result["userErrors"]:
            raise Exception(f"TranslationsRegister errors: {result['userErrors']}")
        return result["translations"]

    def get_translatable_resources(self, resource_type: str, first: int = 50) -> list[dict[str, Any]]:
        """Get translatable resources and their content digests.

        Args:
            resource_type: e.g., "PRODUCT", "COLLECTION", "ONLINE_STORE_PAGE",
                          "ONLINE_STORE_ARTICLE", "ONLINE_STORE_BLOG", "METAOBJECT"
        """
        all_resources = []
        cursor = None
        while True:
            after_clause = f', after: "{cursor}"' if cursor else ""
            query = f"""
            {{
              translatableResources(resourceType: {resource_type}, first: {first}{after_clause}) {{
                edges {{
                  cursor
                  node {{
                    resourceId
                    translatableContent {{
                      key
                      value
                      digest
                      locale
                    }}
                  }}
                }}
                pageInfo {{ hasNextPage }}
              }}
            }}
            """
            data = self._graphql(query)
            edges = data["translatableResources"]["edges"]
            for edge in edges:
                all_resources.append(edge["node"])
                cursor = edge["cursor"]
            if not data["translatableResources"]["pageInfo"]["hasNextPage"]:
                break
        return all_resources

    def get_translatable_resource(self, resource_gid: str) -> dict[str, Any] | None:
        """Get translatable content for a single resource by GID."""
        query = """
        query GetTranslatable($resourceId: ID!) {
          translatableResource(resourceId: $resourceId) {
            resourceId
            translatableContent {
              key
              value
              digest
              locale
            }
          }
        }
        """
        data = self._graphql(query, {"resourceId": resource_gid})
        return data.get("translatableResource")

    def get_translatable_resource_with_translations(
        self, resource_gid: str, locale: str
    ) -> dict[str, Any] | None:
        """Get translatable content and current translations for a single resource."""
        query = """
        query GetTranslatableWithTranslations($resourceId: ID!, $locale: String!) {
          translatableResource(resourceId: $resourceId) {
            resourceId
            translatableContent {
              key
              value
              digest
              locale
            }
            translations(locale: $locale) {
              key
              value
              locale
              outdated
            }
          }
        }
        """
        data = self._graphql(query, {"resourceId": resource_gid, "locale": locale})
        return data.get("translatableResource")

    # --- REST: Collects (product-collection links) ---

    def get_collects(self, collection_id: int | str | None = None) -> list[dict[str, Any]]:
        """Get all product-collection associations."""
        params = {}
        if collection_id:
            params["collection_id"] = collection_id
        return self._paginate("collects.json", "collects", params=params)

    def get_collection_product_ids(self, collection_id: int | str) -> list[int]:
        """Get product IDs in a collection (works with API 2024-10+)."""
        products = self._paginate(f"collections/{collection_id}/products.json", "products",
                                  params={"fields": "id"})
        return [p["id"] for p in products]

    def create_collect(self, product_id: int | str, collection_id: int | str) -> dict[str, Any]:
        """Add a product to a collection. Falls back to GraphQL if REST fails."""
        try:
            resp = self._request("POST", "collects.json", json={
                "collect": {"product_id": product_id, "collection_id": collection_id}
            })
            return resp.json().get("collect", {})
        except Exception as e:
            if "403" in str(e):
                # Fallback: use GraphQL collectionAddProducts
                return self.collection_add_products(
                    collection_id, [product_id])
            raise

    def collection_add_products(self, collection_id: int | str, product_ids: list[int | str]) -> dict[str, Any]:
        """Add products to a collection via GraphQL (works without collects scope)."""
        query = """
        mutation collectionAddProducts($id: ID!, $productIds: [ID!]!) {
          collectionAddProducts(id: $id, productIds: $productIds) {
            collection { id }
            userErrors { field message }
          }
        }
        """
        coll_gid = f"gid://shopify/Collection/{collection_id}" if not str(collection_id).startswith("gid://") else collection_id
        prod_gids = [
            f"gid://shopify/Product/{pid}" if not str(pid).startswith("gid://") else pid
            for pid in product_ids
        ]
        data = self._graphql(query, {"id": coll_gid, "productIds": prod_gids})
        result = data["collectionAddProducts"]
        if result["userErrors"]:
            errors = result["userErrors"]
            if any("already" in e.get("message", "").lower() for e in errors):
                return {}
            raise Exception(f"collectionAddProducts errors: {errors}")
        return result.get("collection", {})

    # --- REST: Redirects ---

    def get_redirects(self) -> list[dict[str, Any]]:
        """Get all URL redirects."""
        return self._paginate("redirects.json", "redirects")

    def create_redirect(self, path: str, target: str) -> dict[str, Any]:
        """Create a URL redirect."""
        resp = self._request("POST", "redirects.json", json={
            "redirect": {"path": path, "target": target}
        })
        return resp.json().get("redirect", {})

    def update_redirect(self, redirect_id: int | str, path: str | None = None, target: str | None = None) -> dict[str, Any]:
        """Update an existing URL redirect."""
        update = {}
        if path is not None:
            update["path"] = path
        if target is not None:
            update["target"] = target
        resp = self._request("PUT", f"redirects/{redirect_id}.json", json={
            "redirect": update
        })
        return resp.json().get("redirect", {})

    def delete_redirect(self, redirect_id: int | str) -> None:
        """Delete a URL redirect."""
        self._request("DELETE", f"redirects/{redirect_id}.json")

    # --- REST: Customers ---

    def get_customers(self) -> list[dict[str, Any]]:
        """Get all customers."""
        return self._paginate("customers.json", "customers")

    def search_customers(self, query: str) -> list[dict[str, Any]]:
        """Search customers by query (e.g. email:foo@bar.com)."""
        data, _ = self._get_json(f"customers/search.json?query={query}")
        return data.get("customers", [])

    def create_customer(self, customer_data: dict[str, Any]) -> dict[str, Any]:
        """Create a customer. Set send_email_invite=False to skip invite."""
        resp = self._request("POST", "customers.json", json={"customer": customer_data})
        return resp.json().get("customer", {})

    # --- REST: Inventory ---

    def get_locations(self) -> list[dict[str, Any]]:
        """Get all inventory locations."""
        data, _ = self._get_json("locations.json")
        return data.get("locations", [])

    # --- REST: Policies ---

    def get_policies(self) -> list[dict[str, Any]]:
        """Get shop policies."""
        data, _ = self._get_json("policies.json")
        return data.get("policies", [])

    # --- GraphQL: Locale management ---

    def enable_locale(self, locale_code: str) -> dict[str, Any] | None:
        """Enable a locale for the store (e.g., 'ar' for Arabic)."""
        query = """
        mutation shopLocaleEnable($locale: String!) {
          shopLocaleEnable(locale: $locale) {
            shopLocale {
              locale
              published
            }
            userErrors {
              field
              message
            }
          }
        }
        """
        data = self._graphql(query, {"locale": locale_code})
        result = data["shopLocaleEnable"]
        if result["userErrors"]:
            errors = result["userErrors"]
            if any("already" in e.get("message", "").lower() for e in errors):
                return result.get("shopLocale")
            raise Exception(f"shopLocaleEnable errors: {errors}")
        return result["shopLocale"]

    def update_locale(self, locale_code: str, shop_locale: dict[str, Any]) -> dict[str, Any]:
        """Update an enabled locale, for example to publish it."""
        query = """
        mutation shopLocaleUpdate($locale: String!, $shopLocale: ShopLocaleInput!) {
          shopLocaleUpdate(locale: $locale, shopLocale: $shopLocale) {
            shopLocale {
              locale
              published
              primary
              name
            }
            userErrors {
              field
              message
            }
          }
        }
        """
        data = self._graphql(query, {"locale": locale_code, "shopLocale": shop_locale})
        result = data["shopLocaleUpdate"]
        if result["userErrors"]:
            raise Exception(f"shopLocaleUpdate errors: {result['userErrors']}")
        return result["shopLocale"]

    def get_locales(self) -> list[dict[str, Any]]:
        """Get all enabled locales for the shop."""
        query = """
        {
          shopLocales {
            locale
            primary
            published
          }
        }
        """
        data = self._graphql(query)
        return data.get("shopLocales", [])

    # --- GraphQL: Inventory quantities ---

    def get_inventory_item_id(self, variant_id: int | str) -> str | None:
        """Get inventory item ID for a variant via GraphQL."""
        query = """
        query getVariant($id: ID!) {
          productVariant(id: $id) {
            inventoryItem {
              id
            }
          }
        }
        """
        data = self._graphql(query, {"id": f"gid://shopify/ProductVariant/{variant_id}"})
        variant = data.get("productVariant")
        if variant and variant.get("inventoryItem"):
            return variant["inventoryItem"]["id"]
        return None

    def set_inventory_quantity(self, inventory_item_id: str, location_id: str, quantity: int) -> dict[str, Any]:
        """Set exact inventory quantity for an item at a location."""
        query = """
        mutation inventorySetOnHandQuantities($input: InventorySetOnHandQuantitiesInput!) {
          inventorySetOnHandQuantities(input: $input) {
            inventoryAdjustmentGroup {
              reason
            }
            userErrors {
              field
              message
            }
          }
        }
        """
        data = self._graphql(query, {
            "input": {
                "reason": "correction",
                "setQuantities": [{
                    "inventoryItemId": inventory_item_id,
                    "locationId": location_id,
                    "quantity": quantity,
                }],
            }
        })
        result = data["inventorySetOnHandQuantities"]
        if result["userErrors"]:
            raise Exception(f"inventorySetOnHandQuantities errors: {result['userErrors']}")
        return result

    # --- GraphQL: Navigation menus ---

    @staticmethod
    def _infer_menu_item_type(item: dict[str, Any]) -> str:
        """Infer the MenuItemType from resourceId or url."""
        rid = item.get("resourceId", "")
        url = item.get("url", "")

        if rid:
            if "/Collection/" in rid:
                return "COLLECTION"
            elif "/Page/" in rid or "/OnlineStorePage/" in rid:
                return "PAGE"
            elif "/Article/" in rid:
                return "ARTICLE"
            elif "/Blog/" in rid:
                return "BLOG"
            elif "/ShopPolicy/" in rid:
                return "SHOP_POLICY"
            elif "/Product/" in rid:
                return "CATALOG"
        if url:
            if url == "/" or url == "":
                return "FRONTPAGE"
            elif url.startswith("/search"):
                return "SEARCH"
            elif url.startswith("/collections"):
                return "HTTP"
            elif url.startswith("/pages"):
                return "HTTP"
            return "HTTP"
        return "HTTP"

    def _prepare_menu_items(self, items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Add 'type' field to menu items recursively."""
        prepared = []
        for item in items:
            pi = dict(item)
            if "type" not in pi:
                pi["type"] = self._infer_menu_item_type(pi)
            if pi.get("items"):
                pi["items"] = self._prepare_menu_items(pi["items"])
            prepared.append(pi)
        return prepared

    def create_menu(self, title: str, handle: str, items: list[dict[str, Any]]) -> dict[str, Any] | None:
        """Create a navigation menu with items.

        Args:
            title: Menu title (e.g., "Main Menu")
            handle: Menu handle (e.g., "main-menu")
            items: List of dicts with title, url (or resourceId), and optional items (nested)
        """
        items = self._prepare_menu_items(items)
        query = """
        mutation menuCreate($title: String!, $handle: String!, $items: [MenuItemCreateInput!]!) {
          menuCreate(title: $title, handle: $handle, items: $items) {
            menu {
              id
              title
              handle
            }
            userErrors {
              field
              message
            }
          }
        }
        """
        data = self._graphql(query, {"title": title, "handle": handle, "items": items})
        result = data["menuCreate"]
        if result["userErrors"]:
            errors = result["userErrors"]
            if any("already" in e.get("message", "").lower() for e in errors):
                return None
            raise Exception(f"menuCreate errors: {errors}")
        return result["menu"]

    def get_menus(self) -> list[dict[str, Any]]:
        """Get all navigation menus."""
        query = """
        {
          menus(first: 50) {
            edges {
              node {
                id
                title
                handle
                items {
                  id
                  title
                  url
                  resourceId
                  items {
                    id
                    title
                    url
                    resourceId
                  }
                }
              }
            }
          }
        }
        """
        data = self._graphql(query)
        return [edge["node"] for edge in data["menus"]["edges"]]

    def delete_menu(self, menu_id: str) -> str:
        """Delete a navigation menu by GID."""
        query = """
        mutation menuDelete($id: ID!) {
          menuDelete(id: $id) {
            deletedMenuId
            userErrors {
              field
              message
            }
          }
        }
        """
        data = self._graphql(query, {"id": menu_id})
        result = data["menuDelete"]
        if result["userErrors"]:
            raise Exception(f"menuDelete errors: {result['userErrors']}")
        return result["deletedMenuId"]

    def update_menu(self, menu_id: str, title: str | None = None, items: list[dict[str, Any]] | None = None) -> dict[str, Any]:
        """Update a navigation menu's title and/or items."""
        if title is None or items is None:
            raise ValueError("update_menu requires both title and items")
        query = """
        mutation menuUpdate($id: ID!, $title: String!, $items: [MenuItemUpdateInput!]!) {
          menuUpdate(id: $id, title: $title, items: $items) {
            menu {
              id
              title
              handle
            }
            userErrors {
              field
              message
            }
          }
        }
        """
        variables = {"id": menu_id}
        if title:
            variables["title"] = title
        if items is not None:
            variables["items"] = self._prepare_menu_items(items)
        data = self._graphql(query, variables)
        result = data["menuUpdate"]
        if result["userErrors"]:
            raise Exception(f"menuUpdate errors: {result['userErrors']}")
        return result["menu"]

    # --- REST: Themes & Assets ---

    def get_themes(self) -> list[dict[str, Any]]:
        """Get all themes."""
        data, _ = self._get_json("themes.json")
        return data.get("themes", [])

    def get_main_theme_id(self) -> int | None:
        """Get the ID of the currently active/main theme."""
        themes = self.get_themes()
        for t in themes:
            if t.get("role") == "main":
                return t["id"]
        return None

    def publish_theme(self, theme_id: int | str) -> dict[str, Any]:
        """Publish a theme as the live/main theme."""
        resp = self._request("PUT", f"themes/{theme_id}.json", json={
            "theme": {
                "id": int(theme_id),
                "role": "main",
            }
        })
        return resp.json().get("theme", {})

    def get_asset(self, theme_id: int | str, key: str) -> dict[str, Any]:
        """Get a single theme asset by key (e.g. 'templates/index.json')."""
        data, _ = self._get_json(f"themes/{theme_id}/assets.json", params={"asset[key]": key})
        return data.get("asset", {})

    def put_asset(
        self,
        theme_id: int | str,
        key: str,
        value: str | None = None,
        attachment: str | None = None,
        src: str | None = None,
    ) -> dict[str, Any]:
        """Create or update a theme asset."""
        asset = {"key": key}
        if value is not None:
            asset["value"] = value
        elif attachment is not None:
            asset["attachment"] = attachment
        elif src is not None:
            asset["src"] = src
        else:
            raise ValueError("put_asset requires one of value, attachment, or src")

        resp = self._request("PUT", f"themes/{theme_id}/assets.json", json={
            "asset": asset
        })
        return resp.json().get("asset", {})

    def list_assets(self, theme_id: int | str) -> list[dict[str, Any]]:
        """List all asset keys for a theme."""
        data, _ = self._get_json(f"themes/{theme_id}/assets.json")
        return data.get("assets", [])

    def delete_asset(self, theme_id: int | str, key: str) -> None:
        """Delete a theme asset by key."""
        self._request("DELETE", f"themes/{theme_id}/assets.json", params={"asset[key]": key})

    # --- REST: Smart Collections ---

    def create_smart_collection(self, collection_data: dict[str, Any]) -> dict[str, Any]:
        """Create a smart collection with rules."""
        resp = self._request("POST", "smart_collections.json", json={"smart_collection": collection_data})
        return resp.json().get("smart_collection", {})

    # --- REST: Price Rules & Discount Codes ---

    def get_price_rules(self) -> list[dict[str, Any]]:
        """Get all price rules."""
        return self._paginate("price_rules.json", "price_rules")

    def get_discount_codes(self, price_rule_id: int | str) -> list[dict[str, Any]]:
        """Get discount codes for a price rule."""
        return self._paginate(f"price_rules/{price_rule_id}/discount_codes.json", "discount_codes")

    def create_price_rule(self, price_rule_data: dict[str, Any]) -> dict[str, Any]:
        """Create a price rule."""
        resp = self._request("POST", "price_rules.json", json={"price_rule": price_rule_data})
        return resp.json().get("price_rule", {})

    def create_discount_code(self, price_rule_id: int | str, code: str) -> dict[str, Any]:
        """Create a discount code for a price rule."""
        resp = self._request("POST", f"price_rules/{price_rule_id}/discount_codes.json", json={
            "discount_code": {"code": code}
        })
        return resp.json().get("discount_code", {})

    # --- GraphQL: Publishing to sales channels ---

    def get_publications(self) -> list[dict[str, Any]]:
        """Get all publications (sales channels)."""
        query = """
        {
          publications(first: 50) {
            edges {
              node {
                id
                name
              }
            }
          }
        }
        """
        data = self._graphql(query)
        return [edge["node"] for edge in data["publications"]["edges"]]

    def publish_resource(self, resource_id: str, publication_ids: list[str]) -> dict[str, Any]:
        """Publish a resource to one or more sales channels.

        Args:
            resource_id: GID of the product/collection (e.g., "gid://shopify/Product/123")
            publication_ids: List of publication GIDs
        """
        query = """
        mutation publishablePublish($id: ID!, $input: [PublicationInput!]!) {
          publishablePublish(id: $id, input: $input) {
            publishable {
              availablePublicationsCount { count }
            }
            userErrors {
              field
              message
            }
          }
        }
        """
        pub_input = [{"publicationId": pid} for pid in publication_ids]
        data = self._graphql(query, {"id": resource_id, "input": pub_input})
        result = data["publishablePublish"]
        if result["userErrors"]:
            raise Exception(f"publishablePublish errors: {result['userErrors']}")
        return result

    def unpublish_resource(self, resource_id: str, publication_ids: list[str]) -> dict[str, Any]:
        """Unpublish a resource from one or more sales channels."""
        query = """
        mutation publishableUnpublish($id: ID!, $input: [PublicationInput!]!) {
          publishableUnpublish(id: $id, input: $input) {
            userErrors {
              field
              message
            }
          }
        }
        """
        pub_input = [{"publicationId": pid} for pid in publication_ids]
        data = self._graphql(query, {"id": resource_id, "input": pub_input})
        result = data["publishableUnpublish"]
        if result["userErrors"]:
            raise Exception(f"publishableUnpublish errors: {result['userErrors']}")
        return result

    # --- GraphQL: SEO meta tags ---

    def update_product_seo(self, product_id: int | str, title_tag: str | None, description_tag: str | None) -> list[dict[str, Any]]:
        """Update product SEO meta tags via REST metafields."""
        metafields = []
        if title_tag:
            metafields.append({
                "ownerId": f"gid://shopify/Product/{product_id}",
                "namespace": "global",
                "key": "title_tag",
                "value": title_tag,
                "type": "single_line_text_field",
            })
        if description_tag:
            metafields.append({
                "ownerId": f"gid://shopify/Product/{product_id}",
                "namespace": "global",
                "key": "description_tag",
                "value": description_tag,
                "type": "single_line_text_field",
            })
        if metafields:
            return self.set_metafields(metafields)
        return []

    # ======================================================================
    # Markets (Shopify Markets API — Admin GraphQL 2024-10)
    # ======================================================================

    def get_markets(self) -> list[dict[str, Any]]:
        """List markets with web presence + base currency (read-only)."""
        query = """
        {
          markets(first: 50) {
            edges { node {
              id name handle enabled primary
              webPresence { id rootUrls { locale url } }
              currencySettings { baseCurrency { currencyCode } }
            } }
          }
        }
        """
        data = self._graphql(query)
        return [e["node"] for e in data.get("markets", {}).get("edges", [])]

    def create_market(self, name: str, country_codes: list[str], enabled: bool = True) -> dict[str, Any]:
        """Create a market covering the given ISO country codes (e.g. ['KW'])."""
        mutation = """
        mutation marketCreate($input: MarketCreateInput!) {
          marketCreate(input: $input) {
            market { id name handle }
            userErrors { field message }
          }
        }
        """
        variables = {"input": {
            "name": name,
            "enabled": enabled,
            "regions": [{"countryCode": c} for c in country_codes],
        }}
        data = self._graphql(mutation, variables)
        result = data["marketCreate"]
        if result["userErrors"]:
            raise GraphQLUserError(f"marketCreate errors: {result['userErrors']}")
        return result["market"]

    def market_add_regions(self, market_id: str, country_codes: list[str]) -> dict[str, Any]:
        """Add country regions to an existing market."""
        mutation = """
        mutation marketRegionsCreate($marketId: ID!, $regions: [MarketRegionCreateInput!]!) {
          marketRegionsCreate(marketId: $marketId, regions: $regions) {
            market { id }
            userErrors { field message }
          }
        }
        """
        variables = {"marketId": market_id, "regions": [{"countryCode": c} for c in country_codes]}
        data = self._graphql(mutation, variables)
        result = data["marketRegionsCreate"]
        if result["userErrors"]:
            raise GraphQLUserError(f"marketRegionsCreate errors: {result['userErrors']}")
        return result["market"]

    def market_update_currency(self, market_id: str, base_currency: str) -> dict[str, Any]:
        """Set a market's base presentment currency (e.g. 'KWD')."""
        mutation = """
        mutation marketCurrencySettingsUpdate($marketId: ID!, $input: MarketCurrencySettingsUpdateInput!) {
          marketCurrencySettingsUpdate(marketId: $marketId, input: $input) {
            market { id }
            userErrors { field message }
          }
        }
        """
        variables = {"marketId": market_id, "input": {"baseCurrency": base_currency}}
        data = self._graphql(mutation, variables)
        result = data["marketCurrencySettingsUpdate"]
        if result["userErrors"]:
            raise GraphQLUserError(f"marketCurrencySettingsUpdate errors: {result['userErrors']}")
        return result["market"]

    def market_create_web_presence(self, market_id: str, default_locale: str,
                                   alternate_locales: list[str] | None = None,
                                   subfolder_suffix: str | None = None,
                                   domain_id: str | None = None) -> dict[str, Any]:
        """Create a web presence for a market (subfolder strategy needs no DNS)."""
        web_presence: dict[str, Any] = {
            "defaultLocale": default_locale,
            "alternateLocales": alternate_locales or [],
        }
        if subfolder_suffix:
            web_presence["subfolderSuffix"] = subfolder_suffix
        if domain_id:
            web_presence["domainId"] = domain_id
        mutation = """
        mutation marketWebPresenceCreate($marketId: ID!, $webPresence: MarketWebPresenceCreateInput!) {
          marketWebPresenceCreate(marketId: $marketId, webPresence: $webPresence) {
            market { id webPresence { id rootUrls { locale url } } }
            userErrors { field message }
          }
        }
        """
        data = self._graphql(mutation, {"marketId": market_id, "webPresence": web_presence})
        result = data["marketWebPresenceCreate"]
        if result["userErrors"]:
            raise GraphQLUserError(f"marketWebPresenceCreate errors: {result['userErrors']}")
        return result["market"]

    # ======================================================================
    # Delivery / shipping (Admin GraphQL 2024-10)
    # ======================================================================

    def get_delivery_profiles(self) -> list[dict[str, Any]]:
        """Read delivery profiles with zones + method definitions (rates)."""
        query = """
        {
          deliveryProfiles(first: 25) {
            edges { node {
              id name default
              profileLocationGroups {
                locationGroup { id }
                locationGroupZones(first: 50) {
                  edges { node {
                    zone { id name countries { code { countryCode } } }
                    methodDefinitions(first: 25) {
                      edges { node {
                        id name active
                        rateProvider {
                          __typename
                          ... on DeliveryRateDefinition { price { amount currencyCode } }
                        }
                      } }
                    }
                  } }
                }
              }
            } }
          }
        }
        """
        data = self._graphql(query)
        return [e["node"] for e in data.get("deliveryProfiles", {}).get("edges", [])]

    def update_delivery_profile(self, profile_id: str, profile_input: dict[str, Any]) -> dict[str, Any]:
        """Thin pass-through for deliveryProfileUpdate (caller builds the input).

        DeliveryProfileInput is deeply nested (locationGroupsToUpdate /
        zonesToCreate / methodDefinitionsToCreate); migrate_shipping.py builds it.
        """
        mutation = """
        mutation deliveryProfileUpdate($id: ID!, $profile: DeliveryProfileInput!) {
          deliveryProfileUpdate(id: $id, profile: $profile) {
            profile { id name }
            userErrors { field message }
          }
        }
        """
        data = self._graphql(mutation, {"id": profile_id, "profile": profile_input})
        result = data["deliveryProfileUpdate"]
        if result["userErrors"]:
            raise GraphQLUserError(f"deliveryProfileUpdate errors: {result['userErrors']}")
        return result["profile"]

    # ======================================================================
    # Domains / payments / webhooks / tax probe (go-live verification)
    # ======================================================================

    def get_domains(self) -> list[dict[str, Any]]:
        """List storefront domains with SSL status (read-only; connect is UI-only)."""
        query = "{ shop { domains { host url sslEnabled } primaryDomain { host url } } }"
        data = self._graphql(query)
        shop = data.get("shop", {}) or {}
        return shop.get("domains", [])

    def get_payment_gateways(self) -> list[dict[str, Any]]:
        """List configured payment gateways (REST; provisioning is merchant-only)."""
        data, _ = self._get_json("payment_gateways.json")
        return data.get("payment_gateways", [])

    def create_webhook_subscription(self, topic: str, callback_url: str,
                                    fmt: str = "JSON") -> dict[str, Any]:
        """Create a webhook subscription for a topic you own."""
        mutation = """
        mutation webhookSubscriptionCreate($topic: WebhookSubscriptionTopic!, $sub: WebhookSubscriptionInput!) {
          webhookSubscriptionCreate(topic: $topic, webhookSubscription: $sub) {
            webhookSubscription { id }
            userErrors { field message }
          }
        }
        """
        variables = {"topic": topic, "sub": {"callbackUrl": callback_url, "format": fmt}}
        data = self._graphql(mutation, variables)
        result = data["webhookSubscriptionCreate"]
        if result["userErrors"]:
            raise GraphQLUserError(f"webhookSubscriptionCreate errors: {result['userErrors']}")
        return result["webhookSubscription"]

    def calculate_draft_order_tax(self, variant_id: str, country_code: str,
                                  quantity: int = 1) -> dict[str, Any]:
        """Probe tax config via draftOrderCalculate (read-only; for VAT verification).

        Returns the calculated tax lines so a preflight/go-live check can confirm
        the destination charges the expected VAT rate for a country.
        """
        mutation = """
        mutation draftOrderCalculate($input: DraftOrderInput!) {
          draftOrderCalculate(input: $input) {
            calculatedDraftOrder {
              totalTaxSet { shopMoney { amount currencyCode } }
              taxLines { title rate priceSet { shopMoney { amount } } }
            }
            userErrors { field message }
          }
        }
        """
        variables = {"input": {
            "lineItems": [{"variantId": variant_id, "quantity": quantity}],
            "shippingAddress": {"countryCode": country_code},
        }}
        data = self._graphql(mutation, variables)
        result = data["draftOrderCalculate"]
        if result["userErrors"]:
            raise GraphQLUserError(f"draftOrderCalculate errors: {result['userErrors']}")
        return result["calculatedDraftOrder"]

    # ======================================================================
    # Bulk Operations API (scalable export / import)
    # ======================================================================

    def get_current_bulk_operation(self) -> dict[str, Any]:
        """Return the current bulk operation status (or {} if none)."""
        data = self._graphql("""
        { currentBulkOperation { id status errorCode objectCount url partialDataUrl } }
        """)
        return data.get("currentBulkOperation") or {}

    def run_bulk_query(self, query: str, poll_interval: float = 2.0,
                       max_wait: float = 600.0) -> list[dict[str, Any]]:
        """Run a bulk query, poll to COMPLETED, download + parse the JSONL result.

        Returns a list of objects (one per JSONL line). Use for large exports
        that would otherwise exhaust the per-request GraphQL cost budget.
        """
        start = self._run_bulk_operation("bulkOperationRunQuery", "query", query)
        url = self._await_bulk_completion(poll_interval, max_wait)
        return self._download_jsonl(url) if url else []

    def run_bulk_mutation(self, mutation: str, staged_upload_path: str,
                          poll_interval: float = 2.0, max_wait: float = 600.0) -> list[dict[str, Any]]:
        """Run a bulk mutation from an already-staged JSONL upload path.

        ``staged_upload_path`` is the key returned by stagedUploadsCreate after
        the JSONL of variables is uploaded. Returns parsed result lines.
        """
        mutation_field = """
        mutation bulkRun($mutation: String!, $path: String!) {
          bulkOperationRunMutation(mutation: $mutation, stagedUploadPath: $path) {
            bulkOperation { id status }
            userErrors { field message }
          }
        }
        """
        data = self._graphql(mutation_field, {"mutation": mutation, "path": staged_upload_path})
        result = data["bulkOperationRunMutation"]
        if result["userErrors"]:
            raise GraphQLUserError(f"bulkOperationRunMutation errors: {result['userErrors']}")
        url = self._await_bulk_completion(poll_interval, max_wait)
        return self._download_jsonl(url) if url else []

    def _run_bulk_operation(self, mutation_name: str, arg_name: str, body: str) -> dict[str, Any]:
        mutation = f"""
        mutation bulkRun(${arg_name}: String!) {{
          {mutation_name}({arg_name}: ${arg_name}) {{
            bulkOperation {{ id status }}
            userErrors {{ field message }}
          }}
        }}
        """
        data = self._graphql(mutation, {arg_name: body})
        result = data[mutation_name]
        if result["userErrors"]:
            raise GraphQLUserError(f"{mutation_name} errors: {result['userErrors']}")
        return result["bulkOperation"]

    def _await_bulk_completion(self, poll_interval: float, max_wait: float) -> str | None:
        waited = 0.0
        while waited <= max_wait:
            op = self.get_current_bulk_operation()
            status = op.get("status")
            if status == "COMPLETED":
                return op.get("url")
            if status in ("FAILED", "CANCELED", "EXPIRED"):
                raise GraphQLUserError(f"Bulk operation {status}: {op.get('errorCode')}")
            self._sleep(poll_interval)
            waited += poll_interval
        raise GraphQLThrottled(f"Bulk operation did not complete within {max_wait}s")

    def _download_jsonl(self, url: str) -> list[dict[str, Any]]:
        resp = self.session.get(url)
        resp.raise_for_status()
        items = []
        for line in resp.text.splitlines():
            line = line.strip()
            if line:
                items.append(json.loads(line))
        return items
