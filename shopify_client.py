import time
import requests


API_VERSION = "2024-10"


class ShopifyClient:
    def __init__(self, shop_url, access_token):
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

    def _request(self, method, endpoint, **kwargs):
        url = f"{self.base_url}/{endpoint}"
        while True:
            resp = self.session.request(method, url, **kwargs)
            if resp.status_code == 429:
                retry_after = float(resp.headers.get("Retry-After", 2))
                print(f"  Rate limited. Retrying after {retry_after}s...")
                time.sleep(retry_after)
                continue
            resp.raise_for_status()
            return resp

    def _get_json(self, endpoint, params=None):
        resp = self._request("GET", endpoint, params=params)
        return resp.json(), resp.headers

    def _paginate(self, endpoint, resource_key, params=None):
        params = params or {}
        params.setdefault("limit", 250)
        all_items = []
        url = f"{self.base_url}/{endpoint}"
        while url:
            resp = self.session.request("GET", url, params=params)
            if resp.status_code == 429:
                retry_after = float(resp.headers.get("Retry-After", 2))
                print(f"  Rate limited. Retrying after {retry_after}s...")
                time.sleep(retry_after)
                continue
            resp.raise_for_status()
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

    def _graphql(self, query, variables=None):
        """Execute a GraphQL query/mutation with rate-limit handling."""
        payload = {"query": query}
        if variables:
            payload["variables"] = variables
        while True:
            resp = self.session.post(self.graphql_url, json=payload)
            if resp.status_code == 429:
                retry_after = float(resp.headers.get("Retry-After", 2))
                print(f"  Rate limited (GraphQL). Retrying after {retry_after}s...")
                time.sleep(retry_after)
                continue
            resp.raise_for_status()
            data = resp.json()
            if data.get("errors"):
                raise Exception(f"GraphQL errors: {data['errors']}")
            return data.get("data", {})

    # --- REST: Read methods ---

    def get_shop(self):
        data, _ = self._get_json("shop.json")
        return data.get("shop", {})

    def get_products(self):
        return self._paginate("products.json", "products")

    def get_collections(self):
        custom = self._paginate("custom_collections.json", "custom_collections")
        smart = self._paginate("smart_collections.json", "smart_collections")
        return custom + smart

    def get_pages(self):
        return self._paginate("pages.json", "pages")

    def get_blogs(self):
        return self._paginate("blogs.json", "blogs")

    def get_articles(self, blog_id):
        return self._paginate(f"blogs/{blog_id}/articles.json", "articles")

    def get_metafields(self, resource, resource_id):
        return self._paginate(f"{resource}/{resource_id}/metafields.json", "metafields")

    # --- REST: Write methods ---

    def create_product(self, product_data):
        resp = self._request("POST", "products.json", json={"product": product_data})
        return resp.json().get("product", {})

    def update_product(self, product_id, product_data):
        resp = self._request("PUT", f"products/{product_id}.json", json={"product": product_data})
        return resp.json().get("product", {})

    def create_custom_collection(self, collection_data):
        resp = self._request("POST", "custom_collections.json", json={"custom_collection": collection_data})
        return resp.json().get("custom_collection", {})

    def create_page(self, page_data):
        resp = self._request("POST", "pages.json", json={"page": page_data})
        return resp.json().get("page", {})

    def create_blog(self, blog_data):
        resp = self._request("POST", "blogs.json", json={"blog": blog_data})
        return resp.json().get("blog", {})

    def create_article(self, blog_id, article_data):
        resp = self._request("POST", f"blogs/{blog_id}/articles.json", json={"article": article_data})
        return resp.json().get("article", {})

    def create_metafield(self, resource, resource_id, metafield_data):
        resp = self._request(
            "POST",
            f"{resource}/{resource_id}/metafields.json",
            json={"metafield": metafield_data},
        )
        return resp.json().get("metafield", {})

    # --- REST: Lookup by handle ---

    def get_products_by_handle(self, handle):
        data, _ = self._get_json("products.json", params={"handle": handle})
        return data.get("products", [])

    def get_pages_by_handle(self, handle):
        data, _ = self._get_json("pages.json", params={"handle": handle})
        return data.get("pages", [])

    def get_collections_by_handle(self, handle):
        data, _ = self._get_json("custom_collections.json", params={"handle": handle})
        return data.get("custom_collections", [])

    def get_blogs_by_handle(self, handle):
        data, _ = self._get_json("blogs.json", params={"handle": handle})
        return data.get("blogs", [])

    # --- GraphQL: Metaobjects ---

    def get_metaobject_definitions(self):
        """Get all metaobject type definitions."""
        query = """
        {
          metaobjectDefinitions(first: 250) {
            edges {
              node {
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
            }
          }
        }
        """
        data = self._graphql(query)
        return [edge["node"] for edge in data["metaobjectDefinitions"]["edges"]]

    def get_metaobjects(self, metaobject_type):
        """Get all metaobjects of a given type (paginated)."""
        all_objects = []
        cursor = None
        while True:
            after_clause = f', after: "{cursor}"' if cursor else ""
            query = f"""
            {{
              metaobjects(type: "{metaobject_type}", first: 250{after_clause}) {{
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
            data = self._graphql(query)
            edges = data["metaobjects"]["edges"]
            for edge in edges:
                all_objects.append(edge["node"])
                cursor = edge["cursor"]
            if not data["metaobjects"]["pageInfo"]["hasNextPage"]:
                break
        return all_objects

    def create_metaobject_definition(self, definition_data):
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
                print(f"    Definition already exists, continuing...")
                return None
            raise Exception(f"MetaobjectDefinitionCreate errors: {errors}")
        return result["metaobjectDefinition"]

    def create_metaobject(self, metaobject_data):
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

    def update_metaobject(self, metaobject_id, fields):
        """Update a metaobject's fields."""
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
        data = self._graphql(query, {"id": metaobject_id, "metaobject": {"fields": fields}})
        result = data["metaobjectUpdate"]
        if result["userErrors"]:
            raise Exception(f"MetaobjectUpdate errors: {result['userErrors']}")
        return result["metaobject"]

    def get_metaobjects_by_handle(self, metaobject_type, handle):
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

    # --- GraphQL: Translations API ---

    def register_translations(self, resource_id, locale, translations):
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

    def get_translatable_resources(self, resource_type, first=50):
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

    def get_translatable_resource(self, resource_gid):
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
