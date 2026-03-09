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

    def update_metaobject_definition(self, definition_id, update_data):
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

    # --- GraphQL: Metafield Definitions ---

    def get_metafield_definitions(self, owner_type):
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

    def create_metafield_definition(self, definition):
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

    def set_metafields(self, metafields):
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

    def staged_uploads_create(self, staged_inputs):
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

    def file_create(self, files_input):
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

    def get_file_by_id(self, file_id):
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

    def upload_file_from_url(self, source_url, filename=None, alt="", optimize=False):
        """Upload a file to Shopify from a public URL.

        Downloads from source_url, optionally optimizes to WebP, stages the
        upload, and creates the file. Returns the Shopify file GID.

        Args:
            source_url: Public URL to download from.
            filename: Override filename (optional).
            alt: Alt text for the file.
            optimize: If True, convert images to optimized WebP before uploading.
        """
        import mimetypes
        import os
        import urllib.parse

        if not filename:
            parsed = urllib.parse.urlparse(source_url)
            filename = os.path.basename(parsed.path) or "file"
            filename = filename.split("?")[0]

        # Download the file
        resp = self.session.get(source_url, stream=True)
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

    def upload_file_bytes(self, content, filename, alt=""):
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

    # --- REST: Collects (product-collection links) ---

    def get_collects(self, collection_id=None):
        """Get all product-collection associations."""
        params = {}
        if collection_id:
            params["collection_id"] = collection_id
        return self._paginate("collects.json", "collects", params=params)

    def get_collection_product_ids(self, collection_id):
        """Get product IDs in a collection (works with API 2024-10+)."""
        products = self._paginate(f"collections/{collection_id}/products.json", "products",
                                  params={"fields": "id"})
        return [p["id"] for p in products]

    def create_collect(self, product_id, collection_id):
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

    def collection_add_products(self, collection_id, product_ids):
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

    def get_redirects(self):
        """Get all URL redirects."""
        return self._paginate("redirects.json", "redirects")

    def create_redirect(self, path, target):
        """Create a URL redirect."""
        resp = self._request("POST", "redirects.json", json={
            "redirect": {"path": path, "target": target}
        })
        return resp.json().get("redirect", {})

    # --- REST: Inventory ---

    def get_locations(self):
        """Get all inventory locations."""
        data, _ = self._get_json("locations.json")
        return data.get("locations", [])

    # --- REST: Policies ---

    def get_policies(self):
        """Get shop policies."""
        data, _ = self._get_json("policies.json")
        return data.get("policies", [])

    # --- GraphQL: Locale management ---

    def enable_locale(self, locale_code):
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

    def get_locales(self):
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

    def get_inventory_item_id(self, variant_id):
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

    def set_inventory_quantity(self, inventory_item_id, location_id, quantity):
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
    def _infer_menu_item_type(item):
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

    def _prepare_menu_items(self, items):
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

    def create_menu(self, title, handle, items):
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

    def get_menus(self):
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

    def delete_menu(self, menu_id):
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

    def update_menu(self, menu_id, title=None, items=None):
        """Update a navigation menu's title and/or items."""
        query = """
        mutation menuUpdate($id: ID!, $title: String, $items: [MenuItemUpdateInput!]) {
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
            variables["items"] = items
        data = self._graphql(query, variables)
        result = data["menuUpdate"]
        if result["userErrors"]:
            raise Exception(f"menuUpdate errors: {result['userErrors']}")
        return result["menu"]

    # --- REST: Themes & Assets ---

    def get_themes(self):
        """Get all themes."""
        data, _ = self._get_json("themes.json")
        return data.get("themes", [])

    def get_main_theme_id(self):
        """Get the ID of the currently active/main theme."""
        themes = self.get_themes()
        for t in themes:
            if t.get("role") == "main":
                return t["id"]
        return None

    def get_asset(self, theme_id, key):
        """Get a single theme asset by key (e.g. 'templates/index.json')."""
        data, _ = self._get_json(f"themes/{theme_id}/assets.json", params={"asset[key]": key})
        return data.get("asset", {})

    def put_asset(self, theme_id, key, value):
        """Create or update a theme asset."""
        resp = self._request("PUT", f"themes/{theme_id}/assets.json", json={
            "asset": {"key": key, "value": value}
        })
        return resp.json().get("asset", {})

    def list_assets(self, theme_id):
        """List all asset keys for a theme."""
        data, _ = self._get_json(f"themes/{theme_id}/assets.json")
        return data.get("assets", [])

    # --- REST: Smart Collections ---

    def create_smart_collection(self, collection_data):
        """Create a smart collection with rules."""
        resp = self._request("POST", "smart_collections.json", json={"smart_collection": collection_data})
        return resp.json().get("smart_collection", {})

    # --- REST: Price Rules & Discount Codes ---

    def get_price_rules(self):
        """Get all price rules."""
        return self._paginate("price_rules.json", "price_rules")

    def get_discount_codes(self, price_rule_id):
        """Get discount codes for a price rule."""
        return self._paginate(f"price_rules/{price_rule_id}/discount_codes.json", "discount_codes")

    def create_price_rule(self, price_rule_data):
        """Create a price rule."""
        resp = self._request("POST", "price_rules.json", json={"price_rule": price_rule_data})
        return resp.json().get("price_rule", {})

    def create_discount_code(self, price_rule_id, code):
        """Create a discount code for a price rule."""
        resp = self._request("POST", f"price_rules/{price_rule_id}/discount_codes.json", json={
            "discount_code": {"code": code}
        })
        return resp.json().get("discount_code", {})

    # --- GraphQL: Publishing to sales channels ---

    def get_publications(self):
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

    def publish_resource(self, resource_id, publication_ids):
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

    # --- GraphQL: SEO meta tags ---

    def update_product_seo(self, product_id, title_tag, description_tag):
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
