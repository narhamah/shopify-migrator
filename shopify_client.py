import time
import requests


API_VERSION = "2024-10"


class ShopifyClient:
    def __init__(self, shop_url, access_token):
        self.shop_url = shop_url.rstrip("/")
        if not self.shop_url.startswith("https://"):
            self.shop_url = f"https://{self.shop_url}"
        self.base_url = f"{self.shop_url}/admin/api/{API_VERSION}"
        self.session = requests.Session()
        self.session.headers.update({
            "X-Shopify-Access-Token": access_token,
            "Content-Type": "application/json",
        })

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
            # Clear params after first request — pagination URL includes them
            params = {}
            # Parse Link header for next page
            url = None
            link_header = resp.headers.get("Link", "")
            for part in link_header.split(","):
                if 'rel="next"' in part:
                    url = part.split("<")[1].split(">")[0]
                    break
        return all_items

    # Read methods

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

    # Write methods

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
