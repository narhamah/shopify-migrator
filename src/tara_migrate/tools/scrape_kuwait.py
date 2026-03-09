#!/usr/bin/env python3
"""Scrape English and Arabic content from the Kuwait Magento PWA site.

Uses Magento GraphQL API (taraformula.com.kw/graphql) to fetch product,
category, and page data in both English and Arabic.

Outputs data in the same format as translate_gaps.py
so it can be fed directly into import_english.py and import_arabic.py.

Usage:
    # Step 1: Explore site structure
    python scrape_kuwait.py --explore

    # Step 2: Scrape all content
    python scrape_kuwait.py --scrape

    # Scrape specific content type
    python scrape_kuwait.py --scrape --only products
    python scrape_kuwait.py --scrape --only collections

Requirements:
    pip install requests
"""

import argparse
import json
import os
import re
import time

import requests as http_requests

from tara_migrate.core import load_json, save_json
from tara_migrate.core.config import AR_DIR, EN_DIR, SPAIN_DIR
from tara_migrate.core.utils import unicode_slugify as slugify

BASE_URL_US = "https://taraformula.com"
BASE_URL_AE = "https://taraformula.ae"

# Default sources: US site for English, UAE site for Arabic
# All regional sites share the same product catalog
DEFAULT_EN_SITE = BASE_URL_US
DEFAULT_EN_STORE = "us-en"
DEFAULT_AR_SITE = BASE_URL_AE
DEFAULT_AR_STORE = "ae-ar"

OUTPUT_DIR_EN = EN_DIR
OUTPUT_DIR_AR = AR_DIR

# Delay between GraphQL requests to avoid 503 rate limiting
REQUEST_DELAY = 3.0

# Map metaobject type → field key that contains the "name" for handle generation
METAOBJECT_NAME_FIELDS = {
    "ingredient": "name",
    "benefit": "title",
    "blog_author": "full_name",
    "faq_entry": "question",
}


def _slugify_metaobject_handles(metaobjects):
    """Update all metaobject handles by slugifying their name/title field."""
    for mo_type, type_data in metaobjects.items():
        name_field_key = METAOBJECT_NAME_FIELDS.get(mo_type, "name")
        for obj in type_data.get("objects", []):
            # Find the name field value
            name_val = ""
            for field in obj.get("fields", []):
                if field["key"] == name_field_key:
                    name_val = field.get("value", "")
                    break
            if name_val:
                obj["handle"] = slugify(name_val)


HEADERS = {
    "Content-Type": "application/json",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
}


# load_json, save_json, slugify imported from tara_migrate.core


# ------------------------------------------------------------------
# GraphQL client with retry and rate-limit handling
# ------------------------------------------------------------------
class MagentoGraphQL:
    def __init__(self, base_url=BASE_URL_US, delay=REQUEST_DELAY):
        self.base_url = base_url
        self.graphql_url = f"{base_url}/graphql"
        self.delay = delay
        self.session = http_requests.Session()
        self.session.headers.update(HEADERS)

    def query(self, gql_query, store_code=None, retries=3):
        """Execute a GraphQL query with rate-limit retry."""
        headers = {}
        if store_code:
            headers["Store"] = store_code

        for attempt in range(retries):
            time.sleep(self.delay)
            try:
                resp = self.session.post(
                    self.graphql_url,
                    json={"query": gql_query},
                    headers=headers,
                    timeout=30,
                )
                if resp.status_code == 200:
                    data = resp.json()
                    if "errors" in data:
                        print(f"    GraphQL errors: {data['errors'][0].get('message', '')}")
                    return data
                elif resp.status_code == 503:
                    wait = self.delay * (attempt + 2)
                    print(f"    503 rate limited, waiting {wait}s (attempt {attempt+1}/{retries})")
                    time.sleep(wait)
                    continue
                else:
                    print(f"    GraphQL HTTP {resp.status_code}")
                    return None
            except Exception as e:
                print(f"    GraphQL error: {e}")
                if attempt < retries - 1:
                    time.sleep(self.delay * 2)
        return None


# ------------------------------------------------------------------
# Explorer: discover site structure
# ------------------------------------------------------------------
def _fetch_robots_and_sitemaps(base_url):
    """Fetch robots.txt and parse sitemaps for URL discovery."""
    import xml.etree.ElementTree as ET
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    result = {"robots_txt": None, "sitemaps": [], "urls": {"products": [], "categories": [], "pages": [], "other": []}}

    # 1. Fetch robots.txt
    robots_url = f"{base_url}/robots.txt"
    print(f"  Fetching {robots_url}")
    try:
        resp = http_requests.get(robots_url, headers=headers, timeout=15)
        if resp.status_code == 200:
            result["robots_txt"] = resp.text
            print(f"    {len(resp.text)} bytes")
            # Extract Sitemap URLs
            for line in resp.text.splitlines():
                line = line.strip()
                if line.lower().startswith("sitemap:"):
                    sitemap_url = line.split(":", 1)[1].strip()
                    result["sitemaps"].append(sitemap_url)
                    print(f"    Sitemap: {sitemap_url}")
                elif line.lower().startswith("disallow:"):
                    path = line.split(":", 1)[1].strip()
                    if path:
                        print(f"    Disallow: {path}")
        else:
            print(f"    HTTP {resp.status_code}")
    except Exception as e:
        print(f"    Error: {e}")

    # 2. Fetch and parse sitemaps
    for sitemap_url in result["sitemaps"]:
        time.sleep(1)
        print(f"  Fetching sitemap: {sitemap_url}")
        try:
            resp = http_requests.get(sitemap_url, headers=headers, timeout=30)
            if resp.status_code != 200:
                print(f"    HTTP {resp.status_code}")
                continue

            content = resp.text
            # Handle sitemap index (list of sub-sitemaps) vs regular sitemap
            try:
                root = ET.fromstring(content)
            except ET.ParseError:
                print(f"    Failed to parse XML ({len(content)} bytes)")
                continue

            ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
            tag = root.tag.split("}")[-1] if "}" in root.tag else root.tag

            if tag == "sitemapindex":
                # Sitemap index — list sub-sitemaps
                sub_sitemaps = root.findall(".//sm:sitemap/sm:loc", ns)
                if not sub_sitemaps:
                    sub_sitemaps = root.findall(".//{http://www.sitemaps.org/schemas/sitemap/0.9}loc")
                print(f"    Sitemap index with {len(sub_sitemaps)} sub-sitemaps:")
                for sub in sub_sitemaps:
                    sub_url = sub.text.strip() if sub.text else ""
                    if sub_url:
                        print(f"      {sub_url}")
                        result["sitemaps"].append(sub_url)
            else:
                # Regular sitemap — extract URLs
                locs = root.findall(".//sm:url/sm:loc", ns)
                if not locs:
                    locs = root.findall(".//{http://www.sitemaps.org/schemas/sitemap/0.9}loc")
                urls = [loc.text.strip() for loc in locs if loc.text]
                print(f"    {len(urls)} URLs found")

                # Classify URLs
                for url in urls:
                    path = url.replace(base_url, "").strip("/")
                    # Strip store code prefix (us-en/, ae-ar/, kw-en/, etc.)
                    path_clean = re.sub(r"^[a-z]{2}(-[a-z]{2})?/", "", path)

                    if any(seg in path for seg in ["/product/", ".html"]) or path_clean.count("/") == 0:
                        # Could be product or category — check more
                        pass

                    if "catalog/product" in path or path_clean.endswith(".html"):
                        result["urls"]["products"].append({"url": url, "path": path_clean})
                    elif "catalog/category" in path or "/" not in path_clean:
                        result["urls"]["categories"].append({"url": url, "path": path_clean})
                    elif any(seg in path for seg in ["cms", "page", "about", "contact", "faq"]):
                        result["urls"]["pages"].append({"url": url, "path": path_clean})
                    else:
                        result["urls"]["other"].append({"url": url, "path": path_clean})

        except Exception as e:
            print(f"    Error: {e}")

    # Print summary
    for cat, urls in result["urls"].items():
        if urls:
            print(f"    {cat}: {len(urls)} URLs")
            for u in urls[:5]:
                print(f"      {u['path']}")
            if len(urls) > 5:
                print(f"      ... and {len(urls) - 5} more")

    return result


def explore(gql_en, gql_ar, en_store, ar_store):
    print("=" * 60)
    print("EXPLORING TARA SITES")
    print("=" * 60)
    print(f"  English: {gql_en.base_url} (store: {en_store or 'default'})")
    print(f"  Arabic:  {gql_ar.base_url} (store: {ar_store or 'default'})")

    reco = {
        "explored_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "en_site": gql_en.base_url,
        "en_store": en_store,
        "ar_site": gql_ar.base_url,
        "ar_store": ar_store,
        "findings": {},
    }

    # 0. Robots.txt and Sitemaps
    sites_to_check = [
        (gql_en.base_url, "en_main"),
        (gql_ar.base_url, "ar_main"),
        ("https://taraformula.com.kw", "kw"),
    ]
    # Deduplicate
    seen = set()
    for base_url, label in sites_to_check:
        if base_url in seen:
            continue
        seen.add(base_url)
        print(f"\n--- Robots & Sitemaps: {base_url} ({label}) ---")
        sitemap_data = _fetch_robots_and_sitemaps(base_url)
        reco["findings"][f"sitemaps_{label}"] = {
            "sitemaps": sitemap_data["sitemaps"],
            "url_counts": {k: len(v) for k, v in sitemap_data["urls"].items()},
            "sample_urls": {k: v[:10] for k, v in sitemap_data["urls"].items() if v},
        }
        if sitemap_data["robots_txt"]:
            reco["findings"][f"robots_{label}"] = sitemap_data["robots_txt"]

    # 1. English site store views
    print("\n--- English Site Store Views ---")
    result = gql_en.query("""{ availableStores { store_code store_name locale default_display_currency_code } }""")
    if result and "data" in result:
        stores = result["data"].get("availableStores", [])
        reco["findings"]["en_available_stores"] = stores
        for s in stores:
            print(f"    {s.get('store_code')}: {s.get('store_name')} ({s.get('locale')}) — {s.get('default_display_currency_code')}")

    # 2. Arabic site store views
    print("\n--- Arabic Site Store Views ---")
    result = gql_ar.query("""{ availableStores { store_code store_name locale default_display_currency_code } }""")
    if result and "data" in result:
        stores = result["data"].get("availableStores", [])
        reco["findings"]["ar_available_stores"] = stores
        for s in stores:
            print(f"    {s.get('store_code')}: {s.get('store_name')} ({s.get('locale')}) — {s.get('default_display_currency_code')}")

    # 3. English products
    print(f"\n--- English Products (store: {en_store or 'default'}) ---")
    result = gql_en.query("""
    { products(search: "", pageSize: 50, currentPage: 1) {
        total_count
        items { id sku name url_key type_id
            description { html } short_description { html }
            meta_title meta_description
            price_range { minimum_price { regular_price { value currency } final_price { value currency } } }
            media_gallery { url label position }
            categories { id name url_key }
        }
        page_info { total_pages }
    }}
    """, store_code=en_store)
    if result and "data" in result:
        products = result["data"].get("products", {})
        reco["findings"]["en_products"] = products
        total = products.get("total_count", 0)
        items = products.get("items", [])
        print(f"  Total: {total} products ({products.get('page_info', {}).get('total_pages', 1)} pages)")
        for item in items[:5]:
            print(f"    - {item.get('name')} (sku: {item.get('sku')}, url_key: {item.get('url_key')})")
            cats = item.get("categories", [])
            if cats:
                print(f"      categories: {[c.get('name') for c in cats]}")
            price = item.get("price_range", {}).get("minimum_price", {})
            fp = price.get("final_price", {})
            if fp:
                print(f"      price: {fp.get('value')} {fp.get('currency')}")
        if len(items) > 5:
            print(f"    ... and {len(items) - 5} more")

    # 4. Arabic products
    print(f"\n--- Arabic Products (store: {ar_store or 'default'}) ---")
    result = gql_ar.query("""
    { products(search: "", pageSize: 50, currentPage: 1) {
        total_count
        items { id sku name url_key type_id
            description { html } short_description { html }
            meta_title meta_description
            price_range { minimum_price { regular_price { value currency } final_price { value currency } } }
            media_gallery { url label position }
            categories { id name url_key }
        }
        page_info { total_pages }
    }}
    """, store_code=ar_store)
    if result and "data" in result:
        products = result["data"].get("products", {})
        reco["findings"]["ar_products"] = products
        items = products.get("items", [])
        total = products.get("total_count", 0)
        print(f"  Total: {total} products")
        for item in items[:5]:
            name = item.get('name', '')
            is_arabic = any('\u0600' <= c <= '\u06FF' for c in name)
            lang = "AR" if is_arabic else "EN"
            print(f"    - [{lang}] {name} (sku: {item.get('sku')})")
        if items:
            first_name = items[0].get("name", "")
            is_arabic = any('\u0600' <= c <= '\u06FF' for c in first_name)
            if is_arabic:
                print("  >>> Arabic content confirmed!")
            else:
                print("  >>> WARNING: Content appears to be English, not Arabic.")
                print("      Try a different store code.")

    # 5. English categories
    print("\n--- English Categories ---")
    result = gql_en.query("""
    { categories(filters: {}) { items {
        id name url_key url_path product_count
        children { id name url_key product_count
            children { id name url_key product_count }
        }
    }}}
    """, store_code=en_store)
    if result and "data" in result:
        reco["findings"]["en_categories"] = result["data"]["categories"]
        _print_category_tree(result["data"]["categories"].get("items", []))

    # 6. Arabic categories
    print("\n--- Arabic Categories ---")
    result = gql_ar.query("""
    { categories(filters: {}) { items {
        id name url_key url_path product_count
        children { id name url_key product_count
            children { id name url_key product_count }
        }
    }}}
    """, store_code=ar_store)
    if result and "data" in result:
        reco["findings"]["ar_categories"] = result["data"]["categories"]
        _print_category_tree(result["data"]["categories"].get("items", []))

    # 7. Ingredients pages
    print("\n--- Ingredients Pages ---")
    import requests as req
    ing_headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    ing_urls = [
        (f"{gql_en.base_url}/kw-en/ingredients", "EN (KW)"),
        (f"{gql_en.base_url}/us-en/ingredients", "EN (US)"),
        (f"{gql_ar.base_url}/ae-ar/ingredients", "AR (AE)"),
        ("https://taraformula.com.kw/ingredients", "AR (KW root)"),
    ]
    for url, label in ing_urls:
        time.sleep(2)
        try:
            resp = req.get(url, headers=ing_headers, timeout=15)
            print(f"  [{label}] {url}: HTTP {resp.status_code} ({len(resp.text)} bytes)")
            if resp.status_code == 200 and len(resp.text) > 1000:
                # Quick check for ingredient content
                html = resp.text
                ing_count = html.lower().count('ingredient')
                print(f"    'ingredient' appears {ing_count} times in HTML")
                # Check for Arabic content
                arabic_chars = sum(1 for c in html if '\u0600' <= c <= '\u06FF')
                if arabic_chars > 50:
                    print(f"    Contains Arabic text ({arabic_chars} Arabic characters)")
                reco["findings"][f"ingredients_page_{label}"] = {
                    "url": url,
                    "status": resp.status_code,
                    "size": len(resp.text),
                    "ingredient_mentions": ing_count,
                    "arabic_chars": arabic_chars if arabic_chars > 0 else 0,
                }
        except Exception as e:
            print(f"  [{label}] {url}: Error - {e}")

    # Save reco
    reco_path = os.path.join("data", "kuwait_reco.json")
    os.makedirs("data", exist_ok=True)
    save_json(reco, reco_path)
    print(f"\n  Reco saved to {reco_path}")

    print("\n--- Exploration Complete ---")
    print("\nNext: python scrape_kuwait.py --scrape")


def _print_category_tree(items, indent=4):
    for item in items:
        prefix = " " * indent
        print(f"{prefix}{item.get('name')} (url_key: {item.get('url_key')}, products: {item.get('product_count', 0)})")
        for child in item.get("children", []):
            _print_category_tree([child], indent + 4)


# ------------------------------------------------------------------
# Scraper: fetch all content and output in Shopify format
# ------------------------------------------------------------------
class KuwaitScraper:
    def __init__(self, gql_en, gql_ar=None, en_store=None, ar_store=None):
        self.gql_en = gql_en
        self.gql_ar = gql_ar or gql_en  # Arabic may come from UAE site
        self.en_store = en_store  # None = default (English on KW)
        self.ar_store = ar_store  # e.g., "ae-ar" on UAE site
        self.spain_products = load_json(os.path.join(SPAIN_DIR, "products.json"))
        self.spain_collections = load_json(os.path.join(SPAIN_DIR, "collections.json"))
        self.spain_pages = load_json(os.path.join(SPAIN_DIR, "pages.json"))
        self.spain_articles = load_json(os.path.join(SPAIN_DIR, "articles.json"))
        self.spain_metaobjects = load_json(os.path.join(SPAIN_DIR, "metaobjects.json"))
        self.spain_blogs = load_json(os.path.join(SPAIN_DIR, "blogs.json"))

        # Build lookup indices
        self.spain_products_by_handle = {p.get("handle", ""): p for p in self.spain_products}
        self.spain_products_by_sku = {}
        for p in self.spain_products:
            for v in p.get("variants", []):
                sku = v.get("sku", "")
                if sku:
                    self.spain_products_by_sku[sku] = p

        self.spain_collections_by_handle = {c.get("handle", ""): c for c in self.spain_collections}

        # Build collection→SKU membership from collects.json
        # This enables matching Spain collections to Magento categories by product overlap
        self.spain_collects = load_json(os.path.join(SPAIN_DIR, "collects.json"))
        self.spain_collection_skus = {}  # collection_id → set of SKUs
        spain_products_by_id = {str(p.get("id", "")): p for p in self.spain_products}
        for collect in self.spain_collects:
            cid = str(collect.get("collection_id", ""))
            pid = str(collect.get("product_id", ""))
            product = spain_products_by_id.get(pid)
            if product:
                if cid not in self.spain_collection_skus:
                    self.spain_collection_skus[cid] = set()
                for v in product.get("variants", []):
                    sku = v.get("sku", "")
                    if sku:
                        self.spain_collection_skus[cid].add(sku)
        # Also build reverse lookup: collection_id → collection object
        self.spain_collections_by_id = {str(c.get("id", "")): c for c in self.spain_collections}

        # Build product image URL index for fallback matching
        self.spain_products_by_image = {}
        for p in self.spain_products:
            for img in p.get("images", []):
                src = img.get("src", "")
                if src:
                    # Normalize: strip query params, use filename only
                    fname = src.split("?")[0].split("/")[-1].lower()
                    self.spain_products_by_image[fname] = p

        # Populated during scrape_products(), used by scrape_collections()
        self.magento_category_skus = {}  # magento_category_url_key → set of SKUs

        # Discover sitemap URLs for completeness checking
        self.sitemap_urls = {"en": {}, "ar": {}}
        self._discover_sitemaps()

    def _discover_sitemaps(self):
        """Fetch robots.txt and sitemaps from both sites for URL discovery."""
        for label, base_url in [("en", self.gql_en.base_url), ("ar", self.gql_ar.base_url)]:
            if base_url == self.gql_en.base_url and label == "ar":
                # Same site, skip duplicate
                if self.gql_en.base_url == self.gql_ar.base_url:
                    continue
            print(f"\n--- Sitemap discovery: {base_url} ---")
            data = _fetch_robots_and_sitemaps(base_url)
            self.sitemap_urls[label] = data["urls"]

            # Extract url_keys from sitemap product/category URLs
            product_keys = set()
            for u in data["urls"].get("products", []):
                path = u.get("path", "")
                # Extract url_key: last segment before .html
                key = path.replace(".html", "").split("/")[-1]
                if key:
                    product_keys.add(key)
            category_keys = set()
            for u in data["urls"].get("categories", []):
                path = u.get("path", "")
                key = path.replace(".html", "").split("/")[-1]
                if key:
                    category_keys.add(key)

            if product_keys:
                print(f"    Sitemap product url_keys: {len(product_keys)}")
            if category_keys:
                print(f"    Sitemap category url_keys: {len(category_keys)}")

    def scrape_all(self, only=None):
        os.makedirs(OUTPUT_DIR_EN, exist_ok=True)
        os.makedirs(OUTPUT_DIR_AR, exist_ok=True)

        if only is None or only == "products":
            self.scrape_products()
        if only is None or only == "collections":
            self.scrape_collections()
        if only is None or only == "pages":
            self.scrape_pages()
        if only is None or only == "articles":
            self.scrape_articles()
        if only is None or only == "metaobjects":
            self.scrape_metaobjects()

        # Copy non-translatable files
        for fname in ["blogs.json", "metaobject_definitions.json"]:
            src = os.path.join(SPAIN_DIR, fname)
            if os.path.exists(src):
                data = load_json(src)
                save_json(data, os.path.join(OUTPUT_DIR_EN, fname))
                save_json(data, os.path.join(OUTPUT_DIR_AR, fname))

        self._print_summary()

    # ------------------------------------------------------------------
    # Products
    # ------------------------------------------------------------------
    def scrape_products(self):
        print("\n" + "=" * 60)
        print("SCRAPING PRODUCTS")
        print("=" * 60)

        # Fetch all English products
        print("\n--- Fetching English products ---")
        en_raw = self._fetch_all_products(self.gql_en, self.en_store)
        print(f"  Fetched {len(en_raw)} English products from GraphQL")

        en_products = self._map_products(en_raw)
        save_json(en_products, os.path.join(OUTPUT_DIR_EN, "products.json"))
        print(f"  Saved {len(en_products)} English products")

        # Fetch Arabic products
        if self.ar_store is not None:
            print("\n--- Fetching Arabic products ---")
            ar_raw = self._fetch_all_products(self.gql_ar, self.ar_store)
            print(f"  Fetched {len(ar_raw)} Arabic products from GraphQL")
            ar_products = self._map_products(ar_raw)
            save_json(ar_products, os.path.join(OUTPUT_DIR_AR, "products.json"))
            print(f"  Saved {len(ar_products)} Arabic products")
        else:
            print("\n--- No Arabic store code — copying English as placeholder ---")
            save_json(en_products, os.path.join(OUTPUT_DIR_AR, "products.json"))

    def _fetch_all_products(self, gql, store_code, page_size=20):
        """Fetch all products via paginated GraphQL queries."""
        all_items = []
        current_page = 1

        while True:
            query = f"""
            {{
                products(search: "", pageSize: {page_size}, currentPage: {current_page}) {{
                    total_count
                    items {{
                        id
                        sku
                        name
                        url_key
                        type_id
                        description {{ html }}
                        short_description {{ html }}
                        meta_title
                        meta_description
                        price_range {{
                            minimum_price {{
                                regular_price {{ value currency }}
                                final_price {{ value currency }}
                            }}
                        }}
                        media_gallery {{
                            url
                            label
                            position
                        }}
                        categories {{
                            id
                            name
                            url_key
                        }}
                    }}
                    page_info {{
                        total_pages
                        current_page
                    }}
                }}
            }}
            """
            print(f"    Page {current_page}...")
            result = gql.query(query, store_code)
            if not result or "data" not in result:
                print(f"    Failed on page {current_page}, stopping")
                break

            products = result["data"].get("products", {})
            items = products.get("items", [])
            all_items.extend(items)

            page_info = products.get("page_info", {})
            total_pages = page_info.get("total_pages", 1)
            total_count = products.get("total_count", 0)
            print(f"    Got {len(items)} products (page {current_page}/{total_pages}, total: {total_count})")

            if current_page >= total_pages:
                break
            current_page += 1

        return all_items

    def _map_products(self, magento_products):
        """Convert Magento products to Shopify format, merging with Spain data."""
        shopify_products = []

        # Build category→SKU index from product category associations
        for mp in magento_products:
            sku = mp.get("sku", "")
            if sku:
                for cat in mp.get("categories", []):
                    cat_key = cat.get("url_key", "")
                    if cat_key:
                        if cat_key not in self.magento_category_skus:
                            self.magento_category_skus[cat_key] = set()
                        self.magento_category_skus[cat_key].add(sku)

        for mp in magento_products:
            url_key = mp.get("url_key", "")
            sku = mp.get("sku", "")
            name = mp.get("name", "")
            desc = mp.get("description", {})
            short_desc = mp.get("short_description", {})
            price_range = mp.get("price_range", {})
            min_price = price_range.get("minimum_price", {})

            # Match with Spain product: SKU → handle → image URL
            spain_product = self.spain_products_by_sku.get(sku)
            if not spain_product:
                spain_product = self.spain_products_by_handle.get(url_key)
            if not spain_product:
                # Fallback: match by image filename
                for img in mp.get("media_gallery", []):
                    img_url = img.get("url", "")
                    if img_url:
                        fname = img_url.split("?")[0].split("/")[-1].lower()
                        spain_product = self.spain_products_by_image.get(fname)
                        if spain_product:
                            print(f"    Matched by image: {name} → {spain_product.get('title', '')}")
                            break

            # Generate handle from product name (supports Arabic/English)
            handle = slugify(name) if name else url_key

            if spain_product:
                # Clone Spain product as base, overlay Kuwait data
                product = json.loads(json.dumps(spain_product))  # deep copy
                product["title"] = name
                product["handle"] = handle
                if desc and desc.get("html"):
                    product["body_html"] = desc["html"]
                elif short_desc and short_desc.get("html"):
                    # Fallback: use short_description if full description is empty
                    product["body_html"] = short_desc["html"]

                # Update tagline from short_description
                if short_desc and short_desc.get("html"):
                    _update_metafield(product, "custom", "tagline",
                                     re.sub(r'<[^>]+>', '', short_desc["html"]).strip())

                # Update SEO
                if mp.get("meta_title"):
                    product["meta_title"] = mp["meta_title"]
                if mp.get("meta_description"):
                    product["meta_description"] = mp["meta_description"]

                # Update tags from categories
                cats = mp.get("categories", [])
                if cats:
                    cat_names = [c.get("name", "") for c in cats if c.get("name")]
                    product["tags"] = ", ".join(cat_names)

            else:
                # No Spain match — create from scratch
                print(f"    No Spain match for: {name} (sku: {sku}, url_key: {url_key})")
                product = {
                    "id": mp.get("id", 0),
                    "handle": handle,
                    "title": name,
                    "body_html": (desc.get("html", "") if desc else "") or (short_desc.get("html", "") if short_desc else ""),
                    "vendor": "TARA",
                    "product_type": "",
                    "tags": ", ".join(c.get("name", "") for c in mp.get("categories", [])),
                    "status": "active",
                    "variants": [{
                        "title": "Default Title",
                        "price": "0",
                        "compare_at_price": None,
                        "sku": sku,
                        "barcode": "",
                        "weight": 0,
                        "weight_unit": "kg",
                        "inventory_management": "shopify",
                        "option1": "Default Title",
                        "option2": None,
                        "option3": None,
                        "requires_shipping": True,
                        "taxable": True,
                    }],
                    "options": [{"name": "Title", "values": ["Default Title"]}],
                    "metafields": [],
                    "images": [],
                }

            # Always update price from Kuwait (round to whole number — no fractional SAR)
            regular_price = min_price.get("regular_price", {}).get("value")
            final_price = min_price.get("final_price", {}).get("value")
            if product.get("variants"):
                for v in product["variants"]:
                    if final_price is not None:
                        v["price"] = str(round(final_price))
                    if regular_price and final_price and regular_price != final_price:
                        v["compare_at_price"] = str(round(regular_price))

            # Always update images from Kuwait
            media = mp.get("media_gallery", [])
            if media:
                # Sort by position
                media_sorted = sorted(media, key=lambda m: m.get("position", 0))
                product["images"] = [
                    {"src": m["url"], "alt": m.get("label", "")}
                    for m in media_sorted if m.get("url")
                ]

            shopify_products.append(product)

        return shopify_products

    # ------------------------------------------------------------------
    # Collections / Categories
    # ------------------------------------------------------------------
    def scrape_collections(self):
        print("\n" + "=" * 60)
        print("SCRAPING COLLECTIONS")
        print("=" * 60)

        print("\n--- Fetching English categories ---")
        en_cats = self._fetch_categories(self.gql_en, self.en_store)
        en_collections = self._map_categories(en_cats)
        save_json(en_collections, os.path.join(OUTPUT_DIR_EN, "collections.json"))
        print(f"  Saved {len(en_collections)} English collections")

        if self.ar_store is not None:
            print("\n--- Fetching Arabic categories ---")
            ar_cats = self._fetch_categories(self.gql_ar, self.ar_store)
            ar_collections = self._map_categories(ar_cats)
            save_json(ar_collections, os.path.join(OUTPUT_DIR_AR, "collections.json"))
            print(f"  Saved {len(ar_collections)} Arabic collections")
        else:
            save_json(en_collections, os.path.join(OUTPUT_DIR_AR, "collections.json"))

    def _fetch_categories(self, gql, store_code):
        result = gql.query("""
        {
            categories(filters: {}) {
                items {
                    id
                    name
                    url_key
                    url_path
                    description
                    image
                    meta_title
                    meta_description
                    product_count
                    children {
                        id name url_key url_path description image product_count
                        children {
                            id name url_key url_path description image product_count
                        }
                    }
                }
            }
        }
        """, store_code)
        if result and "data" in result:
            return _flatten_categories(result["data"].get("categories", {}).get("items", []))
        return []

    def _map_categories(self, categories):
        collections = []
        matched_spain_ids = set()  # Track which Spain collections got matched

        for cat in categories:
            url_key = cat.get("url_key", "")
            cat_name = cat.get("name", "")
            handle = slugify(cat_name) if cat_name else url_key

            # Match with Spain collection: handle → SKU overlap
            spain_coll = self.spain_collections_by_handle.get(url_key)

            if not spain_coll:
                # Fallback: match by product SKU overlap (Jaccard similarity)
                spain_coll = self._match_collection_by_skus(url_key)
                if spain_coll:
                    print(f"    Matched by SKU overlap: Magento '{cat_name}' → Spain '{spain_coll.get('title')}'")

            if spain_coll:
                coll = json.loads(json.dumps(spain_coll))
                coll["title"] = cat_name or coll.get("title", "")
                coll["handle"] = handle
                if cat.get("description"):
                    coll["body_html"] = cat["description"]
                matched_spain_ids.add(str(spain_coll.get("id", "")))
            else:
                coll = {
                    "id": cat.get("id", 0),
                    "handle": handle,
                    "title": cat_name,
                    "body_html": cat.get("description", "") or "",
                }

            if cat.get("image"):
                coll["image"] = {"src": cat["image"]}

            collections.append(coll)
        return collections

    def _match_collection_by_skus(self, magento_category_url_key):
        """Match a Magento category to a Spain collection by product SKU overlap."""
        magento_skus = self.magento_category_skus.get(magento_category_url_key, set())
        if not magento_skus:
            return None

        best_match = None
        best_score = 0

        for cid, spain_skus in self.spain_collection_skus.items():
            if not spain_skus:
                continue
            # Jaccard similarity: intersection / union
            intersection = magento_skus & spain_skus
            if not intersection:
                continue
            union = magento_skus | spain_skus
            score = len(intersection) / len(union)

            if score > best_score:
                best_score = score
                best_match = self.spain_collections_by_id.get(cid)

        # Require at least 30% overlap to consider it a match
        if best_score >= 0.3 and best_match:
            return best_match
        return None

    # ------------------------------------------------------------------
    # Pages (CMS pages)
    # ------------------------------------------------------------------
    def scrape_pages(self):
        print("\n" + "=" * 60)
        print("SCRAPING PAGES")
        print("=" * 60)

        # Try to fetch CMS pages by known identifiers from Spain export
        spain_handles = [p.get("handle", "") for p in self.spain_pages if p.get("handle")]
        # Add common Magento CMS identifiers
        identifiers = list(set(spain_handles + [
            "home", "about", "about-us", "contact", "faq",
            "privacy-policy", "terms-and-conditions", "shipping", "returns",
        ]))

        en_pages = self._fetch_cms_pages(identifiers, self.gql_en, self.en_store)
        save_json(en_pages, os.path.join(OUTPUT_DIR_EN, "pages.json"))
        print(f"  Saved {len(en_pages)} English pages")

        if self.ar_store is not None:
            ar_pages = self._fetch_cms_pages(identifiers, self.gql_ar, self.ar_store)
            save_json(ar_pages, os.path.join(OUTPUT_DIR_AR, "pages.json"))
            print(f"  Saved {len(ar_pages)} Arabic pages")
        else:
            save_json(self.spain_pages, os.path.join(OUTPUT_DIR_AR, "pages.json"))

    def _fetch_cms_pages(self, identifiers, gql, store_code):
        pages = []
        spain_by_handle = {p.get("handle", ""): p for p in self.spain_pages}

        for ident in identifiers:
            result = gql.query(f"""
            {{
                cmsPage(identifier: "{ident}") {{
                    identifier
                    title
                    content
                    content_heading
                    meta_title
                    meta_description
                }}
            }}
            """, store_code)

            if result and "data" in result and result["data"].get("cmsPage"):
                cms = result["data"]["cmsPage"]
                page_title = cms.get("title", "")
                handle = slugify(page_title) if page_title else ident
                spain_page = spain_by_handle.get(ident)
                if spain_page:
                    page = json.loads(json.dumps(spain_page))
                    page["title"] = page_title or page.get("title", "")
                    page["handle"] = handle
                    if cms.get("content"):
                        page["body_html"] = cms["content"]
                else:
                    page = {
                        "id": hash(ident) % 10**9,
                        "handle": handle,
                        "title": page_title,
                        "body_html": cms.get("content", ""),
                        "published_at": "2024-01-01T00:00:00Z",
                        "template_suffix": "",
                    }
                pages.append(page)
                print(f"  Found CMS page: {ident} — {cms.get('title', '')}")

        return pages

    # ------------------------------------------------------------------
    # Articles / Blog posts
    # ------------------------------------------------------------------
    def scrape_articles(self):
        print("\n" + "=" * 60)
        print("SCRAPING ARTICLES")
        print("=" * 60)

        # Magento doesn't have a standard blog — articles are typically from
        # a blog extension or custom module. Copy Spain articles as base.
        # The content will be translated via LLM for what we can't scrape.

        print("  Blog articles not available via Magento GraphQL.")
        print("  Copying Spain articles as base — these will need LLM translation.")

        save_json(self.spain_articles, os.path.join(OUTPUT_DIR_EN, "articles.json"))
        save_json(self.spain_articles, os.path.join(OUTPUT_DIR_AR, "articles.json"))
        save_json(self.spain_blogs, os.path.join(OUTPUT_DIR_EN, "blogs.json"))
        save_json(self.spain_blogs, os.path.join(OUTPUT_DIR_AR, "blogs.json"))
        print(f"  Copied {len(self.spain_articles)} articles")

    # ------------------------------------------------------------------
    # Metaobjects (ingredients, benefits, FAQs)
    # ------------------------------------------------------------------
    def scrape_metaobjects(self):
        print("\n" + "=" * 60)
        print("SCRAPING METAOBJECTS")
        print("=" * 60)

        # Start with Spain metaobjects as base structure
        en_metaobjects = json.loads(json.dumps(self.spain_metaobjects))
        ar_metaobjects = json.loads(json.dumps(self.spain_metaobjects))

        # Slugify all metaobject handles from their name field
        # (Spain handles are Spanish — we need EN/AR handles)
        _slugify_metaobject_handles(en_metaobjects)
        _slugify_metaobject_handles(ar_metaobjects)

        # Scrape ingredients from dedicated pages
        print("\n--- Scraping ingredients from HTML pages ---")
        en_ingredients = self._scrape_ingredients_page(
            f"{self.gql_en.base_url}/kw-en/ingredients",
            fallback_urls=[
                f"{self.gql_en.base_url}/us-en/ingredients",
                f"{self.gql_en.base_url}/ingredients",
            ]
        )
        ar_ingredients = self._scrape_ingredients_page(
            f"{self.gql_ar.base_url}/ae-ar/ingredients",
            fallback_urls=[
                f"{self.gql_ar.base_url}/ingredients",
                "https://taraformula.com.kw/ingredients",
            ]
        )

        if en_ingredients:
            print(f"  Found {len(en_ingredients)} English ingredients")
            self._merge_ingredients(en_metaobjects, en_ingredients)
        else:
            print("  No English ingredients scraped from HTML")

        if ar_ingredients:
            print(f"  Found {len(ar_ingredients)} Arabic ingredients")
            self._merge_ingredients(ar_metaobjects, ar_ingredients)
        else:
            print("  No Arabic ingredients scraped from HTML")

        # Re-slugify handles after merge (names may have changed)
        _slugify_metaobject_handles(en_metaobjects)
        _slugify_metaobject_handles(ar_metaobjects)

        save_json(en_metaobjects, os.path.join(OUTPUT_DIR_EN, "metaobjects.json"))
        save_json(ar_metaobjects, os.path.join(OUTPUT_DIR_AR, "metaobjects.json"))

        total = sum(len(td.get("objects", [])) for td in en_metaobjects.values())
        print(f"  Saved {total} metaobjects across {len(en_metaobjects)} types")

    def _scrape_ingredients_page(self, url, fallback_urls=None):
        """Scrape ingredients from the ingredients listing page."""
        import requests as req
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        }

        urls_to_try = [url] + (fallback_urls or [])
        html = None

        for try_url in urls_to_try:
            print(f"  Trying: {try_url}")
            time.sleep(REQUEST_DELAY)
            try:
                resp = req.get(try_url, headers=headers, timeout=30)
                if resp.status_code == 200 and len(resp.text) > 1000:
                    html = resp.text
                    print(f"  Got {len(html)} bytes from {try_url}")
                    break
                else:
                    print(f"  HTTP {resp.status_code}, {len(resp.text)} bytes")
            except Exception as e:
                print(f"  Error: {e}")

        if not html:
            return []

        # Try to parse with scrapling if available, otherwise use regex
        try:
            from scrapling import Adaptor
            page = Adaptor(html, auto_match=False)
            return self._extract_ingredients_scrapling(page)
        except ImportError:
            return self._extract_ingredients_regex(html)

    def _extract_ingredients_scrapling(self, page):
        """Extract ingredients using scrapling CSS selectors."""
        ingredients = []

        # Try various selectors for ingredient items
        selectors = [
            '.ingredient-item', '.ingredient-card', '.ingredient',
            '[class*="ingredient"]', '[data-ingredient]',
            '.ingredients-list li', '.ingredients-grid > div',
            '#ingredients-list > *',
        ]

        items = []
        for sel in selectors:
            items = page.css(sel)
            if items:
                print(f"    Matched selector: '{sel}' — {len(items)} items")
                break

        if not items:
            # Try to find any structured content
            print("    No ingredient items found with standard selectors")
            print("    Dumping page structure for analysis...")
            # Find all elements with 'ingredient' in class or id
            all_els = page.css('[class*="ingredient"], [id*="ingredient"]')
            for el in all_els[:10]:
                tag = el.tag if hasattr(el, 'tag') else 'unknown'
                classes = el.attrib.get('class', '')
                text = (el.text or '')[:80]
                print(f"      <{tag} class='{classes}'> {text}")
            return []

        for item in items:
            ingredient = {}

            # Extract name
            name_el = item.css('h2, h3, h4, .name, [class*="name"], [class*="title"]')
            if name_el:
                ingredient["name"] = name_el[0].text.strip() if name_el[0].text else ""

            # Extract description
            desc_el = item.css('p, .description, [class*="desc"], [class*="content"]')
            if desc_el:
                ingredient["description"] = desc_el[0].text.strip() if desc_el[0].text else ""

            # Extract image
            img_el = item.css('img')
            if img_el:
                src = img_el[0].attrib.get('src', '') or img_el[0].attrib.get('data-src', '')
                if src:
                    ingredient["image_url"] = src

            # Extract link (for detail page)
            link_el = item.css('a[href]')
            if link_el:
                ingredient["detail_url"] = link_el[0].attrib.get('href', '')

            if ingredient.get("name"):
                ingredients.append(ingredient)

        return ingredients

    def _extract_ingredients_regex(self, html):
        """Fallback: extract ingredients using regex patterns."""
        ingredients = []

        # Look for JSON-LD or embedded JSON data
        json_matches = re.findall(r'<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>', html, re.DOTALL)
        for match in json_matches:
            try:
                data = json.loads(match)
                if isinstance(data, list):
                    for item in data:
                        if item.get("@type") == "Product" or "ingredient" in str(item).lower():
                            ingredients.append({"name": item.get("name", ""), "raw_data": item})
            except json.JSONDecodeError:
                pass

        # Look for embedded state/config JSON
        state_matches = re.findall(r'window\.__\w+__\s*=\s*({.+?});\s*</script>', html, re.DOTALL)
        for match in state_matches:
            try:
                data = json.loads(match)
                print(f"    Found embedded state data with keys: {list(data.keys())[:10]}")
            except json.JSONDecodeError:
                pass

        return ingredients

    def _merge_ingredients(self, metaobjects, scraped_ingredients):
        """Merge scraped ingredient data into metaobjects structure."""
        if "ingredient" not in metaobjects:
            return

        spain_ingredients = metaobjects["ingredient"].get("objects", [])

        # Build name-based lookup (lowercase, stripped)
        scraped_by_name = {}
        for ing in scraped_ingredients:
            name = ing.get("name", "").strip().lower()
            if name:
                scraped_by_name[name] = ing

        updated = 0
        for spain_ing in spain_ingredients:
            # Find matching scraped ingredient by name
            spain_name = ""
            for field in spain_ing.get("fields", []):
                if field["key"] == "name":
                    spain_name = field["value"].strip().lower()
                    break

            handle = spain_ing.get("handle", "").replace("-", " ")
            match = scraped_by_name.get(spain_name) or scraped_by_name.get(handle)

            if match:
                # Update name
                if match.get("name"):
                    for field in spain_ing["fields"]:
                        if field["key"] == "name":
                            field["value"] = match["name"]
                            break

                # Update description
                if match.get("description"):
                    for field in spain_ing["fields"]:
                        if field["key"] == "description":
                            field["value"] = match["description"]
                            break

                # Store scraped image URL so migrate_all_images can upload it
                if match.get("image_url"):
                    spain_ing["_scraped_image_url"] = match["image_url"]

                updated += 1

        print(f"    Updated {updated}/{len(spain_ingredients)} ingredients from scraped data")

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    def _print_summary(self):
        print("\n" + "=" * 60)
        print("SCRAPE SUMMARY")
        print("=" * 60)

        for label, directory in [("English", OUTPUT_DIR_EN), ("Arabic", OUTPUT_DIR_AR)]:
            print(f"\n  {label} ({directory}/):")
            for fname in ["products.json", "collections.json", "pages.json",
                          "articles.json", "blogs.json", "metaobjects.json"]:
                fpath = os.path.join(directory, fname)
                if os.path.exists(fpath):
                    data = load_json(fpath)
                    if isinstance(data, list):
                        print(f"    {fname}: {len(data)} items")
                    elif isinstance(data, dict):
                        total = sum(len(v.get("objects", [])) for v in data.values()) if data else 0
                        print(f"    {fname}: {total} items across {len(data)} types")

        print("\n  Items needing LLM translation:")
        print("    - Articles (blog posts) — not in Magento GraphQL")
        print("    - Benefits, FAQs — Shopify-specific (no Magento page)")
        print("    - Product metafield accordions — Shopify-specific")
        print("\n  Scraped from Magento:")
        print("    - Products (title, description, price, images, tags)")
        print("    - Categories → Shopify collections")
        print("    - Ingredients (from /ingredients HTML page)")
        print("\n  Next: run translate scripts for remaining content,")
        print("  then import_english.py and import_arabic.py")


def _flatten_categories(items, flat=None):
    if flat is None:
        flat = []
    for item in items:
        children = item.pop("children", [])
        flat.append(item)
        if children:
            _flatten_categories(children, flat)
    return flat


def _update_metafield(product, namespace, key, value):
    if "metafields" not in product:
        product["metafields"] = []
    for mf in product["metafields"]:
        if mf.get("namespace") == namespace and mf.get("key") == key:
            mf["value"] = value
            return
    product["metafields"].append({
        "namespace": namespace,
        "key": key,
        "value": value,
        "type": "single_line_text_field",
    })


def main():
    parser = argparse.ArgumentParser(description="Scrape Kuwait TARA site for EN/AR content")
    parser.add_argument("--explore", action="store_true", help="Explore site structure")
    parser.add_argument("--scrape", action="store_true", help="Scrape all content")
    parser.add_argument("--only", choices=["products", "collections", "pages", "articles", "metaobjects"],
                       help="Scrape only a specific content type")
    parser.add_argument("--en-site", default=DEFAULT_EN_SITE,
                       help=f"English site URL (default: {DEFAULT_EN_SITE})")
    parser.add_argument("--en-store", default=DEFAULT_EN_STORE,
                       help=f"English store code (default: {DEFAULT_EN_STORE})")
    parser.add_argument("--ar-site", default=DEFAULT_AR_SITE,
                       help=f"Arabic site URL (default: {DEFAULT_AR_SITE})")
    parser.add_argument("--ar-store", default=DEFAULT_AR_STORE,
                       help=f"Arabic store code (default: {DEFAULT_AR_STORE})")
    parser.add_argument("--delay", type=float, default=REQUEST_DELAY,
                       help=f"Delay between requests in seconds (default: {REQUEST_DELAY})")
    args = parser.parse_args()

    gql_en = MagentoGraphQL(base_url=args.en_site, delay=args.delay)
    gql_ar = MagentoGraphQL(base_url=args.ar_site, delay=args.delay)

    if args.explore:
        explore(gql_en, gql_ar, args.en_store, args.ar_store)
    elif args.scrape:
        scraper = KuwaitScraper(
            gql_en=gql_en,
            gql_ar=gql_ar,
            en_store=args.en_store,
            ar_store=args.ar_store,
        )
        scraper.scrape_all(only=args.only)
    else:
        print("Usage:")
        print("  python scrape_kuwait.py --explore")
        print("  python scrape_kuwait.py --scrape")
        print("  python scrape_kuwait.py --scrape --only products")


if __name__ == "__main__":
    main()
