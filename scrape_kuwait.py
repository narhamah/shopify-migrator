#!/usr/bin/env python3
"""Scrape English and Arabic content from the Kuwait Magento PWA site.

Uses Magento GraphQL API (taraformula.com.kw/graphql) to fetch product,
category, and page data in both English and Arabic.

Outputs data in the same format as translate_to_english.py / translate_to_arabic.py
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


BASE_URL_KW = "https://taraformula.com.kw"
BASE_URL_AE = "https://taraformula.ae"

GRAPHQL_URL_KW = f"{BASE_URL_KW}/graphql"
GRAPHQL_URL_AE = f"{BASE_URL_AE}/graphql"

OUTPUT_DIR_EN = "data/english"
OUTPUT_DIR_AR = "data/arabic"
SPAIN_DIR = "data/spain_export"

# Delay between GraphQL requests to avoid 503 rate limiting
REQUEST_DELAY = 3.0

HEADERS = {
    "Content-Type": "application/json",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
}


def load_json(filepath):
    if not os.path.exists(filepath):
        return [] if filepath.endswith(".json") else {}
    with open(filepath, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json(data, filepath):
    os.makedirs(os.path.dirname(filepath) or ".", exist_ok=True)
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


# ------------------------------------------------------------------
# GraphQL client with retry and rate-limit handling
# ------------------------------------------------------------------
class MagentoGraphQL:
    def __init__(self, base_url=BASE_URL_KW, delay=REQUEST_DELAY):
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
def explore(gql):
    print("=" * 60)
    print("EXPLORING KUWAIT SITE STRUCTURE")
    print("=" * 60)

    # 1. Store config
    print("\n--- Store Config ---")
    result = gql.query("""
    {
        storeConfig {
            store_name
            store_code
            default_display_currency_code
            locale
            base_currency_code
            weight_unit
        }
    }
    """)
    if result and "data" in result:
        config = result["data"]["storeConfig"]
        print(f"  Default store: {config.get('store_name')} ({config.get('store_code')})")
        print(f"  Locale: {config.get('locale')}, Currency: {config.get('default_display_currency_code')}")

    # 2. Available store views
    print("\n--- Store Views ---")
    result = gql.query("""
    {
        availableStores {
            store_code
            store_name
            locale
            default_display_currency_code
            store_group_name
        }
    }
    """)
    store_codes = []
    if result and "data" in result:
        stores = result["data"].get("availableStores", [])
        print(f"  Found {len(stores)} store views:")
        for s in stores:
            code = s.get("store_code", "")
            store_codes.append(code)
            print(f"    {code}: {s.get('store_name')} ({s.get('locale')}) — {s.get('default_display_currency_code')} [group: {s.get('store_group_name')}]")

    # 3. Probe for Arabic store codes on Kuwait site
    print("\n--- Probing for Arabic store code (Kuwait) ---")
    ar_candidates = ["ar", "kw_ar", "kw-ar", "gl-ar", "arabic", "default"]
    ar_candidates = [c for c in ar_candidates if c not in store_codes]

    found_ar_kw = False
    for code in ar_candidates:
        print(f"  Trying store code '{code}' on Kuwait...")
        result = gql.query(f"""
        {{
            storeConfig {{
                store_name
                store_code
                locale
            }}
        }}
        """, store_code=code)
        if result and "data" in result:
            config = result["data"].get("storeConfig", {})
            if config and "ar" in config.get("locale", "").lower():
                print(f"    FOUND Arabic: {config.get('store_name')} ({config.get('locale')})")
                found_ar_kw = True
                break
            elif config:
                print(f"    {config.get('store_name')} ({config.get('locale')}) — not Arabic")
        else:
            print(f"    Not available")

    # 4. Check UAE site (taraformula.ae) for Arabic
    print("\n--- Checking UAE site (taraformula.ae) ---")
    gql_ae = MagentoGraphQL(base_url=BASE_URL_AE, delay=gql.delay)

    # Check default store
    result = gql_ae.query("""
    {
        storeConfig {
            store_name
            store_code
            locale
            default_display_currency_code
        }
    }
    """)
    if result and "data" in result:
        config = result["data"].get("storeConfig", {})
        print(f"  Default: {config.get('store_name')} ({config.get('locale')}) — {config.get('default_display_currency_code')}")

    # Check available stores on UAE
    result = gql_ae.query("""
    {
        availableStores {
            store_code
            store_name
            locale
            default_display_currency_code
        }
    }
    """)
    if result and "data" in result:
        ae_stores = result["data"].get("availableStores", [])
        print(f"  Found {len(ae_stores)} UAE store views:")
        for s in ae_stores:
            print(f"    {s.get('store_code')}: {s.get('store_name')} ({s.get('locale')}) — {s.get('default_display_currency_code')}")

    # Try ae-ar store code for Arabic products
    print("\n--- UAE Arabic sample (store code 'ae-ar') ---")
    for ar_code in ["ae-ar", "ae_ar", "ar", None]:
        label = ar_code or "default"
        print(f"  Trying '{label}'...")
        result = gql_ae.query(f"""
        {{
            products(search: "", pageSize: 2, currentPage: 1) {{
                total_count
                items {{
                    name
                    url_key
                    sku
                }}
            }}
        }}
        """, store_code=ar_code)
        if result and "data" in result:
            products = result["data"].get("products", {})
            items = products.get("items", [])
            total = products.get("total_count", 0)
            print(f"    Total: {total} products")
            for item in items:
                print(f"    - {item.get('name')} (sku: {item.get('sku')})")
            if items:
                # Check if the names are in Arabic
                first_name = items[0].get("name", "")
                is_arabic = any('\u0600' <= c <= '\u06FF' for c in first_name)
                print(f"    Content language: {'Arabic' if is_arabic else 'English/Other'}")
                if is_arabic:
                    print(f"    >>> Use: --ar-site ae --ar-store {label}")
                    break

    # 4. Sample products (no store header — this works)
    print("\n--- Sample Products (default store) ---")
    result = gql.query("""
    {
        products(search: "", pageSize: 5, currentPage: 1) {
            total_count
            items {
                id
                sku
                name
                url_key
                type_id
                description { html }
                short_description { html }
                meta_title
                meta_description
                price_range {
                    minimum_price {
                        regular_price { value currency }
                        final_price { value currency }
                    }
                }
                media_gallery { url label position }
                categories { id name url_key }
            }
            page_info { total_pages current_page }
        }
    }
    """)
    if result and "data" in result:
        products = result["data"].get("products", {})
        total = products.get("total_count", 0)
        items = products.get("items", [])
        print(f"  Total products: {total}")
        for item in items:
            print(f"    - {item.get('name')} (sku: {item.get('sku')}, url_key: {item.get('url_key')})")
            desc = item.get("description", {})
            if desc and desc.get("html"):
                clean = re.sub(r'<[^>]+>', '', desc["html"])[:120]
                print(f"      desc: {clean}...")
            cats = item.get("categories", [])
            if cats:
                print(f"      categories: {[c.get('name') for c in cats]}")
            price = item.get("price_range", {}).get("minimum_price", {})
            fp = price.get("final_price", {})
            if fp:
                print(f"      price: {fp.get('value')} {fp.get('currency')}")
            media = item.get("media_gallery", [])
            print(f"      images: {len(media)}")

    # 5. Categories
    print("\n--- Category Tree (default store) ---")
    result = gql.query("""
    {
        categories(filters: {}) {
            items {
                id name url_key url_path product_count
                children {
                    id name url_key product_count
                    children {
                        id name url_key product_count
                    }
                }
            }
        }
    }
    """)
    if result and "data" in result:
        items = result["data"].get("categories", {}).get("items", [])
        _print_category_tree(items)

    # 6. CMS pages (try without store header)
    print("\n--- CMS Pages (default store) ---")
    for ident in ["home", "about", "about-us", "contact", "faq", "privacy-policy"]:
        result = gql.query(f"""
        {{
            cmsPage(identifier: "{ident}") {{
                identifier
                title
                content_heading
                meta_title
            }}
        }}
        """)
        if result and "data" in result and result["data"].get("cmsPage"):
            page = result["data"]["cmsPage"]
            print(f"  Found: {page.get('identifier')} — {page.get('title')}")

    # Save reco file with all findings
    print("\n--- Saving reconnaissance data ---")
    reco = {
        "explored_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "kuwait_site": BASE_URL_KW,
        "uae_site": BASE_URL_AE,
        "findings": {},
    }

    # Re-fetch key data for reco file
    result = gql.query("""{ storeConfig { store_name store_code locale default_display_currency_code } }""")
    if result and "data" in result:
        reco["findings"]["kw_store_config"] = result["data"]["storeConfig"]

    result = gql.query("""{ availableStores { store_code store_name locale default_display_currency_code } }""")
    if result and "data" in result:
        reco["findings"]["kw_available_stores"] = result["data"]["availableStores"]

    # Fetch all products for reco
    result = gql.query("""
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
    """)
    if result and "data" in result:
        reco["findings"]["kw_products"] = result["data"]["products"]

    # Categories
    result = gql.query("""
    { categories(filters: {}) { items {
        id name url_key url_path product_count
        children { id name url_key product_count
            children { id name url_key product_count }
        }
    }}}
    """)
    if result and "data" in result:
        reco["findings"]["kw_categories"] = result["data"]["categories"]

    # UAE data
    result = gql_ae.query("""{ storeConfig { store_name store_code locale default_display_currency_code } }""")
    if result and "data" in result:
        reco["findings"]["ae_store_config"] = result["data"]["storeConfig"]

    result = gql_ae.query("""{ availableStores { store_code store_name locale default_display_currency_code } }""")
    if result and "data" in result:
        reco["findings"]["ae_available_stores"] = result["data"]["availableStores"]

    # UAE products sample
    for ar_code in ["ae-ar", "ae_ar", "ar", None]:
        result = gql_ae.query("""
        { products(search: "", pageSize: 50, currentPage: 1) {
            total_count
            items { id sku name url_key description { html } short_description { html }
                price_range { minimum_price { final_price { value currency } } }
                media_gallery { url label }
                categories { id name url_key }
            }
            page_info { total_pages }
        }}
        """, store_code=ar_code)
        if result and "data" in result:
            items = result["data"].get("products", {}).get("items", [])
            if items:
                first_name = items[0].get("name", "")
                is_arabic = any('\u0600' <= c <= '\u06FF' for c in first_name)
                reco["findings"][f"ae_products_{ar_code or 'default'}"] = {
                    "store_code": ar_code,
                    "is_arabic": is_arabic,
                    "data": result["data"]["products"],
                }
                if is_arabic:
                    reco["findings"]["arabic_source"] = {
                        "site": "ae",
                        "store_code": ar_code,
                    }
                    break

    reco_path = os.path.join("data", "kuwait_reco.json")
    os.makedirs("data", exist_ok=True)
    save_json(reco, reco_path)
    print(f"  Saved to {reco_path}")

    print("\n--- Exploration Complete ---")
    print("\nNext steps:")
    print("  python scrape_kuwait.py --scrape")


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

        for mp in magento_products:
            url_key = mp.get("url_key", "")
            sku = mp.get("sku", "")
            name = mp.get("name", "")
            desc = mp.get("description", {})
            short_desc = mp.get("short_description", {})
            price_range = mp.get("price_range", {})
            min_price = price_range.get("minimum_price", {})

            # Match with Spain product by SKU first, then by handle
            spain_product = self.spain_products_by_sku.get(sku)
            if not spain_product:
                spain_product = self.spain_products_by_handle.get(url_key)

            if spain_product:
                # Clone Spain product as base, overlay Kuwait data
                product = json.loads(json.dumps(spain_product))  # deep copy
                product["title"] = name
                if desc and desc.get("html"):
                    product["body_html"] = desc["html"]

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
                    "handle": url_key,
                    "title": name,
                    "body_html": desc.get("html", "") if desc else "",
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

            # Always update price from Kuwait
            regular_price = min_price.get("regular_price", {}).get("value")
            final_price = min_price.get("final_price", {}).get("value")
            if product.get("variants"):
                for v in product["variants"]:
                    if final_price is not None:
                        v["price"] = str(final_price)
                    if regular_price and final_price and regular_price != final_price:
                        v["compare_at_price"] = str(regular_price)

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
        for cat in categories:
            url_key = cat.get("url_key", "")
            spain_coll = self.spain_collections_by_handle.get(url_key)

            if spain_coll:
                coll = json.loads(json.dumps(spain_coll))
                coll["title"] = cat.get("name", coll.get("title", ""))
                if cat.get("description"):
                    coll["body_html"] = cat["description"]
            else:
                coll = {
                    "id": cat.get("id", 0),
                    "handle": url_key,
                    "title": cat.get("name", ""),
                    "body_html": cat.get("description", "") or "",
                }

            if cat.get("image"):
                coll["image"] = {"src": cat["image"]}

            collections.append(coll)
        return collections

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
                spain_page = spain_by_handle.get(ident)
                if spain_page:
                    page = json.loads(json.dumps(spain_page))
                    page["title"] = cms.get("title", page.get("title", ""))
                    if cms.get("content"):
                        page["body_html"] = cms["content"]
                else:
                    page = {
                        "id": hash(ident) % 10**9,
                        "handle": ident,
                        "title": cms.get("title", ""),
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

        # Metaobjects are Shopify-specific. Copy Spain structure.
        # Text content will be translated via LLM for what we can't scrape.

        print("  Metaobjects are Shopify-specific — no Magento equivalent.")
        print("  Copying Spain metaobjects as base — these will need LLM translation.")

        save_json(self.spain_metaobjects, os.path.join(OUTPUT_DIR_EN, "metaobjects.json"))
        save_json(self.spain_metaobjects, os.path.join(OUTPUT_DIR_AR, "metaobjects.json"))

        total = sum(len(td.get("objects", [])) for td in self.spain_metaobjects.values())
        print(f"  Copied {total} metaobjects across {len(self.spain_metaobjects)} types")

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
        print("    - Metaobjects (ingredients, benefits, FAQs) — Shopify-specific")
        print("    - Product metafield accordions — Shopify-specific")
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
    parser.add_argument("--en-store", default=None,
                       help="English store code (default: None = default store on KW site)")
    parser.add_argument("--ar-site", choices=["kw", "ae"], default=None,
                       help="Which site to use for Arabic: 'kw' (taraformula.com.kw) or 'ae' (taraformula.ae)")
    parser.add_argument("--ar-store", default=None,
                       help="Arabic store code (e.g., 'ae-ar'). If not set, Arabic output = English copy.")
    parser.add_argument("--delay", type=float, default=REQUEST_DELAY,
                       help=f"Delay between requests in seconds (default: {REQUEST_DELAY})")
    args = parser.parse_args()

    gql_en = MagentoGraphQL(base_url=BASE_URL_KW, delay=args.delay)

    # Arabic GraphQL client
    if args.ar_site == "ae":
        gql_ar = MagentoGraphQL(base_url=BASE_URL_AE, delay=args.delay)
    elif args.ar_site == "kw":
        gql_ar = gql_en
    else:
        gql_ar = None

    if args.explore:
        explore(gql_en)
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
        print("  python scrape_kuwait.py --scrape --ar-site ae --ar-store ae-ar")


if __name__ == "__main__":
    main()
