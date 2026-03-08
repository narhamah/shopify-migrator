#!/usr/bin/env python3
"""Scrape English and Arabic content from the Kuwait Magento PWA site.

Uses Scrapling to fetch product, collection, page, and article content
from taraformula.com.kw in both English (/kw-en/) and Arabic (/).

Outputs data in the same format as translate_to_english.py / translate_to_arabic.py
so it can be fed directly into import_english.py and import_arabic.py.

Usage:
    # Step 1: Explore site structure (discover URLs and data patterns)
    python scrape_kuwait.py --explore

    # Step 2: Scrape all content
    python scrape_kuwait.py --scrape

    # Step 3: Scrape specific content type
    python scrape_kuwait.py --scrape --only products
    python scrape_kuwait.py --scrape --only collections
    python scrape_kuwait.py --scrape --only pages
    python scrape_kuwait.py --scrape --only articles
    python scrape_kuwait.py --scrape --only metaobjects

Requirements:
    pip install scrapling
"""

import argparse
import json
import os
import re
import time
import traceback

from scrapling import Fetcher, StealthyFetcher


BASE_URL = "https://taraformula.com.kw"
EN_PREFIX = "/kw-en"
AR_PREFIX = ""  # Arabic is the default locale

OUTPUT_DIR_EN = "data/english"
OUTPUT_DIR_AR = "data/arabic"
SPAIN_DIR = "data/spain_export"

# Delay between requests to be polite
REQUEST_DELAY = 1.5


def load_json(filepath):
    if not os.path.exists(filepath):
        return [] if filepath.endswith(".json") else {}
    with open(filepath, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json(data, filepath):
    os.makedirs(os.path.dirname(filepath) or ".", exist_ok=True)
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def slugify(text):
    """Convert text to URL slug for matching."""
    text = text.lower().strip()
    text = re.sub(r'[^\w\s-]', '', text)
    text = re.sub(r'[\s_]+', '-', text)
    return text.strip('-')


class KuwaitScraper:
    def __init__(self, use_stealth=False):
        self.fetcher = StealthyFetcher if use_stealth else Fetcher
        self.spain_products = load_json(os.path.join(SPAIN_DIR, "products.json"))
        self.spain_collections = load_json(os.path.join(SPAIN_DIR, "collections.json"))
        self.spain_pages = load_json(os.path.join(SPAIN_DIR, "pages.json"))
        self.spain_articles = load_json(os.path.join(SPAIN_DIR, "articles.json"))
        self.spain_metaobjects = load_json(os.path.join(SPAIN_DIR, "metaobjects.json"))
        self.spain_blogs = load_json(os.path.join(SPAIN_DIR, "blogs.json"))

    def fetch_page(self, url):
        """Fetch a page with rate limiting."""
        print(f"    Fetching: {url}")
        time.sleep(REQUEST_DELAY)
        try:
            page = self.fetcher.get(url, stealthy_headers=True, follow_redirects=True)
            return page
        except Exception as e:
            print(f"    Error fetching {url}: {e}")
            return None

    # ------------------------------------------------------------------
    # EXPLORE: Discover site structure
    # ------------------------------------------------------------------
    def explore(self):
        """Discover the site structure and available content."""
        print("=" * 60)
        print("EXPLORING KUWAIT SITE STRUCTURE")
        print("=" * 60)

        # 1. Check GraphQL endpoint
        print("\n--- Checking GraphQL endpoint ---")
        self._check_graphql()

        # 2. Explore GraphQL in depth (products, categories, CMS pages, store views)
        print("\n--- GraphQL: Store Views ---")
        self._explore_graphql_store_views()

        print("\n--- GraphQL: Sample Products (EN) ---")
        self._explore_graphql_products("kw_en")

        print("\n--- GraphQL: Sample Products (AR / default) ---")
        self._explore_graphql_products(None)

        print("\n--- GraphQL: Categories (EN) ---")
        self._explore_graphql_categories("kw_en")

        print("\n--- GraphQL: Categories (AR / default) ---")
        self._explore_graphql_categories(None)

        print("\n--- GraphQL: CMS Pages ---")
        self._explore_graphql_cms_pages("kw_en")

        print("\n--- GraphQL: Custom Attributes ---")
        self._explore_graphql_custom_attributes()

        # 3. Explore sitemap
        print("\n--- Sitemap ---")
        self._explore_sitemap()

        # 4. Try common Magento API endpoints
        print("\n--- Magento REST API ---")
        self._check_magento_api()

    def _check_graphql(self):
        """Try the Magento GraphQL endpoint."""
        try:
            import requests
            headers = {
                "Content-Type": "application/json",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            }
            query = {
                "query": """
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
                """
            }
            resp = requests.post(f"{BASE_URL}/graphql", json=query, headers=headers, timeout=10)
            print(f"  Status: {resp.status_code}")
            if resp.status_code == 200:
                data = resp.json()
                print(f"  Response: {json.dumps(data, indent=2)[:500]}")
                print("  >>> GraphQL is available! We can use structured queries.")
            else:
                print(f"  Response: {resp.text[:300]}")
        except Exception as e:
            print(f"  Error: {e}")

    def _explore_graphql_store_views(self):
        """Discover available store views (languages)."""
        query = """
        {
            availableStores {
                store_code
                store_name
                locale
                base_currency_code
                default_display_currency_code
            }
        }
        """
        result = self._graphql_query(query)
        if result and "data" in result:
            stores = result["data"].get("availableStores", [])
            print(f"  Found {len(stores)} store views:")
            for s in stores:
                print(f"    {s.get('store_code')}: {s.get('store_name')} ({s.get('locale')}) — {s.get('default_display_currency_code')}")
        else:
            print("  Could not fetch store views")

    def _explore_graphql_products(self, store_code):
        """Fetch a few sample products to see available fields."""
        query = """
        {
            products(search: "", pageSize: 3, currentPage: 1) {
                total_count
                items {
                    id
                    sku
                    name
                    url_key
                    url_suffix
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
                    media_gallery {
                        url
                        label
                        position
                    }
                    categories {
                        id
                        name
                        url_key
                    }
                }
                page_info {
                    total_pages
                    current_page
                }
            }
        }
        """
        label = store_code or "default"
        result = self._graphql_query(query, store_code)
        if result and "data" in result:
            products = result["data"].get("products", {})
            total = products.get("total_count", 0)
            items = products.get("items", [])
            print(f"  Store '{label}': {total} total products")
            for item in items:
                print(f"    - {item.get('name')} (url_key: {item.get('url_key')}, sku: {item.get('sku')})")
                desc = item.get("description", {})
                if desc and desc.get("html"):
                    print(f"      description: {desc['html'][:100]}...")
                cats = item.get("categories", [])
                if cats:
                    print(f"      categories: {[c.get('name') for c in cats]}")
                price = item.get("price_range", {}).get("minimum_price", {})
                fp = price.get("final_price", {})
                if fp:
                    print(f"      price: {fp.get('value')} {fp.get('currency')}")
        else:
            print(f"  Store '{label}': No products or query failed")

    def _explore_graphql_categories(self, store_code):
        """Fetch category tree."""
        query = """
        {
            categories(filters: {}) {
                items {
                    id
                    name
                    url_key
                    url_path
                    product_count
                    children_count
                    children {
                        id
                        name
                        url_key
                        product_count
                        children {
                            id
                            name
                            url_key
                            product_count
                        }
                    }
                }
            }
        }
        """
        label = store_code or "default"
        result = self._graphql_query(query, store_code)
        if result and "data" in result:
            items = result["data"].get("categories", {}).get("items", [])
            print(f"  Store '{label}': {len(items)} top-level categories")
            self._print_category_tree(items, indent=4)
        else:
            print(f"  Store '{label}': No categories or query failed")

    def _print_category_tree(self, items, indent=4):
        """Print category tree recursively."""
        for item in items:
            name = item.get("name", "")
            url_key = item.get("url_key", "")
            count = item.get("product_count", 0)
            prefix = " " * indent
            print(f"{prefix}{name} (url_key: {url_key}, products: {count})")
            children = item.get("children", [])
            if children:
                self._print_category_tree(children, indent + 4)

    def _explore_graphql_cms_pages(self, store_code):
        """Try to discover CMS pages."""
        # Magento doesn't have a "list all CMS pages" query, try known identifiers
        identifiers = ["home", "about", "about-us", "contact", "faq", "privacy-policy",
                       "terms-and-conditions", "shipping", "returns"]
        label = store_code or "default"
        found = []
        for ident in identifiers:
            query = f"""
            {{
                cmsPage(identifier: "{ident}") {{
                    identifier
                    title
                    content_heading
                    meta_title
                }}
            }}
            """
            result = self._graphql_query(query, store_code)
            if result and "data" in result and result["data"].get("cmsPage"):
                page = result["data"]["cmsPage"]
                found.append(page)
                print(f"  Found CMS page: {page.get('identifier')} — {page.get('title')}")

        if not found:
            print(f"  Store '{label}': No CMS pages found with common identifiers")

    def _explore_graphql_custom_attributes(self):
        """Check if custom product attributes are available."""
        query = """
        {
            customAttributeMetadata(attributes: [
                { attribute_code: "description", entity_type: "catalog_product" }
            ]) {
                items {
                    attribute_code
                    attribute_type
                    entity_type
                    input_type
                }
            }
        }
        """
        result = self._graphql_query(query)
        if result and "data" in result:
            items = result["data"].get("customAttributeMetadata", {}).get("items", [])
            print(f"  Custom attributes found: {len(items)}")
            for item in items:
                print(f"    {item.get('attribute_code')}: {item.get('input_type')} ({item.get('attribute_type')})")
        else:
            print("  Could not fetch custom attributes")

    def _explore_page(self, url):
        """Explore a page's structure."""
        page = self.fetch_page(url)
        if not page:
            return

        # Look for navigation links
        nav_links = page.css('nav a')
        print(f"  Navigation links: {len(nav_links)}")
        for link in nav_links[:20]:
            href = link.attrib.get('href', '')
            text = link.text.strip() if link.text else ''
            if text and href:
                print(f"    {text}: {href}")

        # Look for product cards
        product_cards = page.css('[class*="product"], [class*="Product"], .product-item')
        print(f"  Product-like elements: {len(product_cards)}")

        # Look for meta tags
        title = page.css('title')
        if title:
            print(f"  Page title: {title[0].text}")

    def _explore_collection_page(self, url):
        """Explore a collection/category page."""
        page = self.fetch_page(url)
        if not page:
            return

        # Try common product listing selectors
        selectors = [
            'a[href*="/product"]',
            'a[href*="/products/"]',
            '.product-item a',
            '[class*="productCard"] a',
            '[class*="ProductCard"] a',
            '[class*="product-card"] a',
            'a[class*="product"]',
            '.gallery-item a',
            'li.product a',
        ]
        for sel in selectors:
            items = page.css(sel)
            if items:
                print(f"  Selector '{sel}': {len(items)} matches")
                for item in items[:5]:
                    href = item.attrib.get('href', '')
                    text = item.text.strip() if item.text else ''
                    print(f"    {text[:50]}: {href}")

        # Print all links for manual inspection
        all_links = page.css('a[href]')
        product_links = set()
        for link in all_links:
            href = link.attrib.get('href', '')
            if href and not href.startswith('#') and not href.startswith('javascript'):
                # Look for product-like URLs
                if any(x in href for x in ['.html', '/product', '/catalog']):
                    product_links.add(href)
        if product_links:
            print(f"\n  Potential product URLs ({len(product_links)}):")
            for link in sorted(product_links)[:20]:
                print(f"    {link}")

        # Dump page structure for analysis
        print(f"\n  Page HTML size: {len(page.html_content) if hasattr(page, 'html_content') else 'unknown'} bytes")

        # Look for JSON-LD structured data
        scripts = page.css('script[type="application/ld+json"]')
        for s in scripts:
            try:
                data = json.loads(s.text)
                print(f"\n  JSON-LD data found: {json.dumps(data, indent=2)[:500]}")
            except:
                pass

        # Look for inline JSON data (common in PWA/SPA)
        scripts = page.css('script')
        for s in scripts:
            text = s.text or ''
            if 'product' in text.lower() and ('{' in text):
                # Try to find JSON data
                for pattern in [r'window\.__INITIAL_STATE__\s*=\s*({.+?});',
                               r'window\.__STORE_CONFIG__\s*=\s*({.+?});',
                               r'"products"\s*:\s*\[',
                               r'catalogCategory']:
                    if re.search(pattern, text):
                        print(f"\n  Found inline data matching pattern: {pattern}")
                        print(f"    Preview: {text[:300]}")
                        break

    def _explore_product_page_en(self):
        """Try to find and explore a product page."""
        # Use a known product handle from Spain export
        if self.spain_products:
            handle = self.spain_products[0].get('handle', '')
            print(f"  Trying handle from Spain export: {handle}")

        # Try common URL patterns for Magento
        test_urls = [
            f"{BASE_URL}{EN_PREFIX}/hair-care.html",
            f"{BASE_URL}{EN_PREFIX}/shampoo.html",
            f"{BASE_URL}{EN_PREFIX}/hair-care/shampoo",
        ]
        for url in test_urls:
            page = self.fetch_page(url)
            if page:
                title = page.css('title')
                if title:
                    print(f"  Page title: {title[0].text}")

                # Look for product data
                h1 = page.css('h1')
                if h1:
                    print(f"  H1: {h1[0].text}")

                price = page.css('[class*="price"], .price')
                if price:
                    print(f"  Price elements: {len(price)}")
                    for p in price[:3]:
                        print(f"    {p.text.strip() if p.text else ''}")
                break

    def _explore_sitemap(self):
        """Try to fetch the sitemap via requests (not scrapling, to avoid encoding issues)."""
        import requests
        sitemap_urls = [
            f"{BASE_URL}/sitemap.xml",
            f"{BASE_URL}/pub/sitemap/sitemap.xml",
            f"{BASE_URL}/media/sitemap/sitemap.xml",
            f"{BASE_URL}/sitemap_index.xml",
        ]
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
        for url in sitemap_urls:
            try:
                print(f"    Trying: {url}")
                resp = requests.get(url, headers=headers, timeout=10)
                if resp.status_code != 200:
                    continue
                text = resp.text
                if '<url>' in text or '<sitemap>' in text:
                    print(f"  Found sitemap at: {url}")
                    locs = re.findall(r'<loc>(.*?)</loc>', text)
                    print(f"  Total URLs: {len(locs)}")
                    for loc in locs[:20]:
                        print(f"    {loc}")
                    if len(locs) > 20:
                        print(f"    ... and {len(locs) - 20} more")
                    return
            except Exception as e:
                print(f"    Error: {e}")
        print("  No sitemap found")

    def _check_magento_api(self):
        """Try common Magento REST API endpoints."""
        import requests
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        }
        endpoints = [
            "/rest/V1/store/storeConfigs",
            "/rest/V1/products?searchCriteria[pageSize]=1",
            "/rest/V1/categories",
        ]
        for ep in endpoints:
            try:
                resp = requests.get(f"{BASE_URL}{ep}", headers=headers, timeout=10)
                print(f"  {ep}: {resp.status_code}")
                if resp.status_code == 200:
                    print(f"    Response: {resp.text[:300]}")
            except Exception as e:
                print(f"  {ep}: Error - {e}")

    # ------------------------------------------------------------------
    # SCRAPE: Fetch content using GraphQL (preferred) or HTML fallback
    # ------------------------------------------------------------------
    def scrape_all(self, only=None):
        """Scrape all content types."""
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

        # Copy non-translated files
        for fname in ["blogs.json", "metaobject_definitions.json"]:
            src = os.path.join(SPAIN_DIR, fname)
            if os.path.exists(src):
                data = load_json(src)
                save_json(data, os.path.join(OUTPUT_DIR_EN, fname))
                save_json(data, os.path.join(OUTPUT_DIR_AR, fname))

    # ------------------------------------------------------------------
    # GraphQL-based scraping (Magento)
    # ------------------------------------------------------------------
    def _graphql_query(self, query, store_code=None):
        """Execute a Magento GraphQL query."""
        import requests
        headers = {
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        }
        if store_code:
            headers["Store"] = store_code

        time.sleep(REQUEST_DELAY)
        try:
            resp = requests.post(
                f"{BASE_URL}/graphql",
                json={"query": query},
                headers=headers,
                timeout=30,
            )
            if resp.status_code == 200:
                return resp.json()
            else:
                print(f"    GraphQL error: {resp.status_code} - {resp.text[:200]}")
                return None
        except Exception as e:
            print(f"    GraphQL error: {e}")
            return None

    def _graphql_products(self, store_code=None, page_size=50):
        """Fetch all products via GraphQL."""
        all_products = []
        current_page = 1

        while True:
            query = f"""
            {{
                products(
                    search: ""
                    pageSize: {page_size}
                    currentPage: {current_page}
                ) {{
                    total_count
                    items {{
                        id
                        sku
                        name
                        url_key
                        url_suffix
                        type_id
                        description {{
                            html
                        }}
                        short_description {{
                            html
                        }}
                        price_range {{
                            minimum_price {{
                                regular_price {{
                                    value
                                    currency
                                }}
                                final_price {{
                                    value
                                    currency
                                }}
                            }}
                        }}
                        media_gallery {{
                            url
                            label
                        }}
                        categories {{
                            id
                            name
                            url_key
                        }}
                        meta_title
                        meta_description
                    }}
                    page_info {{
                        total_pages
                        current_page
                    }}
                }}
            }}
            """
            print(f"    Fetching products page {current_page}...")
            result = self._graphql_query(query, store_code)
            if not result or "data" not in result:
                break

            products_data = result["data"].get("products", {})
            items = products_data.get("items", [])
            all_products.extend(items)

            page_info = products_data.get("page_info", {})
            total_pages = page_info.get("total_pages", 1)
            print(f"    Got {len(items)} products (page {current_page}/{total_pages})")

            if current_page >= total_pages:
                break
            current_page += 1

        return all_products

    def _graphql_categories(self, store_code=None):
        """Fetch all categories via GraphQL."""
        query = """
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
                    children {
                        id
                        name
                        url_key
                        url_path
                        description
                        children {
                            id
                            name
                            url_key
                            url_path
                            description
                        }
                    }
                }
            }
        }
        """
        result = self._graphql_query(query, store_code)
        if result and "data" in result:
            return self._flatten_categories(result["data"].get("categories", {}).get("items", []))
        return []

    def _flatten_categories(self, items, flat=None):
        """Flatten nested category tree."""
        if flat is None:
            flat = []
        for item in items:
            children = item.pop("children", [])
            flat.append(item)
            if children:
                self._flatten_categories(children, flat)
        return flat

    def _graphql_cms_pages(self, store_code=None):
        """Fetch CMS pages via GraphQL."""
        query = """
        {
            cmsPage(identifier: "home") {
                identifier
                title
                content
                meta_title
                meta_description
            }
        }
        """
        # Magento doesn't have a "list all CMS pages" query easily,
        # so we'll need to try known identifiers
        return self._graphql_query(query, store_code)

    # ------------------------------------------------------------------
    # Product scraping
    # ------------------------------------------------------------------
    def scrape_products(self):
        """Scrape products in both EN and AR."""
        print("\n" + "=" * 60)
        print("SCRAPING PRODUCTS")
        print("=" * 60)

        # Try GraphQL first
        print("\n--- Trying GraphQL for English products ---")
        en_products_raw = self._graphql_products(store_code="kw_en")

        if en_products_raw:
            print(f"\n  GraphQL returned {len(en_products_raw)} English products")
            en_products = self._map_magento_products_to_shopify(en_products_raw)
            save_json(en_products, os.path.join(OUTPUT_DIR_EN, "products.json"))
            print(f"  Saved {len(en_products)} English products")
        else:
            print("  GraphQL failed, trying HTML scraping...")
            en_products = self._scrape_products_html(EN_PREFIX)
            save_json(en_products, os.path.join(OUTPUT_DIR_EN, "products.json"))

        print("\n--- Trying GraphQL for Arabic products ---")
        ar_products_raw = self._graphql_products(store_code="kw_ar")
        if not ar_products_raw:
            # Try default store (Arabic is usually default)
            ar_products_raw = self._graphql_products(store_code=None)

        if ar_products_raw:
            print(f"\n  GraphQL returned {len(ar_products_raw)} Arabic products")
            ar_products = self._map_magento_products_to_shopify(ar_products_raw)
            save_json(ar_products, os.path.join(OUTPUT_DIR_AR, "products.json"))
            print(f"  Saved {len(ar_products)} Arabic products")
        else:
            print("  GraphQL failed, trying HTML scraping...")
            ar_products = self._scrape_products_html(AR_PREFIX)
            save_json(ar_products, os.path.join(OUTPUT_DIR_AR, "products.json"))

    def _map_magento_products_to_shopify(self, magento_products):
        """Convert Magento product data to Shopify format, matching with Spain export."""
        shopify_products = []

        # Build lookup by URL key (handle)
        spain_by_handle = {p.get("handle", ""): p for p in self.spain_products}

        for mp in magento_products:
            url_key = mp.get("url_key", "")
            name = mp.get("name", "")
            desc = mp.get("description", {})
            short_desc = mp.get("short_description", {})
            price_range = mp.get("price_range", {})
            min_price = price_range.get("minimum_price", {})

            # Try to match with Spain product
            spain_product = spain_by_handle.get(url_key)
            if not spain_product:
                # Try fuzzy match by title similarity
                spain_product = self._find_spain_product_match(name, url_key)

            # Use Spain product as base if found, overlay with Kuwait data
            if spain_product:
                product = dict(spain_product)
                product["title"] = name
                product["body_html"] = desc.get("html", "") if desc else product.get("body_html", "")
                if short_desc and short_desc.get("html"):
                    # Store as tagline metafield
                    self._update_metafield(product, "custom", "tagline", short_desc["html"])
            else:
                product = {
                    "id": mp.get("id", 0),
                    "handle": url_key,
                    "title": name,
                    "body_html": desc.get("html", "") if desc else "",
                    "vendor": "TARA",
                    "product_type": "",
                    "tags": "",
                    "status": "active",
                    "variants": [],
                    "options": [],
                    "metafields": [],
                    "images": [],
                }

            # Update price from Kuwait
            regular_price = min_price.get("regular_price", {}).get("value")
            final_price = min_price.get("final_price", {}).get("value")
            if product.get("variants"):
                for v in product["variants"]:
                    if final_price is not None:
                        v["price"] = str(final_price)
                    if regular_price and regular_price != final_price:
                        v["compare_at_price"] = str(regular_price)

            # Update images from Kuwait
            media = mp.get("media_gallery", [])
            if media:
                product["images"] = [{"src": m["url"], "alt": m.get("label", "")} for m in media if m.get("url")]

            shopify_products.append(product)

        return shopify_products

    def _find_spain_product_match(self, name, url_key):
        """Try to match a Kuwait product with a Spain product."""
        # Exact handle match
        for sp in self.spain_products:
            if sp.get("handle") == url_key:
                return sp

        # Normalized handle match
        normalized = slugify(url_key)
        for sp in self.spain_products:
            if slugify(sp.get("handle", "")) == normalized:
                return sp

        return None

    def _update_metafield(self, product, namespace, key, value):
        """Update or add a metafield on a product."""
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

    def _scrape_products_html(self, locale_prefix):
        """Fallback: scrape products from HTML pages."""
        print(f"  HTML scraping products with prefix: {locale_prefix or '(default)'}")
        products = []

        # First, discover product URLs from collection pages
        product_urls = self._discover_product_urls(locale_prefix)
        print(f"  Discovered {len(product_urls)} product URLs")

        for i, url in enumerate(product_urls):
            print(f"  [{i+1}/{len(product_urls)}] Scraping: {url}")
            product = self._scrape_single_product_html(url, locale_prefix)
            if product:
                products.append(product)

        return products

    def _discover_product_urls(self, locale_prefix):
        """Discover product URLs from collection/category pages."""
        urls = set()
        # Try common collection pages
        collection_paths = [
            "/hair-care", "/hair-care.html",
            "/shampoo", "/conditioner", "/serum",
            "/collections/all", "/catalog/category/view",
        ]

        for path in collection_paths:
            page = self.fetch_page(f"{BASE_URL}{locale_prefix}{path}")
            if not page:
                continue

            # Find all product links
            all_links = page.css('a[href]')
            for link in all_links:
                href = link.attrib.get('href', '')
                if href and '.html' in href and '/product' not in href.split('/')[-1].startswith('category'):
                    if any(x not in href for x in ['/customer/', '/checkout/', '/cart/']):
                        urls.add(href if href.startswith('http') else f"{BASE_URL}{href}")

        return sorted(urls)

    def _scrape_single_product_html(self, url, locale_prefix):
        """Scrape a single product page."""
        page = self.fetch_page(url)
        if not page:
            return None

        try:
            title = ""
            h1 = page.css('h1')
            if h1:
                title = h1[0].text.strip() if h1[0].text else ""

            if not title:
                return None

            # Extract product data
            body_html = ""
            desc = page.css('[class*="description"], [class*="Description"], .product-info')
            if desc:
                body_html = desc[0].html_content if hasattr(desc[0], 'html_content') else str(desc[0])

            # Extract URL key as handle
            handle = url.rstrip('/').split('/')[-1].replace('.html', '')

            # Try to match with Spain product
            spain_product = self._find_spain_product_match(title, handle)

            product = dict(spain_product) if spain_product else {
                "id": hash(handle) % 10**9,
                "handle": handle,
                "title": "",
                "body_html": "",
                "vendor": "TARA",
                "product_type": "",
                "tags": "",
                "status": "active",
                "variants": [{"title": "Default Title", "price": "0", "sku": ""}],
                "options": [],
                "metafields": [],
                "images": [],
            }

            product["title"] = title
            if body_html:
                product["body_html"] = body_html

            # Extract price
            price_el = page.css('[class*="price"], .price')
            if price_el:
                price_text = price_el[0].text.strip() if price_el[0].text else ""
                price_num = re.sub(r'[^\d.]', '', price_text)
                if price_num and product.get("variants"):
                    product["variants"][0]["price"] = price_num

            # Extract images
            img_els = page.css('.product-media img, [class*="gallery"] img, [class*="Gallery"] img')
            if img_els:
                product["images"] = []
                for img in img_els:
                    src = img.attrib.get('src', '') or img.attrib.get('data-src', '')
                    if src and 'placeholder' not in src:
                        product["images"].append({"src": src})

            return product

        except Exception as e:
            print(f"    Error parsing product: {e}")
            traceback.print_exc()
            return None

    # ------------------------------------------------------------------
    # Collection scraping
    # ------------------------------------------------------------------
    def scrape_collections(self):
        """Scrape collections/categories in both EN and AR."""
        print("\n" + "=" * 60)
        print("SCRAPING COLLECTIONS")
        print("=" * 60)

        # Try GraphQL
        print("\n--- English collections ---")
        en_cats = self._graphql_categories(store_code="kw_en")
        if en_cats:
            en_collections = self._map_magento_categories_to_shopify(en_cats)
            save_json(en_collections, os.path.join(OUTPUT_DIR_EN, "collections.json"))
            print(f"  Saved {len(en_collections)} English collections")
        else:
            # Copy Spain collections as fallback
            print("  GraphQL failed, copying Spain collections as placeholder")
            save_json(self.spain_collections, os.path.join(OUTPUT_DIR_EN, "collections.json"))

        print("\n--- Arabic collections ---")
        ar_cats = self._graphql_categories(store_code="kw_ar")
        if not ar_cats:
            ar_cats = self._graphql_categories(store_code=None)
        if ar_cats:
            ar_collections = self._map_magento_categories_to_shopify(ar_cats)
            save_json(ar_collections, os.path.join(OUTPUT_DIR_AR, "collections.json"))
            print(f"  Saved {len(ar_collections)} Arabic collections")
        else:
            print("  GraphQL failed, copying Spain collections as placeholder")
            save_json(self.spain_collections, os.path.join(OUTPUT_DIR_AR, "collections.json"))

    def _map_magento_categories_to_shopify(self, categories):
        """Convert Magento categories to Shopify collection format."""
        spain_by_handle = {c.get("handle", ""): c for c in self.spain_collections}
        collections = []

        for cat in categories:
            url_key = cat.get("url_key", "")

            # Match with Spain collection
            spain_coll = spain_by_handle.get(url_key)
            if spain_coll:
                coll = dict(spain_coll)
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
    # Page scraping
    # ------------------------------------------------------------------
    def scrape_pages(self):
        """Scrape CMS pages in both EN and AR."""
        print("\n" + "=" * 60)
        print("SCRAPING PAGES")
        print("=" * 60)

        # For pages, we scrape HTML since Magento GraphQL requires knowing identifiers
        spain_by_handle = {p.get("handle", ""): p for p in self.spain_pages}

        en_pages = []
        ar_pages = []

        for handle, spain_page in spain_by_handle.items():
            print(f"\n  Page: {handle}")

            # Try English
            en_page = self._scrape_cms_page(handle, EN_PREFIX, spain_page)
            if en_page:
                en_pages.append(en_page)

            # Try Arabic
            ar_page = self._scrape_cms_page(handle, AR_PREFIX, spain_page)
            if ar_page:
                ar_pages.append(ar_page)

        if en_pages:
            save_json(en_pages, os.path.join(OUTPUT_DIR_EN, "pages.json"))
            print(f"\n  Saved {len(en_pages)} English pages")
        else:
            save_json(self.spain_pages, os.path.join(OUTPUT_DIR_EN, "pages.json"))

        if ar_pages:
            save_json(ar_pages, os.path.join(OUTPUT_DIR_AR, "pages.json"))
            print(f"  Saved {len(ar_pages)} Arabic pages")
        else:
            save_json(self.spain_pages, os.path.join(OUTPUT_DIR_AR, "pages.json"))

    def _scrape_cms_page(self, handle, locale_prefix, spain_page):
        """Scrape a single CMS page."""
        # Try common URL patterns
        urls_to_try = [
            f"{BASE_URL}{locale_prefix}/{handle}",
            f"{BASE_URL}{locale_prefix}/{handle}.html",
        ]

        for url in urls_to_try:
            page = self.fetch_page(url)
            if not page:
                continue

            h1 = page.css('h1')
            title = h1[0].text.strip() if h1 and h1[0].text else ""
            if not title:
                continue

            # Get main content
            content_selectors = [
                '.cms-content', '.page-content', '[class*="cms"]',
                'main', '.main-content', '#maincontent',
            ]
            body_html = ""
            for sel in content_selectors:
                els = page.css(sel)
                if els:
                    body_html = str(els[0])
                    break

            result = dict(spain_page)
            result["title"] = title
            if body_html:
                result["body_html"] = body_html
            return result

        return None

    # ------------------------------------------------------------------
    # Article/Blog scraping
    # ------------------------------------------------------------------
    def scrape_articles(self):
        """Scrape blog articles in both EN and AR."""
        print("\n" + "=" * 60)
        print("SCRAPING ARTICLES")
        print("=" * 60)

        # Copy Spain articles and try to scrape translated versions
        en_articles = []
        ar_articles = []

        for article in self.spain_articles:
            handle = article.get("handle", "")
            blog_handle = article.get("_blog_handle", "blog")
            print(f"\n  Article: {handle}")

            # Try English
            en_article = self._scrape_article(handle, blog_handle, EN_PREFIX, article)
            en_articles.append(en_article if en_article else article)

            # Try Arabic
            ar_article = self._scrape_article(handle, blog_handle, AR_PREFIX, article)
            ar_articles.append(ar_article if ar_article else article)

        save_json(en_articles, os.path.join(OUTPUT_DIR_EN, "articles.json"))
        save_json(ar_articles, os.path.join(OUTPUT_DIR_AR, "articles.json"))
        print(f"\n  Saved {len(en_articles)} EN / {len(ar_articles)} AR articles")

        # Copy blogs
        save_json(self.spain_blogs, os.path.join(OUTPUT_DIR_EN, "blogs.json"))
        save_json(self.spain_blogs, os.path.join(OUTPUT_DIR_AR, "blogs.json"))

    def _scrape_article(self, handle, blog_handle, locale_prefix, spain_article):
        """Scrape a single article page."""
        urls_to_try = [
            f"{BASE_URL}{locale_prefix}/blog/{handle}",
            f"{BASE_URL}{locale_prefix}/{blog_handle}/{handle}",
            f"{BASE_URL}{locale_prefix}/blog/{handle}.html",
            f"{BASE_URL}{locale_prefix}/{handle}.html",
        ]

        for url in urls_to_try:
            page = self.fetch_page(url)
            if not page:
                continue

            h1 = page.css('h1')
            title = h1[0].text.strip() if h1 and h1[0].text else ""
            if not title:
                continue

            result = dict(spain_article)
            result["title"] = title

            # Get article body
            content_selectors = [
                'article', '.article-content', '.blog-content',
                '.post-content', '[class*="article"]',
            ]
            for sel in content_selectors:
                els = page.css(sel)
                if els:
                    result["body_html"] = str(els[0])
                    break

            return result

        return None

    # ------------------------------------------------------------------
    # Metaobject scraping (ingredients, benefits, FAQs from product pages)
    # ------------------------------------------------------------------
    def scrape_metaobjects(self):
        """Scrape metaobject content from product pages."""
        print("\n" + "=" * 60)
        print("SCRAPING METAOBJECTS")
        print("=" * 60)

        # Metaobjects (benefits, ingredients, FAQs) are typically embedded
        # in product pages on Magento. We'll try to extract them.
        # For now, copy Spain metaobjects as the base structure is the same.

        en_metaobjects = {}
        ar_metaobjects = {}

        for mo_type, type_data in self.spain_metaobjects.items():
            objects = type_data.get("objects", [])
            if not objects:
                en_metaobjects[mo_type] = type_data
                ar_metaobjects[mo_type] = type_data
                continue

            print(f"\n  Type '{mo_type}': {len(objects)} objects")

            # For ingredients and benefits, try to scrape from Kuwait pages
            if mo_type == "ingredient":
                en_objects, ar_objects = self._scrape_ingredients(objects)
            elif mo_type == "benefit":
                en_objects, ar_objects = self._scrape_benefits(objects)
            elif mo_type == "faq_entry":
                en_objects, ar_objects = self._scrape_faqs(objects)
            else:
                en_objects = objects
                ar_objects = objects

            en_metaobjects[mo_type] = {
                "definition": type_data["definition"],
                "objects": en_objects,
            }
            ar_metaobjects[mo_type] = {
                "definition": type_data["definition"],
                "objects": ar_objects,
            }

        save_json(en_metaobjects, os.path.join(OUTPUT_DIR_EN, "metaobjects.json"))
        save_json(ar_metaobjects, os.path.join(OUTPUT_DIR_AR, "metaobjects.json"))
        print(f"\n  Saved metaobjects to both EN and AR directories")

    def _scrape_ingredients(self, spain_ingredients):
        """Try to scrape ingredient pages from Kuwait site."""
        en_ingredients = []
        ar_ingredients = []

        for ing in spain_ingredients:
            handle = ing.get("handle", "")
            name_field = next((f for f in ing.get("fields", []) if f["key"] == "name"), None)
            name = name_field["value"] if name_field else handle

            # Try to fetch ingredient page
            en_ing = dict(ing)
            ar_ing = dict(ing)

            # Try EN ingredient page
            en_page = self.fetch_page(f"{BASE_URL}{EN_PREFIX}/ingredient/{handle}")
            if not en_page:
                en_page = self.fetch_page(f"{BASE_URL}{EN_PREFIX}/ingredient/{handle}.html")

            if en_page:
                en_ing = self._extract_ingredient_from_page(en_page, ing)

            # Try AR ingredient page
            ar_page = self.fetch_page(f"{BASE_URL}/ingredient/{handle}")
            if not ar_page:
                ar_page = self.fetch_page(f"{BASE_URL}/ingredient/{handle}.html")

            if ar_page:
                ar_ing = self._extract_ingredient_from_page(ar_page, ing)

            en_ingredients.append(en_ing)
            ar_ingredients.append(ar_ing)

        return en_ingredients, ar_ingredients

    def _extract_ingredient_from_page(self, page, spain_ingredient):
        """Extract ingredient data from a scraped page."""
        result = dict(spain_ingredient)
        result["fields"] = list(spain_ingredient.get("fields", []))

        h1 = page.css('h1')
        if h1 and h1[0].text:
            self._update_metaobject_field(result, "name", h1[0].text.strip())

        # Try to find description
        desc = page.css('[class*="description"], .content, main p')
        if desc:
            self._update_metaobject_field(result, "description", str(desc[0]))

        return result

    def _scrape_benefits(self, spain_benefits):
        """Benefits are usually embedded in product pages, copy as-is for now."""
        # Benefits don't typically have their own pages
        return list(spain_benefits), list(spain_benefits)

    def _scrape_faqs(self, spain_faqs):
        """FAQs are usually embedded in product pages, copy as-is for now."""
        return list(spain_faqs), list(spain_faqs)

    def _update_metaobject_field(self, obj, key, value):
        """Update a field in a metaobject."""
        for field in obj.get("fields", []):
            if field["key"] == key:
                field["value"] = value
                return
        obj.setdefault("fields", []).append({
            "key": key,
            "value": value,
            "type": "single_line_text_field",
        })


def main():
    parser = argparse.ArgumentParser(description="Scrape Kuwait TARA site for EN/AR content")
    parser.add_argument("--explore", action="store_true", help="Explore site structure first")
    parser.add_argument("--scrape", action="store_true", help="Scrape all content")
    parser.add_argument("--only", choices=["products", "collections", "pages", "articles", "metaobjects"],
                       help="Scrape only a specific content type")
    parser.add_argument("--stealth", action="store_true", help="Use stealth fetcher (slower but bypasses bot detection)")
    parser.add_argument("--delay", type=float, default=1.5, help="Delay between requests in seconds (default: 1.5)")
    args = parser.parse_args()

    global REQUEST_DELAY
    REQUEST_DELAY = args.delay

    scraper = KuwaitScraper(use_stealth=args.stealth)

    if args.explore:
        scraper.explore()
    elif args.scrape:
        scraper.scrape_all(only=args.only)
    else:
        print("Usage:")
        print("  python scrape_kuwait.py --explore          # Discover site structure")
        print("  python scrape_kuwait.py --scrape            # Scrape all content")
        print("  python scrape_kuwait.py --scrape --only products  # Scrape only products")
        print("  python scrape_kuwait.py --stealth --scrape  # Use stealth mode")


if __name__ == "__main__":
    main()
