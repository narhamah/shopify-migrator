#!/usr/bin/env python3
"""Comprehensive Saudi store audit — API checks + storefront scraping.

Tests every aspect of the store against the Complete Store Build Guide:
- Products, collections, pages, blogs, articles
- Metaobject definitions and entries (ingredients, benefits, FAQ, authors)
- Metafield definitions (product + article)
- Theme templates and sections
- Homepage content completeness
- Ingredient pages (clickable + renderable)
- Navigation menus
- Locale configuration
- Storefront page rendering (scrape + check)
- Image accessibility
- SEO meta tags
- Product metafield population

Usage:
    python audit_store.py                  # Full audit
    python audit_store.py --section theme  # Just theme checks
    python audit_store.py --fix            # Auto-fix known issues
"""

import argparse
import json
import os
import re
import time

import requests
from dotenv import load_dotenv

from tara_migrate.client import ShopifyClient

# ─── Expected configuration from the Complete Store Build Guide ───

REQUIRED_METAOBJECT_TYPES = {
    "benefit": {"storefront_access": True, "renderable": False},
    "faq_entry": {"storefront_access": True, "renderable": False},
    "blog_author": {"storefront_access": True, "renderable": False},
    "ingredient": {"storefront_access": True, "renderable": True},
}

REQUIRED_PRODUCT_METAFIELDS = [
    "custom.tagline",
    "custom.ingredients",
    "custom.faqs",
    "custom.key_benefits_heading",
    "custom.key_benefits_content",
    "custom.clinical_results_heading",
    "custom.clinical_results_content",
    "custom.how_to_use_heading",
    "custom.how_to_use_content",
    "custom.whats_inside_heading",
    "custom.whats_inside_content",
    "custom.free_of_heading",
    "custom.free_of_content",
    "custom.awards_heading",
    "custom.awards_content",
    "custom.fragrance_heading",
    "custom.fragrance_content",
    "custom.size_ml",
]

REQUIRED_ARTICLE_METAFIELDS = [
    "custom.author",
    "custom.featured",
    "custom.hero_image",
    "custom.hero_caption",
    "custom.blog_summary",
    "custom.read_time_override",
    "custom.related_articles",
    "custom.related_products",
    "custom.ingredients",
    "custom.show_toc",
    "custom.is_hero",
    "custom.subtitle",
]

REQUIRED_PAGES = ["ingredients", "quiz", "quiz-results", "contact"]
OPTIONAL_PAGES = ["store-locator", "for-pharmacies"]

REQUIRED_TEMPLATES = [
    "templates/index.json",
    "templates/product.json",
    "templates/collection.json",
    "templates/page.ingredients.json",
    "templates/page.quiz.json",
    "templates/page.quiz-results.json",
    "templates/metaobject/ingredient.json",
]

OPTIONAL_TEMPLATES = [
    "templates/article.tara.json",
    "templates/blog.tara.json",
    "templates/page.store-locator.json",
    "templates/page.para-framacias.json",
    "templates/page.contact.json",
]

EXPECTED_MENU_ITEMS = {
    "main-menu": ["shop", "ingredients", "quiz", "blog"],
    "footer": ["contact"],
}

# ─── Scoring ───

class AuditReport:
    def __init__(self):
        self.sections = {}
        self.current_section = None

    def start_section(self, name):
        self.current_section = name
        self.sections[name] = {"pass": [], "fail": [], "warn": [], "info": []}
        print(f"\n{'='*60}")
        print(f"  {name}")
        print(f"{'='*60}")

    def ok(self, msg):
        self.sections[self.current_section]["pass"].append(msg)
        print(f"  ✓ {msg}")

    def fail(self, msg):
        self.sections[self.current_section]["fail"].append(msg)
        print(f"  ✗ {msg}")

    def warn(self, msg):
        self.sections[self.current_section]["warn"].append(msg)
        print(f"  ! {msg}")

    def info(self, msg):
        self.sections[self.current_section]["info"].append(msg)
        print(f"    {msg}")

    def summary(self):
        print(f"\n{'='*60}")
        print("  AUDIT SUMMARY")
        print(f"{'='*60}")
        total_pass = 0
        total_fail = 0
        total_warn = 0
        for section, results in self.sections.items():
            p = len(results["pass"])
            f = len(results["fail"])
            w = len(results["warn"])
            total_pass += p
            total_fail += f
            total_warn += w
            status = "PASS" if f == 0 else "FAIL"
            icon = "✓" if f == 0 else "✗"
            print(f"  {icon} {section}: {p} pass, {f} fail, {w} warn")

        print(f"\n  TOTAL: {total_pass} pass, {total_fail} fail, {total_warn} warnings")
        score = total_pass / max(total_pass + total_fail, 1) * 100
        print(f"  SCORE: {score:.0f}%")

        if total_fail > 0:
            print("\n  FAILURES:")
            for section, results in self.sections.items():
                for msg in results["fail"]:
                    print(f"    [{section}] {msg}")

        return total_fail == 0


# ─── Audit Functions ───

def audit_products(client, report):
    report.start_section("PRODUCTS")
    products = client.get_products()
    report.info(f"Found {len(products)} products")

    if len(products) == 0:
        report.fail("No products found on store")
        return products

    active = [p for p in products if p.get("status") == "active"]
    draft = [p for p in products if p.get("status") == "draft"]
    if active:
        report.ok(f"{len(active)} active products")
    if draft:
        report.warn(f"{len(draft)} draft products: {[p['title'][:30] for p in draft]}")

    for p in products:
        title = p.get("title", "")[:40]
        handle = p.get("handle", "")

        # Images
        if not p.get("images"):
            report.fail(f"No images: '{title}'")

        # Price
        for v in p.get("variants", []):
            price = float(v.get("price", "0"))
            if price == 0:
                report.fail(f"Zero price: '{title}' → {v.get('title', 'Default')}")

        # Spanish handle check
        spanish_words = ["champu", "acondicionador", "mascarilla", "serum-capilar", "rutina-"]
        if any(w in handle for w in spanish_words):
            report.fail(f"Spanish handle: '{title}' → /{handle}")

    report.ok("Product checks complete")
    return products


def audit_product_metafields(client, report, products):
    """Spot-check metafields on a few products."""
    report.start_section("PRODUCT METAFIELDS")

    if not products:
        report.fail("No products to check")
        return

    # Check up to 5 products
    sample = products[:5]
    for p in sample:
        title = p.get("title", "")[:30]
        pid = p["id"]
        mfs = client.get_metafields("products", pid)
        mf_keys = [f"{mf['namespace']}.{mf['key']}" for mf in mfs]

        # Check required fields
        has_tagline = "custom.tagline" in mf_keys
        has_ingredients = "custom.ingredients" in mf_keys

        if has_tagline:
            report.ok(f"'{title}' has tagline")
        else:
            report.warn(f"'{title}' missing custom.tagline metafield")

        if has_ingredients:
            report.ok(f"'{title}' has ingredients list")
        else:
            report.warn(f"'{title}' missing custom.ingredients metafield")

        # Count populated accordion fields
        accordion_keys = [k for k in mf_keys if "heading" in k or "content" in k]
        if accordion_keys:
            report.info(f"  '{title}' has {len(accordion_keys)} accordion fields")


def audit_collections(client, report):
    report.start_section("COLLECTIONS")
    collections = client.get_collections()
    report.info(f"Found {len(collections)} collections")

    if len(collections) == 0:
        report.fail("No collections found")
        return collections

    handles = [c.get("handle", "") for c in collections]

    # Check for best sellers
    if "best-sellers" in handles or "bestsellers" in handles:
        report.ok("Best Sellers collection exists")
    else:
        report.warn("No 'best-sellers' collection found")

    for c in collections:
        title = c.get("title", "")[:40]
        if not c.get("image"):
            report.warn(f"No image: collection '{title}'")

    report.ok(f"{len(collections)} collections found")
    return collections


def audit_pages(client, report):
    report.start_section("PAGES")
    pages = client.get_pages()
    page_handles = {p.get("handle", ""): p for p in pages}
    report.info(f"Found {len(pages)} pages: {list(page_handles.keys())}")

    for handle in REQUIRED_PAGES:
        if handle in page_handles:
            p = page_handles[handle]
            suffix = p.get("template_suffix", "")
            report.ok(f"Page '{handle}' exists (template: {suffix or 'default'})")
        else:
            report.fail(f"Required page missing: '{handle}'")

    for handle in OPTIONAL_PAGES:
        if handle in page_handles:
            report.info(f"Optional page '{handle}' exists")

    return pages


def audit_blogs(client, report):
    report.start_section("BLOGS & ARTICLES")
    blogs = client.get_blogs()
    report.info(f"Found {len(blogs)} blogs")

    if not blogs:
        report.warn("No blogs found")
        return blogs, 0

    total_articles = 0
    for b in blogs:
        articles = client.get_articles(b["id"])
        total_articles += len(articles)
        report.info(f"Blog '{b.get('title', '')}' (/{b.get('handle', '')}): {len(articles)} articles")

    if total_articles > 0:
        report.ok(f"{total_articles} total articles")
    else:
        report.warn("No articles found in any blog")

    return blogs, total_articles


def audit_menus(client, report):
    report.start_section("NAVIGATION MENUS")
    try:
        menus = client.get_menus()
        report.info(f"Found {len(menus)} menus")

        menu_handles = {m.get("handle", ""): m for m in menus}

        for expected_handle, expected_items in EXPECTED_MENU_ITEMS.items():
            if expected_handle in menu_handles:
                menu = menu_handles[expected_handle]
                items = menu.get("items", [])
                report.ok(f"Menu '{expected_handle}' exists with {len(items)} items")

                item_titles = [i.get("title", "").lower() for i in items]
                item_urls = [i.get("url", "").lower() for i in items]
                all_text = " ".join(item_titles + item_urls)

                for keyword in expected_items:
                    if keyword in all_text:
                        report.ok(f"  Menu has '{keyword}' item")
                    else:
                        report.warn(f"  Menu missing '{keyword}' item")

                # Print items for reference
                for item in items:
                    sub = item.get("items", [])
                    suffix = f" ({len(sub)} sub)" if sub else ""
                    report.info(f"  → {item.get('title', '')} [{item.get('url', '')}]{suffix}")
            else:
                report.fail(f"Menu '{expected_handle}' not found")

        if not menus:
            report.fail("No navigation menus configured")

    except Exception as e:
        report.fail(f"Error fetching menus: {e}")


def audit_metaobject_definitions(client, report):
    report.start_section("METAOBJECT DEFINITIONS")
    defs = client.get_metaobject_definitions()
    def_types = {d["type"]: d for d in defs}
    report.info(f"Found {len(defs)} metaobject definitions")

    for mo_type, expected in REQUIRED_METAOBJECT_TYPES.items():
        if mo_type in def_types:
            defn = def_types[mo_type]
            report.ok(f"Definition '{mo_type}' exists")

            # Check capabilities
            caps = defn.get("capabilities", {})
            if expected.get("renderable"):
                # Check if renderable is set
                renderable = caps.get("renderable", {})
                if renderable.get("enabled"):
                    report.ok(f"  '{mo_type}' has renderable capability")
                else:
                    report.fail(f"  '{mo_type}' missing renderable capability (ingredient pages won't work!)")
        else:
            report.fail(f"Missing metaobject definition: '{mo_type}'")


def audit_metaobject_entries(client, report):
    report.start_section("METAOBJECT ENTRIES")

    for mo_type in REQUIRED_METAOBJECT_TYPES:
        entries = client.get_metaobjects(mo_type)
        if entries:
            report.ok(f"{mo_type}: {len(entries)} entries")
        else:
            report.warn(f"{mo_type}: 0 entries")

        # For ingredients, check they have key fields populated
        if mo_type == "ingredient":
            missing_name = 0
            missing_image = 0
            for entry in entries:
                fields = {f["key"]: f.get("value") for f in entry.get("fields", [])}
                if not fields.get("name"):
                    missing_name += 1
                if not fields.get("image") and not fields.get("icon"):
                    missing_image += 1
            if missing_name:
                report.fail(f"  {missing_name} ingredients missing 'name' field")
            if missing_image:
                report.warn(f"  {missing_image} ingredients missing image/icon")


def audit_metafield_definitions(client, report):
    report.start_section("METAFIELD DEFINITIONS")

    # Product metafields
    prod_defs = client.get_metafield_definitions("PRODUCT")
    prod_keys = [f"{d['namespace']}.{d['key']}" for d in prod_defs]
    report.info(f"Found {len(prod_defs)} product metafield definitions")

    missing_prod = []
    for key in REQUIRED_PRODUCT_METAFIELDS:
        if key in prod_keys:
            pass  # Don't print every OK to keep output manageable
        else:
            missing_prod.append(key)

    if missing_prod:
        report.fail(f"Missing {len(missing_prod)} product metafield definitions:")
        for k in missing_prod:
            report.info(f"  - {k}")
    else:
        report.ok(f"All {len(REQUIRED_PRODUCT_METAFIELDS)} required product metafield definitions exist")

    # Article metafields
    art_defs = client.get_metafield_definitions("ARTICLE")
    art_keys = [f"{d['namespace']}.{d['key']}" for d in art_defs]
    report.info(f"Found {len(art_defs)} article metafield definitions")

    missing_art = []
    for key in REQUIRED_ARTICLE_METAFIELDS:
        if key in art_keys:
            pass
        else:
            missing_art.append(key)

    if missing_art:
        report.fail(f"Missing {len(missing_art)} article metafield definitions:")
        for k in missing_art:
            report.info(f"  - {k}")
    else:
        report.ok(f"All {len(REQUIRED_ARTICLE_METAFIELDS)} required article metafield definitions exist")


def audit_theme_templates(client, report):
    report.start_section("THEME TEMPLATES")
    theme_id = client.get_main_theme_id()
    if not theme_id:
        report.fail("No main theme found")
        return None

    assets = client.list_assets(theme_id)
    asset_keys = {a.get("key", "") for a in assets}
    report.info(f"Theme has {len(asset_keys)} assets")

    for template in REQUIRED_TEMPLATES:
        if template in asset_keys:
            report.ok(f"Template exists: {template}")
        else:
            report.fail(f"Template missing: {template}")

    for template in OPTIONAL_TEMPLATES:
        if template in asset_keys:
            report.info(f"Optional template exists: {template}")

    return theme_id


def audit_homepage(client, report, theme_id):
    report.start_section("HOMEPAGE CONTENT")
    if not theme_id:
        report.fail("No theme ID — cannot check homepage")
        return

    try:
        asset = client.get_asset(theme_id, "templates/index.json")
        template = json.loads(asset.get("value", "{}"))
    except Exception as e:
        report.fail(f"Cannot read templates/index.json: {e}")
        return

    sections = template.get("sections", {})
    order = template.get("order", [])
    report.info(f"Homepage has {len(sections)} sections, order has {len(order)} entries")

    # Check for key section types
    section_types = {}
    for sid in order:
        section = sections.get(sid, {})
        stype = section.get("type", "unknown")
        section_types[sid] = stype

    report.info(f"Section types: {list(section_types.values())}")

    # Check for product/collection section
    has_product_section = False
    product_section_empty = False
    for sid, section in sections.items():
        stype = section.get("type", "")
        settings = section.get("settings", {})
        heading = str(settings.get("heading", "") or settings.get("title", "") or "").lower()

        # Check if this looks like a product section
        if any(kw in stype.lower() for kw in ["product", "collection", "featured"]) or \
           any(kw in heading for kw in ["product", "best", "shop"]):
            has_product_section = True

            # Check if collection is assigned
            collection = settings.get("collection", "")
            if not collection:
                product_section_empty = True
                report.fail(f"Product section '{sid}' (type: {stype}) has NO collection assigned")
                report.info(f"  Heading: '{settings.get('heading', 'N/A')}'")
                report.info(f"  Settings: {json.dumps(settings, indent=4)[:300]}")
            else:
                report.ok(f"Product section '{sid}' has collection: {collection}")

    if not has_product_section:
        report.warn("No product/collection section found on homepage")
        # Dump all sections for debugging
        for sid in order:
            section = sections.get(sid, {})
            stype = section.get("type", "")
            heading = section.get("settings", {}).get("heading", "")
            report.info(f"  [{sid}] type={stype} heading='{heading}'")

    # Check for images
    image_count = 0
    empty_images = 0
    for sid, section in sections.items():
        for k, v in section.get("settings", {}).items():
            if "image" in k.lower():
                if v:
                    image_count += 1
                else:
                    empty_images += 1
        for bid, block in section.get("blocks", {}).items():
            for k, v in block.get("settings", {}).items():
                if "image" in k.lower():
                    if v:
                        image_count += 1
                    else:
                        empty_images += 1

    if image_count > 0:
        report.ok(f"{image_count} images configured on homepage")
    if empty_images > 0:
        report.warn(f"{empty_images} empty image slots on homepage")


def audit_ingredients_page(client, report, theme_id):
    """Check that the ingredients listing page and individual ingredient pages work."""
    report.start_section("INGREDIENT PAGES")

    if not theme_id:
        report.fail("No theme ID")
        return

    # 1. Check ingredients listing page template
    try:
        asset = client.get_asset(theme_id, "templates/page.ingredients.json")
        template = json.loads(asset.get("value", "{}"))
        report.ok("page.ingredients.json template exists")

        sections = template.get("sections", {})
        report.info(f"  {len(sections)} sections in ingredients page")

        # Check each section for link/URL handling
        for sid, section in sections.items():
            stype = section.get("type", "")
            report.info(f"  Section: {sid} (type: {stype})")

            # Read section liquid source to check for links
            try:
                section_asset = client.get_asset(theme_id, f"sections/{stype}.liquid")
                liquid = section_asset.get("value", "")

                has_url = "url" in liquid.lower()
                has_href = "href" in liquid.lower()
                has_metaobject_url = ".url" in liquid and "metaobject" in liquid.lower()

                if has_metaobject_url:
                    report.ok(f"  Section '{stype}' references metaobject URLs")
                elif has_href or has_url:
                    report.info(f"  Section '{stype}' has link/URL references")

                    # Look for specific URL patterns
                    url_lines = [line.strip() for line in liquid.split("\n")
                                if ("url" in line.lower() or "href" in line.lower())
                                and not line.strip().startswith("{%- comment")]
                    for line in url_lines[:5]:
                        report.info(f"    → {line[:120]}")
                else:
                    report.warn(f"  Section '{stype}' has NO link/URL references — cards may not be clickable!")
            except Exception as e:
                report.info(f"  Could not read section source for '{stype}': {e}")

    except Exception as e:
        report.fail(f"Cannot read page.ingredients.json: {e}")

    # 2. Check metaobject/ingredient template
    try:
        asset = client.get_asset(theme_id, "templates/metaobject/ingredient.json")
        template = json.loads(asset.get("value", "{}"))
        report.ok("metaobject/ingredient.json template exists")
    except Exception:
        report.fail("metaobject/ingredient.json template MISSING — individual ingredient pages won't render!")

    # 3. Check ingredient renderable capability
    defs = client.get_metaobject_definitions()
    for d in defs:
        if d["type"] == "ingredient":
            caps = d.get("capabilities", {})
            renderable = caps.get("renderable", {})
            publishable = caps.get("publishable", {})
            if renderable.get("enabled"):
                report.ok("Ingredient definition has renderable capability")
            else:
                report.fail("Ingredient definition does NOT have renderable capability")
            if publishable.get("enabled"):
                report.ok("Ingredient definition has publishable capability")
            else:
                report.fail("Ingredient definition does NOT have publishable capability")
            break

    # 4. Check a sample ingredient has a URL
    ingredients = client.get_metaobjects("ingredient")
    if ingredients:
        sample = ingredients[0]
        handle = sample.get("handle", "")
        expected_url = f"/pages/ingredient/{handle}"
        report.info(f"Sample ingredient URL: {expected_url}")
        report.info(f"Total ingredients: {len(ingredients)}")
    else:
        report.fail("No ingredient metaobjects found")


def audit_locales(client, report):
    report.start_section("LOCALES & LANGUAGES")
    try:
        locales = client.get_locales()
        for loc in locales:
            primary = " (PRIMARY)" if loc.get("primary") else ""
            published = "published" if loc.get("published") else "unpublished"
            report.info(f"  {loc['locale']}{primary} [{published}]")

        locale_codes = [l["locale"] for l in locales]
        if "en" in locale_codes:
            report.ok("English locale configured")
        else:
            report.fail("English locale not found")

        has_arabic = any(l.startswith("ar") for l in locale_codes)
        if has_arabic:
            report.ok("Arabic locale configured")
        else:
            report.warn("Arabic locale not configured")

    except Exception as e:
        report.fail(f"Error checking locales: {e}")


def audit_storefront_scrape(report, shop_url):
    """Scrape the public storefront and check key pages render correctly."""
    report.start_section("STOREFRONT RENDERING")

    # Build storefront URL (not admin)
    store_domain = shop_url.replace("https://", "").replace("http://", "").rstrip("/")
    base_url = f"https://{store_domain}"

    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    })

    pages_to_check = [
        ("/", "Homepage"),
        ("/pages/ingredients", "Ingredients Page"),
        ("/pages/quiz", "Quiz Page"),
        ("/pages/contact", "Contact Page"),
        ("/collections", "Collections"),
    ]

    for path, name in pages_to_check:
        url = f"{base_url}{path}"
        try:
            resp = session.get(url, timeout=15, allow_redirects=True)
            status = resp.status_code

            if status == 200:
                html = resp.text
                size_kb = len(html) / 1024

                report.ok(f"{name} ({path}): {status} OK ({size_kb:.0f} KB)")

                # Basic content checks
                if "<title>" in html:
                    title_match = re.search(r"<title>(.*?)</title>", html, re.DOTALL)
                    if title_match:
                        report.info(f"    Title: {title_match.group(1).strip()[:60]}")

                # Check for password protection
                if "password" in html.lower() and "enter store using password" in html.lower():
                    report.warn(f"  {name} appears to be password-protected")

                # Ingredients page specific checks
                if path == "/pages/ingredients":
                    # Check for ingredient cards
                    ingredient_links = re.findall(r'href="[^"]*ingredient[^"]*"', html, re.IGNORECASE)
                    if ingredient_links:
                        report.ok(f"  Found {len(ingredient_links)} ingredient links on page")
                        for link in ingredient_links[:3]:
                            report.info(f"    {link}")
                    else:
                        report.fail("  NO ingredient links found on ingredients page — cards are not clickable!")

                    # Check for ingredient content
                    if "ingredient" in html.lower():
                        report.info("  Page contains 'ingredient' references")

                # Homepage specific checks
                if path == "/":
                    # Check for product cards
                    product_links = re.findall(r'href="/products/[^"]*"', html)
                    if product_links:
                        report.ok(f"  Homepage has {len(product_links)} product links")
                    else:
                        report.warn("  No product links found on homepage")

                    # Check for collection links
                    collection_links = re.findall(r'href="/collections/[^"]*"', html)
                    if collection_links:
                        report.ok(f"  Homepage has {len(collection_links)} collection links")

                    # Check for images
                    img_tags = re.findall(r"<img[^>]*>", html)
                    report.info(f"  Homepage has {len(img_tags)} <img> tags")

                    # Check for key sections
                    if "our best products" in html.lower() or "best sellers" in html.lower():
                        report.ok("  Homepage has products section heading")
                    else:
                        report.warn("  'Our Best Products' heading not found on homepage")

            elif status == 401 or status == 403:
                report.warn(f"{name} ({path}): {status} — store may be password-protected")
            else:
                report.fail(f"{name} ({path}): HTTP {status}")

        except requests.exceptions.RequestException as e:
            report.fail(f"{name} ({path}): Connection error — {e}")

    # Check a sample ingredient detail page
    try:
        ingredients = []
        # Try to find ingredient handles from the ingredients page HTML
        resp = session.get(f"{base_url}/pages/ingredients", timeout=15)
        if resp.status_code == 200:
            handles = re.findall(r'/pages/ingredient/([a-z0-9-]+)', resp.text)
            if handles:
                sample_handle = handles[0]
                detail_url = f"{base_url}/pages/ingredient/{sample_handle}"
                detail_resp = session.get(detail_url, timeout=15)
                if detail_resp.status_code == 200:
                    report.ok(f"Ingredient detail page works: /pages/ingredient/{sample_handle}")
                else:
                    report.fail(f"Ingredient detail page returns {detail_resp.status_code}: /pages/ingredient/{sample_handle}")
    except Exception as e:
        report.info(f"Could not test ingredient detail page: {e}")


def audit_image_accessibility(client, report, products):
    """Check that product images are accessible via CDN."""
    report.start_section("IMAGE ACCESSIBILITY")

    if not products:
        report.warn("No products to check images for")
        return

    session = requests.Session()
    checked = 0
    broken = 0

    # Check first 5 products' images
    for p in products[:5]:
        title = p.get("title", "")[:30]
        images = p.get("images", [])
        for img in images[:2]:  # Check first 2 images per product
            src = img.get("src", "")
            if src:
                try:
                    resp = session.head(src, timeout=10)
                    if resp.status_code == 200:
                        checked += 1
                    else:
                        broken += 1
                        report.fail(f"Broken image ({resp.status_code}): '{title}' → {src[:60]}")
                except Exception:
                    broken += 1
                    report.fail(f"Image unreachable: '{title}' → {src[:60]}")

    if checked > 0 and broken == 0:
        report.ok(f"All {checked} sampled images are accessible")
    elif checked > 0:
        report.warn(f"{checked} images OK, {broken} broken")


def audit_seo(client, report, products):
    """Check SEO meta tags on products."""
    report.start_section("SEO META TAGS")

    if not products:
        report.warn("No products to check")
        return

    has_seo = 0
    missing_seo = 0

    for p in products[:10]:
        pid = p["id"]
        title = p.get("title", "")[:30]
        try:
            mfs = client.get_metafields("products", pid)
            seo_keys = {mf["key"] for mf in mfs if mf.get("namespace") == "global"}
            if "title_tag" in seo_keys or "description_tag" in seo_keys:
                has_seo += 1
            else:
                missing_seo += 1
        except Exception:
            pass

    if has_seo > 0:
        report.ok(f"{has_seo}/{has_seo + missing_seo} sampled products have SEO tags")
    if missing_seo > 0:
        report.warn(f"{missing_seo} products missing SEO meta tags")


# ─── Fix Functions ───

def fix_homepage_products(client, report, theme_id):
    """Fix the empty product section on the homepage by assigning a collection."""
    print("\n--- FIX: Homepage Product Section ---")
    if not theme_id:
        print("  No theme ID — cannot fix")
        return

    asset = client.get_asset(theme_id, "templates/index.json")
    template = json.loads(asset.get("value", "{}"))
    sections = template.get("sections", {})

    # Find the product section with empty collection
    fixed = False
    for sid, section in sections.items():
        stype = section.get("type", "")
        settings = section.get("settings", {})
        heading = str(settings.get("heading", "") or "").lower()

        if any(kw in stype.lower() for kw in ["product", "collection", "featured"]) or \
           any(kw in heading for kw in ["product", "best"]):
            collection = settings.get("collection", "")
            if not collection:
                # Find best-sellers collection
                collections = client.get_collections()
                best_sellers = None
                for c in collections:
                    h = c.get("handle", "")
                    if h in ("best-sellers", "bestsellers", "all"):
                        best_sellers = h
                        break
                if not best_sellers and collections:
                    best_sellers = collections[0].get("handle", "")

                if best_sellers:
                    section.setdefault("settings", {})["collection"] = best_sellers
                    print(f"  SET {sid}.settings.collection = '{best_sellers}'")
                    fixed = True

    if fixed:
        template_str = json.dumps(template, ensure_ascii=False, indent=2)
        client.put_asset(theme_id, "templates/index.json", template_str)
        print("  Homepage template updated!")
    else:
        print("  No empty product section found to fix (or no collections available)")


# ─── Main ───

def main():
    load_dotenv()
    parser = argparse.ArgumentParser(description="Comprehensive Saudi store audit")
    parser.add_argument("--section", type=str, help="Run only a specific section")
    parser.add_argument("--fix", action="store_true", help="Auto-fix known issues")
    parser.add_argument("--no-scrape", action="store_true", help="Skip storefront scraping")
    args = parser.parse_args()

    shop_url = os.environ.get("SAUDI_SHOP_URL")
    access_token = os.environ.get("SAUDI_ACCESS_TOKEN")
    if not shop_url or not access_token:
        print("ERROR: SAUDI_SHOP_URL and SAUDI_ACCESS_TOKEN must be set in .env")
        return

    client = ShopifyClient(shop_url, access_token)
    report = AuditReport()

    print("=" * 60)
    print("  COMPREHENSIVE SAUDI STORE AUDIT")
    print("=" * 60)
    print(f"  Store: {shop_url}")
    print(f"  Mode: {'FIX' if args.fix else 'AUDIT ONLY'}")

    # Run all audit sections
    products = audit_products(client, report)
    audit_product_metafields(client, report, products)
    collections = audit_collections(client, report)
    audit_pages(client, report)
    audit_blogs(client, report)
    audit_menus(client, report)
    audit_metaobject_definitions(client, report)
    audit_metaobject_entries(client, report)
    audit_metafield_definitions(client, report)
    theme_id = audit_theme_templates(client, report)
    audit_homepage(client, report, theme_id)
    audit_ingredients_page(client, report, theme_id)
    audit_locales(client, report)
    audit_image_accessibility(client, report, products)
    audit_seo(client, report, products)

    if not args.no_scrape:
        audit_storefront_scrape(report, shop_url)

    # Apply fixes if requested
    if args.fix:
        fix_homepage_products(client, report, theme_id)

    # Print summary
    all_pass = report.summary()

    # Save detailed report
    report_data = {
        "store": shop_url,
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "sections": report.sections,
        "all_pass": all_pass,
    }
    with open("data/audit_report.json", "w", encoding="utf-8") as f:
        json.dump(report_data, f, indent=2, ensure_ascii=False)
    print("\n  Detailed report saved to data/audit_report.json")


if __name__ == "__main__":
    main()
