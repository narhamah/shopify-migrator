#!/usr/bin/env python3
"""Compare Spanish (source) and Saudi (destination) Shopify stores side-by-side.

Mimics a human browsing both stores and noting every difference:
- Pages, navigation, collections, products
- Product page features (metafields, images, accordions, ingredients)
- Theme templates, homepage sections
- Ingredient pages, blog articles
- Storefront rendering (public scrape)

Usage:
    python compare_stores.py                # Full comparison
    python compare_stores.py --api-only     # Skip storefront scraping
    python compare_stores.py --section nav  # Just navigation comparison
"""

import argparse
import json
import os
import re
import time
import requests

from dotenv import load_dotenv
from shopify_client import ShopifyClient
from utils import load_json, SPAIN_DIR, EN_DIR


# ─── Handle Mapping (Spain → English) ───

def _build_handle_map(spain_file, english_file, id_key="id"):
    """Build a {spanish_handle: english_handle} dict using shared IDs."""
    sp_items = load_json(spain_file)
    en_items = load_json(english_file)
    sp_by_id = {item[id_key]: item.get("handle", "") for item in sp_items}
    en_by_id = {item[id_key]: item.get("handle", "") for item in en_items}
    mapping = {}
    for item_id, sp_handle in sp_by_id.items():
        en_handle = en_by_id.get(item_id)
        if en_handle and sp_handle:
            mapping[sp_handle] = en_handle
    return mapping


def _build_metaobject_handle_map(mo_type):
    """Build a {spanish_handle: english_handle} dict for a metaobject type."""
    sp_data = load_json(os.path.join(SPAIN_DIR, "metaobjects.json"), default={})
    en_data = load_json(os.path.join(EN_DIR, "metaobjects.json"), default={})
    sp_objs = sp_data.get(mo_type, {}).get("objects", [])
    en_objs = en_data.get(mo_type, {}).get("objects", [])
    sp_by_id = {o["id"]: o.get("handle", "") for o in sp_objs}
    en_by_id = {o["id"]: o.get("handle", "") for o in en_objs}
    mapping = {}
    for obj_id, sp_handle in sp_by_id.items():
        en_handle = en_by_id.get(obj_id)
        if en_handle and sp_handle:
            mapping[sp_handle] = en_handle
    return mapping


def load_all_handle_maps():
    """Load all Spain→English handle mappings from local data files."""
    return {
        "products": _build_handle_map(
            os.path.join(SPAIN_DIR, "products.json"),
            os.path.join(EN_DIR, "products.json"),
        ),
        "collections": _build_handle_map(
            os.path.join(SPAIN_DIR, "collections.json"),
            os.path.join(EN_DIR, "collections.json"),
        ),
        "pages": _build_handle_map(
            os.path.join(SPAIN_DIR, "pages.json"),
            os.path.join(EN_DIR, "pages.json"),
        ),
        "articles": _build_handle_map(
            os.path.join(SPAIN_DIR, "articles.json"),
            os.path.join(EN_DIR, "articles.json"),
        ),
        "ingredients": _build_metaobject_handle_map("ingredient"),
    }


# ─── Report ───

class ComparisonReport:
    def __init__(self):
        self.sections = []
        self.current = None
        self.total_missing = 0
        self.total_broken = 0
        self.total_ok = 0

    def section(self, title):
        self.current = {"title": title, "items": []}
        self.sections.append(self.current)
        print(f"\n{'='*70}")
        print(f"  {title}")
        print(f"{'='*70}")

    def match(self, msg):
        self.current["items"].append(("MATCH", msg))
        self.total_ok += 1
        print(f"  ✓ {msg}")

    def missing(self, msg):
        self.current["items"].append(("MISSING", msg))
        self.total_missing += 1
        print(f"  ✗ MISSING: {msg}")

    def broken(self, msg):
        self.current["items"].append(("BROKEN", msg))
        self.total_broken += 1
        print(f"  ✗ BROKEN: {msg}")

    def diff(self, msg):
        self.current["items"].append(("DIFF", msg))
        print(f"  ~ DIFF: {msg}")

    def info(self, msg):
        self.current["items"].append(("INFO", msg))
        print(f"    {msg}")

    def summary(self):
        print(f"\n{'='*70}")
        print(f"  COMPARISON SUMMARY")
        print(f"{'='*70}")

        for s in self.sections:
            missing = sum(1 for t, _ in s["items"] if t == "MISSING")
            broken = sum(1 for t, _ in s["items"] if t == "BROKEN")
            diffs = sum(1 for t, _ in s["items"] if t == "DIFF")
            matches = sum(1 for t, _ in s["items"] if t == "MATCH")
            icon = "✓" if (missing + broken) == 0 else "✗"
            print(f"  {icon} {s['title']}: {matches} match, {missing} missing, {broken} broken, {diffs} diff")

        print(f"\n  TOTALS: {self.total_ok} matching, {self.total_missing} missing, {self.total_broken} broken")

        if self.total_missing + self.total_broken > 0:
            print(f"\n  ALL ISSUES:")
            for s in self.sections:
                for item_type, msg in s["items"]:
                    if item_type in ("MISSING", "BROKEN"):
                        print(f"    [{s['title']}] {msg}")

        return self.total_missing + self.total_broken == 0


# ─── API Comparisons ───

def compare_pages(spain, saudi, report, handle_map=None):
    handle_map = handle_map or {}
    report.section("PAGES")

    spain_pages = spain.get_pages()
    saudi_pages = saudi.get_pages()

    spain_handles = {p.get("handle", ""): p for p in spain_pages}
    saudi_handles = {p.get("handle", ""): p for p in saudi_pages}

    report.info(f"Spain: {len(spain_pages)} pages | Saudi: {len(saudi_pages)} pages")

    matched_saudi = set()
    for handle, sp in spain_handles.items():
        title = sp.get("title", "")
        template = sp.get("template_suffix", "") or "(default)"
        # Try direct handle match first, then mapped English handle
        en_handle = handle_map.get(handle, handle)
        sa = saudi_handles.get(handle) or saudi_handles.get(en_handle)
        matched_key = handle if handle in saudi_handles else en_handle

        if sa:
            matched_saudi.add(matched_key)
            sa_template = sa.get("template_suffix", "") or "(default)"
            label = f"/{handle}" if handle == matched_key else f"/{handle} → /{matched_key}"
            if template == sa_template:
                report.match(f"Page '{label}' exists with template '{template}'")
            else:
                report.diff(f"Page '{label}' template: Spain='{template}' vs Saudi='{sa_template}'")

            sp_body_len = len(sp.get("body_html", "") or "")
            sa_body_len = len(sa.get("body_html", "") or "")
            if sp_body_len > 0 and sa_body_len == 0:
                report.missing(f"Page '{label}' has empty body in Saudi (Spain has {sp_body_len} chars)")
        else:
            report.missing(f"Page '/{handle}' ({title}, template: {template})")

    for handle in saudi_handles:
        if handle not in spain_handles and handle not in matched_saudi:
            report.info(f"Saudi-only page: /{handle}")


def compare_collections(spain, saudi, report, handle_map=None):
    handle_map = handle_map or {}
    report.section("COLLECTIONS")

    spain_cols = spain.get_collections()
    saudi_cols = saudi.get_collections()

    spain_handles = {c.get("handle", ""): c for c in spain_cols}
    saudi_handles = {c.get("handle", ""): c for c in saudi_cols}

    report.info(f"Spain: {len(spain_cols)} collections | Saudi: {len(saudi_cols)} collections")

    missing_collections = []
    for handle, sc in spain_handles.items():
        title = sc.get("title", "")
        en_handle = handle_map.get(handle, handle)
        sa = saudi_handles.get(handle) or saudi_handles.get(en_handle)
        matched_key = handle if handle in saudi_handles else en_handle

        if sa:
            sp_img = bool(sc.get("image"))
            sa_img = bool(sa.get("image"))
            label = f"/{handle}" if handle == matched_key else f"/{handle} → /{matched_key}"
            if sp_img and not sa_img:
                report.diff(f"Collection '{label}': Spain has image, Saudi doesn't")
            report.match(f"Collection '{label}' ({title} → {sa.get('title', '')})")
        else:
            missing_collections.append((handle, title))
            report.missing(f"Collection '/{handle}' ({title})")

    if missing_collections:
        report.info(f"\n  {len(missing_collections)} collections missing in Saudi")


def compare_products(spain, saudi, report, handle_map=None):
    handle_map = handle_map or {}
    report.section("PRODUCTS")

    spain_prods = spain.get_products()
    saudi_prods = saudi.get_products()

    saudi_handles = {p.get("handle", ""): p for p in saudi_prods}

    report.info(f"Spain: {len(spain_prods)} products | Saudi: {len(saudi_prods)} products")

    # Deduplicate Spain products by handle (old/new handle pairs share same title)
    seen_handles = set()
    for sp in spain_prods:
        sp_title = sp.get("title", "")
        sp_handle = sp.get("handle", "")
        sp_images = len(sp.get("images", []))
        sp_variants = len(sp.get("variants", []))

        # Use English handle mapping first, then direct handle, then fuzzy title
        en_handle = handle_map.get(sp_handle)
        sa = None
        matched_label = ""

        if en_handle:
            sa = saudi_handles.get(en_handle)
            matched_label = f"/{sp_handle} → /{en_handle}"
            # Skip if we already matched this English handle from another Spanish handle
            if en_handle in seen_handles:
                continue
            seen_handles.add(en_handle)
        if not sa:
            sa = saudi_handles.get(sp_handle)
            if sa:
                matched_label = f"/{sp_handle}"
                if sp_handle in seen_handles:
                    continue
                seen_handles.add(sp_handle)
        if not sa:
            # Fuzzy title match as last resort
            for sa_p in saudi_prods:
                sa_title_lower = sa_p.get("title", "").lower()
                sp_title_lower = sp_title.lower()
                sp_words = set(sp_title_lower.split())
                sa_words = set(sa_title_lower.split())
                overlap = sp_words & sa_words
                if len(overlap) >= 2 or sp_title_lower in sa_title_lower or sa_title_lower in sp_title_lower:
                    sa = sa_p
                    matched_label = f"/{sp_handle} ~> /{sa.get('handle', '')}"
                    break

        if sa:
            sa_images = len(sa.get("images", []))
            sa_variants = len(sa.get("variants", []))
            sa_status = sa.get("status", "")

            report.match(f"Product '{sp_title[:40]}' → '{sa.get('title', '')[:40]}' ({matched_label})")

            if sp_images > 0 and sa_images == 0:
                report.broken(f"  '{sp_title[:30]}': Spain has {sp_images} images, Saudi has 0")
            elif sp_images != sa_images:
                report.diff(f"  '{sp_title[:30]}': image count Spain={sp_images} vs Saudi={sa_images}")

            if sp_variants != sa_variants:
                report.diff(f"  '{sp_title[:30]}': variant count Spain={sp_variants} vs Saudi={sa_variants}")

            if sa_status != "active":
                report.broken(f"  '{sp_title[:30]}': Saudi status is '{sa_status}' (not active)")
        else:
            report.missing(f"Product '{sp_title}' (/{sp_handle})")


def compare_product_features(spain, saudi, report, handle_map=None):
    """Deep comparison of product page features — metafields, accordions, ingredients."""
    handle_map = handle_map or {}
    report.section("PRODUCT PAGE FEATURES")

    spain_prods = spain.get_products()
    saudi_prods = saudi.get_products()
    saudi_handles = {p.get("handle", ""): p for p in saudi_prods}

    # Sample up to 5 products from Spain
    sample_spain = spain_prods[:5]

    accordion_fields = [
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

    for sp in sample_spain:
        sp_title = sp.get("title", "")[:30]
        sp_handle = sp.get("handle", "")

        # Get Spain metafields
        sp_mfs = spain.get_metafields("products", sp["id"])
        sp_mf_keys = {f"{mf['namespace']}.{mf['key']}": mf for mf in sp_mfs}

        # Find matching Saudi product via handle map first
        en_handle = handle_map.get(sp_handle)
        sa = saudi_handles.get(en_handle) if en_handle else None
        if not sa:
            sa = saudi_handles.get(sp_handle)
        if not sa:
            # Fuzzy title match as last resort
            for sa_p in saudi_prods:
                sa_title_lower = sa_p.get("title", "").lower()
                sp_title_lower = sp.get("title", "").lower()
                sp_words = set(sp_title_lower.split())
                sa_words = set(sa_title_lower.split())
                if len(sp_words & sa_words) >= 2 or sp_title_lower in sa_title_lower:
                    sa = sa_p
                    break

        if not sa:
            report.info(f"Cannot find Saudi match for '{sp_title}' — skipping metafield comparison")
            continue

        sa_title = sa.get("title", "")[:30]
        sa_mfs = saudi.get_metafields("products", sa["id"])
        sa_mf_keys = {f"{mf['namespace']}.{mf['key']}": mf for mf in sa_mfs}

        report.info(f"\n  Product: '{sp_title}' → '{sa_title}'")
        report.info(f"  Spain metafields: {len(sp_mf_keys)} | Saudi metafields: {len(sa_mf_keys)}")

        for field in accordion_fields:
            sp_val = sp_mf_keys.get(field, {}).get("value", "")
            sa_val = sa_mf_keys.get(field, {}).get("value", "")

            if sp_val and sa_val:
                report.match(f"  {field}: populated on both")
            elif sp_val and not sa_val:
                report.missing(f"  {field}: Spain has value, Saudi is EMPTY")
            elif not sp_val and not sa_val:
                pass  # Neither has it, fine
            elif not sp_val and sa_val:
                report.info(f"  {field}: Saudi has value, Spain doesn't (extra)")


def compare_metaobjects(spain, saudi, report, ingredient_handle_map=None):
    ingredient_handle_map = ingredient_handle_map or {}
    report.section("METAOBJECTS")

    sp_defs = spain.get_metaobject_definitions()
    sa_defs = saudi.get_metaobject_definitions()

    sp_types = {d["type"]: d for d in sp_defs}
    sa_types = {d["type"]: d for d in sa_defs}

    report.info(f"Spain: {len(sp_defs)} definitions | Saudi: {len(sa_defs)} definitions")

    for mo_type, sp_def in sp_types.items():
        if mo_type in sa_types:
            report.match(f"Definition '{mo_type}' exists on both stores")

            # Compare entry counts
            sp_entries = spain.get_metaobjects(mo_type)
            sa_entries = saudi.get_metaobjects(mo_type)

            report.info(f"  {mo_type}: Spain={len(sp_entries)} entries, Saudi={len(sa_entries)} entries")

            if len(sp_entries) > 0 and len(sa_entries) == 0:
                report.missing(f"  '{mo_type}': Saudi has 0 entries (Spain has {len(sp_entries)})")
            elif len(sp_entries) != len(sa_entries):
                report.diff(f"  '{mo_type}': entry count differs Spain={len(sp_entries)} vs Saudi={len(sa_entries)}")

            # Compare field definitions
            sp_fields = {f["key"] for f in sp_def.get("fieldDefinitions", [])}
            sa_fields = {f["key"] for f in sa_types[mo_type].get("fieldDefinitions", [])}
            missing_fields = sp_fields - sa_fields
            if missing_fields:
                report.missing(f"  '{mo_type}': Saudi missing fields: {missing_fields}")

            # Compare capabilities
            sp_caps = sp_def.get("capabilities", {})
            sa_caps = sa_types[mo_type].get("capabilities", {})

            sp_renderable = sp_caps.get("renderable", {}).get("enabled", False)
            sa_renderable = sa_caps.get("renderable", {}).get("enabled", False)
            if sp_renderable and not sa_renderable:
                report.broken(f"  '{mo_type}': Spain has renderable, Saudi doesn't")

            # For ingredients: check individual entries for missing data
            if mo_type == "ingredient":
                _compare_ingredient_entries(sp_entries, sa_entries, report, ingredient_handle_map)
        else:
            report.missing(f"Metaobject definition '{mo_type}'")


def _compare_ingredient_entries(sp_entries, sa_entries, report, handle_map=None):
    """Compare individual ingredient entries between stores."""
    handle_map = handle_map or {}
    sp_by_handle = {e["handle"]: e for e in sp_entries}
    sa_by_handle = {e["handle"]: e for e in sa_entries}

    missing_ingredients = []
    for handle, sp_ing in sp_by_handle.items():
        sp_fields = {f["key"]: f.get("value") for f in sp_ing.get("fields", [])}
        name = sp_fields.get("name", handle)

        # Try direct handle, then English mapped handle
        en_handle = handle_map.get(handle, handle)
        sa_ing = sa_by_handle.get(handle) or sa_by_handle.get(en_handle)

        if not sa_ing:
            missing_ingredients.append(name)
            continue

        sa_fields = {f["key"]: f.get("value") for f in sa_ing.get("fields", [])}

        # Check key fields
        for key in ["name", "description", "image", "icon", "one_line_benefit"]:
            sp_val = sp_fields.get(key)
            sa_val = sa_fields.get(key)
            if sp_val and not sa_val:
                report.diff(f"  Ingredient '{name}': field '{key}' empty in Saudi")

    if missing_ingredients:
        report.missing(f"  {len(missing_ingredients)} ingredients missing in Saudi: {missing_ingredients[:5]}")


def compare_navigation(spain, saudi, report):
    report.section("NAVIGATION MENUS")

    try:
        sp_menus = spain.get_menus()
    except Exception:
        sp_menus = []
        report.info("Cannot fetch Spain menus (may not have permissions)")

    try:
        sa_menus = saudi.get_menus()
    except Exception:
        sa_menus = []
        report.broken("Cannot fetch Saudi menus")
        return

    sp_by_handle = {m.get("handle", ""): m for m in sp_menus}
    sa_by_handle = {m.get("handle", ""): m for m in sa_menus}

    report.info(f"Spain: {len(sp_menus)} menus | Saudi: {len(sa_menus)} menus")

    for handle, sp_menu in sp_by_handle.items():
        sp_items = sp_menu.get("items", [])
        if handle in sa_by_handle:
            sa_menu = sa_by_handle[handle]
            sa_items = sa_menu.get("items", [])
            report.match(f"Menu '{handle}': Spain={len(sp_items)} items, Saudi={len(sa_items)} items")

            if len(sp_items) != len(sa_items):
                report.diff(f"Menu '{handle}' item count differs")

            # Compare individual menu items
            for sp_item in sp_items:
                sp_title = sp_item.get("title", "")
                sp_url = sp_item.get("url", "")
                sp_subs = sp_item.get("items", [])

                # Find matching item in Saudi
                sa_item = None
                for sa_i in sa_items:
                    sa_url = sa_i.get("url", "")
                    # Match by URL path (different domains but same path)
                    sp_path = sp_url.split(".com")[-1] if ".com" in sp_url else sp_url
                    sa_path = sa_url.split(".com")[-1] if ".com" in sa_url else sa_url
                    # Normalize: strip /en/ prefix, strip leading/trailing slashes
                    sp_path_norm = re.sub(r"^/en/", "/", sp_path).strip("/")
                    sa_path_norm = re.sub(r"^/en/", "/", sa_path).strip("/")
                    if sp_path_norm == sa_path_norm:
                        sa_item = sa_i
                        break

                if sa_item:
                    sa_title = sa_item.get("title", "")
                    sa_subs = sa_item.get("items", [])
                    report.info(f"  '{sp_title}' → '{sa_title}'")
                    if len(sp_subs) != len(sa_subs):
                        report.diff(f"  Sub-items: '{sp_title}' Spain={len(sp_subs)} vs Saudi={len(sa_subs)}")
                else:
                    report.missing(f"  Menu item '{sp_title}' ({sp_url})")

            # Show Saudi-only items
            for sa_item in sa_items:
                sa_title = sa_item.get("title", "")
                sa_url = sa_item.get("url", "")
                report.info(f"  Saudi menu: '{sa_title}' → {sa_url}")
        else:
            report.missing(f"Menu '{handle}' ({sp_menu.get('title', '')})")

    # Also show Saudi-only menus
    for handle in sa_by_handle:
        if handle not in sp_by_handle:
            menu = sa_by_handle[handle]
            items = menu.get("items", [])
            report.info(f"Saudi-only menu: '{handle}' ({len(items)} items)")
            for item in items:
                report.info(f"  → {item.get('title', '')} [{item.get('url', '')}]")


def compare_blogs(spain, saudi, report, article_handle_map=None):
    article_handle_map = article_handle_map or {}
    report.section("BLOGS & ARTICLES")

    sp_blogs = spain.get_blogs()
    sa_blogs = saudi.get_blogs()

    sp_by_handle = {b.get("handle", ""): b for b in sp_blogs}
    sa_by_handle = {b.get("handle", ""): b for b in sa_blogs}

    report.info(f"Spain: {len(sp_blogs)} blogs | Saudi: {len(sa_blogs)} blogs")

    for handle, sp_blog in sp_by_handle.items():
        sp_articles = spain.get_articles(sp_blog["id"])
        if handle in sa_by_handle:
            sa_blog = sa_by_handle[handle]
            sa_articles = saudi.get_articles(sa_blog["id"])
            report.match(f"Blog '{handle}': Spain={len(sp_articles)} articles, Saudi={len(sa_articles)} articles")

            if len(sp_articles) != len(sa_articles):
                report.diff(f"Blog '{handle}' article count: Spain={len(sp_articles)}, Saudi={len(sa_articles)}")

            # Compare individual articles using handle map
            sp_art_handles = {a.get("handle", ""): a for a in sp_articles}
            sa_art_handles = {a.get("handle", ""): a for a in sa_articles}
            for art_handle, sp_art in sp_art_handles.items():
                en_art_handle = article_handle_map.get(art_handle, art_handle)
                if art_handle in sa_art_handles:
                    report.match(f"Article '/{handle}/{art_handle}'")
                elif en_art_handle in sa_art_handles:
                    report.match(f"Article '/{handle}/{art_handle}' → '/{handle}/{en_art_handle}'")
                else:
                    report.missing(f"Article '/{handle}/{art_handle}' ({sp_art.get('title', '')[:40]})")
        else:
            report.missing(f"Blog '{handle}' ({sp_blog.get('title', '')})")


def compare_theme_templates(spain, saudi, report):
    report.section("THEME TEMPLATES")

    sp_theme_id = spain.get_main_theme_id()
    sa_theme_id = saudi.get_main_theme_id()

    if not sp_theme_id:
        report.info("Cannot access Spain theme")
        return
    if not sa_theme_id:
        report.broken("Saudi has no main theme")
        return

    sp_assets = spain.list_assets(sp_theme_id)
    sa_assets = saudi.list_assets(sa_theme_id)

    sp_keys = {a.get("key", "") for a in sp_assets}
    sa_keys = {a.get("key", "") for a in sa_assets}

    # Focus on templates
    sp_templates = sorted(k for k in sp_keys if k.startswith("templates/"))
    sa_templates = sorted(k for k in sa_keys if k.startswith("templates/"))

    report.info(f"Spain: {len(sp_templates)} templates | Saudi: {len(sa_templates)} templates")

    for t in sp_templates:
        if t in sa_templates:
            report.match(f"Template: {t}")
        else:
            report.missing(f"Template: {t}")

    # Check sections
    sp_sections = sorted(k for k in sp_keys if k.startswith("sections/"))
    sa_sections = sorted(k for k in sa_keys if k.startswith("sections/"))

    report.info(f"\nSpain: {len(sp_sections)} sections | Saudi: {len(sa_sections)} sections")

    missing_sections = []
    for s in sp_sections:
        if s not in sa_sections:
            missing_sections.append(s)

    if missing_sections:
        report.missing(f"{len(missing_sections)} sections missing in Saudi:")
        for s in missing_sections:
            report.info(f"  - {s}")
    else:
        report.match(f"All {len(sp_sections)} sections present in Saudi")

    # Check snippets
    sp_snippets = sorted(k for k in sp_keys if k.startswith("snippets/"))
    sa_snippets = sorted(k for k in sa_keys if k.startswith("snippets/"))

    report.info(f"\nSpain: {len(sp_snippets)} snippets | Saudi: {len(sa_snippets)} snippets")

    missing_snippets = [s for s in sp_snippets if s not in sa_snippets]
    if missing_snippets:
        report.missing(f"{len(missing_snippets)} snippets missing in Saudi:")
        for s in missing_snippets:
            report.info(f"  - {s}")
    else:
        report.match(f"All {len(sp_snippets)} snippets present in Saudi")


def compare_homepage_sections(spain, saudi, report):
    report.section("HOMEPAGE CONTENT")

    sp_theme_id = spain.get_main_theme_id()
    sa_theme_id = saudi.get_main_theme_id()

    if not sp_theme_id or not sa_theme_id:
        report.info("Cannot compare homepages (missing theme)")
        return

    try:
        sp_asset = spain.get_asset(sp_theme_id, "templates/index.json")
        sp_template = json.loads(sp_asset.get("value", "{}"))
    except Exception:
        report.info("Cannot read Spain homepage template")
        return

    try:
        sa_asset = saudi.get_asset(sa_theme_id, "templates/index.json")
        sa_template = json.loads(sa_asset.get("value", "{}"))
    except Exception:
        report.broken("Cannot read Saudi homepage template")
        return

    sp_sections = sp_template.get("sections", {})
    sa_sections = sa_template.get("sections", {})
    sp_order = sp_template.get("order", [])
    sa_order = sa_template.get("order", [])

    report.info(f"Spain: {len(sp_order)} sections | Saudi: {len(sa_order)} sections")

    # Compare section types (not IDs, as those may differ)
    sp_types = [sp_sections.get(sid, {}).get("type", "unknown") for sid in sp_order]
    sa_types = [sa_sections.get(sid, {}).get("type", "unknown") for sid in sa_order]

    report.info(f"Spain section types: {sp_types}")
    report.info(f"Saudi section types: {sa_types}")

    # Find types in Spain but not in Saudi
    sp_type_set = set(sp_types)
    sa_type_set = set(sa_types)

    for t in sp_type_set:
        if t in sa_type_set:
            report.match(f"Section type '{t}' present on both homepages")
        else:
            report.missing(f"Section type '{t}' on Spain homepage but NOT Saudi")

    # Check key content on Saudi homepage
    for sid in sa_order:
        section = sa_sections.get(sid, {})
        stype = section.get("type", "")
        settings = section.get("settings", {})
        blocks = section.get("blocks", {})
        heading = settings.get("heading", "") or settings.get("title", "") or ""

        # Count images
        img_count = 0
        empty_img = 0
        for k, v in settings.items():
            if "image" in k.lower():
                if v:
                    img_count += 1
                else:
                    empty_img += 1
        for bid, block in blocks.items():
            for k, v in block.get("settings", {}).items():
                if "image" in k.lower():
                    if v:
                        img_count += 1
                    else:
                        empty_img += 1

        # Check collection assignment
        collection = settings.get("collection", "")

        info_parts = [f"type={stype}"]
        if heading:
            info_parts.append(f"heading='{heading[:40]}'")
        if img_count or empty_img:
            info_parts.append(f"images={img_count} set, {empty_img} empty")
        if collection:
            info_parts.append(f"collection='{collection}'")

        report.info(f"  [{sid[:25]}] {', '.join(info_parts)}")

        # Flag issues
        if empty_img > 0:
            report.broken(f"  Section '{sid}' has {empty_img} empty image slots")

        # Check for product sections with no collection
        if any(kw in stype.lower() for kw in ["product", "collection", "featured"]):
            if not collection:
                report.broken(f"  Product section '{sid}' ('{heading}') has NO collection assigned!")


def compare_product_collections(spain, saudi, report):
    """Check that products are linked to collections on Saudi store."""
    report.section("PRODUCT-COLLECTION LINKS")

    sa_collections = saudi.get_collections()
    report.info(f"Saudi: {len(sa_collections)} collections")

    empty_collections = []
    for c in sa_collections:
        title = c.get("title", "")[:30]
        handle = c.get("handle", "")
        try:
            product_ids = saudi.get_collection_product_ids(c["id"])
            if not product_ids:
                empty_collections.append((handle, title))
            else:
                report.info(f"  {handle}: {len(product_ids)} products")
        except Exception:
            report.info(f"  {handle}: cannot check product count")

    if empty_collections:
        report.broken(f"{len(empty_collections)} Saudi collections have 0 products:")
        for handle, title in empty_collections:
            report.info(f"  - {handle} ({title})")
    else:
        report.match("All Saudi collections have products linked")


def compare_redirects(spain, saudi, report):
    report.section("URL REDIRECTS")
    try:
        sp_redirects = spain.get_redirects()
    except Exception:
        sp_redirects = []

    try:
        sa_redirects = saudi.get_redirects()
    except Exception:
        sa_redirects = []

    report.info(f"Spain: {len(sp_redirects)} redirects | Saudi: {len(sa_redirects)} redirects")

    if len(sp_redirects) > 0 and len(sa_redirects) == 0:
        report.missing(f"Saudi has no URL redirects (Spain has {len(sp_redirects)})")
    elif len(sa_redirects) > 0:
        report.match(f"Saudi has {len(sa_redirects)} URL redirects configured")


# ─── Storefront Scraping ───

def scrape_compare_storefronts(spain_url, saudi_url, report):
    """Scrape both public storefronts and compare rendering."""
    report.section("STOREFRONT RENDERING (Public Scrape)")

    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    })

    sp_domain = spain_url.replace("https://", "").replace("http://", "").rstrip("/")
    sa_domain = saudi_url.replace("https://", "").replace("http://", "").rstrip("/")

    pages_to_check = [
        ("/", "Homepage"),
        ("/collections", "Collections Index"),
        ("/pages/ingredients", "Ingredients Library"),
        ("/pages/quiz", "Hair Quiz"),
        ("/pages/contact", "Contact"),
        ("/blogs", "Blog"),
    ]

    for path, name in pages_to_check:
        sp_url = f"https://{sp_domain}{path}"
        sa_url_full = f"https://{sa_domain}{path}"

        sp_status, sp_html = _fetch_page(session, sp_url)
        sa_status, sa_html = _fetch_page(session, sa_url_full)

        report.info(f"\n  {name} ({path}):")
        report.info(f"    Spain: HTTP {sp_status} ({len(sp_html)//1024} KB)")
        report.info(f"    Saudi: HTTP {sa_status} ({len(sa_html)//1024} KB)")

        if sp_status == 200 and sa_status != 200:
            report.broken(f"{name}: Spain renders (200) but Saudi returns {sa_status}")
            continue
        elif sp_status != 200 and sa_status != 200:
            report.info(f"  Both stores return non-200 for {path} — may be password-protected")
            continue

        if sp_status == 200 and sa_status == 200:
            # Compare page content
            _compare_page_content(sp_html, sa_html, name, path, report)


def _fetch_page(session, url):
    """Fetch a page and return (status_code, html)."""
    try:
        resp = session.get(url, timeout=15, allow_redirects=True)
        return resp.status_code, resp.text
    except Exception as e:
        return 0, ""


def _compare_page_content(sp_html, sa_html, name, path, report):
    """Compare rendered HTML content between two pages."""

    # Check password protection
    for label, html in [("Spain", sp_html), ("Saudi", sa_html)]:
        if "enter store using password" in html.lower() or "password" in html.lower()[:500]:
            report.info(f"  {label} may be password-protected")

    # Compare key elements
    sp_imgs = len(re.findall(r"<img[^>]*>", sp_html))
    sa_imgs = len(re.findall(r"<img[^>]*>", sa_html))
    report.info(f"  Images: Spain={sp_imgs}, Saudi={sa_imgs}")
    if sp_imgs > 0 and sa_imgs < sp_imgs * 0.5:
        report.broken(f"{name}: Saudi has far fewer images ({sa_imgs} vs {sp_imgs})")

    # Compare links
    sp_product_links = set(re.findall(r'href="[^"]*?/products/([^"?#]*)"', sp_html))
    sa_product_links = set(re.findall(r'href="[^"]*?/products/([^"?#]*)"', sa_html))
    if sp_product_links:
        report.info(f"  Product links: Spain={len(sp_product_links)}, Saudi={len(sa_product_links)}")
        if sa_product_links:
            report.match(f"{name} has product links")
        else:
            report.broken(f"{name}: Spain has {len(sp_product_links)} product links, Saudi has NONE")

    sp_collection_links = set(re.findall(r'href="[^"]*?/collections/([^"?#]*)"', sp_html))
    sa_collection_links = set(re.findall(r'href="[^"]*?/collections/([^"?#]*)"', sa_html))
    if sp_collection_links:
        report.info(f"  Collection links: Spain={len(sp_collection_links)}, Saudi={len(sa_collection_links)}")

    # Ingredients page specific
    if path == "/pages/ingredients":
        sp_ingredient_links = re.findall(r'href="[^"]*ingredient[^"]*"', sp_html, re.IGNORECASE)
        sa_ingredient_links = re.findall(r'href="[^"]*ingredient[^"]*"', sa_html, re.IGNORECASE)

        report.info(f"  Ingredient links: Spain={len(sp_ingredient_links)}, Saudi={len(sa_ingredient_links)}")
        if sp_ingredient_links and not sa_ingredient_links:
            report.broken(f"Ingredients page: Spain has clickable ingredient cards, Saudi does NOT!")
        elif sa_ingredient_links:
            report.match(f"Ingredients page has {len(sa_ingredient_links)} clickable ingredient links")

    # Homepage specific
    if path == "/":
        # Check for key section headings
        sp_headings = re.findall(r"<h[1-6][^>]*>(.*?)</h[1-6]>", sp_html, re.DOTALL | re.IGNORECASE)
        sa_headings = re.findall(r"<h[1-6][^>]*>(.*?)</h[1-6]>", sa_html, re.DOTALL | re.IGNORECASE)

        sp_headings_clean = [re.sub(r"<[^>]+>", "", h).strip() for h in sp_headings if h.strip()]
        sa_headings_clean = [re.sub(r"<[^>]+>", "", h).strip() for h in sa_headings if h.strip()]

        report.info(f"  Headings on Spain homepage ({len(sp_headings_clean)}):")
        for h in sp_headings_clean[:10]:
            report.info(f"    - {h[:60]}")
        report.info(f"  Headings on Saudi homepage ({len(sa_headings_clean)}):")
        for h in sa_headings_clean[:10]:
            report.info(f"    - {h[:60]}")

    # Compare page title
    sp_title = re.search(r"<title>(.*?)</title>", sp_html, re.DOTALL)
    sa_title = re.search(r"<title>(.*?)</title>", sa_html, re.DOTALL)
    if sp_title:
        report.info(f"  Spain title: {sp_title.group(1).strip()[:60]}")
    if sa_title:
        report.info(f"  Saudi title: {sa_title.group(1).strip()[:60]}")


def scrape_product_page(spain_url, saudi_url, spain_client, saudi_client, report):
    """Scrape and compare a sample product page on both stores."""
    report.section("PRODUCT PAGE RENDERING")

    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    })

    sp_domain = spain_url.replace("https://", "").replace("http://", "").rstrip("/")
    sa_domain = saudi_url.replace("https://", "").replace("http://", "").rstrip("/")

    # Get a sample product from each store
    sp_products = spain_client.get_products()
    sa_products = saudi_client.get_products()

    if not sp_products:
        report.info("No Spain products to compare")
        return

    # Try to find a matching product
    sp_sample = sp_products[0]
    sp_handle = sp_sample.get("handle", "")

    sa_sample = None
    for sa_p in sa_products:
        sa_title_lower = sa_p.get("title", "").lower()
        sp_title_lower = sp_sample.get("title", "").lower()
        sp_words = set(sp_title_lower.split())
        sa_words = set(sa_title_lower.split())
        if len(sp_words & sa_words) >= 2:
            sa_sample = sa_p
            break

    if not sa_sample and sa_products:
        sa_sample = sa_products[0]

    if sa_sample:
        sa_handle = sa_sample.get("handle", "")
        sp_url = f"https://{sp_domain}/products/{sp_handle}"
        sa_url_full = f"https://{sa_domain}/products/{sa_handle}"

        sp_status, sp_html = _fetch_page(session, sp_url)
        sa_status, sa_html = _fetch_page(session, sa_url_full)

        report.info(f"Spain: /products/{sp_handle} → HTTP {sp_status} ({len(sp_html)//1024} KB)")
        report.info(f"Saudi: /products/{sa_handle} → HTTP {sa_status} ({len(sa_html)//1024} KB)")

        if sp_status == 200 and sa_status == 200:
            # Check for product page features

            # 1. Add to cart button
            sp_atc = bool(re.search(r'(add.to.cart|añadir)', sp_html, re.IGNORECASE))
            sa_atc = bool(re.search(r'(add.to.cart|añadir)', sa_html, re.IGNORECASE))
            if sa_atc:
                report.match("Add to cart button present")
            elif sp_atc:
                report.broken("Add to cart button missing on Saudi product page")

            # 2. Product images
            sp_prod_imgs = len(re.findall(r'product.*?<img|<img[^>]*product', sp_html, re.IGNORECASE))
            sa_prod_imgs = len(re.findall(r'product.*?<img|<img[^>]*product', sa_html, re.IGNORECASE))
            report.info(f"Product-related images: Spain={sp_prod_imgs}, Saudi={sa_prod_imgs}")

            # 3. Accordion sections (look for common accordion HTML patterns)
            sp_accordions = len(re.findall(r'(accordion|collapsible|details|disclosure)', sp_html, re.IGNORECASE))
            sa_accordions = len(re.findall(r'(accordion|collapsible|details|disclosure)', sa_html, re.IGNORECASE))
            report.info(f"Accordion/collapsible elements: Spain={sp_accordions}, Saudi={sa_accordions}")
            if sp_accordions > 0 and sa_accordions == 0:
                report.broken("Product page: Spain has accordion sections, Saudi has NONE")

            # 4. Price display
            sp_price = bool(re.search(r'(price|precio)', sp_html, re.IGNORECASE))
            sa_price = bool(re.search(r'(price|precio)', sa_html, re.IGNORECASE))
            if sa_price:
                report.match("Price displayed on product page")
            elif sp_price:
                report.broken("Price missing on Saudi product page")

            # 5. Ingredient references
            sp_ingredients = len(re.findall(r'ingredient', sp_html, re.IGNORECASE))
            sa_ingredients = len(re.findall(r'ingredient', sa_html, re.IGNORECASE))
            report.info(f"Ingredient references: Spain={sp_ingredients}, Saudi={sa_ingredients}")
            if sp_ingredients > 5 and sa_ingredients == 0:
                report.broken("Product page: Spain shows ingredient carousel, Saudi does NOT")

            # 6. Reviews
            sp_reviews = bool(re.search(r'(review|reseña|valoraci)', sp_html, re.IGNORECASE))
            sa_reviews = bool(re.search(r'(review|reseña|valoraci)', sa_html, re.IGNORECASE))
            if sp_reviews and not sa_reviews:
                report.missing("Product reviews section not present on Saudi (needs Klaviyo Reviews app)")
            elif sa_reviews:
                report.match("Reviews section present on product page")

            # 7. Subscription option
            sp_sub = bool(re.search(r'(subscri|suscri)', sp_html, re.IGNORECASE))
            sa_sub = bool(re.search(r'(subscri|suscri)', sa_html, re.IGNORECASE))
            if sp_sub and not sa_sub:
                report.missing("Subscription option not present on Saudi product page")
            elif sa_sub:
                report.match("Subscription option present")


def scrape_ingredient_detail(saudi_url, saudi_client, report):
    """Check a sample ingredient detail page on Saudi."""
    report.section("INGREDIENT DETAIL PAGES")

    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    })

    sa_domain = saudi_url.replace("https://", "").replace("http://", "").rstrip("/")

    ingredients = saudi_client.get_metaobjects("ingredient")
    if not ingredients:
        report.broken("No ingredients found on Saudi store")
        return

    report.info(f"Testing {min(3, len(ingredients))} ingredient detail pages...")

    for ing in ingredients[:3]:
        handle = ing.get("handle", "")
        fields = {f["key"]: f.get("value") for f in ing.get("fields", [])}
        name = fields.get("name", handle)

        url = f"https://{sa_domain}/pages/ingredient/{handle}"
        status, html = _fetch_page(session, url)

        if status == 200:
            # Check for meaningful content (not just empty template)
            has_name = name.lower() in html.lower() if name else False
            has_content = len(html) > 5000  # Meaningful page should have substantial HTML

            if has_name and has_content:
                report.match(f"Ingredient '{name}' page renders with content ({len(html)//1024} KB)")
            elif has_content:
                report.info(f"Ingredient '{name}' page renders ({len(html)//1024} KB) but name not found in HTML")
            else:
                report.broken(f"Ingredient '{name}' page is mostly empty ({len(html)//1024} KB)")
        elif status == 404:
            report.broken(f"Ingredient '{name}' returns 404 at {url}")
        else:
            report.broken(f"Ingredient '{name}' returns HTTP {status}")


# ─── Main ───

def main():
    load_dotenv()

    parser = argparse.ArgumentParser(description="Compare Spain ↔ Saudi Shopify stores")
    parser.add_argument("--api-only", action="store_true", help="Skip storefront scraping")
    parser.add_argument("--scrape-only", action="store_true", help="Only do storefront scraping")
    parser.add_argument("--section", type=str, help="Run only a specific section")
    args = parser.parse_args()

    spain_url = os.environ.get("SPAIN_SHOP_URL")
    spain_token = os.environ.get("SPAIN_ACCESS_TOKEN")
    saudi_url = os.environ.get("SAUDI_SHOP_URL")
    saudi_token = os.environ.get("SAUDI_ACCESS_TOKEN")

    if not all([spain_url, spain_token, saudi_url, saudi_token]):
        print("ERROR: Set SPAIN_SHOP_URL, SPAIN_ACCESS_TOKEN, SAUDI_SHOP_URL, SAUDI_ACCESS_TOKEN in .env")
        return

    spain = ShopifyClient(spain_url, spain_token)
    saudi = ShopifyClient(saudi_url, saudi_token)
    report = ComparisonReport()

    # Load Spain→English handle mappings from local translated data
    handle_maps = load_all_handle_maps()
    map_counts = {k: len(v) for k, v in handle_maps.items()}
    print(f"\n  Handle mappings loaded: {map_counts}")

    print("=" * 70)
    print("  STORE COMPARISON: Spain ↔ Saudi")
    print("=" * 70)
    print(f"  Spain: {spain_url}")
    print(f"  Saudi: {saudi_url}")
    print(f"  Mode:  {'API only' if args.api_only else 'Scrape only' if args.scrape_only else 'Full'}")

    if not args.scrape_only:
        # API-based comparisons
        compare_pages(spain, saudi, report, handle_maps.get("pages", {}))
        compare_collections(spain, saudi, report, handle_maps.get("collections", {}))
        compare_products(spain, saudi, report, handle_maps.get("products", {}))
        compare_product_features(spain, saudi, report, handle_maps.get("products", {}))
        compare_metaobjects(spain, saudi, report, handle_maps.get("ingredients", {}))
        compare_navigation(spain, saudi, report)
        compare_blogs(spain, saudi, report, handle_maps.get("articles", {}))
        compare_theme_templates(spain, saudi, report)
        compare_homepage_sections(spain, saudi, report)
        compare_product_collections(spain, saudi, report)
        compare_redirects(spain, saudi, report)

    if not args.api_only:
        # Storefront scraping
        scrape_compare_storefronts(spain_url, saudi_url, report)
        scrape_product_page(spain_url, saudi_url, spain, saudi, report)
        scrape_ingredient_detail(saudi_url, saudi, report)

    # Summary
    all_good = report.summary()

    # Save detailed report
    report_data = {
        "spain_store": spain_url,
        "saudi_store": saudi_url,
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "sections": [{"title": s["title"], "items": s["items"]} for s in report.sections],
        "totals": {
            "matching": report.total_ok,
            "missing": report.total_missing,
            "broken": report.total_broken,
        },
        "all_pass": all_good,
    }
    os.makedirs("data", exist_ok=True)
    with open("data/comparison_report.json", "w", encoding="utf-8") as f:
        json.dump(report_data, f, indent=2, ensure_ascii=False)
    print(f"\n  Detailed report saved to data/comparison_report.json")


if __name__ == "__main__":
    main()
