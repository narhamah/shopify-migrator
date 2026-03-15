#!/usr/bin/env python3
"""Offline comparison of Spain → English → Saudi migration data.

Analyzes local data files to produce a human-style "browsing both sites"
comparison report. No network access needed.

Usage:
    python compare_stores_offline.py
"""

import json
import os
from collections import Counter

from tara_migrate.core import load_json


def main():
    print("=" * 70)
    print("  STORE COMPARISON: Spain → English → Saudi")
    print("  (Offline analysis from local data files)")
    print("=" * 70)

    source_products = load_json("data/source_export/products.json") or []
    english_products = load_json("data/english/products.json") or []
    source_collections = load_json("data/source_export/collections.json") or []
    english_collections = load_json("data/english/collections.json") or []
    source_pages = load_json("data/source_export/pages.json") or []
    english_pages = load_json("data/english/pages.json") or []
    source_metaobjects = load_json("data/source_export/metaobjects.json") or []
    english_metaobjects = load_json("data/english/metaobjects.json") or []
    source_defs = load_json("data/source_export/metaobject_definitions.json") or []
    id_map = load_json("data/id_map.json") or {}

    source_blogs = load_json("data/source_export/blogs.json") or []
    english_blogs = load_json("data/english/blogs.json") or []
    source_articles = load_json("data/source_export/articles.json") or []
    english_articles = load_json("data/english/articles.json") or []

    total_issues = 0
    critical_issues = []
    warnings = []

    # ─── 1. Products ───
    print(f"\n{'='*70}")
    print("  1. PRODUCTS")
    print(f"{'='*70}")
    print(f"  Spain:   {len(source_products)} products")
    print(f"  English: {len(english_products)} products")
    migrated = id_map.get("products", {})
    print(f"  Saudi (migrated): {len(migrated)} products")

    # Find unmigrated products
    not_migrated = []
    for p in english_products:
        gid = p.get("admin_graphql_api_id", f"gid://shopify/Product/{p['id']}")
        if gid not in migrated and str(p["id"]) not in migrated:
            not_migrated.append(p)

    if not_migrated:
        msg = f"{len(not_migrated)} products NOT migrated to destination"
        critical_issues.append(msg)
        print(f"\n  CRITICAL: {msg}:")
        for p in not_migrated:
            mf_count = len(p.get("metafields", []))
            print(f"    - {p['handle']}: {p['title']} ({mf_count} metafields)")
        total_issues += len(not_migrated)

    # Products with zero metafields
    no_mf_products = [p for p in english_products if len(p.get("metafields", [])) == 0]
    if no_mf_products:
        msg = f"{len(no_mf_products)} products have ZERO metafields (no accordion sections, no ingredients)"
        critical_issues.append(msg)
        print(f"\n  CRITICAL: {msg}:")
        for p in no_mf_products:
            print(f"    - {p['handle']}: {p['title']}")
        total_issues += len(no_mf_products)

    # Products with minimal metafields (samples/accessories)
    minimal_mf = [p for p in english_products
                  if 0 < len(p.get("metafields", [])) <= 5
                  and "sample" not in p["handle"] and "tote" not in p["handle"]
                  and "beauty-case" not in p["handle"]]
    if minimal_mf:
        print(f"\n  WARNING: {len(minimal_mf)} products have very few metafields:")
        for p in minimal_mf:
            print(f"    - {p['handle']}: {p['title']} ({len(p['metafields'])} metafields)")

    # Check metafield coverage on main products
    accordion_fields = [
        "custom.tagline", "custom.ingredients", "custom.faqs",
        "custom.key_benefits_heading", "custom.key_benefits_content",
        "custom.clinical_results_heading", "custom.clinical_results_content",
        "custom.how_to_use_heading", "custom.how_to_use_content",
        "custom.whats_inside_heading", "custom.whats_inside_content",
        "custom.free_of_heading", "custom.free_of_content",
        "custom.awards_heading", "custom.awards_content",
        "custom.fragrance_heading", "custom.fragrance_content",
        "custom.size_ml",
    ]

    # Global missing fields across all products
    field_missing_count = Counter()
    main_products = [p for p in english_products if len(p.get("metafields", [])) > 5]
    for p in main_products:
        mf_keys = {f"{mf['namespace']}.{mf['key']}" for mf in p.get("metafields", [])}
        for field in accordion_fields:
            if field not in mf_keys:
                field_missing_count[field] += 1

    if field_missing_count:
        print(f"\n  Metafield gaps across {len(main_products)} main products:")
        for field, count in field_missing_count.most_common():
            pct = count / len(main_products) * 100
            severity = "MISSING FROM ALL" if pct == 100 else f"missing from {count}"
            print(f"    {field}: {severity} ({pct:.0f}%)")
            if pct == 100:
                warnings.append(f"'{field}' missing from ALL products")

    # ─── 2. Collections ───
    print(f"\n{'='*70}")
    print("  2. COLLECTIONS")
    print(f"{'='*70}")
    print(f"  Spain:   {len(source_collections)} collections")
    print(f"  English: {len(english_collections)} collections")

    # Check for duplicates
    eng_handles = [c["handle"] for c in english_collections]
    dupes = {h: c for h, c in Counter(eng_handles).items() if c > 1}
    if dupes:
        msg = f"Duplicate collection handles in English data: {dupes}"
        warnings.append(msg)
        print(f"\n  WARNING: {msg}")
        total_issues += len(dupes)

    eng_unique = set(eng_handles)
    sp_handles = set(c["handle"] for c in source_collections)

    # Expected collections from Build Guide
    expected_range_collections = [
        "black-garlic-ceramides", "onion-peptides", "date-multivitamin",
        "detox", "strawberry-nmf", "rosemary-peptides", "sage-multivitamin",
    ]
    expected_type_collections = [
        "shampoos", "conditioners", "hair-masks", "scalp-serums",
        "finishing-products", "accessories",
    ]
    expected_curation_collections = [
        "best-sellers", "new-arrivals", "award-winners",
    ]

    print("\n  Range collections (product lines):")
    for c in expected_range_collections:
        status = "OK" if c in eng_unique else "MISSING"
        icon = "+" if status == "OK" else "X"
        print(f"    {icon} {c}: {status}")
        if status == "MISSING":
            critical_issues.append(f"Range collection '{c}' missing")
            total_issues += 1

    print("\n  Type collections (product categories):")
    for c in expected_type_collections:
        status = "OK" if c in eng_unique else "MISSING"
        icon = "+" if status == "OK" else "X"
        print(f"    {icon} {c}: {status}")
        if status == "MISSING":
            warnings.append(f"Type collection '{c}' missing")

    print("\n  Curation collections:")
    for c in expected_curation_collections:
        status = "OK" if c in eng_unique else "MISSING"
        icon = "+" if status == "OK" else "X"
        print(f"    {icon} {c}: {status}")
        if status == "MISSING":
            warnings.append(f"Curation collection '{c}' missing")

    # Ingredient collections
    ingredient_cols = [c for c in english_collections
                       if c["handle"] not in expected_range_collections
                       and c["handle"] not in expected_type_collections
                       and c["handle"] not in expected_curation_collections
                       and c["handle"] not in ("tara-formula", "shop-hair", "shop-skin",
                                                "black-friday-sets", "by-product", "sets-capilares")]
    print(f"\n  Ingredient collections: {len(ingredient_cols)}")

    # ─── 3. Pages ───
    print(f"\n{'='*70}")
    print("  3. PAGES")
    print(f"{'='*70}")
    print(f"  Spain:   {len(source_pages)} pages")
    print(f"  English: {len(english_pages)} pages")

    sp_page_handles = {p["handle"]: p for p in source_pages}
    eng_page_handles = {p["handle"]: p for p in english_pages}

    expected_pages = {
        "ingredients": ("ingredients", "Ingredient library with hero grid + filterable cards"),
        "quiz": ("quiz", "Hair diagnosis quiz"),
        "quiz-results": ("quiz-results", "AI-powered quiz results page"),
        "contact": ("contact", "Contact form with map"),
        "store-locator": ("store-locator", "Store locator page"),
        "for-pharmacies": (None, "B2B pharmacy partners page"),
    }

    for handle, (template, desc) in expected_pages.items():
        if handle in eng_page_handles:
            page = eng_page_handles[handle]
            actual_template = page.get("template_suffix", "") or "(default)"
            body_len = len(page.get("body_html", "") or "")
            print(f"  + /{handle}: {desc} (template: {actual_template}, body: {body_len} chars)")
            if body_len == 0:
                warnings.append(f"Page '/{handle}' has empty body HTML")
        else:
            if handle in ("store-locator", "for-pharmacies"):
                print(f"  ? /{handle}: {desc} (optional, not present)")
            else:
                print(f"  X /{handle}: {desc} — MISSING")
                critical_issues.append(f"Required page '/{handle}' missing")
                total_issues += 1

    # ─── 4. Blogs & Articles ───
    print(f"\n{'='*70}")
    print("  4. BLOGS & ARTICLES")
    print(f"{'='*70}")
    print(f"  Spain:   {len(source_blogs)} blogs, {len(source_articles)} articles")
    print(f"  English: {len(english_blogs)} blogs, {len(english_articles)} articles")

    for b in english_blogs:
        handle = b.get("handle", "")
        title = b.get("title", "")
        matching = [a for a in english_articles if a.get("blog_handle") == handle or True]
        print(f"  + Blog '{handle}' ({title})")

    for a in english_articles:
        handle = a.get("handle", "")
        title = a.get("title", "")[:50]
        img_count = len(a.get("images", []) or a.get("image", []) or [])
        mf_count = len(a.get("metafields", []))
        print(f"    - {handle}: {title} ({mf_count} metafields)")

    # ─── 5. Metaobjects ───
    print(f"\n{'='*70}")
    print("  5. METAOBJECTS")
    print(f"{'='*70}")

    # metaobjects.json is dict keyed by type → {definition, objects}
    sp_by_type = {}
    if isinstance(source_metaobjects, dict):
        for t, data in source_metaobjects.items():
            sp_by_type[t] = data.get("objects", []) if isinstance(data, dict) else []
    elif isinstance(source_metaobjects, list):
        for mo in source_metaobjects:
            t = mo.get("type", "unknown")
            sp_by_type.setdefault(t, []).append(mo)

    eng_by_type = {}
    if isinstance(english_metaobjects, dict):
        for t, data in english_metaobjects.items():
            eng_by_type[t] = data.get("objects", []) if isinstance(data, dict) else []
    elif isinstance(english_metaobjects, list):
        for mo in english_metaobjects:
            t = mo.get("type", "unknown")
            eng_by_type.setdefault(t, []).append(mo)

    migrated_mo = {k: v for k, v in id_map.items() if k.startswith("metaobjects_")}

    core_types = ["ingredient", "benefit", "faq_entry", "blog_author", "store_location"]
    for t in core_types:
        sp_count = len(sp_by_type.get(t, []))
        eng_count = len(eng_by_type.get(t, []))
        mig_key = f"metaobjects_{t}"
        mig_count = len(migrated_mo.get(mig_key, {}))

        status = "OK" if mig_count >= eng_count and eng_count > 0 else (
            "NOT MIGRATED" if mig_count == 0 and eng_count > 0 else (
            "PARTIAL" if 0 < mig_count < eng_count else
            "NO DATA" if eng_count == 0 else "OK"
        ))
        icon = "+" if status == "OK" else "X" if status in ("NOT MIGRATED", "NO DATA") else "~"
        print(f"  {icon} {t}: Spain={sp_count}, English={eng_count}, Saudi={mig_count} [{status}]")

        if status == "NOT MIGRATED":
            critical_issues.append(f"Metaobject type '{t}' not migrated to destination")
            total_issues += 1
        elif status == "NO DATA":
            warnings.append(f"Metaobject type '{t}' has 0 entries in source data")

    # Check ingredient data quality
    print("\n  Ingredient data quality:")
    for mo in eng_by_type.get("ingredient", []):
        fields = {f["key"]: f.get("value") for f in mo.get("fields", [])}
        name = fields.get("name", mo["handle"])
        missing = []
        if not fields.get("description"):
            missing.append("description")
        if not fields.get("image") and not fields.get("icon"):
            missing.append("image+icon")
        if not fields.get("one_line_benefit"):
            missing.append("one_line_benefit")
        if missing:
            print(f"    ~ {name}: missing {', '.join(missing)}")

    # ─── 6. Navigation ───
    print(f"\n{'='*70}")
    print("  6. NAVIGATION (Expected from Build Guide)")
    print(f"{'='*70}")

    expected_main_menu = [
        ("Shop", "/collections", [
            "All Products", "Best Sellers", "Scalp Serums",
            "Black Garlic+ Ceramides", "Onion+ Peptides", "Date+ Multivitamin",
            "Detox", "Strawberry+ NMF", "Rosemary+ Peptides", "Sage+ Multivitamin",
        ]),
        ("Ingredients", "/pages/ingredients", []),
        ("Quiz", "/pages/quiz", []),
        ("Blog", "/blogs/journal", []),
    ]

    print("  Main Menu (expected):")
    for title, url, subs in expected_main_menu:
        print(f"    + {title} → {url}")
        for sub in subs:
            print(f"      - {sub}")

    expected_footer = ["About Us", "Contact", "Shipping & Returns", "Privacy Policy", "Terms of Service"]
    print("\n  Footer Menu (expected):")
    for item in expected_footer:
        print(f"    + {item}")

    print("\n  NOTE: Menu structure must be verified on live store (run compare_stores.py locally)")

    # ─── 7. Theme & Templates ───
    print(f"\n{'='*70}")
    print("  7. THEME TEMPLATES (Expected from Build Guide)")
    print(f"{'='*70}")

    required_templates = {
        "templates/index.json": "Homepage",
        "templates/product.json": "Product detail page",
        "templates/collection.json": "Collection page",
        "templates/page.ingredients.json": "Ingredients library",
        "templates/page.quiz.json": "Hair diagnosis quiz",
        "templates/page.quiz-results.json": "Quiz results",
        "templates/metaobject/ingredient.json": "Individual ingredient detail page",
        "templates/article.tara.json": "Custom article template",
        "templates/blog.tara.json": "Custom blog template",
    }

    print("  Required templates:")
    for template, desc in required_templates.items():
        print(f"    ? {template}: {desc}")

    print("\n  NOTE: Template verification requires live store access (run audit_store.py locally)")

    # ─── 8. Product Page Features ───
    print(f"\n{'='*70}")
    print("  8. PRODUCT PAGE FEATURES")
    print(f"{'='*70}")

    features = {
        "Tagline (subtitle under title)": "custom.tagline",
        "Ingredients carousel": "custom.ingredients",
        "FAQ accordion": "custom.faqs",
        "Key Benefits accordion": "custom.key_benefits_heading",
        "Clinical Results accordion": "custom.clinical_results_heading",
        "How to Use accordion": "custom.how_to_use_heading",
        "What's Inside accordion": "custom.whats_inside_heading",
        "Free Of accordion": "custom.free_of_heading",
        "Awards accordion": "custom.awards_heading",
        "Fragrance accordion": "custom.fragrance_heading",
        "Size display": "custom.size_ml",
    }

    # Check across main products
    for desc, field in features.items():
        has_count = sum(1 for p in main_products
                       if field in {f"{mf['namespace']}.{mf['key']}" for mf in p.get("metafields", [])})
        pct = has_count / len(main_products) * 100 if main_products else 0
        status = f"{has_count}/{len(main_products)} products ({pct:.0f}%)"
        icon = "+" if pct > 80 else "~" if pct > 0 else "X"
        print(f"  {icon} {desc}: {status}")

        if pct == 0:
            critical_issues.append(f"Product feature '{desc}' has 0% coverage")
        elif pct < 50:
            warnings.append(f"Product feature '{desc}' only {pct:.0f}% coverage")

    # ─── 9. Ingredient Pages ───
    print(f"\n{'='*70}")
    print("  9. INGREDIENT PAGES (Renderable)")
    print(f"{'='*70}")

    # Check Spain capabilities
    for d in source_defs:
        if d["type"] == "ingredient":
            caps = d.get("capabilities", {})
            sp_renderable = caps.get("renderable", {}).get("enabled", False)
            sp_publishable = caps.get("publishable", {}).get("enabled", False)
            print(f"  Spain: renderable={sp_renderable}, publishable={sp_publishable}")
            if not sp_renderable:
                print("    NOTE: Spain ALSO doesn't have renderable enabled!")
                print("    The ingredient cards are NOT clickable on source either")
                print("    Saudi needs enable_ingredient_pages.py to enable this")

    ingredients = eng_by_type.get("ingredient", [])
    print(f"  Ingredients: {len(ingredients)} total")
    print("  Expected URLs: /pages/ingredient/{handle}")
    print("  Sample URLs:")
    for ing in ingredients[:3]:
        fields = {f["key"]: f.get("value") for f in ing.get("fields", [])}
        name = fields.get("name", ing["handle"])
        print(f"    /pages/ingredient/{ing['handle']} → {name}")

    critical_issues.append(
        "Ingredient cards not clickable — theme section needs <a> tags linking to metaobject URLs"
    )

    # ─── 10. Missing App Integrations ───
    print(f"\n{'='*70}")
    print("  10. APP INTEGRATIONS (Manual)")
    print(f"{'='*70}")

    apps = [
        ("Klaviyo Reviews", "Product ratings + review list on product pages"),
        ("Shopify Subscriptions", "Subscribe & save widget on product pages"),
        ("Tolstoy", "Video carousel on homepage"),
        ("Klaviyo Email", "Newsletter signup, email campaigns, flows"),
    ]

    for app, desc in apps:
        print(f"  ? {app}: {desc}")
        warnings.append(f"App integration needed: {app}")

    # ─── 11. Store Configuration ───
    print(f"\n{'='*70}")
    print("  11. STORE CONFIGURATION (Manual)")
    print(f"{'='*70}")

    manual_items = [
        "Payment gateways (Tap, Mada, Apple Pay)",
        "Saudi VAT 15%",
        "Shipping zones/rates for Saudi Arabia",
        "Domain & DNS configuration",
        "Email notification templates",
        "Shopify Flows (export from source, import to destination)",
        "End-to-end checkout test",
    ]

    for item in manual_items:
        print(f"  ? {item}")

    # ─── Summary ───
    print(f"\n{'='*70}")
    print("  COMPARISON SUMMARY")
    print(f"{'='*70}")

    print(f"\n  CRITICAL ISSUES ({len(critical_issues)}):")
    for i, issue in enumerate(critical_issues, 1):
        print(f"    {i}. {issue}")

    print(f"\n  WARNINGS ({len(warnings)}):")
    for i, w in enumerate(warnings, 1):
        print(f"    {i}. {w}")

    print("\n  DATA COVERAGE:")
    print(f"    Products: {len(migrated)}/{len(english_products)} migrated to destination ({len(migrated)/len(english_products)*100:.0f}%)")
    print(f"    Collections: {len(english_collections)} in data ({len(dupes)} duplicates)")
    print(f"    Pages: {len(english_pages)} in data")
    print(f"    Ingredients: {len(eng_by_type.get('ingredient', []))} entries")
    print(f"    Benefits: {len(eng_by_type.get('benefit', []))} entries")
    print(f"    FAQ entries: {len(eng_by_type.get('faq_entry', []))} entries")
    print(f"    Articles: {len(english_articles)} articles")

    print("\n  NEXT STEPS:")
    print("    1. Run 'python compare_stores.py' locally to check live store rendering")
    print("    2. Run 'python audit_store.py' locally to verify destination store state")
    print(f"    3. Migrate the {len(not_migrated)} missing products")
    print("    4. Fix ingredient card links (theme section edit)")
    print("    5. Configure homepage product section with collection")
    print("    6. Install app integrations (Klaviyo, Subscriptions)")

    # Save report
    report = {
        "critical_issues": critical_issues,
        "warnings": warnings,
        "stats": {
            "products_english": len(english_products),
            "products_migrated": len(migrated),
            "products_not_migrated": len(not_migrated),
            "products_no_metafields": len(no_mf_products),
            "collections_english": len(english_collections),
            "collections_duplicates": len(dupes),
            "pages_english": len(english_pages),
            "ingredients": len(eng_by_type.get("ingredient", [])),
            "benefits": len(eng_by_type.get("benefit", [])),
            "faq_entries": len(eng_by_type.get("faq_entry", [])),
            "articles": len(english_articles),
        },
        "not_migrated_products": [p["handle"] for p in not_migrated],
        "no_metafield_products": [p["handle"] for p in no_mf_products],
    }
    os.makedirs("data", exist_ok=True)
    with open("data/offline_comparison_report.json", "w") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    print("\n  Report saved to data/offline_comparison_report.json")


if __name__ == "__main__":
    main()
