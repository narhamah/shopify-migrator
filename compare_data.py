#!/usr/bin/env python3
"""Step 2a: Compare Spain export vs scraped EN/AR data to identify gaps.

Analyzes what's been scraped from Magento vs what exists in the Spain
Shopify export, and reports what still needs LLM translation.

Usage:
    python compare_data.py
"""

import json
import os
import sys


SPAIN_DIR = "data/spain_export"
EN_DIR = "data/english"
AR_DIR = "data/arabic"


def load_json(filepath):
    if not os.path.exists(filepath):
        return []
    with open(filepath, "r", encoding="utf-8") as f:
        return json.load(f)


def _image_filenames(product):
    """Extract normalized image filenames from a product."""
    fnames = set()
    for img in product.get("images", []):
        src = img.get("src", "")
        if src:
            fnames.add(src.split("?")[0].split("/")[-1].lower())
    return fnames


def compare_products(spain, scraped, label):
    """Compare Spain products vs scraped products by SKU, handle, and image URL."""
    spain_by_sku = {}
    for p in spain:
        for v in p.get("variants", []):
            sku = v.get("sku", "")
            if sku:
                spain_by_sku[sku] = p
    spain_by_handle = {p.get("handle", ""): p for p in spain}
    spain_by_image = {}
    for p in spain:
        for fname in _image_filenames(p):
            spain_by_image[fname] = p

    scraped_by_sku = {}
    for p in scraped:
        for v in p.get("variants", []):
            sku = v.get("sku", "")
            if sku:
                scraped_by_sku[sku] = p
    scraped_by_handle = {p.get("handle", ""): p for p in scraped}
    scraped_by_image = {}
    for p in scraped:
        for fname in _image_filenames(p):
            scraped_by_image[fname] = p

    # Find Spain products not in scraped
    missing = []
    matched = []
    match_methods = {"sku": 0, "handle": 0, "image": 0}
    for p in spain:
        skus = [v.get("sku", "") for v in p.get("variants", []) if v.get("sku")]
        handle = p.get("handle", "")

        found = False
        method = None
        for sku in skus:
            if sku in scraped_by_sku:
                found = True
                method = "sku"
                break
        if not found and handle in scraped_by_handle:
            found = True
            method = "handle"
        if not found:
            for fname in _image_filenames(p):
                if fname in scraped_by_image:
                    found = True
                    method = "image"
                    break

        if found:
            matched.append(p)
            match_methods[method] += 1
        else:
            missing.append(p)

    # Find scraped products not in Spain
    extra = []
    for p in scraped:
        skus = [v.get("sku", "") for v in p.get("variants", []) if v.get("sku")]
        handle = p.get("handle", "")

        found = False
        for sku in skus:
            if sku in spain_by_sku:
                found = True
                break
        if not found and handle in spain_by_handle:
            found = True
        if not found:
            for fname in _image_filenames(p):
                if fname in spain_by_image:
                    found = True
                    break

        if not found:
            extra.append(p)

    print(f"\n  {label} Products:")
    print(f"    Spain: {len(spain)}, Scraped: {len(scraped)}")
    print(f"    Matched: {len(matched)} (by SKU: {match_methods['sku']}, handle: {match_methods['handle']}, image: {match_methods['image']})")
    print(f"    Missing from scrape (need LLM translation): {len(missing)}")
    if missing:
        for p in missing:
            sku = p.get("variants", [{}])[0].get("sku", "?") if p.get("variants") else "?"
            print(f"      - {p.get('title', '?')[:60]} (handle: {p.get('handle')}, sku: {sku})")
    print(f"    Extra in scrape (not in Spain): {len(extra)}")
    if extra:
        for p in extra:
            print(f"      - {p.get('title', '?')[:60]} (handle: {p.get('handle')})")

    # Check which metafields are missing from scraped products
    print(f"\n    Metafield coverage (matched products):")
    from translator import PRODUCT_TRANSLATABLE_METAFIELDS
    mf_coverage = {k: 0 for k in PRODUCT_TRANSLATABLE_METAFIELDS}
    for p in matched:
        for mf in p.get("metafields", []):
            ns_key = f"{mf.get('namespace', '')}.{mf.get('key', '')}"
            if ns_key in mf_coverage:
                mf_coverage[ns_key] += 1

    for ns_key, count in sorted(mf_coverage.items()):
        status = "OK" if count == len(matched) else f"{count}/{len(matched)}"
        if count == 0:
            status = "MISSING — needs LLM"
        print(f"      {ns_key}: {status}")

    return missing, matched


def compare_collections(spain, scraped, label):
    spain_by_handle = {c.get("handle", ""): c for c in spain}
    scraped_by_handle = {c.get("handle", ""): c for c in scraped}

    # Also check if scraped collections contain Spain IDs (from SKU-overlap matching)
    scraped_by_id = {}
    for c in scraped:
        cid = str(c.get("id", ""))
        if cid:
            scraped_by_id[cid] = c

    matched = []
    missing = []
    matched_by_handle = 0
    matched_by_id = 0
    for c in spain:
        handle = c.get("handle", "")
        cid = str(c.get("id", ""))
        if handle in scraped_by_handle:
            matched.append(c)
            matched_by_handle += 1
        elif cid in scraped_by_id:
            matched.append(c)
            matched_by_id += 1
        else:
            missing.append(c)

    extra = [c for c in scraped if c.get("handle") not in spain_by_handle
             and str(c.get("id", "")) not in {str(s.get("id", "")) for s in spain}]

    print(f"\n  {label} Collections:")
    print(f"    Spain: {len(spain)}, Scraped: {len(scraped)}")
    print(f"    Matched: {len(matched)} (by handle: {matched_by_handle}, by SKU overlap: {matched_by_id})")
    print(f"    Missing (need LLM): {len(missing)}")
    if missing:
        for c in missing[:10]:
            print(f"      - {c.get('title', '?')[:60]} (handle: {c.get('handle')})")
        if len(missing) > 10:
            print(f"      ... and {len(missing) - 10} more")
    print(f"    Extra in scrape: {len(extra)}")

    return missing, matched


def compare_pages(spain, scraped, label):
    spain_by_handle = {p.get("handle", ""): p for p in spain}
    scraped_by_handle = {p.get("handle", ""): p for p in scraped}

    matched = [p for p in spain if p.get("handle") in scraped_by_handle]
    missing = [p for p in spain if p.get("handle") not in scraped_by_handle]

    print(f"\n  {label} Pages:")
    print(f"    Spain: {len(spain)}, Scraped: {len(scraped)}")
    print(f"    Matched: {len(matched)}")
    print(f"    Missing (need LLM): {len(missing)}")
    if missing:
        for p in missing:
            print(f"      - {p.get('title', '?')[:60]} (handle: {p.get('handle')})")

    return missing, matched


def compare_metaobjects(spain, scraped, label):
    from translator import METAOBJECT_TRANSLATABLE_FIELDS

    print(f"\n  {label} Metaobjects:")

    needs_translation = {}
    for mo_type, type_data in spain.items():
        spain_objs = type_data.get("objects", [])
        scraped_objs = []
        if isinstance(scraped, dict) and mo_type in scraped:
            scraped_objs = scraped[mo_type].get("objects", [])

        scraped_handles = {o.get("handle", "") for o in scraped_objs}
        has_translatable = mo_type in METAOBJECT_TRANSLATABLE_FIELDS

        # If this type has translatable fields, check if scraped data is
        # actually translated or just copied from Spain (still Spanish)
        if has_translatable and scraped_objs:
            # Check first object: if scraped text matches Spain text, it's a copy
            is_copy = _check_if_copy(spain_objs, scraped_objs, mo_type)
            if is_copy:
                status = f"{len(spain_objs)} total — ALL need LLM (scraped = Spanish copy)"
                needs_translation[mo_type] = spain_objs
                print(f"    {mo_type}: {status}")
                continue

        missing = [o for o in spain_objs if o.get("handle") not in scraped_handles]
        matched = len(spain_objs) - len(missing)

        if has_translatable and not scraped_objs:
            # No scraped data at all — all need translation
            status = f"{len(spain_objs)} total — ALL need LLM (no scraped data)"
            needs_translation[mo_type] = spain_objs
        elif missing:
            status = f"{matched}/{len(spain_objs)} matched, {len(missing)} need LLM"
            needs_translation[mo_type] = missing
        else:
            status = f"{matched}/{len(spain_objs)} matched"

        print(f"    {mo_type}: {len(spain_objs)} total — {status}")

    return needs_translation


def _check_if_copy(spain_objs, scraped_objs, mo_type):
    """Check if scraped metaobjects are just copies of Spain data (still Spanish)."""
    from translator import METAOBJECT_TRANSLATABLE_FIELDS
    translatable_keys = METAOBJECT_TRANSLATABLE_FIELDS.get(mo_type, set())

    # Build handle-based lookup
    spain_by_handle = {o.get("handle", ""): o for o in spain_objs}

    checks = 0
    matches = 0
    for scraped_obj in scraped_objs[:5]:  # Check first 5
        handle = scraped_obj.get("handle", "")
        spain_obj = spain_by_handle.get(handle)
        if not spain_obj:
            continue

        for s_field in spain_obj.get("fields", []):
            if s_field["key"] not in translatable_keys:
                continue
            s_val = s_field.get("value", "")
            if not s_val:
                continue

            # Find same field in scraped
            for sc_field in scraped_obj.get("fields", []):
                if sc_field["key"] == s_field["key"]:
                    checks += 1
                    if sc_field.get("value", "") == s_val:
                        matches += 1
                    break

    # If >80% of checked fields are identical, it's a copy
    if checks > 0 and matches / checks > 0.8:
        return True
    return False


def analyze_translation_cost(gaps, matched_product_count=0):
    """Estimate number of LLM calls needed."""
    from translator import (
        PRODUCT_TRANSLATABLE_METAFIELDS,
        METAOBJECT_TRANSLATABLE_FIELDS,
        ARTICLE_TRANSLATABLE_METAFIELDS,
    )

    total_fields = 0
    detail = {}

    # Unmatched products: title + body_html + tags + variants + metafields
    prod_count = len(gaps.get("products", []))
    if prod_count:
        fields = prod_count * (5 + len(PRODUCT_TRANSLATABLE_METAFIELDS))
        detail["products (unmatched)"] = f"{prod_count} products × ~{5 + len(PRODUCT_TRANSLATABLE_METAFIELDS)} fields = {fields}"
        total_fields += fields

    # Matched products: only metafields need translation (title/body scraped)
    if matched_product_count > 0:
        fields = matched_product_count * len(PRODUCT_TRANSLATABLE_METAFIELDS)
        detail["product metafields (matched)"] = f"{matched_product_count} products × {len(PRODUCT_TRANSLATABLE_METAFIELDS)} metafields = {fields}"
        total_fields += fields

    # Collections: title + body_html
    coll_count = len(gaps.get("collections", []))
    if coll_count:
        fields = coll_count * 2
        detail["collections"] = f"{coll_count} collections × 2 fields = {fields}"
        total_fields += fields

    # Pages: title + body_html
    page_count = len(gaps.get("pages", []))
    if page_count:
        fields = page_count * 2
        detail["pages"] = f"{page_count} pages × 2 fields = {fields}"
        total_fields += fields

    # Articles: title + body_html + summary + tags + metafields
    art_count = len(gaps.get("articles", []))
    if art_count:
        fields = art_count * (4 + len(ARTICLE_TRANSLATABLE_METAFIELDS))
        detail["articles"] = f"{art_count} articles × ~{4 + len(ARTICLE_TRANSLATABLE_METAFIELDS)} fields = {fields}"
        total_fields += fields

    # Metaobjects
    for mo_type, objs in gaps.get("metaobjects", {}).items():
        mo_fields = METAOBJECT_TRANSLATABLE_FIELDS.get(mo_type, set())
        if not mo_fields:
            continue
        count = len(objs) if isinstance(objs, list) else 0
        fields = count * len(mo_fields)
        detail[f"metaobjects_{mo_type}"] = f"{count} {mo_type} × {len(mo_fields)} fields = {fields}"
        total_fields += fields

    batch_size = 40
    print(f"\n  Estimated translation work:")
    for key, desc in detail.items():
        print(f"    {key}: {desc}")
    print(f"\n    Total fields to translate: {total_fields}")
    print(f"    With TOON batching: ~{max(1, (total_fields + batch_size - 1) // batch_size)} API calls (batches of ~{batch_size} fields)")
    print(f"    Without batching: {total_fields} API calls")


def main():
    print("=" * 60)
    print("DATA COMPLETENESS COMPARISON")
    print("Spain Export vs Scraped EN/AR")
    print("=" * 60)

    # Load Spain data
    spain_products = load_json(os.path.join(SPAIN_DIR, "products.json"))
    spain_collections = load_json(os.path.join(SPAIN_DIR, "collections.json"))
    spain_pages = load_json(os.path.join(SPAIN_DIR, "pages.json"))
    spain_articles = load_json(os.path.join(SPAIN_DIR, "articles.json"))
    spain_metaobjects = load_json(os.path.join(SPAIN_DIR, "metaobjects.json"))

    print(f"\nSpain Export: {len(spain_products)} products, {len(spain_collections)} collections, "
          f"{len(spain_pages)} pages, {len(spain_articles)} articles")
    if isinstance(spain_metaobjects, dict):
        mo_total = sum(len(v.get("objects", [])) for v in spain_metaobjects.values())
        print(f"  Metaobjects: {mo_total} across {len(spain_metaobjects)} types")

    if not spain_products:
        print("\nERROR: Spain export is empty. Run export_spain.py first.")
        sys.exit(1)

    gaps = {"en": {}, "ar": {}}

    for lang, directory, label in [("en", EN_DIR, "English"), ("ar", AR_DIR, "Arabic")]:
        print(f"\n{'=' * 60}")
        print(f"  {label.upper()} COMPARISON")
        print(f"{'=' * 60}")

        scraped_products = load_json(os.path.join(directory, "products.json"))
        scraped_collections = load_json(os.path.join(directory, "collections.json"))
        scraped_pages = load_json(os.path.join(directory, "pages.json"))
        scraped_articles = load_json(os.path.join(directory, "articles.json"))
        scraped_metaobjects = load_json(os.path.join(directory, "metaobjects.json"))

        if not scraped_products:
            print(f"\n  WARNING: No scraped {label} data found. Run scrape_kuwait.py --scrape first.")
            # All Spain data needs translation
            gaps[lang] = {
                "products": spain_products,
                "collections": spain_collections,
                "pages": spain_pages,
                "articles": spain_articles,
                "metaobjects": spain_metaobjects if isinstance(spain_metaobjects, dict) else {},
            }
            continue

        missing_products, matched_products = compare_products(spain_products, scraped_products, label)
        missing_collections, _ = compare_collections(spain_collections, scraped_collections, label)
        missing_pages, _ = compare_pages(spain_pages, scraped_pages, label)

        # Articles — always need translation (not in Magento)
        print(f"\n  {label} Articles:")
        print(f"    Spain: {len(spain_articles)} — ALL need LLM translation (no Magento source)")

        # Metaobjects — types with translatable fields always need LLM
        # (scraper copies Spain data, doesn't translate)
        if isinstance(spain_metaobjects, dict):
            missing_metaobjects = compare_metaobjects(spain_metaobjects, scraped_metaobjects, label)
        else:
            missing_metaobjects = {}

        gaps[lang] = {
            "products": missing_products,
            "matched_product_count": len(matched_products),
            "collections": missing_collections,
            "pages": missing_pages,
            "articles": spain_articles,  # All articles need translation
            "metaobjects": missing_metaobjects,
        }

    # Summary
    print(f"\n{'=' * 60}")
    print("TRANSLATION GAP SUMMARY")
    print(f"{'=' * 60}")

    for lang, label in [("en", "English"), ("ar", "Arabic")]:
        print(f"\n  {label}:")
        g = gaps[lang]
        print(f"    Products needing translation:    {len(g.get('products', []))}")
        print(f"    Collections needing translation: {len(g.get('collections', []))}")
        print(f"    Pages needing translation:       {len(g.get('pages', []))}")
        print(f"    Articles needing translation:    {len(g.get('articles', []))}")
        mo_gaps = g.get("metaobjects", {})
        if isinstance(mo_gaps, dict):
            for mo_type, objs in mo_gaps.items():
                if isinstance(objs, list):
                    print(f"    Metaobjects ({mo_type}):          {len(objs)}")

        analyze_translation_cost(g, matched_product_count=g.get("matched_product_count", 0))

    # Save gaps report
    report_path = os.path.join("data", "translation_gaps.json")
    os.makedirs("data", exist_ok=True)

    # Convert to serializable format (just IDs/handles for reference)
    report = {}
    for lang in ["en", "ar"]:
        g = gaps[lang]
        report[lang] = {
            "products": [{"id": p.get("id"), "handle": p.get("handle"), "title": p.get("title")}
                         for p in g.get("products", [])],
            "collections": [{"id": c.get("id"), "handle": c.get("handle"), "title": c.get("title")}
                            for c in g.get("collections", [])],
            "pages": [{"id": p.get("id"), "handle": p.get("handle"), "title": p.get("title")}
                      for p in g.get("pages", [])],
            "articles": [{"id": a.get("id"), "handle": a.get("handle"), "title": a.get("title")}
                         for a in g.get("articles", [])],
            "metaobjects": {
                mo_type: len(objs) if isinstance(objs, list) else 0
                for mo_type, objs in g.get("metaobjects", {}).items()
            },
        }
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    print(f"\n  Gap report saved to {report_path}")
    print(f"\n  Next: python translate_gaps.py --lang en")
    print(f"        python translate_gaps.py --lang ar")


if __name__ == "__main__":
    main()
