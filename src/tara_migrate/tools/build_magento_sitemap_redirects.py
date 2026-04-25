#!/usr/bin/env python3
"""Build and optionally apply Kuwait redirects from old Magento sitemaps.

Uses the old Kuwait EN/AR sitemap URLs as the source of truth for inbound
paths. If those sitemap URLs are no longer live, the script falls back to the
latest 200 snapshot from the Wayback Machine.

Outputs:
  - redirect list: data/<dest>/magento_sitemap_redirects.json
  - build audit:   data/<dest>/magento_sitemap_redirects_audit.json
  - apply audit:   data/<dest>/magento_sitemap_redirect_resolution_audit.json
  - live backup:   data/<dest>/live_redirects_backup_before_magento_sync.json
"""

from __future__ import annotations

import argparse
import os
from collections import defaultdict
from urllib.parse import urlparse
from xml.etree import ElementTree as ET

import requests
from dotenv import load_dotenv

from tara_migrate.client import ShopifyClient
from tara_migrate.core import config, load_json, save_json


DEFAULT_EN_SITEMAP_URL = "https://taraformula.com.kw/media/sitemap/sitemap_kw_en.xml"
DEFAULT_AR_SITEMAP_URL = "https://taraformula.com.kw/media/sitemap/sitemap_kw_ar.xml"
VERIFY_BASE_URL = "https://taraformula.com.kw"
WAYBACK_AVAILABLE_API = "https://archive.org/wayback/available"


EXPLICIT_ROUTE_MAP: dict[str, dict[str, str]] = {
    "hair-care": {
        "kind": "collection",
        "handle": "shop-hair",
        "confidence": "high",
        "reason": "Old Magento category",
    },
    "skin-care": {
        "kind": "collection",
        "handle": "shop-skin",
        "confidence": "high",
        "reason": "Old Magento category",
    },
    "systems": {
        "kind": "collection",
        "handle": "shop-hair",
        "confidence": "medium",
        "reason": "No exact systems collection exists on Shopify",
    },
    "newsletter-subscription": {
        "kind": "page",
        "handle": "newsletter-subscription",
        "confidence": "high",
        "reason": "Exact current page",
    },
    "newsletter-subscription-en": {
        "kind": "page",
        "handle": "newsletter-subscription",
        "confidence": "high",
        "reason": "Old localized page alias",
    },
    "newsletter-subscription-ar": {
        "kind": "page",
        "handle": "newsletter-subscription",
        "confidence": "high",
        "reason": "Old localized page alias",
    },
    "faq": {
        "kind": "page",
        "handle": "faq",
        "confidence": "high",
        "reason": "Exact current page",
    },
    "philosophy": {
        "kind": "page",
        "handle": "philosophy-en",
        "confidence": "high",
        "reason": "Current Shopify page handle differs",
    },
    "privacy-policy": {
        "kind": "page",
        "handle": "privacy-policy-en",
        "confidence": "high",
        "reason": "Current Shopify page handle differs",
    },
    "cookie-policy": {
        "kind": "page",
        "handle": "privacy-policy-en",
        "confidence": "medium",
        "reason": "Cookie policy page not present; closest legal page is privacy policy",
    },
    "terms-and-conditions": {
        "kind": "page",
        "handle": "terms-and-conditions-en",
        "confidence": "high",
        "reason": "Current Shopify page handle differs",
    },
    "contact-us": {
        "kind": "page",
        "handle": "contact",
        "confidence": "high",
        "reason": "Current Shopify page handle differs",
    },
    "returns-exchanges": {
        "kind": "page",
        "handle": "faq",
        "confidence": "medium",
        "reason": "Returns and exchanges content lives in FAQ",
    },
    "returns-exchanges-gcc-ar": {
        "kind": "page",
        "handle": "faq",
        "confidence": "medium",
        "reason": "Returns and exchanges content lives in FAQ",
    },
    "the-best-shampoo-for-hair-loss-according-to-dermatologists": {
        "kind": "blog",
        "handle": "journal",
        "confidence": "medium",
        "reason": "Legacy editorial page; closest current destination is journal",
    },
    "hair-loss-shampoo-system-that-actually-works": {
        "kind": "product",
        "handle": "hair-stimulation-system",
        "confidence": "medium",
        "reason": "Legacy editorial slug mapped to closest current anti-hair-loss system",
    },
    "tara-purify-clay-mask-transform-your-skin": {
        "kind": "product",
        "handle": "deep-cleansing-clay-mask",
        "confidence": "high",
        "reason": "Legacy editorial slug mapped to referenced product",
    },
    "tara-purify-clay-mask-transform-your-skin-ar": {
        "kind": "product",
        "handle": "deep-cleansing-clay-mask",
        "confidence": "high",
        "reason": "Legacy editorial slug mapped to referenced product",
    },
    "restore-hair-mask-is-back": {
        "kind": "product",
        "handle": "repairing-hair-mask",
        "confidence": "high",
        "reason": "Legacy editorial slug mapped to referenced product",
    },
    "restore-hair-mask-is-back-ar": {
        "kind": "product",
        "handle": "repairing-hair-mask",
        "confidence": "high",
        "reason": "Legacy editorial slug mapped to referenced product",
    },
    "electric-massager-brush-rosemary-remedy": {
        "kind": "collection",
        "handle": "accessories",
        "confidence": "medium",
        "reason": "Legacy accessory landing page",
    },
    "black-friday-2024": {
        "kind": "collection",
        "handle": "shop-hair",
        "confidence": "medium",
        "reason": "Old seasonal landing page; Shopify seasonal collection no longer exists",
    },
    "campaign-lander": {
        "kind": "home",
        "confidence": "low",
        "reason": "Obsolete campaign page",
    },
    "campaign-lander2": {
        "kind": "home",
        "confidence": "low",
        "reason": "Obsolete campaign page",
    },
    "cosmoprof": {
        "kind": "home",
        "confidence": "low",
        "reason": "Obsolete event page",
    },
    "month-of-giving": {
        "kind": "home",
        "confidence": "low",
        "reason": "Obsolete campaign page",
    },
    "onionremedylander": {
        "kind": "collection",
        "handle": "onion-peptides",
        "confidence": "medium",
        "reason": "Old onion remedy landing page",
    },
    "new-page-on-prod-for-training": {
        "kind": "home",
        "confidence": "low",
        "reason": "Training/test page",
    },
    "content-feature2": {
        "kind": "home",
        "confidence": "low",
        "reason": "Obsolete content feature page",
    },
    "lander-tester-page": {
        "kind": "home",
        "confidence": "low",
        "reason": "Test landing page",
    },
    "blog/category/hair-care": {
        "kind": "blog",
        "handle": "journal",
        "confidence": "medium",
        "reason": "Old Magento blog category page",
    },
    "blog/category/beauty": {
        "kind": "blog",
        "handle": "journal",
        "confidence": "medium",
        "reason": "Old Magento blog category page",
    },
    "blog/unlocking-hair-growth-potential-with-rosemary-remedy": {
        "kind": "blog",
        "handle": "journal",
        "confidence": "medium",
        "reason": "Legacy article not present in Shopify journal",
    },
    "onionremedytravelkit": {
        "kind": "collection",
        "handle": "onion-peptides",
        "confidence": "medium",
        "reason": "Old product removed; closest current line collection",
    },
    "onionremedytravelkitar": {
        "kind": "collection",
        "handle": "onion-peptides",
        "confidence": "medium",
        "reason": "Old product removed; closest current line collection",
    },
    "mulberry-silk-pillowcase": {
        "kind": "collection",
        "handle": "accessories",
        "confidence": "medium",
        "reason": "Old accessory removed",
    },
    "silicone-massage-comb": {
        "kind": "collection",
        "handle": "accessories",
        "confidence": "medium",
        "reason": "Old accessory removed",
    },
    "rosemary-remedy-anti-hair-fall-serum": {
        "kind": "collection",
        "handle": "scalp-serums",
        "confidence": "medium",
        "reason": "Old serum removed; closest current collection",
    },
    "sculpt-set": {
        "kind": "collection",
        "handle": "shop-skin",
        "confidence": "medium",
        "reason": "Old skin set removed; closest current collection",
    },
    "depuff-set": {
        "kind": "collection",
        "handle": "shop-skin",
        "confidence": "medium",
        "reason": "Old skin set removed; closest current collection",
    },
    "nourish-argan-oil-ar": {
        "kind": "product",
        "handle": "argan-oil",
        "confidence": "high",
        "reason": "Old Arabic localized product alias",
    },
}


def _slug_from_path(path: str) -> tuple[str, str]:
    normalized = (path or "/").rstrip("/") or "/"
    if normalized.startswith("/kw-en/"):
        return "en", normalized[len("/kw-en/"):]
    if normalized == "/kw-en":
        return "en", ""
    return "ar", normalized.lstrip("/")


def _target_path(locale: str, kind: str, handle: str | None = None, blog_handle: str | None = None) -> str:
    prefix = "/ar" if locale == "ar" else ""
    if kind == "home":
        return prefix or "/"
    if kind == "product":
        return f"{prefix}/products/{handle}"
    if kind == "collection":
        return f"{prefix}/collections/{handle}"
    if kind == "page":
        return f"{prefix}/pages/{handle}"
    if kind == "blog":
        return f"{prefix}/blogs/{handle}"
    if kind == "article":
        return f"{prefix}/blogs/{blog_handle}/{handle}"
    if kind == "ingredient":
        return f"{prefix}/pages/ingredient/{handle}"
    raise ValueError(f"Unsupported target kind: {kind}")


def _fetch_wayback_snapshot_url(url: str) -> tuple[str | None, str | None]:
    resp = requests.get(
        WAYBACK_AVAILABLE_API,
        params={"url": url},
        timeout=30,
        headers={"User-Agent": "Mozilla/5.0"},
    )
    resp.raise_for_status()
    data = resp.json()
    closest = data.get("archived_snapshots", {}).get("closest", {})
    if not closest.get("available") or closest.get("status") != "200":
        return None, None
    timestamp = closest.get("timestamp")
    if not timestamp:
        return None, None
    snapshot_url = f"https://web.archive.org/web/{timestamp}id_/{url}"
    return snapshot_url, timestamp


def _fetch_xml(url: str) -> tuple[bytes, dict[str, str]]:
    headers = {"User-Agent": "Mozilla/5.0"}
    live = requests.get(url, timeout=45, headers=headers)
    if live.status_code == 200 and b"<urlset" in live.content:
        return live.content, {"source": "live", "url": url}

    snapshot_url, timestamp = _fetch_wayback_snapshot_url(url)
    if not snapshot_url:
        raise RuntimeError(f"No live or archived sitemap available for {url}")

    archived = requests.get(snapshot_url, timeout=60, headers=headers)
    archived.raise_for_status()
    if b"<urlset" not in archived.content:
        raise RuntimeError(f"Archived sitemap did not return XML for {url}")
    return archived.content, {
        "source": "wayback",
        "url": snapshot_url,
        "timestamp": timestamp or "",
        "original_url": url,
    }


def _parse_sitemap_paths(xml_bytes: bytes) -> list[str]:
    root = ET.fromstring(xml_bytes)
    ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
    paths = []
    for el in root.findall("sm:url/sm:loc", ns):
        if not el.text:
            continue
        path = urlparse(el.text.strip()).path.rstrip("/") or "/"
        paths.append(path)
    return paths


def _load_current_shopify_data(base_dir: str) -> dict[str, object]:
    products = load_json(os.path.join(base_dir, "products.json"), default=[])
    collections = load_json(os.path.join(base_dir, "collections.json"), default=[])
    pages = load_json(os.path.join(base_dir, "pages.json"), default=[])
    blogs = load_json(os.path.join(base_dir, "blogs.json"), default=[])
    articles = load_json(os.path.join(base_dir, "articles.json"), default=[])
    metaobjects = load_json(os.path.join(base_dir, "metaobjects.json"), default={})

    product_handles = {p.get("handle", "") for p in products if p.get("handle")}
    collection_handles = {c.get("handle", "") for c in collections if c.get("handle")}
    page_handles = {p.get("handle", "") for p in pages if p.get("handle")}
    blog_handles = {b.get("handle", "") for b in blogs if b.get("handle")}
    ingredient_handles = {
        o.get("handle", "")
        for o in metaobjects.get("ingredient", {}).get("objects", [])
        if o.get("handle")
    }

    article_by_handle = {}
    blog_handle_by_id = {str(b.get("id")): b.get("handle", "") for b in blogs}
    for article in articles:
        handle = article.get("handle", "")
        if not handle:
            continue
        blog_id = str(article.get("blog_id", article.get("_blog_id", "")))
        article_by_handle[handle] = {
            "handle": handle,
            "blog_handle": article.get("_blog_handle") or blog_handle_by_id.get(blog_id, "journal"),
        }

    product_by_sku = {}
    for product in products:
        for variant in product.get("variants", []):
            sku = (variant.get("sku") or "").strip()
            if sku:
                product_by_sku[sku] = product.get("handle")

    return {
        "products": products,
        "product_handles": product_handles,
        "collection_handles": collection_handles,
        "page_handles": page_handles,
        "blog_handles": blog_handles,
        "article_by_handle": article_by_handle,
        "ingredient_handles": ingredient_handles,
        "product_by_sku": product_by_sku,
    }


def _build_magento_product_map(image_json_path: str, product_by_sku: dict[str, str | None]) -> dict[str, str]:
    slug_map: dict[str, str] = {}
    for item in load_json(image_json_path, default=[]):
        old_key = item.get("url_key", "")
        sku = (item.get("sku") or "").strip()
        handle = product_by_sku.get(sku)
        if old_key and handle:
            slug_map[old_key] = handle
    return slug_map


def _build_redirect_record(
    path: str,
    locale: str,
    slug: str,
    kind: str,
    target_path: str,
    confidence: str,
    reason: str,
) -> dict[str, str]:
    return {
        "path": path,
        "target": target_path,
        "locale": locale,
        "slug": slug,
        "kind": kind,
        "confidence": confidence,
        "reason": reason,
    }


def _map_path(
    path: str,
    locale: str,
    slug: str,
    current: dict[str, object],
    en_product_map: dict[str, str],
    ar_product_map: dict[str, str],
) -> dict[str, str] | None:
    explicit = EXPLICIT_ROUTE_MAP.get(slug)
    if explicit:
        kind = explicit["kind"]
        target = _target_path(locale, kind, explicit.get("handle"))
        return _build_redirect_record(
            path=path,
            locale=locale,
            slug=slug,
            kind=kind,
            target_path=target,
            confidence=explicit["confidence"],
            reason=explicit["reason"],
        )

    product_map = en_product_map if locale == "en" else ar_product_map
    product_handle = product_map.get(slug)
    if product_handle:
        return _build_redirect_record(
            path=path,
            locale=locale,
            slug=slug,
            kind="product",
            target_path=_target_path(locale, "product", product_handle),
            confidence="high",
            reason="Magento product URL key matched to current Shopify product by SKU",
        )

    if slug in current["product_handles"]:
        return _build_redirect_record(
            path=path,
            locale=locale,
            slug=slug,
            kind="product",
            target_path=_target_path(locale, "product", slug),
            confidence="high",
            reason="Exact product handle match",
        )

    if slug in current["collection_handles"]:
        return _build_redirect_record(
            path=path,
            locale=locale,
            slug=slug,
            kind="collection",
            target_path=_target_path(locale, "collection", slug),
            confidence="high",
            reason="Exact collection handle match",
        )

    if slug in current["page_handles"]:
        return _build_redirect_record(
            path=path,
            locale=locale,
            slug=slug,
            kind="page",
            target_path=_target_path(locale, "page", slug),
            confidence="high",
            reason="Exact page handle match",
        )

    if slug in current["ingredient_handles"]:
        return _build_redirect_record(
            path=path,
            locale=locale,
            slug=slug,
            kind="ingredient",
            target_path=_target_path(locale, "ingredient", slug),
            confidence="high",
            reason="Exact ingredient handle match",
        )

    article = current["article_by_handle"].get(slug)
    if article:
        return _build_redirect_record(
            path=path,
            locale=locale,
            slug=slug,
            kind="article",
            target_path=_target_path(locale, "article", article["handle"], blog_handle=article["blog_handle"]),
            confidence="high",
            reason="Exact article handle match",
        )

    if slug.startswith("blog/"):
        remainder = slug[len("blog/"):]
        if remainder in current["blog_handles"]:
            return _build_redirect_record(
                path=path,
                locale=locale,
                slug=slug,
                kind="blog",
                target_path=_target_path(locale, "blog", remainder),
                confidence="high",
                reason="Exact blog handle match",
            )
        article = current["article_by_handle"].get(remainder)
        if article:
            return _build_redirect_record(
                path=path,
                locale=locale,
                slug=slug,
                kind="article",
                target_path=_target_path(locale, "article", article["handle"], blog_handle=article["blog_handle"]),
                confidence="high",
                reason="Legacy blog path matched to current article handle",
            )
        if remainder.startswith("category/"):
            return _build_redirect_record(
                path=path,
                locale=locale,
                slug=slug,
                kind="blog",
                target_path=_target_path(locale, "blog", "journal"),
                confidence="medium",
                reason="Legacy Magento blog category path",
            )
        return _build_redirect_record(
            path=path,
            locale=locale,
            slug=slug,
            kind="blog",
            target_path=_target_path(locale, "blog", "journal"),
            confidence="low",
            reason="Legacy blog path has no exact Shopify article match",
        )

    return None


def _audit_targets(base_url: str, redirects: list[dict[str, str]]) -> dict[str, object]:
    unique_targets = sorted({r["target"] for r in redirects})
    target_statuses = []
    bad_targets = []
    for target in unique_targets:
        url = base_url.rstrip("/") + target
        resp = requests.get(url, timeout=30, allow_redirects=True, headers={"User-Agent": "Mozilla/5.0"})
        final_path = urlparse(resp.url).path.rstrip("/") or "/"
        ok = resp.status_code < 400
        row = {
            "target": target,
            "status": resp.status_code,
            "final_url": resp.url,
            "final_path": final_path,
            "ok": ok,
        }
        target_statuses.append(row)
        if not ok:
            bad_targets.append(row)
    return {
        "target_count": len(unique_targets),
        "bad_target_count": len(bad_targets),
        "bad_targets": bad_targets,
        "sample_targets": target_statuses[:25],
    }


def _sync_live_redirects(
    client: ShopifyClient,
    desired: list[dict[str, str]],
    delete_extra: bool,
) -> dict[str, int]:
    current = client.get_redirects()
    current_by_path = {r.get("path", ""): r for r in current if r.get("path")}
    desired_by_path = {r["path"]: r["target"] for r in desired}

    created = 0
    updated = 0
    unchanged = 0
    deleted = 0

    if delete_extra:
        for path, redirect in current_by_path.items():
            if path in desired_by_path:
                continue
            client.delete_redirect(redirect["id"])
            deleted += 1

    for path, target in desired_by_path.items():
        existing = current_by_path.get(path)
        if not existing:
            client.create_redirect(path, target)
            created += 1
            continue
        if existing.get("target") == target:
            unchanged += 1
            continue
        client.update_redirect(existing["id"], target=target)
        updated += 1

    return {
        "created": created,
        "updated": updated,
        "unchanged": unchanged,
        "deleted": deleted,
    }


def _audit_live_resolution(base_url: str, redirects: list[dict[str, str]]) -> dict[str, object]:
    problems = []
    samples = []
    for redirect in redirects:
        url = base_url.rstrip("/") + redirect["path"]
        resp = requests.get(url, timeout=30, allow_redirects=True, headers={"User-Agent": "Mozilla/5.0"})
        final_path = urlparse(resp.url).path.rstrip("/") or "/"
        expected = redirect["target"].rstrip("/") or "/"
        ok = resp.status_code < 400 and final_path == expected
        row = {
            "path": redirect["path"],
            "configured_target": redirect["target"],
            "status": resp.status_code,
            "final_url": resp.url,
            "final_path": final_path,
            "ok": ok,
        }
        if len(samples) < 25:
            samples.append(row)
        if not ok:
            problems.append(row)
    return {
        "total_redirects": len(redirects),
        "problem_count": len(problems),
        "problems": problems,
        "sample": samples,
    }


def main() -> None:
    load_dotenv()
    parser = argparse.ArgumentParser(description="Build live redirects from old Kuwait Magento sitemap URLs")
    parser.add_argument("--en-sitemap-url", default=DEFAULT_EN_SITEMAP_URL)
    parser.add_argument("--ar-sitemap-url", default=DEFAULT_AR_SITEMAP_URL)
    parser.add_argument("--verify-base-url", default=VERIFY_BASE_URL)
    parser.add_argument("--apply", action="store_true", help="Sync the generated redirect set to live Shopify")
    parser.add_argument("--delete-extra", action="store_true", help="Delete live redirects not present in the generated sitemap-based set")
    args = parser.parse_args()

    dest_name = config.get_dest_name() or "default"
    english_dir = os.path.join("data", dest_name, "english")
    if not os.path.isdir(english_dir):
        english_dir = os.path.join("data", "english")

    output_dir = os.path.join("data", dest_name)
    os.makedirs(output_dir, exist_ok=True)

    en_xml, en_meta = _fetch_xml(args.en_sitemap_url)
    ar_xml, ar_meta = _fetch_xml(args.ar_sitemap_url)

    en_cache_path = os.path.join(output_dir, "source_sitemap_kw_en.xml")
    ar_cache_path = os.path.join(output_dir, "source_sitemap_kw_ar.xml")
    with open(en_cache_path, "wb") as f:
        f.write(en_xml)
    with open(ar_cache_path, "wb") as f:
        f.write(ar_xml)

    en_paths = _parse_sitemap_paths(en_xml)
    ar_paths = _parse_sitemap_paths(ar_xml)

    current = _load_current_shopify_data(english_dir)
    en_product_map = _build_magento_product_map(os.path.join("data", "magento_en_images.json"), current["product_by_sku"])
    ar_product_map = _build_magento_product_map(os.path.join("data", "magento_ar_images.json"), current["product_by_sku"])

    redirects = []
    unresolved = []
    confidence_counts = defaultdict(int)
    kind_counts = defaultdict(int)

    for path in en_paths + ar_paths:
        locale, slug = _slug_from_path(path)
        record = _map_path(path, locale, slug, current, en_product_map, ar_product_map)
        if not record:
            unresolved.append({"path": path, "locale": locale, "slug": slug})
            continue
        redirects.append(record)
        confidence_counts[record["confidence"]] += 1
        kind_counts[record["kind"]] += 1

    redirects.sort(key=lambda item: item["path"])
    low_confidence = [r for r in redirects if r["confidence"] == "low"]
    medium_confidence = [r for r in redirects if r["confidence"] == "medium"]

    target_audit = _audit_targets(args.verify_base_url, redirects)

    redirects_out = [{"path": r["path"], "target": r["target"]} for r in redirects]
    redirects_path = os.path.join(output_dir, "magento_sitemap_redirects.json")
    audit_path = os.path.join(output_dir, "magento_sitemap_redirects_audit.json")
    save_json(redirects_out, redirects_path)
    save_json(
        {
            "sources": {
                "en": en_meta,
                "ar": ar_meta,
            },
            "english_dir": english_dir,
            "counts": {
                "en_paths": len(en_paths),
                "ar_paths": len(ar_paths),
                "redirects": len(redirects),
                "unresolved": len(unresolved),
                "high_confidence": confidence_counts["high"],
                "medium_confidence": confidence_counts["medium"],
                "low_confidence": confidence_counts["low"],
            },
            "kind_counts": dict(kind_counts),
            "unresolved": unresolved,
            "low_confidence": low_confidence,
            "medium_confidence": medium_confidence,
            "target_audit": target_audit,
            "sample_redirects": redirects[:25],
        },
        audit_path,
    )

    print(f"Saved {len(redirects_out)} redirects to {redirects_path}")
    print(f"Build audit saved to {audit_path}")
    print(
        f"Counts: EN {len(en_paths)}, AR {len(ar_paths)}, redirects {len(redirects_out)}, "
        f"unresolved {len(unresolved)}, bad targets {target_audit['bad_target_count']}"
    )

    if not args.apply:
        return

    shop_url = config.get_dest_shop_url()
    access_token = config.get_dest_access_token()
    if not shop_url or not access_token:
        raise RuntimeError("DEST_SHOP_URL and DEST_ACCESS_TOKEN must be set in .env")

    client = ShopifyClient(shop_url, access_token)
    live_backup_path = os.path.join(output_dir, "live_redirects_backup_before_magento_sync.json")
    save_json(client.get_redirects(), live_backup_path)
    print(f"Live backup saved to {live_backup_path}")

    sync_result = _sync_live_redirects(client, redirects_out, delete_extra=args.delete_extra)
    print(
        "Live sync complete: "
        f"created {sync_result['created']}, "
        f"updated {sync_result['updated']}, "
        f"deleted {sync_result['deleted']}, "
        f"unchanged {sync_result['unchanged']}"
    )

    resolution_audit = _audit_live_resolution(args.verify_base_url, redirects_out)
    resolution_path = os.path.join(output_dir, "magento_sitemap_redirect_resolution_audit.json")
    save_json(
        {
            "sync_result": sync_result,
            **resolution_audit,
        },
        resolution_path,
    )
    print(
        f"Resolution audit saved to {resolution_path} "
        f"(problems: {resolution_audit['problem_count']})"
    )


if __name__ == "__main__":
    main()
