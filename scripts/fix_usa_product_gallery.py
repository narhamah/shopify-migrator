#!/usr/bin/env python3
"""Reconcile native Shopify product galleries with live Magento US English.

This script makes the Shopify product gallery match the live Magento US-English
storefront source of truth by filename, image count, and order.

Rules:
- Keep English-tagged and neutral Magento images only.
- Preserve existing Shopify product images when the normalized filename matches.
- Create any missing desired images from the live Magento asset URL.
- Delete extra Shopify product images not present in the desired gallery.
- Skip products that do not exist in Magento (samples, accessories, etc.).

Usage:
    python scripts/fix_usa_product_gallery.py --env-file usa-destination.env --dry-run
    python scripts/fix_usa_product_gallery.py --env-file usa-destination.env
    python scripts/fix_usa_product_gallery.py --env-file usa-destination.env --product strengthening-scalp-serum
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import re
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from tara_migrate.client import ShopifyClient


PRODUCTS_QUERY = """
query Products($pageSize: Int!, $currentPage: Int!) {
  products(search: "", pageSize: $pageSize, currentPage: $currentPage) {
    total_count
    page_info {
      current_page
      total_pages
    }
    items {
      sku
      name
      url_key
      media_gallery {
        url
        label
        position
        disabled
      }
    }
  }
}
"""

LANG_PATTERNS = {
    "en": re.compile(r"[_-](en|eng)[_.\d]", re.IGNORECASE),
    "ar": re.compile(r"[_-]ar[_.\d]", re.IGNORECASE),
    "es": re.compile(r"[_-](es|esp|sp)[_.\d]", re.IGNORECASE),
}


def classify_image(url: str) -> str:
    filename = file_name(url).lower()
    for lang, pattern in LANG_PATTERNS.items():
        if pattern.search(filename):
            return lang
    base = filename.rsplit(".", 1)[0]
    for token in re.split(r"[_-]+", base):
        if token in {"ar", "arabic"}:
            return "ar"
        # Some Magento assets encode Arabic as a token suffix, e.g. purifyar_2.
        if re.fullmatch(r"[a-z]+ar\d*", token):
            return "ar"
    return "neutral"


def file_name(url: str) -> str:
    return url.rsplit("/", 1)[-1].split("?", 1)[0]


def normalize_filename(value: str) -> str:
    name = file_name(value).lower()
    stem, dot, ext = name.rpartition(".")
    if dot:
        stem = re.sub(
            r"_[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
            "",
            stem,
        )
        stem = re.sub(r"_[0-9a-f]{32,}$", "", stem)
        stem = stem.rstrip("_")
        return stem
    return name.rstrip("_")


def dedupe_entries(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[str] = set()
    deduped: list[dict[str, Any]] = []
    for entry in entries:
        norm = entry["norm"]
        if norm in seen:
            continue
        deduped.append(entry)
        seen.add(norm)
    return deduped


def fetch_live_magento_galleries(site_url: str, store_code: str) -> dict[str, dict[str, Any]]:
    session = requests.Session()
    session.headers.update({
        "Content-Type": "application/json",
        "Store": store_code,
        "User-Agent": "Mozilla/5.0",
    })

    galleries: dict[str, dict[str, Any]] = {}
    current_page = 1

    while True:
        response = session.post(
            f"{site_url.rstrip('/')}/graphql",
            json={
                "query": PRODUCTS_QUERY,
                "variables": {"pageSize": 50, "currentPage": current_page},
            },
            timeout=60,
        )
        response.raise_for_status()
        payload = response.json()
        if payload.get("errors"):
            raise RuntimeError(f"Magento GraphQL errors: {payload['errors']}")

        products = payload["data"]["products"]
        for item in products.get("items", []):
            sku = item.get("sku")
            if not sku:
                continue

            media = sorted(
                item.get("media_gallery") or [],
                key=lambda image: (image.get("position") or 0, image.get("url") or ""),
            )
            desired = []
            for image in media:
                url = image.get("url") or ""
                if not url:
                    continue
                if classify_image(url) not in ("en", "neutral"):
                    continue
                desired.append({
                    "url": url,
                    "label": image.get("label") or "",
                    "position": image.get("position") or 0,
                    "filename": file_name(url),
                    "norm": normalize_filename(url),
                })

            galleries[sku] = {
                "sku": sku,
                "name": item.get("name") or "",
                "handle_guess": item.get("url_key") or "",
                "desired": dedupe_entries(desired),
            }

        if current_page >= products["page_info"]["total_pages"]:
            break
        current_page += 1

    return galleries


def queue_images_by_norm(images: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    queued: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for image in sorted(images, key=lambda item: item.get("position") or 0):
        queued[normalize_filename(image.get("src", ""))].append(image)
    return queued


def build_assignment(
    current_images: list[dict[str, Any]],
    desired_entries: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    queued = queue_images_by_norm(current_images)
    assigned: list[dict[str, Any]] = []
    missing: list[dict[str, Any]] = []

    for desired in desired_entries:
        matches = queued.get(desired["norm"]) or []
        if matches:
            assigned.append({"desired": desired, "image": matches.pop(0)})
        else:
            missing.append(desired)

    extras = []
    for leftover in queued.values():
        extras.extend(leftover)
    extras.sort(key=lambda item: item.get("position") or 0)
    return assigned, missing, extras


def update_product_image(client: ShopifyClient, product_id: int | str, image_id: int | str, payload: dict[str, Any]) -> dict[str, Any]:
    response = client._request(
        "PUT",
        f"products/{product_id}/images/{image_id}.json",
        json={"image": {"id": image_id, **payload}},
    )
    return response.json().get("image", {})


def create_product_image(
    client: ShopifyClient,
    product_id: int | str,
    image_url: str,
    alt: str,
    position: int,
) -> dict[str, Any]:
    candidates = [image_url]
    if "/media/catalog/product/cache/" in image_url and "/1/4/" in image_url:
        candidates.append(image_url.replace("/1/4/", "/t/a/"))
    if "taraformula.com/" in image_url:
        candidates.append(image_url.replace("taraformula.com/", "taraformula.ae/"))
    if "taraformula.com/" in image_url and "/1/4/" in image_url:
        candidates.append(image_url.replace("taraformula.com/", "taraformula.ae/").replace("/1/4/", "/t/a/"))

    last_error: Exception | None = None
    seen_urls: set[str] = set()
    for candidate in candidates:
        if candidate in seen_urls:
            continue
        seen_urls.add(candidate)

        src_payload = {"image": {"src": candidate, "alt": alt, "position": position}}
        try:
            response = client._request("POST", f"products/{product_id}/images.json", json=src_payload)
            return response.json().get("image", {})
        except Exception as exc:
            last_error = exc

        try:
            file_response = requests.get(candidate, timeout=60)
            file_response.raise_for_status()
            attachment_payload = {
                "image": {
                    "attachment": base64.b64encode(file_response.content).decode("ascii"),
                    "filename": file_name(candidate),
                    "alt": alt,
                    "position": position,
                }
            }
            response = client._request("POST", f"products/{product_id}/images.json", json=attachment_payload)
            return response.json().get("image", {})
        except Exception as exc:
            last_error = exc

    if last_error:
        raise last_error
    raise RuntimeError(f"Failed to create product image for {file_name(image_url)}")


def delete_product_image(client: ShopifyClient, product_id: int | str, image_id: int | str) -> None:
    client._request("DELETE", f"products/{product_id}/images/{image_id}.json")


def fetch_product(product_id: int | str, client: ShopifyClient) -> dict[str, Any]:
    response = client._request("GET", f"products/{product_id}.json", params={"fields": "id,handle,title,images,variants"})
    return response.json().get("product", {})


def apply_product_gallery_fix(
    client: ShopifyClient,
    product: dict[str, Any],
    desired_entries: list[dict[str, Any]],
) -> dict[str, Any]:
    product_id = product["id"]
    assigned, missing, extras = build_assignment(product.get("images", []), desired_entries)

    created_ids: list[int] = []
    for index, desired in enumerate(missing, start=1):
        created = create_product_image(
            client,
            product_id,
            desired["url"],
            desired["label"],
            len(product.get("images", [])) + len(created_ids) + index,
        )
        if not created or not created.get("id"):
            raise RuntimeError(f"Failed to create product image for {product['handle']}: {desired['filename']}")
        created_ids.append(created["id"])
        time.sleep(0.35)

    refreshed = fetch_product(product_id, client)
    assigned, missing_after, extras = build_assignment(refreshed.get("images", []), desired_entries)
    if missing_after:
        raise RuntimeError(
            f"Still missing {len(missing_after)} desired images after upload: "
            + ", ".join(item["filename"] for item in missing_after[:5])
        )

    for extra in extras:
        delete_product_image(client, product_id, extra["id"])
        time.sleep(0.2)

    for position, item in enumerate(assigned, start=1):
        desired = item["desired"]
        image = item["image"]
        payload = {"position": position}
        desired_alt = desired["label"]
        current_alt = image.get("alt") or ""
        if desired_alt and current_alt != desired_alt:
            payload["alt"] = desired_alt
        update_product_image(client, product_id, image["id"], payload)
        time.sleep(0.15)

    final_product = fetch_product(product_id, client)
    final_names = [
        normalize_filename(image.get("src", ""))
        for image in sorted(final_product.get("images", []), key=lambda item: item.get("position") or 0)
    ]
    expected_names = [entry["norm"] for entry in desired_entries]
    if final_names != expected_names:
        raise RuntimeError(f"Final gallery does not match desired order for {product['handle']}")

    return {
        "created": len(created_ids),
        "deleted": len(extras),
        "final_count": len(final_names),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Fix USA Shopify native product image order/count from live Magento US English")
    parser.add_argument("--env-file", default="usa-destination.env", help="Environment file path")
    parser.add_argument("--dry-run", action="store_true", help="Preview changes without writing")
    parser.add_argument("--product", help="Limit to a single Shopify product handle")
    args = parser.parse_args()

    env_path = Path(args.env_file)
    if not env_path.is_absolute():
        env_path = ROOT / env_path
    load_dotenv(env_path)

    dest_shop = os.environ.get("DEST_SHOP_URL")
    dest_token = os.environ.get("DEST_ACCESS_TOKEN")
    magento_site = os.environ.get("MAGENTO_SITE_URL", "https://taraformula.com")
    magento_store = os.environ.get("MAGENTO_STORE_CODE", "us-en")

    if not dest_shop or not dest_token:
        raise RuntimeError("DEST_SHOP_URL and DEST_ACCESS_TOKEN are required")

    print(f"Store: {dest_shop}")
    print(f"Magento source: {magento_site} [{magento_store}]")
    if args.dry_run:
        print("Mode: dry-run")

    desired_by_sku = fetch_live_magento_galleries(magento_site, magento_store)
    client = ShopifyClient(dest_shop, dest_token)
    products = client.get_products()
    if args.product:
        products = [product for product in products if product.get("handle") == args.product]
        if not products:
            raise RuntimeError(f"Shopify product not found: {args.product}")

    report = {
        "dry_run": args.dry_run,
        "store": dest_shop,
        "magento_site": magento_site,
        "magento_store": magento_store,
        "processed": [],
        "summary": {
            "ok": 0,
            "fixed": 0,
            "skipped_no_magento": 0,
            "errors": 0,
        },
    }

    for product in products:
        sku = next((variant.get("sku") for variant in product.get("variants", []) if variant.get("sku")), None)
        handle = product.get("handle", "")
        current_images = sorted(product.get("images", []), key=lambda item: item.get("position") or 0)
        current_names = [normalize_filename(image.get("src", "")) for image in current_images]

        if not sku or sku not in desired_by_sku:
            report["processed"].append({
                "handle": handle,
                "sku": sku,
                "status": "SKIP_NO_MAGENTO",
                "current_count": len(current_names),
            })
            report["summary"]["skipped_no_magento"] += 1
            print(f"SKIP {handle} ({sku}) - no Magento source")
            continue

        desired_entries = desired_by_sku[sku]["desired"]
        desired_names = [entry["norm"] for entry in desired_entries]

        if current_names == desired_names:
            report["processed"].append({
                "handle": handle,
                "sku": sku,
                "status": "OK",
                "current_count": len(current_names),
                "desired_count": len(desired_names),
            })
            report["summary"]["ok"] += 1
            print(f"OK   {handle} ({sku}) {len(current_names)} images")
            continue

        assigned, missing, extras = build_assignment(current_images, desired_entries)
        entry = {
            "handle": handle,
            "sku": sku,
            "status": "FIXED" if not args.dry_run else "WOULD_FIX",
            "current_count": len(current_names),
            "desired_count": len(desired_names),
            "missing_count": len(missing),
            "extra_count": len(extras),
            "current": current_names,
            "desired": desired_names,
        }

        print(
            f"{'DRY ' if args.dry_run else ''}FIX {handle} ({sku}) "
            f"{len(current_names)} -> {len(desired_names)} images "
            f"[missing {len(missing)}, extra {len(extras)}]"
        )

        if args.dry_run:
            report["processed"].append(entry)
            report["summary"]["fixed"] += 1
            continue

        try:
            result = apply_product_gallery_fix(client, product, desired_entries)
            entry.update(result)
            report["processed"].append(entry)
            report["summary"]["fixed"] += 1
        except Exception as exc:
            entry["status"] = "ERROR"
            entry["error"] = str(exc)
            report["processed"].append(entry)
            report["summary"]["errors"] += 1
            print(f"ERROR {handle}: {exc}")

    output_path = ROOT / "data" / "usa" / "product_gallery_fix_report.json"
    output_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

    print("=" * 72)
    print("Product gallery sync complete")
    print(f"OK:               {report['summary']['ok']}")
    print(f"Fixed:            {report['summary']['fixed']}")
    print(f"Skipped no source:{report['summary']['skipped_no_magento']}")
    print(f"Errors:           {report['summary']['errors']}")
    print(f"Report:           {output_path}")


if __name__ == "__main__":
    main()
