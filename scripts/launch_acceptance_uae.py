#!/usr/bin/env python3
"""Read-only launch acceptance check for the live UAE Shopify store."""

from __future__ import annotations

import re

import requests


HEADERS = {"User-Agent": "Mozilla/5.0", "Cache-Control": "no-cache"}
BASE = "https://taraformula.ae"
EDITOR = "https://tara-product-editor-production.up.railway.app"
EXPECTED_SHOP = "rvgkkk-g3.myshopify.com"


def get(url: str, **kwargs) -> requests.Response:
    return requests.get(url, headers=HEADERS, timeout=40, **kwargs)


def shopify_locale(html: str) -> str | None:
    match = re.search(r'Shopify\.locale\s*=\s*"([^"]+)"', html)
    return match.group(1) if match else None


def html_lang(html: str) -> str | None:
    match = re.search(r'<html[^>]*lang="([^"]+)"', html)
    return match.group(1) if match else None


def shopify_files(html: str) -> set[str]:
    pattern = r"/cdn/shop/files/([A-Za-z0-9_.\-]+?_(?:ar|en)_\d+\.(?:jpg|jpeg|png|webp))"
    return set(re.findall(pattern, html))


def main() -> int:
    results: list[tuple[bool, str]] = []

    try:
        response = get(f"{BASE}/?_acceptance=1")
        shop = re.search(r'Shopify\.shop\s*=\s*"([^"]+)"', response.text)
        password = "/password" in response.url or "password" in response.url
        ok = (
            response.status_code == 200
            and shop
            and shop.group(1) == EXPECTED_SHOP
            and not password
        )
        detail = (
            f"apex live on Shopify "
            f"(status {response.status_code}, shop={shop.group(1) if shop else None}, "
            f"password={'on' if password else 'off'})"
        )
        results.append((bool(ok), detail))
    except Exception as exc:
        results.append((False, f"apex error: {str(exc)[:70]}"))

    try:
        response = get("https://www.taraformula.ae/", allow_redirects=True)
        results.append((response.status_code == 200, f"www status {response.status_code} -> {response.url}"))
    except Exception as exc:
        results.append((False, f"www error: {str(exc)[:70]}"))

    try:
        html = get(f"{BASE}/?_acceptance=2").text
        match = re.search(r'Shopify\.currency\s*=\s*\{[^}]*"active":"([^"]+)"', html)
        results.append((bool(match and match.group(1) == "AED"), f"currency active = {match.group(1) if match else 'unknown'}"))
    except Exception as exc:
        results.append((False, f"currency error: {str(exc)[:70]}"))

    try:
        response = get(f"{BASE}/ar/?_acceptance=1")
        arabic_text = len(re.findall(r"[\u0600-\u06FF]", response.text)) > 500
        rtl = 'dir="rtl"' in response.text
        ok = response.status_code == 200 and rtl and arabic_text and shopify_locale(response.text) == "ar"
        results.append((ok, f"/ar render (lang={html_lang(response.text)}, rtl={rtl}, arabic_text={arabic_text})"))
    except Exception as exc:
        results.append((False, f"/ar error: {str(exc)[:70]}"))

    try:
        en_html = get(f"{BASE}/products/volumizing-shampoo?_acceptance=1").text
        ar_html = get(f"{BASE}/ar/products/volumizing-shampoo?_acceptance=1").text
        ar_only = shopify_files(ar_html) - shopify_files(en_html)
        ok = shopify_locale(ar_html) == "ar" and any("_ar_" in filename for filename in ar_only)
        results.append((ok, f"/ar PDP localized images ({len(ar_only)} ar-only)"))
    except Exception as exc:
        results.append((False, f"localized image error: {str(exc)[:70]}"))

    for old_path, expected in [
        ("/black-garlic-ceramides-shampoo", "/products/"),
        ("/faq", "/pages/faq"),
    ]:
        try:
            response = get(BASE + old_path, allow_redirects=False)
            location = response.headers.get("Location", "")
            ok = response.status_code in (301, 302) and expected in location
            results.append((ok, f"redirect {old_path} -> {location[:60]} ({response.status_code})"))
        except Exception as exc:
            results.append((False, f"redirect {old_path} error: {str(exc)[:70]}"))

    try:
        health = get(f"{EDITOR}/healthz")
        auth = get(f"{EDITOR}/auth/login?shop={EXPECTED_SHOP}", allow_redirects=False)
        results.append((health.status_code == 200 and auth.status_code == 302, f"Product Editor healthz {health.status_code}, auth {auth.status_code}"))
    except Exception as exc:
        results.append((False, f"Product Editor error: {str(exc)[:70]}"))

    print("=== UAE LAUNCH ACCEPTANCE ===")
    for passed, detail in results:
        print(f"  [{'PASS' if passed else 'FAIL'}] {detail}")

    failures = [detail for passed, detail in results if not passed]
    print("\nALL PASS - GREENLIT" if not failures else f"\n{len(failures)} issue(s) to review")
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())
