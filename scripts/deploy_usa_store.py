#!/usr/bin/env python3
from __future__ import annotations

import os
import re
import subprocess
import sys
from pathlib import Path

import requests
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from tara_migrate.client import ShopifyClient  # noqa: E402

THEME_ROOT = Path(r"C:\Users\narha\tara-usa-shopify")
SHOP_IMAGE_RE = re.compile(r"shopify://shop_images/([^\"'\s)]+)")
THEME_TEXT_DIRS = ("assets", "blocks", "config", "layout", "locales", "sections", "snippets", "templates")

PAGE_SPECS = [
    {
        "handles": ["quiz", "consultation"],
        "title": "Consultation",
        "handle": "consultation",
        "template_suffix": "consultation",
    },
    {
        "handles": ["quiz-results", "consultation-results"],
        "title": "Consultation Results",
        "handle": "consultation-results",
        "template_suffix": "consultation-results",
    },
    {
        "handles": ["for-pharmacies"],
        "title": "For Pharmacies",
        "handle": "for-pharmacies",
        "template_suffix": "pharmacies",
    },
    {
        "handles": ["philosophy-en", "philosophy"],
        "title": "Philosophy",
        "handle": "philosophy",
        "template_suffix": None,
    },
    {
        "handles": ["privacy-policy-en", "privacy-policy"],
        "title": "Privacy Policy",
        "handle": "privacy-policy",
        "template_suffix": None,
    },
    {
        "handles": ["terms-and-conditions-en", "terms-and-conditions"],
        "title": "Terms & Conditions",
        "handle": "terms-and-conditions",
        "template_suffix": None,
    },
    {
        "handles": ["home-page-en", "home-page"],
        "title": "Home Page",
        "handle": "home-page",
        "template_suffix": None,
    },
]

MENU_SPECS = {
    "main-menu": {
        "title": "Main menu",
        "items": [
            {"title": "Hair Care", "url": "/collections/shop-hair"},
            {"title": "Consultation", "url": "/pages/consultation"},
            {"title": "Ingredients", "url": "/pages/ingredients"},
            {"title": "Philosophy", "url": "/pages/philosophy"},
            {"title": "Journal", "url": "/blogs/journal"},
        ],
    },
    "footer": {
        "title": "Footer menu",
        "items": [
            {"title": "For Pharmacies", "url": "/pages/for-pharmacies"},
            {"title": "Shop Hair Care", "url": "/collections/shop-hair"},
            {"title": "Consultation", "url": "/pages/consultation"},
            {"title": "Our Philosophy", "url": "/pages/philosophy"},
            {"title": "Our Ingredients", "url": "/pages/ingredients"},
        ],
    },
    "footer-customer-service": {
        "title": "Footer customer service",
        "items": [
            {"title": "FAQ", "url": "/pages/faq"},
            {"title": "Privacy Policy", "url": "/pages/privacy-policy"},
            {"title": "Terms & Conditions", "url": "/pages/terms-and-conditions"},
            {"title": "Contact Us", "url": "/pages/contact"},
        ],
    },
}

REDIRECTS = {
    "/pages/quiz": "/pages/consultation",
    "/pages/quiz-results": "/pages/consultation-results",
    "/pages/consulta-capilar": "/pages/consultation",
    "/pages/resultado-consulta": "/pages/consultation-results",
    "/pages/para-farmacias": "/pages/for-pharmacies",
    "/pages/para-framacias": "/pages/for-pharmacies",
    "/pages/philosophy-en": "/pages/philosophy",
    "/pages/privacy-policy-en": "/pages/privacy-policy",
    "/pages/terms-and-conditions-en": "/pages/terms-and-conditions",
    "/pages/home-page-en": "/",
    "/collections/cabello": "/collections/shop-hair",
}

LEGACY_THEME_ASSETS = [
    "templates/page.quiz.json",
    "templates/page.quiz-results.json",
    "templates/page.consulta-capilar.json",
    "templates/page.resultado-consulta.json",
    "templates/page.pharmacy-lead.json",
    "templates/page.landing-dia-madre.json",
    "templates/collection.tara-catalog.json",
    "templates/collection.tara-line.json",
    "templates/collection.tara-routines.json",
]


def load_clients() -> tuple[ShopifyClient, ShopifyClient, ShopifyClient | None]:
    load_dotenv(ROOT / "usa-destination.env")
    load_dotenv(ROOT / ".env", override=False)

    usa = ShopifyClient(os.environ["DEST_SHOP_URL"], os.environ["DEST_ACCESS_TOKEN"])
    saudi = ShopifyClient(os.environ["SAUDI_SHOP_URL"], os.environ["SAUDI_ACCESS_TOKEN"])

    spain = None
    if os.environ.get("SOURCE_SHOP_URL") and os.environ.get("SOURCE_ACCESS_TOKEN"):
        spain = ShopifyClient(os.environ["SOURCE_SHOP_URL"], os.environ["SOURCE_ACCESS_TOKEN"])

    return usa, saudi, spain


def theme_file_keys_from_git(theme_root: Path) -> list[str]:
    result = subprocess.run(
        ["git", "status", "--short"],
        cwd=theme_root,
        capture_output=True,
        text=True,
        check=True,
    )
    keys: list[str] = []
    for raw_line in result.stdout.splitlines():
        line = raw_line.rstrip()
        if not line:
            continue
        path = line[3:]
        path = path.split(" -> ", 1)[-1]
        if path.startswith(THEME_TEXT_DIRS):
            keys.append(path.replace("\\", "/"))
    return sorted(set(keys))


def deploy_theme_assets(client: ShopifyClient, theme_id: int, theme_root: Path, keys: list[str]) -> None:
    for key in keys:
        full_path = theme_root / key
        if not full_path.exists():
            continue
        value = full_path.read_text(encoding="utf-8")
        client.put_asset(theme_id, key, value=value)
        print(f"UPLOAD {key}")


def collect_shop_image_refs(theme_root: Path) -> list[str]:
    refs: set[str] = set()
    for directory in THEME_TEXT_DIRS:
        base = theme_root / directory
        if not base.exists():
            continue
        for path in base.rglob("*"):
            if not path.is_file():
                continue
            try:
                text = path.read_text(encoding="utf-8")
            except UnicodeDecodeError:
                continue
            refs.update(SHOP_IMAGE_RE.findall(text))
    return sorted(refs)


def build_file_map(client: ShopifyClient) -> dict[str, dict]:
    mapping: dict[str, dict] = {}
    for item in client.get_files():
        url = (item.get("image", {}) or {}).get("url", "") or item.get("url", "")
        name = url.split("/")[-1].split("?", 1)[0]
        if name:
            mapping[name] = item
    return mapping


def refresh_file_map_entry(client: ShopifyClient, mapping: dict[str, dict], file_id: str) -> None:
    info = client.get_file_by_id(file_id)
    if not info:
        return
    url = (info.get("image", {}) or {}).get("url", "") or info.get("url", "")
    if not url:
        return
    name = url.split("/")[-1].split("?", 1)[0]
    if name:
        mapping[name] = info


def ensure_shop_images(
    dest: ShopifyClient,
    primary_source: ShopifyClient,
    secondary_source: ShopifyClient | None,
    required_names: list[str],
) -> None:
    dest_files = build_file_map(dest)
    primary_files = build_file_map(primary_source)
    secondary_files = build_file_map(secondary_source) if secondary_source else {}

    for name in required_names:
        existing = dest_files.get(name)
        is_image_name = name.lower().endswith((".png", ".jpg", ".jpeg", ".webp", ".gif", ".svg", ".avif"))
        if existing and not (is_image_name and "image" not in existing):
            continue
        if existing and is_image_name and "image" not in existing:
            try:
                dest.delete_file(existing["id"])
                print(f"DELETE FILE {name}")
            except Exception as exc:
                print(f"FAILED DELETE FILE {name}: {exc}")
        source_item = primary_files.get(name) or secondary_files.get(name)
        if not source_item:
            print(f"MISSING SHOP IMAGE {name}")
            continue
        url = (source_item.get("image", {}) or {}).get("url", "") or source_item.get("url", "")
        file_id = dest.upload_file_from_url(url, filename=name, alt=name)
        if file_id:
            refresh_file_map_entry(dest, dest_files, file_id)
            print(f"FILE {name}")
        else:
            print(f"FAILED FILE {name}")


def update_page(client: ShopifyClient, page_id: int, payload: dict) -> dict:
    data = {"page": {"id": page_id, **payload}}
    response = client._request("PUT", f"pages/{page_id}.json", json=data)
    return response.json().get("page", {})


def find_page(client: ShopifyClient, handles: list[str]) -> dict | None:
    pages = client.get_pages()
    by_handle = {page.get("handle"): page for page in pages}
    for handle in handles:
        if handle in by_handle:
            return by_handle[handle]
    return None


def configure_pages(client: ShopifyClient) -> None:
    for spec in PAGE_SPECS:
        page = find_page(client, spec["handles"])
        if not page:
            print(f"PAGE MISSING {spec['handles'][0]}")
            continue
        payload = {
            "title": spec["title"],
            "handle": spec["handle"],
            "template_suffix": spec["template_suffix"],
        }
        update_page(client, int(page["id"]), payload)
        print(f"PAGE {page['id']} -> {spec['handle']} ({spec['template_suffix']})")


def configure_menus(client: ShopifyClient) -> None:
    menus = {menu["handle"]: menu for menu in client.get_menus()}
    for handle, spec in MENU_SPECS.items():
        existing = menus.get(handle)
        if existing:
            client.update_menu(existing["id"], title=spec["title"], items=spec["items"])
            print(f"MENU UPDATE {handle}")
        else:
            client.create_menu(spec["title"], handle, spec["items"])
            print(f"MENU CREATE {handle}")


def configure_redirects(client: ShopifyClient) -> None:
    existing = {item["path"]: item for item in client.get_redirects()}
    for path, target in REDIRECTS.items():
        current = existing.get(path)
        if current:
            if current.get("target") != target:
                client.update_redirect(current["id"], path=path, target=target)
                print(f"REDIRECT UPDATE {path} -> {target}")
        else:
            client.create_redirect(path, target)
            print(f"REDIRECT CREATE {path} -> {target}")


def delete_legacy_assets(client: ShopifyClient, theme_id: int) -> None:
    existing_keys = {item["key"] for item in client.list_assets(theme_id)}
    for key in LEGACY_THEME_ASSETS:
        if key in existing_keys:
            client.delete_asset(theme_id, key)
            print(f"DELETE {key}")


def verify_storefront(client: ShopifyClient) -> None:
    shop = client.get_shop()
    if shop.get("password_enabled"):
        print("VERIFY SKIPPED: storefront is password protected")
        return

    base = f"https://{shop['domain']}"
    product = next((p for p in client.get_products() if p.get("handle")), None)
    checks = [
        ("/", ["TARA_Scalp_Serum_6", "Shop Serums"]),
        ("/pages/consultation", ["Consultation"]),
        ("/pages/for-pharmacies", ["Request commercial information"]),
        ("/blogs/journal", ["Tara Journal", "The Tara Dossier"]),
    ]
    if product:
        checks.append((f"/products/{product['handle']}", ["Fast shipping across the USA", "Secure encrypted payment"]))

    session = requests.Session()
    for path, snippets in checks:
        url = f"{base}{path}"
        response = session.get(url, timeout=30)
        status = response.status_code
        body = response.text
        ok = status == 200 and any(snippet in body for snippet in snippets)
        print(f"VERIFY {path} status={status} ok={ok}")
        if not ok:
            raise SystemExit(f"Verification failed for {url}")


def main() -> None:
    usa, saudi, spain = load_clients()
    theme_id = usa.get_main_theme_id()
    if not theme_id:
        raise SystemExit("Could not resolve active USA theme")

    print(f"USA theme: {theme_id}")
    keys = theme_file_keys_from_git(THEME_ROOT)
    if not keys:
        raise SystemExit("No modified theme files detected to deploy")

    deploy_theme_assets(usa, theme_id, THEME_ROOT, keys)
    ensure_shop_images(usa, saudi, spain, collect_shop_image_refs(THEME_ROOT))
    configure_pages(usa)
    configure_menus(usa)
    configure_redirects(usa)
    delete_legacy_assets(usa, theme_id)
    verify_storefront(usa)
    print("USA storefront deployment complete")


if __name__ == "__main__":
    main()
