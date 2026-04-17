#!/usr/bin/env python3
"""Apply homepage meta locale keys to live Shopify themes.

Adds a small theme-level homepage meta/H1 abstraction:
- snippets/meta-tags.liquid reads tara.homepage_meta.* on the homepage
- sections/header.liquid reads tara.homepage_meta.hidden_h1 on the homepage

Then updates locale JSON assets with store-specific values from
data/homepage_meta_locales.json.
"""

from __future__ import annotations

import argparse
import json
import os
import re
from pathlib import Path

from dotenv import load_dotenv

from tara_migrate.client import ShopifyClient


ROOT = Path(__file__).resolve().parents[3]
DATA_FILE = ROOT / "data" / "homepage_meta_locales.json"
LEADING_COMMENT_RE = re.compile(r"^/\*.*?\*/\s*", re.S)

STORE_CONFIG = {
    "kuwait": {
        "shop_env": "DEST_SHOP_URL",
        "token_env": "DEST_ACCESS_TOKEN",
        "locale_assets": {
            "en": "locales/en.default.json",
            "ar": "locales/ar.json",
        },
    },
    "saudi": {
        "shop_env": "SAUDI_SHOP_URL",
        "token_env": "SAUDI_ACCESS_TOKEN",
        "locale_assets": {
            "en": "locales/en.default.json",
            "ar": "locales/ar.json",
        },
    },
    "spain": {
        "shop_env": "SPAIN_SHOP_URL",
        "token_env": "SPAIN_ACCESS_TOKEN",
        "locale_assets": {
            "en": "locales/en.default.json",
            "es": "locales/es.json",
        },
    },
}


def load_payload() -> dict:
    return json.loads(DATA_FILE.read_text(encoding="utf-8"))


def split_locale_asset(text: str) -> tuple[str, dict]:
    match = LEADING_COMMENT_RE.match(text)
    comment = match.group(0) if match else ""
    body = text[match.end():] if match else text
    return comment, json.loads(body)


def dump_locale_asset(comment: str, data: dict) -> str:
    return f"{comment}{json.dumps(data, ensure_ascii=False, indent=2)}\n"


def ensure_homepage_meta_keys(locale_data: dict, values: dict[str, str]) -> None:
    tara = locale_data.setdefault("tara", {})
    homepage_meta = tara.setdefault("homepage_meta", {})
    homepage_meta.update(values)


def patch_meta_tags(content: str, store_name: str) -> str:
    if "tara.homepage_meta.title" in content or "tara_home_meta_title" in content:
        return content

    common_assigns = (
        "  assign tara_home_meta_title = 'tara.homepage_meta.title' | t\n"
        "  assign tara_home_meta_description = 'tara.homepage_meta.description' | t\n"
        "  assign tara_home_og_title = 'tara.homepage_meta.og_title' | t\n"
        "  assign tara_home_og_description = 'tara.homepage_meta.og_description' | t"
    )

    if store_name == "kuwait":
        marker = "  assign consultation_results_title = 'tara.consultation.analysis_ready' | t | default: consultation_title"
        if marker not in content:
            raise RuntimeError("Could not find Kuwait consultation title marker in meta-tags.liquid")
        content = content.replace(marker, f"{marker}\n{common_assigns}", 1)

        block_marker = "  if request.path contains '/pages/consultation-results'"
        homepage_block = (
            "  if request.page_type == 'index'\n"
            "    assign visible_page_title = tara_home_meta_title\n"
            "    assign page_description = tara_home_meta_description\n"
            "    assign og_title = tara_home_og_title\n"
            "    assign og_description = tara_home_og_description\n"
            "  elsif request.path contains '/pages/consultation-results'"
        )
        if block_marker not in content:
            raise RuntimeError("Could not find Kuwait consultation block in meta-tags.liquid")
        content = content.replace(block_marker, homepage_block, 1)

        old_suffix = "{%- unless visible_page_title contains shop.name %} &ndash; {{ shop.name | escape }}{% endunless -%}"
        new_suffix = "{%- unless visible_page_title contains shop.name or request.page_type == 'index' %} &ndash; {{ shop.name | escape }}{% endunless -%}"
        if old_suffix not in content:
            raise RuntimeError("Could not find Kuwait title suffix in meta-tags.liquid")
        content = content.replace(old_suffix, new_suffix, 1)
        return content

    if store_name == "saudi":
        marker = "  assign og_type = 'website'"
        if marker not in content:
            raise RuntimeError("Could not find Saudi og_type marker in meta-tags.liquid")
        content = content.replace(marker, f"{marker}\n{common_assigns}", 1)

        block_marker = "  if request.page_type == 'page'"
        homepage_block = (
            "  if request.page_type == 'index'\n"
            "    assign resolved_page_title = tara_home_meta_title\n"
            "    assign resolved_page_description = tara_home_meta_description\n"
            "  elsif request.page_type == 'page'"
        )
        if block_marker not in content:
            raise RuntimeError("Could not find Saudi page block in meta-tags.liquid")
        content = content.replace(block_marker, homepage_block, 1)

        content = content.replace(
            "  assign og_title = resolved_page_title\n  assign og_description = resolved_page_description",
            "  assign og_title = resolved_page_title\n  assign og_description = resolved_page_description\n\n"
            "  if request.page_type == 'index'\n"
            "    assign og_title = tara_home_og_title\n"
            "    assign og_description = tara_home_og_description\n"
            "  endif",
            1,
        )

        old_suffix = "{%- unless resolved_page_title contains shop.name %} &ndash; {{ shop.name | escape }}{% endunless -%}"
        new_suffix = "{%- unless resolved_page_title contains shop.name or request.page_type == 'index' %} &ndash; {{ shop.name | escape }}{% endunless -%}"
        if old_suffix not in content:
            raise RuntimeError("Could not find Saudi title suffix in meta-tags.liquid")
        content = content.replace(old_suffix, new_suffix, 1)
        return content

    if store_name == "spain":
        marker = "  assign og_description = page_description | default: shop.description | default: shop.name"
        replacement = (
            "  assign resolved_page_title = page_title | default: shop.name\n"
            "  assign resolved_page_description = page_description | default: shop.description | default: shop.name\n"
            "  assign og_title = resolved_page_title\n"
            "  assign og_url = canonical_url | default: request.origin\n"
            "  assign og_type = 'website'\n"
            "  assign og_description = resolved_page_description\n"
            f"{common_assigns}"
        )
        if marker not in content:
            raise RuntimeError("Could not find Spain description marker in meta-tags.liquid")
        content = content.replace(
            "  assign og_title = page_title | default: shop.name\n"
            "  assign og_url = canonical_url | default: request.origin\n"
            "  assign og_type = 'website'\n"
            "  assign og_description = page_description | default: shop.description | default: shop.name",
            replacement,
            1,
        )

        index_block = (
            "  elsif request.page_type == 'index'\n"
            "    if is_spanish_locale\n"
            "      assign og_title = 'Cuidado capilar de precisión que empieza en el cuero cabelludo'\n"
            "      assign og_description = 'Tratamientos formulados en Barcelona para equilibrar el cuero cabelludo, fortalecer la raíz y devolver cuerpo, brillo y resistencia al cabello.'\n"
            "    else\n"
            "      assign og_title = 'Precision hair care that starts at the scalp'\n"
            "      assign og_description = 'Barcelona-formulated routines to rebalance the scalp, strengthen the root, and restore body, shine, and resilience to the hair.'\n"
            "    endif"
        )
        replacement_index_block = (
            "  elsif request.page_type == 'index'\n"
            "    assign resolved_page_title = tara_home_meta_title\n"
            "    assign resolved_page_description = tara_home_meta_description\n"
            "    assign og_title = tara_home_og_title\n"
            "    assign og_description = tara_home_og_description"
        )
        if index_block not in content:
            raise RuntimeError("Could not find Spain homepage block in meta-tags.liquid")
        content = content.replace(index_block, replacement_index_block, 1)

        old_title = "  {{ og_title | escape }}"
        new_title = "  {{ resolved_page_title | escape }}"
        if old_title not in content:
            raise RuntimeError("Could not find Spain title tag in meta-tags.liquid")
        content = content.replace(old_title, new_title, 1)

        old_suffix = "{%- unless og_title contains shop.name %} &ndash; {{ shop.name | escape }}{% endunless -%}"
        new_suffix = "{%- unless resolved_page_title contains shop.name or request.page_type == 'index' %} &ndash; {{ shop.name | escape }}{% endunless -%}"
        if old_suffix not in content:
            raise RuntimeError("Could not find Spain title suffix in meta-tags.liquid")
        content = content.replace(old_suffix, new_suffix, 1)

        old_description = '    content="{{ page_description | escape }}"'
        new_description = '    content="{{ resolved_page_description | escape }}"'
        if old_description not in content:
            raise RuntimeError("Could not find Spain meta description tag in meta-tags.liquid")
        content = content.replace(old_description, new_description, 1)
        return content

    raise RuntimeError(f"Unsupported store for meta-tags patch: {store_name}")


def patch_header(content: str, store_name: str) -> str:
    if "tara.homepage_meta.hidden_h1" in content:
        return content

    replacement_h1 = """{%- liquid
      assign tara_home_hidden_h1 = 'tara.homepage_meta.hidden_h1' | t
    -%}
    <h1 class="visually-hidden">
      {%- if template.name == 'index' -%}
        {{ tara_home_hidden_h1 }}
      {%- else -%}
        {{ shop.name }}
      {%- endif -%}
    </h1>"""
    replacement_p = replacement_h1.replace("<h1", "<p", 1).replace("</h1>", "</p>", 1)

    if store_name in {"kuwait", "saudi"}:
        original = '<h1 class="visually-hidden">{{ shop.name }}</h1>'
        if original not in content:
            raise RuntimeError("Could not find header hidden H1 in header.liquid")
        return content.replace(original, replacement_h1, 1)

    if store_name == "spain":
        original = '<p class="visually-hidden">{{ shop.name }}</p>'
        if original not in content:
            raise RuntimeError("Could not find header hidden paragraph in header.liquid")
        return content.replace(original, replacement_p, 1)

    raise RuntimeError(f"Unsupported store for header patch: {store_name}")


def apply_to_store(store_name: str, dry_run: bool = False) -> dict:
    payload = load_payload()
    conf = STORE_CONFIG[store_name]
    shop_url = os.environ[conf["shop_env"]]
    token = os.environ[conf["token_env"]]
    client = ShopifyClient(shop_url, token)
    theme_id = client.get_main_theme_id()
    if not theme_id:
        raise RuntimeError(f"No main theme found for {store_name}")

    report = {
        "store": store_name,
        "shop_url": shop_url,
        "theme_id": theme_id,
        "updated_assets": [],
    }

    for key, patcher in (
        ("snippets/meta-tags.liquid", patch_meta_tags),
        ("sections/header.liquid", patch_header),
    ):
        asset = client.get_asset(theme_id, key)
        current_value = asset.get("value", "")
        updated_value = patcher(current_value, store_name)
        if updated_value != current_value:
            report["updated_assets"].append(key)
            if not dry_run:
                client.put_asset(theme_id, key, value=updated_value)

    for locale_code, asset_key in conf["locale_assets"].items():
        asset = client.get_asset(theme_id, asset_key)
        comment, data = split_locale_asset(asset.get("value", ""))
        before = json.dumps(data.get("tara", {}).get("homepage_meta", {}), ensure_ascii=False, sort_keys=True)
        ensure_homepage_meta_keys(data, payload[store_name][locale_code])
        after = json.dumps(data.get("tara", {}).get("homepage_meta", {}), ensure_ascii=False, sort_keys=True)
        if before != after:
            report["updated_assets"].append(asset_key)
            if not dry_run:
                client.put_asset(theme_id, asset_key, value=dump_locale_asset(comment, data))

    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="Apply homepage meta locale values to live themes")
    parser.add_argument("--store", choices=sorted(STORE_CONFIG.keys()))
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    load_dotenv(dotenv_path=ROOT / ".env")
    stores = [args.store] if args.store else list(STORE_CONFIG.keys())
    results = [apply_to_store(store, dry_run=args.dry_run) for store in stores]
    print(json.dumps(results, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
