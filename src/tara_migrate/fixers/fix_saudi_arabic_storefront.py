#!/usr/bin/env python3
"""Repair recurring Saudi Arabic storefront issues on the live Shopify theme.

Fixes:
  - Arabic internal CTA links that drop users onto non-Arabic routes
  - Header store-locator links still using the legacy Spanish handle
  - Ingredient card image aspect-ratio drift
  - Missing recoverable ingredient images on ingredient cards/detail pages
  - Broken article images still pointing at taraformula.es
  - Missing Arabic theme translations for consultation CTAs
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[3]
SRC_ROOT = ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))
try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

from tara_migrate.client import ShopifyClient  # noqa: E402
from tara_migrate.core.graphql_queries import fetch_translatable_resources, upload_translations  # noqa: E402

ENV_PATH = ROOT / ".env"
FILE_MAP_PATH = ROOT / "data" / "file_map.json"
EN_METAOBJECTS_PATH = ROOT / "data" / "english" / "metaobjects.json"
AR_PROGRESS_PATH = ROOT / "data" / "arabic" / "_translation_progress_ar.json"
REPORT_PATH = ROOT / "audit_report" / "saudi_ar_storefront_fix_report_2026-04-05.json"

THEME_ASSETS_TO_PATCH = (
    "assets/blog-tara-article.js",
    "blocks/_ingredient-detail-image.liquid",
    "blocks/_blog-post-content.liquid",
    "sections/main-blog-post.liquid",
    "snippets/button.liquid",
    "snippets/header-actions.liquid",
    "snippets/header-drawer.liquid",
    "snippets/ingredient-card-content.liquid",
)

TRANSLATION_MAP = {
    "Start Consultation": "ابدئي الاستشارة",
    "Hair Consultation": "استشارة الشعر",
}

INGREDIENT_IMAGE_HANDLES = (
    "black-garlic",
    "ceramide-np",
    "grape-seed-oil",
    "plant-collagen",
    "yeast-extract",
)


def _load_json(path: Path, default):
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def _save_json(path: Path, payload) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _build_live_clients() -> tuple[ShopifyClient, ShopifyClient]:
    load_dotenv(dotenv_path=ENV_PATH)
    saudi_shop = os.getenv("SAUDI_SHOP_URL") or os.getenv("SOURCE_SHOP_URL")
    saudi_token = os.getenv("SAUDI_ACCESS_TOKEN") or os.getenv("SOURCE_ACCESS_TOKEN")
    spain_shop = os.getenv("SPAIN_SHOP_URL")
    spain_token = os.getenv("SPAIN_ACCESS_TOKEN")
    if not saudi_shop or not saudi_token:
        raise RuntimeError("Missing Saudi shop credentials in .env")
    if not spain_shop or not spain_token:
        raise RuntimeError("Missing Spain shop credentials in .env")
    return ShopifyClient(saudi_shop, saudi_token), ShopifyClient(spain_shop, spain_token)


def _patch_button_snippet(raw: str) -> str:
    if "assign locale_root = request.locale.root_url" in raw:
        return raw

    locale_block = (
        "\n"
        "    assign locale_root = request.locale.root_url | default: routes.root_url\n"
        "    unless resolved_link contains '://'\n"
        "      assign resolved_link_first_char = resolved_link | slice: 0, 1\n"
        "      if locale_root != '/' and resolved_link_first_char == '/'\n"
        "        unless resolved_link contains locale_root\n"
        "          assign resolved_link = locale_root | append: resolved_link | replace: '//', '/'\n"
        "        endunless\n"
        "      endif\n"
        "    endunless\n"
    )

    if "assign resolved_link = link" in raw:
        needle = (
            "    assign resolved_link = resolved_link | replace: '/ar/pages/quiz', '/ar/pages/consultation'\n"
            "    assign resolved_link = resolved_link | replace: '/pages/quiz', '/pages/consultation'\n"
            "  endif\n"
        )
        replacement = (
            "    assign resolved_link = resolved_link | replace: '/ar/pages/quiz', '/ar/pages/consultation'\n"
            "    assign resolved_link = resolved_link | replace: '/pages/quiz', '/pages/consultation'\n"
            f"{locale_block}"
            "  endif\n"
        )
        return raw.replace(needle, replacement, 1)

    needle = "  assign style_class = block_settings.style_class\n"
    replacement = (
        "  assign style_class = block_settings.style_class\n"
        "  assign resolved_link = link\n\n"
        "  if resolved_link != blank\n"
        "    assign resolved_link = resolved_link | replace: '/ar/pages/quiz-results', '/ar/pages/consultation-results'\n"
        "    assign resolved_link = resolved_link | replace: '/pages/quiz-results', '/pages/consultation-results'\n"
        "    assign resolved_link = resolved_link | replace: '/ar/pages/quiz', '/ar/pages/consultation'\n"
        "    assign resolved_link = resolved_link | replace: '/pages/quiz', '/pages/consultation'\n"
        f"{locale_block}"
        "  endif\n"
    )
    updated = raw.replace(needle, replacement, 1)
    updated = updated.replace("{% if link == blank %}", "{% if resolved_link == blank %}", 1)
    updated = updated.replace('href="{{ link }}"', 'href="{{ resolved_link }}"', 1)
    return updated


def _patch_store_locator_links(raw: str) -> str:
    return raw.replace(
        "pages['puntos-de-venta'].url | default: '/pages/puntos-de-venta'",
        "pages['store-locator'].url | default: pages['puntos-de-venta'].url | default: '/pages/store-locator'",
    )


def _patch_ingredient_card_snippet(raw: str) -> str:
    updated = raw
    if "assign card_media = ingredient.image.value" not in updated:
        updated = updated.replace(
            "  assign card_alt = ingredient.name.value | escape\n"
            "  assign card_loading = loading | default: 'lazy'\n",
            "  assign card_alt = ingredient.name.value | escape\n"
            "  assign card_loading = loading | default: 'lazy'\n"
            "  assign card_media = ingredient.image.value\n"
            "  if card_media == blank and ingredient.icon.value != blank\n"
            "    assign card_media = ingredient.icon.value\n"
            "  endif\n\n"
            "  if card_media == blank\n"
            "    assign name_handle = ingredient.name.value | handleize\n"
            "    assign sys_handle = ingredient.system.handle\n"
            "    assign ingredient_collection = collections[name_handle]\n"
            "    if ingredient_collection == blank or ingredient_collection.products.size == 0\n"
            "      assign ingredient_collection = collections[sys_handle]\n"
            "    endif\n"
            "    if ingredient_collection == blank or ingredient_collection.products.size == 0\n"
            "      assign prefixed_handle = 'ingredient-' | append: name_handle\n"
            "      assign ingredient_collection = collections[prefixed_handle]\n"
            "    endif\n"
            "    if ingredient_collection == blank or ingredient_collection.products.size == 0\n"
            "      assign prefixed_handle = 'ingredient-' | append: sys_handle\n"
            "      assign ingredient_collection = collections[prefixed_handle]\n"
            "    endif\n"
            "    if ingredient_collection != blank\n"
            "      if ingredient_collection.image != blank\n"
            "        assign card_media = ingredient_collection.image\n"
            "      elsif ingredient_collection.products.size > 0 and ingredient_collection.products.first.featured_image != blank\n"
            "        assign card_media = ingredient_collection.products.first.featured_image\n"
            "      endif\n"
            "    endif\n"
            "  endif\n",
            1,
        )
    updated = updated.replace(
        "    {%- if ingredient.image.value != blank -%}\n"
        "      {{- ingredient.image.value | image_url: width: 600 | image_tag:\n"
        "            loading: card_loading,\n"
        "            class: 'ing-card__img',\n"
        "            alt: card_alt,\n"
        "            width: ingredient.image.value.width,\n"
        "            height: ingredient.image.value.height,\n"
        "            widths: '200,400,600',\n"
        "            sizes: '(min-width: 750px) 25vw, 50vw' -}}\n"
        "    {%- elsif ingredient.icon.value != blank -%}\n"
        "      {{- ingredient.icon.value | image_url: width: 600 | image_tag:\n"
        "            loading: card_loading,\n"
        "            class: 'ing-card__img',\n"
        "            alt: card_alt,\n"
        "            width: ingredient.icon.value.width,\n"
        "            height: ingredient.icon.value.height,\n"
        "            widths: '200,400,600',\n"
        "            sizes: '(min-width: 750px) 25vw, 50vw' -}}\n"
        "    {%- else -%}\n",
        "    {%- if card_media != blank -%}\n"
        "      {{- card_media | image_url: width: 600 | image_tag:\n"
        "            loading: card_loading,\n"
        "            class: 'ing-card__img',\n"
        "            alt: card_alt,\n"
        "            width: card_media.width,\n"
        "            height: card_media.height,\n"
        "            widths: '200,400,600',\n"
        "            sizes: '(min-width: 750px) 25vw, 50vw' -}}\n"
        "    {%- else -%}\n",
        1,
    )
    if "aspect-ratio: var(--ing-card-image-ratio, 1 / 1);" not in updated:
        updated = updated.replace(
            "  .ing-card__image {\n    position: relative;\n    overflow: hidden;\n    background: rgb(var(--color-foreground-rgb) / 0.04);\n  }\n",
            "  .ing-card__image {\n    position: relative;\n    overflow: hidden;\n    background: rgb(var(--color-foreground-rgb) / 0.04);\n    aspect-ratio: var(--ing-card-image-ratio, 1 / 1);\n  }\n",
            1,
        )
    updated = updated.replace(
        "  .ing-card__placeholder {\n    display: flex;\n    align-items: center;\n    justify-content: center;\n    aspect-ratio: 1;\n",
        "  .ing-card__placeholder {\n    display: flex;\n    align-items: center;\n    justify-content: center;\n    width: 100%;\n    height: 100%;\n    aspect-ratio: auto;\n",
        1,
    )
    if "  .ing-card--compact .ing-card__image {\n    flex: 0 0 88px;\n    width: 88px;\n    height: 88px;\n    aspect-ratio: 1 / 1;\n    align-self: flex-start;\n  }\n" not in updated:
        updated = updated.replace(
            "  .ing-card--compact .ing-card__image {\n    flex: 0 0 80px;\n  }\n",
            "  .ing-card--compact .ing-card__image {\n    flex: 0 0 88px;\n    width: 88px;\n    height: 88px;\n    aspect-ratio: 1 / 1;\n    align-self: flex-start;\n  }\n",
            1,
        )
    return updated


def _patch_ingredient_detail_image_block(raw: str) -> str:
    if "assign ingredient_collection = collections[name_handle]" in raw:
        return raw

    needle = (
        "  assign block_settings = block.settings\n"
        "  assign ing = closest.metaobject.ingredient\n"
        "  assign image = block_settings.image | default: ing.image.value\n"
    )
    replacement = (
        "  assign block_settings = block.settings\n"
        "  assign ing = closest.metaobject.ingredient\n"
        "  assign image = block_settings.image | default: ing.image.value\n"
        "  if image == blank\n"
        "    assign name_handle = ing.name.value | handleize\n"
        "    assign sys_handle = ing.system.handle\n"
        "    assign ingredient_collection = collections[name_handle]\n"
        "    if ingredient_collection == blank or ingredient_collection.products.size == 0\n"
        "      assign ingredient_collection = collections[sys_handle]\n"
        "    endif\n"
        "    if ingredient_collection == blank or ingredient_collection.products.size == 0\n"
        "      assign prefixed_handle = 'ingredient-' | append: name_handle\n"
        "      assign ingredient_collection = collections[prefixed_handle]\n"
        "    endif\n"
        "    if ingredient_collection == blank or ingredient_collection.products.size == 0\n"
        "      assign prefixed_handle = 'ingredient-' | append: sys_handle\n"
        "      assign ingredient_collection = collections[prefixed_handle]\n"
        "    endif\n"
        "    if ingredient_collection != blank\n"
        "      if ingredient_collection.image != blank\n"
        "        assign image = ingredient_collection.image\n"
        "      elsif ingredient_collection.products.size > 0 and ingredient_collection.products.first.featured_image != blank\n"
        "        assign image = ingredient_collection.products.first.featured_image\n"
        "      endif\n"
        "    endif\n"
        "  endif\n"
    )
    return raw.replace(needle, replacement, 1)


def _patch_blog_article_script(raw: str) -> str:
    if "#normalizeLegacyMediaUrls()" in raw:
        return raw

    connected_needle = (
        "    const showToc = this.dataset.showToc === 'true';\n"
        "    const showProgress = this.dataset.showProgress === 'true';\n\n"
    )
    connected_replacement = (
        "    const showToc = this.dataset.showToc === 'true';\n"
        "    const showProgress = this.dataset.showProgress === 'true';\n\n"
        "    this.#normalizeLegacyMediaUrls();\n\n"
    )
    updated = raw.replace(connected_needle, connected_replacement, 1)

    disconnected_needle = (
        "  disconnectedCallback() {\n"
        "    super.disconnectedCallback();\n"
        "    this.#abortController.abort();\n"
        "  }\n\n"
    )
    disconnected_replacement = (
        "  disconnectedCallback() {\n"
        "    super.disconnectedCallback();\n"
        "    this.#abortController.abort();\n"
        "  }\n\n"
        "  #normalizeLegacyMediaUrls() {\n"
        "    const prose = this.querySelector('.article-tara__prose');\n"
        "    if (!prose) return;\n\n"
        "    const normalize = (value) => value\n"
        "      .replaceAll('https://taraformula.es/media/wysiwyg/', 'https://taraformula.com/media/wysiwyg/')\n"
        "      .replaceAll('//taraformula.es/media/wysiwyg/', 'https://taraformula.com/media/wysiwyg/');\n\n"
        "    for (const image of prose.querySelectorAll('img[src], img[srcset]')) {\n"
        "      if (image.hasAttribute('src')) {\n"
        "        image.setAttribute('src', normalize(image.getAttribute('src')));\n"
        "      }\n"
        "      if (image.hasAttribute('srcset')) {\n"
        "        image.setAttribute('srcset', normalize(image.getAttribute('srcset')));\n"
        "      }\n"
        "    }\n\n"
        "    for (const source of prose.querySelectorAll('source[srcset]')) {\n"
        "      source.setAttribute('srcset', normalize(source.getAttribute('srcset')));\n"
        "    }\n"
        "  }\n\n"
    )
    return updated.replace(disconnected_needle, disconnected_replacement, 1)


def _patch_blog_post_content_block(raw: str) -> str:
    if "assign normalized_article_content = article.content" in raw:
        return raw

    doc_end = "{%- enddoc -%}\n"
    liquid_block = (
        "{%- liquid\n"
        "  assign normalized_article_content = article.content\n"
        "  assign normalized_article_content = normalized_article_content | replace: 'https://taraformula.es/media/wysiwyg/', 'https://taraformula.com/media/wysiwyg/'\n"
        "  assign normalized_article_content = normalized_article_content | replace: '//taraformula.es/media/wysiwyg/', 'https://taraformula.com/media/wysiwyg/'\n"
        "-%}\n\n"
    )
    updated = raw.replace(doc_end, doc_end + liquid_block, 1)
    return updated.replace("{{ article.content }}", "{{ normalized_article_content }}", 1)


def _patch_main_blog_post_section(raw: str) -> str:
    if "function normalizeLegacyMedia(root)" in raw:
        return raw

    needle = (
        "<script type=\"application/ld+json\">\n"
        "  {{ article | structured_data }}\n"
        "</script>\n"
    )
    replacement = (
        "<script type=\"application/ld+json\">\n"
        "  {{ article | structured_data }}\n"
        "</script>\n\n"
        "<script>\n"
        "  (function() {\n"
        "    function normalizeLegacyMedia(root) {\n"
        "      if (!root) return;\n\n"
        "      var normalize = function(value) {\n"
        "        return (value || '')\n"
        "          .replace(/https:\\/\\/taraformula\\.es\\/media\\/wysiwyg\\//g, 'https://taraformula.com/media/wysiwyg/')\n"
        "          .replace(/\\/\\/taraformula\\.es\\/media\\/wysiwyg\\//g, 'https://taraformula.com/media/wysiwyg/');\n"
        "      };\n\n"
        "      root.querySelectorAll('img[src], img[srcset]').forEach(function(node) {\n"
        "        if (node.hasAttribute('src')) {\n"
        "          node.setAttribute('src', normalize(node.getAttribute('src')));\n"
        "        }\n"
        "        if (node.hasAttribute('srcset')) {\n"
        "          node.setAttribute('srcset', normalize(node.getAttribute('srcset')));\n"
        "        }\n"
        "      });\n\n"
        "      root.querySelectorAll('source[srcset]').forEach(function(node) {\n"
        "        node.setAttribute('srcset', normalize(node.getAttribute('srcset')));\n"
        "      });\n"
        "    }\n\n"
        "    if (document.readyState === 'loading') {\n"
        "      document.addEventListener('DOMContentLoaded', function() {\n"
        "        normalizeLegacyMedia(document.querySelector('.blog-post-content'));\n"
        "      }, { once: true });\n"
        "    } else {\n"
        "      normalizeLegacyMedia(document.querySelector('.blog-post-content'));\n"
        "    }\n"
        "  })();\n"
        "</script>\n"
    )
    return raw.replace(needle, replacement, 1)


def patch_theme_assets(client: ShopifyClient, dry_run: bool) -> list[dict[str, str]]:
    theme_id = client.get_main_theme_id()
    if not theme_id:
        raise RuntimeError("Could not resolve the live Saudi theme ID")

    changes: list[dict[str, str]] = []
    for key in THEME_ASSETS_TO_PATCH:
        original = client.get_asset(theme_id, key).get("value", "") or ""
        updated = original
        if key == "assets/blog-tara-article.js":
            updated = _patch_blog_article_script(updated)
        elif key == "blocks/_ingredient-detail-image.liquid":
            updated = _patch_ingredient_detail_image_block(updated)
        elif key == "blocks/_blog-post-content.liquid":
            updated = _patch_blog_post_content_block(updated)
        elif key == "sections/main-blog-post.liquid":
            updated = _patch_main_blog_post_section(updated)
        elif key == "snippets/button.liquid":
            updated = _patch_button_snippet(updated)
        elif key in {"snippets/header-actions.liquid", "snippets/header-drawer.liquid"}:
            updated = _patch_store_locator_links(updated)
        elif key == "snippets/ingredient-card-content.liquid":
            updated = _patch_ingredient_card_snippet(updated)

        if updated == original:
            continue

        if not dry_run:
            client.put_asset(theme_id, key, value=updated)
        changes.append({"asset": key, "status": "patched"})

    return changes


def patch_theme_translations(client: ShopifyClient, dry_run: bool) -> list[dict[str, str]]:
    theme_id = client.get_main_theme_id()
    theme_gid = f"gid://shopify/OnlineStoreTheme/{theme_id}"
    resource = fetch_translatable_resources(client, [theme_gid], "ar").get(theme_gid, {})
    content = resource.get("content", {})
    translations = resource.get("translations", {})

    changes: list[dict[str, str]] = []
    for key, entry in content.items():
        source_value = entry.get("value")
        target_value = TRANSLATION_MAP.get(source_value)
        if not target_value:
            continue
        current_translation = translations.get(key, {})
        if current_translation.get("value") == target_value and not current_translation.get("outdated"):
            continue

        translation_input = [{
            "locale": "ar",
            "key": key,
            "value": target_value,
            "translatableContentDigest": entry["digest"],
        }]
        if not dry_run:
            upload_translations(client, theme_gid, translation_input)
        changes.append({"key": key, "english": source_value, "arabic": target_value})

    return changes


def _get_source_image_map() -> dict[str, str]:
    metaobjects = _load_json(EN_METAOBJECTS_PATH, {})
    objects = metaobjects.get("ingredient", {}).get("objects", [])
    by_handle = {obj.get("handle"): obj for obj in objects}
    image_map = {}
    for handle in INGREDIENT_IMAGE_HANDLES:
        obj = by_handle.get(handle)
        if not obj:
            continue
        for field in obj.get("fields", []):
            if field.get("key") == "image" and field.get("value"):
                image_map[handle] = field["value"]
                break
    return image_map


def _get_image_url(client: ShopifyClient, file_gid: str) -> str | None:
    node = client.get_file_by_id(file_gid)
    if not node:
        return None
    if node.get("image"):
        return node["image"].get("url")
    return node.get("url")


def _current_field_map(metaobject: dict) -> dict[str, dict]:
    return {field["key"]: field for field in metaobject.get("fields", [])}


def _is_missing_or_invalid_ref(client: ShopifyClient, ref_gid: str | None) -> bool:
    if not ref_gid:
        return True
    node = client.get_file_by_id(ref_gid)
    return not bool(node)


def fix_ingredient_images(saudi: ShopifyClient, spain: ShopifyClient, dry_run: bool) -> dict[str, list[dict[str, str]]]:
    source_image_map = _get_source_image_map()
    file_map = _load_json(FILE_MAP_PATH, {})
    report = {"updated": [], "skipped": []}

    for handle in INGREDIENT_IMAGE_HANDLES:
        source_gid = source_image_map.get(handle)
        if not source_gid:
            report["skipped"].append({"handle": handle, "reason": "no_source_image"})
            continue

        dest_object = saudi.get_metaobjects_by_handle("ingredient", handle)
        if not dest_object:
            report["skipped"].append({"handle": handle, "reason": "missing_saudi_metaobject"})
            continue

        current_fields = _current_field_map(dest_object)
        current_ref = current_fields.get("image", {}).get("value")
        if not _is_missing_or_invalid_ref(saudi, current_ref):
            report["skipped"].append({"handle": handle, "reason": "already_valid"})
            continue

        dest_ref = file_map.get(source_gid)
        if dest_ref and _is_missing_or_invalid_ref(saudi, dest_ref):
            dest_ref = None

        if not dest_ref:
            source_url = _get_image_url(spain, source_gid)
            if not source_url:
                report["skipped"].append({"handle": handle, "reason": "source_image_unavailable"})
                continue
            if dry_run:
                report["updated"].append({"handle": handle, "status": "would_upload"})
                continue
            dest_ref = saudi.upload_file_from_url(source_url, filename=f"{handle}.jpg", alt=handle)
            if not dest_ref:
                report["skipped"].append({"handle": handle, "reason": "upload_failed"})
                continue
            file_map[source_gid] = dest_ref

        if not dry_run:
            saudi.update_metaobject(dest_object["id"], [{"key": "image", "value": dest_ref}])
        report["updated"].append({"handle": handle, "image_gid": dest_ref})

    if not dry_run:
        _save_json(FILE_MAP_PATH, file_map)
    return report


def fix_legacy_article_images(client: ShopifyClient, dry_run: bool) -> list[dict[str, str]]:
    changes: list[dict[str, str]] = []
    progress_ar = _load_json(AR_PROGRESS_PATH, {})
    for blog in client.get_blogs():
        for article in client.get_articles(blog["id"]):
            body_html = article.get("body_html") or ""
            changed_source = False
            changed_translation = False

            if "taraformula.es/media/wysiwyg/" in body_html:
                updated_body = body_html.replace("https://taraformula.es/media/wysiwyg/", "https://taraformula.com/media/wysiwyg/")
                if updated_body != body_html:
                    if not dry_run:
                        client._request(
                            "PUT",
                            f"blogs/{blog['id']}/articles/{article['id']}.json",
                            json={"article": {"id": article["id"], "body_html": updated_body}},
                        )
                    changed_source = True

            article_gid = article.get("admin_graphql_api_id")
            if article_gid:
                resource = fetch_translatable_resources(client, [article_gid], "ar").get(article_gid, {})
                content = resource.get("content", {})
                translations = resource.get("translations", {})
                translation_inputs = []

                for key in ("summary_html", "body_html"):
                    translated_value = (translations.get(key) or {}).get("value") or ""
                    progress_key = f"art.{article['handle']}.{key}"
                    progress_value = progress_ar.get(progress_key) or ""
                    candidate_value = translated_value
                    if progress_value and len(progress_value) > len(translated_value):
                        candidate_value = progress_value
                    if "taraformula.es/media/wysiwyg/" not in candidate_value:
                        continue
                    digest = (content.get(key) or {}).get("digest")
                    if not digest:
                        continue
                    translation_inputs.append({
                        "locale": "ar",
                        "key": key,
                        "value": candidate_value.replace("https://taraformula.es/media/wysiwyg/", "https://taraformula.com/media/wysiwyg/"),
                        "translatableContentDigest": digest,
                    })

                if translation_inputs:
                    if not dry_run:
                        upload_translations(client, article_gid, translation_inputs)
                    changed_translation = True

            if changed_source or changed_translation:
                changes.append({
                    "article": article["handle"],
                    "blog_id": str(blog["id"]),
                    "source_updated": str(changed_source).lower(),
                    "arabic_translation_updated": str(changed_translation).lower(),
                })
    return changes


def main() -> None:
    parser = argparse.ArgumentParser(description="Fix live Saudi Arabic storefront issues")
    parser.add_argument("--dry-run", action="store_true", help="Preview changes without mutating Shopify")
    args = parser.parse_args()

    saudi, spain = _build_live_clients()
    report = {
        "shop": saudi.shop_url,
        "dry_run": args.dry_run,
        "theme_asset_changes": patch_theme_assets(saudi, args.dry_run),
        "theme_translation_changes": patch_theme_translations(saudi, args.dry_run),
        "ingredient_image_changes": fix_ingredient_images(saudi, spain, args.dry_run),
        "legacy_article_image_changes": fix_legacy_article_images(saudi, args.dry_run),
    }
    _save_json(REPORT_PATH, report)
    print(json.dumps(report, ensure_ascii=False, indent=2))
    print(f"\nSaved report to {REPORT_PATH}")


if __name__ == "__main__":
    main()
