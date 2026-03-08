import json
import re

import anthropic


SYSTEM_PROMPT = """You are a professional translator for TARA, a luxury skincare and beauty brand specializing in scalp care and hair health.

Rules:
- Keep "TARA" unchanged — never translate the brand name
- Keep product-specific names unchanged (e.g., "Kansa Wand", "Gua Sha")
- Keep ingredient scientific names (INCI names) unchanged
- Preserve all HTML tags and their attributes exactly as they are
- Preserve Shopify Liquid tags ({{ }}, {% %}) unchanged
- Keep URLs unchanged
- Keep JSON structure unchanged if the input is JSON (translate only string values)
- When translating to Arabic, use Modern Standard Arabic appropriate for a Gulf/Saudi audience
- Maintain the luxurious, professional tone of the brand
- Scalp-first philosophy: the brand treats scalp as skin, focuses on root causes
- Return ONLY the translated text, no explanations or notes"""


# Metaobject field types that contain translatable text
TRANSLATABLE_FIELD_TYPES = {
    "single_line_text_field",
    "multi_line_text_field",
    "rich_text_field",
}

# Metaobject fields to translate per type
METAOBJECT_TRANSLATABLE_FIELDS = {
    "benefit": {"title", "description", "category", "icon_label"},
    "faq_entry": {"question", "answer"},
    "blog_author": {"name", "bio"},
    "ingredient": {
        "name", "one_line_benefit", "description", "source", "origin",
        "category", "concern",
    },
}

# Product metafields that contain translatable text
PRODUCT_TRANSLATABLE_METAFIELDS = {
    "custom.tagline",
    "custom.short_description",
    "custom.size_ml",
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
}

# Article metafields that contain translatable text
ARTICLE_TRANSLATABLE_METAFIELDS = {
    "custom.blog_summary",
    "custom.hero_caption",
    "custom.short_title",
}


class Translator:
    def __init__(self, api_key):
        self.client = anthropic.Anthropic(api_key=api_key)
        self.model = "claude-sonnet-4-20250514"

    def translate(self, text, source_lang, target_lang):
        if not text or not text.strip():
            return text
        # Skip if text is only HTML tags with no translatable content
        stripped = re.sub(r"<[^>]+>", "", text).strip()
        if not stripped:
            return text

        message = self.client.messages.create(
            model=self.model,
            max_tokens=4096,
            system=SYSTEM_PROMPT,
            messages=[{
                "role": "user",
                "content": f"Translate the following from {source_lang} to {target_lang}. Return ONLY the translation:\n\n{text}",
            }],
        )
        return message.content[0].text

    def translate_rich_text(self, rich_text_json, source_lang, target_lang):
        """Translate a Shopify rich text field (JSON string with text nodes)."""
        if not rich_text_json or not rich_text_json.strip():
            return rich_text_json
        try:
            data = json.loads(rich_text_json)
        except (json.JSONDecodeError, TypeError):
            # Not JSON, treat as plain text
            return self.translate(rich_text_json, source_lang, target_lang)

        def translate_nodes(nodes):
            for node in nodes:
                if node.get("type") == "text" and node.get("value"):
                    node["value"] = self.translate(node["value"], source_lang, target_lang)
                if node.get("children"):
                    translate_nodes(node["children"])

        if isinstance(data, dict) and data.get("children"):
            translate_nodes(data["children"])
        return json.dumps(data, ensure_ascii=False)

    def translate_product(self, product, source_lang, target_lang):
        translated = dict(product)
        translated["title"] = self.translate(product.get("title", ""), source_lang, target_lang)
        translated["body_html"] = self.translate(product.get("body_html", ""), source_lang, target_lang)
        translated["product_type"] = self.translate(product.get("product_type", ""), source_lang, target_lang)

        if product.get("tags"):
            tags = product["tags"] if isinstance(product["tags"], str) else ", ".join(product["tags"])
            translated["tags"] = self.translate(tags, source_lang, target_lang)

        if product.get("variants"):
            translated["variants"] = []
            for variant in product["variants"]:
                tv = dict(variant)
                if variant.get("title") and variant["title"] != "Default Title":
                    tv["title"] = self.translate(variant["title"], source_lang, target_lang)
                if variant.get("option1"):
                    tv["option1"] = self.translate(variant["option1"], source_lang, target_lang)
                if variant.get("option2"):
                    tv["option2"] = self.translate(variant["option2"], source_lang, target_lang)
                if variant.get("option3"):
                    tv["option3"] = self.translate(variant["option3"], source_lang, target_lang)
                translated["variants"].append(tv)

        if product.get("options"):
            translated["options"] = []
            for option in product["options"]:
                to = dict(option)
                to["name"] = self.translate(option.get("name", ""), source_lang, target_lang)
                if option.get("values"):
                    to["values"] = [self.translate(v, source_lang, target_lang) for v in option["values"]]
                translated["options"].append(to)

        # Translate product metafields
        if product.get("metafields"):
            translated["metafields"] = []
            for mf in product["metafields"]:
                tmf = dict(mf)
                ns_key = f"{mf.get('namespace', '')}.{mf.get('key', '')}"
                if ns_key in PRODUCT_TRANSLATABLE_METAFIELDS and mf.get("value"):
                    if mf.get("type") == "rich_text_field":
                        tmf["value"] = self.translate_rich_text(mf["value"], source_lang, target_lang)
                    else:
                        tmf["value"] = self.translate(mf["value"], source_lang, target_lang)
                translated["metafields"].append(tmf)

        return translated

    def translate_page(self, page, source_lang, target_lang):
        translated = dict(page)
        translated["title"] = self.translate(page.get("title", ""), source_lang, target_lang)
        translated["body_html"] = self.translate(page.get("body_html", ""), source_lang, target_lang)
        return translated

    def translate_collection(self, collection, source_lang, target_lang):
        translated = dict(collection)
        translated["title"] = self.translate(collection.get("title", ""), source_lang, target_lang)
        translated["body_html"] = self.translate(collection.get("body_html", ""), source_lang, target_lang)
        return translated

    def translate_article(self, article, source_lang, target_lang):
        translated = dict(article)
        translated["title"] = self.translate(article.get("title", ""), source_lang, target_lang)
        translated["body_html"] = self.translate(article.get("body_html", ""), source_lang, target_lang)
        translated["summary_html"] = self.translate(article.get("summary_html", ""), source_lang, target_lang)
        if article.get("tags"):
            tags = article["tags"] if isinstance(article["tags"], str) else ", ".join(article["tags"])
            translated["tags"] = self.translate(tags, source_lang, target_lang)

        # Translate article metafields
        if article.get("metafields"):
            translated["metafields"] = []
            for mf in article["metafields"]:
                tmf = dict(mf)
                ns_key = f"{mf.get('namespace', '')}.{mf.get('key', '')}"
                if ns_key in ARTICLE_TRANSLATABLE_METAFIELDS and mf.get("value"):
                    tmf["value"] = self.translate(mf["value"], source_lang, target_lang)
                translated["metafields"].append(tmf)

        return translated

    def translate_metaobject(self, metaobject, source_lang, target_lang):
        """Translate a metaobject's text fields based on its type."""
        mo_type = metaobject.get("type", "")
        translatable_keys = METAOBJECT_TRANSLATABLE_FIELDS.get(mo_type, set())

        translated = dict(metaobject)
        translated["fields"] = []
        for field in metaobject.get("fields", []):
            tf = dict(field)
            if field["key"] in translatable_keys and field.get("value"):
                field_type = field.get("type", "")
                if field_type == "rich_text_field":
                    tf["value"] = self.translate_rich_text(field["value"], source_lang, target_lang)
                else:
                    tf["value"] = self.translate(field["value"], source_lang, target_lang)
            translated["fields"].append(tf)
        return translated
