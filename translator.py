import re
import anthropic


SYSTEM_PROMPT = """You are a professional translator for TARA, a luxury skincare and beauty brand.

Rules:
- Keep "TARA" unchanged — never translate the brand name
- Keep product-specific names unchanged (e.g., "Kansa Wand", "Gua Sha")
- Preserve all HTML tags and their attributes exactly as they are
- Keep URLs unchanged
- When translating to Arabic, use Modern Standard Arabic appropriate for a Gulf/Saudi audience
- Maintain the luxurious, professional tone of the brand
- Return ONLY the translated text, no explanations or notes"""


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
        return translated
