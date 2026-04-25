#!/usr/bin/env python3
"""Prepare a clean English-only USA payload from the Saudi English export."""

import argparse
import json
import re
from pathlib import Path

from tara_migrate.core.utils import ascii_slugify

SOURCE_DIR = Path("data/source_export")
DEST_DIR = Path("data/usa/english")

CONTENT_FILES = [
    "products.json",
    "collections.json",
    "pages.json",
    "blogs.json",
    "articles.json",
    "metaobjects.json",
    "metaobject_definitions.json",
    "redirects.json",
]

SEO_REPLACEMENTS = {
    "Acondicionador Suavizante y Nutritivo | TARA": "Smoothing & Nourishing Conditioner | TARA",
    "Repairing & Strengthening Shampoo — Black Garlic + Ceramidas | TARA": "Repairing & Strengthening Shampoo — Black Garlic + Ceramides | TARA",
    "Acondicionador Hidratante Fresa + NMF | TARA": "Hydrating Conditioner with Strawberry + NMF | TARA",
    "Fresa + NMF hair routine": "Strawberry + NMF hair routine",
    "Productos Capilares Premiados | TARA": "Award-Winning Haircare | TARA",
    "Tipo de Producto | TARA": "Shop by Product Type | TARA",
}

ARTICLE_TEXT_REPLACEMENTS = {
    "https://taraformula.es/es-es/collections/acondicionadores": "/collections/conditioners",
    "https://taraformula.es/es-es/collections/champus": "/collections/shampoos",
    "https://taraformula.es/es-es/collections/mascarillas-capilares": "/collections/hair-masks",
    "https://taraformula.es/es-es/collections/productos-de-acabado": "/collections/finishing-products",
    "https://taraformula.es/es-es/collections/serums-cuero-cabelludo": "/collections/scalp-serums",
    "https://taraformula.es/es-es/pages/ingredient/acetil-tetrapeptido-3": "/pages/ingredient/acetyl-tetrapeptide-3",
    "https://taraformula.es/es-es/pages/ingredient/ceramida-np": "/pages/ingredient/ceramide-np",
    "https://taraformula.es/es-es/pages/ingredients": "/pages/ingredients",
    "https://taraformula.es/es-es/pages/quiz": "/pages/quiz",
    "https://taraformula.es/es-es/products/champu-exfoliante": "/products/charcoal-salicylic-exfoliating-shampoo",
    "https://taraformula.es/es-es/products/rutina-reparadora-y-fortalecedora": "/products/hair-strength-system",
    "https://taraformula.es/es-es/products/serum-cuero-cabelludo": "/products/rejuvenating-scalp-serum",
    "Ingredientes Tara": "Tara Ingredients",
    "Diagnóstico Capilar": "Hair Diagnostic",
    "Quiz rutina capilar": "Hair Routine Quiz",
    "Rutina Reparadora y Fortalecedora": "Repairing and Strengthening Routine",
    "Rejuvenecedor capilar": "Rejuvenating Scalp Serum",
    "Serums cuero cabelludo": "Scalp serums",
    "Champús Tara": "Tara Shampoos",
    "Acondicionadores Tara": "Tara Conditioners",
    "Mascarillas Tara": "Tara Hair Masks",
    "Productos de acabado Tara": "Tara Finishing Products",
}

SPANISH_TERM_PATTERN = re.compile(
    r"\b(cabello|cuero|ca[ií]da|cebolla|romero|grasa|sequedad|canas|fortalecedor|tratamiento|"
    r"champ[uú]s?|acondicionador(?:es)?|ingredientes|rutina|reparadora|fortalecedora|filosof[ií]a|"
    r"farmacias|ceramidas|fresa|diagn[oó]stico|capilar(?:es)?|nutritivo|engrosamiento|suavizante|fortalecimiento|"
    r"protecci[oó]n|antica[ií]da|hidrataci[oó]n|hidratante|brillo|reparador|purificante|calvicie|"
    r"densificante|regenerador|limpiador|otros|tel[oó]geno|l[ií]quido|dañado|perdida|pérdida|"
    r"envejecimiento|aceite|uva|vitamina)\b",
    re.IGNORECASE,
)

INGREDIENT_CONCERN_MAP = {
    "Caída": "Hair Loss",
    "Falta de Densidad": "Thinning",
    "Canas": "Gray Hair",
    "Daño y Rotura": "Damage & Breakage",
    "Sequedad": "Dryness",
    "Grasa": "Oiliness",
    "Crecimiento Lento": "Slow Growth",
    "Sensibilidad": "Sensitivity",
    "Caspa": "Dandruff",
    "Envejecimiento": "Hair Aging",
    "Fuerza": "Strength",
    "Acumulación": "Buildup",
}

INGREDIENT_CATEGORY_MAP = {
    "Vegetal": "Botanical",
    "Biotecnología": "Biotechnology",
    "Vitaminas": "Vitamins",
    "Ácidos": "Acids",
    "Minerales": "Minerals",
}

TYPE_VALUE_MAPS = {
    "shopify--conditioner-effect": {
        "Nutritivo": "Nourishing",
        "Engrosamiento": "Thickening",
        "Suavizante": "Smoothing",
        "Fortalecimiento": "Strengthening",
        "Antiencrespamiento": "Anti-frizz",
        "Reparador": "Repairing",
        "Protección": "Protection",
        "Hidratante": "Hydrating",
        "Hidratación": "Hydrating",
        "Brillo": "Shine",
        "Anticaída": "Anti-hair-loss",
        "Voluminizador": "Volumizing",
        "Regenerador": "Regenerating",
        "Regeneración": "Regenerating",
        "Densificante": "Densifying",
        "Densidad": "Densifying",
        "Limpiador": "Cleansing",
        "Limpieza": "Cleansing",
        "Anticaspa": "Anti-dandruff",
        "Purificante": "Purifying",
        "Revitalizante": "Revitalizing",
    },
    "shopify--constitutive-ingredients": {
        "Aceite de oliva": "Olive Oil",
        "Aceite de semilla de uva": "Grape Seed Oil",
        "Vitamina E": "Vitamin E",
    },
    "shopify--hair-care-items-included": {
        "Acondicionador": "Conditioner",
        "Sérum capilar": "Hair Serum",
        "Champú": "Shampoo",
        "Crema para el cabello": "Hair Cream",
    },
    "shopify--hair-loss-type": {
        "Calvicie masculina": "Male Pattern Baldness",
        "Efluvio telógeno": "Telogen Effluvium",
    },
    "shopify--product-certifications-standards": {
        "Ingredientes naturales": "Natural Ingredients",
        "Libre de parabeno": "Paraben Free",
        "Libre de crueldad": "Cruelty Free",
        "Sin sulfatos": "Sulfate Free",
        "Sin silicona": "Silicone Free",
        "Sin aceites": "Oil Free",
    },
    "shopify--product-form": {
        "Líquido": "Liquid",
    },
    "shopify--scalp-concern": {
        "Cabello dañado": "Damaged Hair",
        "Adelgazamiento del cabello": "Hair Thinning",
        "Alopecia": "Alopecia",
        "Pérdida de cabello": "Hair Loss",
        "Envejecimiento capilar": "Hair Aging",
        "Picor del cuero cabelludo": "Scalp Itch",
        "Cuero cabelludo sensible": "Sensitive Scalp",
        "Caspa": "Dandruff",
        "Otros": "Other",
    },
}


def load_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def save_json(path: Path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def repair_mojibake(text: str) -> str:
    if not isinstance(text, str):
        return text
    if not any(marker in text for marker in ("Ã", "â", "€", "Â")):
        return text
    try:
        return text.encode("latin1").decode("utf-8")
    except UnicodeError:
        return text


def normalize_strings(value):
    if isinstance(value, str):
        return repair_mojibake(value)
    if isinstance(value, list):
        return [normalize_strings(item) for item in value]
    if isinstance(value, dict):
        return {key: normalize_strings(item) for key, item in value.items()}
    return value


def get_field_value(obj, key):
    for field in obj.get("fields", []):
        if field.get("key") == key:
            return field.get("value")
    return None


def set_field_value(obj, key, value):
    for field in obj.get("fields", []):
        if field.get("key") == key:
            field["value"] = value
            return


def replace_text(text: str, replacements: dict[str, str]) -> str:
    for old, new in replacements.items():
        text = text.replace(old, new)
    return text


def englishize_metaobjects(metaobjects):
    for obj_type, payload in metaobjects.items():
        for obj in payload.get("objects", []):
            if obj_type == "benefit":
                title = get_field_value(obj, "title")
                if title:
                    obj["handle"] = ascii_slugify(title)
                continue

            if obj_type == "faq_entry":
                question = get_field_value(obj, "question")
                if question:
                    obj["handle"] = ascii_slugify(question)
                continue

            if obj_type == "ingredient":
                name = get_field_value(obj, "name")
                if name:
                    obj["handle"] = ascii_slugify(name)
                concern = get_field_value(obj, "concern")
                category = get_field_value(obj, "category")
                if concern in INGREDIENT_CONCERN_MAP:
                    set_field_value(obj, "concern", INGREDIENT_CONCERN_MAP[concern])
                if category in INGREDIENT_CATEGORY_MAP:
                    set_field_value(obj, "category", INGREDIENT_CATEGORY_MAP[category])
                continue

            label_map = TYPE_VALUE_MAPS.get(obj_type)
            if not label_map:
                continue

            current = get_field_value(obj, "label") or get_field_value(obj, "name")
            if current in label_map:
                translated = label_map[current]
                if get_field_value(obj, "label") is not None:
                    set_field_value(obj, "label", translated)
                elif get_field_value(obj, "name") is not None:
                    set_field_value(obj, "name", translated)
                current = translated
            if current:
                obj["handle"] = ascii_slugify(current)
    return metaobjects


def clean_products(products):
    for product in products:
        tags = [tag.strip() for tag in (product.get("tags") or "").split(",") if tag.strip()]
        normalized_tags = []
        for tag in tags:
            if tag == "Diagnóstico Capilar":
                tag = "Hair Diagnostic"
            if tag not in normalized_tags:
                normalized_tags.append(tag)
        product["tags"] = ", ".join(normalized_tags)
        for metafield in product.get("metafields", []):
            if metafield.get("namespace") == "global" and metafield.get("key") in {"title_tag", "description_tag"}:
                value = metafield.get("value")
                if isinstance(value, str):
                    metafield["value"] = replace_text(value, SEO_REPLACEMENTS)
    return products


def clean_collections(collections):
    for collection in collections:
        for metafield in collection.get("metafields", []):
            if metafield.get("namespace") == "global" and metafield.get("key") in {"title_tag", "description_tag"}:
                value = metafield.get("value")
                if isinstance(value, str):
                    metafield["value"] = replace_text(value, SEO_REPLACEMENTS)
    return collections


def clean_pages(pages):
    for page in pages:
        for metafield in page.get("metafields", []):
            if metafield.get("namespace") == "global" and metafield.get("key") in {"title_tag", "description_tag"}:
                value = metafield.get("value")
                if isinstance(value, str):
                    metafield["value"] = replace_text(value, SEO_REPLACEMENTS)
    return pages


def clean_articles(articles):
    for article in articles:
        if article.get("body_html"):
            body_html = replace_text(article["body_html"], ARTICLE_TEXT_REPLACEMENTS)
            article["body_html"] = replace_text(body_html, SEO_REPLACEMENTS)
        for metafield in article.get("metafields", []):
            value = metafield.get("value")
            if isinstance(value, str):
                metafield["value"] = replace_text(value, ARTICLE_TEXT_REPLACEMENTS)
                metafield["value"] = replace_text(metafield["value"], SEO_REPLACEMENTS)
    return articles


def audit_hits(data):
    text = json.dumps(data, ensure_ascii=False)
    return len(SPANISH_TERM_PATTERN.findall(text))


def main():
    parser = argparse.ArgumentParser(description="Prepare a clean English USA payload from Saudi export")
    parser.add_argument("--dry-run", action="store_true", help="Audit only, do not write files")
    args = parser.parse_args()

    datasets = {
        name: normalize_strings(load_json(SOURCE_DIR / name))
        for name in CONTENT_FILES
        if (SOURCE_DIR / name).exists()
    }

    datasets["products.json"] = clean_products(datasets["products.json"])
    datasets["collections.json"] = clean_collections(datasets["collections.json"])
    datasets["pages.json"] = clean_pages(datasets["pages.json"])
    datasets["articles.json"] = clean_articles(datasets["articles.json"])
    datasets["metaobjects.json"] = englishize_metaobjects(datasets["metaobjects.json"])
    datasets["redirects.json"] = []

    print("USA English payload audit:")
    for name in CONTENT_FILES:
        if name not in datasets:
            continue
        print(f"  {name}: {audit_hits(datasets[name])} Spanish-pattern hits")

    if args.dry_run:
        return

    for name, payload in datasets.items():
        save_json(DEST_DIR / name, payload)

    print(f"\nPrepared {DEST_DIR}")


if __name__ == "__main__":
    main()
