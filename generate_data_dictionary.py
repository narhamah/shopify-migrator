#!/usr/bin/env python3
"""Generate a data dictionary from the Spain export.

Recursively inspects every field in every exported JSON file, recording:
  - Field path (e.g. products[].variants[].title)
  - Data type (str, int, float, bool, null, list, dict)
  - Sample values (first 3 non-empty)
  - Whether it looks like translatable text (Spanish)
  - Count of non-empty values

Output: data/data_dictionary.json (upload to Claude for translation audit)

Usage:
    python generate_data_dictionary.py
"""

import json
import os
import re

from utils import load_json


def detect_language_hint(value):
    """Heuristic: does this string look like it contains Spanish text?"""
    if not isinstance(value, str) or len(value) < 3:
        return None
    # Common Spanish words / patterns
    spanish_markers = [
        r'\bde\b', r'\bdel\b', r'\blos?\b', r'\blas?\b', r'\bcon\b',
        r'\bpara\b', r'\bpor\b', r'\bque\b', r'\buna?\b', r'\bes\b',
        r'\bcomo\b', r'\bmás\b', r'\btambién\b', r'\bnuestro\b',
        r'\bcabello\b', r'\bpiel\b', r'\baceit[ea]\b', r'\bcuero\b',
        r'\bcabeludo\b', r'\btratamiento\b', r'\bingredientes\b',
        r'\bdescripción\b', r'\bbeneficio\b',
        # Accented characters common in Spanish
        r'[áéíóúñ¡¿]',
    ]
    text_lower = value.lower()
    matches = sum(1 for pat in spanish_markers if re.search(pat, text_lower))
    if matches >= 2:
        return "likely_spanish"
    if matches == 1 and len(value) > 20:
        return "possibly_spanish"
    return None


def inspect_value(value, path, registry, depth=0):
    """Recursively inspect a value and register all fields."""
    if depth > 15:
        return

    if isinstance(value, dict):
        for key, val in value.items():
            child_path = f"{path}.{key}"
            inspect_value(val, child_path, registry, depth + 1)
    elif isinstance(value, list):
        array_path = f"{path}[]"
        if array_path not in registry:
            registry[array_path] = {
                "type": "array",
                "count": 0,
                "item_count": 0,
            }
        registry[array_path]["count"] += 1
        registry[array_path]["item_count"] += len(value)
        for item in value[:50]:  # Sample first 50 items
            inspect_value(item, array_path, registry, depth + 1)
    else:
        if path not in registry:
            registry[path] = {
                "type": set(),
                "count": 0,
                "non_empty": 0,
                "samples": [],
                "spanish_count": 0,
                "max_length": 0,
            }
        entry = registry[path]
        if value is None:
            entry["type"].add("null")
        elif isinstance(value, bool):
            entry["type"].add("bool")
        elif isinstance(value, int):
            entry["type"].add("int")
        elif isinstance(value, float):
            entry["type"].add("float")
        elif isinstance(value, str):
            entry["type"].add("str")
        entry["count"] += 1

        if value is not None and value != "" and value != 0:
            entry["non_empty"] += 1
            if isinstance(value, str):
                entry["max_length"] = max(entry["max_length"], len(value))
                lang = detect_language_hint(value)
                if lang:
                    entry["spanish_count"] += 1
                if len(entry["samples"]) < 3:
                    sample = value[:200] + "..." if len(value) > 200 else value
                    if sample not in entry["samples"]:
                        entry["samples"].append(sample)
            elif len(entry["samples"]) < 3:
                entry["samples"].append(str(value)[:100])


def generate_dictionary():
    export_dir = "data/spain_export"
    if not os.path.exists(export_dir):
        print("Error: data/spain_export/ not found. Run export_spain.py first.")
        return

    files = [
        ("products.json", "products"),
        ("collections.json", "collections"),
        ("pages.json", "pages"),
        ("blogs.json", "blogs"),
        ("articles.json", "articles"),
        ("metaobjects.json", "metaobjects"),
        ("metaobject_definitions.json", "metaobject_definitions"),
        ("collects.json", "collects"),
        ("redirects.json", "redirects"),
        ("price_rules.json", "price_rules"),
        ("policies.json", "policies"),
        ("shop.json", "shop"),
    ]

    full_dictionary = {}

    for filename, resource_name in files:
        filepath = os.path.join(export_dir, filename)
        if not os.path.exists(filepath):
            continue

        data = load_json(filepath)
        registry = {}

        if isinstance(data, list):
            root_path = f"{resource_name}[]"
            for item in data:
                inspect_value(item, root_path, registry)
            item_count = len(data)
        elif isinstance(data, dict):
            root_path = resource_name
            inspect_value(data, root_path, registry)
            item_count = 1
        else:
            continue

        # Convert sets to lists for JSON serialization
        for path, info in registry.items():
            if isinstance(info.get("type"), set):
                info["type"] = sorted(info["type"])

        # Add needs_translation flag
        for path, info in registry.items():
            if isinstance(info.get("type"), list) and "str" in info["type"]:
                info["needs_translation"] = info.get("spanish_count", 0) > 0
            else:
                info["needs_translation"] = False

        full_dictionary[resource_name] = {
            "source_file": filename,
            "item_count": item_count,
            "fields": registry,
        }

        # Print summary
        text_fields = [
            (path, info) for path, info in registry.items()
            if isinstance(info.get("type"), list) and "str" in info["type"]
        ]
        spanish_fields = [
            (path, info) for path, info in text_fields
            if info.get("spanish_count", 0) > 0
        ]

        print(f"\n{resource_name} ({item_count} items, {len(registry)} fields)")
        if spanish_fields:
            print(f"  Fields with Spanish text ({len(spanish_fields)}):")
            for path, info in sorted(spanish_fields, key=lambda x: -x[1]["spanish_count"]):
                pct = round(100 * info["spanish_count"] / max(info["non_empty"], 1))
                sample = info["samples"][0][:60] if info["samples"] else ""
                print(f"    {path}: {info['spanish_count']}/{info['non_empty']} ({pct}%) — e.g. \"{sample}\"")

    # Save
    output_path = "data/data_dictionary.json"
    os.makedirs("data", exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(full_dictionary, f, ensure_ascii=False, indent=2, default=str)

    print(f"\n{'='*50}")
    print(f"Data dictionary saved to: {output_path}")
    print(f"Upload this file for translation audit.")
    print(f"{'='*50}")


if __name__ == "__main__":
    generate_dictionary()
