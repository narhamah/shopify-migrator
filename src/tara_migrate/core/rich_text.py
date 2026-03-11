"""Shopify rich_text_field JSON utilities.

Handles detection, text extraction, translation-safe rebuild, and sanitization
of Shopify's rich_text_field JSON format.

Structure:
    {"type": "root", "children": [
        {"type": "paragraph", "children": [
            {"type": "text", "value": "The actual text content"}
        ]}
    ]}
"""

import copy
import json
import re


def is_rich_text_json(value):
    """Check if a value is Shopify rich_text JSON.

    Returns True if the value parses as JSON with {"type": "root"}.
    """
    if not value or not isinstance(value, str):
        return False
    s = value.strip()
    if not s.startswith("{"):
        return False
    try:
        data = json.loads(s)
        return isinstance(data, dict) and data.get("type") == "root"
    except (json.JSONDecodeError, TypeError):
        return False


def extract_text(json_str):
    """Extract all text values from rich_text JSON.

    Returns plain text string (space-joined) or None if not parseable.
    """
    try:
        data = json.loads(json_str)
    except (json.JSONDecodeError, TypeError):
        return None
    parts = []

    def walk(node):
        if isinstance(node, dict):
            if node.get("type") == "text" and "value" in node:
                parts.append(node["value"])
            for child in node.get("children", []):
                walk(child)
        elif isinstance(node, list):
            for item in node:
                walk(item)

    walk(data)
    return " ".join(parts) if parts else None


def extract_text_nodes(json_str):
    """Extract text values with their paths from rich_text JSON.

    Returns (texts, parsed_data) where texts is a list of (path, text_value) tuples.
    Path is a list of keys/indices to navigate back to each text node's "value".

    Use with rebuild() to safely translate rich_text without corrupting JSON structure.
    """
    data = json.loads(json_str)
    texts = []

    def walk(node, path):
        if isinstance(node, dict):
            if node.get("type") == "text" and "value" in node:
                texts.append((list(path) + ["value"], node["value"]))
            for i, child in enumerate(node.get("children", [])):
                walk(child, path + ["children", i])
        elif isinstance(node, list):
            for i, item in enumerate(node):
                walk(item, path + [i])

    walk(data, [])
    return texts, data


def rebuild(parsed_data, translations):
    """Replace text values in parsed rich_text JSON using path->translation map.

    Args:
        parsed_data: The parsed JSON from extract_text_nodes().
        translations: dict of {tuple(path): translated_text}.

    Returns JSON string with translated text and intact structure.
    """
    data = copy.deepcopy(parsed_data)
    for path, translated_text in translations.items():
        node = data
        for step in path[:-1]:
            node = node[step]
        node[path[-1]] = translated_text
    return json.dumps(data, ensure_ascii=False)


def sanitize(value):
    """Fix rich_text JSON corrupted by translation.

    The translator can introduce literal newlines/control chars inside
    JSON string values. This re-serializes the JSON to fix them.
    Returns the original value if not JSON or not fixable.
    """
    if not value or not isinstance(value, str):
        return value
    if not value.strip().startswith("{"):
        return value
    try:
        parsed = json.loads(value)
        return json.dumps(parsed, ensure_ascii=False)
    except json.JSONDecodeError:
        # Try fixing common corruption patterns
        fixed = value
        fixed = fixed.replace('\\\r\n', '\\n').replace('\\\n', '\\n').replace('\\\r', '\\n')
        fixed = fixed.replace('\r\n', '\\n').replace('\n', '\\n').replace('\r', '\\n')
        fixed = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f]', '', fixed)
        try:
            parsed = json.loads(fixed)
            return json.dumps(parsed, ensure_ascii=False)
        except json.JSONDecodeError:
            # Last resort: strip all control chars
            fixed = re.sub(r'[\x00-\x1f]', '', value)
            try:
                parsed = json.loads(fixed)
                return json.dumps(parsed, ensure_ascii=False)
            except json.JSONDecodeError:
                return value


def validate_json(value):
    """Validate that a value is valid JSON. Returns (is_valid, parsed_or_error)."""
    try:
        parsed = json.loads(value)
        return True, parsed
    except (json.JSONDecodeError, TypeError) as e:
        return False, str(e)
