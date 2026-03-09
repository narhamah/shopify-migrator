"""Helper functions for theme image migration.

Extracted from migrate_all_images.py for reusability and clarity.
"""

from tara_migrate.core import IMAGE_KEYWORDS, SECTION_PRESETS


def is_image_setting(key):
    """Check if a theme setting key refers to an image field."""
    k = key.lower()
    # Exclude position/alignment/size sub-settings that contain image keywords
    if k.endswith(("_position", "_alignment", "_size", "_width", "_height", "_ratio")):
        return False
    return any(kw in k for kw in IMAGE_KEYWORDS)


def is_shopify_image_ref(value):
    """Check if a value is a shopify://shop_images/ reference."""
    return isinstance(value, str) and value.startswith("shopify://shop_images/")


def guess_preset(section_type, setting_key):
    """Guess the optimization preset from section type and setting key."""
    st = section_type.lower()
    for pattern, preset in SECTION_PRESETS.items():
        if pattern in st:
            return preset
    key = setting_key.lower()
    if "icon" in key:
        return "icon"
    if "logo" in key:
        return "logo"
    if "hero" in key or "banner" in key:
        return "hero"
    if "thumbnail" in key:
        return "thumbnail"
    return "default"


def resolve_shopify_image_to_url(client, image_ref):
    """Resolve shopify://shop_images/filename to a CDN URL."""
    if not is_shopify_image_ref(image_ref):
        return None
    filename = image_ref.replace("shopify://shop_images/", "")
    try:
        query = """
        query findFile($query: String!) {
          files(first: 1, query: $query) {
            nodes {
              ... on MediaImage { id image { url } }
              ... on GenericFile { id url }
            }
          }
        }
        """
        data = client._graphql(query, {"query": f"filename:{filename}"})
        nodes = data.get("files", {}).get("nodes", [])
        if nodes:
            node = nodes[0]
            img = node.get("image", {})
            if img and img.get("url"):
                return img["url"]
            if node.get("url"):
                return node["url"]
    except Exception as e:
        print(f"    Could not resolve {image_ref}: {e}")
    return None


def extract_template_images(template):
    """Extract all image settings from a Shopify template JSON."""
    sections = template.get("sections", {})
    images = []

    for section_id, section in sections.items():
        section_type = section.get("type", "unknown")
        settings = section.get("settings", {})
        blocks = section.get("blocks", {})

        for key, value in settings.items():
            if is_image_setting(key) and value:
                images.append({
                    "section_id": section_id,
                    "section_type": section_type,
                    "block_id": None,
                    "setting_key": key,
                    "value": value,
                    "preset": guess_preset(section_type, key),
                })

        for block_id, block in blocks.items():
            btype = block.get("type", "unknown")
            for key, value in block.get("settings", {}).items():
                if is_image_setting(key) and value:
                    preset = guess_preset(section_type, key)
                    if "icon" in btype.lower() or "icon" in key.lower():
                        preset = "icon"
                    images.append({
                        "section_id": section_id,
                        "section_type": section_type,
                        "block_id": block_id,
                        "block_type": btype,
                        "setting_key": key,
                        "value": value,
                        "preset": preset,
                    })

    return images
