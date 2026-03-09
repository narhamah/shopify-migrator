"""Extract translatable fields from Shopify resource data structures.

Each extractor takes a resource dict and a prefix string, and returns a
list of {id, value} dicts suitable for TOON encoding and batch translation.
"""

# Metafield types that contain translatable text
TEXT_METAFIELD_TYPES = {
    "single_line_text_field",
    "multi_line_text_field",
    "rich_text_field",
}


def extract_product_fields(product, prefix):
    """Extract all translatable text fields from a product."""
    fields = []
    pid = product.get("handle", product.get("id", ""))

    # Handle (URL slug)
    if product.get("handle"):
        fields.append({"id": f"{prefix}.{pid}.handle", "value": product["handle"]})

    # Core fields
    if product.get("title"):
        fields.append({"id": f"{prefix}.{pid}.title", "value": product["title"]})
    if product.get("body_html"):
        fields.append({"id": f"{prefix}.{pid}.body_html", "value": product["body_html"]})
    if product.get("product_type"):
        fields.append({"id": f"{prefix}.{pid}.product_type", "value": product["product_type"]})
    if product.get("vendor"):
        fields.append({"id": f"{prefix}.{pid}.vendor", "value": product["vendor"]})
    if product.get("tags"):
        tags = product["tags"] if isinstance(product["tags"], str) else ", ".join(product["tags"])
        fields.append({"id": f"{prefix}.{pid}.tags", "value": tags})

    # Variant options
    for i, v in enumerate(product.get("variants", [])):
        if v.get("title") and v["title"] != "Default Title":
            fields.append({"id": f"{prefix}.{pid}.v{i}.title", "value": v["title"]})
        for opt_key in ["option1", "option2", "option3"]:
            if v.get(opt_key) and v[opt_key] != "Default Title":
                fields.append({"id": f"{prefix}.{pid}.v{i}.{opt_key}", "value": v[opt_key]})

    # Options
    for i, opt in enumerate(product.get("options", [])):
        if opt.get("name"):
            fields.append({"id": f"{prefix}.{pid}.opt{i}.name", "value": opt["name"]})
        for j, val in enumerate(opt.get("values", [])):
            fields.append({"id": f"{prefix}.{pid}.opt{i}.val{j}", "value": val})

    # Image alt text
    for i, img in enumerate(product.get("images", [])):
        if img.get("alt"):
            fields.append({"id": f"{prefix}.{pid}.img{i}.alt", "value": img["alt"]})

    # All text-type metafields
    for mf in product.get("metafields", []):
        mf_type = mf.get("type", "")
        ns_key = f"{mf.get('namespace', '')}.{mf.get('key', '')}"
        if mf_type in TEXT_METAFIELD_TYPES and mf.get("value"):
            fields.append({"id": f"{prefix}.{pid}.mf.{ns_key}", "value": mf["value"]})

    return fields


def extract_collection_fields(collection, prefix):
    fields = []
    cid = collection.get("handle", collection.get("id", ""))
    if collection.get("handle"):
        fields.append({"id": f"{prefix}.{cid}.handle", "value": collection["handle"]})
    if collection.get("title"):
        fields.append({"id": f"{prefix}.{cid}.title", "value": collection["title"]})
    if collection.get("body_html"):
        fields.append({"id": f"{prefix}.{cid}.body_html", "value": collection["body_html"]})
    # Collection image alt text
    if collection.get("image") and collection["image"].get("alt"):
        fields.append({"id": f"{prefix}.{cid}.image.alt", "value": collection["image"]["alt"]})
    # All text-type metafields
    for mf in collection.get("metafields", []):
        mf_type = mf.get("type", "")
        ns_key = f"{mf.get('namespace', '')}.{mf.get('key', '')}"
        if mf_type in TEXT_METAFIELD_TYPES and mf.get("value"):
            fields.append({"id": f"{prefix}.{cid}.mf.{ns_key}", "value": mf["value"]})
    return fields


def extract_page_fields(page, prefix):
    fields = []
    pid = page.get("handle", page.get("id", ""))
    if page.get("handle"):
        fields.append({"id": f"{prefix}.{pid}.handle", "value": page["handle"]})
    if page.get("title"):
        fields.append({"id": f"{prefix}.{pid}.title", "value": page["title"]})
    if page.get("body_html"):
        fields.append({"id": f"{prefix}.{pid}.body_html", "value": page["body_html"]})
    # All text-type metafields
    for mf in page.get("metafields", []):
        mf_type = mf.get("type", "")
        ns_key = f"{mf.get('namespace', '')}.{mf.get('key', '')}"
        if mf_type in TEXT_METAFIELD_TYPES and mf.get("value"):
            fields.append({"id": f"{prefix}.{pid}.mf.{ns_key}", "value": mf["value"]})
    return fields


def extract_blog_fields(blog, prefix):
    fields = []
    bid = blog.get("handle", blog.get("id", ""))
    if blog.get("title"):
        fields.append({"id": f"{prefix}.{bid}.title", "value": blog["title"]})
    if blog.get("handle"):
        fields.append({"id": f"{prefix}.{bid}.handle", "value": blog["handle"]})
    if blog.get("tags"):
        tags = blog["tags"] if isinstance(blog["tags"], str) else ", ".join(blog["tags"])
        fields.append({"id": f"{prefix}.{bid}.tags", "value": tags})
    return fields


def extract_article_fields(article, prefix):
    fields = []
    aid = article.get("handle", article.get("id", ""))
    if article.get("handle"):
        fields.append({"id": f"{prefix}.{aid}.handle", "value": article["handle"]})
    if article.get("title"):
        fields.append({"id": f"{prefix}.{aid}.title", "value": article["title"]})
    if article.get("body_html"):
        fields.append({"id": f"{prefix}.{aid}.body_html", "value": article["body_html"]})
    if article.get("summary_html"):
        fields.append({"id": f"{prefix}.{aid}.summary_html", "value": article["summary_html"]})
    if article.get("author"):
        fields.append({"id": f"{prefix}.{aid}.author", "value": article["author"]})
    if article.get("tags"):
        tags = article["tags"] if isinstance(article["tags"], str) else ", ".join(article["tags"])
        fields.append({"id": f"{prefix}.{aid}.tags", "value": tags})
    # Image alt text
    if article.get("image") and article["image"].get("alt"):
        fields.append({"id": f"{prefix}.{aid}.image.alt", "value": article["image"]["alt"]})
    # All text-type metafields
    for mf in article.get("metafields", []):
        mf_type = mf.get("type", "")
        ns_key = f"{mf.get('namespace', '')}.{mf.get('key', '')}"
        if mf_type in TEXT_METAFIELD_TYPES and mf.get("value"):
            fields.append({"id": f"{prefix}.{aid}.mf.{ns_key}", "value": mf["value"]})

    return fields


def extract_metaobject_fields(metaobjects_data, prefix):
    """Extract all text-type fields from all metaobjects."""
    fields = []
    for mo_type, type_data in metaobjects_data.items():
        for obj in type_data.get("objects", []):
            handle = obj.get("handle", obj.get("id", ""))
            # Metaobject handle
            if obj.get("handle"):
                fields.append({"id": f"{prefix}.{mo_type}.{handle}.handle", "value": obj["handle"]})
            # All text-type fields (type-based, not whitelist-based)
            for field in obj.get("fields", []):
                field_type = field.get("type", "")
                if field_type in TEXT_METAFIELD_TYPES and field.get("value"):
                    fid = f"{prefix}.{mo_type}.{handle}.{field['key']}"
                    fields.append({"id": fid, "value": field["value"]})
    return fields
