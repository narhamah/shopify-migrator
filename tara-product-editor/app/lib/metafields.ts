export function canInlineEditMetafield(type: string) {
  return ![
    "file_reference",
    "list.file_reference",
    "product_reference",
    "list.product_reference",
    "collection_reference",
    "list.collection_reference",
    "metaobject_reference",
    "list.metaobject_reference",
  ].includes(type);
}

export function isTextLikeTranslatableMetafieldType(type: string) {
  return [
    "single_line_text_field",
    "multi_line_text_field",
    "rich_text_field",
    "string",
  ].includes(type);
}
