import type { DiscoveryConfig } from "~/types/editor";

export const DISCOVERY_SCHEMA_VERSION = 2;

export const TARA_DISCOVERY_SEED: Omit<DiscoveryConfig, "shop" | "generatedAt"> = {
  schemaVersion: DISCOVERY_SCHEMA_VERSION,
  sampledProducts: 8,
  definitionCount: 23,
  notes: [
    "Live schema discovery on March 21, 2026 found custom.pdp_images and custom.pdp_images_ar as the active locale image metafields.",
    "The source product resource exposes translatable keys title, body_html, handle, product_type, meta_title, and meta_description.",
    "Custom PDP content metafields such as short_description, tagline, key_benefits_*, how_to_use_*, whats_inside_*, fragrance_*, and free_of_* are individually translatable through the Shopify translations APIs.",
    "The app treats custom.pdp_images_ar as the active Arabic PDP gallery. Historical translated image values and custom.arabic_images are kept for diagnostics and cleanup only.",
  ],
  rawDefinitions: [
    { id: "custom.short_description", namespace: "custom", key: "short_description", name: "Short Description", type: "single_line_text_field" },
    { id: "custom.tagline", namespace: "custom", key: "tagline", name: "Tagline", type: "single_line_text_field" },
    { id: "custom.key_benefits_heading", namespace: "custom", key: "key_benefits_heading", name: "Key Benefits Heading", type: "single_line_text_field" },
    { id: "custom.key_benefits_content", namespace: "custom", key: "key_benefits_content", name: "Key Benefits Content", type: "rich_text_field" },
    { id: "custom.clinical_results_heading", namespace: "custom", key: "clinical_results_heading", name: "Clinical Results Heading", type: "single_line_text_field" },
    { id: "custom.clinical_results_content", namespace: "custom", key: "clinical_results_content", name: "Clinical Results Content", type: "rich_text_field" },
    { id: "custom.how_to_use_heading", namespace: "custom", key: "how_to_use_heading", name: "How to Use Heading", type: "single_line_text_field" },
    { id: "custom.how_to_use_content", namespace: "custom", key: "how_to_use_content", name: "How to Use Content", type: "rich_text_field" },
    { id: "custom.whats_inside_heading", namespace: "custom", key: "whats_inside_heading", name: "What's Inside Heading", type: "single_line_text_field" },
    { id: "custom.whats_inside_content", namespace: "custom", key: "whats_inside_content", name: "What's Inside Content", type: "rich_text_field" },
    { id: "custom.fragrance_heading", namespace: "custom", key: "fragrance_heading", name: "Fragrance Heading", type: "single_line_text_field" },
    { id: "custom.fragrance_content", namespace: "custom", key: "fragrance_content", name: "Fragrance Content", type: "rich_text_field" },
    { id: "custom.free_of_heading", namespace: "custom", key: "free_of_heading", name: "Free Of Heading", type: "single_line_text_field" },
    { id: "custom.free_of_content", namespace: "custom", key: "free_of_content", name: "Free Of Content", type: "rich_text_field" },
    { id: "custom.pdp_images", namespace: "custom", key: "pdp_images", name: "PDP Images", type: "list.file_reference" },
    { id: "custom.pdp_images_ar", namespace: "custom", key: "pdp_images_ar", name: "PDP Images (Arabic)", type: "list.file_reference" },
    { id: "custom.arabic_images", namespace: "custom", key: "arabic_images", name: "Arabic Images", type: "json" }
  ],
  productContentMappings: {
    coreFields: [
      { key: "title", label: "Title" },
      { key: "handle", label: "Handle" },
      { key: "descriptionHtml", label: "Description" },
      { key: "vendor", label: "Vendor" },
      { key: "productType", label: "Product type" },
      { key: "tags", label: "Tags" },
      { key: "status", label: "Status" },
      { key: "seo.title", label: "SEO title" },
      { key: "seo.description", label: "SEO description" }
    ],
    metafields: [
      { namespace: "custom", key: "short_description", label: "Short description", type: "single_line_text_field", group: "Hero" },
      { namespace: "custom", key: "tagline", label: "Tagline", type: "single_line_text_field", group: "Hero" },
      { namespace: "custom", key: "key_benefits_heading", label: "Key benefits heading", type: "single_line_text_field", group: "Key Benefits" },
      { namespace: "custom", key: "key_benefits_content", label: "Key benefits content", type: "rich_text_field", group: "Key Benefits" },
      { namespace: "custom", key: "clinical_results_heading", label: "Clinical results heading", type: "single_line_text_field", group: "Clinical Results" },
      { namespace: "custom", key: "clinical_results_content", label: "Clinical results content", type: "rich_text_field", group: "Clinical Results" },
      { namespace: "custom", key: "how_to_use_heading", label: "How to use heading", type: "single_line_text_field", group: "How To Use" },
      { namespace: "custom", key: "how_to_use_content", label: "How to use content", type: "rich_text_field", group: "How To Use" },
      { namespace: "custom", key: "whats_inside_heading", label: "What's inside heading", type: "single_line_text_field", group: "What's Inside" },
      { namespace: "custom", key: "whats_inside_content", label: "What's inside content", type: "rich_text_field", group: "What's Inside" },
      { namespace: "custom", key: "fragrance_heading", label: "Fragrance heading", type: "single_line_text_field", group: "Fragrance" },
      { namespace: "custom", key: "fragrance_content", label: "Fragrance content", type: "rich_text_field", group: "Fragrance" },
      { namespace: "custom", key: "free_of_heading", label: "Free of heading", type: "single_line_text_field", group: "Free Of" },
      { namespace: "custom", key: "free_of_content", label: "Free of content", type: "rich_text_field", group: "Free Of" },
      { namespace: "custom", key: "size_ml", label: "Size", type: "single_line_text_field", group: "Hero" }
    ]
  },
  translationMappings: {
    product: [
      { id: "product.title", label: "Arabic title", key: "title", type: "product", sourceResourceType: "PRODUCT", resourceKey: "title", translationKey: "title", fieldType: "product_field", group: "Product" },
      { id: "product.body_html", label: "Arabic description", key: "body_html", type: "product", sourceResourceType: "PRODUCT", resourceKey: "body_html", translationKey: "body_html", fieldType: "rich_text_field", group: "Product" },
      { id: "product.meta_title", label: "Arabic SEO title", key: "meta_title", type: "product", sourceResourceType: "PRODUCT", resourceKey: "meta_title", translationKey: "meta_title", fieldType: "product_field", group: "SEO" },
      { id: "product.meta_description", label: "Arabic SEO description", key: "meta_description", type: "product", sourceResourceType: "PRODUCT", resourceKey: "meta_description", translationKey: "meta_description", fieldType: "product_field", group: "SEO" },
      { id: "product.product_type", label: "Arabic product type", key: "product_type", type: "product", sourceResourceType: "PRODUCT", resourceKey: "product_type", translationKey: "product_type", fieldType: "product_field", group: "Product" },
      { id: "product.handle", label: "Arabic handle", key: "handle", type: "product", sourceResourceType: "PRODUCT", resourceKey: "handle", translationKey: "handle", fieldType: "product_field", group: "Advanced" }
    ],
    metafields: [
      { id: "custom.short_description", label: "Arabic short description", namespace: "custom", key: "short_description", type: "metafield", sourceResourceType: "METAFIELD", resourceKey: "custom.short_description", translationKey: "value", fieldType: "single_line_text_field", group: "Hero" },
      { id: "custom.tagline", label: "Arabic tagline", namespace: "custom", key: "tagline", type: "metafield", sourceResourceType: "METAFIELD", resourceKey: "custom.tagline", translationKey: "value", fieldType: "single_line_text_field", group: "Hero" },
      { id: "custom.size_ml", label: "Arabic size", namespace: "custom", key: "size_ml", type: "metafield", sourceResourceType: "METAFIELD", resourceKey: "custom.size_ml", translationKey: "value", fieldType: "single_line_text_field", group: "Hero" },
      { id: "custom.key_benefits_heading", label: "Arabic key benefits heading", namespace: "custom", key: "key_benefits_heading", type: "metafield", sourceResourceType: "METAFIELD", resourceKey: "custom.key_benefits_heading", translationKey: "value", fieldType: "single_line_text_field", group: "Key Benefits" },
      { id: "custom.key_benefits_content", label: "Arabic key benefits content", namespace: "custom", key: "key_benefits_content", type: "metafield", sourceResourceType: "METAFIELD", resourceKey: "custom.key_benefits_content", translationKey: "value", fieldType: "rich_text_field", group: "Key Benefits" },
      { id: "custom.clinical_results_heading", label: "Arabic clinical results heading", namespace: "custom", key: "clinical_results_heading", type: "metafield", sourceResourceType: "METAFIELD", resourceKey: "custom.clinical_results_heading", translationKey: "value", fieldType: "single_line_text_field", group: "Clinical Results" },
      { id: "custom.clinical_results_content", label: "Arabic clinical results content", namespace: "custom", key: "clinical_results_content", type: "metafield", sourceResourceType: "METAFIELD", resourceKey: "custom.clinical_results_content", translationKey: "value", fieldType: "rich_text_field", group: "Clinical Results" },
      { id: "custom.how_to_use_heading", label: "Arabic how to use heading", namespace: "custom", key: "how_to_use_heading", type: "metafield", sourceResourceType: "METAFIELD", resourceKey: "custom.how_to_use_heading", translationKey: "value", fieldType: "single_line_text_field", group: "How To Use" },
      { id: "custom.how_to_use_content", label: "Arabic how to use content", namespace: "custom", key: "how_to_use_content", type: "metafield", sourceResourceType: "METAFIELD", resourceKey: "custom.how_to_use_content", translationKey: "value", fieldType: "rich_text_field", group: "How To Use" },
      { id: "custom.whats_inside_heading", label: "Arabic what's inside heading", namespace: "custom", key: "whats_inside_heading", type: "metafield", sourceResourceType: "METAFIELD", resourceKey: "custom.whats_inside_heading", translationKey: "value", fieldType: "single_line_text_field", group: "What's Inside" },
      { id: "custom.whats_inside_content", label: "Arabic what's inside content", namespace: "custom", key: "whats_inside_content", type: "metafield", sourceResourceType: "METAFIELD", resourceKey: "custom.whats_inside_content", translationKey: "value", fieldType: "rich_text_field", group: "What's Inside" },
      { id: "custom.fragrance_heading", label: "Arabic fragrance heading", namespace: "custom", key: "fragrance_heading", type: "metafield", sourceResourceType: "METAFIELD", resourceKey: "custom.fragrance_heading", translationKey: "value", fieldType: "single_line_text_field", group: "Fragrance" },
      { id: "custom.fragrance_content", label: "Arabic fragrance content", namespace: "custom", key: "fragrance_content", type: "metafield", sourceResourceType: "METAFIELD", resourceKey: "custom.fragrance_content", translationKey: "value", fieldType: "rich_text_field", group: "Fragrance" },
      { id: "custom.free_of_heading", label: "Arabic free of heading", namespace: "custom", key: "free_of_heading", type: "metafield", sourceResourceType: "METAFIELD", resourceKey: "custom.free_of_heading", translationKey: "value", fieldType: "single_line_text_field", group: "Free Of" },
      { id: "custom.free_of_content", label: "Arabic free of content", namespace: "custom", key: "free_of_content", type: "metafield", sourceResourceType: "METAFIELD", resourceKey: "custom.free_of_content", translationKey: "value", fieldType: "rich_text_field", group: "Free Of" }
    ]
  },
  imageMetafieldMappings: {
    groups: [
      {
        id: "pdp-gallery",
        label: "PDP gallery",
        notes: [
          "custom.pdp_images is the English source metafield.",
          "custom.pdp_images_ar is the active Arabic PDP gallery.",
          "Historical translated custom.pdp_images values are preserved for diagnostics, but are not shown as the active Arabic gallery."
        ],
        english: [
          { id: "custom.pdp_images", label: "English source", locale: "en", storage: "metafield", namespace: "custom", key: "pdp_images", type: "list.file_reference" }
        ],
        arabic: [
          { id: "custom.pdp_images_ar", label: "Arabic metafield", locale: "ar", storage: "metafield", namespace: "custom", key: "pdp_images_ar", type: "list.file_reference" }
        ],
        legacy: [
          { id: "custom.pdp_images.translation.ar", label: "Legacy Arabic translation", locale: "ar", storage: "translation", namespace: "custom", key: "pdp_images", translationKey: "value", sourceMetafield: "custom.pdp_images", type: "list.file_reference" },
          { id: "custom.arabic_images", label: "Legacy Arabic JSON", locale: "ar", storage: "metafield", namespace: "custom", key: "arabic_images", type: "file_reference" }
        ]
      }
    ]
  }
};
