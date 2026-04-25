import type { DiscoveryConfig, ProductEditorModel } from "../../app/types/editor";

export const productEditorModelFixture: ProductEditorModel = {
  product: {
    id: "gid://shopify/Product/1",
    legacyId: "1",
    title: "Volumizing Shampoo",
    handle: "volumizing-shampoo",
    descriptionHtml: "<p>English description for testing.</p>",
    vendor: "Tara",
    productType: "Shampoo",
    tags: ["shop hair"],
    status: "ACTIVE",
    seo: {
      title: "Volumizing Shampoo SEO",
      description: "English SEO description",
    },
    options: [],
    media: [],
  },
  arabicFields: [
    {
      resourceId: "gid://shopify/Product/1",
      label: "Arabic title",
      key: "title",
      translationKey: "title",
      group: "Title and description",
      fieldType: "product_field",
      sourceValue: "Volumizing Shampoo",
      arabicValue: "شامبو التكثيف",
      digest: "digest-title",
    },
    {
      resourceId: "gid://shopify/Product/1",
      label: "Arabic description",
      key: "body_html",
      translationKey: "body_html",
      group: "Title and description",
      fieldType: "product_field",
      sourceValue: "<p>English description for testing.</p>",
      arabicValue: "<p>وصف عربي للاختبار.</p>",
      digest: "digest-description",
    },
    {
      resourceId: "gid://shopify/Metafield/10",
      label: "Ingredients content",
      key: "value",
      translationKey: "value",
      group: "Metafields",
      fieldType: "single_line_text_field",
      sourceValue: "English ingredient copy",
      arabicValue: "محتوى المكونات بالعربية",
      digest: "digest-metafield",
    },
  ],
  metafields: [
    {
      id: "gid://shopify/Metafield/10",
      namespace: "custom",
      key: "ingredients",
      type: "list.metaobject_reference",
      name: "Ingredients",
      value: '["gid://shopify/Metaobject/1"]',
      referenceIds: ["gid://shopify/Metaobject/1"],
      references: [
        {
          id: "gid://shopify/Metaobject/1",
          kind: "METAOBJECT",
          title: "Black Garlic",
          localizedTitle: "الثوم الأسود",
          subtitle: "Ingredient",
          localizedSubtitle: "مكوّن",
          metaobject: {
            definitionId: "gid://shopify/MetaobjectDefinition/1",
            definitionName: "Ingredient",
            displayNameKey: "name",
            translatable: true,
            fields: [
              {
                key: "name",
                label: "Name",
                type: "single_line_text_field",
                value: "Black Garlic",
                arabicValue: "الثوم الأسود",
                isTranslatable: true,
              },
            ],
          },
        },
      ],
      isPinned: false,
      isPopulated: true,
      allowedMetaobjectTypes: ["ingredient"],
    },
  ],
  imageGroups: [],
  metaobjectDefinitions: [
    {
      id: "gid://shopify/MetaobjectDefinition/1",
      type: "ingredient",
      name: "Ingredient",
      displayNameKey: "name",
      translatable: true,
      fieldDefinitions: [
        {
          key: "name",
          name: "Name",
          required: true,
          type: "single_line_text_field",
        },
      ],
    },
  ],
  raw: {
    productTranslatableKeys: ["title", "body_html"],
    metafieldDefinitions: [],
    translationResourceIds: ["gid://shopify/Product/1", "gid://shopify/Metafield/10"],
    lastDiscoveredAt: "2026-03-22T00:00:00.000Z",
  },
};

export const discoveryFixture: DiscoveryConfig = {
  schemaVersion: 1,
  shop: "tara-saudi",
  generatedAt: "2026-03-22T00:00:00.000Z",
  sampledProducts: 1,
  definitionCount: 1,
  notes: [],
  rawDefinitions: [],
  productContentMappings: {
    coreFields: [],
    metafields: [],
  },
  translationMappings: {
    product: [],
    metafields: [],
  },
  imageMetafieldMappings: {
    groups: [],
  },
};

export const navigationFixture = {
  queue: ["gid://shopify/Product/1"],
  currentIndex: 0,
  returnTo: "/app/products",
};
