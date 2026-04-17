import { buildEditorImageGroups } from "~/services/images.server";
import { getMetaobjectReferenceSummariesByIds } from "~/services/metaobjects.server";
import { getFavoriteMetafields } from "~/services/discovery.server";
import { getTranslatableResource, getTranslatableResourcesByIds } from "~/services/translations.server";
import { adminGraphql, type AdminGraphqlClient } from "~/lib/shopify-admin.server";
import { isTextLikeTranslatableMetafieldType } from "~/lib/metafields";
import { htmlToEditableText, richTextJsonToEditableText } from "~/lib/rich-text";
import { buildProductSearchQuery, parseReferenceValue } from "~/lib/utils";
import type {
  DiscoveryConfig,
  EditorMetafield,
  EditorTranslatableField,
  MetaobjectDefinitionOption,
  ProductEditorModel,
  ProductImageItem,
  ProductIndexItem,
  ProductIndexResult,
  ProductListFilters,
  ReferenceSummary,
  SaveResult,
} from "~/types/editor";

const INDEX_IMAGE_TRANSFORM = "transform: { maxWidth: 160, maxHeight: 160 }";
const EDITOR_IMAGE_TRANSFORM = "transform: { maxWidth: 720, maxHeight: 720 }";
const METAOBJECT_DEFINITIONS_CACHE_TTL_MS = 5 * 60 * 1000;
const metaobjectDefinitionsCache = new Map<
  string,
  { expiresAt: number; definitions: MetaobjectDefinitionOption[] }
>();

const PRODUCT_EDITOR_QUERY = `
  query ProductEditor($id: ID!) {
    product(id: $id) {
      id
      legacyResourceId
      title
      handle
      descriptionHtml
      vendor
      productType
      tags
      status
      seo {
        title
        description
      }
      options {
        id
        name
        values
      }
      media(first: 75) {
        nodes {
          ... on MediaImage {
            id
            alt
            createdAt
            image {
              url(${EDITOR_IMAGE_TRANSFORM})
              width
              height
            }
          }
        }
      }
      metafields(first: 120) {
        nodes {
          id
          compareDigest
          namespace
          key
          type
          value
          definition {
            name
            description
            validations {
              name
              value
            }
          }
          reference {
            __typename
            ... on MediaImage {
              id
              alt
              createdAt
              image {
                url(${EDITOR_IMAGE_TRANSFORM})
                width
                height
              }
            }
            ... on Product {
              id
              title
              handle
              featuredMedia {
                ... on MediaImage {
                  id
                  alt
                  createdAt
                  image {
                    url(${EDITOR_IMAGE_TRANSFORM})
                    width
                    height
                  }
                }
              }
            }
            ... on Collection {
              id
              title
              handle
              image {
                url(${EDITOR_IMAGE_TRANSFORM})
                width
                height
                altText
              }
            }
            ... on Metaobject {
              id
              displayName
              handle
              type
            }
          }
          references(first: 50) {
            nodes {
              __typename
              ... on MediaImage {
                id
                alt
                createdAt
                image {
                  url(${EDITOR_IMAGE_TRANSFORM})
                  width
                  height
                }
              }
              ... on Product {
                id
                title
                handle
                featuredMedia {
                ... on MediaImage {
                  id
                  alt
                  createdAt
                  image {
                    url(${EDITOR_IMAGE_TRANSFORM})
                    width
                    height
                  }
                }
                }
              }
              ... on Collection {
                id
                title
                handle
                image {
                  url(${EDITOR_IMAGE_TRANSFORM})
                  width
                  height
                  altText
                }
              }
              ... on Metaobject {
                id
                displayName
                handle
                type
              }
            }
          }
        }
      }
    }
  }
`;

const METAOBJECT_DEFINITIONS_QUERY = `
  query MetaobjectDefinitions($first: Int!, $after: String) {
    metaobjectDefinitions(first: $first, after: $after) {
      nodes {
        id
        name
        type
      }
      pageInfo {
        hasNextPage
        endCursor
      }
    }
  }
`;

const TRANSLATION_STATUS_QUERY = `
  query ProductTranslationStatus($resourceIds: [ID!]!, $first: Int!, $locale: String!) {
    translatableResourcesByIds(resourceIds: $resourceIds, first: $first) {
      edges {
        node {
          resourceId
          translatableContent {
            key
          }
          translations(locale: $locale) {
            key
            value
            outdated
          }
        }
      }
    }
  }
`;

const MEDIA_NODES_QUERY = `
  query MediaNodes($ids: [ID!]!) {
    nodes(ids: $ids) {
      ... on MediaImage {
        id
        alt
        createdAt
        image {
          url(${INDEX_IMAGE_TRANSFORM})
          width
          height
        }
      }
    }
  }
`;

const PRODUCT_UPDATE_MUTATION = `
  mutation ProductUpdate($product: ProductUpdateInput!) {
    productUpdate(product: $product) {
      product {
        id
      }
      userErrors {
        field
        message
      }
    }
  }
`;

export async function listProducts(
  admin: AdminGraphqlClient,
  discoveryConfig: DiscoveryConfig,
  filters: ProductListFilters,
  after?: string,
) {
  const query = buildProductSearchQuery(filters);
  const primaryGroup = discoveryConfig.imageMetafieldMappings.groups[0];
  const sourceKey = primaryGroup?.english[0]
    ? `${primaryGroup.english[0].namespace}.${primaryGroup.english[0].key}`
    : "custom.pdp_images";
  const activeArabicTargets = primaryGroup?.arabic.filter((target) => target.storage === "metafield") ?? [];
  const fallbackTranslatedTargets =
    activeArabicTargets.length === 0
      ? primaryGroup?.arabic.filter((target) => target.storage === "translation") ?? []
      : [];
  const arabicKeys = activeArabicTargets.length
    ? activeArabicTargets.map((target) => `${target.namespace}.${target.key}`)
    : ["custom.pdp_images_ar"];
  const indexMetafieldTargets = buildIndexMetafieldTargets(sourceKey, activeArabicTargets);
  const data = await adminGraphql<{
    products: {
      nodes: Array<
        {
          id: string;
          legacyResourceId: string;
          title: string;
          handle: string;
          status: string;
          vendor: string;
          productType: string;
          tags: string[];
          updatedAt: string;
          featuredMedia?: {
            id: string;
            alt?: string | null;
            createdAt?: string | null;
            image?: {
              url: string;
              width?: number | null;
              height?: number | null;
            } | null;
          } | null;
        } & Record<string, unknown>
      >;
      pageInfo: {
        hasNextPage: boolean;
        endCursor?: string | null;
      };
    };
  }>(admin, buildProductIndexQuery(indexMetafieldTargets), {
    first: 25,
    after,
    query,
  });

  const translationMap = await buildProductTranslationStatusMap(
    admin,
    data.products.nodes.map((product) => product.id),
  );

  const imageTranslationMetafieldIds = fallbackTranslatedTargets.length
    ? (data.products.nodes
        .map((product) =>
          extractIndexMetafields(product, indexMetafieldTargets).find(
            (metafield) => `${metafield.namespace}.${metafield.key}` === sourceKey,
          )?.id,
        )
        .filter(Boolean) as string[])
    : [];
  const imageTranslationMap = await getTranslatableResourcesByIds(admin, imageTranslationMetafieldIds, "ar");
  const localeThumbnailIds = data.products.nodes
    .map((product) =>
      getIndexLocaleThumbnailId({
        productMetafields: extractIndexMetafields(product, indexMetafieldTargets),
        localeView: filters.localeView || "english",
        sourceKey,
        activeArabicTargets,
        fallbackTranslatedTargets,
        imageTranslationMap,
      }),
    )
    .filter(Boolean) as string[];
  const localeThumbnailMap = await resolveProductImageItems(admin, localeThumbnailIds);

  let items: ProductIndexItem[] = data.products.nodes.map((product) => {
    const productMetafields = extractIndexMetafields(product, indexMetafieldTargets);
    const englishMetafield = productMetafields.find(
      (metafield) => `${metafield.namespace}.${metafield.key}` === sourceKey,
    );
    const englishCount = englishMetafield ? parseReferenceValue(englishMetafield.value).length : 0;
    const arabicCountCandidates = productMetafields
      .filter((metafield) => arabicKeys.includes(`${metafield.namespace}.${metafield.key}`))
      .map((metafield) => parseReferenceValue(metafield.value).length);
    const translatedArabicCount = englishMetafield && fallbackTranslatedTargets.length
      ? parseReferenceValue(
          imageTranslationMap.get(englishMetafield.id)?.translations.find((translation) => translation.key === "value")
            ?.value || "",
        ).length
      : 0;

    const arabicCounts = [...arabicCountCandidates, translatedArabicCount].filter(Boolean);
    const primaryArabicCount = arabicCounts.length ? Math.max(...arabicCounts) : 0;
    const status = translationMap.get(product.id);
    const localeThumbnailId = getIndexLocaleThumbnailId({
      productMetafields,
      localeView: filters.localeView || "english",
      sourceKey,
      activeArabicTargets,
      fallbackTranslatedTargets,
      imageTranslationMap,
    });
    const featured = product.featuredMedia?.image
      ? {
          id: product.featuredMedia.id,
          url: product.featuredMedia.image.url,
          alt: product.featuredMedia.alt,
          width: product.featuredMedia.image.width,
          height: product.featuredMedia.image.height,
          createdAt: product.featuredMedia.createdAt,
          source: "product" as const,
        }
      : null;
    const localeThumbnail = localeThumbnailId ? localeThumbnailMap.get(localeThumbnailId) || null : null;

    return {
      id: product.id,
      legacyId: product.legacyResourceId,
      title:
        filters.localeView === "arabic"
          ? status?.arabicTitle?.trim() || product.title
          : product.title,
      handle: product.handle,
      status: product.status,
      vendor: product.vendor,
      productType: product.productType,
      tags: product.tags,
      updatedAt: product.updatedAt,
      thumbnail: localeThumbnail || featured,
      translationStatus: {
        hasArabic: Boolean(status?.translatedKeys),
        translatedKeys: status?.translatedKeys || 0,
        totalKeys: status?.totalKeys || 0,
        outdatedKeys: status?.outdatedKeys || 0,
      },
      localeImageStatus: {
        hasArabic: arabicCounts.some((count) => count > 0),
        englishCount,
        arabicCount: primaryArabicCount,
        mismatch: englishCount !== primaryArabicCount,
        delta: primaryArabicCount - englishCount,
        sources: [
          ...(translatedArabicCount ? ["Translation"] : []),
          ...productMetafields
            .filter((metafield) => arabicKeys.includes(`${metafield.namespace}.${metafield.key}`))
            .filter((metafield) => parseReferenceValue(metafield.value).length > 0)
            .map((metafield) => metafield.key),
        ],
      },
    };
  });

  items = applyProductFocus(items, filters.focus || "all");
  items = sortProductIndexItems(items, filters.sort || "updated");

  const result: ProductIndexResult = {
    items,
    pageInfo: data.products.pageInfo,
    cursorState: {
      after,
      history: [],
    },
    filters,
  };

  return result;
}

type IndexMetafieldTarget = {
  alias: string;
  namespace: string;
  key: string;
};

type IndexMetafieldNode = {
  id: string;
  namespace: string;
  key: string;
  type: string;
  value: string;
};

function buildProductIndexQuery(targets: IndexMetafieldTarget[]) {
  const metafieldSelections = targets
    .map(
      (target) => `
        ${target.alias}: metafield(namespace: "${escapeGraphqlString(target.namespace)}", key: "${escapeGraphqlString(target.key)}") {
          id
          namespace
          key
          type
          value
        }
      `,
    )
    .join("\n");

  return `
    query ProductIndex($first: Int!, $after: String, $query: String) {
      products(first: $first, after: $after, query: $query, sortKey: UPDATED_AT, reverse: true) {
        nodes {
          id
          legacyResourceId
          title
          handle
          status
          vendor
          productType
          tags
          updatedAt
          featuredMedia {
            ... on MediaImage {
              id
              alt
              createdAt
              image {
                url(${INDEX_IMAGE_TRANSFORM})
                width
                height
              }
            }
          }
          ${metafieldSelections}
        }
        pageInfo {
          hasNextPage
          endCursor
        }
      }
    }
  `;
}

function buildIndexMetafieldTargets(
  sourceKey: string,
  activeArabicTargets: DiscoveryConfig["imageMetafieldMappings"]["groups"][number]["arabic"],
) {
  const uniqueKeys = [...new Set([sourceKey, ...activeArabicTargets.map((target) => `${target.namespace}.${target.key}`)])];

  return uniqueKeys.map((metafieldKey, index) => {
    const separatorIndex = metafieldKey.indexOf(".");
    return {
      alias: `imageMetafield_${index}`,
      namespace: metafieldKey.slice(0, separatorIndex),
      key: metafieldKey.slice(separatorIndex + 1),
    } satisfies IndexMetafieldTarget;
  });
}

function extractIndexMetafields(product: Record<string, unknown>, targets: IndexMetafieldTarget[]) {
  return targets
    .map((target) => product[target.alias])
    .filter((value): value is IndexMetafieldNode => {
      if (!value || typeof value !== "object") {
        return false;
      }

      return "id" in value && "namespace" in value && "key" in value && "value" in value;
    });
}

function applyProductFocus(
  items: ProductIndexItem[],
  focus: NonNullable<ProductListFilters["focus"]>,
) {
  if (focus === "missingArabic") {
    return items.filter(
      (item) => item.translationStatus.totalKeys === 0 || item.translationStatus.translatedKeys < item.translationStatus.totalKeys,
    );
  }

  if (focus === "outdatedArabic") {
    return items.filter((item) => item.translationStatus.outdatedKeys > 0);
  }

  if (focus === "imageMismatch") {
    return items.filter((item) => item.localeImageStatus.mismatch);
  }

  if (focus === "missingArabicMedia") {
    return items.filter(
      (item) => item.localeImageStatus.englishCount > 0 && item.localeImageStatus.arabicCount === 0,
    );
  }

  return items;
}

function sortProductIndexItems(
  items: ProductIndexItem[],
  sort: NonNullable<ProductListFilters["sort"]>,
) {
  const next = [...items];

  if (sort === "title") {
    return next.sort((left, right) => left.title.localeCompare(right.title));
  }

  if (sort === "arabicReadiness") {
    return next.sort((left, right) => {
      const leftCompletion = left.translationStatus.totalKeys
        ? left.translationStatus.translatedKeys / left.translationStatus.totalKeys
        : 1;
      const rightCompletion = right.translationStatus.totalKeys
        ? right.translationStatus.translatedKeys / right.translationStatus.totalKeys
        : 1;

      if (leftCompletion !== rightCompletion) {
        return leftCompletion - rightCompletion;
      }

      return new Date(right.updatedAt).getTime() - new Date(left.updatedAt).getTime();
    });
  }

  if (sort === "mediaMismatch") {
    return next.sort((left, right) => {
      if (left.localeImageStatus.mismatch !== right.localeImageStatus.mismatch) {
        return left.localeImageStatus.mismatch ? -1 : 1;
      }

      const leftDelta = Math.abs(left.localeImageStatus.delta);
      const rightDelta = Math.abs(right.localeImageStatus.delta);
      if (leftDelta !== rightDelta) {
        return rightDelta - leftDelta;
      }

      return new Date(right.updatedAt).getTime() - new Date(left.updatedAt).getTime();
    });
  }

  return next.sort((left, right) => new Date(right.updatedAt).getTime() - new Date(left.updatedAt).getTime());
}

function escapeGraphqlString(value: string) {
  return value.replace(/\\/g, "\\\\").replace(/"/g, '\\"');
}

export async function getProductEditorModel(
  admin: AdminGraphqlClient,
  shop: string,
  productId: string,
  discoveryConfig: DiscoveryConfig,
) {
  const favoritesPromise = getFavoriteMetafields(shop);
  const metaobjectDefinitionsPromise = fetchMetaobjectDefinitions(admin, shop);
  const data = await adminGraphql<{
    product: {
      id: string;
      legacyResourceId: string;
      title: string;
      handle: string;
      descriptionHtml: string;
      vendor: string;
      productType: string;
      tags: string[];
      status: string;
      seo: { title: string; description: string };
      options: Array<{ id: string; name: string; values: string[] }>;
      media: {
        nodes: Array<{
          id: string;
          alt?: string | null;
          createdAt?: string | null;
          image?: {
            url: string;
            width?: number | null;
            height?: number | null;
          } | null;
        }>;
      };
      metafields: {
        nodes: Array<{
          id: string;
          compareDigest?: string | null;
          namespace: string;
          key: string;
          type: string;
          value: string;
          definition?: {
            name?: string | null;
            description?: string | null;
            validations?: Array<{
              name: string;
              value: string;
            }> | null;
          } | null;
          reference?: ProductReferenceNode | null;
          references?: {
            nodes: ProductReferenceNode[];
          } | null;
        }>;
      };
    } | null;
  }>(admin, PRODUCT_EDITOR_QUERY, {
    id: `gid://shopify/Product/${productId}`,
  });

  if (!data.product) {
    throw new Error(`Product ${productId} was not found.`);
  }

  const [favorites, metaobjectDefinitions] = await Promise.all([
    favoritesPromise,
    metaobjectDefinitionsPromise,
  ]);
  const metaobjectDefinitionById = new Map(metaobjectDefinitions.map((definition) => [definition.id, definition]));
  const product = data.product;
  const media = product.media.nodes
    .filter((node) => node.image?.url)
    .map(
      (node): ProductImageItem => ({
        id: node.id,
        url: node.image?.url || "",
        alt: node.alt,
        width: node.image?.width,
        height: node.image?.height,
        createdAt: node.createdAt,
        source: "product" as const,
      }),
    );

  const metafields = product.metafields.nodes.map((metafield) => ({
    id: metafield.id,
    namespace: metafield.namespace,
    key: metafield.key,
    type: metafield.type,
    value: normalizeEditorFieldValue(metafield.type, metafield.value),
    compareDigest: metafield.compareDigest,
    name: metafield.definition?.name || metafield.key,
    description: metafield.definition?.description,
    validations: metafield.definition?.validations || [],
    references: [
      ...toReferenceSummaries(metafield.reference ? [metafield.reference] : []),
      ...toReferenceSummaries(metafield.references?.nodes || []),
    ],
  }));

  const translatableMetafieldIds = collectTranslatableMetafieldIds(metafields, discoveryConfig);
  const metaobjectReferenceIds = metafields.flatMap((metafield) =>
    metafield.references
      .filter((reference) => reference.kind === "METAOBJECT")
      .map((reference) => reference.id),
  );

  const [productTranslatable, translatableMetafields, metaobjectReferenceMap] = await Promise.all([
    getTranslatableResource(admin, product.id, "ar"),
    getTranslatableResourcesByIds(admin, translatableMetafieldIds, "ar"),
    getMetaobjectReferenceSummariesByIds(admin, metaobjectReferenceIds, "ar"),
  ]);

  const arabicFields = buildArabicFields(discoveryConfig, product, metafields, productTranslatable, translatableMetafields);
  const editorMetafields: EditorMetafield[] = metafields.map((metafield) => ({
    id: metafield.id,
    namespace: metafield.namespace,
    key: metafield.key,
    type: metafield.type,
    name: metafield.name,
    description: metafield.description,
    value: metafield.value,
    compareDigest: metafield.compareDigest,
    referenceIds: parseReferenceValue(metafield.value),
    references: metafield.references.map((reference) =>
      reference.kind === "METAOBJECT" ? metaobjectReferenceMap.get(reference.id) || reference : reference,
    ),
    isPinned: favorites.has(`${metafield.namespace}.${metafield.key}`),
    isPopulated: Boolean(metafield.value && metafield.value !== "[]" && metafield.value !== "{}"),
    translation: buildMetafieldTranslation(metafield.id, arabicFields),
    validations: metafield.validations,
    allowedMetaobjectTypes: resolveAllowedMetaobjectTypes(metafield.validations || [], metaobjectDefinitionById),
  }));

  const imageGroups = buildEditorImageGroups({
    discoveryConfig,
    productMetafields: metafields,
    productMedia: media,
    translatableMetafields,
  });

  const model: ProductEditorModel = {
    product: {
      id: product.id,
      legacyId: product.legacyResourceId,
      title: product.title,
      handle: product.handle,
      descriptionHtml: product.descriptionHtml,
      vendor: product.vendor,
      productType: product.productType,
      tags: product.tags,
      status: product.status,
      seo: {
        title: product.seo?.title || "",
        description: product.seo?.description || "",
      },
      options: product.options,
      media,
    },
    arabicFields,
    metafields: editorMetafields,
    imageGroups,
    metaobjectDefinitions,
    raw: {
      productTranslatableKeys: productTranslatable?.translatableContent.map((item) => item.key) || [],
      metafieldDefinitions: discoveryConfig.rawDefinitions,
      translationResourceIds: [product.id, ...translatableMetafields.keys()],
      lastDiscoveredAt: discoveryConfig.generatedAt,
    },
  };

  return model;
}

export async function saveCoreProduct(
  admin: AdminGraphqlClient,
  input: {
    productId: string;
    title: string;
    handle: string;
    descriptionHtml: string;
    vendor: string;
    productType: string;
    tags: string[];
    status: string;
    seoTitle: string;
    seoDescription: string;
  },
) {
  const data = await adminGraphql<{
    productUpdate: {
      userErrors: Array<{
        field?: string[] | null;
        message: string;
      }>;
    };
  }>(admin, PRODUCT_UPDATE_MUTATION, {
    product: {
      id: input.productId,
      title: input.title,
      handle: input.handle,
      descriptionHtml: input.descriptionHtml,
      vendor: input.vendor,
      productType: input.productType,
      tags: input.tags,
      status: input.status,
      seo: {
        title: input.seoTitle,
        description: input.seoDescription,
      },
    },
  });

  const result: SaveResult = data.productUpdate.userErrors.length
    ? {
        ok: false,
        message: "Product core fields could not be saved.",
        errors: data.productUpdate.userErrors.map((error) => ({
          field: error.field?.join("."),
          message: error.message,
        })),
      }
    : {
        ok: true,
        message: "Core product fields saved.",
      };

  return result;
}

async function buildProductTranslationStatusMap(admin: AdminGraphqlClient, productIds: string[]) {
  if (productIds.length === 0) {
    return new Map<string, { translatedKeys: number; totalKeys: number; outdatedKeys: number; arabicTitle?: string }>();
  }

  const data = await adminGraphql<{
    translatableResourcesByIds: {
      edges: Array<{
        node: {
          resourceId: string;
          translatableContent: Array<{ key: string }>;
          translations: Array<{ key: string; value: string; outdated?: boolean }>;
        };
      }>;
    };
  }>(admin, TRANSLATION_STATUS_QUERY, {
    resourceIds: productIds,
    first: productIds.length,
    locale: "ar",
  });

  const relevantKeys = new Set(["title", "body_html", "meta_title", "meta_description", "product_type"]);

  return new Map(
    data.translatableResourcesByIds.edges.map((edge) => {
      const totalKeys = edge.node.translatableContent.filter((item) => relevantKeys.has(item.key)).length;
      const translatedKeys = edge.node.translations.filter(
        (item) => relevantKeys.has(item.key) && item.value.trim().length > 0,
      ).length;
      const outdatedKeys = edge.node.translations.filter(
        (item) => relevantKeys.has(item.key) && item.outdated,
      ).length;
      return [
        edge.node.resourceId,
        {
          translatedKeys,
          totalKeys,
          outdatedKeys,
          arabicTitle: edge.node.translations.find((item) => item.key === "title")?.value || "",
        },
      ] as const;
    }),
  );
}

async function resolveProductImageItems(admin: AdminGraphqlClient, ids: string[]) {
  const uniqueIds = [...new Set(ids)];
  if (uniqueIds.length === 0) {
    return new Map<string, ProductImageItem>();
  }

  const data = await adminGraphql<{
    nodes: Array<{
      id: string;
      alt?: string | null;
      createdAt?: string | null;
      image?: {
        url: string;
        width?: number | null;
        height?: number | null;
      } | null;
    } | null>;
  }>(admin, MEDIA_NODES_QUERY, { ids: uniqueIds });

  return new Map(
    data.nodes
      .filter((node): node is NonNullable<(typeof data.nodes)[number]> => Boolean(node?.image?.url))
      .map((node) => [
        node.id,
        {
          id: node.id,
          url: node.image?.url || "",
          alt: node.alt,
          width: node.image?.width,
          height: node.image?.height,
          createdAt: node.createdAt,
          source: "reference" as const,
        } satisfies ProductImageItem,
      ]),
  );
}

function getIndexLocaleThumbnailId(input: {
  productMetafields: Array<{
    id: string;
    namespace: string;
    key: string;
    type: string;
    value: string;
  }>;
  localeView: NonNullable<ProductListFilters["localeView"]>;
  sourceKey: string;
  activeArabicTargets: DiscoveryConfig["imageMetafieldMappings"]["groups"][number]["arabic"];
  fallbackTranslatedTargets: DiscoveryConfig["imageMetafieldMappings"]["groups"][number]["arabic"];
  imageTranslationMap: Awaited<ReturnType<typeof getTranslatableResourcesByIds>>;
}) {
  if (input.localeView === "english") {
    return parseReferenceValue(
      input.productMetafields.find((metafield) => `${metafield.namespace}.${metafield.key}` === input.sourceKey)?.value,
    )[0];
  }

  for (const target of input.activeArabicTargets) {
    const metafield = input.productMetafields.find(
      (candidate) => candidate.namespace === target.namespace && candidate.key === target.key,
    );
    const id = parseReferenceValue(metafield?.value)[0];
    if (id) {
      return id;
    }
  }

  if (input.fallbackTranslatedTargets.length > 0) {
    const sourceMetafield = input.productMetafields.find(
      (metafield) => `${metafield.namespace}.${metafield.key}` === input.sourceKey,
    );
    const translatedValue =
      sourceMetafield
        ? input.imageTranslationMap
            .get(sourceMetafield.id)
            ?.translations.find((translation) => translation.key === "value")?.value || ""
        : "";
    return parseReferenceValue(translatedValue)[0];
  }

  return undefined;
}

function buildArabicFields(
  discoveryConfig: DiscoveryConfig,
  product: {
    id: string;
  },
  metafields: Array<{
    id: string;
    namespace: string;
    key: string;
    name?: string;
    type: string;
    value: string;
  }>,
  productTranslatable: Awaited<ReturnType<typeof getTranslatableResource>>,
  translatableMetafields: Awaited<ReturnType<typeof getTranslatableResourcesByIds>>,
) {
  const fields: EditorTranslatableField[] = [];
  const metafieldById = new Map(metafields.map((metafield) => [metafield.id, metafield]));
  const mappedMetafieldKeys = new Set(
    discoveryConfig.translationMappings.metafields.map((mapping) => `${mapping.namespace}.${mapping.key}`),
  );

  for (const mapping of discoveryConfig.translationMappings.product) {
    const source = productTranslatable?.translatableContent.find((item) => item.key === mapping.translationKey);
    const translated = productTranslatable?.translations.find((item) => item.key === mapping.translationKey);
    if (!source) continue;

    fields.push({
      resourceId: product.id,
      label: mapping.label,
      key: mapping.key,
      translationKey: mapping.translationKey,
      group: mapping.group,
      fieldType: mapping.fieldType,
      sourceValue: normalizeTranslationValue(mapping.fieldType, source.value, mapping.translationKey),
      arabicValue: normalizeTranslationValue(mapping.fieldType, translated?.value || "", mapping.translationKey),
      digest: source.digest,
      outdated: translated?.outdated,
    });
  }

  for (const mapping of discoveryConfig.translationMappings.metafields) {
    const metafield = metafields.find(
      (item) => item.namespace === mapping.namespace && item.key === mapping.key,
    );
    if (!metafield) continue;

    const translatable = translatableMetafields.get(metafield.id);
    const source = translatable?.translatableContent.find((item) => item.key === mapping.translationKey);
    if (!source) continue;
    const translated = translatable?.translations.find((item) => item.key === mapping.translationKey);

    fields.push({
      resourceId: metafield.id,
      label: mapping.label,
      key: mapping.key,
      translationKey: mapping.translationKey,
      group: mapping.group,
      fieldType: mapping.fieldType,
      sourceValue: normalizeTranslationValue(mapping.fieldType, source.value, mapping.translationKey),
      arabicValue: normalizeTranslationValue(mapping.fieldType, translated?.value || "", mapping.translationKey),
      digest: source.digest,
      outdated: translated?.outdated,
    });
  }

  for (const [resourceId, translatable] of translatableMetafields.entries()) {
    const metafield = metafieldById.get(resourceId);
    if (!metafield) continue;

    const metafieldKey = `${metafield.namespace}.${metafield.key}`;
    if (mappedMetafieldKeys.has(metafieldKey)) {
      continue;
    }

    if (!isTextLikeTranslatableMetafieldType(metafield.type)) {
      continue;
    }

    const source = translatable.translatableContent.find((item) => item.key === "value");
    if (!source) {
      continue;
    }

    const translated = translatable.translations.find((item) => item.key === "value");

    fields.push({
      resourceId: metafield.id,
      label: `Arabic ${metafield.name || metafield.key}`,
      key: metafield.key,
      translationKey: "value",
      group: metafield.namespace,
      fieldType: metafield.type as EditorTranslatableField["fieldType"],
      sourceValue: normalizeTranslationValue(metafield.type, source.value, "value"),
      arabicValue: normalizeTranslationValue(metafield.type, translated?.value || "", "value"),
      digest: source.digest,
      outdated: translated?.outdated,
    });
  }

  return fields;
}

type MediaImageReferenceNode = {
  __typename: "MediaImage";
  id: string;
  alt?: string | null;
  createdAt?: string | null;
  image?: {
    url: string;
    width?: number | null;
    height?: number | null;
  } | null;
};

type ProductResourceReferenceNode = {
  __typename: "Product";
  id: string;
  title: string;
  handle?: string | null;
  featuredMedia?: MediaImageReferenceNode | null;
};

type CollectionReferenceNode = {
  __typename: "Collection";
  id: string;
  title: string;
  handle?: string | null;
  image?: {
    url: string;
    width?: number | null;
    height?: number | null;
    altText?: string | null;
  } | null;
};

type MetaobjectReferenceNode = {
  __typename: "Metaobject";
  id: string;
  displayName: string;
  handle?: string | null;
  type: string;
};

type ProductReferenceNode =
  | MediaImageReferenceNode
  | ProductResourceReferenceNode
  | CollectionReferenceNode
  | MetaobjectReferenceNode;

function toReferenceSummaries(references: ProductReferenceNode[]) {
  return references.map((reference): ReferenceSummary => {
    switch (reference.__typename) {
      case "MediaImage":
        return {
          id: reference.id,
          kind: "MEDIA_IMAGE",
          title: reference.alt || "Image",
          image: reference.image?.url
            ? {
                id: reference.id,
                url: reference.image.url,
                alt: reference.alt,
                width: reference.image.width,
                height: reference.image.height,
                createdAt: reference.createdAt,
                source: "reference" as const,
              }
            : null,
        };
      case "Product":
        return {
          id: reference.id,
          kind: "PRODUCT",
          title: reference.title,
          handle: reference.handle,
          subtitle: reference.handle ? `/${reference.handle}` : "Product",
          image: reference.featuredMedia?.image?.url
            ? {
                id: reference.featuredMedia.id,
                url: reference.featuredMedia.image.url,
                alt: reference.featuredMedia.alt,
                width: reference.featuredMedia.image.width,
                height: reference.featuredMedia.image.height,
                createdAt: reference.featuredMedia.createdAt,
                source: "reference" as const,
              }
            : null,
        };
      case "Collection":
        return {
          id: reference.id,
          kind: "COLLECTION",
          title: reference.title,
          handle: reference.handle,
          subtitle: reference.handle ? `/${reference.handle}` : "Collection",
          image: reference.image?.url
            ? {
                id: reference.id,
                url: reference.image.url,
                alt: reference.image.altText,
                width: reference.image.width,
                height: reference.image.height,
                source: "reference" as const,
              }
            : null,
        };
      case "Metaobject":
        return {
          id: reference.id,
          kind: "METAOBJECT",
          title: reference.displayName,
          handle: reference.handle,
          subtitle: reference.type,
        };
    }
  });
}

function buildMetafieldTranslation(resourceId: string, arabicFields: EditorTranslatableField[]) {
  const field = arabicFields.find((candidate) => candidate.resourceId === resourceId);
  if (!field) {
    return undefined;
  }

  return {
    label: field.label,
    arabicValue: field.arabicValue,
    digest: field.digest,
    fieldType: field.fieldType,
    outdated: field.outdated,
  };
}

function collectTranslatableMetafieldIds(
  metafields: Array<{
    id: string;
    namespace: string;
    key: string;
    type: string;
  }>,
  discoveryConfig: DiscoveryConfig,
) {
  const translationStorageSourceKeys = new Set(
    discoveryConfig.imageMetafieldMappings.groups.flatMap((group) =>
      group.arabic
        .filter((target) => target.storage === "translation")
        .map((target) => target.sourceMetafield || `${target.namespace}.${target.key}`),
    ),
  );

  return metafields
    .filter(
      (metafield) =>
        isTextLikeTranslatableMetafieldType(metafield.type) ||
        translationStorageSourceKeys.has(`${metafield.namespace}.${metafield.key}`),
    )
    .map((metafield) => metafield.id);
}

async function fetchMetaobjectDefinitions(admin: AdminGraphqlClient, shop: string) {
  const cached = metaobjectDefinitionsCache.get(shop);
  if (cached && cached.expiresAt > Date.now()) {
    return cached.definitions;
  }

  const definitions: MetaobjectDefinitionOption[] = [];
  let after: string | undefined;

  try {
    while (true) {
      const data = await adminGraphql<{
        metaobjectDefinitions: {
          nodes: MetaobjectDefinitionOption[];
          pageInfo: {
            hasNextPage: boolean;
            endCursor?: string | null;
          };
        };
      }>(admin, METAOBJECT_DEFINITIONS_QUERY, { first: 100, after });

      definitions.push(...data.metaobjectDefinitions.nodes);

      if (!data.metaobjectDefinitions.pageInfo.hasNextPage) {
        break;
      }

      after = data.metaobjectDefinitions.pageInfo.endCursor || undefined;
    }
  } catch (error) {
    if (isMetaobjectDefinitionsAccessError(error)) {
      console.warn("Metaobject definition access unavailable for this app installation; continuing without definitions.");
      metaobjectDefinitionsCache.set(shop, {
        expiresAt: Date.now() + METAOBJECT_DEFINITIONS_CACHE_TTL_MS,
        definitions: [],
      });
      return [];
    }

    throw error;
  }

  const sortedDefinitions = definitions.sort((left, right) => left.name.localeCompare(right.name));
  metaobjectDefinitionsCache.set(shop, {
    expiresAt: Date.now() + METAOBJECT_DEFINITIONS_CACHE_TTL_MS,
    definitions: sortedDefinitions,
  });

  return sortedDefinitions;
}

function isMetaobjectDefinitionsAccessError(error: unknown) {
  if (!(error instanceof Error)) {
    return false;
  }

  return error.message.includes("Access denied for metaobjectDefinitions field");
}

function resolveAllowedMetaobjectTypes(
  validations: Array<{ name: string; value: string }>,
  definitionById: Map<string, MetaobjectDefinitionOption>,
) {
  const ids = new Set<string>();

  for (const validation of validations) {
    if (!validation.name.includes("metaobject_definition")) {
      continue;
    }

    if (validation.value.trim().startsWith("[")) {
      try {
        for (const id of JSON.parse(validation.value) as string[]) {
          ids.add(id);
        }
      } catch {
        ids.add(validation.value);
      }
      continue;
    }

    ids.add(validation.value);
  }

  const types = [...ids]
    .map((id) => definitionById.get(id)?.type)
    .filter(Boolean) as string[];

  return types.length ? [...new Set(types)] : undefined;
}

function normalizeEditorFieldValue(type: string, value: string) {
  return type === "rich_text_field" ? richTextJsonToEditableText(value) : value;
}

function normalizeTranslationValue(
  fieldType: string,
  value: string,
  translationKey: string,
) {
  if (translationKey === "body_html") {
    return htmlToEditableText(value);
  }

  return fieldType === "rich_text_field" ? richTextJsonToEditableText(value) : value;
}
