import prisma from "~/db.server";
import { DISCOVERY_SCHEMA_VERSION, TARA_DISCOVERY_SEED } from "~/config/discovery-seed";
import { adminGraphql, type AdminGraphqlClient } from "~/lib/shopify-admin.server";
import { safeJsonParse } from "~/lib/utils";
import type {
  DiscoveryConfig,
  MetafieldDefinitionSummary,
  ProductContentMapping,
  TranslationMapping,
} from "~/types/editor";

const PRODUCT_DEFINITIONS_QUERY = `
  query ProductMetafieldDefinitions($first: Int!, $after: String) {
    metafieldDefinitions(ownerType: PRODUCT, first: $first, after: $after) {
      edges {
        node {
          id
          namespace
          key
          name
          description
          type {
            name
          }
        }
      }
      pageInfo {
        hasNextPage
        endCursor
      }
    }
  }
`;

const SAMPLE_PRODUCTS_QUERY = `
  query DiscoverySampleProducts($first: Int!) {
    products(first: $first, query: "status:active", sortKey: UPDATED_AT, reverse: true) {
      nodes {
        id
        handle
        title
        metafields(first: 50) {
          nodes {
            id
            namespace
            key
            type
          }
        }
      }
    }
  }
`;

const TRANSLATABLE_BY_IDS_QUERY = `
  query DiscoveryTranslatableResources($resourceIds: [ID!]!, $first: Int!) {
    translatableResourcesByIds(resourceIds: $resourceIds, first: $first) {
      edges {
        node {
          resourceId
          translations(locale: "ar") {
            key
            locale
          }
        }
      }
    }
  }
`;

export async function ensureDiscoveryConfig(admin: AdminGraphqlClient, shop: string) {
  const existing = await prisma.discoveryConfig.findUnique({ where: { shop } });
  if (existing) {
    const parsed = safeJsonParse<DiscoveryConfig>(existing.schemaJson, {
      ...TARA_DISCOVERY_SEED,
      shop,
      generatedAt: new Date().toISOString(),
    });

    if (parsed.schemaVersion !== DISCOVERY_SCHEMA_VERSION) {
      return discoverStoreSchema(admin, shop);
    }

    return parsed;
  }

  return discoverStoreSchema(admin, shop);
}

export async function discoverStoreSchema(admin: AdminGraphqlClient, shop: string) {
  const definitions = await fetchProductDefinitions(admin);
  const sampleProducts = await fetchSampleProducts(admin);
  const sampleMetafieldMap = new Map<string, string[]>();

  for (const product of sampleProducts) {
    for (const metafield of product.metafields.nodes) {
      const key = `${metafield.namespace}.${metafield.key}`;
      if (!sampleMetafieldMap.has(key)) {
        sampleMetafieldMap.set(key, []);
      }
      sampleMetafieldMap.get(key)?.push(metafield.id);
    }
  }

  const translatableLookup = await fetchTranslatableMetafields(
    admin,
    [...sampleMetafieldMap.values()].flat().slice(0, 30),
  );

  const metafieldMappings = inferMetafieldMappings(definitions, sampleMetafieldMap, translatableLookup);

  const config: DiscoveryConfig = {
    ...TARA_DISCOVERY_SEED,
    schemaVersion: DISCOVERY_SCHEMA_VERSION,
    shop,
    generatedAt: new Date().toISOString(),
    sampledProducts: sampleProducts.length,
    definitionCount: definitions.length,
    rawDefinitions: definitions,
    productContentMappings: {
      ...TARA_DISCOVERY_SEED.productContentMappings,
      metafields: metafieldMappings.contentMappings,
    },
    translationMappings: {
      product: TARA_DISCOVERY_SEED.translationMappings.product,
      metafields: metafieldMappings.translationMappings,
    },
    notes: [
      ...TARA_DISCOVERY_SEED.notes,
      `Discovery refreshed against ${definitions.length} product metafield definitions and ${sampleProducts.length} sample products.`,
    ],
  };

  await prisma.discoveryConfig.upsert({
    where: { shop },
    update: { schemaJson: JSON.stringify(config) },
    create: { shop, schemaJson: JSON.stringify(config) },
  });

  return config;
}

export async function getFavoriteMetafields(shop: string) {
  const favorites = await prisma.favoriteMetafield.findMany({
    where: { shop },
    orderBy: { key: "asc" },
  });

  return new Set(favorites.map((favorite: { namespace: string; key: string }) => `${favorite.namespace}.${favorite.key}`));
}

export async function toggleFavoriteMetafield(shop: string, namespace: string, key: string) {
  const existing = await prisma.favoriteMetafield.findUnique({
    where: {
      shop_namespace_key: { shop, namespace, key },
    },
  });

  if (existing) {
    await prisma.favoriteMetafield.delete({ where: { id: existing.id } });
    return false;
  }

  await prisma.favoriteMetafield.create({
    data: { shop, namespace, key },
  });
  return true;
}

async function fetchProductDefinitions(admin: AdminGraphqlClient) {
  const definitions: MetafieldDefinitionSummary[] = [];
  let after: string | undefined;

  while (true) {
    const data = await adminGraphql<{
      metafieldDefinitions: {
        edges: Array<{
          node: {
            id: string;
            namespace: string;
            key: string;
            name: string;
            description?: string | null;
            type: { name: string };
          };
        }>;
        pageInfo: {
          hasNextPage: boolean;
          endCursor?: string | null;
        };
      };
    }>(admin, PRODUCT_DEFINITIONS_QUERY, { first: 250, after });

    definitions.push(
      ...data.metafieldDefinitions.edges.map(({ node }) => ({
        id: node.id,
        namespace: node.namespace,
        key: node.key,
        name: node.name,
        description: node.description,
        type: node.type.name as MetafieldDefinitionSummary["type"],
      })),
    );

    if (!data.metafieldDefinitions.pageInfo.hasNextPage) {
      break;
    }

    after = data.metafieldDefinitions.pageInfo.endCursor || undefined;
  }

  return definitions.sort((left, right) =>
    `${left.namespace}.${left.key}`.localeCompare(`${right.namespace}.${right.key}`),
  );
}

async function fetchSampleProducts(admin: AdminGraphqlClient) {
  return adminGraphql<{
    products: {
      nodes: Array<{
        id: string;
        handle: string;
        title: string;
        metafields: {
          nodes: Array<{
            id: string;
            namespace: string;
            key: string;
            type: string;
          }>;
        };
      }>;
    };
  }>(admin, SAMPLE_PRODUCTS_QUERY, { first: 10 }).then((data) => data.products.nodes);
}

async function fetchTranslatableMetafields(admin: AdminGraphqlClient, resourceIds: string[]) {
  if (resourceIds.length === 0) {
    return new Set<string>();
  }

  const data = await adminGraphql<{
    translatableResourcesByIds: {
      edges: Array<{
        node: {
          resourceId: string;
          translations: Array<{ key: string; locale: string }>;
        };
      }>;
    };
  }>(admin, TRANSLATABLE_BY_IDS_QUERY, {
    resourceIds,
    first: resourceIds.length,
  });

  return new Set(
    data.translatableResourcesByIds.edges
      .filter((edge) => edge.node.translations.some((translation) => translation.locale === "ar"))
      .map((edge) => edge.node.resourceId),
  );
}

function inferMetafieldMappings(
  definitions: MetafieldDefinitionSummary[],
  sampleMetafieldMap: Map<string, string[]>,
  translatableLookup: Set<string>,
) {
  const contentMappings: ProductContentMapping[] = [];
  const translationMappings: TranslationMapping[] = [];

  for (const definition of definitions) {
    const key = `${definition.namespace}.${definition.key}`;
    const sampleResourceId = sampleMetafieldMap.get(key)?.[0];

    if (!sampleResourceId || !isContentMetafield(definition)) {
      continue;
    }

    contentMappings.push({
      namespace: definition.namespace,
      key: definition.key,
      label: definition.name,
      type: definition.type as ProductContentMapping["type"],
      group: guessGroup(definition.key),
    });

    if (translatableLookup.has(sampleResourceId)) {
      translationMappings.push({
        id: key,
        label: `Arabic ${definition.name.toLowerCase()}`,
        namespace: definition.namespace,
        key: definition.key,
        type: "metafield",
        sourceResourceType: "METAFIELD",
        resourceKey: key,
        translationKey: "value",
        fieldType: definition.type as TranslationMapping["fieldType"],
        group: guessGroup(definition.key),
      });
    }
  }

  return {
    contentMappings: contentMappings.sort((left, right) => left.label.localeCompare(right.label)),
    translationMappings: translationMappings.sort((left, right) => left.label.localeCompare(right.label)),
  };
}

function isContentMetafield(definition: MetafieldDefinitionSummary) {
  return (
    definition.namespace === "custom" &&
    ["single_line_text_field", "multi_line_text_field", "rich_text_field"].includes(definition.type)
  );
}

function guessGroup(key: string) {
  if (key.includes("benefits")) return "Key Benefits";
  if (key.includes("clinical")) return "Clinical Results";
  if (key.includes("how_to_use")) return "How To Use";
  if (key.includes("inside")) return "What's Inside";
  if (key.includes("fragrance")) return "Fragrance";
  if (key.includes("free_of")) return "Free Of";
  return "Hero";
}
