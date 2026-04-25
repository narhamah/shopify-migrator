import type { LoaderFunctionArgs } from "react-router";

import { adminGraphql } from "~/lib/shopify-admin.server";
import { searchMetaobjectReferences } from "~/services/metaobjects.server";
import { authenticate } from "~/shopify.server";
import type { ProductImageItem, ReferenceSummary } from "~/types/editor";

const PRODUCT_REFERENCE_SEARCH_QUERY = `
  query ReferenceProducts($first: Int!, $query: String) {
    products(first: $first, query: $query, sortKey: TITLE) {
      nodes {
        id
        title
        handle
        featuredMedia {
          ... on MediaImage {
            id
            alt
            image {
              url
              width
              height
            }
          }
        }
      }
    }
  }
`;

const COLLECTION_REFERENCE_SEARCH_QUERY = `
  query ReferenceCollections($first: Int!, $query: String) {
    collections(first: $first, query: $query, sortKey: TITLE) {
      nodes {
        id
        title
        handle
        image {
          url
          width
          height
          altText
        }
      }
    }
  }
`;

export async function loader({ request }: LoaderFunctionArgs) {
  const { admin } = await authenticate.admin(request);
  const url = new URL(request.url);
  const kind = url.searchParams.get("kind") || "";
  const query = url.searchParams.get("query")?.trim() || undefined;
  const metaobjectType = url.searchParams.get("metaobjectType") || undefined;
  const locale = (url.searchParams.get("locale") || "en") as "en" | "ar";

  if (kind === "product") {
    const data = await adminGraphql<{
      products: {
        nodes: Array<{
          id: string;
          title: string;
          handle?: string | null;
          featuredMedia?: {
            id: string;
            alt?: string | null;
            image?: {
              url: string;
              width?: number | null;
              height?: number | null;
            } | null;
          } | null;
        }>;
      };
    }>(admin, PRODUCT_REFERENCE_SEARCH_QUERY, {
      first: 24,
      query,
    });

    return {
      items: data.products.nodes.map((product): ReferenceSummary => ({
        id: product.id,
        kind: "PRODUCT",
        title: product.title,
        handle: product.handle,
        subtitle: product.handle ? `/${product.handle}` : "Product",
        image: toMediaImage(product.featuredMedia),
      })),
    };
  }

  if (kind === "collection") {
    const data = await adminGraphql<{
      collections: {
        nodes: Array<{
          id: string;
          title: string;
          handle?: string | null;
          image?: {
            url: string;
            width?: number | null;
            height?: number | null;
            altText?: string | null;
          } | null;
        }>;
      };
    }>(admin, COLLECTION_REFERENCE_SEARCH_QUERY, {
      first: 24,
      query,
    });

    return {
      items: data.collections.nodes.map((collection): ReferenceSummary => ({
        id: collection.id,
        kind: "COLLECTION",
        title: collection.title,
        handle: collection.handle,
        subtitle: collection.handle ? `/${collection.handle}` : "Collection",
        image: collection.image?.url
          ? {
              id: collection.id,
              url: collection.image.url,
              alt: collection.image.altText,
              width: collection.image.width,
              height: collection.image.height,
            }
          : null,
      })),
    };
  }

  if (kind === "metaobject" && metaobjectType) {
    return {
      items: await searchMetaobjectReferences(admin, {
        type: metaobjectType,
        query,
        locale,
      }),
    };
  }

  return { items: [] as ReferenceSummary[] };
}

export default function ReferencesRoute() {
  return null;
}

function toMediaImage(
  media:
    | {
        id: string;
        alt?: string | null;
        image?: {
          url: string;
          width?: number | null;
          height?: number | null;
        } | null;
      }
    | null
    | undefined,
) {
  if (!media?.image?.url) {
    return null;
  }

  return {
    id: media.id,
    url: media.image.url,
    alt: media.alt,
    width: media.image.width,
    height: media.image.height,
  } satisfies ProductImageItem;
}
