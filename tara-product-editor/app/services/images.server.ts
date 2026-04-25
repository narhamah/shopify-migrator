import { adminGraphql, type AdminGraphqlClient } from "~/lib/shopify-admin.server";
import { parseReferenceValue } from "~/lib/utils";
import { saveMetafields } from "~/services/metafields.server";
import { getTranslatableResourcesByIds, registerArabicTranslations } from "~/services/translations.server";
import type {
  DiscoveryConfig,
  EditorImageGroup,
  EditorImageTarget,
  ProductImageItem,
  ReferenceSummary,
  SaveResult,
} from "~/types/editor";

const FILE_LIBRARY_IMAGE_TRANSFORM = "transform: { maxWidth: 320, maxHeight: 320 }";

const FILE_SEARCH_QUERY = `
  query SearchFiles($first: Int!, $query: String, $after: String) {
    files(first: $first, query: $query, after: $after) {
      nodes {
        ... on MediaImage {
          id
          alt
          createdAt
          image {
            url(${FILE_LIBRARY_IMAGE_TRANSFORM})
            width
            height
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

const FILE_PAGE_SIZE = 60;

const STAGED_UPLOADS_CREATE_MUTATION = `
  mutation CreateStagedUpload($input: [StagedUploadInput!]!) {
    stagedUploadsCreate(input: $input) {
      stagedTargets {
        url
        resourceUrl
        parameters {
          name
          value
        }
      }
      userErrors {
        field
        message
      }
    }
  }
`;

const FILE_CREATE_MUTATION = `
  mutation CreateFile($files: [FileCreateInput!]!) {
    fileCreate(files: $files) {
      files {
        ... on MediaImage {
          id
          alt
          fileStatus
          createdAt
          image {
            url(${FILE_LIBRARY_IMAGE_TRANSFORM})
            width
            height
          }
        }
      }
      userErrors {
        field
        message
      }
    }
  }
`;

const FILE_NODE_QUERY = `
  query FileNode($id: ID!) {
    node(id: $id) {
        ... on MediaImage {
          id
          alt
          fileStatus
          createdAt
          image {
            url(${FILE_LIBRARY_IMAGE_TRANSFORM})
            width
            height
        }
      }
    }
  }
`;

const METAFIELD_VALUES_BY_IDS_QUERY = `
  query MetafieldValuesByIds($ids: [ID!]!) {
    nodes(ids: $ids) {
      ... on Metafield {
        id
        namespace
        key
        value
      }
    }
  }
`;

interface ProductMetafieldRecord {
  id: string;
  compareDigest?: string | null;
  namespace: string;
  key: string;
  type: string;
  value: string;
  references: ReferenceSummary[];
}

interface TranslatableMetafieldRecord {
  resourceId: string;
  translatableContent: Array<{ key: string; value: string; digest: string }>;
  translations: Array<{ key: string; value: string; outdated: boolean }>;
}

export function buildEditorImageGroups(input: {
  discoveryConfig: DiscoveryConfig;
  productMetafields: ProductMetafieldRecord[];
  productMedia: ProductImageItem[];
  translatableMetafields: Map<string, TranslatableMetafieldRecord>;
}) {
  const imageLookup = new Map<string, ProductImageItem>();

  for (const media of input.productMedia) {
    imageLookup.set(media.id, media);
  }

  for (const metafield of input.productMetafields) {
    for (const reference of metafield.references) {
      if (reference.image) {
        imageLookup.set(reference.id, reference.image);
      }
    }
  }

  return input.discoveryConfig.imageMetafieldMappings.groups.map((group) => {
    const english = group.english.map((target) =>
      resolveTarget(target, input.productMetafields, input.translatableMetafields, imageLookup),
    );
    const arabic = group.arabic.map((target) =>
      resolveTarget(target, input.productMetafields, input.translatableMetafields, imageLookup),
    );
    const legacy = group.legacy.map((target) =>
      resolveTarget(target, input.productMetafields, input.translatableMetafields, imageLookup),
    );

    return {
      id: group.id,
      label: group.label,
      notes: group.notes,
      english,
      arabic,
      legacy,
      mismatchWarning: buildMismatchWarning(group.label, english[0]?.images.length || 0, arabic[0]?.images.length || 0),
    } satisfies EditorImageGroup;
  });
}

export async function saveLocaleImageTargets(
  admin: AdminGraphqlClient,
  ownerId: string,
  targets: Array<EditorImageTarget>,
) {
  const metafieldTargets = targets.filter((target) => target.target.storage === "metafield");
  const translationTargets = targets.filter((target) => target.target.storage === "translation");
  const errors: Array<{ field?: string; message: string }> = [];

  if (metafieldTargets.length > 0) {
    const metafieldResult = await saveMetafields(
      admin,
      ownerId,
      metafieldTargets.map((target) => ({
        id: target.resourceId,
        name: target.target.label,
        namespace: target.target.namespace,
        key: target.target.key,
        type: target.target.type,
        value: JSON.stringify(target.images.map((image) => image.id)),
        compareDigest: target.compareDigest,
      })),
      "images",
    );

    errors.push(...(metafieldResult.errors || []));

    if (metafieldResult.ok) {
      await waitForMetafieldImageTargetsToPersist(admin, metafieldTargets);
    }
  }

  const latestTranslationResources = await getTranslatableResourcesByIds(
    admin,
    [...new Set(translationTargets.map((target) => target.resourceId).filter(Boolean) as string[])],
    "ar",
  );

  for (const target of translationTargets) {
    const latestDigest =
      target.resourceId && target.target.translationKey
        ? latestTranslationResources
            .get(target.resourceId)
            ?.translatableContent.find((item) => item.key === target.target.translationKey)?.digest
        : undefined;

    if (!target.resourceId || !(latestDigest || target.digest) || !target.target.translationKey) {
      errors.push({
        field: target.target.id,
        message: "Missing translation digest for locale image target.",
      });
      continue;
    }

    const userErrors = await registerArabicTranslations(admin, target.resourceId, [
      {
        key: target.target.translationKey,
        value: JSON.stringify(target.images.map((image) => image.id)),
        translatableContentDigest: latestDigest || target.digest!,
      },
    ]);

    errors.push(
      ...userErrors.map((error) => ({
        field: error.field?.join("."),
        message: error.message,
      })),
    );
  }

  const result: SaveResult = errors.length
    ? {
        ok: false,
        message: "Some locale image updates failed.",
        errors,
      }
    : {
        ok: true,
        message: "Locale images saved.",
      };

  return result;
}

async function waitForMetafieldImageTargetsToPersist(
  admin: AdminGraphqlClient,
  targets: Array<EditorImageTarget>,
) {
  const ids = [...new Set(targets.map((target) => target.resourceId).filter(Boolean) as string[])];
  if (ids.length === 0) {
    return;
  }

  const expectedById = new Map(
    targets
      .filter((target): target is EditorImageTarget & { resourceId: string } => Boolean(target.resourceId))
      .map((target) => [target.resourceId, JSON.stringify(target.images.map((image) => image.id))]),
  );

  for (let attempt = 0; attempt < 6; attempt += 1) {
    const data = await adminGraphql<{
      nodes: Array<
        | {
            id: string;
            namespace: string;
            key: string;
            value: string;
          }
        | null
      >;
    }>(admin, METAFIELD_VALUES_BY_IDS_QUERY, { ids });

    const allMatched = data.nodes.every((node) => {
      if (!node) {
        return false;
      }
      return expectedById.get(node.id) === node.value;
    });

    if (allMatched) {
      return;
    }

    await new Promise((resolve) => setTimeout(resolve, 500));
  }
}

export async function searchShopifyFiles(
  admin: AdminGraphqlClient,
  input: {
    query?: string;
    after?: string;
  },
) {
  const data = await adminGraphql<{
    files: {
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
      pageInfo: {
        hasNextPage: boolean;
        endCursor?: string | null;
      };
    };
  }>(admin, FILE_SEARCH_QUERY, {
    first: FILE_PAGE_SIZE,
    after: input.after,
    query: input.query?.trim() ? input.query.trim() : undefined,
  });

  return {
    files: data.files.nodes
      .filter((node) => node.image?.url)
      .map(
        (node): ProductImageItem => ({
          id: node.id,
          url: node.image?.url || "",
          alt: node.alt,
          width: node.image?.width,
          height: node.image?.height,
          createdAt: node.createdAt,
          source: "files",
        }),
      ),
    pageInfo: data.files.pageInfo,
  };
}

export async function uploadShopifyImageFile(
  admin: AdminGraphqlClient,
  input: {
    file: File;
    alt?: string;
  },
) {
  const staged = await adminGraphql<{
    stagedUploadsCreate: {
      stagedTargets: Array<{
        url: string;
        resourceUrl: string;
        parameters: Array<{ name: string; value: string }>;
      }>;
      userErrors: Array<{
        field?: string[] | null;
        message: string;
      }>;
    };
  }>(admin, STAGED_UPLOADS_CREATE_MUTATION, {
    input: [
      {
        filename: input.file.name,
        mimeType: input.file.type || "image/jpeg",
        httpMethod: "POST",
        resource: "IMAGE",
        fileSize: String(input.file.size),
      },
    ],
  });

  if (staged.stagedUploadsCreate.userErrors.length > 0 || !staged.stagedUploadsCreate.stagedTargets[0]) {
    return {
      ok: false,
      message: "Could not prepare image upload.",
      errors: staged.stagedUploadsCreate.userErrors.map((error) => ({
        field: error.field?.join("."),
        message: error.message,
      })),
    } satisfies SaveResult & { file?: ProductImageItem };
  }

  const target = staged.stagedUploadsCreate.stagedTargets[0];
  const uploadForm = new FormData();
  for (const parameter of target.parameters) {
    uploadForm.append(parameter.name, parameter.value);
  }
  uploadForm.append("file", input.file, input.file.name);

  const uploadResponse = await fetch(target.url, {
    method: "POST",
    body: uploadForm,
  });

  if (!uploadResponse.ok) {
    return {
      ok: false,
      message: "Image upload to Shopify storage failed.",
      errors: [
        {
          message: `Upload target returned ${uploadResponse.status}.`,
        },
      ],
    } satisfies SaveResult & { file?: ProductImageItem };
  }

  const created = await adminGraphql<{
    fileCreate: {
      files: Array<{
        id: string;
        alt?: string | null;
        fileStatus?: string | null;
        createdAt?: string | null;
        image?: {
          url: string;
          width?: number | null;
          height?: number | null;
        } | null;
      }>;
      userErrors: Array<{
        field?: string[] | null;
        message: string;
      }>;
    };
  }>(admin, FILE_CREATE_MUTATION, {
    files: [
      {
        originalSource: target.resourceUrl,
        alt: input.alt || input.file.name,
        contentType: "IMAGE",
      },
    ],
  });

  if (created.fileCreate.userErrors.length > 0 || !created.fileCreate.files[0]) {
    return {
      ok: false,
      message: "Shopify could not register the uploaded image.",
      errors: created.fileCreate.userErrors.map((error) => ({
        field: error.field?.join("."),
        message: error.message,
      })),
    } satisfies SaveResult & { file?: ProductImageItem };
  }

  const uploaded = await waitForImageReady(admin, created.fileCreate.files[0].id, input.alt || input.file.name);
  return {
    ok: true,
    message: "Image uploaded to Shopify Files.",
    file: uploaded,
  } satisfies SaveResult & { file?: ProductImageItem };
}

function resolveTarget(
  target: {
    id: string;
    label: string;
    locale: "en" | "ar";
    storage: "metafield" | "translation";
    namespace: string;
    key: string;
    type: "file_reference" | "list.file_reference";
    translationKey?: string;
    sourceMetafield?: string;
  },
  metafields: ProductMetafieldRecord[],
  translatableMetafields: Map<string, TranslatableMetafieldRecord>,
  imageLookup: Map<string, ProductImageItem>,
) {
  const metafield = metafields.find(
    (item) => item.namespace === target.namespace && item.key === target.key,
  );

  if (!metafield) {
    return {
      target,
      images: [],
      rawValue: "",
    } satisfies EditorImageTarget;
  }

  if (target.storage === "metafield") {
    const ids = parseReferenceValue(metafield.value);
    return {
      target: {
        ...target,
        type: metafield.type as "list.file_reference",
      },
      resourceId: metafield.id,
      compareDigest: metafield.compareDigest,
      images: ids.map((id) => imageLookup.get(id)).filter(Boolean) as ProductImageItem[],
      rawValue: metafield.value,
    } satisfies EditorImageTarget;
  }

  const translatable = translatableMetafields.get(metafield.id);
  const translatedValue = translatable?.translations.find((translation) => translation.key === "value")?.value || "[]";
  const digest = translatable?.translatableContent.find((item) => item.key === "value")?.digest;
  const ids = parseReferenceValue(translatedValue);

  return {
    target: {
      ...target,
      type: metafield.type as "list.file_reference",
    },
    resourceId: metafield.id,
    digest,
    images: ids.map((id) => imageLookup.get(id)).filter(Boolean) as ProductImageItem[],
    rawValue: translatedValue,
  } satisfies EditorImageTarget;
}

async function waitForImageReady(admin: AdminGraphqlClient, id: string, fallbackAlt?: string) {
  for (let attempt = 0; attempt < 8; attempt += 1) {
    const data = await adminGraphql<{
      node: {
        id: string;
      alt?: string | null;
      fileStatus?: string | null;
      createdAt?: string | null;
      image?: {
        url: string;
          width?: number | null;
          height?: number | null;
        } | null;
      } | null;
    }>(admin, FILE_NODE_QUERY, { id });

    if (data.node?.image?.url) {
      return {
        id: data.node.id,
        url: data.node.image.url,
        alt: data.node.alt || fallbackAlt,
        width: data.node.image.width,
        height: data.node.image.height,
        createdAt: data.node.createdAt,
        source: "upload",
      } satisfies ProductImageItem;
    }

    await new Promise((resolve) => setTimeout(resolve, 1000));
  }

  return {
    id,
    url: "",
    alt: fallbackAlt,
    source: "upload",
  } satisfies ProductImageItem;
}

function buildMismatchWarning(label: string, englishCount: number, arabicCount: number) {
  if (englishCount === arabicCount) {
    return undefined;
  }

  const difference = Math.abs(englishCount - arabicCount);
  if (arabicCount > englishCount) {
    return `${label}: Arabic has ${difference} more image${difference === 1 ? "" : "s"} than English.`;
  }

  return `${label}: Arabic is missing ${difference} image${difference === 1 ? "" : "s"} compared with English.`;
}
