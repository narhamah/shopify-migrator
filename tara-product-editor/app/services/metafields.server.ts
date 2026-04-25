import { richTextJsonToEditableText, sanitizeRichTextJson } from "~/lib/rich-text";
import { adminGraphql, type AdminGraphqlClient } from "~/lib/shopify-admin.server";
import { normalizeMultilineIds } from "~/lib/utils";
import type { EditorMetafield, SaveConflict, SaveResult } from "~/types/editor";

const METAFIELDS_SET_MUTATION = `
  mutation SetMetafields($metafields: [MetafieldsSetInput!]!) {
    metafieldsSet(metafields: $metafields) {
      metafields {
        id
        namespace
        key
      }
      userErrors {
        field
        message
      }
    }
  }
`;

const METAFIELD_NODES_QUERY = `
  query MetafieldNodes($ids: [ID!]!) {
    nodes(ids: $ids) {
      ... on Metafield {
        id
        compareDigest
        value
        type
      }
    }
  }
`;

export async function saveMetafields(
  admin: AdminGraphqlClient,
  ownerId: string,
  metafields: Array<
    Pick<EditorMetafield, "namespace" | "key" | "type" | "value"> & {
      id?: string;
      compareDigest?: string | null;
      name?: string;
    }
  >,
  section: "metafields" | "images" = "metafields",
) {
  const payload = metafields.map((metafield) => ({
    id: metafield.id,
    label: metafield.name || `${metafield.namespace}.${metafield.key}`,
    input: {
      ownerId,
      namespace: metafield.namespace,
      key: metafield.key,
      type: metafield.type,
      value: normalizeMetafieldValue(metafield.type, metafield.value),
      compareDigest: metafield.compareDigest ?? undefined,
    },
  }));

  const errors: Array<{ field?: string; message: string }> = [];
  const conflicts: SaveConflict[] = [];

  for (let index = 0; index < payload.length; index += 25) {
    const batch = payload.slice(index, index + 25);
    const data = await adminGraphql<{
      metafieldsSet: {
        userErrors: Array<{
          field?: string[] | null;
          message: string;
        }>;
      };
    }>(admin, METAFIELDS_SET_MUTATION, {
      metafields: batch.map((entry) => entry.input),
    });

    const conflictIds = new Set<string>(
      data.metafieldsSet.userErrors
        .map((error) => {
          const inputIndex = getUserErrorInputIndex(error.field);
          const target = inputIndex === null ? undefined : batch[inputIndex];
          return isCompareDigestConflict(error.message) ? target?.id : undefined;
        })
        .filter(Boolean) as string[],
    );
    const latestById = conflictIds.size ? await getLatestMetafieldsByIds(admin, [...conflictIds]) : new Map();

    for (const error of data.metafieldsSet.userErrors) {
      const inputIndex = getUserErrorInputIndex(error.field);
      const target = inputIndex === null ? undefined : batch[inputIndex];

      if (target?.id && isCompareDigestConflict(error.message)) {
        const latest = latestById.get(target.id);
        conflicts.push({
          kind: section === "images" ? "image" : "metafield",
          section,
          resourceId: target.id,
          fieldKey: `${target.input.namespace}.${target.input.key}`,
          label: target.label,
          message: error.message,
          latestValue: latest
            ? normalizeMetafieldEditorValue(latest.type, latest.value)
            : target.input.value,
          latestCompareDigest: latest?.compareDigest,
        });
        continue;
      }

      errors.push({
        field: error.field?.join("."),
        message: error.message,
      });
    }
  }

  const result: SaveResult = errors.length || conflicts.length
    ? {
        ok: false,
        message: "Some metafields could not be saved.",
        errors,
        conflicts,
      }
    : {
        ok: true,
        message: "Metafields saved.",
        conflicts,
      };

  return result;
}

function normalizeMetafieldValue(type: string, value: string) {
  if (type === "rich_text_field") {
    return sanitizeRichTextJson(value);
  }

  if (type === "boolean") {
    return String(value === "true" || value === "1");
  }

  if (type === "number_integer" || type === "number_decimal") {
    return String(value).trim();
  }

  if (type === "json") {
    return JSON.stringify(JSON.parse(value));
  }

  if (type.startsWith("list.")) {
    const ids = value.trim().startsWith("[") ? JSON.parse(value) : normalizeMultilineIds(value);
    return JSON.stringify(ids);
  }

  if (
    type === "file_reference" ||
    type === "product_reference" ||
    type === "collection_reference" ||
    type === "metaobject_reference"
  ) {
    return normalizeMultilineIds(value)[0] || "";
  }

  return value;
}

async function getLatestMetafieldsByIds(admin: AdminGraphqlClient, ids: string[]) {
  const data = await adminGraphql<{
    nodes: Array<
      | {
          id: string;
          compareDigest?: string | null;
          value: string;
          type: string;
        }
      | null
    >;
  }>(admin, METAFIELD_NODES_QUERY, { ids });

  return new Map(
    data.nodes
      .filter((node): node is NonNullable<(typeof data.nodes)[number]> => Boolean(node))
      .map((node) => [node.id, node]),
  );
}

function getUserErrorInputIndex(field?: string[] | null) {
  if (!field?.length) {
    return null;
  }

  for (const segment of field) {
    const value = Number(segment);
    if (Number.isInteger(value)) {
      return value;
    }
  }

  return null;
}

function isCompareDigestConflict(message: string) {
  return message.toLowerCase().includes("comparedigest");
}

function normalizeMetafieldEditorValue(type: string, value: string) {
  return type === "rich_text_field" ? richTextJsonToEditableText(value) : value;
}
