import { canInlineEditMetafield } from "~/lib/metafields";
import { editableTextToHtml, richTextJsonToEditableText, sanitizeRichTextJson } from "~/lib/rich-text";
import { adminGraphql, type AdminGraphqlClient } from "~/lib/shopify-admin.server";
import { normalizeMultilineIds } from "~/lib/utils";
import { getTranslatableResourcesByIds, saveArabicFields } from "~/services/translations.server";
import type { EditorTranslatableField, MetaobjectReferenceField, ReferenceSummary, SaveResult } from "~/types/editor";

const METAOBJECT_NODES_QUERY = `
  query MetaobjectNodes($ids: [ID!]!) {
    nodes(ids: $ids) {
      ... on Metaobject {
        id
        displayName
        handle
        type
        definition {
          id
          name
          displayNameKey
          capabilities {
            translatable {
              enabled
            }
          }
        }
        fields {
          key
          value
          type
          definition {
            key
            name
            required
            type {
              name
            }
          }
        }
      }
    }
  }
`;

const METAOBJECT_SEARCH_QUERY = `
  query SearchMetaobjects($first: Int!, $type: String!, $query: String) {
    metaobjects(first: $first, type: $type, query: $query) {
      nodes {
        id
        displayName
        handle
        type
        definition {
          id
          name
          displayNameKey
          capabilities {
            translatable {
              enabled
            }
          }
        }
        fields {
          key
          value
          type
          definition {
            key
            name
            required
            type {
              name
            }
          }
        }
      }
    }
  }
`;

const METAOBJECT_UPDATE_MUTATION = `
  mutation UpdateMetaobject($id: ID!, $metaobject: MetaobjectUpdateInput!) {
    metaobjectUpdate(id: $id, metaobject: $metaobject) {
      metaobject {
        id
      }
      userErrors {
        field
        message
      }
    }
  }
`;

type RawMetaobjectNode = {
  id: string;
  displayName: string;
  handle?: string | null;
  type: string;
  definition?: {
    id: string;
    name: string;
    displayNameKey?: string | null;
    capabilities?: {
      translatable?: {
        enabled?: boolean | null;
      } | null;
    } | null;
  } | null;
  fields: Array<{
    key: string;
    value?: string | null;
    type: string;
    definition?: {
      key: string;
      name: string;
      required?: boolean | null;
      type?: {
        name: string;
      } | null;
    } | null;
  }>;
};

export async function searchMetaobjectReferences(
  admin: AdminGraphqlClient,
  input: {
    type: string;
    query?: string;
    locale?: "en" | "ar";
  },
) {
  const data = await adminGraphql<{
    metaobjects: {
      nodes: RawMetaobjectNode[];
    };
  }>(admin, METAOBJECT_SEARCH_QUERY, {
    first: 24,
    type: input.type,
    query: input.query,
  });

  return buildMetaobjectReferenceSummaries(admin, data.metaobjects.nodes, input.locale || "ar");
}

export async function getMetaobjectReferenceSummariesByIds(
  admin: AdminGraphqlClient,
  ids: string[],
  locale: "en" | "ar" = "ar",
) {
  const uniqueIds = [...new Set(ids)];
  if (uniqueIds.length === 0) {
    return new Map<string, ReferenceSummary>();
  }

  const data = await adminGraphql<{
    nodes: Array<RawMetaobjectNode | null>;
  }>(admin, METAOBJECT_NODES_QUERY, {
    ids: uniqueIds,
  });

  const summaries = await buildMetaobjectReferenceSummaries(
    admin,
    data.nodes.filter((node): node is RawMetaobjectNode => Boolean(node)),
    locale,
  );

  return new Map(summaries.map((summary) => [summary.id, summary]));
}

export async function enrichReferenceSummaries(
  admin: AdminGraphqlClient,
  references: ReferenceSummary[],
  locale: "en" | "ar" = "ar",
) {
  const metaobjectIds = references
    .filter((reference) => reference.kind === "METAOBJECT")
    .map((reference) => reference.id);
  const enriched = await getMetaobjectReferenceSummariesByIds(admin, metaobjectIds, locale);

  return references.map((reference) =>
    reference.kind === "METAOBJECT" ? enriched.get(reference.id) || reference : reference,
  );
}

export async function saveMetaobjectReferenceFields(
  admin: AdminGraphqlClient,
  input: {
    metaobjectId: string;
    locale: "english" | "arabic";
    fields: Array<{
      key: string;
      label: string;
      type: string;
      value: string;
    }>;
  },
) {
  const editableFields = input.fields.filter((field) => canInlineEditMetafield(field.type));

  if (editableFields.length === 0) {
    return {
      ok: false,
      message: "No editable metaobject fields were provided.",
      errors: [{ message: "This metaobject doesn't expose inline-editable fields in the app yet." }],
    } satisfies SaveResult & { reference?: ReferenceSummary };
  }

  let result: SaveResult;

  if (input.locale === "english") {
    const data = await adminGraphql<{
      metaobjectUpdate: {
        userErrors: Array<{
          field?: string[] | null;
          message: string;
        }>;
      };
    }>(admin, METAOBJECT_UPDATE_MUTATION, {
      id: input.metaobjectId,
      metaobject: {
        fields: editableFields.map((field) => ({
          key: field.key,
          value: normalizeMetaobjectFieldValue(field.type, field.value),
        })),
      },
    });

    result = data.metaobjectUpdate.userErrors.length
      ? {
          ok: false,
          message: "Some metaobject fields could not be saved.",
          errors: data.metaobjectUpdate.userErrors.map((error) => ({
            field: error.field?.join("."),
            message: error.message,
          })),
        }
      : {
          ok: true,
          message: "Metaobject fields saved.",
        };
  } else {
    const translatableResources = await getTranslatableResourcesByIds(admin, [input.metaobjectId], "ar");
    const translatable = translatableResources.get(input.metaobjectId);

    if (!translatable) {
      return {
        ok: false,
        message: "This metaobject isn't available for translation.",
        errors: [{ message: "Shopify did not return a translatable metaobject resource for this entry." }],
      } satisfies SaveResult & { reference?: ReferenceSummary };
    }

    const translatableContentByKey = new Map(
      translatable.translatableContent.map((item) => [item.key, item]),
    );
    const translationErrors: Array<{ field?: string; message: string }> = [];
    const translationFields: Array<
      Pick<EditorTranslatableField, "resourceId" | "translationKey" | "digest" | "arabicValue" | "fieldType" | "label">
    > = [];

    for (const field of editableFields) {
      const source = translatableContentByKey.get(field.key);
      if (!source) {
        translationErrors.push({
          field: field.key,
          message: "This metaobject field is not translatable in Shopify.",
        });
        continue;
      }

      translationFields.push({
        resourceId: input.metaobjectId,
        translationKey: field.key,
        digest: source.digest,
        arabicValue: field.value,
        fieldType: field.type as EditorTranslatableField["fieldType"],
        label: field.label,
      });
    }

    const translationResult = await saveArabicFields(admin, translationFields);
    result =
      translationErrors.length || translationResult.errors?.length
        ? {
            ok: false,
            message: "Some Arabic metaobject fields could not be saved.",
            errors: [...translationErrors, ...(translationResult.errors || [])],
          }
        : {
            ok: true,
            message: translationResult.message,
          };
  }

  const reference = result.ok
    ? (await getMetaobjectReferenceSummariesByIds(admin, [input.metaobjectId], "ar")).get(input.metaobjectId)
    : undefined;

  return {
    ...result,
    reference,
  } satisfies SaveResult & { reference?: ReferenceSummary };
}

async function buildMetaobjectReferenceSummaries(
  admin: AdminGraphqlClient,
  nodes: RawMetaobjectNode[],
  locale: "en" | "ar",
) {
  const translations: Awaited<ReturnType<typeof getTranslatableResourcesByIds>> =
    locale === "ar"
      ? await getTranslatableResourcesByIds(
          admin,
          nodes.map((node) => node.id),
          "ar",
        )
      : new Map();

  return nodes.map((node) => {
    const translatable = locale === "ar" ? translations.get(node.id) : undefined;
    const translationByKey = new Map(translatable?.translations.map((item) => [item.key, item]) || []);
    const translatableContentByKey = new Map(translatable?.translatableContent.map((item) => [item.key, item]) || []);
    const fields = node.fields.map((field): MetaobjectReferenceField => {
      const translated = translationByKey.get(field.key);
      return {
        key: field.key,
        label: field.definition?.name || titleizeKey(field.key),
        type: field.definition?.type?.name || field.type,
        value: normalizeMetaobjectFieldForEditor(field.definition?.type?.name || field.type, field.value || ""),
        arabicValue: normalizeMetaobjectFieldForEditor(
          field.definition?.type?.name || field.type,
          translated?.value || "",
        ),
        digest: translatableContentByKey.get(field.key)?.digest,
        required: Boolean(field.definition?.required),
        outdated: translated?.outdated,
        isTranslatable: translatableContentByKey.has(field.key),
      };
    });
    const displayNameKey = node.definition?.displayNameKey || null;
    const localizedTitle =
      locale === "ar" && displayNameKey
        ? fields.find((field) => field.key === displayNameKey)?.arabicValue.trim() || null
        : null;

    return {
      id: node.id,
      kind: "METAOBJECT",
      title: node.displayName,
      localizedTitle,
      handle: node.handle,
      subtitle: node.definition?.name || node.type,
      localizedSubtitle: locale === "ar" ? node.definition?.name || node.type : null,
      metaobject: {
        definitionId: node.definition?.id,
        definitionName: node.definition?.name,
        displayNameKey,
        translatable: Boolean(node.definition?.capabilities?.translatable?.enabled),
        fields,
      },
    } satisfies ReferenceSummary;
  });
}

function normalizeMetaobjectFieldForEditor(type: string, value: string) {
  return type === "rich_text_field" ? richTextJsonToEditableText(value) : value;
}

function normalizeMetaobjectFieldValue(type: string, value: string) {
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

  if (type === "multi_line_text_field") {
    return value;
  }

  if (type === "body_html") {
    return editableTextToHtml(value);
  }

  return value;
}

function titleizeKey(value: string) {
  return value
    .split("_")
    .filter(Boolean)
    .map((part) => part[0]?.toUpperCase() + part.slice(1))
    .join(" ");
}
