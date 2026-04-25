import {
  editableTextToHtml,
  htmlToEditableText,
  richTextJsonToEditableText,
  sanitizeRichTextJson,
} from "~/lib/rich-text";
import { isTextLikeTranslatableMetafieldType } from "~/lib/metafields";
import { adminGraphql, type AdminGraphqlClient } from "~/lib/shopify-admin.server";
import type { EditorTranslatableField, SaveConflict, SaveResult } from "~/types/editor";

const TRANSLATABLE_RESOURCE_QUERY = `
  query TranslatableResource($resourceId: ID!, $locale: String!) {
    translatableResource(resourceId: $resourceId) {
      resourceId
      translatableContent {
        key
        value
        digest
        locale
      }
      translations(locale: $locale) {
        key
        value
        outdated
        locale
      }
    }
  }
`;

const TRANSLATABLE_BY_IDS_QUERY = `
  query TranslatableResourcesByIds($resourceIds: [ID!]!, $first: Int!, $locale: String!) {
    translatableResourcesByIds(resourceIds: $resourceIds, first: $first) {
      edges {
        node {
          resourceId
          translatableContent {
            key
            value
            digest
            locale
          }
          translations(locale: $locale) {
            key
            value
            outdated
            locale
          }
        }
      }
    }
  }
`;

const TRANSLATIONS_REGISTER_MUTATION = `
  mutation RegisterTranslations($resourceId: ID!, $translations: [TranslationInput!]!) {
    translationsRegister(resourceId: $resourceId, translations: $translations) {
      userErrors {
        field
        message
      }
      translations {
        key
        value
        locale
      }
    }
  }
`;

interface TranslatableResourcePayload {
  resourceId: string;
  translatableContent: Array<{
    key: string;
    value: string;
    digest: string;
    locale: string;
  }>;
  translations: Array<{
    key: string;
    value: string;
    outdated: boolean;
    locale: string;
  }>;
}

export async function getTranslatableResource(
  admin: AdminGraphqlClient,
  resourceId: string,
  locale = "ar",
) {
  const data = await adminGraphql<{
    translatableResource: TranslatableResourcePayload | null;
  }>(admin, TRANSLATABLE_RESOURCE_QUERY, { resourceId, locale });

  return data.translatableResource;
}

export async function getTranslatableResourcesByIds(
  admin: AdminGraphqlClient,
  resourceIds: string[],
  locale = "ar",
) {
  if (resourceIds.length === 0) {
    return new Map<string, TranslatableResourcePayload>();
  }

  const data = await adminGraphql<{
    translatableResourcesByIds: {
      edges: Array<{
        node: TranslatableResourcePayload;
      }>;
    };
  }>(admin, TRANSLATABLE_BY_IDS_QUERY, {
    resourceIds,
    first: resourceIds.length,
    locale,
  });

  return new Map(
    data.translatableResourcesByIds.edges.map((edge) => [edge.node.resourceId, edge.node]),
  );
}

export async function registerArabicTranslations(
  admin: AdminGraphqlClient,
  resourceId: string,
  translations: Array<{
    key: string;
    value: string;
    translatableContentDigest: string;
  }>,
) {
  const data = await adminGraphql<{
    translationsRegister: {
      userErrors: Array<{
        field?: string[] | null;
        message: string;
      }>;
    };
  }>(admin, TRANSLATIONS_REGISTER_MUTATION, {
    resourceId,
    translations: translations.map((translation) => ({
      ...translation,
      locale: "ar",
    })),
  });

  return data.translationsRegister.userErrors;
}

export async function saveArabicFields(
  admin: AdminGraphqlClient,
  fields: Array<Pick<EditorTranslatableField, "resourceId" | "translationKey" | "digest" | "arabicValue" | "fieldType" | "label">>,
) {
  const grouped = new Map<
    string,
    Array<Pick<EditorTranslatableField, "translationKey" | "digest" | "arabicValue" | "fieldType" | "label">>
  >();

  for (const field of fields) {
    if (
      field.fieldType !== "product_field" &&
      !isTextLikeTranslatableMetafieldType(field.fieldType)
    ) {
      continue;
    }

    if (field.arabicValue.trim().length === 0) {
      continue;
    }
    if (!grouped.has(field.resourceId)) {
      grouped.set(field.resourceId, []);
    }
    grouped.get(field.resourceId)?.push(field);
  }

  if (grouped.size === 0) {
    return {
      ok: true,
      message: "No Arabic translations to save.",
    } satisfies SaveResult;
  }

  const errors: Array<{ field?: string; message: string }> = [];
  const conflicts: SaveConflict[] = [];
  const latestResources = await getTranslatableResourcesByIds(admin, [...grouped.keys()], "ar");

  for (const [resourceId, group] of grouped.entries()) {
    const latestResource = latestResources.get(resourceId);
    const userErrors = await registerArabicTranslations(
      admin,
      resourceId,
      group.map((field) => ({
        key: field.translationKey,
        value: normalizeArabicTranslationValue(field),
        translatableContentDigest:
          latestResource?.translatableContent.find((item) => item.key === field.translationKey)?.digest || field.digest,
      })),
    );

    if (userErrors.some((error) => isTranslationDigestConflict(error.message))) {
      const refreshedResource = await getTranslatableResource(admin, resourceId, "ar");

      for (const error of userErrors) {
        const inputIndex = getUserErrorInputIndex(error.field);
        const targetField = inputIndex === null ? undefined : group[inputIndex];

        if (!targetField) {
          errors.push({
            field: error.field?.join("."),
            message: error.message,
          });
          continue;
        }

        if (!isTranslationDigestConflict(error.message)) {
          errors.push({
            field: error.field?.join("."),
            message: error.message,
          });
          continue;
        }

        const refreshedTranslation = refreshedResource?.translations.find(
          (item) => item.key === targetField.translationKey,
        );
        const refreshedSource = refreshedResource?.translatableContent.find(
          (item) => item.key === targetField.translationKey,
        );

        conflicts.push({
          kind: "translation",
          section: "arabic",
          resourceId,
          fieldKey: targetField.translationKey,
          label: targetField.label,
          message: error.message,
          latestValue: normalizeTranslationEditorValue(
            targetField.fieldType,
            refreshedTranslation?.value || "",
            targetField.translationKey,
          ),
          latestDigest: refreshedSource?.digest,
          latestSourceValue: normalizeTranslationEditorValue(
            targetField.fieldType,
            refreshedSource?.value || "",
            targetField.translationKey,
          ),
        });
      }
      continue;
    }

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
      message: "Some Arabic translations could not be saved.",
      errors,
      conflicts,
    }
    : {
        ok: true,
        message: "Arabic content saved.",
        conflicts,
      };

  return result;
}

function normalizeArabicTranslationValue(
  field: Pick<EditorTranslatableField, "translationKey" | "arabicValue" | "fieldType">,
) {
  if (field.translationKey === "body_html") {
    return editableTextToHtml(field.arabicValue);
  }

  if (field.fieldType === "rich_text_field") {
    return sanitizeRichTextJson(field.arabicValue);
  }

  return field.arabicValue;
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

function isTranslationDigestConflict(message: string) {
  return message.includes("Translatable content hash is invalid");
}

function normalizeTranslationEditorValue(
  fieldType: EditorTranslatableField["fieldType"],
  value: string,
  translationKey: string,
) {
  if (translationKey === "body_html") {
    return htmlToEditableText(value);
  }

  if (fieldType === "rich_text_field") {
    return richTextJsonToEditableText(value);
  }

  return value;
}
