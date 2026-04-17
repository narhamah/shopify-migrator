import { saveLocaleImageTargets } from "~/services/images.server";
import { saveMetafields } from "~/services/metafields.server";
import { getProductEditorModel } from "~/services/products.server";
import { saveArabicFields } from "~/services/translations.server";
import { writeAuditLog } from "~/services/audit.server";
import type { AdminGraphqlClient } from "~/lib/shopify-admin.server";
import type { DiscoveryConfig, EditorImageTarget, ProductEditorModel } from "~/types/editor";

export async function bulkCopyEnglishImagesToArabic(input: {
  admin: AdminGraphqlClient;
  shop: string;
  discoveryConfig: DiscoveryConfig;
  productIds: string[];
}) {
  return bulkImageOperation({
    ...input,
    action: "bulkCopyEnglishImagesToArabic",
    transform(model) {
      const primaryGroup = model.imageGroups[0];
      if (!primaryGroup || primaryGroup.english.length === 0) {
        return [];
      }

      const sourceImages = primaryGroup.english[0].images;
      return primaryGroup.arabic.map((target) => ({
        ...target,
        images: sourceImages,
      }));
    },
  });
}

export async function bulkClearArabicImages(input: {
  admin: AdminGraphqlClient;
  shop: string;
  discoveryConfig: DiscoveryConfig;
  productIds: string[];
}) {
  return bulkImageOperation({
    ...input,
    action: "bulkClearArabicImages",
    transform(model) {
      const primaryGroup = model.imageGroups[0];
      return (primaryGroup?.arabic || []).map((target) => ({
        ...target,
        images: [],
      }));
    },
  });
}

export async function bulkSetMetafield(input: {
  admin: AdminGraphqlClient;
  shop: string;
  productIds: string[];
  namespace: string;
  key: string;
  type: string;
  value: string;
}) {
  const failures: Array<{ productId: string; message: string }> = [];

  for (const productId of input.productIds) {
    const ownerId = `gid://shopify/Product/${productId}`;
    const result = await saveMetafields(input.admin, ownerId, [
      {
        namespace: input.namespace,
        key: input.key,
        type: input.type,
        value: input.value,
      },
    ]);

    if (!result.ok) {
      failures.push({
        productId,
        message: result.errors?.map((error) => error.message).join("; ") || result.message,
      });
    }
  }

  await writeAuditLog({
    shop: input.shop,
    action: "bulkSetMetafield",
    status: failures.length ? "error" : "success",
    payload: {
      productIds: input.productIds,
      namespace: input.namespace,
      key: input.key,
    },
    errorMessage: failures.length ? JSON.stringify(failures) : undefined,
  });

  return {
    ok: failures.length === 0,
    message: failures.length
      ? `${failures.length} products failed during metafield bulk update.`
      : `Updated ${input.productIds.length} products.`,
    failures,
  };
}

export async function bulkApplyArabicTranslations(input: {
  admin: AdminGraphqlClient;
  shop: string;
  discoveryConfig: DiscoveryConfig;
  productIds: string[];
  productValues?: Record<string, string>;
  metafieldValues?: Record<string, string>;
}) {
  const failures: Array<{ productId: string; message: string }> = [];

  for (const productId of input.productIds) {
    const model = await getProductEditorModel(input.admin, input.shop, productId, input.discoveryConfig);
    const fields = model.arabicFields.filter((field) => {
      if (field.resourceId === model.product.id) {
        return input.productValues?.[field.key] !== undefined;
      }

      return input.metafieldValues?.[field.key] !== undefined;
    });

    const result = await saveArabicFields(
      input.admin,
      fields.map((field) => ({
        ...field,
        arabicValue:
          field.resourceId === model.product.id
            ? input.productValues?.[field.key] || field.arabicValue
            : input.metafieldValues?.[field.key] || field.arabicValue,
      })),
    );

    if (!result.ok) {
      failures.push({
        productId,
        message: result.errors?.map((error) => error.message).join("; ") || result.message,
      });
    }
  }

  return {
    ok: failures.length === 0,
    message: failures.length
      ? `${failures.length} products failed during Arabic bulk updates.`
      : `Updated Arabic fields on ${input.productIds.length} products.`,
    failures,
  };
}

export async function bulkImportJson(input: {
  admin: AdminGraphqlClient;
  shop: string;
  discoveryConfig: DiscoveryConfig;
  updates: Array<{
    productId: string;
    metafields?: Array<{ namespace: string; key: string; type: string; value: string }>;
    arabicFields?: Array<{ key: string; value: string }>;
    copyEnglishImagesToArabic?: boolean;
  }>;
}) {
  const failures: Array<{ productId: string; message: string }> = [];

  for (const update of input.updates) {
    try {
      const model = await getProductEditorModel(
        input.admin,
        input.shop,
        update.productId,
        input.discoveryConfig,
      );

      if (update.metafields?.length) {
        const metafieldResult = await saveMetafields(input.admin, model.product.id, update.metafields);
        if (!metafieldResult.ok) {
          throw new Error(metafieldResult.message);
        }
      }

      if (update.arabicFields?.length) {
        const translationResult = await saveArabicFields(
          input.admin,
          model.arabicFields
            .filter((field) => update.arabicFields?.some((candidate) => candidate.key === field.key))
            .map((field) => ({
              ...field,
              arabicValue:
                update.arabicFields?.find((candidate) => candidate.key === field.key)?.value ||
                field.arabicValue,
            })),
        );
        if (!translationResult.ok) {
          throw new Error(translationResult.message);
        }
      }

      if (update.copyEnglishImagesToArabic) {
        const primaryGroup = model.imageGroups[0];
        if (primaryGroup?.english.length && primaryGroup.arabic.length) {
          const imageResult = await saveLocaleImageTargets(
            input.admin,
            model.product.id,
            primaryGroup.arabic.map((target) => ({
              ...target,
              images: primaryGroup.english[0].images,
            })),
          );
          if (!imageResult.ok) {
            throw new Error(imageResult.message);
          }
        }
      }
    } catch (error) {
      failures.push({
        productId: update.productId,
        message: error instanceof Error ? error.message : "Bulk import failed.",
      });
    }
  }

  return {
    ok: failures.length === 0,
    message: failures.length
      ? `${failures.length} products failed during JSON import.`
      : `Imported updates for ${input.updates.length} products.`,
    failures,
  };
}

export async function exportProductsJson(input: {
  admin: AdminGraphqlClient;
  shop: string;
  discoveryConfig: DiscoveryConfig;
  productIds: string[];
}) {
  const payload = [];
  for (const productId of input.productIds) {
    payload.push(await getProductEditorModel(input.admin, input.shop, productId, input.discoveryConfig));
  }
  return payload;
}

export async function exportProductsCsv(input: {
  admin: AdminGraphqlClient;
  shop: string;
  discoveryConfig: DiscoveryConfig;
  productIds: string[];
}) {
  const models: ProductEditorModel[] = [];
  for (const productId of input.productIds) {
    models.push(await getProductEditorModel(input.admin, input.shop, productId, input.discoveryConfig));
  }

  const headers = [
    "product_id",
    "handle",
    "title",
    "status",
    "vendor",
    "product_type",
    "english_image_count",
    "arabic_image_count",
  ];

  const rows = models.map((model) => [
    model.product.legacyId,
    model.product.handle,
    model.product.title,
    model.product.status,
    model.product.vendor,
    model.product.productType,
    String(model.imageGroups[0]?.english[0]?.images.length || 0),
    String(model.imageGroups[0]?.arabic[0]?.images.length || 0),
  ]);

  return [headers, ...rows]
    .map((row) => row.map(escapeCsvCell).join(","))
    .join("\n");
}

async function bulkImageOperation(input: {
  admin: AdminGraphqlClient;
  shop: string;
  discoveryConfig: DiscoveryConfig;
  productIds: string[];
  action: string;
  transform: (model: ProductEditorModel) => EditorImageTarget[];
}) {
  const failures: Array<{ productId: string; message: string }> = [];

  for (const productId of input.productIds) {
    try {
      const model = await getProductEditorModel(
        input.admin,
        input.shop,
        productId,
        input.discoveryConfig,
      );
      const targets = input.transform(model);
      const result = await saveLocaleImageTargets(input.admin, model.product.id, targets);
      if (!result.ok) {
        throw new Error(result.message);
      }
    } catch (error) {
      failures.push({
        productId,
        message: error instanceof Error ? error.message : "Bulk image operation failed.",
      });
    }
  }

  await writeAuditLog({
    shop: input.shop,
    action: input.action,
    status: failures.length ? "error" : "success",
    payload: { productIds: input.productIds },
    errorMessage: failures.length ? JSON.stringify(failures) : undefined,
  });

  return {
    ok: failures.length === 0,
    message: failures.length
      ? `${failures.length} products failed during ${input.action}.`
      : `Processed ${input.productIds.length} products.`,
    failures,
  };
}

function escapeCsvCell(value: string) {
  if (/[,"\n]/.test(value)) {
    return `"${value.replaceAll('"', '""')}"`;
  }
  return value;
}
