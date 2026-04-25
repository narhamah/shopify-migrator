import type { ActionFunctionArgs, LoaderFunctionArgs } from "react-router";
import { useLoaderData } from "react-router";

import { ProductEditorScreen } from "~/components/ProductEditorScreen";
import { canInlineEditMetafield } from "~/lib/metafields";
import { editableTextToHtml } from "~/lib/rich-text";
import { writeAuditLog } from "~/services/audit.server";
import { ensureDiscoveryConfig, toggleFavoriteMetafield } from "~/services/discovery.server";
import { saveLocaleImageTargets } from "~/services/images.server";
import { saveMetafields } from "~/services/metafields.server";
import { getProductEditorModel, saveCoreProduct } from "~/services/products.server";
import { saveArabicFields } from "~/services/translations.server";
import { authenticate } from "~/shopify.server";
import type { EditorImageGroup, EditorMetafield, EditorTranslatableField, SaveResult } from "~/types/editor";

export async function loader({ request, params }: LoaderFunctionArgs) {
  const { admin, session } = await authenticate.admin(request);
  const url = new URL(request.url);
  const discovery = await ensureDiscoveryConfig(admin, session.shop);
  const model = await getProductEditorModel(admin, session.shop, params.productId!, discovery);
  const queue = (url.searchParams.get("queue") || "")
    .split(",")
    .map((value) => value.trim())
    .filter(Boolean);
  const requestedIndex = Number(url.searchParams.get("index"));
  const matchedIndex = queue.findIndex((id) => id === params.productId);
  const currentIndex =
    Number.isInteger(requestedIndex) && queue[requestedIndex] === params.productId
      ? requestedIndex
      : matchedIndex;

  return {
    model,
    discovery,
    navigation: {
      queue,
      currentIndex,
      previousProductId: currentIndex > 0 ? queue[currentIndex - 1] : undefined,
      nextProductId: currentIndex >= 0 && currentIndex < queue.length - 1 ? queue[currentIndex + 1] : undefined,
      returnTo: url.searchParams.get("returnTo") || "/app/products",
    },
  };
}

export async function action({ request, params }: ActionFunctionArgs) {
  const { admin, session } = await authenticate.admin(request);
  const formData = await request.formData();
  const intent = String(formData.get("intent") || "");
  const payload = JSON.parse(String(formData.get("payload") || "{}")) as Record<string, unknown>;
  const productGid = `gid://shopify/Product/${params.productId}`;

  if (intent === "saveCore") {
    const result = await saveCoreProduct(admin, {
      productId: productGid,
      title: String(payload.title || ""),
      handle: String(payload.handle || ""),
      descriptionHtml: editableTextToHtml(String(payload.descriptionHtml || "")),
      vendor: String(payload.vendor || ""),
      productType: String(payload.productType || ""),
      tags: Array.isArray(payload.tags) ? (payload.tags as string[]) : [],
      status: String(payload.status || "ACTIVE"),
      seoTitle: String(payload.seoTitle || ""),
      seoDescription: String(payload.seoDescription || ""),
    });

    await writeAuditLog({
      shop: session.shop,
      action: "saveCore",
      status: result.ok ? "success" : "error",
      resourceType: "product",
      resourceId: productGid,
      payload,
      errorMessage: result.errors?.map((error) => error.message).join("; "),
    });
    return result;
  }

  if (intent === "saveArabic") {
    return saveArabicFields(admin, payload.fields as EditorTranslatableField[]);
  }

  if (intent === "saveMetafields") {
    const metafieldResult = await saveMetafields(
      admin,
      productGid,
      (payload.metafields as EditorMetafield[]).filter((metafield) => canInlineEditMetafield(metafield.type)),
    );
    const arabicMetafieldFields = Array.isArray(payload.arabicFields)
      ? (payload.arabicFields as EditorTranslatableField[]).filter((field) => field.resourceId !== productGid)
      : [];

    if (arabicMetafieldFields.length === 0) {
      return metafieldResult;
    }

    const arabicResult = await saveArabicFields(admin, arabicMetafieldFields);
    return combineResults(
      [
        { key: "metafields", result: metafieldResult },
        { key: "arabic", result: arabicResult },
      ],
      "Metafields saved.",
      "Some metafield updates could not be saved.",
    );
  }

  if (intent === "saveImages") {
    const targets = flattenImageTargets(payload.targets as EditorImageGroup[]);
    return saveLocaleImageTargets(admin, productGid, targets);
  }

  if (intent === "saveAll") {
    const corePayload = isRecord(payload.core) ? payload.core : null;
    const metafieldPayload = Array.isArray(payload.metafields) ? (payload.metafields as EditorMetafield[]) : [];
    const arabicPayload = Array.isArray(payload.arabicFields)
      ? (payload.arabicFields as EditorTranslatableField[])
      : [];
    const imagePayload = Array.isArray(payload.imageGroups)
      ? (payload.imageGroups as EditorImageGroup[])
      : [];

    const coreResult = corePayload
      ? await saveCoreProduct(admin, {
          productId: productGid,
          title: String(corePayload.title || ""),
          handle: String(corePayload.handle || ""),
          descriptionHtml: editableTextToHtml(String(corePayload.descriptionHtml || "")),
          vendor: String(corePayload.vendor || ""),
          productType: String(corePayload.productType || ""),
          tags: Array.isArray(corePayload.tags) ? (corePayload.tags as string[]) : [],
          status: String(corePayload.status || "ACTIVE"),
          seoTitle: String(corePayload.seoTitle || ""),
          seoDescription: String(corePayload.seoDescription || ""),
        })
      : successResult("No core changes to save.");
    const metafieldResult = metafieldPayload.length
      ? await saveMetafields(
          admin,
          productGid,
          metafieldPayload.filter((metafield) => canInlineEditMetafield(metafield.type)),
        )
      : successResult("No metafield changes to save.");
    const arabicResult = arabicPayload.length
      ? await saveArabicFields(admin, arabicPayload)
      : successResult("No Arabic translation changes to save.");
    const imageResult = imagePayload.length
      ? await saveLocaleImageTargets(admin, productGid, flattenImageTargets(imagePayload))
      : successResult("No media changes to save.");

    const result = combineResults(
      [
        { key: "core", result: coreResult },
        { key: "arabic", result: arabicResult },
        { key: "metafields", result: metafieldResult },
        { key: "images", result: imageResult },
      ],
      "All product changes saved.",
      "Some product updates could not be saved.",
    );

    await writeAuditLog({
      shop: session.shop,
      action: "saveAll",
      status: result.ok ? "success" : "error",
      resourceType: "product",
      resourceId: productGid,
      payload,
      errorMessage: result.errors?.map((error) => error.message).join("; "),
    });
    return result;
  }

  if (intent === "toggleFavorite") {
    const isPinned = await toggleFavoriteMetafield(
      session.shop,
      String(payload.namespace || ""),
      String(payload.key || ""),
    );
    return {
      ok: true,
      message: isPinned ? "Metafield pinned." : "Metafield unpinned.",
    } satisfies SaveResult;
  }

  return {
    ok: false,
    message: "Unknown action.",
  } satisfies SaveResult;
}

export default function ProductEditorRoute() {
  const { model, discovery, navigation } = useLoaderData<typeof loader>();
  return <ProductEditorScreen model={model} discovery={discovery} navigation={navigation} />;
}

function flattenImageTargets(groups: EditorImageGroup[]) {
  return groups
    .flatMap((group) => [...group.english, ...group.arabic])
    .filter((target) => target.target.storage === "translation" || target.target.storage === "metafield");
}

function combineResults(
  results: Array<{ key: "core" | "arabic" | "metafields" | "images"; result: SaveResult }>,
  successMessage: string,
  errorMessage: string,
) {
  const errors = results.flatMap(({ result }) => result.errors || []);
  const conflicts = results.flatMap(({ result }) => result.conflicts || []);
  const sectionResults = results.map(({ key, result }) => ({
    key,
    ok: result.ok,
    message: result.message,
    errorCount: result.errors?.length || 0,
  }));

  if (errors.length > 0 || conflicts.length > 0) {
    return {
      ok: false,
      message: errorMessage,
      errors,
      sectionResults,
      conflicts,
    } satisfies SaveResult;
  }

  return {
    ok: true,
    message: successMessage,
    sectionResults,
    conflicts,
  } satisfies SaveResult;
}

function successResult(message: string) {
  return {
    ok: true,
    message,
  } satisfies SaveResult;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}
