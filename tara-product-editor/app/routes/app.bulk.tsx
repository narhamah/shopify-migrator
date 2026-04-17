import type { CSSProperties, ReactNode } from "react";

import {
  Banner,
  BlockStack,
  Button,
  Card,
  Page,
  Select,
  Text,
} from "@shopify/polaris";
import type { ActionFunctionArgs, LoaderFunctionArgs } from "react-router";
import { Form, useActionData, useLoaderData, useSearchParams } from "react-router";

import {
  bulkApplyArabicTranslations,
  bulkClearArabicImages,
  bulkCopyEnglishImagesToArabic,
  bulkImportJson,
  bulkSetMetafield,
  exportProductsCsv,
  exportProductsJson,
} from "~/services/bulk.server";
import { ensureDiscoveryConfig } from "~/services/discovery.server";
import { authenticate } from "~/shopify.server";

export async function loader({ request }: LoaderFunctionArgs) {
  const { admin, session } = await authenticate.admin(request);
  const url = new URL(request.url);
  const ids = (url.searchParams.get("ids") || "")
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);
  const discovery = await ensureDiscoveryConfig(admin, session.shop);

  return {
    ids,
    discovery,
  };
}

export async function action({ request }: ActionFunctionArgs) {
  const { admin, session } = await authenticate.admin(request);
  const formData = await request.formData();
  const intent = String(formData.get("intent") || "");
  const productIds = String(formData.get("productIds") || "")
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);
  const discovery = await ensureDiscoveryConfig(admin, session.shop);

  if (intent === "copyImages") {
    return bulkCopyEnglishImagesToArabic({
      admin,
      shop: session.shop,
      discoveryConfig: discovery,
      productIds,
    });
  }

  if (intent === "clearArabicImages") {
    return bulkClearArabicImages({
      admin,
      shop: session.shop,
      discoveryConfig: discovery,
      productIds,
    });
  }

  if (intent === "bulkSetMetafield") {
    return bulkSetMetafield({
      admin,
      shop: session.shop,
      productIds,
      namespace: String(formData.get("namespace") || ""),
      key: String(formData.get("key") || ""),
      type: String(formData.get("type") || ""),
      value: String(formData.get("value") || ""),
    });
  }

  if (intent === "bulkApplyArabic") {
    const payload = JSON.parse(String(formData.get("translationPayload") || "{}")) as {
      productValues?: Record<string, string>;
      metafieldValues?: Record<string, string>;
    };
    return bulkApplyArabicTranslations({
      admin,
      shop: session.shop,
      discoveryConfig: discovery,
      productIds,
      productValues: payload.productValues,
      metafieldValues: payload.metafieldValues,
    });
  }

  if (intent === "exportJson") {
    const payload = await exportProductsJson({
      admin,
      shop: session.shop,
      discoveryConfig: discovery,
      productIds,
    });
    return new Response(JSON.stringify(payload, null, 2), {
      headers: {
        "Content-Type": "application/json",
        "Content-Disposition": 'attachment; filename="tara-product-editor-export.json"',
      },
    });
  }

  if (intent === "exportCsv") {
    const payload = await exportProductsCsv({
      admin,
      shop: session.shop,
      discoveryConfig: discovery,
      productIds,
    });
    return new Response(payload, {
      headers: {
        "Content-Type": "text/csv; charset=utf-8",
        "Content-Disposition": 'attachment; filename="tara-product-editor-export.csv"',
      },
    });
  }

  if (intent === "importJson") {
    const updates = JSON.parse(String(formData.get("importPayload") || "[]")) as Array<{
      productId: string;
      metafields?: Array<{ namespace: string; key: string; type: string; value: string }>;
      arabicFields?: Array<{ key: string; value: string }>;
      copyEnglishImagesToArabic?: boolean;
    }>;
    return bulkImportJson({
      admin,
      shop: session.shop,
      discoveryConfig: discovery,
      updates,
    });
  }

  return {
    ok: false,
    message: "Unknown bulk action.",
  };
}

export default function BulkPage() {
  const { ids } = useLoaderData<typeof loader>();
  const actionData = useActionData<typeof action>();
  const [searchParams] = useSearchParams();
  const idsValue = searchParams.get("ids") || ids.join(",");

  return (
    <Page title="Bulk Tools" subtitle="Run image, metafield, translation, export, and import operations">
      <BlockStack gap="500">
        {actionData && "ok" in actionData ? (
          <Banner tone={actionData.ok ? "success" : "critical"} title={actionData.message}>
            {"failures" in actionData && actionData.failures?.length ? (
              <BlockStack gap="100">
                {actionData.failures.slice(0, 10).map((failure) => (
                  <Text key={`${failure.productId}-${failure.message}`} as="p" variant="bodySm">
                    {failure.productId}: {failure.message}
                  </Text>
                ))}
              </BlockStack>
            ) : null}
          </Banner>
        ) : null}

        <Card>
          <BlockStack gap="200">
            <Text as="h2" variant="headingMd">
              Selected products
            </Text>
            <Text as="p" variant="bodySm" tone="subdued">
              Pass product numeric IDs in the query string or edit them here before running a bulk action.
            </Text>
            <Text as="p" variant="bodyMd">
              {ids.length} selected
            </Text>
          </BlockStack>
        </Card>

        <ActionCard
          title="Copy English images to Arabic"
          description="Copies the English locale image set into all configured Arabic image targets."
        >
          <BaseBulkForm intent="copyImages" productIds={idsValue}>
            <Button submit variant="primary">
              Copy English images to Arabic
            </Button>
          </BaseBulkForm>
        </ActionCard>

        <ActionCard
          title="Clear Arabic images"
          description="Clears Arabic image metafields and Arabic translations for the configured gallery targets."
        >
          <BaseBulkForm intent="clearArabicImages" productIds={idsValue}>
            <Button submit tone="critical">
              Clear Arabic image targets
            </Button>
          </BaseBulkForm>
        </ActionCard>

        <ActionCard
          title="Set one metafield across selected products"
          description="Applies the same metafield value to every selected product."
        >
          <BaseBulkForm intent="bulkSetMetafield" productIds={idsValue}>
            <BlockStack gap="300">
              <label>
                <Text as="span" variant="bodyMd">
                  Namespace
                </Text>
                <input name="namespace" defaultValue="custom" style={inputStyle} />
              </label>
              <label>
                <Text as="span" variant="bodyMd">
                  Key
                </Text>
                <input name="key" style={inputStyle} />
              </label>
              <Select
                label="Type"
                name="type"
                options={[
                  { label: "Single line text", value: "single_line_text_field" },
                  { label: "Multi line text", value: "multi_line_text_field" },
                  { label: "Rich text", value: "rich_text_field" },
                  { label: "JSON", value: "json" },
                  { label: "List file reference", value: "list.file_reference" },
                ]}
              />
              <label>
                <Text as="span" variant="bodyMd">
                  Value
                </Text>
                <textarea name="value" style={textareaStyle} rows={6} />
              </label>
              <Button submit variant="primary">
                Set metafield
              </Button>
            </BlockStack>
          </BaseBulkForm>
        </ActionCard>

        <ActionCard
          title="Bulk apply Arabic content"
          description='Paste a JSON object like {"productValues":{"title":"..."}, "metafieldValues":{"short_description":"..."}}.'
        >
          <BaseBulkForm intent="bulkApplyArabic" productIds={idsValue}>
            <BlockStack gap="300">
              <label>
                <Text as="span" variant="bodyMd">
                  Translation payload JSON
                </Text>
                <textarea
                  name="translationPayload"
                  defaultValue='{"productValues": {}, "metafieldValues": {}}'
                  style={textareaStyle}
                  rows={10}
                />
              </label>
              <Button submit variant="primary">
                Apply Arabic values
              </Button>
            </BlockStack>
          </BaseBulkForm>
        </ActionCard>

        <ActionCard
          title="Export selected products"
          description="Export the selected product editor model to JSON or a compact CSV."
        >
          <BaseBulkForm intent="exportJson" productIds={idsValue}>
            <BlockStack gap="300">
              <Button submit variant="primary">
                Export JSON
              </Button>
            </BlockStack>
          </BaseBulkForm>
          <BoxSpacer />
          <BaseBulkForm intent="exportCsv" productIds={idsValue}>
            <Button submit>Export CSV</Button>
          </BaseBulkForm>
        </ActionCard>

        <ActionCard
          title="Import structured JSON updates"
          description="Import a JSON array of structured updates for selected products."
        >
          <BaseBulkForm intent="importJson" productIds={idsValue}>
            <BlockStack gap="300">
              <label>
                <Text as="span" variant="bodyMd">
                  Import payload
                </Text>
                <textarea
                  name="importPayload"
                  defaultValue='[{"productId":"9218580021481","copyEnglishImagesToArabic":true}]'
                  style={textareaStyle}
                  rows={12}
                />
              </label>
              <Button submit variant="primary">
                Import JSON updates
              </Button>
            </BlockStack>
          </BaseBulkForm>
        </ActionCard>
      </BlockStack>
    </Page>
  );
}

function BaseBulkForm({
  children,
  intent,
  productIds,
}: {
  children: ReactNode;
  intent: string;
  productIds: string;
}) {
  return (
    <Form method="post">
      <input type="hidden" name="intent" value={intent} />
      <input type="hidden" name="productIds" value={productIds} />
      <BlockStack gap="300">{children}</BlockStack>
    </Form>
  );
}

function ActionCard({
  title,
  description,
  children,
}: {
  title: string;
  description: string;
  children: ReactNode;
}) {
  return (
    <Card>
      <BlockStack gap="300">
        <Text as="h2" variant="headingMd">
          {title}
        </Text>
        <Text as="p" variant="bodySm" tone="subdued">
          {description}
        </Text>
        {children}
      </BlockStack>
    </Card>
  );
}

function BoxSpacer() {
  return <div style={{ height: 12 }} />;
}

const inputStyle = {
  display: "block",
  width: "100%",
  marginTop: 6,
  padding: "10px 12px",
  borderRadius: 8,
  border: "1px solid #c9cccf",
} satisfies CSSProperties;

const textareaStyle = {
  ...inputStyle,
  fontFamily: "monospace",
  resize: "vertical",
} satisfies CSSProperties;
