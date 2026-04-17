import {
  BlockStack,
  Button,
  Card,
  InlineStack,
  Page,
  Text,
} from "@shopify/polaris";
import type { ActionFunctionArgs, LoaderFunctionArgs } from "react-router";
import { Form, useActionData, useLoaderData } from "react-router";

import { JsonCodeBlock } from "~/components/JsonCodeBlock";
import { discoverStoreSchema, ensureDiscoveryConfig } from "~/services/discovery.server";
import { authenticate } from "~/shopify.server";

export async function loader({ request }: LoaderFunctionArgs) {
  const { admin, session } = await authenticate.admin(request);
  const discovery = await ensureDiscoveryConfig(admin, session.shop);

  return { discovery };
}

export async function action({ request }: ActionFunctionArgs) {
  const { admin, session } = await authenticate.admin(request);
  const formData = await request.formData();
  const intent = formData.get("intent");

  if (intent === "rediscover") {
    const discovery = await discoverStoreSchema(admin, session.shop);
    return { ok: true, discovery };
  }

  return { ok: false };
}

export default function DiscoveryPage() {
  const { discovery } = useLoaderData<typeof loader>();
  const actionData = useActionData<typeof action>();
  const activeDiscovery = actionData?.discovery || discovery;

  return (
    <Page title="Store Discovery" subtitle="Live product schema and locale mapping">
      <BlockStack gap="500">
        <Card>
          <BlockStack gap="300">
            <InlineStack align="space-between" blockAlign="center">
              <Text as="h2" variant="headingMd">
                Current discovery snapshot
              </Text>
              <Form method="post">
                <input type="hidden" name="intent" value="rediscover" />
                <Button submit variant="primary">
                  Re-run discovery
                </Button>
              </Form>
            </InlineStack>
            <Text as="p" variant="bodySm" tone="subdued">
              Generated at {activeDiscovery.generatedAt}
            </Text>
            {activeDiscovery.notes.map((note) => (
              <Text key={note} as="p" variant="bodyMd">
                {note}
              </Text>
            ))}
          </BlockStack>
        </Card>

        <Card>
          <BlockStack gap="300">
            <Text as="h2" variant="headingMd">
              Mapping JSON
            </Text>
            <JsonCodeBlock value={JSON.stringify(activeDiscovery, null, 2)} height={520} />
          </BlockStack>
        </Card>
      </BlockStack>
    </Page>
  );
}
