import enTranslations from "@shopify/polaris/locales/en.json";
import {
  AppProvider as PolarisAppProvider,
  Box,
  Button,
  Frame,
  InlineStack,
  Loading,
} from "@shopify/polaris";
import { AppProvider as ShopifyAppProvider } from "@shopify/shopify-app-react-router/react";
import { Outlet, useLoaderData, useNavigation } from "react-router";
import type { LoaderFunctionArgs } from "react-router";

import { PolarisRouterLink } from "~/components/PolarisRouterLink";
import { authenticate, getShopifyApiKeyForRequest } from "~/shopify.server";

export async function loader({ request }: LoaderFunctionArgs) {
  const { session } = await authenticate.admin(request);

  return {
    apiKey: getShopifyApiKeyForRequest(request),
    shop: session.shop,
  };
}

export default function AppLayout() {
  const { apiKey } = useLoaderData<typeof loader>();
  const navigation = useNavigation();

  return (
    <ShopifyAppProvider embedded apiKey={apiKey}>
      <PolarisAppProvider i18n={enTranslations} linkComponent={PolarisRouterLink}>
        <Frame>
          {navigation.state !== "idle" ? <Loading /> : null}
          <Box padding="400">
            <InlineStack gap="300">
              <Button url="/app" variant="tertiary">
                Overview
              </Button>
              <Button url="/app/products" variant="tertiary">
                Products
              </Button>
              <Button url="/app/bulk" variant="tertiary">
                Bulk Tools
              </Button>
              <Button url="/app/discovery" variant="tertiary">
                Discovery
              </Button>
            </InlineStack>
          </Box>
          <Outlet />
        </Frame>
      </PolarisAppProvider>
    </ShopifyAppProvider>
  );
}
