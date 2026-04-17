import type { LoaderFunctionArgs } from "react-router";

import shopify from "~/shopify.server";

export async function loader({ request }: LoaderFunctionArgs) {
  await shopify.authenticate.admin(request);
  return null;
}

export default function AuthRoute() {
  return null;
}
