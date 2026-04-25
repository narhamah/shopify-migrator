import type { ActionFunctionArgs } from "react-router";

import shopify from "~/shopify.server";
import { writeAuditLog } from "~/services/audit.server";

export async function action({ request }: ActionFunctionArgs) {
  const { shop, topic, payload } = await shopify.authenticate.webhook(request);

  await writeAuditLog({
    shop,
    action: topic,
    status: "success",
    payload,
  });

  return new Response();
}
