import type { ActionFunctionArgs } from "react-router";

import prisma from "~/db.server";
import shopify from "~/shopify.server";

export async function action({ request }: ActionFunctionArgs) {
  const { shop, topic } = await shopify.authenticate.webhook(request);

  if (topic === "APP_UNINSTALLED") {
    await prisma.session.deleteMany({ where: { shop } });
    await prisma.discoveryConfig.deleteMany({ where: { shop } });
    await prisma.favoriteMetafield.deleteMany({ where: { shop } });
  }

  return new Response();
}
