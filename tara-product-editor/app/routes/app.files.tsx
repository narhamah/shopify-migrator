import type { ActionFunctionArgs, LoaderFunctionArgs } from "react-router";

import { searchShopifyFiles, uploadShopifyImageFile } from "~/services/images.server";
import { authenticate } from "~/shopify.server";

export async function loader({ request }: LoaderFunctionArgs) {
  const { admin } = await authenticate.admin(request);
  const url = new URL(request.url);
  const query = url.searchParams.get("query") || undefined;
  const after = url.searchParams.get("after") || undefined;
  const append = url.searchParams.get("append") === "1";
  const result = await searchShopifyFiles(admin, { query, after });

  return {
    append,
    query: query || "",
    ...result,
  };
}

export async function action({ request }: ActionFunctionArgs) {
  const { admin } = await authenticate.admin(request);
  const formData = await request.formData();
  const uploaded = formData.get("file");
  const alt = String(formData.get("alt") || "");

  if (!(uploaded instanceof File)) {
    return {
      ok: false,
      message: "No file was uploaded.",
    };
  }

  return uploadShopifyImageFile(admin, {
    file: uploaded,
    alt,
  });
}

export default function FilesRoute() {
  return null;
}
