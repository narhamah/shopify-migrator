import type { ActionFunctionArgs } from "react-router";

import { saveMetaobjectReferenceFields } from "~/services/metaobjects.server";
import { authenticate } from "~/shopify.server";

export async function action({ request }: ActionFunctionArgs) {
  const { admin } = await authenticate.admin(request);
  const formData = await request.formData();
  const intent = String(formData.get("intent") || "");

  if (intent !== "saveMetaobject") {
    return {
      ok: false,
      message: "Unknown metaobject action.",
    };
  }

  const payload = JSON.parse(String(formData.get("payload") || "{}")) as {
    metaobjectId?: string;
    locale?: "english" | "arabic";
    fields?: Array<{
      key: string;
      label: string;
      type: string;
      value: string;
    }>;
  };

  if (!payload.metaobjectId || !payload.locale || !Array.isArray(payload.fields)) {
    return {
      ok: false,
      message: "Metaobject payload is incomplete.",
    };
  }

  return saveMetaobjectReferenceFields(admin, {
    metaobjectId: payload.metaobjectId,
    locale: payload.locale,
    fields: payload.fields,
  });
}

export default function MetaobjectsRoute() {
  return null;
}
