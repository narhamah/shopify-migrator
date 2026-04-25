import type { LoaderFunctionArgs } from "react-router";
import { redirect } from "react-router";

export async function loader({ request }: LoaderFunctionArgs) {
  const url = new URL(request.url);
  const search = url.search.length > 0 ? url.search : "";

  throw redirect(`/app/products${search}`);
}

export default function RootIndexRedirect() {
  return null;
}
