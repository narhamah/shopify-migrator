import polarisStyles from "@shopify/polaris/build/esm/styles.css?url";
import {
  isRouteErrorResponse,
  Links,
  Meta,
  Outlet,
  Scripts,
  ScrollRestoration,
  useRouteError,
} from "react-router";

export function links() {
  return [{ rel: "stylesheet", href: polarisStyles }];
}

export default function Root() {
  return (
    <html lang="en">
      <head>
        <meta charSet="utf-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <Meta />
        <Links />
      </head>
      <body>
        <Outlet />
        <ScrollRestoration />
        <Scripts />
      </body>
    </html>
  );
}

export function ErrorBoundary() {
  const error = useRouteError();

  let title = "Application error";
  let message = "The app could not complete this request.";

  if (isRouteErrorResponse(error)) {
    title = `${error.status} ${error.statusText}`;
    message =
      typeof error.data === "string"
        ? error.data
        : "The requested route returned an error response.";
  } else if (error instanceof Error) {
    message = error.message;
  }

  return (
    <html lang="en">
      <head>
        <meta charSet="utf-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <title>{title}</title>
        <Links />
      </head>
      <body>
        <main style={{ padding: "2rem", fontFamily: "Inter, sans-serif" }}>
          <h1>{title}</h1>
          <p>{message}</p>
        </main>
        <Scripts />
      </body>
    </html>
  );
}
