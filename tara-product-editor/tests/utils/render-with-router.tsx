import type { ReactElement } from "react";

import { render } from "@testing-library/react";
import { createMemoryRouter, RouterProvider } from "react-router";
import { AppProvider } from "@shopify/polaris";
import enTranslations from "@shopify/polaris/locales/en.json";

export function renderWithRouter(element: ReactElement, initialEntry = "/") {
  let currentElement = element;

  function RouteElement() {
    return currentElement;
  }

  const router = createMemoryRouter(
    [
      {
        path: "*",
        element: <RouteElement />,
      },
    ],
    {
      initialEntries: [initialEntry],
    },
  );

  const rendered = render(
    <AppProvider i18n={enTranslations}>
      <RouterProvider router={router} />
    </AppProvider>,
  );

  return {
    ...rendered,
    rerenderWithRouter(nextElement: ReactElement) {
      currentElement = nextElement;
      rendered.rerender(
        <AppProvider i18n={enTranslations}>
          <RouterProvider router={router} />
        </AppProvider>,
      );
    },
  };
}
