import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it } from "vitest";

import { ProductEditorScreen } from "../app/components/ProductEditorScreen";

import { discoveryFixture, navigationFixture, productEditorModelFixture } from "./fixtures/editor";
import { renderWithRouter } from "./utils/render-with-router";

describe("ProductEditorScreen", () => {
  it("switches between Arabic and English content cleanly", async () => {
    const user = userEvent.setup();
    window.localStorage.removeItem("tara-product-editor-content-view");

    renderWithRouter(
      <ProductEditorScreen
        model={productEditorModelFixture}
        discovery={discoveryFixture}
        navigation={navigationFixture}
      />,
    );

    expect(screen.getByRole("textbox", { name: "English title" })).toHaveValue("Volumizing Shampoo");

    await waitFor(() => {
      expect(screen.getByLabelText("English description")).toHaveTextContent(
        "English description for testing.",
      );
    });

    await user.click(screen.getAllByRole("button", { name: "Arabic" })[0]);

    expect(screen.getByRole("textbox", { name: "Arabic title" })).toHaveValue("شامبو التكثيف");

    await waitFor(() => {
      expect(screen.getByLabelText("Arabic description")).toHaveTextContent("وصف عربي للاختبار.");
    });

    await user.click(screen.getByRole("button", { name: "English" }));

    expect(screen.getByRole("textbox", { name: "English title" })).toHaveValue("Volumizing Shampoo");

    await waitFor(() => {
      expect(screen.getByLabelText("English description")).toHaveTextContent(
        "English description for testing.",
      );
    });
  });

  it("keeps an Arabic gallery deletion after a rerender with the same loaded model data", async () => {
    const user = userEvent.setup();
    window.localStorage.setItem("tara-product-editor-content-view", "arabic");
    const modelWithImages = {
      ...productEditorModelFixture,
      imageGroups: [
        {
          id: "pdp-gallery",
          label: "PDP gallery",
          notes: [],
          mismatchWarning: undefined,
          english: [
            {
              target: {
                id: "custom.pdp_images",
                label: "English source",
                locale: "en",
                storage: "metafield",
                namespace: "custom",
                key: "pdp_images",
                type: "list.file_reference",
              },
              resourceId: "gid://shopify/Metafield/20",
              images: [
                {
                  id: "gid://shopify/MediaImage/1",
                  url: "https://example.com/en-one.jpg",
                  alt: "English one",
                  width: 1000,
                  height: 1000,
                  source: "files" as const,
                },
              ],
              rawValue: '["gid://shopify/MediaImage/1"]',
            },
          ],
          arabic: [
            {
              target: {
                id: "custom.pdp_images_ar",
                label: "Arabic metafield",
                locale: "ar",
                storage: "metafield",
                namespace: "custom",
                key: "pdp_images_ar",
                type: "list.file_reference",
              },
              resourceId: "gid://shopify/Metafield/21",
              images: [
                {
                  id: "gid://shopify/MediaImage/2",
                  url: "https://example.com/ar-one.jpg",
                  alt: "Arabic one",
                  width: 1000,
                  height: 1000,
                  source: "files" as const,
                },
                {
                  id: "gid://shopify/MediaImage/3",
                  url: "https://example.com/ar-two.jpg",
                  alt: "Arabic two",
                  width: 1000,
                  height: 1000,
                  source: "files" as const,
                },
              ],
              rawValue: '["gid://shopify/MediaImage/2","gid://shopify/MediaImage/3"]',
            },
          ],
          legacy: [],
        },
      ],
    };

    const { rerenderWithRouter } = renderWithRouter(
      <ProductEditorScreen
        model={modelWithImages}
        discovery={discoveryFixture}
        navigation={navigationFixture}
      />,
    );

    await waitFor(() => {
      expect(screen.getByText("2 images")).toBeInTheDocument();
    });

    await user.click(screen.getAllByRole("button", { name: "Remove" })[0]);
    expect(screen.getByText("1 images")).toBeInTheDocument();

    rerenderWithRouter(
      <ProductEditorScreen
        model={{
          ...modelWithImages,
          imageGroups: [...modelWithImages.imageGroups],
        }}
        discovery={discoveryFixture}
        navigation={navigationFixture}
      />,
    );

    expect(screen.getByText("1 images")).toBeInTheDocument();
    window.localStorage.removeItem("tara-product-editor-content-view");
  });
});
