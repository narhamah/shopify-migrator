import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";

import { ImageTargetEditor } from "../app/components/ImageTargetEditor";

import { renderWithRouter } from "./utils/render-with-router";

describe("ImageTargetEditor", () => {
  it("removes an image from the gallery when trash is clicked", async () => {
    const user = userEvent.setup();
    const handleChange = vi.fn();

    renderWithRouter(
      <ImageTargetEditor
        title="PDP gallery"
        tone="success"
        images={[
          {
            id: "gid://shopify/MediaImage/1",
            url: "https://example.com/one.jpg",
            alt: "One",
            width: 1000,
            height: 1000,
            source: "files",
          },
          {
            id: "gid://shopify/MediaImage/2",
            url: "https://example.com/two.jpg",
            alt: "Two",
            width: 1000,
            height: 1000,
            source: "files",
          },
        ]}
        availableImages={[]}
        galleryQuery=""
        hasMoreFiles={false}
        onGalleryQueryChange={() => {}}
        onSearchGallery={() => {}}
        onLoadMoreFiles={() => {}}
        onUploadFile={() => {}}
        onChange={handleChange}
      />,
    );

    const removeButtons = screen.getAllByRole("button", { name: "Remove" });
    await user.click(removeButtons[0]);

    expect(handleChange).toHaveBeenCalledTimes(1);
    expect(handleChange).toHaveBeenCalledWith([
      expect.objectContaining({ id: "gid://shopify/MediaImage/2" }),
    ]);
  });
});
