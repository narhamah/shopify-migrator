import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it } from "vitest";

import { MetafieldReferencePicker } from "../app/components/MetafieldReferencePicker";

import { productEditorModelFixture } from "./fixtures/editor";
import { renderWithRouter } from "./utils/render-with-router";

describe("MetafieldReferencePicker", () => {
  it("uses the new card interaction for selected metaobjects", async () => {
    const user = userEvent.setup();
    const metafield = productEditorModelFixture.metafields[0];

    renderWithRouter(
      <MetafieldReferencePicker
        metafield={metafield}
        metaobjectDefinitions={productEditorModelFixture.metaobjectDefinitions}
        contentView="english"
        onChange={() => {}}
      />,
    );

    expect(screen.queryByText("METAOBJECT")).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Edit details" })).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Move earlier" })).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Move later" })).not.toBeInTheDocument();
    expect(screen.getByLabelText("Remove Black Garlic")).toBeInTheDocument();

    const expandButton = screen.getByText("Black Garlic").closest("button");
    expect(expandButton).not.toBeNull();

    await user.click(expandButton!);

    expect(screen.getByRole("button", { name: "Save metaobject" })).toBeInTheDocument();
    expect(screen.getByText("Edit English source")).toBeInTheDocument();
  });
});
