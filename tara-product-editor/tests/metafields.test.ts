import { describe, expect, it } from "vitest";

import { isTextLikeTranslatableMetafieldType } from "../app/lib/metafields";

describe("isTextLikeTranslatableMetafieldType", () => {
  it("accepts text-like metafields and rejects reference/image metafields", () => {
    expect(isTextLikeTranslatableMetafieldType("single_line_text_field")).toBe(true);
    expect(isTextLikeTranslatableMetafieldType("multi_line_text_field")).toBe(true);
    expect(isTextLikeTranslatableMetafieldType("rich_text_field")).toBe(true);
    expect(isTextLikeTranslatableMetafieldType("string")).toBe(true);

    expect(isTextLikeTranslatableMetafieldType("list.file_reference")).toBe(false);
    expect(isTextLikeTranslatableMetafieldType("file_reference")).toBe(false);
    expect(isTextLikeTranslatableMetafieldType("metaobject_reference")).toBe(false);
    expect(isTextLikeTranslatableMetafieldType("list.metaobject_reference")).toBe(false);
  });
});
