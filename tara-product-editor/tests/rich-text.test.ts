import { describe, expect, it } from "vitest";

import { extractRichTextPlainText, htmlToEditableText } from "../app/lib/rich-text";

describe("rich-text HTML cleanup", () => {
  it("strips page-builder CSS noise from editable HTML text", () => {
    const value =
      '#html-body [data-pb-style=YWGLYJM]{justify-content:flex-start;display:flex;flex-direction:column;background-position:left top;background-size:cover;background-repeat:no-repeat;background-attachment:scroll} ابدأ رحلتك نحو شعر شبابي';

    expect(htmlToEditableText(value)).toBe("ابدأ رحلتك نحو شعر شبابي");
  });

  it("strips page-builder CSS noise from plain-text extraction", () => {
    const value =
      '#html-body [data-pb-style=YWGLYJM]{justify-content:flex-start;display:flex} <p>ابدأ رحلتك نحو شعر شبابي</p>';

    expect(extractRichTextPlainText(value)).toBe("ابدأ رحلتك نحو شعر شبابي");
  });
});
