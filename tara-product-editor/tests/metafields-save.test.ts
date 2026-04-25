import { beforeEach, describe, expect, it, vi } from "vitest";

const { adminGraphqlMock } = vi.hoisted(() => ({
  adminGraphqlMock: vi.fn(),
}));

vi.mock("../app/lib/shopify-admin.server", () => ({
  adminGraphql: adminGraphqlMock,
}));

import { saveMetafields } from "../app/services/metafields.server";

describe("saveMetafields", () => {
  beforeEach(() => {
    adminGraphqlMock.mockReset();
  });

  it("sends only Shopify-supported MetafieldsSetInput fields", async () => {
    adminGraphqlMock.mockResolvedValue({
      metafieldsSet: {
        userErrors: [],
      },
    });

    await saveMetafields(
      { graphql: vi.fn() } as never,
      "gid://shopify/Product/1",
      [
        {
          id: "gid://shopify/Metafield/1",
          name: "PDP gallery",
          namespace: "custom",
          key: "pdp_images",
          type: "list.file_reference",
          value: '["gid://shopify/MediaImage/1"]',
          compareDigest: "digest-1",
        },
      ],
      "images",
    );

    expect(adminGraphqlMock).toHaveBeenCalledTimes(1);
    expect(adminGraphqlMock).toHaveBeenCalledWith(
      expect.anything(),
      expect.any(String),
      {
        metafields: [
          {
            ownerId: "gid://shopify/Product/1",
            namespace: "custom",
            key: "pdp_images",
            type: "list.file_reference",
            value: '["gid://shopify/MediaImage/1"]',
            compareDigest: "digest-1",
          },
        ],
      },
    );
  });
});
