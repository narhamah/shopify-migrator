import { afterEach, describe, expect, it, vi } from "vitest";

import { adminGraphql, type AdminGraphqlClient } from "../app/lib/shopify-admin.server";

describe("adminGraphql", () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  it("retries throttled GraphQL responses and returns data once Shopify responds", async () => {
    vi.useFakeTimers();

    const admin: AdminGraphqlClient = {
      graphql: vi
        .fn()
        .mockResolvedValueOnce(
          buildResponse({
            status: 200,
            errors: [{ message: "Throttled", extensions: { code: "THROTTLED" } }],
          }),
        )
        .mockResolvedValueOnce(
          buildResponse({
            status: 200,
            data: { product: { id: "gid://shopify/Product/1" } },
          }),
        ),
    };

    const promise = adminGraphql<{ product: { id: string } }>(admin, "query Test { product { id } }");
    await vi.runAllTimersAsync();

    await expect(promise).resolves.toEqual({ product: { id: "gid://shopify/Product/1" } });
    expect(admin.graphql).toHaveBeenCalledTimes(2);
  });

  it("throws immediately for non-retryable GraphQL errors", async () => {
    const admin: AdminGraphqlClient = {
      graphql: vi.fn().mockResolvedValue(
        buildResponse({
          status: 200,
          errors: [{ message: "Access denied", extensions: { code: "ACCESS_DENIED" } }],
        }),
      ),
    };

    await expect(adminGraphql(admin, "query Test { shop { id } }")).rejects.toThrow("Access denied");
    expect(admin.graphql).toHaveBeenCalledTimes(1);
  });
});

function buildResponse(input: {
  status: number;
  data?: unknown;
  errors?: Array<{ message: string; extensions?: { code?: string; requestId?: string } }>;
}) {
  return {
    status: input.status,
    json: vi.fn().mockResolvedValue({
      data: input.data,
      errors: input.errors,
    }),
  } as unknown as Response;
}
