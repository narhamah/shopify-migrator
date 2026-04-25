export interface AdminGraphqlClient {
  graphql: (
    query: string,
    options?: {
      variables?: Record<string, unknown>;
    },
  ) => Promise<Response>;
}

const MAX_GRAPHQL_ATTEMPTS = 4;
const BASE_RETRY_DELAY_MS = 350;

export async function adminGraphql<TData>(
  admin: AdminGraphqlClient,
  query: string,
  variables?: Record<string, unknown>,
) {
  for (let attempt = 1; attempt <= MAX_GRAPHQL_ATTEMPTS; attempt += 1) {
    const response = await admin.graphql(query, { variables });
    const json = (await response.json()) as {
      data?: TData;
      errors?: Array<{
        message: string;
        extensions?: {
          code?: string;
          requestId?: string;
        };
      }>;
    };

    if (!shouldRetryGraphqlRequest(response.status, json.errors, attempt)) {
      if (json.errors?.length) {
        throw new Error(formatGraphqlErrors(json.errors, response.status));
      }

      if (!json.data) {
        throw new Error("Shopify Admin GraphQL returned no data.");
      }

      return json.data;
    }

    await waitForRetry(attempt);
  }

  throw new Error("Shopify Admin GraphQL request failed after retries.");
}

function shouldRetryGraphqlRequest(
  status: number,
  errors: Array<{ extensions?: { code?: string } }> | undefined,
  attempt: number,
) {
  if (attempt >= MAX_GRAPHQL_ATTEMPTS) {
    return false;
  }

  if (status === 429 || status >= 500) {
    return true;
  }

  return (errors || []).some((error) => {
    const code = error.extensions?.code;
    return code === "THROTTLED" || code === "INTERNAL_SERVER_ERROR";
  });
}

function formatGraphqlErrors(
  errors: Array<{
    message: string;
    extensions?: {
      code?: string;
      requestId?: string;
    };
  }>,
  status: number,
) {
  return errors
    .map((error) => {
      const details = [error.extensions?.code, error.extensions?.requestId].filter(Boolean).join(" | ");
      return details ? `${error.message} (${details}, HTTP ${status})` : `${error.message} (HTTP ${status})`;
    })
    .join("; ");
}

function waitForRetry(attempt: number) {
  const delay = BASE_RETRY_DELAY_MS * 2 ** (attempt - 1);
  return new Promise((resolve) => setTimeout(resolve, delay));
}
