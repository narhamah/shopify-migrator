"""Shared GraphQL query templates for Shopify translation operations.

Used by fix_translations, audit_translations, upload_translations,
and translate_csv modules.
"""

import time


# ─────────────────────────────────────────────────────────────────────────────
# Query templates
# ─────────────────────────────────────────────────────────────────────────────

FETCH_DIGESTS_QUERY = """
query($resourceIds: [ID!]!, $first: Int!) {
  translatableResourcesByIds(resourceIds: $resourceIds, first: $first) {
    edges {
      node {
        resourceId
        translatableContent {
          key
          value
          digest
          locale
        }
        translations(locale: "%LOCALE%") {
          key
          value
          outdated
        }
      }
    }
  }
}
"""

REGISTER_TRANSLATIONS_MUTATION = """
mutation translationsRegister($resourceId: ID!, $translations: [TranslationInput!]!) {
  translationsRegister(resourceId: $resourceId, translations: $translations) {
    userErrors {
      message
      field
    }
    translations {
      key
      value
    }
  }
}
"""

FETCH_METAOBJECTS_QUERY = """
query($type: String!, $first: Int!, $after: String) {
  metaobjects(type: $type, first: $first, after: $after) {
    edges {
      node {
        id
        handle
        fields {
          key
          value
          type
        }
      }
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
"""

FETCH_PRODUCTS_QUERY = """
query($first: Int!, $after: String) {
  products(first: $first, after: $after) {
    edges {
      node {
        id
        title
        metafields(first: 30) {
          edges {
            node {
              id
              namespace
              key
              value
              type
            }
          }
        }
      }
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
"""

FETCH_THEME_DIGESTS_QUERY = """
query($resourceId: ID!) {
  translatableResource(resourceId: $resourceId) {
    resourceId
    translatableContent {
      key
      value
      digest
      locale
    }
  }
}
"""

TRANSLATABLE_RESOURCES_QUERY = """
query($resourceType: TranslatableResourceType!, $first: Int!, $after: String) {
  translatableResources(resourceType: $resourceType, first: $first, after: $after) {
    edges {
      node {
        resourceId
        translatableContent {
          key
          value
          digest
          locale
        }
        translations(locale: "%LOCALE%") {
          key
          value
          outdated
        }
      }
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
"""


# ─────────────────────────────────────────────────────────────────────────────
# Shared helpers
# ─────────────────────────────────────────────────────────────────────────────

def fetch_translatable_resources(client, gids, locale):
    """Fetch digest map for a list of resource GIDs.

    Returns: {gid: {"content": {key: {"digest": ..., "value": ...}},
                     "translations": {key: {"value": ..., "outdated": ...}}}}
    """
    query = FETCH_DIGESTS_QUERY.replace("%LOCALE%", locale)
    digest_map = {}
    for i in range(0, len(gids), 10):
        batch = gids[i:i + 10]
        for attempt in range(4):
            try:
                data = client._graphql(query, {
                    "resourceIds": batch,
                    "first": len(batch),
                })
                edges = data.get("translatableResourcesByIds", {}).get("edges", [])
                for edge in edges:
                    node = edge["node"]
                    rid = node["resourceId"]
                    digest_map[rid] = {
                        "content": {
                            tc["key"]: {"digest": tc["digest"], "value": tc["value"]}
                            for tc in node["translatableContent"]
                        },
                        "translations": {
                            t["key"]: {"value": t["value"], "outdated": t["outdated"]}
                            for t in node["translations"]
                        },
                    }
                break  # success
            except Exception as e:
                if attempt < 3:
                    wait = 2 ** (attempt + 1)
                    print(f"  Connection error (attempt {attempt + 1}/4), "
                          f"retrying in {wait}s: {e}")
                    time.sleep(wait)
                else:
                    print(f"  Error fetching digests for batch after 4 attempts: {e}")
        time.sleep(0.3)
    return digest_map


def upload_translations(client, gid, translations_input):
    """Upload translations one key at a time to isolate errors.

    Returns (uploaded_count, error_count).
    Aborts early if Shopify returns a resource-level limit error
    ("Too many translation keys") to avoid wasting API calls.
    """
    total_uploaded = 0
    total_errors = 0

    for t in translations_input:
        for attempt in range(4):
            try:
                result = client._graphql(REGISTER_TRANSLATIONS_MUTATION, {
                    "resourceId": gid,
                    "translations": [t],
                })
                user_errors = result.get("translationsRegister", {}).get("userErrors", [])
                if user_errors:
                    for ue in user_errors:
                        msg = ue["message"]
                        print(f"    ERROR {gid}: {ue['field']}: {msg}")
                        # Abort early on resource-level limits — every subsequent
                        # call will fail with the same error
                        if "Too many translation keys" in msg:
                            remaining = len(translations_input) - total_uploaded - total_errors - 1
                            if remaining > 0:
                                print(f"    Aborting {remaining} remaining fields for {gid} "
                                      f"(theme key limit ~3,400 exceeded)")
                            total_errors += remaining + 1
                            return total_uploaded, total_errors
                    total_errors += 1
                else:
                    total_uploaded += 1
                break  # success (even if user_errors, we handled it)
            except Exception as e:
                if attempt < 3:
                    wait = 2 ** (attempt + 1)
                    time.sleep(wait)
                else:
                    print(f"    ERROR uploading {gid} [{t.get('key', '?')}]: {e}")
                    total_errors += 1

    return total_uploaded, total_errors


def paginate_query(client, query, result_key, variables=None, page_size=50):
    """Generic paginated GraphQL query. Yields nodes."""
    cursor = None
    variables = dict(variables or {})
    variables["first"] = page_size
    while True:
        if cursor:
            variables["after"] = cursor
        elif "after" in variables:
            del variables["after"]
        try:
            data = client._graphql(query, variables)
        except Exception as e:
            print(f"  Error fetching {result_key}: {e}")
            break
        container = data.get(result_key, {})
        edges = container.get("edges", [])
        for edge in edges:
            yield edge["node"]
        page_info = container.get("pageInfo", {})
        if page_info.get("hasNextPage"):
            cursor = page_info["endCursor"]
        else:
            break
        time.sleep(0.3)
