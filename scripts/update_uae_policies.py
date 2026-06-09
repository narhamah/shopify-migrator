"""Upload prepared bilingual UAE shop policies to Shopify.

Expected env:
  DEST_SHOP_URL=rvgkkk-g3.myshopify.com
  DEST_ACCESS_TOKEN=<valid UAE Admin API token>

Payload:
  data/uae/policies_bilingual_gpt41.json
"""

from __future__ import annotations

import json
import os
from pathlib import Path

from tara_migrate.client.shopify_client import ShopifyClient

PAYLOAD_PATH = Path("data/uae/policies_bilingual_gpt41.json")


def load_dotenv(path: Path, *, override: bool = False) -> None:
    if not path.exists():
        return

    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        name, value = line.split("=", 1)
        key = name.strip()
        if override or key not in os.environ:
            os.environ[key] = value.strip()


def update_policy(client: ShopifyClient, policy_type: str, body: str) -> None:
    mutation = """
    mutation UpdateShopPolicy($shopPolicy: ShopPolicyInput!) {
      shopPolicyUpdate(shopPolicy: $shopPolicy) {
        shopPolicy {
          id
          type
          url
        }
        userErrors {
          field
          message
        }
      }
    }
    """
    data = client._graphql(
        mutation,
        {
            "shopPolicy": {
                "type": policy_type,
                "body": body,
            }
        },
    )
    result = data["shopPolicyUpdate"]
    if result["userErrors"]:
        raise RuntimeError(f"{policy_type}: {result['userErrors']}")

    policy = result["shopPolicy"]
    print(f"updated {policy_type}: {policy.get('url')}")


def main() -> None:
    load_dotenv(Path(".env"))
    load_dotenv(Path("uae-destination.env"), override=True)
    load_dotenv(Path(r"C:\Users\narha\tara-uae-shopify\.env.migration.local"), override=True)

    shop_url = os.environ.get("DEST_SHOP_URL")
    token = os.environ.get("DEST_ACCESS_TOKEN")
    if not shop_url or not token or token.startswith("<"):
        raise RuntimeError("Set DEST_SHOP_URL and a valid DEST_ACCESS_TOKEN for UAE before running.")

    payload = json.loads(PAYLOAD_PATH.read_text(encoding="utf-8"))
    client = ShopifyClient(shop_url, token)

    for policy in payload["policies"]:
        update_policy(client, policy["type"], policy["body_html"])


if __name__ == "__main__":
    main()
