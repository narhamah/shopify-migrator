"""
Get a Shopify Admin API access token via OAuth.

Usage:
    python get_token.py --shop xkgw0m-sm.myshopify.com --client-id YOUR_CLIENT_ID --client-secret YOUR_CLIENT_SECRET

This will:
1. Print an authorization URL for you to visit in your browser
2. You authorize the app, get redirected to a page that won't load
3. Copy the "code" parameter from the browser's address bar
4. Paste it back here — the script exchanges it for an access token
"""

import argparse
import urllib.parse

import requests

SCOPES = "read_products,write_products,read_content,write_content,read_inventory,write_inventory,read_locales,write_locales,read_translations,write_translations,read_files,write_files,read_metaobject_definitions,write_metaobject_definitions,read_metaobjects,write_metaobjects"
# This URL won't actually load — we just need it registered in the app settings
# so Shopify redirects there with the ?code= parameter visible in the address bar
REDIRECT_URI = "https://localhost/callback"


def main():
    parser = argparse.ArgumentParser(description="Get Shopify access token via OAuth")
    parser.add_argument("--shop", required=True, help="Store URL (e.g., xkgw0m-sm.myshopify.com)")
    parser.add_argument("--client-id", required=True, help="App Client ID from Dev Dashboard")
    parser.add_argument("--client-secret", required=True, help="App Client Secret from Dev Dashboard")
    args = parser.parse_args()

    shop = args.shop.replace("https://", "").rstrip("/")

    auth_url = (
        f"https://{shop}/admin/oauth/authorize"
        f"?client_id={args.client_id}"
        f"&scope={SCOPES}"
        f"&redirect_uri={urllib.parse.quote(REDIRECT_URI)}"
    )

    print(f"\n{'='*60}")
    print("STEP 1: Visit this URL in your browser and click 'Install':\n")
    print(f"  {auth_url}\n")
    print("STEP 2: After authorizing, you'll be redirected to a page")
    print("  that won't load. That's OK! Look at the address bar.")
    print("  It will look like:")
    print("  https://localhost/callback?code=XXXXXXXXXX&...")
    print(f"\nSTEP 3: Copy the 'code' value and paste it below.")
    print(f"{'='*60}\n")

    code = input("Paste the code here: ").strip()

    if not code:
        print("Error: No code provided.")
        return

    # Exchange code for access token
    print("\nExchanging code for access token...")
    token_url = f"https://{shop}/admin/oauth/access_token"
    resp = requests.post(token_url, json={
        "client_id": args.client_id,
        "client_secret": args.client_secret,
        "code": code,
    })

    if resp.status_code == 200:
        data = resp.json()
        token = data.get("access_token")
        print(f"\n{'='*60}")
        print(f"ACCESS TOKEN: {token}")
        print(f"{'='*60}")
        print(f"\nAdd this to your .env file:")
        print(f"  SAUDI_ACCESS_TOKEN={token}")
    else:
        print(f"\nError {resp.status_code}: {resp.text}")


if __name__ == "__main__":
    main()
