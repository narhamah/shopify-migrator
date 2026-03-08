"""
Get a Shopify Admin API access token via OAuth.

Usage:
    python get_token.py --shop xkgw0m-sm.myshopify.com --client-id YOUR_CLIENT_ID --client-secret YOUR_CLIENT_SECRET

This will:
1. Start a local server on port 3456
2. Open your browser to authorize the app
3. Catch the redirect and exchange the code for an access token
4. Print the access token for you to copy into .env
"""

import argparse
import hashlib
import hmac
import http.server
import json
import threading
import time
import urllib.parse
import webbrowser

import requests

SCOPES = "read_products,write_products,read_content,write_content,read_inventory,write_inventory,read_locales,write_locales,read_translations,write_translations,read_files,write_files,read_metaobject_definitions,write_metaobject_definitions,read_metaobjects,write_metaobjects"
REDIRECT_URI = "http://localhost:3456/callback"


class OAuthHandler(http.server.BaseHTTPRequestHandler):
    access_token = None
    client_id = None
    client_secret = None
    shop = None

    def log_message(self, format, *args):
        pass  # Suppress default logging

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path != "/callback":
            self.send_response(404)
            self.end_headers()
            return

        params = urllib.parse.parse_qs(parsed.query)
        code = params.get("code", [None])[0]

        if not code:
            self.send_response(400)
            self.end_headers()
            self.wfile.write(b"No code received. Please try again.")
            return

        # Exchange code for access token
        token_url = f"https://{self.shop}/admin/oauth/access_token"
        resp = requests.post(token_url, json={
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "code": code,
        })

        if resp.status_code == 200:
            data = resp.json()
            OAuthHandler.access_token = data.get("access_token")
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.end_headers()
            self.wfile.write(b"""
            <html><body style="font-family: sans-serif; text-align: center; padding: 50px;">
            <h1>Success!</h1>
            <p>Access token retrieved. You can close this tab and check your terminal.</p>
            </body></html>
            """)
        else:
            self.send_response(500)
            self.send_header("Content-Type", "text/html")
            self.end_headers()
            self.wfile.write(f"Error: {resp.status_code} {resp.text}".encode())


def main():
    parser = argparse.ArgumentParser(description="Get Shopify access token via OAuth")
    parser.add_argument("--shop", required=True, help="Store URL (e.g., xkgw0m-sm.myshopify.com)")
    parser.add_argument("--client-id", required=True, help="App Client ID from Dev Dashboard")
    parser.add_argument("--client-secret", required=True, help="App Client Secret from Dev Dashboard")
    parser.add_argument("--port", type=int, default=3456, help="Local server port (default: 3456)")
    args = parser.parse_args()

    shop = args.shop.replace("https://", "").rstrip("/")

    OAuthHandler.client_id = args.client_id
    OAuthHandler.client_secret = args.client_secret
    OAuthHandler.shop = shop

    redirect_uri = f"http://localhost:{args.port}/callback"

    server = http.server.HTTPServer(("localhost", args.port), OAuthHandler)
    thread = threading.Thread(target=server.serve_forever)
    thread.daemon = True
    thread.start()

    auth_url = (
        f"https://{shop}/admin/oauth/authorize"
        f"?client_id={args.client_id}"
        f"&scope={SCOPES}"
        f"&redirect_uri={urllib.parse.quote(redirect_uri)}"
    )

    print(f"\nOpening browser for authorization...")
    print(f"If it doesn't open, visit this URL:\n")
    print(f"  {auth_url}\n")
    webbrowser.open(auth_url)

    print("Waiting for authorization...")
    while OAuthHandler.access_token is None:
        time.sleep(0.5)

    print(f"\n{'='*60}")
    print(f"ACCESS TOKEN: {OAuthHandler.access_token}")
    print(f"{'='*60}")
    print(f"\nCopy this into your .env file as SAUDI_ACCESS_TOKEN")

    server.shutdown()


if __name__ == "__main__":
    main()
