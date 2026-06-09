#!/usr/bin/env python3
"""Run a tara_migrate module against the UAE store with a freshly-minted token.

Loads uae-destination.env + .env, mints a live admin token via the migration
app's client_credentials grant (cached in data/.uae_cc_token, ~24h), injects it
as DEST_ACCESS_TOKEN, sets DEST_NAME=uae, then runs the target module's main().

Usage:
    python scripts/run_uae.py <module> [args...]
    python scripts/run_uae.py tara_migrate.tools.review_arabic --audit --type PRODUCT
"""
import importlib
import os
import sys
import time

import requests
from dotenv import load_dotenv

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))
load_dotenv(os.path.join(ROOT, "uae-destination.env"))
if os.path.exists(os.path.join(ROOT, ".env")):
    load_dotenv(os.path.join(ROOT, ".env"))

# Migration app (UAE) — client_credentials grant. Secret comes from env.
MIG_CLIENT_ID = os.environ.get("UAE_MIGRATION_CLIENT_ID", "33ea409bca8725946fccff70862fdb27")
MIG_SECRET = os.environ.get("UAE_MIGRATION_CLIENT_SECRET", "")
SHOP = os.environ.get("DEST_SHOP_URL", "rvgkkk-g3.myshopify.com")
CACHE = os.path.join(ROOT, "data", ".uae_cc_token")


def mint_token():
    # reuse cached token if < 23h old
    if os.path.exists(CACHE) and (time.time() - os.path.getmtime(CACHE)) < 23 * 3600:
        tok = open(CACHE).read().strip()
        if tok:
            return tok
    if not MIG_SECRET:
        # fall back to whatever DEST_ACCESS_TOKEN is already set (may be cached file)
        if os.path.exists(CACHE):
            return open(CACHE).read().strip()
        raise SystemExit("Set UAE_MIGRATION_CLIENT_SECRET to mint a token (or populate data/.uae_cc_token).")
    r = requests.post(
        f"https://{SHOP}/admin/oauth/access_token",
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        data={"grant_type": "client_credentials", "client_id": MIG_CLIENT_ID, "client_secret": MIG_SECRET},
        timeout=30,
    )
    r.raise_for_status()
    tok = r.json()["access_token"]
    os.makedirs(os.path.dirname(CACHE), exist_ok=True)
    open(CACHE, "w").write(tok)
    return tok


def main():
    if len(sys.argv) < 2:
        raise SystemExit(__doc__)
    os.environ["DEST_ACCESS_TOKEN"] = mint_token()
    os.environ.setdefault("DEST_NAME", "uae")
    module = sys.argv[1]
    sys.argv = [module] + sys.argv[2:]
    importlib.import_module(module).main()


if __name__ == "__main__":
    main()
