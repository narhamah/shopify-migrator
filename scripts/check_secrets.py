#!/usr/bin/env python3
"""Reject committed secrets. Takes filenames as args; exits non-zero on a hit.

Catches Shopify Admin tokens (shpat_/shpca_/shppa_), OpenAI/Anthropic keys
(sk-..., sk-ant-...), and generic private keys. Used by pre-commit and CI.
Files that only *reference* an env var (e.g. access_token_env = "DEST_...") are
fine — we match the actual token value shapes, not the variable names.
"""

import re
import sys

PATTERNS = [
    re.compile(r"shp(at|ca|pa|ss)_[A-Za-z0-9]{16,}"),   # Shopify access/custom/partner tokens
    re.compile(r"sk-ant-[A-Za-z0-9_\-]{20,}"),           # Anthropic
    re.compile(r"sk-[A-Za-z0-9]{20,}"),                  # OpenAI
    re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----"),   # PEM private keys
]

# This file itself contains the patterns as regex literals; skip it.
SKIP_FILES = {"scripts/check_secrets.py", "scripts\\check_secrets.py"}

# Substrings that mark a value as an obvious placeholder, not a real secret.
_PLACEHOLDER_MARKERS = ("xxxx", "your", "example", "placeholder", "changeme",
                        "redacted", "dummy", "<", "...")


def _looks_placeholder(token):
    low = token.lower()
    if any(marker in low for marker in _PLACEHOLDER_MARKERS):
        return True
    # A long run of one repeated character (e.g. shpat_aaaaaaaa...) is a placeholder.
    if re.search(r"(.)\1{7,}", token):
        return True
    return False


def scan(paths):
    hits = []
    for path in paths:
        if path in SKIP_FILES:
            continue
        try:
            with open(path, "r", encoding="utf-8", errors="ignore") as f:
                for lineno, line in enumerate(f, 1):
                    for pat in PATTERNS:
                        m = pat.search(line)
                        if m and not _looks_placeholder(m.group(0)):
                            hits.append((path, lineno, pat.pattern))
                            break
        except (OSError, IsADirectoryError):
            continue
    return hits


def main(argv):
    hits = scan(argv)
    if hits:
        print("Potential secrets detected — do NOT commit:")
        for path, lineno, pat in hits:
            print(f"  {path}:{lineno} matches /{pat}/")
        print("Rotate the exposed credential and store it in an env var / secret manager.")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
