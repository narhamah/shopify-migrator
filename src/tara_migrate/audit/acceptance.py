"""Acceptance gate — the single pass/fail check that a build is actually done.

Consumes the run manifest + local id_map and probes the live destination store.
A build is "complete" only when this gate is green. The orchestrator/CI can run
``python acceptance.py`` (exit 0 == pass) after a build, or ``pytest -m acceptance``
against a sandbox store.

Each check is a small pure-ish function returning a Check so the logic is unit
testable without a live store; ``run_acceptance`` wires them to a real client.
"""

from dataclasses import dataclass, field

from tara_migrate.core.logging import get_logger

logger = get_logger(__name__)


@dataclass
class Check:
    name: str
    passed: bool
    detail: str = ""


@dataclass
class AcceptanceResult:
    checks: list = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return all(c.passed for c in self.checks)

    def add(self, name, passed, detail=""):
        self.checks.append(Check(name, passed, detail))

    def summary(self) -> str:
        lines = [f"Acceptance: {'PASS' if self.ok else 'FAIL'} ({sum(c.passed for c in self.checks)}/{len(self.checks)} checks)"]
        for c in self.checks:
            lines.append(f"  [{'PASS' if c.passed else 'FAIL'}] {c.name}{': ' + c.detail if c.detail else ''}")
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# Pure check functions (unit-testable)
# ---------------------------------------------------------------------------

def check_manifest(manifest_data) -> Check:
    """The run manifest must report a completed build with no failed phases."""
    if not manifest_data:
        return Check("manifest_completed", False, "no run_manifest.json found")
    status = manifest_data.get("status")
    failed = [k for k, v in (manifest_data.get("phases") or {}).items() if v.get("status") == "failed"]
    if status == "completed" and not failed:
        return Check("manifest_completed", True, "status=completed")
    return Check("manifest_completed", False, f"status={status}, failed phases={failed}")


def check_id_map(id_map) -> Check:
    """id_map must have non-empty products and collections (core resources)."""
    if not id_map:
        return Check("id_map_populated", False, "id_map missing/empty")
    products = id_map.get("products") or {}
    collections = id_map.get("collections") or {}
    if products and collections:
        return Check("id_map_populated", True, f"{len(products)} products, {len(collections)} collections")
    return Check("id_map_populated", False,
                 f"products={len(products)}, collections={len(collections)} (expected both > 0)")


def check_products_have_images(products) -> Check:
    """Every destination product must have at least one image."""
    missing = [p.get("handle") or p.get("id") for p in products if not p.get("images")]
    if not products:
        return Check("products_have_images", False, "no destination products found")
    if not missing:
        return Check("products_have_images", True, f"all {len(products)} products have images")
    return Check("products_have_images", False,
                 f"{len(missing)} of {len(products)} missing images: {missing[:10]}")


def check_locales(locales, expected) -> Check:
    """Every expected locale must be enabled and published on the destination."""
    by_code = {l.get("locale"): l for l in (locales or [])}
    problems = []
    for code in expected:
        loc = by_code.get(code)
        if not loc:
            problems.append(f"{code}:missing")
        elif not loc.get("published"):
            problems.append(f"{code}:unpublished")
    if not problems:
        return Check("locales_registered", True, f"{expected} present & published")
    return Check("locales_registered", False, ", ".join(problems))


# ---------------------------------------------------------------------------
# Live runner
# ---------------------------------------------------------------------------

def run_acceptance(client, manifest_data, id_map, expected_locales=None) -> AcceptanceResult:
    """Run all acceptance checks against a live destination client + local artifacts."""
    result = AcceptanceResult()
    result.checks.append(check_manifest(manifest_data))
    result.checks.append(check_id_map(id_map))

    try:
        products = client.get_products()
        result.checks.append(check_products_have_images(products))
    except Exception as e:
        result.add("products_have_images", False, f"could not fetch products: {e}")

    if expected_locales:
        try:
            locales = client.get_locales()
            result.checks.append(check_locales(locales, expected_locales))
        except Exception as e:
            result.add("locales_registered", False, f"could not fetch locales: {e}")

    return result


def main():
    import os
    import sys

    from dotenv import load_dotenv

    from tara_migrate.client import ShopifyClient
    from tara_migrate.core import config, load_json

    load_dotenv()
    manifest_data = load_json(config.get_progress_file("run_manifest.json"), default={})
    id_map = load_json(config.get_id_map_file(), default={})
    expected = [l for l in (os.environ.get("EXPECTED_LOCALES", "").split(",")) if l]

    client = ShopifyClient(config.get_dest_shop_url(), config.get_dest_access_token())
    result = run_acceptance(client, manifest_data, id_map, expected_locales=expected or None)
    print(result.summary())
    sys.exit(0 if result.ok else 1)


if __name__ == "__main__":
    main()
