"""Guided manual-steps engine.

Replaces the old blind ``print("Remaining MANUAL steps: ...")`` block with a
machine-readable, deep-linked, *verified-where-possible* checklist. Each step
carries a state (DONE / PENDING / UNKNOWN), an admin deep link, and a note.
Read-only probes confirm what can be confirmed (payments configured, domain SSL
live); the irreducible platform-gated steps (VAT self-cert, payment KYC,
registrar DNS, password toggle, 3rd-party app OAuth) become one confirmed click
with the exact link, not a vague reminder.

The result is saved to ``data/{dest}/manual_steps.json`` so the orchestrator can
fold it into the run manifest and the operator gets a precise punch-list.
"""

from tara_migrate.core import config, save_json
from tara_migrate.core.logging import get_logger

logger = get_logger(__name__)

DONE = "DONE"
PENDING = "PENDING"
UNKNOWN = "UNKNOWN"


def admin_store_handle(shop_url: str) -> str:
    """'977mp2-qa.myshopify.com' (or https://...) -> '977mp2-qa'."""
    host = shop_url.replace("https://", "").replace("http://", "").rstrip("/")
    return host.split(".myshopify.com")[0]


def admin_deep_link(shop_url: str, path: str) -> str:
    """Build a Shopify admin deep link for a settings/section path."""
    handle = admin_store_handle(shop_url)
    return f"https://admin.shopify.com/store/{handle}/{path.lstrip('/')}"


def _safe(probe, default=UNKNOWN):
    try:
        return probe()
    except Exception as e:  # never let a probe crash the checklist
        logger.warning("Manual-step probe failed: %s", e)
        return default


def build_manual_steps(client, shop_url, expected_locales=None):
    """Build the guided manual-step checklist with live read-only probes."""
    steps = []

    # --- Payments (KYC + merchant Activate; verify via REST) ---
    def _payments_state():
        gateways = client.get_payment_gateways()
        return DONE if gateways else PENDING
    steps.append({
        "key": "payments",
        "title": "Activate payment gateways (Tap, Mada, Apple Pay)",
        "state": _safe(_payments_state),
        "deep_link": admin_deep_link(shop_url, "settings/payments"),
        "note": "Install the provider app + complete KYC; cannot be automated (PCI/merchant consent).",
    })

    # --- Domain / SSL (connect is UI-only; verify SSL live) ---
    def _domain_state():
        domains = client.get_domains()
        custom = [d for d in domains if not d.get("host", "").endswith(".myshopify.com")]
        if custom and all(d.get("sslEnabled") for d in custom):
            return DONE
        return PENDING
    steps.append({
        "key": "domain",
        "title": "Connect custom domain + DNS (or use subfolder market)",
        "state": _safe(_domain_state),
        "deep_link": admin_deep_link(shop_url, "settings/domains"),
        "note": "Subfolder market needs no DNS. Otherwise point A/CNAME at Shopify, then verify SSL.",
    })

    # --- VAT / taxes (legal self-cert; UI-only) ---
    steps.append({
        "key": "vat",
        "title": "Enable VAT / tax registration (e.g. 15% KSA, 5% GCC)",
        "state": PENDING,
        "deep_link": admin_deep_link(shop_url, "settings/taxes"),
        "note": "Self-certification of tax registration; verify after with a draft-order tax probe.",
    })

    # --- Notifications (bodies UI-only; translations automated separately) ---
    steps.append({
        "key": "notifications",
        "title": "Review email/SMS notification templates",
        "state": PENDING,
        "deep_link": admin_deep_link(shop_url, "settings/notifications"),
        "note": "Arabic translations are migrated automatically; body edits are UI-only.",
    })

    # --- Third-party apps (merchant OAuth) ---
    steps.append({
        "key": "apps",
        "title": "Install third-party apps (Klaviyo, reviews, loyalty)",
        "state": PENDING,
        "deep_link": "https://apps.shopify.com/",
        "note": "OAuth install is merchant-only; Klaviyo config auto-runs after install (setup_klaviyo.py).",
    })

    # --- Store password / go-live ---
    steps.append({
        "key": "password",
        "title": "Remove storefront password at go-live",
        "state": PENDING,
        "deep_link": admin_deep_link(shop_url, "online_store/preferences"),
        "note": "Password toggle is read-only via API; flip it manually when ready to launch.",
    })

    return steps


def render_manual_steps(steps):
    lines = ["\n=== Guided manual steps (deep-linked + verified) ==="]
    for i, s in enumerate(steps, 1):
        lines.append(f"  {i}. [{s['state']}] {s['title']}")
        lines.append(f"       link: {s['deep_link']}")
        if s.get("note"):
            lines.append(f"       note: {s['note']}")
    pending = [s for s in steps if s["state"] != DONE]
    lines.append(f"\n  {len(pending)} step(s) still need attention; "
                 f"{len(steps) - len(pending)} verified done.")
    return "\n".join(lines)


def write_manual_steps(client, shop_url, expected_locales=None):
    """Build, persist, and render the checklist. Returns the steps list."""
    steps = build_manual_steps(client, shop_url, expected_locales=expected_locales)
    save_json(steps, config.get_progress_file("manual_steps.json"))
    print(render_manual_steps(steps))
    return steps
