"""Tests for the guided manual-steps engine."""
from unittest.mock import MagicMock

from tara_migrate.pipeline.manual_steps import (
    DONE,
    PENDING,
    admin_deep_link,
    admin_store_handle,
    build_manual_steps,
    render_manual_steps,
)


def test_admin_store_handle():
    assert admin_store_handle("977mp2-qa.myshopify.com") == "977mp2-qa"
    assert admin_store_handle("https://977mp2-qa.myshopify.com/") == "977mp2-qa"


def test_admin_deep_link():
    link = admin_deep_link("977mp2-qa.myshopify.com", "settings/taxes")
    assert link == "https://admin.shopify.com/store/977mp2-qa/settings/taxes"


def test_payments_done_when_gateway_present():
    client = MagicMock()
    client.get_payment_gateways.return_value = [{"id": 1, "name": "Tap"}]
    client.get_domains.return_value = [{"host": "x.myshopify.com", "sslEnabled": True}]
    steps = build_manual_steps(client, "977mp2-qa.myshopify.com")
    payments = next(s for s in steps if s["key"] == "payments")
    assert payments["state"] == DONE


def test_payments_pending_when_no_gateway():
    client = MagicMock()
    client.get_payment_gateways.return_value = []
    client.get_domains.return_value = []
    steps = build_manual_steps(client, "977mp2-qa.myshopify.com")
    assert next(s for s in steps if s["key"] == "payments")["state"] == PENDING


def test_domain_done_when_custom_ssl_live():
    client = MagicMock()
    client.get_payment_gateways.return_value = []
    client.get_domains.return_value = [{"host": "kw.tara.com", "sslEnabled": True}]
    steps = build_manual_steps(client, "977mp2-qa.myshopify.com")
    assert next(s for s in steps if s["key"] == "domain")["state"] == DONE


def test_probe_failure_is_unknown_not_crash():
    client = MagicMock()
    client.get_payment_gateways.side_effect = Exception("boom")
    client.get_domains.side_effect = Exception("boom")
    steps = build_manual_steps(client, "977mp2-qa.myshopify.com")
    assert next(s for s in steps if s["key"] == "payments")["state"] == "UNKNOWN"


def test_render_lists_all_steps():
    client = MagicMock()
    client.get_payment_gateways.return_value = []
    client.get_domains.return_value = []
    text = render_manual_steps(build_manual_steps(client, "x.myshopify.com"))
    assert "Guided manual steps" in text
    assert "settings/payments" in text
