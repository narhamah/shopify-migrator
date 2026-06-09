"""Tests for Phase 2 feature modules: markets, shipping, flows, klaviyo."""
from unittest.mock import MagicMock

from tara_migrate.pipeline.migrate_flows import rebuild_patterns
from tara_migrate.pipeline.migrate_shipping import (
    _extract_zones,
    build_profile_input,
    migrate_shipping,
)
from tara_migrate.setup.setup_markets import setup_market
from tara_migrate.tools.setup_klaviyo import KlaviyoClient, ensure_lists

# --- setup_markets ---

class TestSetupMarkets:
    def test_creates_market_and_web_presence(self):
        client = MagicMock()
        client.get_markets.return_value = []
        client.create_market.return_value = {"id": "gid://m/1", "name": "Kuwait", "webPresence": None}
        setup_market(client, "Kuwait", ["KW"], currency="KWD",
                     locales=["en", "ar"], default_locale="en", subfolder_suffix="kw")
        client.create_market.assert_called_once_with("Kuwait", ["KW"])
        client.market_update_currency.assert_called_once_with("gid://m/1", "KWD")
        client.market_create_web_presence.assert_called_once()
        _, kwargs = client.market_create_web_presence.call_args
        assert kwargs["subfolder_suffix"] == "kw"
        assert kwargs["alternate_locales"] == ["ar"]

    def test_reuses_existing_market(self):
        client = MagicMock()
        client.get_markets.return_value = [{"id": "gid://m/9", "name": "Kuwait", "webPresence": {"id": "wp"}}]
        setup_market(client, "Kuwait", ["KW"], currency="KWD")
        client.create_market.assert_not_called()
        client.market_create_web_presence.assert_not_called()  # already has one

    def test_dry_run_creates_nothing(self):
        client = MagicMock()
        client.get_markets.return_value = []
        setup_market(client, "Kuwait", ["KW"], dry_run=True)
        client.create_market.assert_not_called()


# --- migrate_shipping ---

class TestMigrateShipping:
    def _source_profile(self):
        return {
            "default": True,
            "profileLocationGroups": [{
                "locationGroup": {"id": "gid://lg/1"},
                "locationGroupZones": {"edges": [{"node": {
                    "zone": {"name": "Gulf", "countries": [{"code": {"countryCode": "KW"}}]},
                    "methodDefinitions": {"edges": [{"node": {
                        "name": "Standard",
                        "rateProvider": {"__typename": "DeliveryRateDefinition",
                                          "price": {"amount": "1.50", "currencyCode": "KWD"}},
                    }}]},
                }}]},
            }],
        }

    def test_extract_zones(self):
        zones = _extract_zones(self._source_profile())
        assert zones[0]["countries"] == ["KW"]
        assert zones[0]["rates"][0]["amount"] == "1.50"

    def test_currency_override(self):
        zones = _extract_zones(self._source_profile(), currency_override="USD")
        assert zones[0]["rates"][0]["currency"] == "USD"

    def test_build_profile_input(self):
        zones = [{"name": "Gulf", "countries": ["KW"], "rates": [{"name": "Std", "amount": "1.5", "currency": "KWD"}]}]
        inp = build_profile_input("gid://lg/1", zones)
        group = inp["locationGroupsToUpdate"][0]
        assert group["id"] == "gid://lg/1"
        zone = group["zonesToCreate"][0]
        assert zone["countries"] == [{"code": "KW"}]
        assert zone["methodDefinitionsToCreate"][0]["rateDefinition"]["price"]["amount"] == "1.5"

    def test_migrate_applies_to_dest_default(self):
        source = MagicMock()
        source.get_delivery_profiles.return_value = [self._source_profile()]
        dest = MagicMock()
        dest.get_delivery_profiles.return_value = [{
            "id": "gid://dp/dest", "default": True,
            "profileLocationGroups": [{"locationGroup": {"id": "gid://lg/dest"},
                                       "locationGroupZones": {"edges": []}}],
        }]
        migrate_shipping(source, dest)
        dest.update_delivery_profile.assert_called_once()
        args = dest.update_delivery_profile.call_args[0]
        assert args[0] == "gid://dp/dest"

    def test_dry_run_no_write(self):
        source = MagicMock()
        source.get_delivery_profiles.return_value = [self._source_profile()]
        dest = MagicMock()
        dest.get_delivery_profiles.return_value = [{
            "id": "gid://dp/dest", "default": True,
            "profileLocationGroups": [{"locationGroup": {"id": "gid://lg/dest"}, "locationGroupZones": {"edges": []}}],
        }]
        migrate_shipping(source, dest, dry_run=True)
        dest.update_delivery_profile.assert_not_called()


# --- migrate_flows ---

class TestMigrateFlows:
    def test_creates_and_publishes_smart_collection(self):
        client = MagicMock()
        client.get_collections.return_value = []
        client.get_publications.return_value = [{"id": "gid://pub/1"}]
        client.create_smart_collection.return_value = {"id": 555}
        patterns = [{"type": "smart_collection", "title": "Shampoos", "handle": "shampoos",
                     "rules": [{"column": "type", "relation": "equals", "condition": "Shampoo"}],
                     "publish": True}]
        created, published, skipped = rebuild_patterns(client, patterns)
        assert created == 1 and published == 1 and skipped == []
        client.publish_resource.assert_called_once()
        assert "gid://shopify/Collection/555" in client.publish_resource.call_args[0]

    def test_skips_unsupported_pattern(self):
        client = MagicMock()
        client.get_collections.return_value = []
        client.get_publications.return_value = []
        created, published, skipped = rebuild_patterns(
            client, [{"type": "email_flow", "title": "Welcome"}])
        assert created == 0 and len(skipped) == 1

    def test_dry_run(self):
        client = MagicMock()
        client.get_collections.return_value = []
        rebuild_patterns(client, [{"type": "smart_collection", "title": "X", "handle": "x", "rules": []}],
                         dry_run=True)
        client.create_smart_collection.assert_not_called()


# --- setup_klaviyo ---

class TestSetupKlaviyo:
    def test_ensure_lists_creates_missing(self):
        session = MagicMock()
        client = KlaviyoClient("pk_x", session=session)
        client.get_lists = MagicMock(return_value=[{"attributes": {"name": "Newsletter"}}])
        client.create_list = MagicMock(return_value={"id": "l1"})
        created = ensure_lists(client, ["Newsletter", "Back in stock"])
        assert created == 1
        client.create_list.assert_called_once_with("Back in stock")

    def test_dry_run(self):
        client = MagicMock()
        client.get_lists.return_value = []
        created = ensure_lists(client, ["Newsletter"], dry_run=True)
        assert created == 0
        client.create_list.assert_not_called()
