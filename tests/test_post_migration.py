"""Tests for post_migration.py."""
import json
import os
from unittest.mock import MagicMock, patch, call

import pytest

from post_migration import (
    step_enable_arabic,
    step_link_products_to_collections,
    step_build_navigation,
    step_set_seo_tags,
    step_create_redirects,
    step_set_inventory,
    step_publish_resources,
    step_migrate_discounts,
    step_activate_products,
    step_create_policies,
    main,
)


# ---------------------------------------------------------------------------
# Step 1: Enable Arabic locale
# ---------------------------------------------------------------------------

class TestStepEnableArabic:
    def test_enables_arabic(self):
        client = MagicMock()
        client.get_locales.return_value = [{"locale": "en", "primary": True, "published": True}]
        client.enable_locale.return_value = {"locale": "ar", "published": True}

        step_enable_arabic(client)

        client.enable_locale.assert_called_once_with("ar")

    def test_skips_if_already_enabled(self):
        client = MagicMock()
        client.get_locales.return_value = [
            {"locale": "en", "primary": True, "published": True},
            {"locale": "ar", "primary": False, "published": True},
        ]

        step_enable_arabic(client)

        client.enable_locale.assert_not_called()

    def test_dry_run(self):
        client = MagicMock()
        step_enable_arabic(client, dry_run=True)
        client.get_locales.assert_not_called()
        client.enable_locale.assert_not_called()

    def test_handles_error(self, capsys):
        client = MagicMock()
        client.get_locales.return_value = [{"locale": "en", "primary": True, "published": True}]
        client.enable_locale.side_effect = Exception("API error")

        step_enable_arabic(client)

        captured = capsys.readouterr()
        assert "error" in captured.out.lower()


# ---------------------------------------------------------------------------
# Step 2: Link products to collections
# ---------------------------------------------------------------------------

class TestStepLinkProducts:
    def test_creates_collects(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        export_dir = data_dir / "spain_export"
        export_dir.mkdir()

        # id_map with product and collection mappings
        id_map = {
            "products": {"100": 200},
            "collections": {"10": 20},
        }
        (data_dir / "id_map.json").write_text(json.dumps(id_map))

        # collects from source
        collects = [{"product_id": 100, "collection_id": 10}]
        (export_dir / "collects.json").write_text(json.dumps(collects))

        client = MagicMock()
        client.create_collect.return_value = {"id": 999}

        step_link_products_to_collections(client)

        client.create_collect.assert_called_once_with(200, 20)

    def test_skips_missing_mappings(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        (data_dir / "spain_export").mkdir()

        id_map = {"products": {}, "collections": {}}
        (data_dir / "id_map.json").write_text(json.dumps(id_map))
        (data_dir / "spain_export" / "collects.json").write_text(
            json.dumps([{"product_id": 100, "collection_id": 10}])
        )

        client = MagicMock()
        step_link_products_to_collections(client)
        client.create_collect.assert_not_called()

    def test_no_collects_data(self, tmp_path, monkeypatch, capsys):
        monkeypatch.chdir(tmp_path)
        (tmp_path / "data").mkdir()
        (tmp_path / "data" / "id_map.json").write_text("{}")

        client = MagicMock()
        step_link_products_to_collections(client)

        captured = capsys.readouterr()
        assert "no collects data" in captured.out.lower()


# ---------------------------------------------------------------------------
# Step 3: Build navigation
# ---------------------------------------------------------------------------

class TestStepBuildNavigation:
    def test_creates_menus(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        data_dir = tmp_path / "data"
        en_dir = data_dir / "english"
        en_dir.mkdir(parents=True)

        id_map = {"collections": {"1": 10}, "pages": {"2": 20}}
        (data_dir / "id_map.json").write_text(json.dumps(id_map))
        (en_dir / "collections.json").write_text(json.dumps([{"id": 1, "title": "Skincare"}]))
        (en_dir / "pages.json").write_text(json.dumps([{"id": 2, "title": "About Us"}]))

        client = MagicMock()
        client.create_menu.return_value = {"id": "gid://shopify/Menu/1", "title": "Main Menu", "handle": "main-menu"}

        step_build_navigation(client)

        assert client.create_menu.call_count == 2

    def test_dry_run(self, tmp_path, monkeypatch, capsys):
        monkeypatch.chdir(tmp_path)
        data_dir = tmp_path / "data"
        en_dir = data_dir / "english"
        en_dir.mkdir(parents=True)

        id_map = {"collections": {"1": 10}, "pages": {}}
        (data_dir / "id_map.json").write_text(json.dumps(id_map))
        (en_dir / "collections.json").write_text(json.dumps([{"id": 1, "title": "Skincare"}]))
        (en_dir / "pages.json").write_text(json.dumps([]))

        client = MagicMock()
        step_build_navigation(client, dry_run=True)

        client.create_menu.assert_not_called()
        captured = capsys.readouterr()
        assert "Skincare" in captured.out


# ---------------------------------------------------------------------------
# Step 4: SEO tags
# ---------------------------------------------------------------------------

class TestStepSetSeoTags:
    def test_sets_product_seo(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        data_dir = tmp_path / "data"
        en_dir = data_dir / "english"
        en_dir.mkdir(parents=True)

        id_map = {"products": {"1": 10}, "collections": {}, "pages": {}}
        (data_dir / "id_map.json").write_text(json.dumps(id_map))
        (en_dir / "products.json").write_text(json.dumps([{
            "id": 1, "title": "Serum",
            "metafields": [
                {"namespace": "global", "key": "title_tag", "value": "Best Serum", "type": "single_line_text_field"},
                {"namespace": "global", "key": "description_tag", "value": "Amazing serum", "type": "single_line_text_field"},
            ],
        }]))
        (en_dir / "collections.json").write_text("[]")
        (en_dir / "pages.json").write_text("[]")

        client = MagicMock()
        client.update_product_seo.return_value = []

        step_set_seo_tags(client)

        client.update_product_seo.assert_called_once_with(10, "Best Serum", "Amazing serum")

    def test_skips_no_seo_data(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        data_dir = tmp_path / "data"
        en_dir = data_dir / "english"
        en_dir.mkdir(parents=True)

        (data_dir / "id_map.json").write_text(json.dumps({"products": {"1": 10}, "collections": {}, "pages": {}}))
        (en_dir / "products.json").write_text(json.dumps([{"id": 1, "title": "Serum", "metafields": []}]))
        (en_dir / "collections.json").write_text("[]")
        (en_dir / "pages.json").write_text("[]")

        client = MagicMock()
        step_set_seo_tags(client)
        client.update_product_seo.assert_not_called()


# ---------------------------------------------------------------------------
# Step 5: Redirects
# ---------------------------------------------------------------------------

class TestStepCreateRedirects:
    def test_creates_redirects(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        data_dir = tmp_path / "data"
        export_dir = data_dir / "spain_export"
        export_dir.mkdir(parents=True)

        redirects = [{"path": "/old-page", "target": "/new-page"}]
        (export_dir / "redirects.json").write_text(json.dumps(redirects))

        client = MagicMock()
        client.create_redirect.return_value = {"id": 1}

        step_create_redirects(client)

        client.create_redirect.assert_called_once_with("/old-page", "/new-page")

    def test_no_redirects(self, tmp_path, monkeypatch, capsys):
        monkeypatch.chdir(tmp_path)
        (tmp_path / "data" / "spain_export").mkdir(parents=True)

        client = MagicMock()
        step_create_redirects(client)

        captured = capsys.readouterr()
        assert "no redirects" in captured.out.lower()


# ---------------------------------------------------------------------------
# Step 6: Inventory
# ---------------------------------------------------------------------------

class TestStepSetInventory:
    def test_sets_inventory(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        (data_dir / "id_map.json").write_text(json.dumps({"products": {"1": 10}}))

        client = MagicMock()
        client.get_locations.return_value = [{"id": 777, "name": "Warehouse"}]
        client.get_products.return_value = [{
            "id": 10,
            "variants": [{"id": 101, "inventory_management": "shopify"}],
        }]
        client.get_inventory_item_id.return_value = "gid://shopify/InventoryItem/501"
        client.set_inventory_quantity.return_value = {}

        step_set_inventory(client, default_quantity=50)

        client.set_inventory_quantity.assert_called_once_with(
            "gid://shopify/InventoryItem/501",
            "gid://shopify/Location/777",
            50,
        )

    def test_skips_unmanaged_variants(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        (tmp_path / "data").mkdir()
        (tmp_path / "data" / "id_map.json").write_text(json.dumps({"products": {}}))

        client = MagicMock()
        client.get_locations.return_value = [{"id": 1, "name": "HQ"}]
        client.get_products.return_value = [{
            "id": 10,
            "variants": [{"id": 101, "inventory_management": None}],
        }]

        step_set_inventory(client)

        client.get_inventory_item_id.assert_not_called()

    def test_no_locations(self, tmp_path, monkeypatch, capsys):
        monkeypatch.chdir(tmp_path)
        (tmp_path / "data").mkdir()
        (tmp_path / "data" / "id_map.json").write_text(json.dumps({}))

        client = MagicMock()
        client.get_locations.return_value = []

        step_set_inventory(client)

        captured = capsys.readouterr()
        assert "no locations" in captured.out.lower()

    def test_dry_run(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        (tmp_path / "data").mkdir()
        (tmp_path / "data" / "id_map.json").write_text(json.dumps({}))

        client = MagicMock()
        step_set_inventory(client, dry_run=True)
        client.get_locations.assert_not_called()


# ---------------------------------------------------------------------------
# Step 7: Publish resources
# ---------------------------------------------------------------------------

class TestStepPublishResources:
    def test_publishes_products_and_collections(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        data_dir = tmp_path / "data"
        data_dir.mkdir()

        id_map = {"products": {"1": 10, "2": 20}, "collections": {"3": 30}}
        (data_dir / "id_map.json").write_text(json.dumps(id_map))

        client = MagicMock()
        client.get_publications.return_value = [
            {"id": "gid://shopify/Publication/1", "name": "Online Store"},
        ]
        client.publish_resource.return_value = {}

        step_publish_resources(client)

        # 2 products + 1 collection = 3 publish calls
        assert client.publish_resource.call_count == 3

    def test_dry_run(self, tmp_path, monkeypatch, capsys):
        monkeypatch.chdir(tmp_path)
        (tmp_path / "data").mkdir()

        client = MagicMock()
        step_publish_resources(client, dry_run=True)
        client.get_publications.assert_not_called()

    def test_no_publications(self, tmp_path, monkeypatch, capsys):
        monkeypatch.chdir(tmp_path)
        (tmp_path / "data").mkdir()
        (tmp_path / "data" / "id_map.json").write_text("{}")

        client = MagicMock()
        client.get_publications.return_value = []

        step_publish_resources(client)

        captured = capsys.readouterr()
        assert "no publications" in captured.out.lower()


# ---------------------------------------------------------------------------
# Step 8: Migrate discounts
# ---------------------------------------------------------------------------

class TestStepMigrateDiscounts:
    def test_creates_price_rules_and_codes(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        data_dir = tmp_path / "data"
        export_dir = data_dir / "spain_export"
        export_dir.mkdir(parents=True)

        price_rules = [{
            "id": 100,
            "title": "Summer Sale",
            "target_type": "line_item",
            "target_selection": "all",
            "allocation_method": "across",
            "value_type": "percentage",
            "value": "-20.0",
            "customer_selection": "all",
            "starts_at": "2024-06-01T00:00:00Z",
            "discount_codes": [{"code": "SUMMER20"}],
        }]
        (export_dir / "price_rules.json").write_text(json.dumps(price_rules))

        client = MagicMock()
        client.create_price_rule.return_value = {"id": 200}
        client.create_discount_code.return_value = {"id": 300, "code": "SUMMER20"}

        step_migrate_discounts(client)

        client.create_price_rule.assert_called_once()
        client.create_discount_code.assert_called_once_with(200, "SUMMER20")

    def test_no_price_rules(self, tmp_path, monkeypatch, capsys):
        monkeypatch.chdir(tmp_path)
        (tmp_path / "data" / "spain_export").mkdir(parents=True)

        client = MagicMock()
        step_migrate_discounts(client)

        captured = capsys.readouterr()
        assert "no price rules" in captured.out.lower()

    def test_dry_run(self, tmp_path, monkeypatch, capsys):
        monkeypatch.chdir(tmp_path)
        export_dir = tmp_path / "data" / "spain_export"
        export_dir.mkdir(parents=True)
        (export_dir / "price_rules.json").write_text(json.dumps([{
            "id": 1, "title": "Test", "value_type": "percentage", "value": "-10",
            "discount_codes": [{"code": "TEST10"}],
        }]))

        client = MagicMock()
        step_migrate_discounts(client, dry_run=True)

        client.create_price_rule.assert_not_called()
        captured = capsys.readouterr()
        assert "TEST10" in captured.out


# ---------------------------------------------------------------------------
# Step 9: Activate products
# ---------------------------------------------------------------------------

class TestStepActivateProducts:
    def test_activates_products(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        data_dir = tmp_path / "data"
        data_dir.mkdir()

        id_map = {"products": {"1": 10, "2": 20}}
        (data_dir / "id_map.json").write_text(json.dumps(id_map))

        client = MagicMock()
        client.update_product.return_value = {"id": 10, "status": "active"}

        step_activate_products(client)

        assert client.update_product.call_count == 2
        client.update_product.assert_any_call(10, {"status": "active"})
        client.update_product.assert_any_call(20, {"status": "active"})

    def test_dry_run(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        (tmp_path / "data").mkdir()

        client = MagicMock()
        step_activate_products(client, dry_run=True)
        client.update_product.assert_not_called()


# ---------------------------------------------------------------------------
# Step 10: Policies
# ---------------------------------------------------------------------------

class TestStepCreatePolicies:
    def test_with_policies(self, tmp_path, monkeypatch, capsys):
        monkeypatch.chdir(tmp_path)
        export_dir = tmp_path / "data" / "spain_export"
        export_dir.mkdir(parents=True)

        policies = [{"title": "Privacy Policy", "body": "<p>We protect your data.</p>"}]
        (export_dir / "policies.json").write_text(json.dumps(policies))

        client = MagicMock()
        step_create_policies(client)

        captured = capsys.readouterr()
        assert "Privacy Policy" in captured.out

    def test_no_policies(self, tmp_path, monkeypatch, capsys):
        monkeypatch.chdir(tmp_path)
        (tmp_path / "data" / "spain_export").mkdir(parents=True)

        client = MagicMock()
        step_create_policies(client)

        captured = capsys.readouterr()
        assert "manually" in captured.out.lower()


# ---------------------------------------------------------------------------
# main()
# ---------------------------------------------------------------------------

class TestMain:
    @patch("post_migration.load_dotenv")
    @patch("post_migration.ShopifyClient")
    def test_runs_all_steps(self, MockClient, mock_dotenv, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        (tmp_path / "data" / "english").mkdir(parents=True)
        (tmp_path / "data" / "spain_export").mkdir(parents=True)
        (tmp_path / "data" / "id_map.json").write_text("{}")
        (tmp_path / "data" / "english" / "products.json").write_text("[]")
        (tmp_path / "data" / "english" / "collections.json").write_text("[]")
        (tmp_path / "data" / "english" / "pages.json").write_text("[]")

        client = MagicMock()
        MockClient.return_value = client
        client.get_locales.return_value = [{"locale": "en", "primary": True, "published": True}]
        client.enable_locale.return_value = {"locale": "ar"}
        client.get_locations.return_value = [{"id": 1, "name": "HQ"}]
        client.get_products.return_value = []
        client.get_publications.return_value = []
        client.update_product.return_value = {}

        os.environ.update({"SAUDI_SHOP_URL": "saudi.myshopify.com", "SAUDI_ACCESS_TOKEN": "tok"})
        try:
            import sys
            monkeypatch.setattr(sys, "argv", ["post_migration.py"])
            main()
        finally:
            del os.environ["SAUDI_SHOP_URL"]
            del os.environ["SAUDI_ACCESS_TOKEN"]

        client.enable_locale.assert_called_once_with("ar")

    @patch("post_migration.load_dotenv")
    @patch("post_migration.ShopifyClient")
    def test_runs_single_step(self, MockClient, mock_dotenv, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        (tmp_path / "data").mkdir()
        (tmp_path / "data" / "id_map.json").write_text("{}")

        client = MagicMock()
        MockClient.return_value = client
        client.get_locales.return_value = [{"locale": "ar", "primary": False, "published": True}]

        os.environ.update({"SAUDI_SHOP_URL": "saudi.myshopify.com", "SAUDI_ACCESS_TOKEN": "tok"})
        try:
            import sys
            monkeypatch.setattr(sys, "argv", ["post_migration.py", "--step", "1"])
            main()
        finally:
            del os.environ["SAUDI_SHOP_URL"]
            del os.environ["SAUDI_ACCESS_TOKEN"]

        # Should have checked locales but not called enable since ar already exists
        client.get_locales.assert_called_once()
        client.enable_locale.assert_not_called()
        # Should NOT have run other steps
        client.create_collect.assert_not_called()
        client.create_menu.assert_not_called()
