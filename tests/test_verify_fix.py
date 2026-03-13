"""Tests for the unified verify_fix translation pipeline."""
import csv
import json
import os
from unittest.mock import MagicMock, patch, call

import pytest

from tara_migrate.translation.verify_fix import (
    FIXABLE_STATUSES,
    clean_csv,
    phase_audit,
    phase_fix,
    phase_verify,
    run_pipeline,
    _validate_and_normalize_json,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_problem(resource_id="gid://shopify/Product/1", key="title",
                  status="MISSING", english="Hello", arabic="",
                  resource_type="PRODUCT"):
    return {
        "resource_id": resource_id,
        "resource_type": resource_type,
        "key": key,
        "status": status,
        "detail": "test detail",
        "english": english,
        "arabic": arabic,
        "digest": "abc123",
    }


def _make_stats(total=10, ok=8, missing=1, identical=1):
    return {
        "total": total, "ok": ok, "missing": missing, "identical": identical,
        "not_arabic": 0, "mixed": 0, "corrupted": 0, "outdated": 0, "skip": 0,
    }


# ---------------------------------------------------------------------------
# _validate_and_normalize_json
# ---------------------------------------------------------------------------

class TestValidateAndNormalizeJson:
    def test_plain_text_passthrough(self):
        val, ok = _validate_and_normalize_json("Hello world")
        assert ok is True
        assert val == "Hello world"

    def test_valid_rich_text_json(self):
        rt = json.dumps({"type": "root", "children": []})
        val, ok = _validate_and_normalize_json(rt)
        assert ok is True
        parsed = json.loads(val)
        assert parsed["type"] == "root"

    def test_invalid_json_detected(self):
        val, ok = _validate_and_normalize_json('{"type": "root", broken')
        assert ok is False

    def test_icu_template_not_treated_as_json(self):
        val, ok = _validate_and_normalize_json("{count} items")
        assert ok is True
        assert val == "{count} items"

    def test_json_array(self):
        val, ok = _validate_and_normalize_json('[{"key": "val"}]')
        assert ok is True


# ---------------------------------------------------------------------------
# FIXABLE_STATUSES constant
# ---------------------------------------------------------------------------

class TestFixableStatuses:
    def test_contains_expected_statuses(self):
        assert "MISSING" in FIXABLE_STATUSES
        assert "IDENTICAL" in FIXABLE_STATUSES
        assert "NOT_ARABIC" in FIXABLE_STATUSES
        assert "MIXED_LANGUAGE" in FIXABLE_STATUSES
        assert "CORRUPTED_JSON" in FIXABLE_STATUSES

    def test_ok_not_fixable(self):
        assert "OK" not in FIXABLE_STATUSES

    def test_outdated_not_auto_fixed(self):
        assert "OUTDATED" not in FIXABLE_STATUSES


# ---------------------------------------------------------------------------
# phase_audit
# ---------------------------------------------------------------------------

class TestPhaseAudit:
    @patch("tara_migrate.translation.verify_fix.audit_translations")
    def test_calls_audit_translations(self, mock_audit):
        mock_audit.return_value = ([], _make_stats(total=0, ok=0))
        client = MagicMock()

        problems, stats = phase_audit(client, "ar")

        mock_audit.assert_called_once_with(
            client, locale="ar", resource_types=None, verbose=False,
        )
        assert problems == []

    @patch("tara_migrate.translation.verify_fix.audit_translations")
    def test_passes_resource_types(self, mock_audit):
        mock_audit.return_value = ([], _make_stats(total=0, ok=0))
        client = MagicMock()

        phase_audit(client, "ar", resource_types=["PRODUCT"], verbose=True)

        mock_audit.assert_called_once_with(
            client, locale="ar", resource_types=["PRODUCT"], verbose=True,
        )


# ---------------------------------------------------------------------------
# phase_fix
# ---------------------------------------------------------------------------

class TestPhaseFix:
    def test_no_fixable_problems(self):
        client = MagicMock()
        engine = MagicMock()
        problems = [_make_problem(status="OUTDATED")]

        uploaded, errors, skipped = phase_fix(
            client, engine, "ar", problems,
        )

        assert uploaded == 0
        assert errors == 0
        assert skipped == 0
        engine.translate_fields.assert_not_called()

    def test_dry_run_no_upload(self):
        client = MagicMock()
        engine = MagicMock()
        problems = [_make_problem(status="MISSING")]

        uploaded, errors, skipped = phase_fix(
            client, engine, "ar", problems, dry_run=True,
        )

        assert uploaded == 0
        engine.translate_fields.assert_not_called()

    def test_fix_only_specific_statuses(self):
        client = MagicMock()
        engine = MagicMock()
        problems = [
            _make_problem(status="MISSING", key="title"),
            _make_problem(status="IDENTICAL", key="body_html"),
        ]

        uploaded, errors, skipped = phase_fix(
            client, engine, "ar", problems,
            fix_statuses=frozenset({"IDENTICAL"}), dry_run=True,
        )

        # Only IDENTICAL should be in the plan, not MISSING
        assert uploaded == 0  # dry run

    @patch("tara_migrate.translation.verify_fix.upload_translations")
    @patch("tara_migrate.translation.verify_fix.fetch_translatable_resources")
    def test_translates_and_uploads(self, mock_fetch, mock_upload):
        client = MagicMock()
        engine = MagicMock()

        problems = [_make_problem(
            resource_id="gid://shopify/Product/1",
            key="title", status="MISSING", english="Hello",
        )]

        mock_fetch.return_value = {
            "gid://shopify/Product/1": {
                "content": {
                    "title": {"digest": "digest123", "value": "Hello"},
                },
                "translations": {},
            },
        }
        engine.translate_fields.return_value = {
            "PRODUCT|gid://shopify/Product/1|title": "مرحبا",
        }
        mock_upload.return_value = (1, 0)

        uploaded, errors, skipped = phase_fix(
            client, engine, "ar", problems,
        )

        assert uploaded == 1
        assert errors == 0
        engine.translate_fields.assert_called_once()
        mock_upload.assert_called_once()

    @patch("tara_migrate.translation.verify_fix.upload_translations")
    @patch("tara_migrate.translation.verify_fix.fetch_translatable_resources")
    def test_skips_handle_fields(self, mock_fetch, mock_upload):
        client = MagicMock()
        engine = MagicMock()

        problems = [_make_problem(key="handle", status="MISSING")]
        mock_fetch.return_value = {
            "gid://shopify/Product/1": {
                "content": {"handle": {"digest": "d", "value": "test"}},
                "translations": {},
            },
        }
        engine.translate_fields.return_value = {}
        mock_upload.return_value = (0, 0)

        uploaded, errors, skipped = phase_fix(
            client, engine, "ar", problems,
        )

        # Handle fields are skipped
        assert skipped >= 1

    @patch("tara_migrate.translation.verify_fix.upload_translations")
    @patch("tara_migrate.translation.verify_fix.fetch_translatable_resources")
    def test_progress_file_saves(self, mock_fetch, mock_upload, tmp_path):
        client = MagicMock()
        engine = MagicMock()
        progress_file = str(tmp_path / "progress.json")

        problems = [_make_problem(status="MISSING")]
        mock_fetch.return_value = {
            "gid://shopify/Product/1": {
                "content": {"title": {"digest": "d", "value": "Hello"}},
                "translations": {},
            },
        }
        engine.translate_fields.return_value = {
            "PRODUCT|gid://shopify/Product/1|title": "مرحبا",
        }
        mock_upload.return_value = (1, 0)

        phase_fix(client, engine, "ar", problems,
                  progress_file=progress_file)

        assert os.path.exists(progress_file)
        with open(progress_file) as f:
            data = json.load(f)
        assert len(data["uploaded"]) == 1

    @patch("tara_migrate.translation.verify_fix.upload_translations")
    @patch("tara_migrate.translation.verify_fix.fetch_translatable_resources")
    def test_resumes_from_progress(self, mock_fetch, mock_upload, tmp_path):
        client = MagicMock()
        engine = MagicMock()
        progress_file = str(tmp_path / "progress.json")

        # Write existing progress
        with open(progress_file, "w") as f:
            json.dump({"uploaded": [
                "PRODUCT|gid://shopify/Product/1|title",
            ]}, f)

        problems = [_make_problem(status="MISSING")]
        uploaded, errors, skipped = phase_fix(
            client, engine, "ar", problems,
            progress_file=progress_file,
        )

        # Should skip the already-done field
        assert uploaded == 0
        engine.translate_fields.assert_not_called()


# ---------------------------------------------------------------------------
# phase_verify
# ---------------------------------------------------------------------------

class TestPhaseVerify:
    @patch("tara_migrate.translation.verify_fix.audit_translations")
    def test_calls_audit_again(self, mock_audit):
        mock_audit.return_value = ([], _make_stats(total=10, ok=10))
        client = MagicMock()

        problems, stats = phase_verify(client, "ar")

        mock_audit.assert_called_once_with(
            client, locale="ar", resource_types=None, verbose=False,
        )


# ---------------------------------------------------------------------------
# run_pipeline
# ---------------------------------------------------------------------------

class TestRunPipeline:
    @patch("tara_migrate.translation.verify_fix.phase_verify")
    @patch("tara_migrate.translation.verify_fix.phase_fix")
    @patch("tara_migrate.translation.verify_fix.phase_audit")
    def test_audit_only_skips_fix_and_verify(self, mock_audit, mock_fix,
                                              mock_verify):
        mock_audit.return_value = (
            [_make_problem()], _make_stats(total=10, ok=9, missing=1),
        )
        client = MagicMock()

        result = run_pipeline(client, None, "ar", audit_only=True)

        mock_audit.assert_called_once()
        mock_fix.assert_not_called()
        mock_verify.assert_not_called()
        assert len(result["audit"]["problems"]) == 1

    @patch("tara_migrate.translation.verify_fix.phase_verify")
    @patch("tara_migrate.translation.verify_fix.phase_fix")
    @patch("tara_migrate.translation.verify_fix.phase_audit")
    def test_no_problems_skips_fix(self, mock_audit, mock_fix, mock_verify):
        mock_audit.return_value = ([], _make_stats(total=10, ok=10))
        client = MagicMock()

        result = run_pipeline(client, MagicMock(), "ar")

        mock_fix.assert_not_called()
        mock_verify.assert_not_called()

    @patch("tara_migrate.translation.verify_fix.phase_verify")
    @patch("tara_migrate.translation.verify_fix.phase_fix")
    @patch("tara_migrate.translation.verify_fix.phase_audit")
    def test_full_pipeline_runs_all_phases(self, mock_audit, mock_fix,
                                            mock_verify):
        mock_audit.return_value = (
            [_make_problem()], _make_stats(total=10, ok=9, missing=1),
        )
        mock_fix.return_value = (1, 0, 0)
        mock_verify.return_value = ([], _make_stats(total=10, ok=10))

        client = MagicMock()
        engine = MagicMock()

        result = run_pipeline(client, engine, "ar")

        mock_audit.assert_called_once()
        mock_fix.assert_called_once()
        mock_verify.assert_called_once()
        assert result["fix"]["uploaded"] == 1

    @patch("tara_migrate.translation.verify_fix.phase_verify")
    @patch("tara_migrate.translation.verify_fix.phase_fix")
    @patch("tara_migrate.translation.verify_fix.phase_audit")
    def test_no_verify_flag(self, mock_audit, mock_fix, mock_verify):
        mock_audit.return_value = (
            [_make_problem()], _make_stats(total=10, ok=9, missing=1),
        )
        mock_fix.return_value = (1, 0, 0)

        client = MagicMock()
        engine = MagicMock()

        result = run_pipeline(client, engine, "ar", no_verify=True)

        mock_verify.assert_not_called()

    @patch("tara_migrate.translation.verify_fix.phase_verify")
    @patch("tara_migrate.translation.verify_fix.phase_fix")
    @patch("tara_migrate.translation.verify_fix.phase_audit")
    def test_dry_run_skips_verify(self, mock_audit, mock_fix, mock_verify):
        mock_audit.return_value = (
            [_make_problem()], _make_stats(total=10, ok=9, missing=1),
        )
        mock_fix.return_value = (0, 0, 0)

        client = MagicMock()
        engine = MagicMock()

        result = run_pipeline(client, engine, "ar", dry_run=True)

        mock_verify.assert_not_called()

    @patch("tara_migrate.translation.verify_fix.phase_verify")
    @patch("tara_migrate.translation.verify_fix.phase_fix")
    @patch("tara_migrate.translation.verify_fix.phase_audit")
    def test_no_engine_for_fix_shows_error(self, mock_audit, mock_fix,
                                            mock_verify):
        mock_audit.return_value = (
            [_make_problem()], _make_stats(total=10, ok=9, missing=1),
        )

        client = MagicMock()

        result = run_pipeline(client, None, "ar")

        mock_fix.assert_not_called()

    @patch("tara_migrate.translation.verify_fix.phase_verify")
    @patch("tara_migrate.translation.verify_fix.phase_fix")
    @patch("tara_migrate.translation.verify_fix.phase_audit")
    def test_zero_uploads_skips_verify(self, mock_audit, mock_fix,
                                       mock_verify):
        mock_audit.return_value = (
            [_make_problem()], _make_stats(total=10, ok=9, missing=1),
        )
        mock_fix.return_value = (0, 1, 0)  # 0 uploaded, 1 error

        client = MagicMock()
        engine = MagicMock()

        result = run_pipeline(client, engine, "ar")

        mock_verify.assert_not_called()

    @patch("tara_migrate.translation.verify_fix.phase_verify")
    @patch("tara_migrate.translation.verify_fix.phase_fix")
    @patch("tara_migrate.translation.verify_fix.phase_audit")
    def test_resource_types_passed_through(self, mock_audit, mock_fix,
                                            mock_verify):
        mock_audit.return_value = ([], _make_stats(total=5, ok=5))
        client = MagicMock()

        run_pipeline(client, None, "ar",
                     resource_types=["PRODUCT"], audit_only=True)

        mock_audit.assert_called_once_with(
            client, "ar", resource_types=["PRODUCT"], verbose=False,
        )


# ---------------------------------------------------------------------------
# clean_csv
# ---------------------------------------------------------------------------

def _write_csv(path, rows, fieldnames=None):
    """Helper to write a CSV file from a list of dicts."""
    if not fieldnames:
        fieldnames = [
            "Type", "Identification", "Field",
            "Default content", "Translated content",
        ]
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _read_csv(path):
    """Helper to read a CSV file."""
    with open(path, "r", encoding="utf-8") as f:
        return list(csv.DictReader(f))


class TestCleanCsv:
    def test_removes_non_translatable_urls(self, tmp_path):
        csv_path = str(tmp_path / "input.csv")
        _write_csv(csv_path, [
            {"Type": "PRODUCT", "Identification": "123", "Field": "title",
             "Default content": "My Product", "Translated content": "\u0645\u0646\u062a\u062c"},
            {"Type": "PRODUCT", "Identification": "123", "Field": "image",
             "Default content": "https://cdn.shopify.com/img.jpg",
             "Translated content": "https://cdn.shopify.com/img.jpg"},
        ])

        kept, removed, reasons = clean_csv(csv_path)

        assert kept == 1
        assert removed == 1
        assert reasons["non_translatable"] >= 1

    def test_removes_keep_as_is_fields(self, tmp_path):
        csv_path = str(tmp_path / "input.csv")
        _write_csv(csv_path, [
            {"Type": "PRODUCT", "Identification": "123", "Field": "title",
             "Default content": "Product", "Translated content": "\u0645\u0646\u062a\u062c"},
            {"Type": "ONLINE_STORE_THEME", "Identification": "456",
             "Field": "facebook_url",
             "Default content": "Follow us on Facebook",
             "Translated content": "Follow us on Facebook"},
        ])

        kept, removed, reasons = clean_csv(csv_path)

        assert kept == 1
        assert reasons["keep_as_is"] >= 1

    def test_removes_empty_translations(self, tmp_path):
        csv_path = str(tmp_path / "input.csv")
        _write_csv(csv_path, [
            {"Type": "PRODUCT", "Identification": "123", "Field": "title",
             "Default content": "Product", "Translated content": ""},
            {"Type": "PRODUCT", "Identification": "123", "Field": "body",
             "Default content": "Description",
             "Translated content": "\u0648\u0635\u0641"},
        ])

        kept, removed, reasons = clean_csv(csv_path)

        assert kept == 1
        assert reasons["empty_translation"] == 1

    def test_removes_identical_no_arabic(self, tmp_path):
        csv_path = str(tmp_path / "input.csv")
        _write_csv(csv_path, [
            {"Type": "PRODUCT", "Identification": "123", "Field": "title",
             "Default content": "English Only",
             "Translated content": "English Only"},
        ])

        kept, removed, reasons = clean_csv(csv_path)

        assert kept == 0
        assert reasons["identical_no_arabic"] == 1

    def test_keeps_identical_arabic(self, tmp_path):
        csv_path = str(tmp_path / "input.csv")
        arabic_text = "\u0645\u0646\u062a\u062c \u0639\u0631\u0628\u064a"
        _write_csv(csv_path, [
            {"Type": "PRODUCT", "Identification": "123", "Field": "title",
             "Default content": arabic_text,
             "Translated content": arabic_text},
        ])

        kept, removed, reasons = clean_csv(csv_path)

        assert kept == 1

    def test_removes_handle_fields(self, tmp_path):
        csv_path = str(tmp_path / "input.csv")
        _write_csv(csv_path, [
            {"Type": "PRODUCT", "Identification": "123", "Field": "handle",
             "Default content": "my-product",
             "Translated content": "my-product"},
        ])

        kept, removed, reasons = clean_csv(csv_path)

        assert kept == 0
        assert reasons["non_translatable"] >= 1

    def test_removes_gid_values(self, tmp_path):
        csv_path = str(tmp_path / "input.csv")
        _write_csv(csv_path, [
            {"Type": "METAFIELD", "Identification": "789", "Field": "value",
             "Default content": "gid://shopify/Metaobject/100",
             "Translated content": "gid://shopify/Metaobject/100"},
        ])

        kept, removed, reasons = clean_csv(csv_path)

        assert kept == 0

    def test_removes_pure_numbers(self, tmp_path):
        csv_path = str(tmp_path / "input.csv")
        _write_csv(csv_path, [
            {"Type": "PRODUCT", "Identification": "123", "Field": "price",
             "Default content": "29.99",
             "Translated content": "29.99"},
        ])

        kept, removed, reasons = clean_csv(csv_path)

        assert kept == 0

    def test_deduplicates_rows(self, tmp_path):
        csv_path = str(tmp_path / "input.csv")
        row = {"Type": "PRODUCT", "Identification": "123", "Field": "title",
               "Default content": "Product",
               "Translated content": "\u0645\u0646\u062a\u062c"}
        _write_csv(csv_path, [row, row])

        kept, removed, reasons = clean_csv(csv_path)

        assert kept == 1
        assert reasons["duplicate"] == 1

    def test_custom_output_path(self, tmp_path):
        csv_path = str(tmp_path / "input.csv")
        out_path = str(tmp_path / "custom_output.csv")
        _write_csv(csv_path, [
            {"Type": "PRODUCT", "Identification": "123", "Field": "title",
             "Default content": "Product",
             "Translated content": "\u0645\u0646\u062a\u062c"},
        ])

        clean_csv(csv_path, out_path)

        assert os.path.exists(out_path)
        rows = _read_csv(out_path)
        assert len(rows) == 1

    def test_default_output_path(self, tmp_path):
        csv_path = str(tmp_path / "translations.csv")
        _write_csv(csv_path, [
            {"Type": "PRODUCT", "Identification": "123", "Field": "title",
             "Default content": "Product",
             "Translated content": "\u0645\u0646\u062a\u062c"},
        ])

        clean_csv(csv_path)

        expected = str(tmp_path / "translations_clean.csv")
        assert os.path.exists(expected)

    def test_realistic_mix(self, tmp_path):
        """Simulate a typical Shopify CSV with ~70% junk rows."""
        csv_path = str(tmp_path / "export.csv")
        rows = [
            # Should keep: real translations
            {"Type": "PRODUCT", "Identification": "1", "Field": "title",
             "Default content": "Scalp Serum",
             "Translated content": "\u0633\u064a\u0631\u0648\u0645 \u0641\u0631\u0648\u0629 \u0627\u0644\u0631\u0623\u0633"},
            {"Type": "COLLECTION", "Identification": "2", "Field": "title",
             "Default content": "Best Sellers",
             "Translated content": "\u0627\u0644\u0623\u0643\u062b\u0631 \u0645\u0628\u064a\u0639\u0627\u064b"},
            # Should remove: URLs
            {"Type": "PRODUCT", "Identification": "1", "Field": "image_src",
             "Default content": "https://cdn.shopify.com/img.jpg",
             "Translated content": "https://cdn.shopify.com/img.jpg"},
            # Should remove: GIDs
            {"Type": "METAFIELD", "Identification": "3", "Field": "value",
             "Default content": "gid://shopify/Product/999",
             "Translated content": "gid://shopify/Product/999"},
            # Should remove: empty translation
            {"Type": "PRODUCT", "Identification": "1", "Field": "body_html",
             "Default content": "<p>Body text</p>",
             "Translated content": ""},
            # Should remove: number
            {"Type": "PRODUCT", "Identification": "1", "Field": "weight",
             "Default content": "0.5",
             "Translated content": "0.5"},
            # Should remove: handle
            {"Type": "PRODUCT", "Identification": "1", "Field": "handle",
             "Default content": "scalp-serum",
             "Translated content": "scalp-serum"},
        ]
        _write_csv(csv_path, rows)

        kept, removed, reasons = clean_csv(csv_path)

        assert kept == 2  # Only the two real translations
        assert removed == 5
