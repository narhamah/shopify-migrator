"""Tests for the unified verify_fix translation pipeline."""
import json
import os
from unittest.mock import MagicMock, patch, call

import pytest

from tara_migrate.translation.verify_fix import (
    FIXABLE_STATUSES,
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
