"""Tests for the unified migrate CLI and wrapper linter."""
import sys
from unittest.mock import MagicMock, patch

import pytest

from tara_migrate import cli
from tara_migrate.core._lint_wrappers import find_fat_wrappers


class TestCli:
    def test_help_returns_zero(self, capsys):
        assert cli.main(["--help"]) == 0
        assert "verbs:" in capsys.readouterr().out

    def test_unknown_verb(self, capsys):
        assert cli.main(["frobnicate"]) == 2
        assert "Unknown verb" in capsys.readouterr().out

    def test_dispatch_forwards_args(self):
        fake = MagicMock()
        recorded = {}
        def fake_main():
            recorded["argv"] = list(sys.argv)
            return 0
        fake.main = fake_main
        with patch("importlib.import_module", return_value=fake) as imp:
            rc = cli.main(["export", "--dry-run", "--foo", "bar"])
        assert rc == 0
        imp.assert_called_once_with("tara_migrate.pipeline.export_source")
        assert recorded["argv"] == ["migrate export", "--dry-run", "--foo", "bar"]

    def test_resume_injects_flag(self):
        fake = MagicMock()
        recorded = {}
        fake.main = lambda: recorded.setdefault("argv", list(sys.argv)) or 0
        with patch("importlib.import_module", return_value=fake):
            cli.main(["resume", "--lang", "ar"])
        assert recorded["argv"] == ["migrate resume", "--resume", "--lang", "ar"]


class TestWrapperLint:
    def test_flags_fat_wrapper(self, tmp_path):
        (tmp_path / "thin.py").write_text(
            'from x import main\nif __name__ == "__main__":\n    main()\n', encoding="utf-8")
        (tmp_path / "fat.py").write_text("\n".join(f"x{i} = {i}" for i in range(20)), encoding="utf-8")
        violations = dict(find_fat_wrappers(str(tmp_path), allowlist=set()))
        assert "fat.py" in violations
        assert "thin.py" not in violations

    def test_allowlist_excludes(self, tmp_path):
        (tmp_path / "legacy.py").write_text("\n".join(f"x{i}={i}" for i in range(20)), encoding="utf-8")
        violations = dict(find_fat_wrappers(str(tmp_path), allowlist={"legacy.py"}))
        assert "legacy.py" not in violations

    def test_new_wrappers_are_thin(self):
        """The wrappers added in this refactor must stay thin."""
        violations = dict(find_fat_wrappers("."))
        for wrapper in ("migrate.py", "setup_markets.py", "migrate_shipping.py",
                        "migrate_flows.py", "setup_klaviyo.py", "acceptance.py"):
            assert wrapper not in violations
