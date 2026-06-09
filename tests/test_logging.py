"""Tests for core.logging structured logging."""
import importlib
import logging

import pytest


@pytest.fixture
def fresh_logging(monkeypatch):
    """Reload the module so env-driven format/level is re-read per test."""
    import tara_migrate.core.logging as lg
    importlib.reload(lg)
    yield lg
    # Detach any handlers we added to the shared tree.
    logging.getLogger(lg.ROOT_LOGGER_NAME).handlers.clear()


def test_default_console_is_plain(fresh_logging, monkeypatch):
    monkeypatch.delenv("LOG_FORMAT", raising=False)
    assert fresh_logging._console_format() == fresh_logging.PLAIN_FORMAT


def test_rich_console_via_env(fresh_logging, monkeypatch):
    monkeypatch.setenv("LOG_FORMAT", "rich")
    assert fresh_logging._console_format() == fresh_logging.RICH_FORMAT


def test_level_via_env(fresh_logging, monkeypatch):
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")
    assert fresh_logging._level() == logging.DEBUG


def test_get_logger_is_idempotent(fresh_logging):
    a = fresh_logging.get_logger("tara_migrate.test.idem")
    n = len(a.handlers)
    b = fresh_logging.get_logger("tara_migrate.test.idem")
    assert a is b
    assert len(b.handlers) == n  # no duplicate handlers


def test_run_log_file_captures_output(fresh_logging, tmp_path):
    log_path = tmp_path / "logs" / "build.log"
    fresh_logging.add_run_log_file(str(log_path))
    # Re-attaching the same path is a no-op.
    fresh_logging.add_run_log_file(str(log_path))
    root = logging.getLogger(fresh_logging.ROOT_LOGGER_NAME)
    file_handlers = [h for h in root.handlers if isinstance(h, logging.FileHandler)]
    assert len(file_handlers) == 1

    child = fresh_logging.get_logger("tara_migrate.test.filecapture")
    child.info("hello-run-log")
    for h in file_handlers:
        h.flush()
    assert "hello-run-log" in log_path.read_text(encoding="utf-8")
