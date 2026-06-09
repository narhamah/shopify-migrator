"""Structured logging for the migration pipeline.

Backwards compatible by default: console output stays plain (``%(message)s``)
so the existing print-style pipeline output is unchanged. Opt into richer
behaviour via environment variables:

  LOG_LEVEL   DEBUG|INFO|WARNING|ERROR   (default INFO)
  LOG_FORMAT  plain|rich                 (default plain; rich adds timestamp/level/module)
  LOG_FILE    path                       (always written with the rich format)

A run can also attach a structured file handler programmatically with
``add_run_log_file(path)`` so every module logger also writes to one audit log.
"""

import logging
import os
import sys

PLAIN_FORMAT = "%(message)s"
RICH_FORMAT = "%(asctime)s %(levelname)-7s %(name)s | %(message)s"


def configure_console():
    """Force UTF-8 on stdout/stderr.

    The pipeline prints Unicode (Arabic, →, ═). On a Windows cp1252 console
    that raises UnicodeEncodeError and crashes the run. Reconfiguring to UTF-8
    (Python 3.7+) makes output safe everywhere. Idempotent and best-effort.
    """
    for stream in (sys.stdout, sys.stderr):
        try:
            if hasattr(stream, "reconfigure") and (getattr(stream, "encoding", "") or "").lower().replace("-", "") != "utf8":
                stream.reconfigure(encoding="utf-8")
        except Exception:
            pass


# Run on import so every entry point (all import core.logging via the client) is safe.
configure_console()

# All pipeline loggers are children of this name, so a single handler attached
# here captures the whole tree via propagation.
ROOT_LOGGER_NAME = "tara_migrate"


def _level() -> int:
    return getattr(logging, os.environ.get("LOG_LEVEL", "INFO").upper(), logging.INFO)


def _console_format() -> str:
    return RICH_FORMAT if os.environ.get("LOG_FORMAT", "plain").lower() == "rich" else PLAIN_FORMAT


def _make_file_handler(path: str) -> logging.FileHandler:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    handler = logging.FileHandler(path, encoding="utf-8")
    handler.setFormatter(logging.Formatter(RICH_FORMAT))
    return handler


def get_logger(name: str) -> logging.Logger:
    """Get a configured logger.

    Idempotent: repeated calls for the same name never stack duplicate handlers.
    """
    logger = logging.getLogger(name)
    if not getattr(logger, "_tara_configured", False):
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter(_console_format()))
        logger.addHandler(handler)
        logger.setLevel(_level())
        logger._tara_configured = True  # type: ignore[attr-defined]
        log_file = os.environ.get("LOG_FILE")
        if log_file:
            add_run_log_file(log_file)
    return logger


def add_run_log_file(path: str) -> logging.FileHandler:
    """Attach a structured file handler to the whole ``tara_migrate`` logger tree.

    Call once at the start of a run so every module logger's output is also
    captured to a single on-disk audit log (the run report's companion).
    Idempotent per path — re-attaching the same file is a no-op.
    """
    root = logging.getLogger(ROOT_LOGGER_NAME)
    abspath = os.path.abspath(path)
    for existing in root.handlers:
        if isinstance(existing, logging.FileHandler) and os.path.abspath(existing.baseFilename) == abspath:
            return existing
    handler = _make_file_handler(path)
    root.addHandler(handler)
    if root.level == logging.NOTSET:
        root.setLevel(_level())
    return handler
