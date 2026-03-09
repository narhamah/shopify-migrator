"""Structured logging setup for the migration pipeline."""

import logging
import sys


def get_logger(name):
    """Get a logger with console output matching the existing print() format."""
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter("%(message)s"))
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger
