#!/usr/bin/env python3
"""Run the root-wrapper lint from a fresh checkout."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from tara_migrate.core._lint_wrappers import main  # noqa: E402

if __name__ == "__main__":
    raise SystemExit(main())
