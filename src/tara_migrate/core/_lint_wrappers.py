"""Lint root-level scripts to enforce the thin-wrapper convention.

Per CLAUDE.md, every root *.py is meant to be a 3-5 line entry point that imports
from src/tara_migrate and calls main(). This finds root scripts carrying real
logic so they can be migrated into the package. A curated allowlist holds the
known legacy offenders so CI fails on NEW violations without blocking on the
existing backlog.
"""

import ast
import os

# Known legacy root scripts that still carry logic (to be migrated incrementally).
# New root scripts must be thin; do not add to this list without migrating soon.
LEGACY_ALLOWLIST = {
    "analyze_theme_keys.py",
    "clean_pdp_images.py",
    "fix_pdp_audit.py",
    "fix_pdp_order.py",
    "audit_pdp_images.py",
    "fix_pdp_ar_metafield.py",
    "create_bundles.py",
}

MAX_WRAPPER_LINES = 10


def _significant_lines(path):
    """Count non-blank, non-comment, non-docstring statements in a file."""
    try:
        tree = ast.parse(open(path, encoding="utf-8").read())
    except (SyntaxError, OSError):
        return 0
    count = 0
    for node in tree.body:
        # Skip module docstring and bare string expressions.
        if isinstance(node, ast.Expr) and isinstance(getattr(node, "value", None), ast.Constant):
            continue
        count += 1
        # Count nested statements inside if __name__ == "__main__": blocks too.
        for child in ast.iter_child_nodes(node):
            if isinstance(child, ast.stmt):
                count += 1
    return count


def find_fat_wrappers(root=".", threshold=MAX_WRAPPER_LINES, allowlist=None):
    """Return [(filename, statement_count)] for root scripts exceeding *threshold*."""
    allowlist = LEGACY_ALLOWLIST if allowlist is None else allowlist
    violations = []
    for name in sorted(os.listdir(root)):
        if not name.endswith(".py") or name in allowlist:
            continue
        path = os.path.join(root, name)
        if not os.path.isfile(path):
            continue
        n = _significant_lines(path)
        if n > threshold:
            violations.append((name, n))
    return violations


def main():
    import sys
    violations = find_fat_wrappers(".")
    if not violations:
        print("Wrapper lint: OK — all root scripts are thin.")
        return 0
    print("Wrapper lint: the following root scripts carry too much logic "
          "(move into src/tara_migrate/):")
    for name, n in violations:
        print(f"  {name}: {n} statements (max {MAX_WRAPPER_LINES})")
    return 1


if __name__ == "__main__":
    import sys
    sys.exit(main())
