#!/usr/bin/env python3
"""Unified `migrate` CLI — one entry point dispatching to every pipeline verb.

Collapses the sprawl of root scripts into a single coherent command:

    migrate run        [build_site args]      # full / phased build (idempotent, resumable)
    migrate resume     [build_site args]      # build_site --resume
    migrate export     [export_source args]
    migrate import     [import_english args]
    migrate translate  [translate_gaps args]
    migrate arabic     [import_arabic args]
    migrate images     [migrate_all_images args]
    migrate post       [post_migration args]
    migrate markets    [setup_markets args]
    migrate shipping   [migrate_shipping args]
    migrate flows      [migrate_flows args]
    migrate setup      [setup_store args]
    migrate verify     [acceptance args]

Each verb forwards its remaining args to the underlying module's main(), so all
existing flags keep working.
"""

import importlib
import sys

# verb -> (module path, extra argv to inject before the user's args)
VERBS = {
    "run":       ("tara_migrate.pipeline.build_site", []),
    "resume":    ("tara_migrate.pipeline.build_site", ["--resume"]),
    "export":    ("tara_migrate.pipeline.export_source", []),
    "import":    ("tara_migrate.pipeline.import_english", []),
    "translate": ("tara_migrate.translation.translate_gaps", []),
    "arabic":    ("tara_migrate.pipeline.import_arabic", []),
    "images":    ("tara_migrate.pipeline.migrate_all_images", []),
    "post":      ("tara_migrate.pipeline.post_migration", []),
    "markets":   ("tara_migrate.setup.setup_markets", []),
    "shipping":  ("tara_migrate.pipeline.migrate_shipping", []),
    "flows":     ("tara_migrate.pipeline.migrate_flows", []),
    "setup":     ("tara_migrate.setup.setup_store", []),
    "verify":    ("tara_migrate.audit.acceptance", []),
}


def _usage():
    print("usage: migrate <verb> [args...]\n")
    print("verbs:")
    for verb in VERBS:
        print(f"  {verb}")
    print("\nRun `migrate <verb> --help` for a verb's options.")


def main(argv=None):
    argv = list(sys.argv[1:] if argv is None else argv)
    if not argv or argv[0] in ("-h", "--help", "help"):
        _usage()
        return 0
    verb = argv[0]
    if verb not in VERBS:
        print(f"Unknown verb: {verb}\n")
        _usage()
        return 2

    module_path, injected = VERBS[verb]
    module = importlib.import_module(module_path)
    # Rebuild argv for the target module: program name + injected flags + user args.
    sys.argv = [f"migrate {verb}"] + injected + argv[1:]
    result = module.main()
    return result if isinstance(result, int) else 0


if __name__ == "__main__":
    raise SystemExit(main())
