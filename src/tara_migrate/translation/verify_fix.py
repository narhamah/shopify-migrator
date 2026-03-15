"""Unified translation verify-and-fix pipeline.

Combines audit, fix, and re-verify into a single workflow so users don't
need to chain 3-5 separate tools with manual JSON plumbing.

Pipeline:
    1. AUDIT   — Scan all translatable resources, classify each field
    2. FIX     — AI-translate broken/missing fields, upload to Shopify
    3. VERIFY  — Re-audit to confirm fixes landed

Usage:
    # Full pipeline: audit -> fix -> verify
    python verify_fix_translations.py

    # Audit only (no changes)
    python verify_fix_translations.py --audit-only

    # Fix only specific problem types
    python verify_fix_translations.py --fix-only MISSING,IDENTICAL

    # Fix only specific resource types
    python verify_fix_translations.py --type PRODUCT

    # Dry run (audit + show what would be fixed, no uploads)
    python verify_fix_translations.py --dry-run

    # Skip verify step
    python verify_fix_translations.py --no-verify

    # Clean a CSV before Shopify import (no API calls needed)
    python verify_fix_translations.py --clean-csv translations.csv
"""

import argparse
import csv
import json
import os
import re
import sys
import time

from tara_migrate.audit.audit_translations import (
    audit_translations,
    classify_translation,
    _is_csv_non_translatable,
    _is_keep_as_is,
    _has_arabic_for_upload,
)
from tara_migrate.client.shopify_client import ShopifyClient
from tara_migrate.core.graphql_queries import (
    fetch_translatable_resources,
    upload_translations,
)
from tara_migrate.core.rich_text import extract_text, is_rich_text_json
from tara_migrate.core.shopify_fields import TRANSLATABLE_RESOURCE_TYPES
from tara_migrate.translation.engine import TranslationEngine, load_developer_prompt


# Problem types that can be auto-fixed by re-translation
FIXABLE_STATUSES = frozenset({
    "MISSING", "IDENTICAL", "NOT_ARABIC", "MIXED_LANGUAGE", "CORRUPTED_JSON",
})


# ---------------------------------------------------------------------------
# Phase 1: Audit
# ---------------------------------------------------------------------------

def phase_audit(client, locale, resource_types=None, verbose=False):
    """Run translation audit across the store.

    Returns (problems, stats) where problems is a list of dicts with
    resource_id, resource_type, key, status, detail, english, arabic, digest.
    """
    problems, stats = audit_translations(
        client, locale=locale, resource_types=resource_types, verbose=verbose,
    )
    return problems, stats


# ---------------------------------------------------------------------------
# Phase 2: Fix
# ---------------------------------------------------------------------------

def _validate_and_normalize_json(value):
    """Validate JSON value if it looks like rich_text. Returns (value, ok)."""
    stripped = value.strip()
    if stripped.startswith('{"type"') or stripped.startswith("[{"):
        try:
            parsed = json.loads(value)
            return json.dumps(parsed, ensure_ascii=False), True
        except json.JSONDecodeError:
            return value, False
    return value, True


def phase_fix(client, engine, locale, problems, dry_run=False,
              fix_statuses=None, progress_file=None):
    """Translate and upload fixes for audit problems.

    Args:
        client: ShopifyClient instance.
        engine: TranslationEngine for AI translation.
        locale: Target locale (e.g. "ar").
        problems: List of problem dicts from phase_audit.
        dry_run: If True, show plan without uploading.
        fix_statuses: Set of status strings to fix (default: all fixable).
        progress_file: Path to save/resume progress.

    Returns (uploaded, errors, skipped).
    """
    if fix_statuses is None:
        fix_statuses = FIXABLE_STATUSES

    # Filter to fixable problems
    fixable = [p for p in problems if p["status"] in fix_statuses]

    if not fixable:
        print("\n  No fixable problems found.")
        return 0, 0, 0

    # Load progress from previous run
    done_ids = set()
    if progress_file and os.path.exists(progress_file):
        with open(progress_file, "r", encoding="utf-8") as f:
            done_ids = set(json.load(f).get("uploaded", []))
        if done_ids:
            print(f"  Resuming: {len(done_ids)} fields already uploaded")

    # Group by resource, skip already-done
    by_resource = {}
    for p in fixable:
        field_id = f"{p['resource_type']}|{p['resource_id']}|{p['key']}"
        if field_id in done_ids:
            continue
        rid = p["resource_id"]
        if rid not in by_resource:
            by_resource[rid] = []
        by_resource[rid].append(p)

    remaining = sum(len(v) for v in by_resource.values())
    print(f"\n{'=' * 70}")
    print(f"  FIX PHASE")
    print(f"{'=' * 70}")
    print(f"  Fixable problems: {len(fixable)}")
    print(f"  Already done:     {len(done_ids)}")
    print(f"  Remaining:        {remaining} fields across {len(by_resource)} resources")

    # Breakdown by status
    status_counts = {}
    for p in fixable:
        field_id = f"{p['resource_type']}|{p['resource_id']}|{p['key']}"
        if field_id not in done_ids:
            status_counts[p["status"]] = status_counts.get(p["status"], 0) + 1
    if status_counts:
        parts = [f"{s}={c}" for s, c in sorted(status_counts.items())]
        print(f"  Breakdown:        {', '.join(parts)}")

    if remaining == 0:
        print("  Nothing to fix!")
        return 0, 0, 0

    if dry_run:
        print("\n  --- DRY RUN (no changes will be made) ---")
        for rid, items in list(by_resource.items())[:10]:
            print(f"\n  {rid}:")
            for item in items[:5]:
                en_preview = item["english"][:60]
                if item["english"].startswith("{") and '"type"' in item["english"]:
                    extracted = extract_text(item["english"])
                    if extracted:
                        en_preview = f"[rich_text] {extracted[:50]}"
                print(f"    [{item['status']:15s}] {item['key']}: {en_preview}")
            if len(items) > 5:
                print(f"    ... and {len(items) - 5} more fields")
        if len(by_resource) > 10:
            print(f"\n  ... and {len(by_resource) - 10} more resources")
        return 0, 0, 0

    # Fetch full English values from Shopify (audit truncates to 200 chars)
    gid_list = list(by_resource.keys())
    print(f"\n  Fetching full content for {len(gid_list)} resources...")
    full_digest_map = fetch_translatable_resources(client, gid_list, locale)
    print(f"  Fetched digests for {len(full_digest_map)} resources")

    # Log resources that disappeared between audit and fix
    missing_resources = [gid for gid in gid_list if gid not in full_digest_map]
    if missing_resources:
        print(f"  WARNING: {len(missing_resources)} resources deleted since audit:")
        for gid in missing_resources[:5]:
            print(f"    {gid}")
        if len(missing_resources) > 5:
            print(f"    ... and {len(missing_resources) - 5} more")

    # Build translation input using full English from API
    fields_for_ai = []
    content_changed = 0
    for rid, items in by_resource.items():
        dm = full_digest_map.get(rid, {})
        for item in items:
            if item["key"] == "handle":
                continue
            field_id = f"{item['resource_type']}|{rid}|{item['key']}"
            # Prefer full English from API (audit truncates to 200 chars)
            english = item["english"]
            if dm and "content" in dm:
                api_content = dm["content"].get(item["key"])
                if api_content and api_content.get("value"):
                    api_english = api_content["value"]
                    # Detect content changes between audit and fix
                    if (item["english"] and len(item["english"]) < 200
                            and api_english.strip() != item["english"].strip()):
                        content_changed += 1
                    english = api_english
            fields_for_ai.append({"id": field_id, "value": english})

    if content_changed:
        print(f"  NOTE: {content_changed} fields changed since audit "
              f"(will translate current content)")

    print(f"  Fields to translate: {len(fields_for_ai)}")

    # Translate via engine (handles rich_text safely)
    t_map = engine.translate_fields(fields_for_ai)
    print(f"  Translated: {len(t_map)} fields")

    # Upload grouped by resource
    uploaded = 0
    errors = 0
    skipped = 0
    skip_reasons = {"resource_deleted": 0, "handle_field": 0,
                    "ai_no_translation": 0, "field_removed": 0,
                    "invalid_json": 0}

    for batch_start in range(0, len(gid_list), 10):
        batch_gids = gid_list[batch_start:batch_start + 10]
        batch_num = batch_start // 10 + 1
        total_batches = (len(gid_list) + 9) // 10

        if batch_num % 10 == 1 or total_batches <= 20:
            print(f"  Upload batch {batch_num}/{total_batches}...")

        for gid in batch_gids:
            if gid not in full_digest_map:
                count = len(by_resource.get(gid, []))
                skipped += count
                skip_reasons["resource_deleted"] += count
                continue

            dm = full_digest_map[gid]
            translations_input = []
            field_ids_in_batch = []

            for item in by_resource[gid]:
                if item["key"] == "handle":
                    skipped += 1
                    skip_reasons["handle_field"] += 1
                    continue
                field_id = f"{item['resource_type']}|{gid}|{item['key']}"
                ar_value = t_map.get(field_id)
                if not ar_value:
                    skipped += 1
                    skip_reasons["ai_no_translation"] += 1
                    continue

                shopify_field = dm["content"].get(item["key"])
                if not shopify_field:
                    skipped += 1
                    skip_reasons["field_removed"] += 1
                    continue

                # Validate JSON before uploading
                ar_value, is_valid = _validate_and_normalize_json(ar_value)
                if not is_valid:
                    print(f"    WARNING: Skipping invalid JSON for "
                          f"{gid} {item['key']} ({len(ar_value)} chars)")
                    errors += 1
                    skip_reasons["invalid_json"] += 1
                    continue

                translations_input.append({
                    "locale": locale,
                    "key": item["key"],
                    "value": ar_value,
                    "translatableContentDigest": shopify_field["digest"],
                })
                field_ids_in_batch.append(field_id)

            if translations_input:
                u, e = upload_translations(client, gid, translations_input)
                uploaded += u
                errors += e
                if u > 0:
                    done_ids.update(field_ids_in_batch)
                    if progress_file:
                        with open(progress_file, "w", encoding="utf-8") as f:
                            json.dump({"uploaded": sorted(done_ids)}, f, indent=2)

        time.sleep(0.3)

    print(f"\n  Fix results: uploaded={uploaded}, errors={errors}, skipped={skipped}")
    if skipped > 0:
        reasons = {k: v for k, v in skip_reasons.items() if v > 0}
        if reasons:
            parts = [f"{k}={v}" for k, v in sorted(reasons.items())]
            print(f"  Skip breakdown: {', '.join(parts)}")
    return uploaded, errors, skipped


# ---------------------------------------------------------------------------
# Phase 3: Verify
# ---------------------------------------------------------------------------

def phase_verify(client, locale, resource_types=None):
    """Re-audit to confirm fixes landed.

    Returns (problems, stats) — same format as phase_audit.
    """
    print(f"\n{'=' * 70}")
    print(f"  VERIFY PHASE (re-auditing)")
    print(f"{'=' * 70}")
    return audit_translations(
        client, locale=locale, resource_types=resource_types, verbose=False,
    )


# ---------------------------------------------------------------------------
# Full pipeline
# ---------------------------------------------------------------------------

def run_pipeline(client, engine, locale, resource_types=None, verbose=False,
                 dry_run=False, audit_only=False, no_verify=False,
                 fix_statuses=None, progress_file=None):
    """Run the full audit -> fix -> verify pipeline.

    Args:
        client: ShopifyClient instance.
        engine: TranslationEngine for AI translation (can be None if audit_only).
        locale: Target locale (e.g. "ar").
        resource_types: List of resource types to process (default: all).
        verbose: Show every problem during audit.
        dry_run: Show plan without uploading.
        audit_only: Only audit, skip fix and verify.
        no_verify: Skip the verification re-audit.
        fix_statuses: Set of problem statuses to fix (default: all fixable).
        progress_file: Path to save/resume fix progress.

    Returns:
        {
            "audit": {"problems": [...], "stats": {...}},
            "fix": {"uploaded": N, "errors": N, "skipped": N},
            "verify": {"problems": [...], "stats": {...}},
        }
    """
    result = {"audit": {}, "fix": {}, "verify": {}}

    # Phase 1: Audit
    problems, stats = phase_audit(
        client, locale, resource_types=resource_types, verbose=verbose,
    )
    result["audit"] = {"problems": problems, "stats": stats}

    n_problems = stats["total"] - stats["ok"]
    if n_problems == 0:
        print("\n  All translations OK! Nothing to fix.")
        return result

    if audit_only:
        print(f"\n  Audit complete. {n_problems} problems found.")
        print("  Run without --audit-only to fix them.")
        return result

    # Phase 2: Fix
    if engine is None:
        print("\n  ERROR: TranslationEngine required for fix phase.")
        print("  Set OPENAI_API_KEY or use --audit-only.")
        return result

    uploaded, errors, skipped = phase_fix(
        client, engine, locale, problems,
        dry_run=dry_run, fix_statuses=fix_statuses,
        progress_file=progress_file,
    )
    result["fix"] = {
        "uploaded": uploaded, "errors": errors, "skipped": skipped,
    }

    if dry_run or no_verify:
        return result

    # Phase 3: Verify
    if uploaded > 0:
        print("\n  Waiting 2s for Shopify to propagate translations...")
        time.sleep(2)
        verify_problems, verify_stats = phase_verify(
            client, locale, resource_types=resource_types,
        )
        result["verify"] = {"problems": verify_problems, "stats": verify_stats}

        # Print before/after comparison
        before = stats["total"] - stats["ok"]
        after = verify_stats["total"] - verify_stats["ok"]
        delta = before - after
        print(f"\n{'=' * 70}")
        print(f"  SUMMARY")
        print(f"{'=' * 70}")
        print(f"  Before:  {before} problems")
        print(f"  Fixed:   {uploaded} fields uploaded")
        print(f"  After:   {after} problems")
        print(f"  Delta:   {delta:+d} ({'improved' if delta > 0 else 'unchanged'})")
        if after > 0:
            print(f"\n  Remaining problems:")
            remaining_by_status = {}
            for p in verify_problems:
                remaining_by_status[p["status"]] = (
                    remaining_by_status.get(p["status"], 0) + 1
                )
            for s, c in sorted(remaining_by_status.items()):
                print(f"    {s}: {c}")
            print(f"\n  Re-run to fix remaining issues.")
        else:
            print(f"\n  All translations verified OK!")
        print(f"{'=' * 70}")
    else:
        print("\n  No uploads made, skipping verification.")

    return result


# ---------------------------------------------------------------------------
# CSV Cleaning — strip junk rows before Shopify import
# ---------------------------------------------------------------------------

def clean_csv(input_path, output_path=None):
    """Remove non-translatable rows from a Shopify translation CSV.

    Strips rows that Shopify will skip anyway: URLs, GIDs, handles, images,
    pure numbers, hex IDs, keep-as-is fields, empty defaults, and rows
    where the translation is empty or identical with no Arabic.

    Args:
        input_path: Path to the Shopify CSV export.
        output_path: Path for the cleaned CSV. Default: input_clean.csv.

    Returns:
        (kept, removed, removed_reasons) where removed_reasons is a dict.
    """
    if not output_path:
        base, ext = os.path.splitext(input_path)
        output_path = f"{base}_clean{ext}"

    with open(input_path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        rows = list(reader)

    print(f"\n{'=' * 70}")
    print(f"  CLEAN CSV")
    print(f"{'=' * 70}")
    print(f"  Input:  {input_path} ({len(rows)} rows)")

    kept_rows = []
    reasons = {
        "non_translatable": 0, "keep_as_is": 0, "empty_default": 0,
        "empty_translation": 0, "identical_no_arabic": 0, "duplicate": 0,
    }
    seen = set()

    for row in rows:
        default = row.get("Default content", "").strip()
        translated = row.get("Translated content", "").strip()
        field = row.get("Field", "").strip()
        csv_type = row.get("Type", "").strip()
        identification = row.get("Identification", "").strip()

        # Dedup by (Type, Identification, Field)
        dedup_key = (csv_type, identification, field)
        if dedup_key in seen:
            reasons["duplicate"] += 1
            continue
        seen.add(dedup_key)

        # Strip non-translatable rows
        if _is_csv_non_translatable(row):
            reasons["non_translatable"] += 1
            continue

        # Strip keep-as-is rows (images, URLs, form IDs, etc.)
        if _is_keep_as_is(row):
            reasons["keep_as_is"] += 1
            continue

        # Strip rows with no default content
        if not default:
            reasons["empty_default"] += 1
            continue

        # Strip rows with no translation
        if not translated:
            reasons["empty_translation"] += 1
            continue

        # Strip identical translations with no Arabic
        if translated == default and not _has_arabic_for_upload(translated):
            reasons["identical_no_arabic"] += 1
            continue

        kept_rows.append(row)

    # Write clean CSV
    with open(output_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(kept_rows)

    total_removed = len(rows) - len(kept_rows)
    print(f"  Output: {output_path} ({len(kept_rows)} rows)")
    print(f"  Removed: {total_removed} rows ({total_removed / len(rows) * 100:.1f}%)")
    if total_removed > 0:
        active_reasons = {k: v for k, v in reasons.items() if v > 0}
        parts = [f"{k}={v}" for k, v in sorted(active_reasons.items())]
        print(f"  Breakdown: {', '.join(parts)}")
    print(f"{'=' * 70}")

    return len(kept_rows), total_removed, reasons


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Unified translation verify-and-fix pipeline: audit -> fix -> verify",
    )
    parser.add_argument(
        "--locale", default="ar",
        help="Target locale (default: ar)",
    )
    parser.add_argument(
        "--type", default=None,
        help="Resource type filter (PRODUCT, COLLECTION, METAFIELD, etc.)",
    )
    parser.add_argument(
        "--audit-only", action="store_true",
        help="Only audit, don't fix or verify",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Show what would be fixed without uploading",
    )
    parser.add_argument(
        "--no-verify", action="store_true",
        help="Skip re-audit verification after fixing",
    )
    parser.add_argument(
        "--fix-only", default=None,
        help="Comma-separated problem statuses to fix "
             "(e.g. MISSING,IDENTICAL). Default: all fixable",
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true",
        help="Show every problem during audit",
    )
    parser.add_argument(
        "--model", default="gpt-5-nano",
        help="OpenAI model for translation (default: gpt-5-nano)",
    )
    parser.add_argument(
        "--reasoning", default="minimal",
        choices=["minimal", "low", "medium", "high"],
        help="Reasoning effort (default: minimal)",
    )
    parser.add_argument(
        "--batch-size", type=int, default=80,
        help="Fields per translation batch (default: 80)",
    )
    parser.add_argument(
        "--prompt", default=None,
        help="Path to developer prompt file",
    )
    parser.add_argument(
        "--progress-file", default=None,
        help="Path to save/resume progress (default: auto-generated)",
    )
    parser.add_argument(
        "--save-audit", default=None,
        help="Save audit problems to JSON file",
    )
    parser.add_argument(
        "--clean-csv", default=None,
        help="Strip non-translatable rows from a Shopify CSV before import "
             "(no API calls needed, exits after cleaning)",
    )
    parser.add_argument(
        "--clean-csv-output", default=None,
        help="Output path for cleaned CSV (default: <input>_clean.csv)",
    )
    parser.add_argument(
        "--shop-url-env", default="DEST_SHOP_URL",
        help="Env var for shop URL (default: DEST_SHOP_URL)",
    )
    parser.add_argument(
        "--token-env", default="DEST_ACCESS_TOKEN",
        help="Env var for access token (default: DEST_ACCESS_TOKEN)",
    )
    args = parser.parse_args()

    # CSV cleaning mode — no API calls, no env vars needed
    if args.clean_csv:
        if not os.path.exists(args.clean_csv):
            print(f"ERROR: CSV not found: {args.clean_csv}")
            sys.exit(1)
        clean_csv(args.clean_csv, args.clean_csv_output)
        return

    from dotenv import load_dotenv
    load_dotenv()

    shop_url = os.environ.get(args.shop_url_env)
    token = os.environ.get(args.token_env)
    if not shop_url or not token:
        print(f"ERROR: Set {args.shop_url_env} and {args.token_env} in .env")
        sys.exit(1)

    client = ShopifyClient(shop_url, token)

    # Resource types
    resource_types = None
    if args.type:
        resource_types = [t.strip().upper() for t in args.type.split(",")]

    # Fix statuses
    fix_statuses = None
    if args.fix_only:
        fix_statuses = frozenset(
            s.strip().upper() for s in args.fix_only.split(",")
        )

    # Translation engine (only needed if not audit-only)
    engine = None
    if not args.audit_only:
        # Find developer prompt
        prompt_path = args.prompt
        if not prompt_path:
            project_root = os.path.dirname(os.path.dirname(os.path.dirname(
                os.path.dirname(os.path.abspath(__file__)))))
            candidates = [
                os.path.join(project_root, "Arabic",
                             "tara_cached_developer_prompt.txt"),
                os.path.join(project_root, "developer_prompt.txt"),
            ]
            for c in candidates:
                if os.path.exists(c):
                    prompt_path = c
                    break

        developer_prompt = load_developer_prompt(
            prompt_path or "developer_prompt.txt",
        )
        engine = TranslationEngine(
            developer_prompt,
            model=args.model,
            reasoning_effort=args.reasoning,
            batch_size=args.batch_size,
        )

    # Progress file
    progress_file = args.progress_file
    if not progress_file and not args.audit_only and not args.dry_run:
        progress_file = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(
                os.path.dirname(os.path.abspath(__file__))))),
            "data", "verify_fix_progress.json",
        )

    # Run pipeline
    result = run_pipeline(
        client, engine, locale=args.locale,
        resource_types=resource_types, verbose=args.verbose,
        dry_run=args.dry_run, audit_only=args.audit_only,
        no_verify=args.no_verify, fix_statuses=fix_statuses,
        progress_file=progress_file,
    )

    # Save audit JSON if requested
    if args.save_audit and result["audit"].get("problems"):
        with open(args.save_audit, "w", encoding="utf-8") as f:
            json.dump(result["audit"]["problems"], f, ensure_ascii=False,
                      indent=2)
        print(f"\n  Audit problems saved to {args.save_audit}")


if __name__ == "__main__":
    main()
