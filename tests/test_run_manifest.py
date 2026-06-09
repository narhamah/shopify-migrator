"""Tests for core.run_manifest."""
import json

from tara_migrate.core.run_manifest import (
    COMPLETED,
    FAILED,
    RunManifest,
    hash_paths,
    hash_values,
)


class TestHashing:
    def test_hash_values_stable(self):
        assert hash_values(["a", "b"]) == hash_values(["a", "b"])
        assert hash_values(["a", "b"]) != hash_values(["a", "c"])

    def test_hash_paths_detects_change(self, tmp_path):
        f = tmp_path / "x.json"
        f.write_text("hello", encoding="utf-8")
        h1 = hash_paths([str(tmp_path)])
        f.write_text("hello world longer", encoding="utf-8")
        h2 = hash_paths([str(tmp_path)])
        assert h1 != h2

    def test_hash_paths_missing_ok(self, tmp_path):
        assert hash_paths([str(tmp_path / "nope")]).startswith("sha256:")


class TestRunManifest:
    def test_lifecycle_completed(self, tmp_path):
        path = str(tmp_path / "run_manifest.json")
        mf = RunManifest.load_or_create(path, destination="kuwait", config_hash="c1", source_export_hash="s1")
        mf.begin_run("build-1")
        mf.start_phase("phase_1")
        mf.complete_phase("phase_1", counts={"created": 5})
        status = mf.end_run()
        assert status == COMPLETED
        on_disk = json.loads((tmp_path / "run_manifest.json").read_text(encoding="utf-8"))
        assert on_disk["status"] == COMPLETED
        assert on_disk["phases"]["phase_1"]["counts"]["created"] == 5

    def test_failure_sets_failed_status_and_exit(self, tmp_path):
        path = str(tmp_path / "m.json")
        mf = RunManifest.load_or_create(path, destination="d", config_hash="c", source_export_hash="s")
        mf.begin_run("b")
        mf.start_phase("phase_3")
        mf.fail_phase("phase_3", "boom", checkpoint={"last_item": "product:7"})
        assert mf.end_run() == FAILED
        assert mf.data["phases"]["phase_3"]["checkpoint"]["last_item"] == "product:7"
        assert mf.data["summary"]["phase_failures"] == 1

    def test_resume_skips_completed_when_hashes_match(self, tmp_path):
        path = str(tmp_path / "m.json")
        mf = RunManifest.load_or_create(path, "d", config_hash="c", source_export_hash="s")
        mf.begin_run("b")
        mf.complete_phase("phase_1")
        # Reload with SAME hashes -> phase_1 still completed.
        mf2 = RunManifest.load_or_create(path, "d", config_hash="c", source_export_hash="s")
        assert mf2.is_completed("phase_1")

    def test_changed_hash_resets_phases(self, tmp_path):
        path = str(tmp_path / "m.json")
        mf = RunManifest.load_or_create(path, "d", config_hash="c", source_export_hash="s")
        mf.begin_run("b")
        mf.complete_phase("phase_1")
        # Reload with DIFFERENT source hash -> prior phase results invalidated.
        mf2 = RunManifest.load_or_create(path, "d", config_hash="c", source_export_hash="s_NEW")
        assert not mf2.is_completed("phase_1")

    def test_summary_aggregates_counts(self, tmp_path):
        mf = RunManifest.load_or_create(str(tmp_path / "m.json"), "d", "c", "s")
        mf.complete_phase("p1", counts={"created": 3, "failed": 1})
        mf.complete_phase("p2", counts={"created": 2, "skipped": 4})
        totals = mf.summary_counts()
        assert totals["created"] == 5
        assert totals["failed"] == 1
        assert totals["skipped"] == 4
