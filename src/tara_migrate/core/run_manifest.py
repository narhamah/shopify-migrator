"""Typed, resumable run manifest — the source of truth for a migration build.

Replaces the orchestrator's stdout-only "BUILD COMPLETE" with a machine-readable
record at ``data/{dest}/run_manifest.json`` that captures, per phase:
status, counts (created/updated/failed/skipped), errors, timing, and a resume
checkpoint. The orchestrator uses it to skip already-completed phases (when the
config and source export are unchanged) and to set a truthful process exit code.
"""

import hashlib
import json
import os
from datetime import datetime, timezone

PENDING = "pending"
RUNNING = "running"
COMPLETED = "completed"
FAILED = "failed"


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def hash_paths(paths) -> str:
    """Stable hash of a set of files by (relpath, size, mtime).

    Cheap enough to run on a large export directory and sensitive enough to
    detect that the source data changed between runs.
    """
    h = hashlib.sha256()
    entries = []
    for path in paths:
        if os.path.isdir(path):
            for root, _dirs, files in os.walk(path):
                for fn in files:
                    fp = os.path.join(root, fn)
                    try:
                        st = os.stat(fp)
                    except OSError:
                        continue
                    entries.append((os.path.relpath(fp, path), st.st_size, int(st.st_mtime)))
        elif os.path.isfile(path):
            st = os.stat(path)
            entries.append((os.path.basename(path), st.st_size, int(st.st_mtime)))
    for rel, size, mtime in sorted(entries):
        h.update(f"{rel}|{size}|{mtime}\n".encode("utf-8"))
    return "sha256:" + h.hexdigest()[:16]


def hash_values(values) -> str:
    """Stable hash of an ordered list of config values (for config_hash)."""
    h = hashlib.sha256()
    for v in values:
        h.update(f"{v}\n".encode("utf-8"))
    return "sha256:" + h.hexdigest()[:16]


class RunManifest:
    def __init__(self, path, data=None):
        self.path = path
        self.data = data or {
            "build_id": None,
            "destination": None,
            "config_hash": None,
            "source_export_hash": None,
            "status": PENDING,
            "started_at": None,
            "ended_at": None,
            "phases": {},
            "summary": {},
        }

    # --- lifecycle ---

    @classmethod
    def load(cls, path):
        with open(path, "r", encoding="utf-8") as f:
            return cls(path, json.load(f))

    @classmethod
    def load_or_create(cls, path, destination=None, config_hash=None, source_export_hash=None):
        if os.path.exists(path):
            mf = cls.load(path)
        else:
            mf = cls(path)
        mf.data["destination"] = destination
        # If the inputs changed, the prior phase results are stale -> reset resume eligibility.
        if mf.data.get("config_hash") != config_hash or mf.data.get("source_export_hash") != source_export_hash:
            if mf.data.get("phases"):
                mf.data["phases"] = {}
        mf.data["config_hash"] = config_hash
        mf.data["source_export_hash"] = source_export_hash
        return mf

    def begin_run(self, build_id):
        self.data["build_id"] = build_id
        self.data["status"] = RUNNING
        self.data["started_at"] = _now()
        self.data["ended_at"] = None
        self.save()

    def end_run(self):
        phases = self.data["phases"].values()
        any_failed = any(p.get("status") == FAILED for p in phases)
        self.data["status"] = FAILED if any_failed else COMPLETED
        self.data["ended_at"] = _now()
        self.data["summary"] = self.summary_counts()
        self.save()
        return self.data["status"]

    # --- per-phase ---

    def _phase(self, name):
        return self.data["phases"].setdefault(name, {"status": PENDING})

    def start_phase(self, name):
        p = self._phase(name)
        p["status"] = RUNNING
        p["started_at"] = _now()
        p.pop("error", None)
        self.save()

    def complete_phase(self, name, counts=None):
        p = self._phase(name)
        p["status"] = COMPLETED
        p["ended_at"] = _now()
        if counts:
            p["counts"] = counts
        p.pop("checkpoint", None)
        self.save()

    def fail_phase(self, name, error, checkpoint=None, counts=None):
        p = self._phase(name)
        p["status"] = FAILED
        p["ended_at"] = _now()
        p["error"] = str(error)[:2000]
        if checkpoint is not None:
            p["checkpoint"] = checkpoint
        if counts:
            p["counts"] = counts
        self.save()

    def is_completed(self, name):
        return self.data["phases"].get(name, {}).get("status") == COMPLETED

    def set_manual_steps(self, steps):
        """Attach the guided manual-step checklist to the manifest summary."""
        self.data.setdefault("summary", {})["manual_steps"] = steps
        self.save()

    # --- summary / io ---

    def summary_counts(self):
        totals = {"created": 0, "updated": 0, "failed": 0, "skipped": 0}
        phase_failures = 0
        for p in self.data["phases"].values():
            if p.get("status") == FAILED:
                phase_failures += 1
            for k, v in (p.get("counts") or {}).items():
                if k in totals and isinstance(v, int):
                    totals[k] += v
        totals["phase_failures"] = phase_failures
        return totals

    def save(self):
        os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
        tmp = self.path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(self.data, f, ensure_ascii=False, indent=2)
        os.replace(tmp, self.path)
