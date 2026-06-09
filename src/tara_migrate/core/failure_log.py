"""Per-item failure log — machine-readable record of which items failed and why.

Item errors used to be ``print()``-only while id_map recorded just successes, so
one transient failure was invisible and a phase still exited 0. This writes a
``phase_errors_<phase>.json`` artifact ([{type, source_id, action, error,
handle, retry_count}]) that:
  * makes partial failure visible and machine-readable,
  * lets the orchestrator fail the phase truthfully (non-zero exit), and
  * supports targeted retry (re-running only the failed items).

A successful (or now-skipped) item clears its prior entry, so the log always
reflects the current outstanding failures.
"""

import json
import os


class FailureLog:
    def __init__(self, path, items=None):
        self.path = path
        self.items = items or []

    @classmethod
    def load(cls, path):
        if path and os.path.exists(path):
            try:
                with open(path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                if isinstance(data, list):
                    return cls(path, data)
            except (ValueError, OSError):
                pass
        return cls(path, [])

    def _key(self, item_type, source_id):
        return (item_type, str(source_id))

    def record(self, item_type, source_id, action, error, handle=None):
        """Add or update a failure entry, incrementing retry_count on repeats."""
        key = self._key(item_type, source_id)
        for entry in self.items:
            if self._key(entry["type"], entry["source_id"]) == key:
                entry["action"] = action
                entry["error"] = str(error)[:1000]
                entry["handle"] = handle
                entry["retry_count"] = entry.get("retry_count", 0) + 1
                return
        self.items.append({
            "type": item_type,
            "source_id": str(source_id),
            "action": action,
            "error": str(error)[:1000],
            "handle": handle,
            "retry_count": 0,
        })

    def clear(self, item_type, source_id):
        """Remove a resolved item's failure entry (call on success/skip)."""
        key = self._key(item_type, source_id)
        self.items = [e for e in self.items if self._key(e["type"], e["source_id"]) != key]

    def by_type(self, item_type):
        return [e for e in self.items if e["type"] == item_type]

    @property
    def count(self):
        return len(self.items)

    def summary(self):
        if not self.items:
            return "No item failures."
        by_type = {}
        for e in self.items:
            by_type.setdefault(e["type"], 0)
            by_type[e["type"]] += 1
        parts = ", ".join(f"{n} {t}" for t, n in sorted(by_type.items()))
        return f"{self.count} item failure(s): {parts}"

    def save(self):
        if not self.path:
            return
        os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
        tmp = self.path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(self.items, f, ensure_ascii=False, indent=2)
        os.replace(tmp, self.path)
