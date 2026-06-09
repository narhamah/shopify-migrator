"""Tests for core.failure_log."""

from tara_migrate.core.failure_log import FailureLog


def test_record_and_save_load(tmp_path):
    path = str(tmp_path / "phase_errors.json")
    fl = FailureLog(path)
    fl.record("product", "100", "create_product", "422 bad", handle="serum")
    fl.save()
    reloaded = FailureLog.load(path)
    assert reloaded.count == 1
    e = reloaded.items[0]
    assert e["type"] == "product"
    assert e["source_id"] == "100"
    assert e["handle"] == "serum"
    assert e["retry_count"] == 0


def test_record_same_item_increments_retry(tmp_path):
    fl = FailureLog(str(tmp_path / "e.json"))
    fl.record("product", "1", "create", "err1")
    fl.record("product", "1", "create", "err2")
    assert fl.count == 1
    assert fl.items[0]["retry_count"] == 1
    assert fl.items[0]["error"] == "err2"


def test_clear_removes_entry(tmp_path):
    fl = FailureLog(str(tmp_path / "e.json"))
    fl.record("product", "1", "create", "err")
    fl.record("product", "2", "create", "err")
    fl.clear("product", "1")
    assert fl.count == 1
    assert fl.items[0]["source_id"] == "2"


def test_clear_nonexistent_is_noop(tmp_path):
    fl = FailureLog(str(tmp_path / "e.json"))
    fl.clear("product", "999")  # no error
    assert fl.count == 0


def test_by_type_and_summary(tmp_path):
    fl = FailureLog(str(tmp_path / "e.json"))
    fl.record("product", "1", "create", "e")
    fl.record("collection", "2", "create", "e")
    assert len(fl.by_type("product")) == 1
    assert "1 collection" in fl.summary()
    assert "1 product" in fl.summary()


def test_load_missing_file_is_empty(tmp_path):
    fl = FailureLog.load(str(tmp_path / "nope.json"))
    assert fl.count == 0
    assert "No item failures" in fl.summary()


def test_load_corrupt_file_is_empty(tmp_path):
    p = tmp_path / "bad.json"
    p.write_text("{not json", encoding="utf-8")
    fl = FailureLog.load(str(p))
    assert fl.count == 0
