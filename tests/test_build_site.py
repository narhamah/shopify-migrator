"""Tests for the build_site orchestrator: fail-fast, truthful exit code, resume."""
import json
import sys
import types

import pytest

import tara_migrate.pipeline.build_site as bs


@pytest.fixture
def env(monkeypatch, tmp_path):
    monkeypatch.setenv("SOURCE_SHOP_URL", "src.myshopify.com")
    monkeypatch.setenv("SOURCE_ACCESS_TOKEN", "shpat_src")
    monkeypatch.setenv("DEST_SHOP_URL", "dst.myshopify.com")
    monkeypatch.setenv("DEST_ACCESS_TOKEN", "shpat_dst")
    monkeypatch.delenv("DEST_NAME", raising=False)
    # Don't hit the network or write log files into a tmp that disappears.
    monkeypatch.setattr(bs, "ShopifyClient", lambda *a, **k: object())
    monkeypatch.setattr(bs, "add_run_log_file", lambda p: None)
    # No-op load_dotenv so a stray repo .env can't override our env.
    monkeypatch.setattr(bs, "load_dotenv", lambda *a, **k: None)
    monkeypatch.chdir(tmp_path)
    return tmp_path


# --- run_subprocess ---

def test_run_subprocess_raises_on_nonzero(monkeypatch):
    monkeypatch.setattr(bs.subprocess, "run", lambda cmd: types.SimpleNamespace(returncode=2))
    with pytest.raises(bs.PhaseError):
        bs.run_subprocess(["x"], "X")


def test_run_subprocess_ok_on_zero(monkeypatch):
    monkeypatch.setattr(bs.subprocess, "run", lambda cmd: types.SimpleNamespace(returncode=0))
    bs.run_subprocess(["x"], "X")  # no raise


# --- orchestrator main() ---

def _register_phase(monkeypatch, num, func):
    monkeypatch.setitem(bs.PHASES, num, (f"Phase{num}", func, "desc", {"en", "ar", "all"}))


def test_main_fail_fast_exits_nonzero(env, monkeypatch):
    def boom(**kw):
        raise RuntimeError("kaboom")
    _register_phase(monkeypatch, 99, boom)
    monkeypatch.setattr(sys, "argv", ["build_site.py", "--phase", "99", "--skip-preflight"])
    with pytest.raises(SystemExit) as ei:
        bs.main()
    assert ei.value.code == 1
    manifest = json.loads((env / "data" / "run_manifest.json").read_text(encoding="utf-8"))
    assert manifest["status"] == "failed"
    assert manifest["phases"]["phase_99"]["status"] == "failed"


def test_main_success_writes_completed_manifest(env, monkeypatch):
    calls = []
    def ok(**kw):
        calls.append(1)
        return {"created": 2}
    _register_phase(monkeypatch, 99, ok)
    monkeypatch.setattr(sys, "argv", ["build_site.py", "--phase", "99", "--skip-preflight"])
    bs.main()  # success path returns normally
    assert calls == [1]
    manifest = json.loads((env / "data" / "run_manifest.json").read_text(encoding="utf-8"))
    assert manifest["status"] == "completed"
    assert manifest["phases"]["phase_99"]["counts"]["created"] == 2


def test_main_resume_skips_completed(env, monkeypatch):
    runs = []
    def ok(**kw):
        runs.append(1)
    _register_phase(monkeypatch, 99, ok)

    monkeypatch.setattr(sys, "argv", ["build_site.py", "--phase", "99", "--skip-preflight"])
    bs.main()
    monkeypatch.setattr(sys, "argv", ["build_site.py", "--phase", "99", "--skip-preflight", "--resume"])
    bs.main()
    assert runs == [1]  # second (resumed) run skipped the completed phase


def test_main_keep_going_continues_then_fails(env, monkeypatch):
    order = []
    def boom(**kw):
        order.append("boom")
        raise RuntimeError("x")
    def ok(**kw):
        order.append("ok")
    _register_phase(monkeypatch, 98, boom)
    _register_phase(monkeypatch, 99, ok)
    monkeypatch.setattr(sys, "argv", ["build_site.py", "--phase", "98,99", "--skip-preflight", "--keep-going"])
    with pytest.raises(SystemExit) as ei:
        bs.main()
    assert ei.value.code == 1          # overall failure
    assert order == ["boom", "ok"]     # but later phase still ran
