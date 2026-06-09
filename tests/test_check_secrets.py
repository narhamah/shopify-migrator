"""Tests for scripts/check_secrets.py secret scanner."""
import importlib.util
import os

_spec = importlib.util.spec_from_file_location(
    "check_secrets",
    os.path.join(os.path.dirname(__file__), "..", "scripts", "check_secrets.py"),
)
check_secrets = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(check_secrets)


def _write(tmp_path, name, content):
    p = tmp_path / name
    p.write_text(content, encoding="utf-8")
    return str(p)


def test_detects_real_shopify_token(tmp_path):
    token = "shpat_" + "a1b2c3d4e5f6g7h8i9j0k1l2"
    p = _write(tmp_path, "leak.env", f"DEST_ACCESS_TOKEN={token}\n")
    assert check_secrets.scan([p])


def test_detects_anthropic_and_openai(tmp_path):
    openai_key = "sk-" + "proj1234567890abcdefghijklmnop"
    anthropic_key = "sk-ant-" + "api03-abcdefghij1234567890"
    p = _write(tmp_path, "k.env",
               f"OPENAI_API_KEY={openai_key}\nANTHROPIC_API_KEY={anthropic_key}\n")
    assert len(check_secrets.scan([p])) == 2


def test_ignores_placeholders(tmp_path):
    p = _write(tmp_path, "example.env",
               "SOURCE_ACCESS_TOKEN=shpat_xxxxxxxxxxxxxxxxxxxxxxxx\nOPENAI_API_KEY=sk-xxxxxxxxxxxxxxxxxxxxxxxx\n")
    assert check_secrets.scan([p]) == []


def test_ignores_env_var_references(tmp_path):
    # A config that only names the env var (not the value) is safe.
    p = _write(tmp_path, "kuwait.toml", 'access_token_env = "DEST_ACCESS_TOKEN"\n')
    assert check_secrets.scan([p]) == []


def test_detects_private_key(tmp_path):
    marker = "-----BEGIN " + "RSA PRIVATE KEY-----"
    p = _write(tmp_path, "id_rsa", f"{marker}\nabc\n")
    assert check_secrets.scan([p])


def test_repo_example_env_is_clean():
    """The tracked .example file uses placeholders and must not trip the scanner."""
    example = os.path.join(os.path.dirname(__file__), "..", "usa-destination.env.example")
    if os.path.exists(example):
        assert check_secrets.scan([example]) == []
