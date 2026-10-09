"""The local key form has a fixed destination and keeps secrets in stdin."""

from types import SimpleNamespace
from urllib.parse import urlencode

import pytest

from scripts import set_explorer_keys as setup


def test_import_uses_stdin_and_fixed_fly_app(monkeypatch):
    key = "test-key-" + "a" * 30
    calls = []
    def run(args, **kwargs):
        calls.append((args, kwargs))
        return SimpleNamespace(returncode=0)
    monkeypatch.setattr(setup.subprocess, "run", run)
    assert setup.import_keys({"TRONGRID_API_KEY": key})
    args, kwargs = calls[0]
    assert args == ["fly", "secrets", "import", "--app", "ratichat-bot-prod", "--stage"]
    assert key not in " ".join(args) and kwargs["input"] == "TRONGRID_API_KEY=" + key + "\n"
    assert kwargs["capture_output"] and kwargs["timeout"] == 120


@pytest.mark.parametrize("body", [
    b"ADMIN_API_TOKEN=" + b"a" * 30,
    b"TRONGRID_API_KEY=" + b"a" * 30 + b"&TRONGRID_API_KEY=" + b"b" * 30,
    b"TRONGRID_API_KEY=abc%0aINJECTED%3D" + b"a" * 30,
    b"TRONGRID_API_KEY=short",
    b"TRONGRID_API_KEY=&BLOCKSCOUT_API_KEY=",
    b"x" * 4097,
])
def test_key_input_rejects_other_secrets_duplicates_and_line_injection(body):
    with pytest.raises(ValueError):
        setup.parse_keys(body)


def test_one_provider_key_can_be_saved_with_an_empty_other_field():
    key = "test-key-" + "a" * 30
    body = urlencode({"TRONGRID_API_KEY": key, "BLOCKSCOUT_API_KEY": ""}).encode()
    assert setup.parse_keys(body) == {"TRONGRID_API_KEY": key}


@pytest.mark.parametrize("method,path,host,origin,allowed", [
    ("GET", "/private", "127.0.0.1:9000", None, True),
    ("GET", "/wrong", "127.0.0.1:9000", None, False),
    ("GET", "/private", "evil.example:9000", None, False),
    ("POST", "/private", "127.0.0.1:9000", "http://127.0.0.1:9000", True),
    ("POST", "/private", "127.0.0.1:9000", "https://evil.example", False),
    ("POST", "/private", "127.0.0.1:9000", None, False),
])
def test_local_setup_requires_its_private_path_host_and_post_origin(method, path, host, origin, allowed):
    assert setup.request_allowed(method, path, {"Host": host, "Origin": origin}, "private", 9000) is allowed


def test_form_has_password_fields_and_fixed_destination():
    page = setup.form_page("private")
    assert page.count('type="password"') == 2
    assert "ratichat-bot-prod" in page and 'action="/private"' in page
