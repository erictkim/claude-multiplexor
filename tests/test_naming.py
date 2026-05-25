"""Naming store + slash-command parser tests."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from claude_multiplexor import naming
from claude_multiplexor import hook_push


@pytest.fixture
def tmp_store(tmp_path, monkeypatch):
    store_dir = tmp_path / "store"
    monkeypatch.setattr(naming, "STORE_DIR", store_dir)
    monkeypatch.setattr(naming, "STORE_PATH", store_dir / "names.json")
    return store_dir


def test_set_name_persists(tmp_store: Path):
    out = naming.set_name("sid1", "pricing-bug")
    assert out == {"sid1": "pricing-bug"}
    assert json.loads((tmp_store / "names.json").read_text()) == {"sid1": "pricing-bug"}


def test_set_name_clears_on_empty(tmp_store: Path):
    naming.set_name("sid1", "foo")
    out = naming.set_name("sid1", "")
    assert out == {}
    assert json.loads((tmp_store / "names.json").read_text()) == {}


def test_load_missing_returns_empty(tmp_store: Path):
    assert naming.load() == {}


def test_parse_name_command():
    assert naming.parse_name_command("/name foo bar") == (False, "foo bar")
    assert naming.parse_name_command("/rename baz") == (False, "baz")
    assert naming.parse_name_command("/name") == (True, "")
    assert naming.parse_name_command("/Name  Spaced ") == (False, "Spaced")
    assert naming.parse_name_command("hello") is None
    assert naming.parse_name_command("") is None


def test_hook_parse_command_tag_form():
    raw = "<command-name>/name</command-name><command-message>name</command-message><command-args>pricing-bug investigation</command-args>"
    assert hook_push.parse_name_intent(raw) == (False, "pricing-bug investigation")


def test_hook_parse_plain_form():
    assert hook_push.parse_name_intent("/rename my-session") == (False, "my-session")
    assert hook_push.parse_name_intent("/name") == (True, "")
    assert hook_push.parse_name_intent("not a name command") is None
