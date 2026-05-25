"""Event dispatch state-machine tests."""
from __future__ import annotations

import pytest

from claude_multiplexor import server


@pytest.fixture(autouse=True)
def clean_state(monkeypatch, tmp_path):
    server.sessions.clear()
    server._names_cache.clear()
    monkeypatch.setattr(server.naming, "STORE_PATH", tmp_path / "names.json")
    monkeypatch.setattr(server.naming, "STORE_DIR", tmp_path)
    yield
    server.sessions.clear()


def test_session_start_creates_ready():
    server.dispatch_event("SessionStart", {"session_id": "s1", "cwd": "/a/b"})
    assert server.sessions["s1"]["status"] == "ready"
    assert server.sessions["s1"]["cwd"] == "/a/b"


def test_user_prompt_sets_busy_and_summary():
    server.dispatch_event("SessionStart", {"session_id": "s1"})
    server.dispatch_event("UserPromptSubmit", {"session_id": "s1", "prompt": "do the thing"})
    assert server.sessions["s1"]["status"] == "busy"
    assert server.sessions["s1"]["summary"] == "do the thing"


def test_agent_name_pulled_from_transcript(tmp_path, monkeypatch):
    encoded_cwd = "/Users/eric/Projects/foo"
    encoded_dir = tmp_path / encoded_cwd.replace("/", "-")
    encoded_dir.mkdir()
    sid = "s1"
    transcript = encoded_dir / f"{sid}.jsonl"
    transcript.write_text(
        '{"type":"user","content":"hi"}\n'
        f'{{"type":"agent-name","agentName":"pricing-bug","sessionId":"{sid}"}}\n'
    )
    monkeypatch.setattr(server, "PROJECTS_DIR", tmp_path)
    server.dispatch_event("SessionStart", {"session_id": sid, "cwd": encoded_cwd})
    assert server.sessions[sid]["display_name"] == "pricing-bug"


def test_bg_pending_counts_started_minus_completed(tmp_path, monkeypatch):
    encoded_cwd = "/Users/eric/Projects/foo"
    encoded_dir = tmp_path / encoded_cwd.replace("/", "-")
    encoded_dir.mkdir()
    sid = "s1"
    transcript = encoded_dir / f"{sid}.jsonl"
    import json as _json
    lines = [
        _json.dumps({
            "type": "assistant",
            "message": {"content": [
                {"type": "tool_use", "id": "tu_1", "name": "Bash",
                 "input": {"run_in_background": True, "description": "run backtest"}}
            ]},
        }),
        _json.dumps({
            "type": "assistant",
            "message": {"content": [
                {"type": "tool_use", "id": "tu_2", "name": "Bash",
                 "input": {"run_in_background": True, "description": "another"}}
            ]},
        }),
        # tu_1 finished via task-notification
        _json.dumps({
            "type": "user",
            "message": {"content": (
                "<task-notification>\n<tool-use-id>tu_1</tool-use-id>\n"
                "<status>completed</status>\n</task-notification>"
            )},
        }),
    ]
    transcript.write_text("\n".join(lines) + "\n")
    monkeypatch.setattr(server, "PROJECTS_DIR", tmp_path)
    server.dispatch_event("SessionStart", {"session_id": sid, "cwd": encoded_cwd})
    server.dispatch_event("Stop", {"session_id": sid})
    assert server.sessions[sid]["bg_pending"] == 1
    assert server.sessions[sid]["bg_label"] == "another"
    assert server.sessions[sid]["status"] == "background"


def test_bg_pending_clears_status_when_zero(tmp_path, monkeypatch):
    encoded_cwd = "/Users/eric/Projects/foo"
    encoded_dir = tmp_path / encoded_cwd.replace("/", "-")
    encoded_dir.mkdir()
    sid = "s1"
    transcript = encoded_dir / f"{sid}.jsonl"
    import json as _json
    transcript.write_text(
        _json.dumps({
            "type": "assistant",
            "message": {"content": [
                {"type": "tool_use", "id": "tu_1", "name": "Bash",
                 "input": {"run_in_background": True, "description": "x"}}
            ]},
        }) + "\n"
        + _json.dumps({
            "type": "user",
            "message": {"content": (
                "<task-notification><tool-use-id>tu_1</tool-use-id><status>completed</status></task-notification>"
            )},
        }) + "\n"
    )
    monkeypatch.setattr(server, "PROJECTS_DIR", tmp_path)
    server.dispatch_event("SessionStart", {"session_id": sid, "cwd": encoded_cwd})
    server.dispatch_event("Stop", {"session_id": sid})
    assert server.sessions[sid]["bg_pending"] == 0
    assert server.sessions[sid]["status"] == "ready"


def test_agent_name_latest_wins(tmp_path, monkeypatch):
    encoded_cwd = "/Users/eric/Projects/foo"
    encoded_dir = tmp_path / encoded_cwd.replace("/", "-")
    encoded_dir.mkdir()
    sid = "s1"
    transcript = encoded_dir / f"{sid}.jsonl"
    transcript.write_text(
        f'{{"type":"agent-name","agentName":"first","sessionId":"{sid}"}}\n'
        f'{{"type":"agent-name","agentName":"second","sessionId":"{sid}"}}\n'
    )
    monkeypatch.setattr(server, "PROJECTS_DIR", tmp_path)
    server.dispatch_event("SessionStart", {"session_id": sid, "cwd": encoded_cwd})
    assert server.sessions[sid]["display_name"] == "second"


def test_pre_post_tool_cycle():
    server.dispatch_event("SessionStart", {"session_id": "s1"})
    server.dispatch_event("PreToolUse", {"session_id": "s1", "tool_name": "Bash"})
    assert server.sessions["s1"]["status"] == "busy"
    assert server.sessions["s1"]["current_tool"] == "Bash"
    server.dispatch_event("PostToolUse", {"session_id": "s1", "tool_name": "Bash"})
    assert server.sessions["s1"]["current_tool"] is None


def test_ask_user_question_flips_waiting_input():
    server.dispatch_event("SessionStart", {"session_id": "s1"})
    server.dispatch_event("PreToolUse", {"session_id": "s1", "tool_name": "AskUserQuestion"})
    assert server.sessions["s1"]["status"] == "waiting_input"
    server.dispatch_event("PostToolUse", {"session_id": "s1", "tool_name": "AskUserQuestion"})
    assert server.sessions["s1"]["status"] == "busy"


def test_permission_request():
    server.dispatch_event("SessionStart", {"session_id": "s1"})
    server.dispatch_event("PermissionRequest", {"session_id": "s1", "tool_name": "Bash"})
    assert server.sessions["s1"]["status"] == "waiting_permission"


def test_stop_returns_to_ready():
    server.dispatch_event("SessionStart", {"session_id": "s1"})
    server.dispatch_event("UserPromptSubmit", {"session_id": "s1", "prompt": "hi"})
    server.dispatch_event("Stop", {"session_id": "s1"})
    assert server.sessions["s1"]["status"] == "ready"


def test_system_reminder_does_not_overwrite_summary():
    server.dispatch_event("SessionStart", {"session_id": "s1"})
    server.dispatch_event("UserPromptSubmit", {"session_id": "s1", "prompt": "real prompt"})
    server.dispatch_event(
        "UserPromptSubmit",
        {"session_id": "s1", "prompt": "<system-reminder>noise</system-reminder>"},
    )
    assert server.sessions["s1"]["summary"] == "real prompt"


def test_session_end_drops_record():
    server.dispatch_event("SessionStart", {"session_id": "s1"})
    server.dispatch_event("SessionEnd", {"session_id": "s1"})
    assert "s1" not in server.sessions


def test_unknown_event_returns_false():
    assert server.dispatch_event("MysteryEvent", {"session_id": "s1"}) is False


def test_missing_session_id_returns_false():
    assert server.dispatch_event("SessionStart", {}) is False
