"""Claude Code hook -> claude-multiplexor bridge.

Invoked by `claude` as a `type: command` hook. Reads the hook payload as JSON
on stdin, augments it with tmux context, and pushes to either a Redis Stream
or an HTTP fallback endpoint on the dashboard server.

UserPromptSubmit also handles `/name <text>` and `/rename <text>` slash commands
by intercepting the prompt — sets the dashboard display name and blocks the
prompt so the model doesn't run a fake command.

Usage:
    claude-mux-hook <event_name>

Env vars:
    CLAUDE_MUX_REDIS_URL       default redis://127.0.0.1:6379/0
    CLAUDE_MUX_REDIS_STREAM    default claude:mux:hooks
    CLAUDE_MUX_STREAM_MAXLEN   default 10000
    CLAUDE_MUX_HTTP_URL        default http://127.0.0.1:8765 (used if Redis fails)
    CLAUDE_MUX_REDIS           "0" to skip Redis entirely
"""
from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from typing import Any

REDIS_URL = os.environ.get("CLAUDE_MUX_REDIS_URL", "redis://127.0.0.1:6379/0")
REDIS_STREAM = os.environ.get("CLAUDE_MUX_REDIS_STREAM", "claude:mux:hooks")
REDIS_ENABLED = os.environ.get("CLAUDE_MUX_REDIS", "1") != "0"
HTTP_URL = os.environ.get("CLAUDE_MUX_HTTP_URL", "http://127.0.0.1:8765")

NAME_CMD_RE = re.compile(
    r"^<command-name>\s*/?(?P<cmd>name|rename)\s*</command-name>"
    r"(?:\s*<command-message>[^<]*</command-message>)?"
    r"(?:\s*<command-args>(?P<args>[^<]*)</command-args>)?",
    re.IGNORECASE | re.DOTALL,
)
PLAIN_NAME_RE = re.compile(r"^\s*/(?:name|rename)(?:\s+(.*))?\s*$", re.IGNORECASE | re.DOTALL)


def capture_tmux(full: bool) -> dict[str, str]:
    tmux_env = os.environ.get("TMUX", "")
    if not tmux_env:
        return {}
    socket = tmux_env.split(",", 1)[0]
    pane = os.environ.get("TMUX_PANE", "")
    info: dict[str, str] = {"tmux_socket": socket, "tmux_pane": pane}
    if full and pane:
        try:
            out = subprocess.run(
                ["tmux", "-S", socket, "display-message", "-pt", pane, "#S|#I"],
                capture_output=True, text=True, timeout=1.0,
            ).stdout.strip()
            sess, _, win = out.partition("|")
            if sess:
                info["tmux_session"] = sess
            if win:
                info["tmux_window"] = win
        except Exception:
            pass
    return info


def parse_name_intent(raw: str) -> tuple[bool, str] | None:
    """Detect /name or /rename in a UserPromptSubmit payload prompt.

    Returns (is_clear, name) or None if not a name command.
    """
    if not raw:
        return None
    s = raw.lstrip()
    m = NAME_CMD_RE.search(s)
    if m:
        arg = (m.group("args") or "").strip()
        return (arg == "", arg)
    m2 = PLAIN_NAME_RE.match(s)
    if m2:
        arg = (m2.group(1) or "").strip()
        return (arg == "", arg)
    return None


def post_name(session_id: str, name: str) -> None:
    """Best-effort POST to dashboard. Never raises."""
    try:
        import urllib.request
        req = urllib.request.Request(
            f"{HTTP_URL.rstrip('/')}/api/sessions/{session_id}/name",
            data=json.dumps({"name": name}).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        urllib.request.urlopen(req, timeout=1.0).read()
    except Exception:
        pass


def emit_block_for_name(name: str, is_clear: bool) -> None:
    """Print the UserPromptSubmit hook JSON that blocks the prompt."""
    msg = "claude-multiplexor: cleared session name" if is_clear else f"claude-multiplexor: session name set to '{name}'"
    out = {
        "decision": "block",
        "reason": msg,
        "hookSpecificOutput": {
            "hookEventName": "UserPromptSubmit",
            "additionalContext": msg,
        },
    }
    print(json.dumps(out))


def push_event(event: str, payload: dict[str, Any]) -> bool:
    """Try Redis then HTTP. Returns True on success."""
    encoded = json.dumps(payload, ensure_ascii=False)
    if REDIS_ENABLED:
        try:
            import redis
            try:
                maxlen = int(os.environ.get("CLAUDE_MUX_STREAM_MAXLEN", "10000"))
            except ValueError:
                maxlen = 10000
            client = redis.Redis.from_url(
                REDIS_URL, socket_timeout=2.0, socket_connect_timeout=2.0,
            )
            client.xadd(
                REDIS_STREAM,
                {"event": event, "payload": encoded},
                maxlen=maxlen,
                approximate=True,
            )
            return True
        except Exception:
            pass
    # HTTP fallback
    try:
        import urllib.request
        req = urllib.request.Request(
            f"{HTTP_URL.rstrip('/')}/hook/{event}",
            data=encoded.encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        urllib.request.urlopen(req, timeout=1.0).read()
        return True
    except Exception:
        return False


def main(argv: list[str] | None = None) -> int:
    argv = argv if argv is not None else sys.argv
    if len(argv) < 2:
        return 0
    event = argv[1]

    raw = sys.stdin.read()
    if not raw.strip():
        return 0
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return 0
    if not isinstance(payload, dict):
        return 0

    payload.update(capture_tmux(full=(event == "SessionStart")))

    # Intercept /name and /rename before the prompt reaches the model.
    if event == "UserPromptSubmit":
        parsed = parse_name_intent(payload.get("prompt") or "")
        if parsed is not None:
            is_clear, name = parsed
            sid = payload.get("session_id") or ""
            if sid:
                post_name(sid, "" if is_clear else name)
            emit_block_for_name(name, is_clear)
            return 0

    push_event(event, payload)
    return 0


def main_entry() -> None:
    sys.exit(main())


if __name__ == "__main__":
    main_entry()
