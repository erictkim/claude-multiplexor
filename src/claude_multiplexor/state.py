"""Persistent dashboard session state.

Survives server restarts so the browser can re-attach to existing tmux panes
without waiting for the next hook event. Validation on load drops entries
whose underlying tmux pane is gone.
"""
from __future__ import annotations

import json
import os
import subprocess
import threading
from pathlib import Path
from typing import Any

STORE_DIR = Path(
    os.environ.get(
        "CLAUDE_MUX_HOME",
        str(Path.home() / ".claude" / "claude-multiplexor"),
    )
)
STORE_PATH = STORE_DIR / "sessions.json"
_lock = threading.Lock()


def load() -> dict[str, dict[str, Any]]:
    with _lock:
        try:
            data = json.loads(STORE_PATH.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return {}
        except (json.JSONDecodeError, OSError):
            return {}
    if not isinstance(data, dict):
        return {}
    return {
        sid: payload
        for sid, payload in data.items()
        if isinstance(sid, str) and isinstance(payload, dict)
    }


def save(sessions: dict[str, dict[str, Any]]) -> None:
    with _lock:
        STORE_DIR.mkdir(parents=True, exist_ok=True)
        tmp = STORE_PATH.with_suffix(".json.tmp")
        tmp.write_text(
            json.dumps(sessions, ensure_ascii=False), encoding="utf-8"
        )
        tmp.replace(STORE_PATH)


def tmux_pane_alive(
    tmux_bin: str, socket: str, session: str, window: str, pane: str
) -> bool:
    """Return True if the named pane is still attached to the tmux server."""
    if not (tmux_bin and socket and session and window and pane):
        return False
    target = f"{session}:{window}.{pane}"
    try:
        r = subprocess.run(
            [tmux_bin, "-S", socket, "display-message", "-pt", target, "ok"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=2.0,
        )
    except (subprocess.TimeoutExpired, OSError):
        return False
    return r.returncode == 0
