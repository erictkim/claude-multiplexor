"""Persistent session-name store.

Names survive dashboard restarts and are re-applied when a known session_id
reappears via SessionStart.
"""
from __future__ import annotations

import json
import os
import re
import threading
from pathlib import Path

STORE_DIR = Path(
    os.environ.get(
        "CLAUDE_MUX_HOME",
        str(Path.home() / ".claude" / "claude-multiplexor"),
    )
)
STORE_PATH = STORE_DIR / "names.json"

_NAME_RE = re.compile(r"^/\s*(?:name|rename)(?:\s+(.*))?$", re.IGNORECASE | re.DOTALL)
_lock = threading.Lock()


def parse_name_command(raw: str) -> tuple[bool, str] | None:
    """Return (is_clear, name) if `raw` is a /name or /rename command, else None.

    `is_clear` is True when no argument is supplied.
    """
    if not raw:
        return None
    m = _NAME_RE.match(raw.strip())
    if not m:
        return None
    arg = (m.group(1) or "").strip()
    return (arg == "", arg)


def _load_unsafe() -> dict[str, str]:
    try:
        return json.loads(STORE_PATH.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {}
    except (json.JSONDecodeError, OSError):
        return {}


def load() -> dict[str, str]:
    with _lock:
        return _load_unsafe()


def save(names: dict[str, str]) -> None:
    with _lock:
        STORE_DIR.mkdir(parents=True, exist_ok=True)
        tmp = STORE_PATH.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(names, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(STORE_PATH)


def set_name(session_id: str, name: str) -> dict[str, str]:
    """Persist or clear a name. Empty `name` clears the entry. Returns full map."""
    with _lock:
        names = _load_unsafe()
        if name:
            names[session_id] = name[:80]
        else:
            names.pop(session_id, None)
        STORE_DIR.mkdir(parents=True, exist_ok=True)
        tmp = STORE_PATH.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(names, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(STORE_PATH)
        return dict(names)
