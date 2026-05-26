"""Claude Multiplexor dashboard server.

FastAPI app that:
 - consumes Claude Code hook events from a Redis Stream (or HTTP fallback)
 - tracks per-session state in memory
 - broadcasts snapshots over SSE
 - exposes POST /switch/{sid} to focus the real tmux pane + terminal window
 - exposes POST /api/sessions/{sid}/name to set a display name
"""
from __future__ import annotations

import asyncio
import fcntl
import json
import logging
import os
import pty
import re
import shutil
import signal
import struct
import termios
import time
import uuid
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from starlette.websockets import WebSocket, WebSocketDisconnect

PROJECTS_DIR = Path.home() / ".claude" / "projects"
TMUX_BIN = shutil.which("tmux") or "tmux"

from fastapi import FastAPI, HTTPException, Request
from fastapi.staticfiles import StaticFiles
from sse_starlette.sse import EventSourceResponse
from fastapi import WebSocket as FastAPIWebSocket

from . import naming, state
from .switch import SwitchError, SwitchTarget, switch_to

SUMMARY_MAX = 120
STALE_AFTER_SEC = 600
SCAN_INTERVAL_SEC = 5

REDIS_URL = os.environ.get("CLAUDE_MUX_REDIS_URL", "redis://127.0.0.1:6379/0")
REDIS_STREAM = os.environ.get("CLAUDE_MUX_REDIS_STREAM", "claude:mux:hooks")
REDIS_ENABLED = os.environ.get("CLAUDE_MUX_REDIS", "1") != "0"

log = logging.getLogger("claude_multiplexor")

sessions: dict[str, dict[str, Any]] = {}
subscribers: set[asyncio.Queue] = set()
_names_cache: dict[str, str] = {}
_state_dirty: asyncio.Event | None = None
STATE_SAVE_DEBOUNCE_SEC = 0.5


def now() -> float:
    return time.time()


TMUX_FIELDS = ("tmux_socket", "tmux_session", "tmux_window", "tmux_pane")


def transcript_path(session_id: str, cwd: str) -> Path | None:
    """Locate the session's jsonl transcript in ~/.claude/projects/."""
    if not session_id or not cwd:
        return None
    encoded = cwd.replace("/", "-")
    p = PROJECTS_DIR / encoded / f"{session_id}.jsonl"
    return p if p.exists() else None


def read_agent_name(path: Path) -> str | None:
    """Scan jsonl for the most recent agent-name record. Returns None if none."""
    try:
        data = path.read_bytes()
    except OSError:
        return None
    # Scan backwards: latest agent-name wins.
    latest: str | None = None
    for line in data.splitlines():
        if b'"agent-name"' not in line and b'"agentName"' not in line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(obj, dict) and obj.get("type") == "agent-name":
            name = obj.get("agentName")
            if isinstance(name, str):
                latest = name
    return latest


def refresh_agent_name(session: dict[str, Any]) -> bool:
    """If the session's transcript has an agent-name, sync display_name. Returns changed."""
    if session.get("display_name"):
        # If user set name via API already, prefer transcript only if it changed.
        pass
    path = transcript_path(session["session_id"], session.get("cwd", ""))
    if path is None:
        return False
    name = read_agent_name(path)
    if name is None:
        return False
    if session.get("display_name") == name:
        return False
    session["display_name"] = name
    _names_cache[session["session_id"]] = name
    naming.set_name(session["session_id"], name)
    return True


BG_NOTIF_RE = re.compile(
    r"<task-notification>[\s\S]*?<tool-use-id>([^<]+)</tool-use-id>[\s\S]*?</task-notification>"
)


def scan_bg_pending(path: Path) -> tuple[int, str | None]:
    """Count background tool_uses still pending in this jsonl.

    Pending = started (assistant tool_use with input.run_in_background=true)
              minus completed (task-notification with matching tool-use-id).
    Also returns a short label for the most recent pending task (e.g. its
    description) so the UI can show what's running.
    """
    try:
        data = path.read_text(encoding="utf-8", errors="ignore")
    except OSError:
        return 0, None

    # Two passes: (1) collect every tool-use-id ever mentioned by a
    # task-notification — regardless of which transcript message type wraps it
    # (user message, attachment, queue-operation, etc; recent Claude Code
    # writes notifications across all three). (2) collect bg tool_uses,
    # excluding any whose id was already notified-as-completed. This makes the
    # accounting order-independent within the transcript.
    completed: set[str] = set()
    for line in data.splitlines():
        if "task-notification" not in line:
            continue
        for m in BG_NOTIF_RE.finditer(line):
            completed.add(m.group(1).strip())

    started: dict[str, str] = {}  # tool_use_id -> short label
    for line in data.splitlines():
        if '"run_in_background"' not in line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not (isinstance(obj, dict) and obj.get("type") == "assistant"):
            continue
        msg = obj.get("message") or {}
        content = msg.get("content")
        if not isinstance(content, list):
            continue
        for c in content:
            if not isinstance(c, dict):
                continue
            if c.get("type") != "tool_use":
                continue
            inp = c.get("input") or {}
            if not inp.get("run_in_background"):
                continue
            tid = c.get("id")
            if not isinstance(tid, str) or tid in completed:
                continue
            label = (inp.get("description") or c.get("name") or "background")
            started[tid] = str(label)[:60]

    if not started:
        return 0, None
    # Pick label of most-recently-added (insertion order)
    return len(started), next(reversed(started.values()))


def refresh_bg_state(session: dict[str, Any]) -> bool:
    """Update session bg_pending + bg_label. Returns True if anything changed."""
    path = transcript_path(session["session_id"], session.get("cwd", ""))
    if path is None:
        return False
    pending, label = scan_bg_pending(path)
    changed = False
    if session.get("bg_pending") != pending:
        session["bg_pending"] = pending
        changed = True
    if session.get("bg_label") != label:
        session["bg_label"] = label
        changed = True
    # Surface as a status only if the session is otherwise idle.
    base = session.get("base_status") or session.get("status")
    if session.get("status") == "background" and pending == 0:
        session["status"] = base if base and base != "background" else "ready"
        changed = True
    elif pending > 0 and session.get("status") in ("ready",):
        session["base_status"] = session["status"]
        session["status"] = "background"
        changed = True
    return changed


def ensure_session(session_id: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    payload = payload or {}
    cwd = payload.get("cwd")
    s = sessions.get(session_id)
    if s is None:
        s = {
            "session_id": session_id,
            "cwd": cwd or "",
            "status": "ready",
            "summary": "",
            "current_tool": None,
            "pending_msg": None,
            "error": None,
            "started_at": now(),
            "last_event_at": now(),
            "ended_at": None,
            "display_name": _names_cache.get(session_id, ""),
            "bg_pending": 0,
            "bg_label": None,
            "tmux_socket": "",
            "tmux_session": "",
            "tmux_window": "",
            "tmux_pane": "",
        }
        sessions[session_id] = s
    if cwd:
        s["cwd"] = cwd
    for k in TMUX_FIELDS:
        v = payload.get(k)
        if v:
            s[k] = v
    s["last_event_at"] = now()
    return s


def snapshot() -> list[dict[str, Any]]:
    return sorted(sessions.values(), key=lambda s: s["started_at"])


async def broadcast() -> None:
    payload = snapshot()
    dead: list[asyncio.Queue] = []
    for q in subscribers:
        try:
            q.put_nowait(payload)
        except asyncio.QueueFull:
            dead.append(q)
    for q in dead:
        subscribers.discard(q)
    mark_state_dirty()


def mark_state_dirty() -> None:
    """Signal the saver loop that `sessions` needs to be persisted."""
    if _state_dirty is not None:
        _state_dirty.set()


async def state_saver() -> None:
    """Persist `sessions` to disk, debounced. Coalesces bursts of mutations."""
    assert _state_dirty is not None
    loop = asyncio.get_running_loop()
    while True:
        await _state_dirty.wait()
        _state_dirty.clear()
        await asyncio.sleep(STATE_SAVE_DEBOUNCE_SEC)
        try:
            snap = {sid: dict(s) for sid, s in sessions.items()}
            await loop.run_in_executor(None, state.save, snap)
        except Exception as e:
            log.warning("state save failed: %s", e)


def bootstrap_sessions_from_disk() -> None:
    """Load persisted sessions, drop entries whose tmux pane no longer exists.

    Transient runtime fields (status=busy, current_tool, etc.) are reset so
    the dashboard does not show stale "in progress" state for processes that
    are no longer being tracked by live hook events.
    """
    loaded = state.load()
    if not loaded:
        return
    kept = 0
    for sid, payload in loaded.items():
        socket = payload.get("tmux_socket") or ""
        sess = payload.get("tmux_session") or ""
        window = payload.get("tmux_window") or ""
        pane = payload.get("tmux_pane") or ""
        if not state.tmux_pane_alive(TMUX_BIN, socket, sess, window, pane):
            continue
        # Carry forward identity + tmux mapping; reset transient state. The
        # next hook event will replace these with live values.
        payload["current_tool"] = None
        payload["pending_msg"] = None
        payload["error"] = None
        payload.pop("base_status", None)
        if payload.get("status") not in ("ready", "background"):
            payload["status"] = "ready"
        sessions[sid] = payload
        # Re-derive background-task count from the transcript, then agent name.
        refresh_bg_state(payload)
        refresh_agent_name(payload)
        kept += 1
    log.info("bootstrap: restored %d/%d sessions from disk", kept, len(loaded))


# --- per-event handlers -----------------------------------------------------

def on_session_start(p: dict[str, Any]) -> None:
    s = ensure_session(p["session_id"], p)
    s["status"] = "ready"
    s["error"] = None
    refresh_agent_name(s)
    refresh_bg_state(s)


SYSTEM_TAG_PREFIXES = (
    "<task-notification",
    "<system-reminder",
    "<local-command-stdout",
    "<local-command-stderr",
)
COMMAND_RE = re.compile(
    r"<command-name>([^<]+)</command-name>"
    r"(?:\s*<command-message>[^<]*</command-message>)?"
    r"(?:\s*<command-args>([^<]*)</command-args>)?",
    re.DOTALL,
)


def clean_prompt(raw: str) -> str | None:
    if not raw:
        return None
    s = raw.lstrip()
    if any(s.startswith(p) for p in SYSTEM_TAG_PREFIXES):
        return None
    m = COMMAND_RE.search(s)
    if m:
        cmd = m.group(1).strip()
        args = (m.group(2) or "").strip().replace("\n", " ")
        return f"/{cmd.lstrip('/')} {args}".strip()
    return s.replace("\n", " ").strip()


def on_user_prompt(p: dict[str, Any]) -> None:
    s = ensure_session(p["session_id"], p)
    cleaned = clean_prompt(p.get("prompt") or "")
    if cleaned is None:
        return
    s["status"] = "busy"
    s["pending_msg"] = None
    s["summary"] = cleaned[:SUMMARY_MAX] + ("…" if len(cleaned) > SUMMARY_MAX else "")
    # `/rename` and bg task-notification system reminders arrive on the next
    # prompt cycle; refresh both whenever the user submits a prompt.
    refresh_agent_name(s)
    refresh_bg_state(s)


QUESTION_TOOLS = {"AskUserQuestion", "ExitPlanMode"}


def on_pre_tool(p: dict[str, Any]) -> None:
    s = ensure_session(p["session_id"], p)
    tool = p.get("tool_name")
    s["current_tool"] = tool
    if tool in QUESTION_TOOLS:
        s["status"] = "waiting_input"
        s["pending_msg"] = "agent asking question"
    else:
        s["status"] = "busy"


def on_post_tool(p: dict[str, Any]) -> None:
    s = ensure_session(p["session_id"], p)
    tool = p.get("tool_name")
    s["current_tool"] = None
    if tool in QUESTION_TOOLS and s["status"] == "waiting_input":
        s["status"] = "busy"
        s["pending_msg"] = None
    # New bg tool_use lands in jsonl right after PostToolUse fires for the
    # spawning Bash/Agent call — pick it up immediately rather than waiting
    # for the 5s scanner tick.
    if tool in ("Bash", "Agent"):
        refresh_bg_state(s)


def on_permission_request(p: dict[str, Any]) -> None:
    s = ensure_session(p["session_id"], p)
    s["status"] = "waiting_permission"
    s["pending_msg"] = f"{p.get('tool_name', 'tool')} permission"


def on_notification(p: dict[str, Any]) -> None:
    s = ensure_session(p["session_id"], p)
    ntype = (p.get("type") or "").lower()
    msg = p.get("message") or ""
    if "permission" in ntype:
        s["status"] = "waiting_permission"
        s["pending_msg"] = msg


def on_elicitation(p: dict[str, Any]) -> None:
    s = ensure_session(p["session_id"], p)
    s["status"] = "waiting_input"
    s["pending_msg"] = f"MCP {p.get('server_name', '')} asking"


def on_elicitation_result(p: dict[str, Any]) -> None:
    s = ensure_session(p["session_id"], p)
    if s["status"] == "waiting_input":
        s["status"] = "busy"
        s["pending_msg"] = None


def on_stop(p: dict[str, Any]) -> None:
    s = ensure_session(p["session_id"], p)
    s["status"] = "ready"
    s["current_tool"] = None
    s["pending_msg"] = None
    # If background tasks are still running, reflect that in status.
    refresh_bg_state(s)


def on_stop_failure(p: dict[str, Any]) -> None:
    s = ensure_session(p["session_id"], p)
    s["status"] = "error"
    s["error"] = f"{p.get('error_type', 'error')}: {p.get('error_message', '')}"
    s["current_tool"] = None


def on_cwd_changed(p: dict[str, Any]) -> None:
    ensure_session(p["session_id"], p)


def on_session_end(p: dict[str, Any]) -> None:
    sid = p.get("session_id")
    if sid:
        sessions.pop(sid, None)


HANDLERS: dict[str, Any] = {
    "SessionStart": on_session_start,
    "UserPromptSubmit": on_user_prompt,
    "PreToolUse": on_pre_tool,
    "PostToolUse": on_post_tool,
    "PostToolUseFailure": on_post_tool,
    "PermissionRequest": on_permission_request,
    "Notification": on_notification,
    "Stop": on_stop,
    "StopFailure": on_stop_failure,
    "CwdChanged": on_cwd_changed,
    "SessionEnd": on_session_end,
    "Elicitation": on_elicitation,
    "ElicitationResult": on_elicitation_result,
}


def dispatch_event(event_name: str, payload: dict[str, Any]) -> bool:
    handler = HANDLERS.get(event_name)
    if handler is None or "session_id" not in payload:
        return False
    handler(payload)
    return True


# --- background tasks -------------------------------------------------------

async def scanner() -> None:
    while True:
        await asyncio.sleep(SCAN_INTERVAL_SEC)
        changed = False
        cutoff = now() - STALE_AFTER_SEC
        for sid in list(sessions):
            s = sessions[sid]
            if s["status"] == "busy" and s["last_event_at"] < cutoff:
                s["status"] = "stale"
                changed = True
            # Pick up `/rename` activity even on otherwise-idle sessions.
            if refresh_agent_name(s):
                changed = True
            # Track background tool_use lifecycle from transcript jsonl.
            if refresh_bg_state(s):
                changed = True
        if changed:
            await broadcast()


async def redis_consumer() -> None:
    try:
        from redis import asyncio as aioredis
    except ImportError:
        log.warning("redis package not installed; skipping redis consumer")
        return

    backoff = 1.0
    last_id = "$"
    while True:
        try:
            client = aioredis.from_url(REDIS_URL, decode_responses=True)
            await client.ping()
            log.info("redis consumer connected url=%s stream=%s", REDIS_URL, REDIS_STREAM)
            backoff = 1.0
            while True:
                resp = await client.xread({REDIS_STREAM: last_id}, block=0, count=64)
                if not resp:
                    continue
                changed = False
                for _stream, entries in resp:
                    for entry_id, fields in entries:
                        last_id = entry_id
                        event_name = fields.get("event")
                        raw = fields.get("payload") or "{}"
                        try:
                            payload = json.loads(raw)
                        except json.JSONDecodeError:
                            continue
                        if not isinstance(payload, dict):
                            continue
                        if dispatch_event(event_name, payload):
                            changed = True
                if changed:
                    await broadcast()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.warning("redis consumer error: %s (retry in %.1fs)", e, backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30.0)


# --- lifespan & routes ------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    global _names_cache, _state_dirty
    _names_cache = naming.load()
    _state_dirty = asyncio.Event()
    bootstrap_sessions_from_disk()
    tasks = [asyncio.create_task(scanner()), asyncio.create_task(state_saver())]
    if REDIS_ENABLED:
        tasks.append(asyncio.create_task(redis_consumer()))
    try:
        yield
    finally:
        for t in tasks:
            t.cancel()
        # Flush a final snapshot so a graceful shutdown doesn't lose the last
        # mutation that fell inside the debounce window.
        try:
            state.save({sid: dict(s) for sid, s in sessions.items()})
        except Exception as e:
            log.warning("state save on shutdown failed: %s", e)


app = FastAPI(lifespan=lifespan)


@app.post("/hook/{event_name}")
async def hook(event_name: str, request: Request) -> dict[str, str]:
    try:
        payload = await request.json()
    except json.JSONDecodeError:
        return {"status": "bad_json"}
    if not dispatch_event(event_name, payload):
        return {"status": "ignored"}
    await broadcast()
    return {"status": "ok"}


@app.get("/api/sessions")
async def list_sessions() -> list[dict[str, Any]]:
    return snapshot()


@app.post("/api/sessions/{session_id}/name")
async def set_session_name(session_id: str, request: Request) -> dict[str, Any]:
    try:
        body = await request.json()
    except json.JSONDecodeError:
        body = {}
    name = (body.get("name") or "").strip()[:80]
    _names_cache.update(naming.set_name(session_id, name))
    s = sessions.get(session_id)
    if s is not None:
        s["display_name"] = name
        await broadcast()
    return {"ok": True, "display_name": name}


@app.post("/switch/{session_id}")
async def switch_session(session_id: str) -> dict[str, Any]:
    s = sessions.get(session_id)
    if s is None:
        raise HTTPException(status_code=404, detail="unknown session")
    target = SwitchTarget(
        socket=s.get("tmux_socket", ""),
        session=s.get("tmux_session", ""),
        window=s.get("tmux_window", ""),
        pane=s.get("tmux_pane", ""),
    )
    try:
        await asyncio.to_thread(switch_to, target)
    except SwitchError as e:
        raise HTTPException(status_code=409, detail=str(e))
    return {"ok": True}


@app.get("/events")
async def events(request: Request) -> EventSourceResponse:
    queue: asyncio.Queue = asyncio.Queue(maxsize=64)
    subscribers.add(queue)
    queue.put_nowait(snapshot())

    async def gen():
        try:
            while True:
                if await request.is_disconnected():
                    break
                try:
                    data = await asyncio.wait_for(queue.get(), timeout=15.0)
                    yield {"event": "snapshot", "data": json.dumps(data)}
                except asyncio.TimeoutError:
                    yield {"event": "ping", "data": ""}
        finally:
            subscribers.discard(queue)

    return EventSourceResponse(gen())


# --- embedded tmux view (grouped session per browser client) ---------------

def _set_winsize(fd: int, rows: int, cols: int) -> None:
    rows = max(1, min(rows, 500))
    cols = max(1, min(cols, 1000))
    fcntl.ioctl(fd, termios.TIOCSWINSZ, struct.pack("HHHH", rows, cols, 0, 0))


def _spawn_grouped_view(
    socket: str, target: str, window: str | None, cols: int, rows: int
) -> tuple[int, int, str]:
    """Fork a child running `tmux new-session -A -s <view> -t <target>` on a PTY.

    Grouped session means: window operations on the view do NOT affect other
    clients attached to <target>. Killing the view leaves <target> alone.
    """
    view_name = f"mux_{uuid.uuid4().hex[:8]}"
    cols = max(1, min(cols, 1000))
    rows = max(1, min(rows, 500))
    pid, fd = pty.fork()
    if pid == 0:
        try:
            os.environ.pop("TMUX", None)
            args = [
                TMUX_BIN, "-S", socket, "new-session", "-A",
                "-s", view_name, "-t", target,
                "-x", str(cols), "-y", str(rows),
            ]
            os.execvp(args[0], args)
        except Exception:
            os._exit(127)
    return pid, fd, view_name


def _configure_view_sync(
    socket: str, view_name: str, window: str | None, cols: int, rows: int
) -> None:
    """Apply window-size policy + force exact dims synchronously.

    Called right after spawn so the view's first render uses our dims, not the
    smallest other client attached to the target session.
    """
    import subprocess
    cols = max(1, min(cols, 1000))
    rows = max(1, min(rows, 500))
    cmds = [
        ["set-option", "-t", view_name, "window-size", "manual"],
        ["set-option", "-t", view_name, "aggressive-resize", "on"],
        ["resize-window", "-t", view_name, "-x", str(cols), "-y", str(rows)],
    ]
    if window:
        cmds.append(["select-window", "-t", f"{view_name}:{window}"])
    # Retry briefly: tmux server may not have registered the new session yet.
    deadline = time.monotonic() + 1.5
    for args in cmds:
        while True:
            try:
                r = subprocess.run(
                    [TMUX_BIN, "-S", socket, *args],
                    timeout=2.0, check=False,
                    stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
                )
                if r.returncode == 0:
                    break
            except Exception:
                pass
            if time.monotonic() >= deadline:
                break
            time.sleep(0.05)


async def _resize_window(socket: str, view_name: str, cols: int, rows: int) -> None:
    """Force the view's active window to exact dims (requires window-size manual)."""
    cols = max(1, min(cols, 1000))
    rows = max(1, min(rows, 500))
    try:
        proc = await asyncio.create_subprocess_exec(
            TMUX_BIN, "-S", socket, "resize-window",
            "-t", view_name, "-x", str(cols), "-y", str(rows),
            stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL,
        )
        await asyncio.wait_for(proc.wait(), timeout=2.0)
    except Exception:
        pass


async def _tmux_view_cmd(
    socket: str, view_name: str, action: str, lines: int = 1
) -> None:
    """Run a whitelisted copy-mode command against the embedded view.

    Each client's grouped view has its own tmux client, so copy-mode here
    does not affect the user's own attached terminal on the same window.
    """
    if not socket or not view_name:
        return
    lines = max(1, min(int(lines), 200))
    if action == "scroll_up":
        argv = [
            TMUX_BIN, "-S", socket,
            "copy-mode", "-t", view_name, ";",
            "send-keys", "-t", view_name, "-X", "-N", str(lines), "scroll-up",
        ]
    elif action == "scroll_down":
        argv = [
            TMUX_BIN, "-S", socket,
            "copy-mode", "-t", view_name, ";",
            "send-keys", "-t", view_name, "-X", "-N", str(lines), "scroll-down",
        ]
    elif action == "cancel":
        argv = [
            TMUX_BIN, "-S", socket,
            "send-keys", "-t", view_name, "-X", "cancel",
        ]
    else:
        return
    try:
        proc = await asyncio.create_subprocess_exec(
            *argv,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        await asyncio.wait_for(proc.wait(), timeout=2.0)
    except Exception:
        pass


async def _kill_view(socket: str, view_name: str) -> None:
    try:
        proc = await asyncio.create_subprocess_exec(
            TMUX_BIN, "-S", socket, "kill-session", "-t", view_name,
            stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL,
        )
        await asyncio.wait_for(proc.wait(), timeout=2.0)
    except Exception:
        pass


@app.websocket("/ws/embed")
async def embed_ws(ws: FastAPIWebSocket) -> None:
    """Persistent embedded tmux view. Client controls the target session.

    Protocol (client → server, JSON text frames):
      {"type": "switch", "session_id": "<sid>", "cols": N, "rows": N}
      {"type": "resize", "cols": N, "rows": N}
      {"type": "input", "data": "..."}     (or binary frames containing raw bytes)
      {"type": "tmux_cmd", "action": "scroll_up|scroll_down|cancel", "lines": N}
    Server → client: binary frames containing PTY output.
    """
    await ws.accept()

    state: dict[str, Any] = {
        "socket": "",
        "view_name": "",
        "pid": -1,
        "fd": -1,
        "rows": 30,
        "cols": 100,
    }
    loop = asyncio.get_running_loop()
    closed = asyncio.Event()

    async def teardown_current() -> None:
        fd = state["fd"]
        pid = state["pid"]
        view_name = state["view_name"]
        socket_path = state["socket"]
        state["fd"] = -1
        state["pid"] = -1
        state["view_name"] = ""
        if fd >= 0:
            try:
                loop.remove_reader(fd)
            except Exception:
                pass
            try:
                os.close(fd)
            except OSError:
                pass
        if pid > 0:
            try:
                os.kill(pid, signal.SIGTERM)
            except OSError:
                pass
            try:
                await loop.run_in_executor(None, os.waitpid, pid, 0)
            except (ChildProcessError, OSError):
                pass
        if socket_path and view_name:
            await _kill_view(socket_path, view_name)

    def install_reader(fd: int) -> None:
        flags = fcntl.fcntl(fd, fcntl.F_GETFL)
        fcntl.fcntl(fd, fcntl.F_SETFL, flags | os.O_NONBLOCK)

        def on_readable() -> None:
            try:
                data = os.read(fd, 8192)
            except BlockingIOError:
                return
            except OSError:
                data = b""
            if not data:
                try:
                    loop.remove_reader(fd)
                except Exception:
                    pass
                closed.set()
                return
            try:
                asyncio.ensure_future(ws.send_bytes(data))
            except Exception:
                closed.set()

        try:
            loop.add_reader(fd, on_readable)
        except NotImplementedError:
            async def pump() -> None:
                while not closed.is_set():
                    try:
                        data = await loop.run_in_executor(None, os.read, fd, 8192)
                    except OSError:
                        break
                    if not data:
                        break
                    await ws.send_bytes(data)
                closed.set()
            asyncio.create_task(pump())

    async def open_target(sid: str) -> str | None:
        """Spawn a new grouped view for the given session_id. Returns error string."""
        s = sessions.get(sid)
        if s is None:
            return "unknown session"
        socket = s.get("tmux_socket") or ""
        target = s.get("tmux_session") or ""
        window = s.get("tmux_window") or None
        if not socket or not target:
            return "session not in tmux"
        await teardown_current()
        cols, rows = state["cols"], state["rows"]
        log.info("embed open: sid=%s target=%s window=%s dims=%dx%d",
                 sid, target, window, cols, rows)
        pid, fd, view_name = _spawn_grouped_view(socket, target, window, cols, rows)
        state["socket"] = socket
        state["view_name"] = view_name
        state["pid"] = pid
        state["fd"] = fd
        _set_winsize(fd, rows, cols)
        # Configure window-size policy + force exact dims before first render.
        # Done off the event loop because subprocess.run blocks; await ensures
        # client doesn't see stale (truncated) output before policy takes hold.
        await loop.run_in_executor(
            None, _configure_view_sync, socket, view_name, window, cols, rows
        )
        install_reader(fd)

        # Tmux may recompute window size shortly after attach because of
        # grouped-session reconciliation or another client's SIGWINCH. Re-apply
        # our size a few times so the final state is ours.
        async def _reapply() -> None:
            for delay in (0.15, 0.4, 1.0):
                await asyncio.sleep(delay)
                if state["view_name"] != view_name:
                    return
                c, r = state["cols"], state["rows"]
                if state["fd"] >= 0:
                    try:
                        _set_winsize(state["fd"], r, c)
                    except OSError:
                        pass
                await _resize_window(socket, view_name, c, r)
            try:
                proc = await asyncio.create_subprocess_exec(
                    TMUX_BIN, "-S", socket, "refresh-client", "-t", view_name,
                    stdout=asyncio.subprocess.DEVNULL,
                    stderr=asyncio.subprocess.DEVNULL,
                )
                await asyncio.wait_for(proc.wait(), timeout=2.0)
            except Exception:
                pass
        asyncio.create_task(_reapply())
        return None

    async def reader() -> None:
        try:
            while True:
                msg = await ws.receive()
                t = msg.get("type")
                if t == "websocket.disconnect":
                    break
                if "bytes" in msg and msg["bytes"]:
                    if state["fd"] >= 0:
                        os.write(state["fd"], msg["bytes"])
                    continue
                if "text" not in msg or msg["text"] is None:
                    continue
                txt = msg["text"]
                if not txt.startswith("{"):
                    if state["fd"] >= 0:
                        os.write(state["fd"], txt.encode("utf-8", "ignore"))
                    continue
                try:
                    ctl = json.loads(txt)
                except json.JSONDecodeError:
                    continue
                if not isinstance(ctl, dict):
                    continue
                ctype = ctl.get("type")
                if ctype == "switch":
                    if "cols" in ctl and "rows" in ctl:
                        try:
                            state["cols"] = int(ctl["cols"])
                            state["rows"] = int(ctl["rows"])
                        except (TypeError, ValueError):
                            pass
                    err = await open_target(str(ctl.get("session_id", "")))
                    if err is not None:
                        try:
                            await ws.send_text(json.dumps({"type": "error", "error": err}))
                        except Exception:
                            pass
                elif ctype == "resize":
                    try:
                        state["cols"] = int(ctl.get("cols", state["cols"]))
                        state["rows"] = int(ctl.get("rows", state["rows"]))
                        if state["fd"] >= 0:
                            _set_winsize(state["fd"], state["rows"], state["cols"])
                        # window-size manual: pty SIGWINCH alone won't resize
                        # the tmux window. Force it via resize-window too.
                        if state["socket"] and state["view_name"]:
                            asyncio.create_task(_resize_window(
                                state["socket"], state["view_name"],
                                state["cols"], state["rows"],
                            ))
                    except (TypeError, ValueError, OSError):
                        pass
                elif ctype == "input":
                    data = ctl.get("data", "")
                    if state["fd"] >= 0 and data:
                        os.write(state["fd"], data.encode("utf-8", "ignore"))
                elif ctype == "tmux_cmd":
                    action = str(ctl.get("action", ""))
                    try:
                        lines = int(ctl.get("lines", 1))
                    except (TypeError, ValueError):
                        lines = 1
                    if state["socket"] and state["view_name"]:
                        await _tmux_view_cmd(
                            state["socket"], state["view_name"], action, lines,
                        )
        except WebSocketDisconnect:
            pass
        except Exception as e:
            log.warning("embed ws reader error: %s", e)

    reader_task = asyncio.create_task(reader())
    try:
        done_task = asyncio.create_task(closed.wait())
        await asyncio.wait({reader_task, done_task}, return_when=asyncio.FIRST_COMPLETED)
    finally:
        reader_task.cancel()
        await teardown_current()
        try:
            await ws.close()
        except Exception:
            pass


STATIC_DIR = Path(__file__).resolve().parent.parent.parent / "static"
if STATIC_DIR.is_dir():
    app.mount("/", StaticFiles(directory=str(STATIC_DIR), html=True), name="static")
