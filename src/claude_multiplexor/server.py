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
import json
import logging
import os
import re
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

PROJECTS_DIR = Path.home() / ".claude" / "projects"

from fastapi import FastAPI, HTTPException, Request
from fastapi.staticfiles import StaticFiles
from sse_starlette.sse import EventSourceResponse

from . import naming
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


# --- per-event handlers -----------------------------------------------------

def on_session_start(p: dict[str, Any]) -> None:
    s = ensure_session(p["session_id"], p)
    s["status"] = "ready"
    s["error"] = None
    refresh_agent_name(s)


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
    # Built-in `/rename` is a local command that the CLI handles before any prompt
    # reaches the model — that path arrives here as a system-reminder and is dropped
    # by clean_prompt. Sync display name from the transcript on every prompt so the
    # card label updates within one SSE tick of `/rename` running.
    refresh_agent_name(s)


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
    global _names_cache
    _names_cache = naming.load()
    tasks = [asyncio.create_task(scanner())]
    if REDIS_ENABLED:
        tasks.append(asyncio.create_task(redis_consumer()))
    try:
        yield
    finally:
        for t in tasks:
            t.cancel()


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


STATIC_DIR = Path(__file__).resolve().parent.parent.parent / "static"
if STATIC_DIR.is_dir():
    app.mount("/", StaticFiles(directory=str(STATIC_DIR), html=True), name="static")
