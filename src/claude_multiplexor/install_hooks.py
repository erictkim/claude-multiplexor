"""Idempotent installer: merge claude-multiplexor hooks into ~/.claude/settings.json.

Re-runnable. Existing multiplexor hooks (marked `_multiplexor: true`) are
replaced; foreign hooks (dashboard, caveman, etc.) are preserved.

Uses `claude-mux-hook` console script (installed by pip), or falls back to
`python -m claude_multiplexor.hook_push` if the script isn't on PATH.
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
from pathlib import Path

SETTINGS = Path.home() / ".claude" / "settings.json"
MARKER = "_multiplexor"
HOOK_TIMEOUT = 5

EVENTS: dict[str, str | None] = {
    "SessionStart": None,
    "UserPromptSubmit": None,
    "PreToolUse": None,
    "PostToolUse": None,
    "PostToolUseFailure": None,
    "PermissionRequest": None,
    "Notification": "idle_prompt|permission_prompt",
    "Stop": None,
    "StopFailure": None,
    "CwdChanged": None,
    "SessionEnd": None,
    "Elicitation": None,
    "ElicitationResult": None,
}


def hook_command() -> str:
    found = shutil.which("claude-mux-hook")
    if found:
        return found
    return f"{sys.executable} -m claude_multiplexor.hook_push"


def build_hook(event: str, cmd_base: str) -> dict:
    return {
        "type": "command",
        "command": f"{cmd_base} {event}",
        "timeout": HOOK_TIMEOUT,
        MARKER: True,
    }


def is_multiplexor_entry(entry: dict) -> bool:
    hooks = entry.get("hooks", [])
    return any(isinstance(h, dict) and h.get(MARKER) for h in hooks)


def merge(settings: dict, cmd_base: str) -> dict:
    hooks_root = settings.setdefault("hooks", {})
    for event, matcher in EVENTS.items():
        entries = hooks_root.setdefault(event, [])
        entries[:] = [e for e in entries if not is_multiplexor_entry(e)]
        new_entry: dict = {"hooks": [build_hook(event, cmd_base)]}
        if matcher is not None:
            new_entry["matcher"] = matcher
        entries.append(new_entry)
    return settings


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Install claude-multiplexor hooks.")
    ap.add_argument(
        "--settings",
        type=Path,
        default=SETTINGS,
        help="Path to settings.json (default: ~/.claude/settings.json)",
    )
    ap.add_argument(
        "--uninstall",
        action="store_true",
        help="Remove multiplexor hooks instead of installing.",
    )
    args = ap.parse_args(argv)

    settings_path: Path = args.settings
    if not settings_path.exists():
        print(f"error: {settings_path} does not exist", file=sys.stderr)
        return 1

    cmd_base = hook_command()

    backup = settings_path.with_suffix(settings_path.suffix + ".pre-multiplexor.bak")
    if not backup.exists():
        shutil.copy2(settings_path, backup)
        print(f"backup: {backup}")

    settings = json.loads(settings_path.read_text())

    if args.uninstall:
        hooks_root = settings.get("hooks", {})
        for event in list(hooks_root):
            entries = hooks_root[event]
            entries[:] = [e for e in entries if not is_multiplexor_entry(e)]
            if not entries:
                del hooks_root[event]
        action = "uninstalled"
    else:
        merge(settings, cmd_base)
        action = "installed"

    tmp = settings_path.with_suffix(settings_path.suffix + ".tmp")
    tmp.write_text(json.dumps(settings, indent=2) + "\n")
    os.replace(tmp, settings_path)
    print(f"{action} multiplexor hooks in {settings_path}")
    if not args.uninstall:
        print(f"hook command: {cmd_base}")
        print(f"events: {', '.join(EVENTS)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
