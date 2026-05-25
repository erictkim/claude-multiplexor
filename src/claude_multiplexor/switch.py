"""Switch the user's real terminal to a tmux pane.

Pure functions over `subprocess.run` so the whole thing is unit-testable by
monkeypatching `_run`.
"""
from __future__ import annotations

import shutil
import subprocess
from dataclasses import dataclass
from typing import Callable

TMUX_BIN = shutil.which("tmux") or "tmux"
OSASCRIPT_BIN = shutil.which("osascript") or "osascript"

# Map tmux client-termname / TERM_PROGRAM hints to macOS bundle IDs.
TERM_TO_BUNDLE: dict[str, str] = {
    "iTerm.app": "com.googlecode.iterm2",
    "iterm": "com.googlecode.iterm2",
    "iterm2": "com.googlecode.iterm2",
    "Apple_Terminal": "com.apple.Terminal",
    "Terminal": "com.apple.Terminal",
    "WezTerm": "com.github.wez.wezterm",
    "wezterm": "com.github.wez.wezterm",
    "Alacritty": "org.alacritty",
    "alacritty": "org.alacritty",
    "kitty": "net.kovidgoyal.kitty",
    "ghostty": "com.mitchellh.ghostty",
    "Ghostty": "com.mitchellh.ghostty",
}


@dataclass
class SwitchTarget:
    socket: str
    session: str
    window: str
    pane: str


class SwitchError(Exception):
    """Raised when tmux/osascript cannot complete the switch."""


def _run(cmd: list[str], *, timeout: float = 2.0) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )


def _tmux(socket: str, *args: str, runner: Callable[..., subprocess.CompletedProcess[str]] = _run) -> subprocess.CompletedProcess[str]:
    return runner([TMUX_BIN, "-S", socket, *args])


def detect_term_bundle(
    socket: str,
    *,
    runner: Callable[..., subprocess.CompletedProcess[str]] = _run,
) -> str | None:
    """Return the macOS bundle ID for the terminal hosting the tmux client, or None."""
    proc = _tmux(
        socket,
        "list-clients",
        "-F",
        "#{client_termname}|#{client_tty}|#{client_pid}",
        runner=runner,
    )
    if proc.returncode != 0:
        return None
    for line in proc.stdout.splitlines():
        termname, _, _ = line.partition("|")
        termname = termname.strip()
        if not termname:
            continue
        for key, bundle in TERM_TO_BUNDLE.items():
            if key.lower() in termname.lower():
                return bundle
    return None


def activate_app(
    bundle_id: str,
    *,
    runner: Callable[..., subprocess.CompletedProcess[str]] = _run,
) -> bool:
    proc = runner(
        [OSASCRIPT_BIN, "-e", f'tell application id "{bundle_id}" to activate']
    )
    return proc.returncode == 0


def activate_frontmost_terminal(
    *,
    runner: Callable[..., subprocess.CompletedProcess[str]] = _run,
) -> bool:
    """Best-effort fallback: activate whichever common terminal app exists."""
    for bundle in (
        "com.googlecode.iterm2",
        "com.mitchellh.ghostty",
        "com.github.wez.wezterm",
        "net.kovidgoyal.kitty",
        "org.alacritty",
        "com.apple.Terminal",
    ):
        if activate_app(bundle, runner=runner):
            return True
    return False


def switch_to(
    target: SwitchTarget,
    *,
    runner: Callable[..., subprocess.CompletedProcess[str]] = _run,
) -> None:
    """Focus a tmux pane and bring its terminal app to front.

    Raises SwitchError if any tmux step fails. Terminal activation failure is
    non-fatal (pane is still focused for next attach), but logged via the
    exception message only when activation cannot find any known app.
    """
    if not (target.socket and target.session and target.window and target.pane):
        raise SwitchError("missing tmux coordinates")

    win_target = f"{target.session}:{target.window}"
    # Order matters: select-pane first (cheap, validates pane exists), then
    # select-window (cheap), then switch-client (moves attached client).
    for args in (
        ("select-pane", "-t", target.pane),
        ("select-window", "-t", win_target),
        ("switch-client", "-t", win_target),
    ):
        proc = _tmux(target.socket, *args, runner=runner)
        if proc.returncode != 0:
            err = (proc.stderr or proc.stdout or "").strip() or f"tmux {args[0]} failed"
            raise SwitchError(err)

    bundle = detect_term_bundle(target.socket, runner=runner)
    if bundle:
        activate_app(bundle, runner=runner)
    else:
        activate_frontmost_terminal(runner=runner)
