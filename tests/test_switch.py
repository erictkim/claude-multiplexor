"""Unit tests for switch.py — monkeypatched subprocess."""
from __future__ import annotations

import subprocess
from dataclasses import dataclass

import pytest

from claude_multiplexor import switch


@dataclass
class FakeProc:
    returncode: int = 0
    stdout: str = ""
    stderr: str = ""


def make_runner(script):
    """`script` maps tuple-of-args -> FakeProc."""
    calls: list[list[str]] = []

    def runner(cmd, *, timeout=2.0):
        calls.append(list(cmd))
        for pattern, proc in script.items():
            if all(p in cmd for p in pattern):
                return proc
        return FakeProc(returncode=0, stdout="")

    runner.calls = calls
    return runner


def test_switch_runs_all_tmux_steps():
    runner = make_runner({
        ("select-pane",): FakeProc(0),
        ("select-window",): FakeProc(0),
        ("switch-client",): FakeProc(0),
        ("list-clients",): FakeProc(0, "iTerm.app|/dev/ttys001|1234\n"),
        ("activate",): FakeProc(0),
    })
    target = switch.SwitchTarget(socket="/tmp/sock", session="main", window="2", pane="%3")
    switch.switch_to(target, runner=runner)
    cmds = [" ".join(c) for c in runner.calls]
    assert any("select-pane" in c for c in cmds)
    assert any("select-window" in c for c in cmds)
    assert any("switch-client" in c for c in cmds)
    # osascript activate hit
    assert any("activate" in c for c in cmds)


def test_switch_raises_on_missing_coords():
    target = switch.SwitchTarget(socket="", session="main", window="0", pane="%1")
    with pytest.raises(switch.SwitchError):
        switch.switch_to(target, runner=make_runner({}))


def test_switch_raises_on_tmux_failure():
    runner = make_runner({
        ("select-pane",): FakeProc(1, stderr="can't find pane"),
    })
    target = switch.SwitchTarget(socket="/tmp/sock", session="main", window="0", pane="%1")
    with pytest.raises(switch.SwitchError, match="can't find pane"):
        switch.switch_to(target, runner=runner)


def test_detect_term_bundle_iterm():
    runner = make_runner({
        ("list-clients",): FakeProc(0, "iTerm.app|/dev/ttys002|9999\n"),
    })
    assert switch.detect_term_bundle("/tmp/sock", runner=runner) == "com.googlecode.iterm2"


def test_detect_term_bundle_unknown():
    runner = make_runner({
        ("list-clients",): FakeProc(0, "weirdshell|/dev/ttys002|0\n"),
    })
    assert switch.detect_term_bundle("/tmp/sock", runner=runner) is None


def test_detect_term_bundle_kitty():
    runner = make_runner({
        ("list-clients",): FakeProc(0, "xterm-kitty|/dev/ttys002|1\n"),
    })
    assert switch.detect_term_bundle("/tmp/sock", runner=runner) == "net.kovidgoyal.kitty"
