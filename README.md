# claude-multiplexor

Local dashboard for switching between Claude Code CLI sessions running in
tmux. Click a card in the browser → your terminal pops to the front with the
right tmux pane already focused.

## What it does

- Lists every running `claude` session with live status (busy / waiting-input /
  waiting-permission / ready / stale / error).
- Shows the latest user prompt as a summary.
- Lets you label sessions from inside the CLI: `/name pricing-bug` or
  `/rename pricing-bug`. Names persist across dashboard restarts.
- **Click a card (or hit `1`–`9`)** → runs `tmux switch-client` and activates
  the terminal app holding the tmux client. Typical end-to-end ~50 ms.

## Requirements

- macOS (uses `osascript` to activate terminal apps).
- tmux. You must run `claude` inside a tmux pane.
- Python 3.10+.
- Optional: Redis (used as the hook → server transport; HTTP fallback is
  automatic).

## Install

```sh
git clone <this repo> && cd claude-multiplexor
python3 -m venv .venv
.venv/bin/pip install -e .
```

Register hooks (idempotent, backs up your settings.json once):

```sh
.venv/bin/claude-mux install-hooks
```

## Run

```sh
.venv/bin/claude-mux serve --port 8765
```

Open `http://127.0.0.1:8765/`. Start `claude` in a tmux pane — card appears.

To remove hooks later: `claude-mux uninstall-hooks`.

## Usage

- **Click a card** to focus its tmux pane and bring the terminal to front.
- **`1`–`9`** quick-switch to Nth session.
- **`j`/`k`** highlight up/down; **Enter** switches.
- **`/name <text>`** in the CLI sets the card label; **`/name`** clears it.
  The hook intercepts the prompt so Claude never sees it.

## Configuration

Env vars (all optional):

| Var | Default | Purpose |
| --- | --- | --- |
| `CLAUDE_MUX_REDIS_URL` | `redis://127.0.0.1:6379/0` | Redis Stream transport. |
| `CLAUDE_MUX_REDIS_STREAM` | `claude:mux:hooks` | Stream key. |
| `CLAUDE_MUX_REDIS` | `1` | Set to `0` to disable Redis entirely (HTTP only). |
| `CLAUDE_MUX_HTTP_URL` | `http://127.0.0.1:8765` | HTTP fallback target. |
| `CLAUDE_MUX_HOME` | `~/.claude/claude-multiplexor` | Where `names.json` lives. |

## Architecture

```
claude CLI ─hook(stdin JSON)─> claude-mux-hook ─Redis|HTTP─> server.py
                                                                │
                                                                ├─ in-mem session map
                                                                ├─ SSE /events ──> web UI
                                                                └─ POST /switch/{sid}
                                                                       │
                                                                       └─ tmux select-pane/window
                                                                          tmux switch-client
                                                                          osascript activate <app>
```

`switch.py` detects which terminal app holds the tmux client via
`tmux list-clients -F '#{client_termname}'` and maps to a bundle ID:
iTerm2, Terminal.app, WezTerm, Alacritty, kitty, Ghostty. Unknown terminals
fall back to a best-effort activation of any common terminal app.

## Tests

```sh
.venv/bin/pytest
```
