# claude-multiplexor

Local dashboard for switching between Claude Code CLI sessions running in
tmux. Click a card in the browser → your terminal pops to the front with the
right tmux pane already focused.

## What it does

- Lists every running `claude` session with live status (busy / waiting-input /
  waiting-permission / ready / background / stale / error).
- Shows the latest user prompt as a summary.
- Tracks background tool_use lifecycle from the transcript jsonl: cards flip to
  a `background` pill while a task is pending and show the latest label.
- Lets you label sessions from inside the CLI: `/name my-session` or
  `/rename my-session`. Names persist across dashboard restarts.
- **Click a card (or hit `1`–`9`)** → either attaches the session in an
  embedded xterm.js pane (default) or runs `tmux switch-client` to bring the
  external terminal to front. Typical end-to-end ~50 ms.

## ⚠️ Security

**Local use only. No authentication.** The server binds to `127.0.0.1` by
default and ships with no auth, no TLS, and no access controls. The
`/ws/embed` WebSocket attaches a live tmux pty — anyone who can reach the
port can read and write to your `claude` sessions and execute arbitrary
shell commands as your user.

Do **not**:

- Bind to a non-loopback address (`--host 0.0.0.0`, public IPs, LAN
  interfaces).
- Expose the port through ngrok, Cloudflare Tunnel, SSH `-R`, reverse proxies,
  or container port-forwards to anything other than your own loopback.
- Run it on a shared machine where other users you don't trust have local
  network access.

If you need remote access, terminate auth + TLS in front of it yourself
(e.g. SSH local-forward `-L 8765:127.0.0.1:8765`) — this project will not
do it for you.

## Requirements

- tmux. You must run `claude` inside a tmux pane.
- Python 3.10+.
- Optional: a running Redis daemon. The hook prefers a Redis Stream transport
  but falls back to HTTP automatically when Redis is unreachable, so a daemon
  is not required. (The `redis` Python client is installed as a dependency
  regardless.)

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

### Starting more sessions

You only need one tmux session running. To add another `claude` instance,
press the tmux prefix (default `Ctrl-b`) then `c` to open a new window,
`cd` into the project you want, and run `claude`. A second card will appear
on the dashboard. Repeat for as many as you want — `Ctrl-b n` / `Ctrl-b p`
to cycle windows in the terminal, or just click the card.

To remove hooks later: `claude-mux uninstall-hooks`.

## Usage

- **Click a card** to attach the session in the embedded terminal pane (right
  side). Toggle to external-terminal mode in the header to switch the local
  tmux client + activate the host terminal app instead.
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
                                                                ├─ transcript jsonl scan (bg tool_use)
                                                                ├─ SSE /events ──> web UI
                                                                ├─ WS /ws/embed ──> xterm.js (grouped tmux)
                                                                └─ POST /switch/{sid}
                                                                       │
                                                                       └─ tmux select-pane/window
                                                                          tmux switch-client
                                                                          (macOS: osascript activate <app>)
```

`switch.py` runs `tmux select-pane`/`select-window`/`switch-client` to focus
the right pane. On macOS it additionally activates the host terminal app via
`osascript` — it detects the app via `tmux list-clients -F
'#{client_termname}'` and maps to a bundle ID (iTerm2, Terminal.app, WezTerm,
Alacritty, kitty, Ghostty). On other OSes the tmux-side switch happens; the
host terminal is not auto-raised.

## Tests

```sh
.venv/bin/pytest
```
