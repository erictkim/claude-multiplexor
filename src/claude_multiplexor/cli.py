"""`claude-mux` CLI: serve | install-hooks | uninstall-hooks."""
from __future__ import annotations

import argparse
import sys

from . import install_hooks


def cmd_serve(args: argparse.Namespace) -> int:
    import uvicorn
    uvicorn.run(
        "claude_multiplexor.server:app",
        host=args.host,
        port=args.port,
        log_level=args.log_level,
        reload=args.reload,
    )
    return 0


def cmd_install_hooks(args: argparse.Namespace) -> int:
    return install_hooks.main([])


def cmd_uninstall_hooks(args: argparse.Namespace) -> int:
    return install_hooks.main(["--uninstall"])


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="claude-mux")
    sub = ap.add_subparsers(dest="command", required=True)

    p_serve = sub.add_parser("serve", help="Run the dashboard HTTP server.")
    p_serve.add_argument("--host", default="127.0.0.1")
    p_serve.add_argument("--port", type=int, default=8765)
    p_serve.add_argument("--log-level", default="info")
    p_serve.add_argument("--reload", action="store_true")
    p_serve.set_defaults(func=cmd_serve)

    p_install = sub.add_parser("install-hooks", help="Install hooks into ~/.claude/settings.json.")
    p_install.set_defaults(func=cmd_install_hooks)

    p_uninstall = sub.add_parser("uninstall-hooks", help="Remove claude-multiplexor hooks.")
    p_uninstall.set_defaults(func=cmd_uninstall_hooks)

    args = ap.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
