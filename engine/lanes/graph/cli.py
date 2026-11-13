"""CLI of the graph lane (contract section 6): one JSON summary line on stdout."""

from __future__ import annotations

import argparse
import json
import sys
import time
from typing import Any

from .db import LaneError, load_cfg, state_dsn


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m engine.lanes.graph", description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    for name in ("init", "ingest", "reconcile", "export", "reset"):
        p = sub.add_parser(name)
        p.add_argument("--config", required=True)
        if name == "ingest":
            p.add_argument("--source", required=True)
        p.add_argument("--run-id", required=name in ("ingest", "reconcile"))
        if name == "reconcile":
            p.add_argument(
                "--components",
                choices=("hop", "varlen"),
                default="hop",
                help="cluster traversal: one-hop MATCH adjacency (default) or var-length paths",
            )
        if name == "export":
            p.add_argument("--out", required=True)
    return parser


def run(argv: list[str] | None = None) -> dict[str, Any]:
    args = _parser().parse_args(argv)
    cfg = load_cfg(args.config)
    dsn = state_dsn(cfg)
    t0 = time.perf_counter()
    if args.command == "init":
        from .schema import init

        summary: dict[str, Any] = {
            "lane": "graph",
            "command": "init",
            "status": "ok",
            **init(cfg, dsn),
        }
    elif args.command == "reset":
        from .schema import reset

        summary = {"lane": "graph", "command": "reset", "status": "ok", **reset(cfg, dsn)}
    elif args.command == "ingest":
        from .ingest import ingest

        summary = ingest(cfg, args.source, args.run_id, dsn)
    elif args.command == "reconcile":
        from .reconcile import reconcile

        summary = reconcile(cfg, args.run_id, dsn, args.components)
    else:
        from .export import export

        summary = export(cfg, dsn, args.out)
    summary["wall_seconds"] = round(time.perf_counter() - t0, 4)
    return summary


def main(argv: list[str] | None = None) -> int:
    try:
        summary = run(argv)
    except LaneError as error:
        print(json.dumps({"lane": "graph", "status": "error", "error": str(error)}))
        return 2
    except SystemExit:
        raise
    except Exception as error:  # noqa: BLE001 - the contract wants a JSON line + non-zero exit
        print(
            json.dumps(
                {"lane": "graph", "status": "error", "error": f"{type(error).__name__}: {error}"}
            )
        )
        return 1
    print(json.dumps(summary, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
