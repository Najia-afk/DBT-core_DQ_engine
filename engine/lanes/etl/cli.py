"""CLI of the ETL lane: init | ingest | reconcile | export | reset (docs/MDM_CONTRACT.md section 6).

One JSON line on stdout; exit code 0 = success, 1 = failure (message on stderr and in the JSON
line), 2 = invalid usage or configuration.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import traceback
from typing import Any

from engine.core.canonical import canonical_run_id
from engine.core.config import ReferentialConfig, load_config

from .db import LaneError, Pg

LANE = "etl"


def peak_rss_mb() -> float:
    try:
        import psutil

        info = psutil.Process().memory_info()
        peak = getattr(info, "peak_wset", None) or getattr(info, "rss", 0)
        return round(peak / (1 << 20), 1)
    except ImportError:  # pragma: no cover
        return 0.0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m engine.lanes.etl", description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    for name in ("init", "ingest", "reconcile", "export", "reset"):
        p = sub.add_parser(name)
        p.add_argument("--config", required=True, help="referential YAML")
        if name == "ingest":
            p.add_argument("--source", required=True)
        p.add_argument("--run-id", required=name in ("ingest", "reconcile"))
        if name == "export":
            p.add_argument("--out", required=True, help="directory receiving <table>.parquet")
    return parser


def execute(command: str, cfg: ReferentialConfig, args: argparse.Namespace) -> dict[str, Any]:
    pg = Pg(cfg)
    try:
        if command == "init":
            from .schema import init

            return {"counts": init(cfg, pg)}
        if command == "reset":
            from .schema import reset

            return {"counts": reset(cfg, pg)}
        if command == "ingest":
            from .ingest import ingest

            counts = ingest(cfg, args.source, canonical_run_id(args.run_id), pg)
            return {"run_id": args.run_id, "source": args.source, "counts": counts}
        if command == "reconcile":
            from .reconcile import reconcile

            counts = reconcile(cfg, canonical_run_id(args.run_id), pg)
            return {"run_id": args.run_id, "counts": counts}
        from .export import export

        return {"counts": export(cfg, pg, args.out)}
    finally:
        pg.close()


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    started = time.perf_counter()
    summary: dict[str, Any] = {"lane": LANE, "command": args.command}
    try:
        cfg = load_config(args.config)
    except ValueError as error:
        summary.update(status="failed", error=str(error))
        print(json.dumps(summary, sort_keys=True))
        print(f"error: {error}", file=sys.stderr)
        return 2
    summary["referential"] = cfg.referential
    code = 0
    try:
        result = execute(args.command, cfg, args)
        counts = result.pop("counts", {})
        seconds = counts.pop("seconds", None) if isinstance(counts, dict) else None
        summary.update(result)
        summary.update(status=counts.get("status", "ok") if isinstance(counts, dict) else "ok")
        summary["counts"] = counts
        if seconds is not None:
            summary["seconds"] = seconds
    except (LaneError, ValueError) as error:
        summary.update(status="failed", error=str(error))
        print(f"error: {error}", file=sys.stderr)
        code = 1
    except Exception as error:
        summary.update(status="failed", error=f"{type(error).__name__}: {error}")
        traceback.print_exc(file=sys.stderr)
        code = 1
    summary["total_seconds"] = round(time.perf_counter() - started, 3)
    summary["peak_rss_mb"] = peak_rss_mb()
    print(json.dumps(summary, sort_keys=True, default=str))
    return code
