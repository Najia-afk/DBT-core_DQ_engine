"""CLI de la voie ELT (contrat docs/MDM_CONTRACT.md §6) :

    python -m engine.lanes.elt init      --config C
    python -m engine.lanes.elt ingest    --config C --source S [--source S2 ...] --run-id R
    python -m engine.lanes.elt reconcile --config C --run-id R
    python -m engine.lanes.elt export    --config C --out DIR
    python -m engine.lanes.elt reset     --config C

Une seule ligne JSON de synthèse sur stdout ; code de sortie 1 en cas d'échec (la ligne JSON porte
alors `status: error`). Secrets : variables d'environnement nommées par la config.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from typing import Any

from .lane import Lane


def _parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="python -m engine.lanes.elt", description=__doc__)
    sub = p.add_subparsers(dest="command", required=True)
    for name in ("init", "ingest", "reconcile", "export", "reset"):
        s = sub.add_parser(name)
        s.add_argument("--config", required=True)
        if name == "ingest":
            s.add_argument("--source", action="append", required=True, help="répétable ou liste")
        s.add_argument("--run-id", required=name in {"ingest", "reconcile"})
        if name == "export":
            s.add_argument("--out", required=True)
    return p


def _emit(payload: dict[str, Any]) -> None:
    sys.stdout.write(json.dumps(payload, default=str, sort_keys=True) + "\n")
    sys.stdout.flush()


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        lane = Lane(args.config)
        if args.command == "init":
            result = lane.init()
        elif args.command == "reset":
            result = lane.reset()
        elif args.command == "export":
            result = lane.export(args.out)
        elif args.command == "ingest":
            sources = [s.strip() for item in args.source for s in item.split(",") if s.strip()]
            result = asyncio.run(lane.ingest(sources, args.run_id))
        else:
            result = asyncio.run(lane.reconcile(args.run_id))
    except Exception as error:  # noqa: BLE001 - la synthèse JSON porte l'erreur
        _emit(
            {
                "lane": "elt",
                "command": args.command,
                "status": "error",
                "error": f"{type(error).__name__}: {error}"[:2000],
            }
        )
        return 1
    _emit(result)
    return 0


if __name__ == "__main__":
    sys.exit(main())
