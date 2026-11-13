"""CLI : python -m engine.runtime submit|worker|status|cancel|resolve."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import signal
import sys
from typing import Any

from engine.runtime.admission import AdmissionError, IdempotencyConflict
from engine.runtime.engine import RuntimeEngine
from engine.runtime.planner import PlanError
from engine.runtime.settings import load_runtime_settings
from engine.runtime.store import RuntimeStore

EXIT_ERROR = 1
EXIT_ADMISSION = 3  # équivalent 429
EXIT_CONFLICT = 4  # équivalent 409
EXIT_NOT_FOUND = 5


def _emit(payload: dict[str, Any]) -> None:
    print(json.dumps(payload, default=str, sort_keys=True))


def _parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="python -m engine.runtime", description=__doc__)
    p.add_argument("--dsn", help="DSN pg-meta (défaut : variable MDM_META_DSN)")
    p.add_argument(
        "--runtime-config",
        default=os.environ.get("MDM_RUNTIME_CONFIG"),
        help="YAML du runtime (défaut : variable MDM_RUNTIME_CONFIG, sinon valeurs par défaut)",
    )
    p.add_argument("-v", "--verbose", action="store_true")
    sub = p.add_subparsers(dest="command", required=True)

    s = sub.add_parser("submit", help="persiste un run (n'exécute rien)")
    s.add_argument("--config", required=True)
    s.add_argument("--lane", default="etl")
    s.add_argument("--key", help="clé d'idempotence (défaut : générée)")
    s.add_argument("--include-init", action="store_true")
    s.add_argument("--include-export", action="store_true")
    s.add_argument("--export-out")
    s.add_argument("--sources", help="sous-ensemble de sources, séparées par des virgules")
    s.add_argument("--profile")
    s.add_argument("--timeout-seconds", type=int)
    s.add_argument("--max-attempts", type=int)

    w = sub.add_parser("worker", help="exécute la file")
    w.add_argument("--until-idle", action="store_true", help="s'arrête quand la file est vide")
    w.add_argument("--max-seconds", type=float)

    st = sub.add_parser("status", help="état d'un run ou liste des runs")
    st.add_argument("--run-id")
    st.add_argument("--limit", type=int, default=50)

    c = sub.add_parser("cancel", help="demande l'annulation d'un run")
    c.add_argument("--run-id", required=True)

    r = sub.add_parser("resolve", help="clôt un run needs_attention (arrêt confirmé à la main)")
    r.add_argument("--run-id", required=True)
    r.add_argument("--confirm-stopped", action="store_true")
    return p


def _params(args: argparse.Namespace) -> dict[str, Any]:
    params: dict[str, Any] = {}
    if args.include_init:
        params["include_init"] = True
    if args.include_export:
        params["include_export"] = True
    if args.export_out:
        params["export_out"] = args.export_out
    if args.sources:
        params["sources"] = [x.strip() for x in args.sources.split(",") if x.strip()]
    if args.profile:
        params["profile"] = args.profile
    if args.timeout_seconds:
        params["timeout_seconds"] = args.timeout_seconds
    if args.max_attempts:
        params["max_attempts"] = args.max_attempts
    return params


async def _worker(args: argparse.Namespace, engine: RuntimeEngine) -> None:
    loop = asyncio.get_running_loop()
    for name in ("SIGINT", "SIGTERM"):
        try:
            loop.add_signal_handler(getattr(signal, name), engine.request_stop)
        except (NotImplementedError, AttributeError, ValueError):
            signal.signal(
                getattr(signal, name),
                lambda *_: loop.call_soon_threadsafe(engine.request_stop),
            )
    try:
        if args.until_idle:
            await engine.run_until_idle(max_seconds=args.max_seconds)
        else:
            await engine.run_forever()
    finally:
        await engine.stop()


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        stream=sys.stderr,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    try:
        settings = load_runtime_settings(args.runtime_config)
        dsn = args.dsn or os.environ.get("MDM_META_DSN")
        if not dsn:
            _emit({"error": "missing pg-meta DSN (--dsn or MDM_META_DSN)"})
            return EXIT_ERROR
        if args.command == "worker":
            asyncio.run(_worker(args, RuntimeEngine(dsn, settings)))
            _emit({"worker": "stopped"})
            return 0
        with RuntimeStore(dsn, settings) as store:
            store.migrate()
            return _run_command(args, store)
    except AdmissionError as error:
        _emit(
            {"error": "admission_refused", "reason": error.reason, "retry_after": error.retry_after}
        )
        return EXIT_ADMISSION
    except IdempotencyConflict as error:
        _emit({"error": "idempotency_conflict", "reason": str(error)})
        return EXIT_CONFLICT
    except KeyError as error:
        _emit({"error": "not_found", "run_id": str(error.args[0])})
        return EXIT_NOT_FOUND
    except (ValueError, PlanError, TimeoutError, OSError) as error:
        _emit({"error": type(error).__name__, "reason": str(error)})
        return EXIT_ERROR


def _run_command(args: argparse.Namespace, store: RuntimeStore) -> int:
    if args.command == "submit":
        result = store.submit_run(
            args.config, lane=args.lane, idempotency_key=args.key, params=_params(args)
        )
        _emit({"run_id": result.run_id, "created": result.created, "status": result.status})
    elif args.command == "status":
        if args.run_id:
            run = store.get_run(args.run_id)
            if run is None:
                raise KeyError(args.run_id)
            _emit(run)
        else:
            _emit({"runs": store.list_runs(limit=args.limit)})
    elif args.command == "cancel":
        _emit({"run_id": args.run_id, "status": store.cancel_run(args.run_id)})
    elif args.command == "resolve":
        status = store.resolve_attention(args.run_id, confirmed_stopped=args.confirm_stopped)
        _emit({"run_id": args.run_id, "status": status})
    return 0
