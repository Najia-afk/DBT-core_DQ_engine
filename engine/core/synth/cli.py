"""CLI : python -m engine.core.synth {generate,next-day,config,stage,load-pg}."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

from .generator import generate, generate_delta, stage_live
from .params import DEFAULT_TYPE_MIX, DeltaParams, SynthParams
from .plan import write_config


def _csv(value: str) -> tuple[str, ...]:
    return tuple(v.strip() for v in value.split(",") if v.strip())


def _type_mix(value: str) -> dict[str, float]:
    mix = {}
    for part in value.split(","):
        name, _, share = part.partition("=")
        mix[name.strip()] = float(share)
    return mix


def _add_params(p: argparse.ArgumentParser) -> None:
    d = SynthParams()
    p.add_argument("--nodes", type=int, default=d.nodes, help="entités golden (vérité)")
    p.add_argument("--attributes", type=int, default=d.attributes)
    p.add_argument("--sources", type=int, default=d.sources)
    p.add_argument("--seed", type=int, default=d.seed)
    p.add_argument("--referential", default=d.referential)
    p.add_argument("--kinds", type=_csv, default=(), help="ex. postgres,parquet,postgres")
    p.add_argument("--snapshots", type=_csv, default=(), help="ex. full,full,delta")
    mix = ",".join(f"{k}={v}" for k, v in DEFAULT_TYPE_MIX.items())
    p.add_argument("--type-mix", type=_type_mix, default=dict(DEFAULT_TYPE_MIX), help=mix)
    for name in (
        "overlap", "dup_rate", "noise_rate", "dup_noise_rate", "null_rate", "nullable_share",
        "violation_rate", "conflict_rate", "contradiction_rate", "check_share", "coverage",
        "most_recent_share", "attr_change_rate",
    ):  # fmt: skip
        p.add_argument(f"--{name.replace('_', '-')}", type=float, default=getattr(d, name))
    p.add_argument("--part-rows", type=int, default=d.part_rows)
    p.add_argument("--compression", default=d.compression)
    p.add_argument("--workers", type=int, default=0, help="0 = auto (threads machine, max 32)")
    p.add_argument("--pg-schema", default=d.pg_schema)
    p.add_argument("--start-date", default=d.start_date)


def _add_delta(p: argparse.ArgumentParser) -> None:
    d = DeltaParams()
    p.add_argument("--update-pct", type=float, default=d.update_pct)
    p.add_argument("--new-pct", type=float, default=d.new_pct)
    p.add_argument("--delete-pct", type=float, default=d.delete_pct)
    p.add_argument("--broken-pct", type=float, default=d.broken_pct)
    p.add_argument("--merge-pct", type=float, default=d.merge_pct)
    p.add_argument("--split-pct", type=float, default=d.split_pct)
    p.add_argument("--new-existing-share", type=float, default=d.new_existing_share)


def _params(a: argparse.Namespace) -> SynthParams:
    return SynthParams(
        nodes=a.nodes,
        attributes=a.attributes,
        sources=a.sources,
        seed=a.seed,
        referential=a.referential,
        kinds=a.kinds,
        snapshots=a.snapshots,
        type_mix=a.type_mix,
        overlap=a.overlap,
        dup_rate=a.dup_rate,
        noise_rate=a.noise_rate,
        dup_noise_rate=a.dup_noise_rate,
        null_rate=a.null_rate,
        nullable_share=a.nullable_share,
        violation_rate=a.violation_rate,
        conflict_rate=a.conflict_rate,
        contradiction_rate=a.contradiction_rate,
        check_share=a.check_share,
        coverage=a.coverage,
        most_recent_share=a.most_recent_share,
        attr_change_rate=a.attr_change_rate,
        part_rows=a.part_rows,
        compression=a.compression,
        workers=a.workers,
        pg_schema=a.pg_schema,
        start_date=a.start_date,
    )


def _delta(a: argparse.Namespace) -> DeltaParams:
    return DeltaParams(
        update_pct=a.update_pct,
        new_pct=a.new_pct,
        delete_pct=a.delete_pct,
        broken_pct=a.broken_pct,
        merge_pct=a.merge_pct,
        split_pct=a.split_pct,
        new_existing_share=a.new_existing_share,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m engine.core.synth", description=__doc__)
    sub = parser.add_subparsers(dest="cmd", required=True)

    g = sub.add_parser("generate", help="jour 0 puis --days deltas")
    _add_params(g)
    _add_delta(g)
    g.add_argument("--out", required=True)
    g.add_argument("--days", type=int, default=0)
    g.add_argument("--force", action="store_true", help="écrase un jeu existant")

    n = sub.add_parser("next-day", help="ajoute un jour delta à un jeu existant")
    _add_delta(n)
    n.add_argument("--out", required=True)
    n.add_argument("--workers", type=int, default=0)

    c = sub.add_parser("config", help="écrit uniquement la config YAML")
    _add_params(c)
    c.add_argument("--out", required=True, help="fichier YAML")
    c.add_argument("--data-root", default=None)

    s = sub.add_parser("stage", help="expose un jour aux sources parquet (live/)")
    s.add_argument("--out", required=True)
    s.add_argument("--day", type=int, required=True)

    pg = sub.add_parser("load-pg", help="charge un jour dans PostgreSQL")
    pg.add_argument("--dataset", required=True)
    pg.add_argument("--dsn", default=None, help="sinon variable --dsn-env")
    pg.add_argument("--dsn-env", default="MDM_SYNTH_PG_DSN")
    pg.add_argument("--day", type=int, default=0)
    pg.add_argument("--sources", type=_csv, default=None)
    pg.add_argument("--method", choices=["copy", "duckdb"], default="copy")
    pg.add_argument("--workers", type=int, default=8)
    pg.add_argument("--delta-mode", choices=["upsert", "append", "replace"], default="upsert")
    pg.add_argument("--replace-mode", choices=["swap", "truncate"], default="swap")
    pg.add_argument("--pk", action="store_true", help="clé primaire sur la colonne clé")
    pg.add_argument("--unlogged", action="store_true")
    pg.add_argument(
        "--analyze", action="store_true", help="ANALYZE après chargement (lent : 200 colonnes)"
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    summary: dict[str, Any]
    if args.cmd == "generate":
        summary = generate(
            _params(args), args.out, days=args.days, delta=_delta(args), force=args.force
        )
    elif args.cmd == "next-day":
        summary = generate_delta(args.out, None, _delta(args))
    elif args.cmd == "config":
        cfg = write_config(_params(args), args.out, data_root=args.data_root)
        summary = {"config": str(Path(args.out)), "attributes": len(cfg.attributes)}
    elif args.cmd == "stage":
        summary = stage_live(args.out, args.day)
    else:
        from .pg import load_postgres

        dsn = args.dsn or os.environ.get(args.dsn_env)
        if not dsn:
            print(f"error: no --dsn and env {args.dsn_env} unset", file=sys.stderr)
            return 2
        summary = load_postgres(
            None,
            dsn,
            args.dataset,
            day=args.day,
            sources=list(args.sources) if args.sources else None,
            method=args.method,
            workers=args.workers,
            delta_mode=args.delta_mode,
            replace_mode=args.replace_mode,
            primary_key=args.pk,
            unlogged=args.unlogged,
            analyze=args.analyze,
        )
    print(json.dumps(summary, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
