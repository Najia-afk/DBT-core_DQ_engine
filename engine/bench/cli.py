"""CLI : python -m engine.bench {run,ladder,equiv,compare,concurrency,report}."""

from __future__ import annotations

import argparse
import json
import sys
import tempfile
from pathlib import Path

from .dataset import DatasetSpec
from .guards import Limits
from .pg import PgTuning
from .scenario import REPO_ROOT, RunSpec, run_lane_bench
from .steps import LADDER, SMALL_DELTA

DEFAULT_ROOT = Path(tempfile.gettempdir()) / "mdm2-bench"
DEFAULT_OUT = REPO_ROOT / "docs" / "bench"


def _csv(value: str) -> tuple[str, ...]:
    return tuple(v.strip() for v in value.split(",") if v.strip())


def _limits(a: argparse.Namespace) -> Limits:
    return Limits(a.min_free_disk_gb, a.max_step_minutes * 60.0, a.max_total_ram_gb)


def _tuning(a: argparse.Namespace) -> PgTuning:
    return PgTuning(
        memory=a.pg_memory,
        shared_buffers=a.pg_shared_buffers,
        work_mem=a.pg_work_mem,
        max_wal_size=a.pg_max_wal_size,
        synchronous_commit=a.pg_sync_commit,
    )


def _common(p: argparse.ArgumentParser) -> None:
    p.add_argument("--root", type=Path, default=DEFAULT_ROOT, help="données de travail")
    p.add_argument("--out", type=Path, default=DEFAULT_OUT, help="répertoire docs/bench")
    p.add_argument("--ingest-mode", choices=["parallel", "sequential", "native"])
    p.add_argument("--batch-rows", type=int)
    p.add_argument("--elt-memory", default="6GB")
    p.add_argument("--elt-threads", type=int, default=8)
    p.add_argument("--etl-workers", type=int, default=8)
    p.add_argument("--pg-memory", default="12g")
    p.add_argument("--pg-shared-buffers", default="3GB")
    p.add_argument("--pg-work-mem", default="64MB")
    p.add_argument("--pg-max-wal-size", default="16GB")
    p.add_argument("--pg-sync-commit", default="off", choices=["on", "off"])
    p.add_argument("--min-free-disk-gb", type=float, default=25.0)
    p.add_argument("--max-step-minutes", type=float, default=90.0)
    p.add_argument("--max-total-ram-gb", type=float, default=27.0)
    p.add_argument("--keep-dataset", action="store_true")
    p.add_argument("--export", choices=["none", "final", "each"], default="none")
    p.add_argument("--tag", default="")
    p.add_argument("--env", action="append", default=[], help="KEY=VALUE pour les lanes")


def _spec(a: argparse.Namespace, lane: str, dataset: DatasetSpec, name: str) -> RunSpec:
    return RunSpec(
        lane=lane,
        dataset=dataset,
        name=name,
        root=a.root,
        out_dir=a.out / "runs",
        ingest_mode=a.ingest_mode,
        batch_rows=a.batch_rows,
        elt_memory=a.elt_memory,
        elt_threads=a.elt_threads,
        etl_workers=a.etl_workers,
        tuning=_tuning(a),
        limits=_limits(a),
        export=a.export,
        keep_dataset=a.keep_dataset,
        tag=a.tag,
        extra_env=dict(kv.split("=", 1) for kv in a.env),
    )


def _print_result(result: dict) -> int:
    print(json.dumps({k: result[k] for k in ("name", "status", "abort_reason", "totals")}))
    return 0 if result["status"] == "ok" else 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m engine.bench", description=__doc__)
    sub = parser.add_subparsers(dest="cmd", required=True)

    r = sub.add_parser("run", help="une voie, un jeu, des jours")
    _common(r)
    r.add_argument("--lane", required=True, choices=["etl", "elt", "graph"])
    r.add_argument("--nodes", type=int, required=True)
    r.add_argument("--attributes", type=int, required=True)
    r.add_argument("--sources", type=int, default=3)
    r.add_argument("--seed", type=int, default=42)
    r.add_argument("--kinds", type=_csv, default=("parquet",) * 3)
    r.add_argument("--snapshots", type=_csv, default=("full", "full", "delta"))
    r.add_argument("--update-pcts", type=_csv, default=("1", "5"), help="% de mises à jour/jour")
    r.add_argument("--name")

    s = sub.add_parser("ladder", help="une étape nommée de l'échelle : " + ",".join(LADDER))
    _common(s)
    s.add_argument("--lane", required=True, choices=["etl", "elt", "graph"])
    s.add_argument("--step", required=True, choices=sorted(LADDER))
    s.add_argument("--name")

    e = sub.add_parser("equiv", help="équivalence : référence (<=10k) et voie à voie")
    _common(e)
    e.add_argument("--mode", required=True, choices=["reference", "lanes"])
    e.add_argument("--lanes", type=_csv, default=("etl", "elt", "graph"))
    e.add_argument("--step", default="", help="mode lanes : étape (L1, C50...)")
    e.add_argument("--nodes", type=int, default=10_000)
    e.add_argument("--attributes", type=int, default=30)
    e.add_argument("--days", type=int, default=3)

    c = sub.add_parser("compare", help="compare deux répertoires d'export (mémoire DuckDB bornée)")
    c.add_argument("--a", type=Path, required=True)
    c.add_argument("--b", type=Path, required=True)
    c.add_argument("--json", type=Path)
    c.add_argument("--hash", action="store_true", help="compare aussi row_hash")
    c.add_argument("--memory", default="6GB")

    k = sub.add_parser("concurrency", help="3 référentiels via engine.runtime")
    _common(k)
    k.add_argument("--lane", required=True, choices=["etl", "elt", "graph"])
    k.add_argument("--nodes", type=int, default=20_000)
    k.add_argument("--attributes", type=int, default=30)
    k.add_argument("--referentials", type=int, default=3)

    rp = sub.add_parser("report", help="régénère le bloc de tableaux de docs/bench/RESULTS.md")
    rp.add_argument("--out", type=Path, default=DEFAULT_OUT)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.cmd == "run":
        deltas = tuple({**SMALL_DELTA, "update_pct": float(u)} for u in args.update_pcts)
        ds = DatasetSpec(
            nodes=args.nodes, attributes=args.attributes, sources=args.sources, seed=args.seed,
            kinds=args.kinds, snapshots=args.snapshots, deltas=deltas,
        )  # fmt: skip
        name = args.name or f"{args.lane}-{args.nodes // 1000}k-{args.attributes}a"
        return _print_result(run_lane_bench(_spec(args, args.lane, ds, name)))
    if args.cmd == "ladder":
        step = LADDER[args.step]
        name = args.name or f"{args.lane}-{step.name}"
        return _print_result(run_lane_bench(_spec(args, args.lane, step.dataset(), name)))
    if args.cmd == "compare":
        from .equiv import compare_dirs

        return compare_dirs(args.a, args.b, args.json, args.hash, args.memory)
    if args.cmd == "equiv":
        from .equiv import run_equivalence

        return run_equivalence(args, _spec)
    if args.cmd == "concurrency":
        from .concurrency import run_concurrency

        return run_concurrency(args)
    from .report import write_results

    write_results(args.out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
