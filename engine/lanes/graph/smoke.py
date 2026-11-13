"""Scale smoke of the graph lane (one container, synthetic data, measured phases).

    python -m engine.lanes.graph.smoke --nodes 27500 --attributes 30

`--nodes` is the number of synthetic ENTITIES; with 3 sources (overlap 0.6, 2% duplicates) it
yields about 1.85 source records per entity (27 500 entities ~ 50 000 source nodes). Prints one
JSON report; the container (`mdm2-graph-smoke-*`, 6 GB cap) is always removed.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import psutil
import psycopg
import yaml

from . import _docker as dk

ROOT = Path(__file__).resolve().parents[3]


def run_measured(cmd: list[str], env: dict[str, str], work: Path, tag: str) -> dict[str, Any]:
    """Run a lane command; poll the RSS of the process tree; return summary + peak RSS."""
    out_path, err_path = work / f"{tag}.out", work / f"{tag}.err"
    peak = 0
    t0 = time.perf_counter()
    with out_path.open("w") as out, err_path.open("w") as err:
        proc = subprocess.Popen(cmd, stdout=out, stderr=err, env=env, cwd=ROOT)
        ps = psutil.Process(proc.pid)
        while proc.poll() is None:
            try:
                rss = ps.memory_info().rss + sum(
                    c.memory_info().rss for c in ps.children(recursive=True)
                )
                peak = max(peak, rss)
            except psutil.Error:
                pass
            time.sleep(0.05)
    wall = time.perf_counter() - t0
    lines = [ln for ln in out_path.read_text().splitlines() if ln.strip()]
    summary = json.loads(lines[-1]) if lines else {}
    if proc.returncode != 0:
        raise RuntimeError(
            f"{tag} failed ({proc.returncode}): {lines[-1:]} {err_path.read_text()[-500:]}"
        )
    return {"summary": summary, "wall_s": round(wall, 3), "peak_rss_mb": round(peak / 2**20, 1)}


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--nodes", type=int, default=27_500)
    p.add_argument("--attributes", type=int, default=30)
    p.add_argument("--workdir", default="smoke_work")
    p.add_argument("--report", default=None)
    p.add_argument("--keep", action="store_true")
    p.add_argument("--no-day2", action="store_true")
    p.add_argument("--components", default="hop", choices=("hop", "varlen"))
    args = p.parse_args(argv)

    from engine.core.synth import DeltaParams, SynthParams, generate_day0, generate_delta
    from engine.core.synth.generator import stage_live
    from engine.core.synth.pg import load_postgres

    work = Path(args.workdir).resolve()
    shutil.rmtree(work, ignore_errors=True)
    work.mkdir(parents=True)
    ds = work / "dataset"
    params = SynthParams(
        nodes=args.nodes,
        attributes=args.attributes,
        sources=3,
        seed=11,
        kinds=("postgres", "postgres", "parquet"),
        snapshots=("full", "full", "delta"),
    )
    report: dict[str, Any] = {"params": {"entities": args.nodes, "attributes": args.attributes}}
    t = time.perf_counter()
    day0 = generate_day0(params, ds)
    report["source_rows_day0"] = day0["rows"]
    report["generate_day0_s"] = round(time.perf_counter() - t, 2)
    deltas = [DeltaParams(update_pct=0.6, new_pct=0.2, delete_pct=0.1, broken_pct=0.05)]
    if not args.no_day2:
        deltas.append(DeltaParams())
    for k, dp in enumerate(deltas, start=1):
        generate_delta(ds, k, dp)

    container = dk.start("smoke")
    report["container"] = {"name": container.name, "memory_cap": dk.MEMORY, "shm": "512m"}
    try:
        with psycopg.connect(container.dsn(), autocommit=True) as conn:
            conn.execute('CREATE DATABASE "state"')
            conn.execute('CREATE DATABASE "src"')
        raw = yaml.safe_load((ds / "config.yml").read_text())
        raw["state"]["schema"] = "mdm_smoke"
        cfg_path = work / "config.yml"
        cfg_path.write_text(yaml.safe_dump(raw, sort_keys=False), encoding="utf-8")
        env = {
            **os.environ,
            "PYTHONDONTWRITEBYTECODE": "1",
            raw["state"]["dsn_env"]: container.dsn("state"),
        }
        for src in raw["sources"]:
            if src["kind"] == "postgres":
                env[src["dsn_env"]] = container.dsn("src")
        from engine.core.config import load_config

        cfg = load_config(cfg_path)
        base = [sys.executable, "-B", "-m", "engine.lanes.graph"]
        container.start_watch()
        report["init"] = run_measured([*base, "init", "--config", str(cfg_path)], env, work, "init")
        for day in range(len(deltas) + 1):
            stage_live(ds, day)
            load_postgres(cfg, container.dsn("src"), ds, day=day)
            run = f"day{day}"
            entry: dict[str, Any] = {}
            for src in cfg.sources:
                r = run_measured(
                    [
                        *base,
                        "ingest",
                        "--config",
                        str(cfg_path),
                        "--source",
                        src.id,
                        "--run-id",
                        run,
                    ],
                    env,
                    work,
                    f"{run}_ingest_{src.id}",
                )
                entry[f"ingest_{src.id}"] = {
                    **r,
                    "counts": r["summary"]["counts"],
                    "phases": r["summary"]["seconds"],
                }
                entry[f"ingest_{src.id}"].pop("summary")
            r = run_measured(
                [
                    *base,
                    "reconcile",
                    "--config",
                    str(cfg_path),
                    "--run-id",
                    run,
                    "--components",
                    args.components,
                ],
                env,
                work,
                f"{run}_reconcile",
            )
            entry["reconcile"] = {
                "wall_s": r["wall_s"],
                "peak_rss_mb": r["peak_rss_mb"],
                "counts": r["summary"]["counts"],
                "phases": r["summary"]["seconds"],
            }
            r = run_measured(
                [*base, "export", "--config", str(cfg_path), "--out", str(work / f"export{day}")],
                env,
                work,
                f"{run}_export",
            )
            entry["export"] = {
                "wall_s": r["wall_s"],
                "peak_rss_mb": r["peak_rss_mb"],
                "counts": r["summary"]["counts"],
                "phases": r["summary"]["seconds"],
            }
            report[run] = entry
        report["container"]["peak_mem_mb"] = round(container.stop_watch() / 2**20, 1)
        with psycopg.connect(container.dsn("state"), autocommit=True) as conn:
            conn.execute("LOAD 'age'")
            conn.execute("SET search_path = ag_catalog, public")
            rows = conn.execute(
                "SELECT n.nspname, c.relname, pg_total_relation_size(c.oid) FROM pg_class c"
                " JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.relkind = 'r'"
                " AND n.nspname IN ('mdm_smoke', 'mdm_smoke_g') AND c.relname NOT LIKE '\\_%'"
                " ORDER BY 3 DESC"
            ).fetchall()
            report["table_sizes_mb"] = {f"{s}.{r}": round(b / 2**20, 2) for s, r, b in rows}
            report["state_db_mb"] = round(
                conn.execute("SELECT pg_database_size('state')").fetchone()[0] / 2**20, 1
            )
        report["final_rows"] = {
            "source_record": sum(
                r for r in [report[f"day{len(deltas)}"]["export"]["counts"]["source_record"]]
            )
        }
    finally:
        container.stop_watch()
        container.remove()
        if not args.keep:
            shutil.rmtree(work, ignore_errors=True)
    text = json.dumps(report, indent=2)
    if args.report:
        Path(args.report).write_text(text, encoding="utf-8")
    print(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())
