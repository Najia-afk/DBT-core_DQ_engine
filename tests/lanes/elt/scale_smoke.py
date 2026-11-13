"""Smoke d'échelle de la voie ELT (hors suite pytest) : 100 000 nœuds x 50 attributs.

    python tests/lanes/elt/scale_smoke.py [--nodes N] [--attributes A] [--out FICHIER.json]

Démarre un PostgreSQL jetable `mdm2-elt-smoke` (6 Go mémoire+swap, shm 512 Mo), génère le jeu
(`engine.core.synth`), charge les sources, puis mesure jour 0 (chargement initial) et jour 1 (delta
~1 %) : lignes/s par phase et pic de RSS de l'arbre de processus (Python + dbt + DuckDB). DuckDB
est borné explicitement (memory_limit <= 6 Go, threads <= 8). Le conteneur est supprimé en fin de
run.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import shutil
import socket
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path

import psutil
import psycopg

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from engine.core.synth import DeltaParams, SynthParams, generate, stage_live  # noqa: E402
from engine.core.synth.pg import load_postgres  # noqa: E402
from engine.lanes.elt.lane import Lane  # noqa: E402
from engine.lanes.elt.settings import Settings  # noqa: E402


class RssSampler(threading.Thread):
    """Pic de RSS cumulé du processus courant et de tous ses descendants (dbt, DuckDB)."""

    def __init__(self) -> None:
        super().__init__(daemon=True)
        self.peak = 0
        self.stop = threading.Event()
        self.me = psutil.Process()

    def run(self) -> None:
        while not self.stop.is_set():
            total = 0
            for proc in [self.me, *self.me.children(recursive=True)]:
                try:
                    total += proc.memory_info().rss
                except psutil.Error:
                    pass
            self.peak = max(self.peak, total)
            self.stop.wait(0.5)


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def pg_peak_memory(name: str) -> float:
    out = subprocess.run(
        ["docker", "stats", "--no-stream", "--format", "{{.MemUsage}}", name],
        capture_output=True,
        text=True,
    ).stdout.strip()
    return out


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--nodes", type=int, default=100_000)
    ap.add_argument("--attributes", type=int, default=50)
    ap.add_argument("--memory", default="5GB")
    ap.add_argument("--threads", type=int, default=8)
    ap.add_argument("--out", default=".mdm_work/smoke.json")
    ap.add_argument("--keep-data", action="store_true")
    args = ap.parse_args()

    name = f"mdm2-elt-smoke-{uuid.uuid4().hex[:6]}"
    port = free_port()
    work = Path(".mdm_work/smoke").resolve()
    if work.exists():
        shutil.rmtree(work)
    work.mkdir(parents=True)
    subprocess.run(
        [
            "docker", "run", "-d", "--name", name, "--memory", "6g", "--memory-swap", "6g",
            "--shm-size", "512m", "-e", "POSTGRES_PASSWORD=smoke", "-e", "POSTGRES_DB=mdm",
            "-p", f"127.0.0.1:{port}:5432", "postgres:16.4-bookworm",
            "-c", "shared_buffers=1GB", "-c", "max_wal_size=4GB", "-c", "synchronous_commit=off",
            "-c", "checkpoint_timeout=15min", "-c", "max_connections=60",
        ],
        check=True, capture_output=True,
    )  # fmt: skip
    dsn = f"postgresql://postgres:smoke@127.0.0.1:{port}/mdm"
    report: dict = {"nodes": args.nodes, "attributes": args.attributes}
    sampler = RssSampler()
    try:
        deadline = time.time() + 90
        while True:
            try:
                psycopg.connect(dsn, connect_timeout=3).close()
                break
            except psycopg.OperationalError:
                if time.time() > deadline:
                    raise
                time.sleep(1)
        for key in ("MDM_STATE_DSN", "MDM_SRC_CRM_DSN", "MDM_SRC_ERP_DSN"):
            os.environ[key] = dsn
        os.environ["MDM_ELT_WORKDIR"] = str(work / "elt")
        os.environ["MDM_ELT_MEMORY_LIMIT"] = args.memory
        os.environ["MDM_ELT_THREADS"] = str(args.threads)
        os.environ["MDM_ELT_DBT_TIMEOUT_S"] = "3000"

        params = SynthParams(
            nodes=args.nodes, attributes=args.attributes, sources=3, seed=42, workers=4,
            kinds=("postgres", "postgres", "parquet"), snapshots=("full", "full", "delta"),
        )  # fmt: skip
        delta = DeltaParams(update_pct=0.7, new_pct=0.2, delete_pct=0.1, broken_pct=0.1)
        t = time.perf_counter()
        gen = generate(params, work / "ds", days=1, delta=delta)
        report["generate_seconds"] = round(time.perf_counter() - t, 1)
        report["dataset_rows"] = [d["rows"] for d in gen["days"]]
        cfg_path = work / "ds" / "config.yml"
        lane = Lane(cfg_path, Settings.from_env())
        lane.reset()
        lane.init()
        sampler.start()
        for day in (0, 1):
            t = time.perf_counter()
            load_postgres(lane.config, dsn, work / "ds", day=day, workers=4)
            stage_live(work / "ds", day)
            report[f"day{day}_load_sources_seconds"] = round(time.perf_counter() - t, 1)
            sampler.peak = 0
            run = f"day{day}"
            ids = [s.id for s in lane.config.sources]
            ing = await lane.ingest(ids, run)
            ingest_peak = sampler.peak
            sampler.peak = 0
            rows = {s: ing["counters"][s]["rows_raw"] for s in ids}
            report[f"day{day}_ingest"] = {
                "rows": rows,
                "seconds_total": ing["seconds"]["total"],
                "per_source_seconds": ing["per_source_seconds"],
                "rows_per_second_all_sources": round(sum(rows.values()) / ing["seconds"]["total"]),
                "counters": {
                    s: {k: v for k, v in ing["counters"][s].items() if k != "dbt_models"}
                    for s in ids
                },
                "dbt_slowest": {
                    s: dict(
                        sorted(ing["counters"][s]["dbt_models"].items(), key=lambda kv: -kv[1])[:5]
                    )
                    for s in ids
                },
                "peak_rss_gb": round(ingest_peak / 1024**3, 2),
            }
            rec = await lane.reconcile(run)
            c = rec["counters"]
            report[f"day{day}_reconcile"] = {
                "seconds": rec["seconds"],
                "delta_nodes": c["delta_nodes"],
                "nodes_per_second": round(c["delta_nodes"] / rec["seconds"]["total"], 1),
                "counters": {k: v for k, v in c.items() if k != "dbt_models"},
                "dbt_slowest": dict(sorted(c["dbt_models"].items(), key=lambda kv: -kv[1])[:8]),
                "peak_rss_gb": round(sampler.peak / 1024**3, 2),
            }
            report[f"day{day}_pg_container_memory"] = pg_peak_memory(name)
        with psycopg.connect(dsn) as conn:
            schema = lane.config.state.schema_name
            q = lambda s: conn.execute(s).fetchone()[0]  # noqa: E731
            report["state"] = {
                "active_records": q(
                    f"select count(*) from {schema}.source_record where status='active'"
                ),
                "stagings_active": q(
                    f"select count(*) from {schema}.staging where status='active'"
                ),
                "golden_published": q(
                    f"select count(*) from {schema}.golden where status='published'"
                ),
                "golden_blocked": q(f"select count(*) from {schema}.golden where status='blocked'"),
                "match_pairs": q(f"select count(*) from {schema}.match_pair"),
                "members_without_active_record": q(
                    f"select count(*) from {schema}.staging_member m"
                    f" left join {schema}.source_record r using (source_id, source_key)"
                    " where r.status is distinct from 'active'"
                ),
                "active_without_staging": q(
                    f"select count(*) from {schema}.source_record r"
                    f" left join {schema}.staging_member m using (source_id, source_key)"
                    " where r.status='active' and m.staging_id is null"
                ),
                "db_size_mb": q("select pg_database_size('mdm') / 1048576"),
            }
        report["peak_rss_overall_gb"] = round(sampler.peak / 1024**3, 2)
    finally:
        sampler.stop.set()
        subprocess.run(["docker", "rm", "-f", name], capture_output=True)
        if not args.keep_data:
            shutil.rmtree(work, ignore_errors=True)
    Path(args.out).write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    print(json.dumps(report, indent=2, default=str))


if __name__ == "__main__":
    asyncio.run(main())
