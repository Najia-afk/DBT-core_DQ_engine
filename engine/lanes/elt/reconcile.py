"""Reconcile : modèles dbt générés (delta, paires, clusters, golden) puis publication atomique."""

from __future__ import annotations

import asyncio
import shutil
import time
from pathlib import Path
from typing import Any

import duckdb
import psycopg

from engine.core.canonical import canonical_run_id

from . import statedb
from .dbtrun import run_dbt
from .render import render_project
from .settings import split_dsn, state_dsn

ZERO_COUNTERS = {
    "delta_nodes": 0,
    "rescored_nodes": 0,
    "pairs": 0,
    "affected_nodes": 0,
    "stagings": 0,
    "merges": 0,
    "splits": 0,
    "creates": 0,
    "golden_changes": 0,
}


def _read_counters(db: Path) -> dict[str, Any]:
    con = duckdb.connect(str(db), read_only=True)
    try:
        cur = con.execute("SELECT * FROM r_counters")
        names = [d[0] for d in cur.description]
        row = cur.fetchone()
        return {k: int(v) for k, v in zip(names, row or [], strict=False)}
    finally:
        con.close()


def _precheck(lane: Any, run_id: str) -> tuple[psycopg.Connection, dict | None, int]:
    spec = lane.spec
    timeout = lane.settings.dbt_timeout_s or 3600.0
    conn = statedb.connect(lane.dsn, autocommit=True)
    try:
        statedb.acquire_reconcile_lock(conn, spec, timeout)
        previous = statedb.logged_summary(conn, spec, run_id, "reconcile")
        row = conn.execute(f"SELECT count(*) FROM {spec.schema}.pending_delta").fetchone()
        statedb.drop_work_tables(conn, spec, "_wr_")
        return conn, previous, int(row[0]) if row else 0
    except psycopg.errors.UndefinedTable as error:
        conn.close()
        raise statedb.LaneError("state not initialised: run `init` first") from error
    except BaseException:
        conn.close()
        raise


def _log_empty(lane: Any, run_id: str, started: float) -> None:
    with statedb.connect(lane.dsn) as conn:
        statedb.log_run(conn, lane.spec, run_id, "reconcile", started, ZERO_COUNTERS)
        conn.commit()


async def reconcile_run(lane: Any, run_id: str) -> dict[str, Any]:
    """Un reconcile traite tout le delta en attente ; idempotent par run-id ; un seul à la fois
    par référentiel (verrou consultatif PostgreSQL)."""
    run_id = canonical_run_id(run_id)
    cfg, spec, settings = lane.config, lane.spec, lane.settings
    started, t0 = time.time(), time.perf_counter()
    phases: dict[str, float] = {}
    lock_conn, previous, pending = await asyncio.to_thread(_precheck, lane, run_id)
    try:
        if previous is not None:
            phases["total"] = time.perf_counter() - t0
            return lane.summary("reconcile", run_id, previous, phases, replayed=True)
        if pending == 0:
            await asyncio.to_thread(_log_empty, lane, run_id, started)
            phases["total"] = time.perf_counter() - t0
            return lane.summary("reconcile", run_id, dict(ZERO_COUNTERS), phases)
        work = lane.run_dir(run_id) / "reconcile"
        if work.exists():
            await asyncio.to_thread(shutil.rmtree, work, True)
        work.mkdir(parents=True)

        t = time.perf_counter()
        project = render_project(spec, settings, work / "project", "reconcile", {}, work)
        phases["render"] = time.perf_counter() - t

        conninfo, pgenv = split_dsn(state_dsn(cfg))
        scratch = work / "scratch.duckdb"
        env = {"MDM_DUCKDB_PATH": str(scratch), "MDM_PG_CONN": conninfo, **pgenv}
        dbt = await run_dbt(project, work / "dbt", env, {"run_id": run_id}, settings.dbt_timeout_s)
        phases["dbt"] = dbt.seconds
        counters = await asyncio.to_thread(_read_counters, scratch)
        statedb.fault(settings, "reconcile_after_dbt")

        t = time.perf_counter()
        await asyncio.to_thread(
            statedb.apply_reconcile, lane.dsn, spec, settings, run_id, started, counters
        )
        phases["apply"] = time.perf_counter() - t
        counters["dbt_models"] = dbt.model_seconds
        if not settings.keep_work:
            await asyncio.to_thread(shutil.rmtree, work, True)
        phases["total"] = time.perf_counter() - t0
        return lane.summary("reconcile", run_id, counters, phases)
    finally:
        await asyncio.to_thread(lock_conn.close)
