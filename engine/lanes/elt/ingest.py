"""Ingest d'une source : extraction du brut, modèles dbt générés, fusion dans l'état PostgreSQL."""

from __future__ import annotations

import asyncio
import shutil
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb
import psycopg

from engine.core.canonical import canonical_run_id
from engine.core.config import Source

from . import statedb
from .dbtrun import run_dbt
from .extract import extract_raw
from .render import render_project
from .settings import split_dsn, state_dsn
from .sqlgen import (
    EPS,
    AttrView,
    CheckView,
    Spec,
    cast_sql,
    check_pass_sql,
    normalize_sql,
    num,
    q,
    raw_class,
    row_hash_sql,
    timestamp_sql,
)


@dataclass
class CheckCtx:
    idx: int
    rule: str
    action: str
    fq: str
    stops: str | None
    nulls: bool
    view: CheckView


@dataclass
class ColCtx:
    a: AttrView
    raw_q: str
    cast_sql: str
    can_fail: bool
    checks: list[CheckCtx]

    @property
    def cq(self) -> str:
        return q("c_" + self.a.name)

    @property
    def nq(self) -> str:
        return q("n_" + self.a.name)

    @property
    def vq(self) -> str:
        return q("v_" + self.a.name)

    @property
    def cfq(self) -> str:
        return q("cf_" + self.a.name)

    @property
    def rpq(self) -> str:
        return q("rp_" + self.a.name)

    def norm_sql(self, alias: str) -> str:
        value = f"{alias}.{self.cq}"
        if self.a.type == "string":
            return normalize_sql(value, list(self.a.attr.normalize))
        return value

    def pass_sql(self, chk: CheckCtx, alias: str) -> str:
        return check_pass_sql(self.a.attr, chk.view.check, f"{alias}.{self.nq}")

    @property
    def final_sql(self) -> str:
        nulling = [f"k.{c.fq}" for c in self.checks if c.nulls]
        if not nulling:
            return f"k.{self.nq}"
        return f"CASE WHEN {' OR '.join(nulling)} THEN NULL ELSE k.{self.nq} END"


def build_context(
    spec: Spec, source: Source, raw_types: dict[str, str], raw_glob: str
) -> dict[str, Any]:
    cfg = spec.config
    classes = {name: raw_class(t) for name, t in raw_types.items()}
    mapping = cfg.mapping(source.id)
    cols: list[ColCtx] = []
    for a in spec.attrs:
        column = mapping.get(a.name)
        if column is None:
            continue
        if column in classes:
            raw = f"s.{q(column)}"
            expr, can_fail = cast_sql(raw, classes[column], a.type)
        else:  # colonne absente de la source : toujours nulle (comme `row.get` de la référence)
            raw, expr, can_fail = "CAST(NULL AS VARCHAR)", "NULL", False
        checks: list[CheckCtx] = []
        for chk in a.checks:
            stops = [c.fq for c in checks if c.nulls]
            checks.append(
                CheckCtx(
                    chk.idx,
                    chk.rule,
                    chk.action,
                    q(f"f_{a.name}_{chk.idx}"),
                    " OR ".join(stops) if stops else None,
                    chk.nulls,
                    chk,
                )
            )
        cols.append(ColCtx(a, raw, f"CAST({expr} AS {a.sql_type})", can_fail, checks))
    key_raw = f"s.{q(source.key)}"
    if source.revision:
        rev_raw = f"s.{q(source.revision)}"
        rev_expr, _ = timestamp_sql(rev_raw, classes[source.revision])
        rev_sql, rev_present = f"CAST({rev_expr} AS TIMESTAMP)", f"{rev_raw} IS NOT NULL"
    else:
        rev_sql, rev_present = "CAST(NULL AS TIMESTAMP)", "FALSE"
    by_name = {c.a.name: c for c in cols}
    value_of = {
        a.name: (f"n.{by_name[a.name].vq}" if a.name in by_name else f"CAST(NULL AS {a.sql_type})")
        for a in spec.attrs
    }
    reject_any = [f"n.{c.cfq}" for c in cols if c.can_fail]
    reject_any += [f"n.{k.fq}" for c in cols for k in c.checks if k.action == "reject_record"]
    return {
        "src": source,
        "cols": cols,
        "key_raw": key_raw,
        "rev_sql": rev_sql,
        "rev_present_sql": rev_present,
        "row_hash": row_hash_sql(cfg, source.id, lambda a: f"n.{q('v_' + a.name)}"),
        "value_of": value_of,
        "reject_any": " OR ".join(reject_any) if reject_any else "FALSE",
        "min_ratio": num(source.full_snapshot_min_ratio - EPS),
        "min_ratio_text": str(source.full_snapshot_min_ratio),
        "raw_glob": raw_glob,
    }


def read_counters(db: Path) -> dict[str, Any]:
    con = duckdb.connect(str(db), read_only=True)
    try:
        cur = con.execute("SELECT * FROM i_counters")
        names = [d[0] for d in cur.description]
        row = cur.fetchone()
        return dict(zip(names, row or [], strict=False))
    finally:
        con.close()


def result_counters(raw: dict[str, Any], extract: dict[str, Any]) -> dict[str, Any]:
    blocked = bool(raw["blocked"])
    return {
        "inserted": 0 if blocked else int(raw["inserted"]),
        "updated": 0 if blocked else int(raw["updated"]),
        "deleted": 0 if blocked else int(raw["deleted"]),
        "unchanged": 0 if blocked else int(raw["unchanged_raw"]),
        "rejected": 0 if blocked else int(raw["rejected_raw"]),
        "blocked": blocked,
        "rows_raw": int(raw["rows_raw"]),
        "duplicates": int(raw["duplicates"]),
        "key_null": int(raw["key_null"]),
        "reject_rows": 0 if blocked else int(raw["reject_rows"]),
        "raw_bytes": extract["bytes"],
        "missing_columns": extract["missing_columns"],
    }


async def ingest_source(lane: Any, source_id: str, run_id: str) -> dict[str, Any]:
    """Un ingest complet (idempotent par run-id) ; plusieurs sources peuvent tourner en parallèle,
    chacune avec son propre fichier DuckDB et sa propre transaction de fusion."""
    run_id = canonical_run_id(run_id)
    cfg, spec, settings = lane.config, lane.spec, lane.settings
    source = cfg.source(source_id)
    started = time.time()
    t0 = time.perf_counter()
    phases: dict[str, float] = {}
    step = f"ingest:{source_id}"
    dsn = lane.dsn

    def precheck() -> tuple[psycopg.Connection, dict | None]:
        conn = statedb.connect(dsn, autocommit=True)
        try:
            statedb.acquire_source_lock(conn, spec, source_id)
            return conn, statedb.logged_summary(conn, spec, run_id, step)
        except psycopg.errors.UndefinedTable as error:
            conn.close()
            raise statedb.LaneError("state not initialised: run `init` first") from error
        except BaseException:
            conn.close()
            raise

    lock_conn, previous = await asyncio.to_thread(precheck)
    try:
        if previous is not None:
            return lane.summary(
                "ingest",
                run_id,
                previous,
                {"total": time.perf_counter() - t0},
                source=source_id,
                replayed=True,
            )
        work = lane.run_dir(run_id) / f"ingest-{source_id}"
        if work.exists():
            await asyncio.to_thread(shutil.rmtree, work, True)
        work.mkdir(parents=True)
        local = settings
        raw_dir = work / "raw"
        t = time.perf_counter()
        extract = await asyncio.to_thread(extract_raw, cfg, source, local, raw_dir)
        phases["extract"] = time.perf_counter() - t

        t = time.perf_counter()
        glob_path = (raw_dir / "part-*.parquet").as_posix()
        context = build_context(spec, source, extract["raw_types"], glob_path)
        project = render_project(spec, local, work / "project", "ingest", context, work)
        phases["render"] = time.perf_counter() - t

        conninfo, pgenv = split_dsn(state_dsn(cfg))
        scratch = work / "scratch.duckdb"
        env = {"MDM_DUCKDB_PATH": str(scratch), "MDM_PG_CONN": conninfo, **pgenv}
        await asyncio.to_thread(_drop_leftovers, dsn, spec, source_id)
        dbt = await run_dbt(
            project,
            work / "dbt",
            env,
            {"run_id": run_id, "raw_glob": glob_path},
            local.dbt_timeout_s,
        )
        phases["dbt"] = dbt.seconds
        raw = await asyncio.to_thread(read_counters, scratch)
        counters = result_counters(raw, extract)
        statedb.fault(settings, "ingest_after_dbt")

        t = time.perf_counter()
        await asyncio.to_thread(
            statedb.merge_ingest, dsn, spec, settings, source_id, run_id, started, counters
        )
        phases["merge"] = time.perf_counter() - t
        counters["dbt_models"] = dbt.model_seconds
        if not settings.keep_work:
            await asyncio.to_thread(shutil.rmtree, work, True)
        phases["total"] = time.perf_counter() - t0
        return lane.summary("ingest", run_id, counters, phases, source=source_id)
    finally:
        await asyncio.to_thread(lock_conn.close)


def _drop_leftovers(dsn: str, spec: Spec, source_id: str) -> None:
    with statedb.connect(dsn, autocommit=True) as conn:
        statedb.drop_tables(conn, spec, [f"_wi_{n}_{source_id}" for n in statedb.INGEST_WORK])
