"""État PostgreSQL de la voie ELT : DDL, verrous, journal de runs et transactions de publication.

Toutes les mutations d'un run (ingest ou reconcile) sont appliquées dans UNE transaction à partir
de tables de travail alimentées par dbt (`_wi_<nom>_<source>` pour l'ingest, `_wr_<nom>` pour le
reconcile) : un lecteur voit l'ancien ou le nouvel état, jamais un état intermédiaire, et un
échec laisse l'état publié précédent intact.
"""

from __future__ import annotations

import json
import os
import time
from collections.abc import Iterable
from typing import Any

import psycopg

from .settings import ConfigurationError, Settings
from .sqlgen import Spec, q

RECORD_FIXED = [
    ("source_id", "text NOT NULL"),
    ("source_key", "text NOT NULL"),
    ("row_hash", "text NOT NULL"),
    ("revision", "timestamp"),
    ("status", "text NOT NULL"),
    ("first_run", "text NOT NULL"),
    ("last_change_run", "text NOT NULL"),
]
RECORD_FIXED_NAMES = [name for name, _ in RECORD_FIXED]
PAIR_KEYS = ["left_source_id", "left_key", "right_source_id", "right_key"]
PAIR_COLUMNS = [
    *PAIR_KEYS,
    "score",
    "decision",
    "contradiction",
    "eligible",
    "reason",
    "rule_version",
    "run_id",
]
STATE_TABLES = [
    "source_record",
    "source_delta",
    "pending_delta",
    "block_index",
    "match_pair",
    "staging",
    "staging_member",
    "staging_event",
    "golden",
    "golden_delta",
    "reject",
    "anomaly",
    "run_log",
    "meta",
]
LOCK_RECONCILE = 1
INGEST_WORK = ("up", "ret", "delta", "rej", "anom")


class LaneError(RuntimeError):
    """Erreur d'exploitation (message destiné à l'opérateur)."""


def fault(settings: Settings, point: str) -> None:
    """Injection de panne : mort immédiate du processus (comme un kill -9) au point demandé."""
    if settings.fault == point:
        os._exit(137)


def connect(dsn: str, **kwargs: Any) -> psycopg.Connection:
    return psycopg.connect(dsn, **kwargs)


def golden_columns(spec: Spec) -> list[str]:
    cols = ["golden_id", "status", "stale"]
    for a in spec.attrs:
        cols.append(a.name)
        if spec.provenance:
            cols.append(a.name + "__src")
    return cols


def record_columns(spec: Spec) -> list[str]:
    return [*RECORD_FIXED_NAMES, *(a.name for a in spec.attrs)]


def ddl(spec: Spec) -> list[str]:
    s = spec.schema
    record = [f"{q(n)} {t}" for n, t in RECORD_FIXED]
    record += [f"{a.q} {a.pg_type}" for a in spec.attrs]
    golden = ["golden_id bigint PRIMARY KEY", "status text NOT NULL", "stale boolean NOT NULL"]
    for a in spec.attrs:
        golden.append(f"{a.q} {a.pg_type}")
        if spec.provenance:
            golden.append(f"{a.qsrc} text")
    return [
        f"CREATE SCHEMA IF NOT EXISTS {s}",
        f"CREATE TABLE IF NOT EXISTS {s}.source_record ({', '.join(record)},"
        " PRIMARY KEY (source_id, source_key))",
        f"CREATE TABLE IF NOT EXISTS {s}.source_delta (run_id text NOT NULL,"
        " source_id text NOT NULL, source_key text NOT NULL, kind text NOT NULL,"
        " old_hash text, new_hash text)",
        f"CREATE INDEX IF NOT EXISTS source_delta_run ON {s}.source_delta (run_id)",
        f"CREATE SEQUENCE IF NOT EXISTS {s}.pending_seq",
        f"CREATE TABLE IF NOT EXISTS {s}.pending_delta (source_id text NOT NULL,"
        f" source_key text NOT NULL, seq bigint NOT NULL DEFAULT nextval('{spec.schema_name}"
        ".pending_seq'), PRIMARY KEY (source_id, source_key))",
        f"CREATE TABLE IF NOT EXISTS {s}.block_index (block_name text NOT NULL,"
        " block_value text NOT NULL, source_id text NOT NULL, source_key text NOT NULL,"
        " PRIMARY KEY (block_name, block_value, source_id, source_key))",
        f"CREATE INDEX IF NOT EXISTS block_index_node ON {s}.block_index (source_id, source_key)",
        f"CREATE TABLE IF NOT EXISTS {s}.match_pair (left_source_id text NOT NULL,"
        " left_key text NOT NULL, right_source_id text NOT NULL, right_key text NOT NULL,"
        " score double precision, decision text NOT NULL, contradiction boolean NOT NULL,"
        " eligible boolean NOT NULL, reason text NOT NULL, rule_version text NOT NULL,"
        " run_id text NOT NULL,"
        " PRIMARY KEY (left_source_id, left_key, right_source_id, right_key))",
        f"CREATE INDEX IF NOT EXISTS match_pair_right ON {s}.match_pair"
        " (right_source_id, right_key)",
        f"CREATE TABLE IF NOT EXISTS {s}.staging (staging_id bigint PRIMARY KEY,"
        " status text NOT NULL, created_run text NOT NULL)",
        f"CREATE TABLE IF NOT EXISTS {s}.staging_member (staging_id bigint NOT NULL,"
        " source_id text NOT NULL, source_key text NOT NULL, PRIMARY KEY (source_id, source_key))",
        f"CREATE INDEX IF NOT EXISTS staging_member_stg ON {s}.staging_member (staging_id)",
        f"CREATE TABLE IF NOT EXISTS {s}.staging_event (run_id text NOT NULL, kind text NOT NULL,"
        " staging_id bigint NOT NULL, related_id bigint)",
        f"CREATE TABLE IF NOT EXISTS {s}.golden ({', '.join(golden)})",
        f"CREATE TABLE IF NOT EXISTS {s}.golden_delta (run_id text NOT NULL,"
        " golden_id bigint NOT NULL, attribute text NOT NULL, old_value text, new_value text,"
        " winning_source text)",
        f"CREATE TABLE IF NOT EXISTS {s}.reject (run_id text NOT NULL, source_id text NOT NULL,"
        " source_key text NOT NULL, attribute text NOT NULL, rule text NOT NULL,"
        " action text NOT NULL)",
        f"CREATE TABLE IF NOT EXISTS {s}.anomaly (run_id text NOT NULL, kind text NOT NULL,"
        " subject text NOT NULL, detail text)",
        f"CREATE TABLE IF NOT EXISTS {s}.run_log (run_id text NOT NULL, referential text NOT NULL,"
        " step text NOT NULL, status text NOT NULL, started_at timestamptz NOT NULL,"
        " ended_at timestamptz, counts jsonb, error text, PRIMARY KEY (run_id, step))",
        f"CREATE TABLE IF NOT EXISTS {s}.meta (key text PRIMARY KEY, value bigint NOT NULL)",
        f"INSERT INTO {s}.meta (key, value) VALUES ('next_staging_id', 1)"
        " ON CONFLICT (key) DO NOTHING",
        f"CREATE OR REPLACE VIEW {s}.review AS SELECT run_id, left_source_id, left_key,"
        f" right_source_id, right_key, score, reason FROM {s}.match_pair"
        " WHERE decision = 'review'",
    ]


def init_state(dsn: str, spec: Spec) -> dict[str, Any]:
    with connect(dsn) as conn:
        for statement in ddl(spec):
            conn.execute(statement)
        _check_columns(conn, spec)
        conn.commit()
    return {"schema": spec.schema_name, "tables": len(STATE_TABLES)}


def _check_columns(conn: psycopg.Connection, spec: Spec) -> None:
    expected = {"source_record": record_columns(spec), "golden": golden_columns(spec)}
    for table, cols in expected.items():
        rows = conn.execute(
            "SELECT column_name FROM information_schema.columns"
            " WHERE table_schema = %s AND table_name = %s",
            [spec.schema_name, table],
        ).fetchall()
        have = {r[0] for r in rows}
        if have != set(cols):
            raise ConfigurationError(
                f"state table {spec.schema_name}.{table} does not match the config"
                f" (missing={sorted(set(cols) - have)[:5]}, extra={sorted(have - set(cols))[:5]});"
                " migrations are not supported: run `reset` then re-ingest"
            )


def reset_state(dsn: str, spec: Spec) -> dict[str, Any]:
    with connect(dsn, autocommit=True) as conn:
        existed = conn.execute(
            "SELECT 1 FROM information_schema.schemata WHERE schema_name = %s", [spec.schema_name]
        ).fetchone()
        conn.execute(f"DROP SCHEMA IF EXISTS {spec.schema} CASCADE")
    return {"schema": spec.schema_name, "dropped": bool(existed)}


def lock_key(spec: Spec) -> int:
    value = 0
    for ch in spec.schema_name:
        value = (value * 131 + ord(ch)) % 2_000_000_011
    return value


def acquire_reconcile_lock(conn: psycopg.Connection, spec: Spec, timeout_s: float) -> None:
    """Un seul reconcile par référentiel (verrou de session, attente bornée)."""
    key = lock_key(spec)
    deadline = time.monotonic() + max(timeout_s, 1.0)
    while True:
        row = conn.execute("SELECT pg_try_advisory_lock(%s, %s)", [key, LOCK_RECONCILE]).fetchone()
        if row and row[0]:
            return
        if time.monotonic() > deadline:
            raise LaneError(f"another reconcile is running on {spec.schema_name}")
        time.sleep(0.2)


def acquire_source_lock(conn: psycopg.Connection, spec: Spec, source_id: str) -> None:
    sub = 100 + (sum(ord(c) * (i + 1) for i, c in enumerate(source_id)) % 1_000_000)
    row = conn.execute("SELECT pg_try_advisory_lock(%s, %s)", [lock_key(spec), sub]).fetchone()
    if not (row and row[0]):
        raise LaneError(f"another ingest is running on source {source_id}")


def logged_summary(conn: psycopg.Connection, spec: Spec, run_id: str, step: str) -> dict | None:
    """Résumé d'un run déjà publié (idempotence : un même run-id n'est jamais rejoué)."""
    row = conn.execute(
        f"SELECT counts FROM {spec.schema}.run_log WHERE run_id = %s AND step = %s"
        " AND status = 'ok'",
        [run_id, step],
    ).fetchone()
    return row[0] if row else None


def _like(text: str) -> str:
    return text.replace("\\", "\\\\").replace("_", "\\_").replace("%", "\\%")


def work_tables(conn: psycopg.Connection, spec: Spec, prefix: str, suffix: str = "") -> list[str]:
    rows = conn.execute(
        "SELECT table_name FROM information_schema.tables"
        " WHERE table_schema = %s AND table_name LIKE %s ESCAPE '\\'",
        [spec.schema_name, _like(prefix) + "%" + _like(suffix)],
    ).fetchall()
    return [r[0] for r in rows]


def drop_work_tables(conn: psycopg.Connection, spec: Spec, prefix: str, suffix: str = "") -> int:
    return drop_tables(conn, spec, work_tables(conn, spec, prefix, suffix))


def drop_tables(conn: psycopg.Connection, spec: Spec, names: Iterable[str]) -> int:
    count = 0
    for name in names:
        conn.execute(f"DROP TABLE IF EXISTS {spec.schema}.{q(name)}")
        count += 1
    return count


def _cols(names: Iterable[str]) -> str:
    return ", ".join(q(n) for n in names)


def log_run(
    conn: psycopg.Connection, spec: Spec, run_id: str, step: str, started: float, counts: dict
) -> None:
    conn.execute(
        f"INSERT INTO {spec.schema}.run_log (run_id, referential, step, status, started_at,"
        " ended_at, counts) VALUES (%s, %s, %s, 'ok', to_timestamp(%s), now(), %s)"
        " ON CONFLICT (run_id, step) DO UPDATE SET status = 'ok', ended_at = now(),"
        " counts = EXCLUDED.counts, error = NULL",
        [run_id, spec.config.referential, step, started, json.dumps(counts)],
    )


def merge_ingest(
    dsn: str,
    spec: Spec,
    settings: Settings,
    source_id: str,
    run_id: str,
    started: float,
    counts: dict,
) -> bool:
    """Fusionne un lot d'ingest dans l'état en une transaction (verrou partagé : jamais pendant
    un reconcile). Retourne False si ce run-id était déjà publié (rien n'est refait)."""
    s, suffix = spec.schema, f"_{source_id}"

    def w(name: str) -> str:
        return f"{s}.{q('_wi_' + name + suffix)}"

    cols = record_columns(spec)
    updates = ", ".join(
        f"{q(c)} = EXCLUDED.{q(c)}"
        for c in cols
        if c not in {"source_id", "source_key", "first_run"}
    )
    with connect(dsn) as conn:
        conn.execute(
            "SELECT pg_advisory_xact_lock_shared(%s, %s)", [lock_key(spec), LOCK_RECONCILE]
        )
        if logged_summary(conn, spec, run_id, f"ingest:{source_id}") is not None:
            conn.rollback()
            return False
        statements: list[tuple[str, list[Any]]] = [
            (
                f"INSERT INTO {s}.source_record ({_cols(cols)}) SELECT {_cols(cols)}"
                f" FROM {w('up')} ON CONFLICT (source_id, source_key) DO UPDATE SET {updates}",
                [],
            ),
            (
                f"UPDATE {s}.source_record sr SET status = 'retired', last_change_run = %s"
                f" FROM {w('ret')} r WHERE sr.source_id = %s AND sr.source_key = r.source_key",
                [run_id, source_id],
            ),
            (f"INSERT INTO {s}.source_delta SELECT * FROM {w('delta')}", []),
            (f"INSERT INTO {s}.reject SELECT * FROM {w('rej')}", []),
            (f"INSERT INTO {s}.anomaly SELECT * FROM {w('anom')}", []),
            (
                f"INSERT INTO {s}.pending_delta (source_id, source_key)"
                f" SELECT source_id, source_key FROM {w('delta')}"
                " ON CONFLICT (source_id, source_key) DO UPDATE SET seq = nextval("
                f"'{spec.schema_name}.pending_seq')",
                [],
            ),
        ]
        for statement, params in statements:
            conn.execute(statement, params)
        fault(settings, "ingest_before_commit")
        log_run(conn, spec, run_id, f"ingest:{source_id}", started, counts)
        drop_tables(conn, spec, [f"_wi_{n}{suffix}" for n in INGEST_WORK])
        conn.commit()
    return True


def apply_reconcile(
    dsn: str, spec: Spec, settings: Settings, run_id: str, started: float, counts: dict
) -> None:
    """Publication atomique d'un reconcile : toutes les écritures dans une seule transaction."""
    s = spec.schema

    def w(name: str) -> str:
        return f"{s}.{q('_wr_' + name)}"

    pair_set = ", ".join(
        f"{q(c)} = EXCLUDED.{q(c)}" for c in PAIR_COLUMNS if c not in set(PAIR_KEYS)
    )
    gold_cols = golden_columns(spec)
    gold_set = ", ".join(f"{q(c)} = EXCLUDED.{q(c)}" for c in gold_cols if c != "golden_id")
    on = " AND ".join(f"mp.{q(k)} = d.{q(k)}" for k in PAIR_KEYS)
    first: list[str] = [
        f"DELETE FROM {s}.match_pair mp USING {w('pair_del')} d WHERE {on}",
        f"INSERT INTO {s}.match_pair ({_cols(PAIR_COLUMNS)}) SELECT {_cols(PAIR_COLUMNS)}"
        f" FROM {w('pair_up')} ON CONFLICT ({_cols(PAIR_KEYS)}) DO UPDATE SET {pair_set}",
        f"DELETE FROM {s}.block_index b USING {w('blk_nodes')} d"
        " WHERE b.source_id = d.source_id AND b.source_key = d.source_key",
        f"INSERT INTO {s}.block_index (block_name, block_value, source_id, source_key)"
        f" SELECT block_name, block_value, source_id, source_key FROM {w('blk_new')}",
        f"DELETE FROM {s}.staging_member m USING {w('sm_del')} d"
        " WHERE m.source_id = d.source_id AND m.source_key = d.source_key",
        f"INSERT INTO {s}.staging_member (staging_id, source_id, source_key)"
        f" SELECT staging_id, source_id, source_key FROM {w('sm_up')}"
        " ON CONFLICT (source_id, source_key) DO UPDATE SET staging_id = EXCLUDED.staging_id",
        f"INSERT INTO {s}.staging (staging_id, status, created_run)"
        f" SELECT staging_id, status, created_run FROM {w('stg_new')}",
        f"UPDATE {s}.staging g SET status = 'retired' FROM {w('stg_ret')} r"
        " WHERE g.staging_id = r.staging_id",
        f"INSERT INTO {s}.staging_event SELECT * FROM {w('stg_event')}",
    ]
    second: list[str] = [
        f"INSERT INTO {s}.golden ({_cols(gold_cols)}) SELECT {_cols(gold_cols)}"
        f" FROM {w('gold_up')} ON CONFLICT (golden_id) DO UPDATE SET {gold_set}",
        f"UPDATE {s}.golden g SET status = 'retired', stale = false FROM {w('gold_ret')} r"
        " WHERE g.golden_id = r.golden_id",
        f"INSERT INTO {s}.golden_delta SELECT * FROM {w('gold_delta')}",
        f"INSERT INTO {s}.anomaly SELECT * FROM {w('anom')}",
        f"DELETE FROM {s}.pending_delta p USING {w('done')} d"
        " WHERE p.source_id = d.source_id AND p.source_key = d.source_key AND p.seq = d.seq",
        f"UPDATE {s}.meta SET value = (SELECT next_id FROM {w('meta')})"
        " WHERE key = 'next_staging_id'",
    ]
    with connect(dsn) as conn:
        for statement in first:
            conn.execute(statement)
        fault(settings, "reconcile_mid_apply")
        for statement in second:
            conn.execute(statement)
        fault(settings, "reconcile_before_commit")
        log_run(conn, spec, run_id, "reconcile", started, counts)
        drop_work_tables(conn, spec, "_wr_")
        conn.commit()
