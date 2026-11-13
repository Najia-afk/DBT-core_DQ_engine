"""State schema: relational side tables + AGE graph (labels, indexes). Idempotent."""

from __future__ import annotations

import json

import psycopg

from engine.core.config import ReferentialConfig

from .db import EDGE_LABELS, VERTEX_LABELS, Ctx, LaneError, q
from .values import PG_TYPES

SOURCE_RECORD_FIXED = (
    "_vid",
    "_source_id",
    "_source_key",
    "_row_hash",
    "_revision",
    "_status",
    "_first_run",
    "_last_change_run",
)
# Expression indexes make `WHERE x.v IN [...]` / `{bk: ...}` index-driven in Cypher.
PROP_INDEXES = {"Record": ("v",), "Block": ("v", "bk"), "Staging": ("v",), "Golden": ("v",)}


def attr_signature(cfg: ReferentialConfig) -> str:
    return json.dumps(
        [[a.name, a.type] for a in cfg.attributes] + [cfg.publication.provenance], sort_keys=True
    )


def _attr_columns(cfg: ReferentialConfig) -> str:
    return ", ".join(f"{q(a.name)} {PG_TYPES[a.type]}" for a in cfg.attributes)


def _golden_columns(cfg: ReferentialConfig) -> str:
    cols = []
    for a in cfg.attributes:
        cols.append(f"{q(a.name)} {PG_TYPES[a.type]}")
        if cfg.publication.provenance:
            cols.append(f"{q(a.name + '__src')} text")
    return ", ".join(cols)


def ddl(cfg: ReferentialConfig, schema: str) -> list[str]:
    s = q(schema)
    return [
        f"CREATE SCHEMA IF NOT EXISTS {s}",
        f"""CREATE TABLE IF NOT EXISTS {s}.source_record (
            _vid bigint NOT NULL, _source_id text NOT NULL, _source_key text NOT NULL,
            _row_hash text NOT NULL, _revision timestamp(6), _status text NOT NULL,
            _first_run text NOT NULL, _last_change_run text NOT NULL, {_attr_columns(cfg)},
            PRIMARY KEY (_source_id, _source_key))""",
        f"CREATE UNIQUE INDEX IF NOT EXISTS source_record_vid ON {s}.source_record (_vid)",
        f"CREATE TABLE IF NOT EXISTS {s}.pending (vid bigint PRIMARY KEY)",
        f"""CREATE TABLE IF NOT EXISTS {s}.source_delta (
            run_id text NOT NULL, source_id text NOT NULL, source_key text NOT NULL,
            kind text NOT NULL, old_hash text, new_hash text)""",
        f"""CREATE TABLE IF NOT EXISTS {s}.reject (
            run_id text NOT NULL, source_id text NOT NULL, source_key text NOT NULL,
            attribute text NOT NULL, rule text NOT NULL, action text NOT NULL)""",
        f"""CREATE TABLE IF NOT EXISTS {s}.anomaly (
            run_id text NOT NULL, kind text NOT NULL, subject text NOT NULL, detail text)""",
        f"""CREATE TABLE IF NOT EXISTS {s}.staging (
            staging_id bigint PRIMARY KEY, status text NOT NULL, created_run text NOT NULL)""",
        f"""CREATE TABLE IF NOT EXISTS {s}.staging_event (
            run_id text NOT NULL, kind text NOT NULL, staging_id bigint NOT NULL,
            related_id bigint)""",
        f"""CREATE TABLE IF NOT EXISTS {s}.golden (
            golden_id bigint PRIMARY KEY, status text NOT NULL, stale boolean NOT NULL,
            _published_once boolean NOT NULL, {_golden_columns(cfg)})""",
        f"""CREATE TABLE IF NOT EXISTS {s}.golden_delta (
            run_id text NOT NULL, golden_id bigint NOT NULL, attribute text NOT NULL,
            old_value text, new_value text, winning_source text)""",
        f"""CREATE TABLE IF NOT EXISTS {s}.run_log (
            id bigserial PRIMARY KEY, run_id text NOT NULL, referential text NOT NULL,
            step text NOT NULL, status text NOT NULL, started_at timestamptz NOT NULL,
            ended_at timestamptz, counts jsonb, error text)""",
        f"""CREATE UNIQUE INDEX IF NOT EXISTS run_log_done ON {s}.run_log (run_id, step)
            WHERE status IN ('done', 'blocked')""",
        f"CREATE TABLE IF NOT EXISTS {s}.meta (key text PRIMARY KEY, value text NOT NULL)",
    ]


def init(cfg: ReferentialConfig, dsn: str) -> dict:
    """Create extension, relational tables, graph, labels and indexes (all idempotent)."""
    from .db import connect

    with psycopg.connect(dsn, autocommit=True) as boot:
        try:
            boot.execute("CREATE EXTENSION IF NOT EXISTS age")
        except psycopg.Error as error:
            raise LaneError(f"cannot create the AGE extension: {error}") from error
    conn = connect(dsn)
    ctx = Ctx(cfg, conn)
    created = False
    try:
        with conn.transaction():
            conn.execute("SELECT pg_advisory_xact_lock(7771)")
            for stmt in ddl(cfg, ctx.schema):
                conn.execute(stmt)
            signature = attr_signature(cfg)
            row = conn.execute(
                f"SELECT value FROM {ctx.t('meta')} WHERE key = 'attributes'"
            ).fetchone()
            if row is None:
                conn.execute(f"INSERT INTO {ctx.t('meta')} VALUES ('attributes', %s)", (signature,))
            elif row[0] != signature:
                raise LaneError(
                    "the attribute list of the config changed since init: run `reset` first"
                )
            exists = conn.execute(
                "SELECT 1 FROM ag_catalog.ag_graph WHERE name = %s", (ctx.graph,)
            ).fetchone()
            if not exists:
                conn.execute("SELECT ag_catalog.create_graph(%s)", (ctx.graph,))
                created = True
            have = {
                r[0]
                for r in conn.execute(
                    "SELECT l.name FROM ag_catalog.ag_label l JOIN ag_catalog.ag_graph g"
                    " ON g.graphid = l.graph WHERE g.name = %s",
                    (ctx.graph,),
                ).fetchall()
            }
            for label in VERTEX_LABELS:
                if label not in have:
                    conn.execute("SELECT ag_catalog.create_vlabel(%s, %s)", (ctx.graph, label))
            for label in EDGE_LABELS:
                if label not in have:
                    conn.execute("SELECT ag_catalog.create_elabel(%s, %s)", (ctx.graph, label))
            ctx.load_labels()
            for label in (*VERTEX_LABELS, *EDGE_LABELS):
                conn.execute(f"CREATE INDEX IF NOT EXISTS ix_{label}_id ON {ctx.lt(label)} (id)")
            for label, props in PROP_INDEXES.items():
                for prop in props:
                    conn.execute(
                        f"CREATE INDEX IF NOT EXISTS ix_{label}_{prop} ON {ctx.lt(label)}"
                        f" (ag_catalog.agtype_access_operator(properties, '\"{prop}\"'::agtype))"
                    )
            for label in EDGE_LABELS:
                for col in ("start_id", "end_id"):
                    conn.execute(
                        f"CREATE INDEX IF NOT EXISTS ix_{label}_{col} ON {ctx.lt(label)} ({col})"
                    )
        return {"graph": ctx.graph, "schema": ctx.schema, "graph_created": created}
    finally:
        conn.close()


def reset(cfg: ReferentialConfig, dsn: str) -> dict:
    from .db import connect

    conn = connect(dsn)
    ctx = Ctx(cfg, conn)
    try:
        with conn.transaction():
            conn.execute("SELECT pg_advisory_xact_lock(7771)")
            dropped_graph = False
            ext = conn.execute("SELECT 1 FROM pg_extension WHERE extname = 'age'").fetchone()
            if (
                ext
                and conn.execute(
                    "SELECT 1 FROM ag_catalog.ag_graph WHERE name = %s", (ctx.graph,)
                ).fetchone()
            ):
                conn.execute("SELECT ag_catalog.drop_graph(%s, true)", (ctx.graph,))
                dropped_graph = True
            conn.execute(f"DROP SCHEMA IF EXISTS {q(ctx.schema)} CASCADE")
        return {"graph": ctx.graph, "schema": ctx.schema, "graph_dropped": dropped_graph}
    finally:
        conn.close()
