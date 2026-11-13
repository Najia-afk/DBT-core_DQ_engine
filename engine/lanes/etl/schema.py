"""State schema of the ETL lane: versioned migrations, attribute column sync, reset.

Tables (contract section 4, physical layout of this lane): node identity is a surrogate
`node_id` (identity column of `source_record`); pairs, blocking index and stagings refer to it.
`source_record` and `golden` have one typed column per attribute (`golden` also `<attr>__src`
when `publication.provenance`).
"""

from __future__ import annotations

from typing import Any

import psycopg

from engine.core.config import ReferentialConfig

from .arrowio import ident
from .db import LaneError, Pg, advisory_key

PG_TYPES = {
    "string": "text",
    "int": "bigint",
    "decimal": "numeric(18,4)",
    "date": "date",
    "timestamp": "timestamp",
    "bool": "boolean",
}
SOURCE_RECORD_FIXED = {
    "node_id", "source_id", "source_key", "row_hash", "revision", "status", "first_run",
    "last_change_run",
}  # fmt: skip
GOLDEN_FIXED = {"golden_id", "status", "stale", "published_once"}
_KEY = 'text COLLATE "C" NOT NULL'


def check_names(cfg: ReferentialConfig) -> None:
    """Attribute names must not collide with the physical columns of the lane."""
    for attr in cfg.attributes:
        if attr.name in SOURCE_RECORD_FIXED | GOLDEN_FIXED:
            raise LaneError(
                f"attribute name {attr.name!r} collides with a physical column of the state schema"
            )
        longest = len(attr.name) + (5 if cfg.publication.provenance else 0)
        if longest > 63:
            raise LaneError(f"attribute {attr.name!r}: <name>__src exceeds 63 characters")
    if len(cfg.state.schema_name) > 63:
        raise LaneError("state schema name too long")


def attr_columns(cfg: ReferentialConfig, *, golden: bool) -> list[tuple[str, str]]:
    cols: list[tuple[str, str]] = []
    for attr in cfg.attributes:
        cols.append((attr.name, PG_TYPES[attr.type]))
        if golden and cfg.publication.provenance:
            cols.append((f"{attr.name}__src", "text"))
    return cols


def _cols(cols: list[tuple[str, str]]) -> str:
    return ",\n  ".join(f"{ident(n)} {t}" for n, t in cols)


def migration_v1(cfg: ReferentialConfig) -> list[str]:
    sr_cols = _cols(attr_columns(cfg, golden=False))
    g_cols = _cols(attr_columns(cfg, golden=True))
    return [
        f"""CREATE TABLE source_record (
  node_id bigint GENERATED ALWAYS AS IDENTITY,
  source_id {_KEY},
  source_key {_KEY},
  row_hash text NOT NULL,
  revision timestamp,
  status text NOT NULL,
  first_run text NOT NULL,
  last_change_run text NOT NULL,
  {sr_cols},
  PRIMARY KEY (source_id, source_key)
)""",
        "CREATE UNIQUE INDEX source_record_node ON source_record (node_id)",
        """CREATE TABLE source_delta (
  run_id text NOT NULL, source_id text NOT NULL, source_key text NOT NULL, kind text NOT NULL,
  old_hash text, new_hash text, node_id bigint NOT NULL
)""",
        "CREATE INDEX source_delta_run ON source_delta (run_id)",
        "CREATE TABLE pending_node (node_id bigint PRIMARY KEY)",
        """CREATE TABLE match_pair (
  left_id bigint NOT NULL, right_id bigint NOT NULL, score double precision,
  contradiction boolean NOT NULL, eligible boolean NOT NULL, decision text NOT NULL,
  reason text NOT NULL, run_id text NOT NULL, PRIMARY KEY (left_id, right_id)
)""",
        "CREATE INDEX match_pair_right ON match_pair (right_id)",
        """CREATE TABLE block_index (
  node_id bigint NOT NULL, bname text COLLATE "C" NOT NULL, bvalue text COLLATE "C" NOT NULL,
  PRIMARY KEY (node_id, bname)
)""",
        "CREATE INDEX block_index_lookup ON block_index (bname, bvalue, node_id)",
        "CREATE SEQUENCE staging_seq",
        "CREATE TABLE staging (staging_id bigint PRIMARY KEY, status text NOT NULL,"
        " created_run text NOT NULL)",
        "CREATE TABLE staging_member (node_id bigint PRIMARY KEY, staging_id bigint NOT NULL)",
        "CREATE INDEX staging_member_staging ON staging_member (staging_id)",
        "CREATE TABLE staging_event (run_id text NOT NULL, kind text NOT NULL,"
        " staging_id bigint NOT NULL, related_id bigint)",
        f"""CREATE TABLE golden (
  golden_id bigint PRIMARY KEY, status text NOT NULL, stale boolean NOT NULL,
  published_once boolean NOT NULL DEFAULT false,
  {g_cols}
)""",
        "CREATE TABLE golden_delta (run_id text NOT NULL, golden_id bigint NOT NULL,"
        " attribute text NOT NULL, old_value text, new_value text, winning_source text)",
        "CREATE INDEX golden_delta_run ON golden_delta (run_id)",
        "CREATE TABLE reject (run_id text NOT NULL, source_id text NOT NULL,"
        " source_key text NOT NULL, attribute text NOT NULL, rule text NOT NULL,"
        " action text NOT NULL)",
        "CREATE INDEX reject_run ON reject (run_id)",
        "CREATE TABLE anomaly (run_id text NOT NULL, kind text NOT NULL, subject text NOT NULL,"
        " detail text)",
        """CREATE TABLE run_log (
  run_id text NOT NULL, referential text NOT NULL, step text NOT NULL, status text NOT NULL,
  started_at timestamptz, ended_at timestamptz, counts jsonb, error text,
  PRIMARY KEY (run_id, step)
)""",
        """CREATE VIEW match_pair_keyed AS
SELECT p.left_id, p.right_id, p.score, p.contradiction, p.decision, p.reason, p.run_id,
  CASE WHEN o THEN ls ELSE rs END AS left_source_id, CASE WHEN o THEN lk ELSE rk END AS left_key,
  CASE WHEN o THEN rs ELSE ls END AS right_source_id, CASE WHEN o THEN rk ELSE lk END AS right_key
FROM (
  SELECT m.*, l.source_id AS ls, l.source_key AS lk, r.source_id AS rs, r.source_key AS rk,
    ((l.source_id, l.source_key) < (r.source_id, r.source_key)) AS o
  FROM match_pair m JOIN source_record l ON l.node_id = m.left_id
  JOIN source_record r ON r.node_id = m.right_id
) p""",
        """CREATE VIEW review AS
SELECT run_id, left_source_id, left_key, right_source_id, right_key, score, reason
FROM match_pair_keyed WHERE decision = 'review'""",
    ]


MIGRATIONS = [(1, "base tables", migration_v1)]


def _existing_columns(conn: psycopg.Connection[Any], schema: str, table: str) -> dict[str, str]:
    rows = conn.execute(
        "SELECT a.attname, format_type(a.atttypid, a.atttypmod) FROM pg_attribute a"
        " JOIN pg_class c ON c.oid = a.attrelid JOIN pg_namespace n ON n.oid = c.relnamespace"
        " WHERE n.nspname = %s AND c.relname = %s AND a.attnum > 0 AND NOT a.attisdropped",
        [schema, table],
    ).fetchall()
    return {r[0]: r[1] for r in rows}


def sync_attribute_columns(conn: psycopg.Connection[Any], cfg: ReferentialConfig) -> int:
    """Add the columns of attributes introduced after the first init; refuse type changes."""
    added = 0
    for table, golden in (("source_record", False), ("golden", True)):
        have = _existing_columns(conn, cfg.state.schema_name, table)
        for name, pg_type in attr_columns(cfg, golden=golden):
            shown = "timestamp without time zone" if pg_type == "timestamp" else pg_type
            if name not in have:
                conn.execute(f"ALTER TABLE {ident(table)} ADD COLUMN {ident(name)} {pg_type}")
                added += 1
            elif have[name] != shown:
                raise LaneError(
                    f"{table}.{name}: column type {have[name]} differs from config ({pg_type});"
                    " changing the type of an attribute needs a reset"
                )
    return added


def init(cfg: ReferentialConfig, pg: Pg) -> dict[str, Any]:
    """Create or upgrade the state schema (idempotent, serialised by an advisory lock)."""
    check_names(cfg)
    schema = cfg.state.schema_name
    conn = pg.conn()
    applied: list[int] = []
    with conn.transaction():
        conn.execute("SELECT pg_advisory_xact_lock(%s)", [advisory_key(schema, "init")])
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {ident(schema)}")
        conn.execute(
            f"CREATE TABLE IF NOT EXISTS {ident(schema)}.schema_version (version int PRIMARY KEY,"
            " description text NOT NULL, applied_at timestamptz NOT NULL DEFAULT now())"
        )
        row = conn.execute(f"SELECT coalesce(max(version), 0) FROM {ident(schema)}.schema_version")
        current = row.fetchone()[0]
        conn.execute(f"SET LOCAL search_path = {ident(schema)}")
        for version, description, build in MIGRATIONS:
            if version <= current:
                continue
            for statement in build(cfg):
                conn.execute(statement)
            conn.execute(
                "INSERT INTO schema_version (version, description) VALUES (%s, %s)",
                [version, description],
            )
            applied.append(version)
        added = sync_attribute_columns(conn, cfg)
        version_now = conn.execute("SELECT max(version) FROM schema_version").fetchone()[0]
    return {"schema": schema, "version": version_now, "migrations_applied": applied,
            "columns_added": added}  # fmt: skip


def require_initialised(conn: psycopg.Connection[Any], cfg: ReferentialConfig) -> None:
    ok = conn.execute(
        "SELECT to_regclass(%s) IS NOT NULL", [f"{ident(cfg.state.schema_name)}.run_log"]
    ).fetchone()[0]
    if not ok:
        raise LaneError(
            f"state schema {cfg.state.schema_name!r} is not initialised: run `init` first"
        )


def reset(cfg: ReferentialConfig, pg: Pg) -> dict[str, Any]:
    schema = cfg.state.schema_name
    conn = pg.conn()
    conn.execute("SELECT pg_advisory_lock(%s)", [advisory_key(schema, "init")])
    try:
        existed = conn.execute(
            "SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = %s)", [schema]
        ).fetchone()[0]
        conn.execute(f"DROP SCHEMA IF EXISTS {ident(schema)} CASCADE")
    finally:
        conn.execute("SELECT pg_advisory_unlock(%s)", [advisory_key(schema, "init")])
    return {"schema": schema, "dropped": bool(existed)}
