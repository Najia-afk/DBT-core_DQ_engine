"""Export of the canonical projections to Parquet (`<out>/<table>.parquet`, one snapshot)."""

from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Any

import psycopg
import pyarrow as pa
import pyarrow.parquet as pq
from psycopg import sql

from engine.core.canonical import canonical_schemas
from engine.core.config import ReferentialConfig

from .arrowio import copy_out, ident
from .db import Pg
from .schema import require_initialised


def _queries(cfg: ReferentialConfig, conn: psycopg.Connection[Any]) -> dict[str, str]:
    attrs = ", ".join(ident(a.name) for a in cfg.attributes)
    golden_cols = []
    for a in cfg.attributes:
        golden_cols.append(ident(a.name))
        if cfg.publication.provenance:
            golden_cols.append(ident(f"{a.name}__src"))
    version = sql.Literal(cfg.matching.version).as_string(conn)
    return {
        "source_record": "SELECT source_id, source_key, row_hash, revision, status, first_run,"
        f" last_change_run, {attrs} FROM source_record",
        "source_delta": "SELECT run_id, source_id, source_key, kind, old_hash, new_hash"
        " FROM source_delta",
        "match_pair": "SELECT left_source_id, left_key, right_source_id, right_key, score,"
        f" decision, contradiction, {version} FROM match_pair_keyed",
        "staging": "SELECT staging_id, status, created_run FROM staging",
        "staging_member": "SELECT m.staging_id, r.source_id, r.source_key FROM staging_member m"
        " JOIN source_record r ON r.node_id = m.node_id",
        "staging_event": "SELECT run_id, kind, staging_id, related_id FROM staging_event",
        "golden": f"SELECT golden_id, status, stale, {', '.join(golden_cols)} FROM golden",
        "golden_delta": "SELECT run_id, golden_id, attribute, old_value, new_value, winning_source"
        " FROM golden_delta",
        "review": "SELECT run_id, left_source_id, left_key, right_source_id, right_key, score,"
        " reason FROM review",
        "reject": "SELECT run_id, source_id, source_key, attribute, rule, action FROM reject",
        "anomaly": "SELECT run_id, kind, subject, detail FROM anomaly",
    }


def export(cfg: ReferentialConfig, pg: Pg, out_dir: str) -> dict[str, Any]:
    """Write every canonical table from ONE consistent snapshot (repeatable read, read only)."""
    require_initialised(pg.conn(), cfg)
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    schemas = canonical_schemas(cfg)
    rows: dict[str, int] = {}
    started = time.perf_counter()
    seconds: dict[str, float] = {}

    def run(conn: psycopg.Connection[Any]) -> None:
        with conn.transaction():
            conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            queries = _queries(cfg, conn)
            for name, schema in schemas.items():
                t0 = time.perf_counter()
                final = out / f"{name}.parquet"
                part = out / f"{name}.parquet.part"
                count = 0
                with pq.ParquetWriter(part, schema) as writer:
                    for batch in copy_out(conn, queries[name], schema):
                        table = pa.Table.from_batches([batch]).cast(schema)
                        writer.write_table(table)
                        count += table.num_rows
                os.replace(part, final)
                rows[name] = count
                seconds[name] = round(time.perf_counter() - t0, 3)

    pg.run(run, "export")
    return {
        "out": str(out),
        "tables": rows,
        "seconds": seconds,
        "total_rows": sum(rows.values()),
        "export_seconds": round(time.perf_counter() - started, 3),
    }
