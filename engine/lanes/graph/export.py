"""EXPORT: canonical Parquet projections (schemas of `engine.core.canonical`).

match_pair, review and staging_member are read back through openCypher (the graph is the
source of truth for them); the other tables are relational side tables.
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any

import pyarrow as pa

from engine.core.canonical import canonical_schemas, write_parquet

from .db import ENTRY_MASK, Ctx, connect, q


def _select(
    ctx: Ctx, table: str, schema: pa.Schema, columns: dict[str, str] | None = None
) -> pa.Table:
    columns = columns or {}
    cols = ", ".join(q(columns.get(f.name, f.name)) for f in schema)
    rows = ctx.conn.execute(f"SELECT {cols} FROM {ctx.t(table)}").fetchall()
    data = {f.name: [r[i] for r in rows] for i, f in enumerate(schema)}
    return pa.Table.from_pydict(data, schema=schema)


def build_tables(ctx: Ctx) -> dict[str, pa.Table]:
    cfg = ctx.cfg
    schemas = canonical_schemas(cfg)
    out: dict[str, pa.Table] = {}
    renames = {
        "_source_id": "source_id",
        "_source_key": "source_key",
        "_row_hash": "row_hash",
        "_revision": "revision",
        "_status": "status",
        "_first_run": "first_run",
        "_last_change_run": "last_change_run",
    }
    out["source_record"] = _select(
        ctx, "source_record", schemas["source_record"], {v: k for k, v in renames.items()}
    )
    for name in ("source_delta", "reject", "anomaly", "staging", "staging_event", "golden_delta"):
        out[name] = _select(ctx, name, schemas[name])
    out["golden"] = _select(ctx, "golden", schemas["golden"])

    node = {
        vid: (sid, key)
        for vid, sid, key in ctx.conn.execute(
            f"SELECT _vid, _source_id, _source_key FROM {ctx.t('source_record')}"
        ).fetchall()
    }
    pairs = ctx.cypher(
        "MATCH (a:Record)-[m:MATCH]->(b:Record) RETURN a.v, b.v, m.s, m.c, m.d",
        ("int", "int", "float", "bool", "str"),
    )
    out["match_pair"] = pa.Table.from_pydict(
        {
            "left_source_id": [node[a][0] for a, *_ in pairs],
            "left_key": [node[a][1] for a, *_ in pairs],
            "right_source_id": [node[b][0] for _, b, *_ in pairs],
            "right_key": [node[b][1] for _, b, *_ in pairs],
            "score": [p[2] for p in pairs],
            "decision": [p[4] for p in pairs],
            "contradiction": [p[3] for p in pairs],
            "rule_version": [cfg.matching.version] * len(pairs),
        },
        schema=schemas["match_pair"],
    )
    reviews = ctx.cypher(
        "MATCH (a:Record)-[m:REVIEW]->(b:Record) RETURN a.v, b.v, m.s, m.r, m.run",
        ("int", "int", "float", "str", "str"),
    )
    out["review"] = pa.Table.from_pydict(
        {
            "run_id": [r[4] for r in reviews],
            "left_source_id": [node[r[0]][0] for r in reviews],
            "left_key": [node[r[0]][1] for r in reviews],
            "right_source_id": [node[r[1]][0] for r in reviews],
            "right_key": [node[r[1]][1] for r in reviews],
            "score": [r[2] for r in reviews],
            "reason": [r[3] for r in reviews],
        },
        schema=schemas["review"],
    )
    members = ctx.cypher(
        "MATCH (r:Record)-[:MEMBER_OF]->(s:Staging) RETURN r.v, s.v", ("int", "int")
    )
    out["staging_member"] = pa.Table.from_pydict(
        {
            "staging_id": [s & ENTRY_MASK for _, s in members],
            "source_id": [node[r][0] for r, _ in members],
            "source_key": [node[r][1] for r, _ in members],
        },
        schema=schemas["staging_member"],
    )
    return out


def export(cfg: Any, dsn: str, out_dir: str) -> dict[str, Any]:
    t0 = time.perf_counter()
    conn = connect(dsn)
    ctx = Ctx(cfg, conn)
    try:
        ctx.load_labels()
        with conn.transaction():
            conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            tables = build_tables(ctx)
    finally:
        conn.close()
    t1 = time.perf_counter()
    write_parquet(tables, Path(out_dir))
    return {
        "lane": "graph",
        "command": "export",
        "status": "ok",
        "out": str(out_dir),
        "counts": {name: t.num_rows for name, t in tables.items()},
        "seconds": {"read": round(t1 - t0, 4), "write": round(time.perf_counter() - t1, 4)},
    }
