"""Cold recompute helper (acceptance property n°1): replay the ACTIVE source records of a state
into a fresh schema and reconcile them in one run. Test/diagnostic tool, not part of the CLI."""

from __future__ import annotations

from typing import Any

from engine.core.config import ReferentialConfig

from . import schema as schema_mod
from .db import Ctx, connect, q
from .ingest import _vertex_props
from .reconcile import reconcile


def replay_active(
    cfg: ReferentialConfig, dsn: str, scratch: ReferentialConfig, run_id: str = "scratch"
) -> dict[str, Any]:
    """`scratch` is the same config with another `state.schema`; it is (re)initialised."""
    schema_mod.reset(scratch, dsn)
    schema_mod.init(scratch, dsn)
    conn = connect(dsn)
    src, dst = Ctx(cfg, conn), Ctx(scratch, conn)
    try:
        dst.load_labels()
        with conn.transaction():
            rows = conn.execute(
                f"SELECT _source_id, _source_key FROM {src.t('source_record')}"
                " WHERE _status = 'active' ORDER BY _source_id, _source_key"
            ).fetchall()
            vids = dst.alloc("Record", len(rows))
            dst.copy_vertices(
                "Record",
                vids,
                [_vertex_props(int(v), s, k) for v, (s, k) in zip(vids, rows, strict=True)],
            )
            conn.execute("CREATE TEMP TABLE _m (sid text, k text, vid bigint) ON COMMIT DROP")
            with conn.cursor() as cur, cur.copy("COPY _m FROM STDIN") as cp:
                for v, (s, k) in zip(vids.tolist(), rows, strict=True):
                    cp.write_row((s, k, v))
            names = ", ".join(q(a.name) for a in cfg.attributes)
            e_names = ", ".join(f"e.{q(a.name)}" for a in cfg.attributes)
            conn.execute(
                f"INSERT INTO {dst.t('source_record')} (_vid, _source_id, _source_key, _row_hash,"
                f" _revision, _status, _first_run, _last_change_run, {names})"
                " SELECT m.vid, e._source_id, e._source_key, e._row_hash, e._revision, 'active',"
                f" %s, %s, {e_names} FROM {src.t('source_record')} e"
                " JOIN _m m ON m.sid = e._source_id AND m.k = e._source_key",
                (run_id, run_id),
            )
            conn.execute(f"INSERT INTO {dst.t('pending')} SELECT vid FROM _m")
    finally:
        conn.close()
    return reconcile(scratch, run_id, dsn)
