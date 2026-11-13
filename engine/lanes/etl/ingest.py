"""Ingest: stream a source, transform in the engine, bulk-load, MERGE by hash in SQL.

Per batch (own transaction, retried on transient errors): the narrow metadata of every row goes to
an UNLOGGED stage table; only rows whose hash differs from `source_record` also load their
attribute values into the wide stage table. After the last batch ONE transaction deduplicates,
checks completeness, merges into `source_record`, emits `source_delta` / `reject`, flags the
nodes in `pending_node` and closes the run log row: it is atomic and rolls back as a whole.
"""

from __future__ import annotations

import time
from typing import Any

import numpy as np
import psycopg

from engine.core.canonical import canonical_run_id
from engine.core.config import ReferentialConfig

from .arrowio import copy_in, ident
from .db import LaneError, Pg, advisory_key, fault, is_transient, run_begin, run_fail, run_finish
from .schema import PG_TYPES, require_initialised
from .sources import source_batches
from .transform import KIND_OK, SourceTransform

EPS = 1e-9


def _stage_names(source_id: str) -> tuple[str, str, str]:
    base = f"stg_{source_id[:40]}"
    return f"{base}_meta", f"{base}_val", f"{base}_rej"


def _create_stage(conn: psycopg.Connection[Any], tf: SourceTransform) -> None:
    meta, val, rej = _stage_names(tf.source.id)
    for name in (meta, val, rej):
        conn.execute(f"DROP TABLE IF EXISTS {ident(name)}")
    conn.execute(
        f'CREATE UNLOGGED TABLE {ident(meta)} (_ord bigint, source_key text COLLATE "C",'
        " revision timestamp, kind smallint, row_hash text)"
    )
    columns = ", ".join(f"{ident(a.name)} {PG_TYPES[a.type]}" for a in tf.attrs)
    conn.execute(f"CREATE UNLOGGED TABLE {ident(val)} (_ord bigint, {columns})")
    conn.execute(
        f'CREATE UNLOGGED TABLE {ident(rej)} (_ord bigint, source_key text COLLATE "C",'
        " attribute text, rule text, action text, scope smallint)"
    )


def _drop_stage(pg: Pg, source_id: str) -> None:
    try:
        conn = pg.conn()
        for name in _stage_names(source_id):
            conn.execute(f"DROP TABLE IF EXISTS {ident(name)}")
    except psycopg.Error:
        pass


def _stage_batch(conn: psycopg.Connection[Any], tf: SourceTransform, batch: Any) -> int:
    """Load one batch (single transaction). Returns the number of wide rows loaded."""
    meta, val, rej = _stage_names(tf.source.id)
    with conn.transaction():
        conn.execute(f"CREATE TEMP TABLE b_meta (LIKE {ident(meta)}) ON COMMIT DROP")
        copy_in(conn, "b_meta", batch.meta)
        rows = conn.execute(
            "SELECT m._ord FROM b_meta m LEFT JOIN source_record r"
            " ON r.source_id = %s AND r.source_key = m.source_key"
            " WHERE m.kind = %s AND r.row_hash IS DISTINCT FROM m.row_hash",
            [tf.source.id, KIND_OK],
        ).fetchall()
        needed = np.fromiter((r[0] for r in rows), dtype=np.int64, count=len(rows))
        conn.execute(f"INSERT INTO {ident(meta)} SELECT * FROM b_meta")
        wide = batch.values
        if wide.num_rows:
            ords = wide.column("_ord").to_numpy(zero_copy_only=False)
            wide = wide.filter(np.isin(ords, needed))
        copy_in(conn, ident(val), wide)
        copy_in(conn, ident(rej), batch.rejects)
    return wide.num_rows


def _merge_sql(tf: SourceTransform) -> dict[str, Any]:
    cols = [ident(a.name) for a in tf.attrs]
    return {
        "cols": ", ".join(cols),
        "vcols": ", ".join(f"v.{c}" for c in cols),
        "set": ", ".join(f"{c} = v.{c}" for c in cols),
    }


def _merge(pg: Pg, cfg: ReferentialConfig, tf: SourceTransform, run_id: str) -> dict[str, Any]:
    source = tf.source
    meta, val, rej = _stage_names(source.id)
    parts = _merge_sql(tf)
    sid = source.id

    def once(conn: psycopg.Connection[Any]) -> dict[str, Any]:
        counts: dict[str, Any] = {}
        with conn.transaction():
            conn.execute(
                "SELECT pg_advisory_xact_lock_shared(%s)", [advisory_key(pg.schema, "state")]
            )
            conn.execute(
                f"CREATE TEMP TABLE t_win ON COMMIT DROP AS SELECT DISTINCT ON (source_key)"
                f" _ord, source_key, revision, kind, row_hash FROM {ident(meta)}"
                " WHERE kind IN (0, 1) ORDER BY source_key, revision DESC NULLS LAST, _ord DESC"
            )
            conn.execute("CREATE UNIQUE INDEX ON t_win (_ord)")
            conn.execute("CREATE UNIQUE INDEX ON t_win (source_key)")
            conn.execute(
                f"CREATE TEMP TABLE t_present ON COMMIT DROP AS"
                f" SELECT DISTINCT source_key FROM {ident(meta)}"
            )
            conn.execute("CREATE UNIQUE INDEX ON t_present (source_key)")
            conn.execute("ANALYZE t_win")
            conn.execute("ANALYZE t_present")
            if source.snapshot == "full":
                present, total = conn.execute(
                    "SELECT count(p.source_key), count(*) FROM source_record r"
                    " LEFT JOIN t_present p ON p.source_key = r.source_key"
                    " WHERE r.source_id = %s AND r.status = 'active'",
                    [sid],
                ).fetchone()
                counts["active_keys"] = total
                if total:
                    ratio = present / total
                    if ratio < source.full_snapshot_min_ratio - EPS:
                        detail = f"ratio={ratio:.4f} min={source.full_snapshot_min_ratio}"
                        conn.execute(
                            "INSERT INTO anomaly (run_id, kind, subject, detail)"
                            " VALUES (%s, 'batch_blocked', %s, %s)",
                            [run_id, sid, detail],
                        )
                        counts.update(blocked=True, ratio=round(ratio, 4))
                        run_finish(conn, run_id, f"ingest:{sid}", "blocked", counts)
                        return counts
            conn.execute(
                "CREATE TEMP TABLE t_chg ON COMMIT DROP AS"
                " SELECT w._ord, w.source_key, w.revision, w.row_hash AS new_hash, r.node_id,"
                " r.row_hash AS old_hash,"
                " CASE WHEN r.node_id IS NULL OR r.status = 'retired' THEN 'I' ELSE 'U' END AS kind"
                " FROM t_win w LEFT JOIN source_record r"
                " ON r.source_id = %s AND r.source_key = w.source_key"
                " WHERE w.kind = 0 AND (r.node_id IS NULL OR r.status = 'retired'"
                " OR r.row_hash <> w.row_hash)",
                [sid],
            )
            fault("ingest:before_apply")
            n_new_i, n_new_u = conn.execute(
                "SELECT count(*) FILTER (WHERE kind = 'I'), count(*) FILTER (WHERE kind = 'U')"
                " FROM t_chg"
            ).fetchone()
            conn.execute(  # retired key back with the same hash: status/revision only
                "UPDATE source_record r SET status = 'active', revision = c.revision,"
                " last_change_run = %s FROM t_chg c WHERE r.source_id = %s"
                " AND r.source_key = c.source_key AND c.node_id IS NOT NULL"
                " AND r.row_hash = c.new_hash",
                [run_id, sid],
            )
            conn.execute(
                f"INSERT INTO source_record (source_id, source_key, row_hash, revision, status,"
                f" first_run, last_change_run, {parts['cols']})"
                f" SELECT %s, c.source_key, c.new_hash, c.revision, 'active', %s, %s,"
                f" {parts['vcols']} FROM t_chg c JOIN {ident(val)} v ON v._ord = c._ord"
                f" WHERE c.node_id IS NULL",
                [sid, run_id, run_id],
            )
            conn.execute(
                f"UPDATE source_record r SET row_hash = c.new_hash, revision = c.revision,"
                f" status = 'active', last_change_run = %s, {parts['set']}"
                f" FROM t_chg c JOIN {ident(val)} v ON v._ord = c._ord"
                f" WHERE r.source_id = %s AND r.source_key = c.source_key"
                f" AND c.node_id IS NOT NULL AND r.row_hash <> c.new_hash",
                [run_id, sid],
            )
            conn.execute(
                "INSERT INTO source_delta (run_id, source_id, source_key, kind, old_hash,"
                " new_hash, node_id) SELECT %s, %s, c.source_key, c.kind,"
                " CASE WHEN c.kind = 'U' THEN c.old_hash END, c.new_hash, r.node_id"
                " FROM t_chg c JOIN source_record r"
                " ON r.source_id = %s AND r.source_key = c.source_key",
                [run_id, sid, sid],
            )
            conn.execute(
                "INSERT INTO pending_node SELECT r.node_id FROM t_chg c JOIN source_record r"
                " ON r.source_id = %s AND r.source_key = c.source_key ON CONFLICT DO NOTHING",
                [sid],
            )
            deleted = 0
            if source.snapshot == "full":
                deleted = conn.execute(
                    "WITH gone AS (UPDATE source_record r SET status = 'retired',"
                    " last_change_run = %s WHERE r.source_id = %s AND r.status = 'active'"
                    " AND NOT EXISTS (SELECT 1 FROM t_present p WHERE p.source_key = r.source_key)"
                    " RETURNING r.node_id, r.source_key, r.row_hash),"
                    " d AS (INSERT INTO source_delta (run_id, source_id, source_key, kind,"
                    " old_hash, new_hash, node_id) SELECT %s, %s, source_key, 'D', row_hash,"
                    " NULL, node_id FROM gone RETURNING node_id)"
                    " INSERT INTO pending_node SELECT node_id FROM d ON CONFLICT DO NOTHING",
                    [run_id, sid, run_id, sid],
                ).rowcount
            rejects = conn.execute(
                "INSERT INTO reject (run_id, source_id, source_key, attribute, rule, action)"
                " SELECT %s, %s, s.source_key, s.attribute, s.rule, s.action"
                f" FROM {ident(rej)} s WHERE s.scope = 0 OR EXISTS"
                " (SELECT 1 FROM t_win w WHERE w._ord = s._ord)"
                " UNION ALL SELECT %s, %s, m.source_key, '', 'duplicate_key', 'reject_record'"
                f" FROM {ident(meta)} m WHERE m.kind IN (0, 1) AND NOT EXISTS"
                " (SELECT 1 FROM t_win w WHERE w._ord = m._ord)",
                [run_id, sid, run_id, sid],
            ).rowcount
            winners_ok, winners_dq = conn.execute(
                "SELECT count(*) FILTER (WHERE kind = 0), count(*) FILTER (WHERE kind = 1)"
                " FROM t_win"
            ).fetchone()
            counts.update(
                inserted=n_new_i,
                updated=n_new_u,
                deleted=deleted,
                rejected_records=winners_dq,
                reject_rows=rejects,
                valid_records=winners_ok,
                blocked=False,
            )
            run_finish(conn, run_id, f"ingest:{sid}", "ok", counts)
        return counts

    return pg.run(once, f"ingest {sid} merge")


def ingest(cfg: ReferentialConfig, source_id: str, run_id: str, pg: Pg) -> dict[str, Any]:
    run_id = canonical_run_id(run_id)
    if source_id not in {s.id for s in cfg.sources}:
        raise LaneError(f"unknown source {source_id!r}")
    step = f"ingest:{source_id}"
    require_initialised(pg.conn(), cfg)
    previous = run_begin(pg.conn(), cfg, run_id, step)
    if previous is not None:
        return {"replayed": True, **previous}
    tf = SourceTransform(cfg, source_id)
    lock = advisory_key(pg.schema, step)
    conn = pg.conn()
    if not conn.execute("SELECT pg_try_advisory_lock(%s)", [lock]).fetchone()[0]:
        raise LaneError(f"another ingest of source {source_id} is running")
    phases = {"extract": 0.0, "transform": 0.0, "stage": 0.0, "merge": 0.0}
    totals = {"rows_read": 0, "batches": 0, "key_null": 0, "wide_rows": 0, "retries": 0}
    try:
        for attempt in range(1, cfg.runtime.max_attempts + 1):
            phases = {k: 0.0 for k in phases}
            totals = {k: 0 for k in totals}
            try:
                pg.run(lambda c: _create_stage(c, tf), f"ingest {source_id} stage")
                _stream(cfg, tf, pg, phases, totals)
                t0 = time.perf_counter()
                counts = _merge(pg, cfg, tf, run_id)
                phases["merge"] = time.perf_counter() - t0
                break
            except LaneError:
                raise
            except Exception as error:
                if not is_transient(error) or attempt == cfg.runtime.max_attempts:
                    raise
                pg.note_retry()
                totals["retries"] += 1
        else:  # pragma: no cover
            raise LaneError("ingest: retries exhausted")
    except BaseException as error:
        try:
            run_fail(pg.conn(), run_id, step, str(error))
        except psycopg.Error:
            pass
        raise
    finally:
        _drop_stage(pg, source_id)
        try:
            pg.conn().execute("SELECT pg_advisory_unlock(%s)", [lock])
        except psycopg.Error:
            pass
    counts.update(totals)
    counts["retries"] = totals["retries"] + pg.retries
    counts["seconds"] = {k: round(v, 3) for k, v in phases.items()}
    return counts


def _stream(
    cfg: ReferentialConfig,
    tf: SourceTransform,
    pg: Pg,
    phases: dict[str, float],
    totals: dict[str, int],
) -> None:
    ordinal = 0
    iterator = iter(source_batches(cfg, tf.source, tf.columns))
    while True:
        t0 = time.perf_counter()
        raw = next(iterator, None)
        phases["extract"] += time.perf_counter() - t0
        if raw is None:
            break
        t0 = time.perf_counter()
        batch = tf.run(raw, ordinal)
        phases["transform"] += time.perf_counter() - t0
        ordinal += raw.num_rows
        t0 = time.perf_counter()
        label = f"ingest {tf.source.id} batch"
        wide = pg.run(lambda c, b=batch: _stage_batch(c, tf, b), label)
        phases["stage"] += time.perf_counter() - t0
        totals["rows_read"] += raw.num_rows
        totals["batches"] += 1
        totals["key_null"] += batch.key_null
        totals["wide_rows"] += wide
        fault("ingest:after_batch")
