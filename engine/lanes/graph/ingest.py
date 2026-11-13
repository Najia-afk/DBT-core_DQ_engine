"""INGEST (contract section 2, step 1): normalise + DQ + row_hash, merge into source_record, emit
source_delta. One transaction per (run, source); idempotent for the same run id."""

from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pacsv

from engine.core.canonical import arrow_type, canonical_run_id
from engine.core.config import ReferentialConfig, Source

from . import values as V
from .db import Ctx, LaneError, connect, q
from .sources import read_source

EPS = 1e-9


def fail_point(name: str) -> None:
    """Failure injection for tests: simulate a kill (no cleanup, connection dropped)."""
    if os.environ.get("MDM_GRAPH_FAIL_AT") == name:
        os._exit(137)


@dataclass
class Batch:
    n_rows: int
    present: pa.Array
    keys: pa.Array
    revision: pa.Array
    row_hash: list[str]
    values: dict[str, pa.Array]
    rejects: list[tuple[str, str, str, str]] = field(default_factory=list)
    rejected_records: int = 0
    duplicates: int = 0


def build_batch(cfg: ReferentialConfig, source: Source, table: pa.Table) -> Batch:
    """Reference ingest semantics (see engine.core.reference docstring), vectorised."""
    n = table.num_rows
    keys = V.string_keys(table[source.key])
    key_ok = pc.and_(pc.is_valid(keys), pc.invert(pc.fill_null(pc.equal(keys, ""), True)))
    key_ok_np = key_ok.to_numpy(zero_copy_only=False)
    rejects: list[tuple[str, str, str, str]] = [("", "", "key_null", "reject_record")] * int(
        n - key_ok_np.sum()
    )
    present = pc.unique(keys.filter(key_ok))
    if source.revision:
        revision, bad_rev = V.parse_revision(table[source.revision])
    else:
        revision, bad_rev = pa.nulls(n, pa.timestamp("us")), np.zeros(n, dtype=bool)
    keys_py = keys.to_pylist() if bad_rev.any() else None
    for i in np.flatnonzero(key_ok_np & bad_rev).tolist():
        rejects.append((keys_py[i], "", "revision", "reject_record"))
    candidates = np.flatnonzero(key_ok_np & ~bad_rev)
    has_rev = pc.is_valid(revision).to_numpy(zero_copy_only=False)
    rev_us = pc.fill_null(revision.cast(pa.int64()), 0).to_numpy(zero_copy_only=False)
    codes = (
        keys.take(pa.array(candidates)).dictionary_encode().indices.to_numpy(zero_copy_only=False)
    )
    order = np.lexsort((candidates, rev_us[candidates], has_rev[candidates], codes))
    sorted_idx, sorted_codes = candidates[order], codes[order]
    last = np.ones(len(order), dtype=bool)
    last[:-1] = sorted_codes[:-1] != sorted_codes[1:]
    losers = sorted_idx[~last]
    winners = np.sort(sorted_idx[last])
    for k in keys.take(pa.array(losers)).to_pylist():
        rejects.append((k, "", "duplicate_key", "reject_record"))
    win_idx = pa.array(winners)
    win_keys = keys.take(win_idx)
    win_revision = revision.take(win_idx)
    mapping = cfg.mapping(source.id)
    rejected = np.zeros(len(winners), dtype=bool)
    columns: dict[str, pa.Array] = {}
    key_list: list[str] | None = None
    for attr in cfg.attributes:
        column = mapping.get(attr.name)
        if column is None:
            columns[attr.name] = pa.nulls(len(winners), arrow_type(attr.type))
            continue
        result = V.process_column(attr, table[column].take(win_idx))
        columns[attr.name] = result.values
        for idx, rule, action in result.failures:
            key_list = key_list if key_list is not None else win_keys.to_pylist()
            rejects.extend((key_list[i], attr.name, rule, action) for i in idx.tolist())
            if action == "reject_record":
                rejected[idx] = True
    keep = pa.array(~rejected)
    kept_values = {name: arr.filter(keep) for name, arr in columns.items()}
    kept_count = int((~rejected).sum())
    return Batch(
        n_rows=n,
        present=present,
        keys=win_keys.filter(keep),
        revision=win_revision.filter(keep),
        row_hash=V.row_hash_column(cfg, source.id, kept_values, kept_count),
        values=kept_values,
        rejects=rejects,
        rejected_records=int(rejected.sum()),
        duplicates=len(losers),
    )


def copy_arrow(
    ctx: Ctx, target: str, columns: list[str], table: pa.Table, rows: int = 100_000
) -> None:
    cols = ", ".join(columns)
    with ctx.conn.cursor() as cur:
        with cur.copy(f"COPY {target} ({cols}) FROM STDIN WITH (FORMAT csv)") as cp:
            for start in range(0, table.num_rows, rows):
                sink = pa.BufferOutputStream()
                pacsv.write_csv(
                    table.slice(start, rows), sink, pacsv.WriteOptions(include_header=False)
                )
                cp.write(sink.getvalue().to_pybytes())


def _vertex_props(vid: int, source_id: str, key: str) -> str:
    return json.dumps(
        {"v": vid, "s": source_id, "k": key}, ensure_ascii=False, separators=(",", ":")
    )


def _timed(timings: dict[str, float], name: str, t0: float) -> float:
    now = time.perf_counter()
    timings[name] = round(now - t0, 4)
    return now


def ingest(cfg: ReferentialConfig, source_id: str, run_id: str, dsn: str) -> dict[str, Any]:
    run_id = canonical_run_id(run_id)
    try:
        source = cfg.source(source_id)
    except StopIteration:
        raise LaneError(f"unknown source {source_id!r}") from None
    step = f"ingest:{source_id}"
    conn = connect(dsn)
    ctx = Ctx(cfg, conn)
    started = datetime.now(UTC)
    try:
        ctx.load_labels()
        with conn.transaction():
            ctx.lock(exclusive=False)
            prior = ctx.done_counts(run_id, step)
            if prior is not None:
                return _summary(source_id, run_id, prior, {}, replayed=True)
            summary = _ingest_tx(ctx, source, run_id, step, started)
            fail_point("ingest_before_commit")
        return summary
    finally:
        conn.close()


def _ingest_tx(ctx: Ctx, source: Source, run_id: str, step: str, started: datetime) -> dict:
    cfg, conn = ctx.cfg, ctx.conn
    timings: dict[str, float] = {}
    t = time.perf_counter()
    table = read_source(cfg, source)
    t = _timed(timings, "read", t)
    batch = build_batch(cfg, source, table)
    del table
    t = _timed(timings, "transform", t)
    srec = ctx.t("source_record")
    counts = {
        "rows_read": batch.n_rows,
        "rejected_records": batch.rejected_records,
        "duplicate_rows": batch.duplicates,
        "inserted": 0,
        "updated": 0,
        "deleted": 0,
        "unchanged": 0,
        "blocked": False,
    }
    conn.execute("CREATE TEMP TABLE _present (k text) ON COMMIT DROP")
    copy_arrow(ctx, "_present", ["k"], pa.table({"k": batch.present}))
    active, inter = conn.execute(
        f"SELECT count(*), count(p.k) FROM {srec} e LEFT JOIN _present p ON p.k = e._source_key"
        " WHERE e._source_id = %s AND e._status = 'active'",
        (source.id,),
    ).fetchone()
    if source.snapshot == "full" and active:
        ratio = inter / active
        if ratio < source.full_snapshot_min_ratio - EPS:
            conn.execute(
                f"INSERT INTO {ctx.t('anomaly')} VALUES (%s, 'batch_blocked', %s, %s)",
                (run_id, source.id, f"ratio={ratio:.4f} min={source.full_snapshot_min_ratio}"),
            )
            counts["blocked"] = True
            ctx.log_step(run_id, step, "blocked", started, counts)
            return _summary(source.id, run_id, counts, timings, status="blocked")

    attr_defs = ", ".join(f"{q(a.name)} {V.PG_TYPES[a.type]}" for a in cfg.attributes)
    names = [q(a.name) for a in cfg.attributes]
    conn.execute(
        "CREATE TEMP TABLE _batch (_source_key text, _row_hash text, _revision timestamp(6),"
        f" {attr_defs}) ON COMMIT DROP"
    )
    out = pa.table(
        {
            "_source_key": batch.keys,
            "_row_hash": pa.array(batch.row_hash, type=pa.string()),
            "_revision": batch.revision,
            **{a.name: batch.values[a.name] for a in cfg.attributes},
        }
    )
    copy_arrow(ctx, "_batch", ["_source_key", "_row_hash", "_revision", *names], out)
    conn.execute("CREATE INDEX ON _batch (_source_key)")
    conn.execute(
        "CREATE TEMP TABLE _cls ON COMMIT DROP AS SELECT b._source_key AS k, e._vid AS vid,"
        " e._status AS old_status, e._row_hash AS old_hash,"
        " CASE WHEN e._vid IS NULL OR e._status = 'retired' THEN 'I'"
        " WHEN e._row_hash = b._row_hash THEN NULL ELSE 'U' END AS kind"
        f" FROM _batch b LEFT JOIN {srec} e ON e._source_id = %s AND e._source_key = b._source_key",
        (source.id,),
    )
    new_keys = [r[0] for r in conn.execute("SELECT k FROM _cls WHERE vid IS NULL").fetchall()]
    if new_keys:
        vids = ctx.alloc("Record", len(new_keys))
        conn.execute("CREATE TEMP TABLE _newv (k text, vid bigint) ON COMMIT DROP")
        with conn.cursor() as cur, cur.copy("COPY _newv FROM STDIN") as cp:
            for k, vid in zip(new_keys, vids.tolist(), strict=True):
                cp.write_row((k, vid))
        conn.execute("UPDATE _cls c SET vid = n.vid FROM _newv n WHERE c.k = n.k")
        ctx.copy_vertices(
            "Record",
            vids,
            [_vertex_props(v, source.id, k) for v, k in zip(vids.tolist(), new_keys, strict=True)],
        )
    t = _timed(timings, "stage", t)

    cols = ", ".join(names)
    b_cols = ", ".join(f"b.{n}" for n in names)
    sets = ", ".join(f"{n} = b.{n}" for n in names)
    conn.execute(
        f"INSERT INTO {srec} (_vid, _source_id, _source_key, _row_hash, _revision, _status,"
        f" _first_run, _last_change_run, {cols}) SELECT c.vid, %s, b._source_key, b._row_hash,"
        f" b._revision, 'active', %s, %s, {b_cols} FROM _batch b JOIN _cls c ON c.k = b._source_key"
        " WHERE c.old_status IS NULL",
        (source.id, run_id, run_id),
    )
    conn.execute(
        f"UPDATE {srec} e SET _row_hash = b._row_hash, _revision = b._revision,"
        f" _status = 'active', _last_change_run = %s, {sets} FROM _batch b"
        " JOIN _cls c ON c.k = b._source_key WHERE e._source_id = %s AND"
        " e._source_key = b._source_key AND c.old_status IS NOT NULL AND c.kind IS NOT NULL",
        (run_id, source.id),
    )
    counts["inserted"], counts["updated"], counts["unchanged"] = conn.execute(
        "SELECT count(*) FILTER (WHERE kind = 'I'), count(*) FILTER (WHERE kind = 'U'),"
        " count(*) FILTER (WHERE kind IS NULL) FROM _cls"
    ).fetchone()
    counts["vertices_created"] = len(new_keys)
    conn.execute(
        f"INSERT INTO {ctx.t('source_delta')} SELECT %s, %s, c.k, c.kind,"
        " CASE WHEN c.kind = 'U' THEN c.old_hash END, b._row_hash FROM _cls c"
        " JOIN _batch b ON b._source_key = c.k WHERE c.kind IS NOT NULL",
        (run_id, source.id),
    )
    conn.execute(
        f"INSERT INTO {ctx.t('pending')} SELECT vid FROM _cls WHERE kind IS NOT NULL"
        " ON CONFLICT DO NOTHING"
    )
    if source.snapshot == "full":
        conn.execute(
            "CREATE TEMP TABLE _del ON COMMIT DROP AS SELECT e._vid, e._source_key, e._row_hash"
            f" FROM {srec} e WHERE e._source_id = %s AND e._status = 'active'"
            " AND NOT EXISTS (SELECT 1 FROM _present p WHERE p.k = e._source_key)",
            (source.id,),
        )
        cur = conn.execute(
            f"UPDATE {srec} e SET _status = 'retired', _last_change_run = %s FROM _del d"
            " WHERE e._vid = d._vid",
            (run_id,),
        )
        counts["deleted"] = cur.rowcount
        conn.execute(
            f"INSERT INTO {ctx.t('source_delta')} SELECT %s, %s, _source_key, 'D', _row_hash, NULL"
            " FROM _del",
            (run_id, source.id),
        )
        conn.execute(f"INSERT INTO {ctx.t('pending')} SELECT _vid FROM _del ON CONFLICT DO NOTHING")
    if batch.rejects:
        with conn.cursor() as cur, cur.copy(f"COPY {ctx.t('reject')} FROM STDIN") as cp:
            for key, attribute, rule, action in batch.rejects:
                cp.write_row((run_id, source.id, key, attribute, rule, action))
    t = _timed(timings, "merge", t)
    ctx.log_step(run_id, step, "done", started, counts)
    _timed(timings, "log", t)
    return _summary(source.id, run_id, counts, timings)


def _summary(
    source_id: str,
    run_id: str,
    counts: dict,
    timings: dict,
    *,
    replayed: bool = False,
    status: str = "ok",
) -> dict[str, Any]:
    return {
        "lane": "graph",
        "command": "ingest",
        "run_id": run_id,
        "source": source_id,
        "status": "blocked" if counts.get("blocked") else status,
        "replayed": replayed,
        "counts": counts,
        "seconds": timings,
    }
