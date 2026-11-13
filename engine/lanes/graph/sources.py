"""Read a source (postgres table or parquet glob) into an Arrow table of source columns."""

from __future__ import annotations

import glob
import os
from pathlib import Path

import psycopg
import pyarrow as pa
import pyarrow.dataset as ds
from psycopg import sql

from engine.core.config import ReferentialConfig, Source

from .db import LaneError


def needed_columns(cfg: ReferentialConfig, source: Source) -> list[str]:
    cols = [source.key]
    if source.revision:
        cols.append(source.revision)
    for column in cfg.mapping(source.id).values():
        if column not in cols:
            cols.append(column)
    return cols


def _split_location(location: str) -> tuple[str, str]:
    schema, sep, table = location.partition(".")
    if not sep or not table:
        raise LaneError(f"postgres location must be schema.table, got {location!r}")
    return schema, table


def _read_postgres(source: Source, cols: list[str], page: int) -> pa.Table:
    dsn = os.environ.get(source.dsn_env or "")
    if not dsn:
        raise LaneError(f"environment variable {source.dsn_env} is not set (source {source.id})")
    schema, table = _split_location(source.location)
    query = sql.SQL("SELECT {} FROM {}.{}").format(
        sql.SQL(", ").join(sql.Identifier(c) for c in cols),
        sql.Identifier(schema),
        sql.Identifier(table),
    )
    data: list[list] = [[] for _ in cols]
    try:
        with psycopg.connect(dsn) as conn, conn.cursor(name="mdm_graph_read") as cur:
            cur.itersize = page
            cur.execute(query)
            while True:
                rows = cur.fetchmany(page)
                if not rows:
                    break
                for col, values in zip(data, zip(*rows, strict=True), strict=True):
                    col.extend(values)
    except psycopg.Error as error:
        raise LaneError(f"cannot read source {source.id} ({source.location}): {error}") from error
    arrays = []
    for values in data:
        try:
            arrays.append(pa.array(values))
        except (pa.ArrowInvalid, pa.ArrowTypeError, OverflowError):
            # mixed/odd python values: keep them as text, the cast step re-parses them
            arrays.append(pa.array([None if v is None else str(v) for v in values]))
    return pa.table(dict(zip(cols, arrays, strict=True)))


def _read_parquet(source: Source, cols: list[str]) -> pa.Table:
    files = sorted(glob.glob(source.location, recursive=True))
    if not files:
        raise LaneError(f"no parquet file matches {source.location!r} (source {source.id})")
    dataset = ds.dataset([Path(f).as_posix() for f in files], format="parquet")
    missing = [c for c in cols if c not in dataset.schema.names]
    if missing:
        raise LaneError(f"source {source.id}: missing columns {missing}")
    return dataset.to_table(columns=cols)


def read_source(cfg: ReferentialConfig, source: Source, page: int | None = None) -> pa.Table:
    cols = needed_columns(cfg, source)
    if source.kind == "postgres":
        return _read_postgres(source, cols, page or cfg.runtime.batch_rows)
    return _read_parquet(source, cols)
