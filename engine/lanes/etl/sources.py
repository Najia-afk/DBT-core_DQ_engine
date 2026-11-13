"""Streaming extraction of a source as Arrow tables (postgres via COPY, parquet via dataset)."""

from __future__ import annotations

import glob
import os
from collections.abc import Iterator
from pathlib import Path

import psycopg
import pyarrow as pa
import pyarrow.dataset as pads
from psycopg import sql

from engine.core.config import ReferentialConfig, Source

from .arrowio import copy_out, rebatch
from .db import LaneError, env_value, open_connection

_INT = {20, 21, 23}
_FLOAT = {700, 701}
_BOOL, _DATE, _TS, _TSTZ = 16, 1082, 1114, 1184


def split_location(location: str) -> tuple[str, str]:
    schema, _, table = location.rpartition(".")
    return schema or "public", table


def _pg_batches(
    cfg: ReferentialConfig, source: Source, columns: list[str], batch_rows: int
) -> Iterator[pa.Table]:
    schema, table = split_location(source.location)
    dsn = env_value(source.dsn_env or "")
    with open_connection(cfg, dsn, read_only=True) as conn:
        target = sql.Identifier(schema, table)
        cols = sql.SQL(", ").join(sql.Identifier(c) for c in columns)
        probe = conn.execute(sql.SQL("SELECT {} FROM {} LIMIT 0").format(cols, target))
        description = probe.description or []
        select: list[str] = []
        fields: list[pa.Field] = []
        for column, desc in zip(columns, description, strict=True):
            quoted = sql.Identifier(column).as_string(conn)
            oid = desc.type_code
            if oid in _INT:
                typ, expr = pa.int64(), quoted
            elif oid in _FLOAT:
                typ, expr = pa.float64(), quoted
            elif oid == _BOOL:
                typ, expr = pa.bool_(), quoted
            elif oid == _DATE:
                typ, expr = pa.date32(), quoted
            elif oid == _TS:
                typ, expr = pa.timestamp("us"), quoted
            elif oid == _TSTZ:
                typ, expr = pa.timestamp("us"), f"({quoted} AT TIME ZONE 'UTC')"
            else:  # numeric is read as text and cast exactly (HALF_UP) by the transform
                typ, expr = pa.string(), f"{quoted}::text"
            select.append(f"{expr} AS c{len(fields)}")
            fields.append(pa.field(column, typ))
        query = f"SELECT {', '.join(select)} FROM {target.as_string(conn)}"
        wire = pa.schema([pa.field(f"c{i}", f.type) for i, f in enumerate(fields)])
        stream = copy_out(conn, query, wire)
        try:
            tables = (pa.Table.from_batches([b]) for b in stream)
            for table_ in rebatch(tables, batch_rows):
                yield table_.rename_columns(columns)
        finally:
            stream.close()


def expand_location(location: str) -> list[str]:
    path = Path(location)
    if path.is_dir():
        files = sorted(str(p) for p in path.rglob("*.parquet"))
    else:
        files = sorted(glob.glob(location, recursive=True))
    return [f for f in files if os.path.isfile(f)]


def _parquet_batches(source: Source, columns: list[str], batch_rows: int) -> Iterator[pa.Table]:
    files = expand_location(source.location)
    if not files:
        raise LaneError(f"source {source.id}: no parquet file matches {source.location}")
    dataset = pads.dataset(files, format="parquet")
    missing = [c for c in columns if c not in dataset.schema.names]
    if missing:
        raise LaneError(f"source {source.id}: column(s) missing in parquet files: {missing}")
    scanner = dataset.scanner(columns=columns, batch_size=batch_rows)
    tables = (pa.Table.from_batches([b]) for b in scanner.to_batches())
    yield from rebatch(tables, batch_rows)


def source_batches(
    cfg: ReferentialConfig, source: Source, columns: list[str]
) -> Iterator[pa.Table]:
    batch_rows = cfg.runtime.batch_rows
    try:
        if source.kind == "postgres":
            yield from _pg_batches(cfg, source, columns, batch_rows)
        else:
            yield from _parquet_batches(source, columns, batch_rows)
    except psycopg.errors.UndefinedColumn as error:
        raise LaneError(f"source {source.id}: {error}") from error
    except psycopg.errors.UndefinedTable as error:
        raise LaneError(f"source {source.id}: table {source.location} not found") from error
    except pa.ArrowInvalid as error:
        raise LaneError(f"source {source.id}: cannot read data: {error}") from error
