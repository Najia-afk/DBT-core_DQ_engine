"""Arrow <-> PostgreSQL bulk transfer through COPY CSV (vectorised on both sides)."""

from __future__ import annotations

import io
from collections.abc import Iterator
from typing import Any

import psycopg
import pyarrow as pa
import pyarrow.csv as pacsv
from psycopg import sql

_WRITE = pacsv.WriteOptions(include_header=False)
_BLOCK = 8 << 20


def ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def copy_in(conn: psycopg.Connection[Any], table: str, data: pa.Table) -> int:
    """COPY an Arrow table into `table` (columns by name). Nulls are unquoted empty fields."""
    if data.num_rows == 0:
        return 0
    cols = sql.SQL(", ").join(sql.Identifier(c) for c in data.column_names)
    stmt = sql.SQL("COPY {} ({}) FROM STDIN (FORMAT csv)").format(sql.SQL(table), cols)
    step = max(1, min(data.num_rows, 20_000))
    with conn.cursor() as cur, cur.copy(stmt) as copy:
        for start in range(0, data.num_rows, step):
            buf = io.BytesIO()
            pacsv.write_csv(data.slice(start, step), buf, write_options=_WRITE)
            copy.write(buf.getvalue())
    return data.num_rows


class _CopyStream(io.RawIOBase):
    """File-like view of a COPY ... TO STDOUT stream, consumed by the Arrow CSV reader."""

    def __init__(self, copy: Any) -> None:
        self._chunks = iter(copy)
        self._view = memoryview(b"")

    def readable(self) -> bool:
        return True

    def readinto(self, buffer: Any) -> int:
        filled, size = 0, len(buffer)
        while filled < size:  # fill the whole request: Arrow treats each read as a block
            if len(self._view) == 0:
                try:
                    self._view = memoryview(next(self._chunks))
                except StopIteration:
                    break
            n = min(size - filled, len(self._view))
            buffer[filled : filled + n] = self._view[:n]
            self._view = self._view[n:]
            filled += n
        return filled


def _convert_options(schema: pa.Schema) -> pacsv.ConvertOptions:
    return pacsv.ConvertOptions(
        column_types={f.name: f.type for f in schema},
        null_values=[""],
        strings_can_be_null=True,
        quoted_strings_can_be_null=False,
        true_values=["t"],
        false_values=["f"],
    )


def copy_out(
    conn: psycopg.Connection[Any], query: str, schema: pa.Schema, params: Any = None
) -> Iterator[pa.RecordBatch]:
    """Stream the rows of `query` as Arrow batches typed by `schema` (positional columns)."""
    stmt = sql.SQL("COPY ({}) TO STDOUT (FORMAT csv)").format(sql.SQL(query))
    with conn.cursor() as cur, cur.copy(stmt, params) as copy:
        try:
            reader = pacsv.open_csv(
                _CopyStream(copy),
                read_options=pacsv.ReadOptions(column_names=schema.names, block_size=_BLOCK),
                convert_options=_convert_options(schema),
            )
        except pa.ArrowInvalid as error:
            if "Empty CSV" not in str(error):
                raise
            return
        yield from reader


def fetch_table(
    conn: psycopg.Connection[Any], query: str, schema: pa.Schema, params: Any = None
) -> pa.Table:
    batches = list(copy_out(conn, query, schema, params))
    if not batches:
        return schema.empty_table()
    return pa.Table.from_batches(batches).cast(schema)


def rebatch(batches: Iterator[pa.Table], rows: int) -> Iterator[pa.Table]:
    """Regroup tables into tables of exactly `rows` rows (the last one may be shorter)."""
    pending: list[pa.Table] = []
    held = 0
    for table in batches:
        pending.append(table)
        held += table.num_rows
        while held >= rows:
            whole = pa.concat_tables(pending)
            yield whole.slice(0, rows)
            rest = whole.slice(rows)
            pending, held = ([rest], rest.num_rows) if rest.num_rows else ([], 0)
    if held:
        yield pa.concat_tables(pending)
