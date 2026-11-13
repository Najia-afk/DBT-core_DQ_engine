"""Chargement des Parquet synthétiques dans PostgreSQL (tables larges nommées par `location`).

Deux méthodes : `copy` (psycopg COPY CSV, une connexion par fichier, en parallèle) et `duckdb`
(extension postgres de DuckDB). Remplacement intégral : table temporaire chargée puis échange
atomique (DROP + RENAME dans une transaction) ; `truncate` : TRUNCATE + COPY dans une transaction.
"""

from __future__ import annotations

import io
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import psycopg
import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq
from psycopg import sql

from engine.core.config import ReferentialConfig, Source, load_config

from .generator import load_manifest
from .params import SynthParams
from .plan import build_config

DELTA_MODES = ("upsert", "append", "replace")
BATCH_ROWS = 25_000


def pg_type(t: pa.DataType) -> str:
    if pa.types.is_int64(t):
        return "bigint"
    if pa.types.is_string(t):
        return "text"
    if pa.types.is_decimal(t):
        return f"numeric({t.precision},{t.scale})"
    if pa.types.is_date32(t):
        return "date"
    if pa.types.is_timestamp(t):
        return "timestamp"
    if pa.types.is_boolean(t):
        return "boolean"
    raise ValueError(f"unsupported arrow type {t}")


def _resolve_config(
    config_or_params: ReferentialConfig | SynthParams | str | Path | None, dataset: Path
) -> ReferentialConfig:
    if config_or_params is None:
        return load_config(dataset / "config.yml")
    if isinstance(config_or_params, ReferentialConfig):
        return config_or_params
    if isinstance(config_or_params, SynthParams):
        return build_config(config_or_params, dataset)
    return load_config(config_or_params)


def _split_location(location: str) -> tuple[str, str]:
    schema, _, table = location.rpartition(".")
    return schema or "public", table


def _ident(schema: str, table: str) -> sql.Composable:
    return sql.Identifier(schema, table)


def _ddl(
    schema: str, table: str, arrow_schema: pa.Schema, key: str, unlogged: bool
) -> sql.Composable:
    cols = [
        sql.SQL("{} {}{}").format(
            sql.Identifier(f.name),
            sql.SQL(pg_type(f.type)),
            sql.SQL(" NOT NULL") if f.name == key else sql.SQL(""),
        )
        for f in arrow_schema
    ]
    return sql.SQL("CREATE {}TABLE {} ({})").format(
        sql.SQL("UNLOGGED ") if unlogged else sql.SQL(""),
        _ident(schema, table),
        sql.SQL(", ").join(cols),
    )


def _copy_file(conn: psycopg.Connection, target: sql.Composable, path: Path) -> int:
    rows = 0
    options = pacsv.WriteOptions(include_header=False)
    with conn.cursor() as cur:
        with cur.copy(sql.SQL("COPY {} FROM STDIN (FORMAT csv)").format(target)) as copy:
            for batch in pq.ParquetFile(path).iter_batches(batch_size=BATCH_ROWS):
                buf = io.BytesIO()
                pacsv.write_csv(pa.Table.from_batches([batch]), buf, write_options=options)
                copy.write(buf.getvalue())
                rows += batch.num_rows
    return rows


def _copy_parallel(dsn: str, target: sql.Composable, files: list[Path], workers: int) -> int:
    def one(path: Path) -> int:
        with psycopg.connect(dsn, autocommit=True) as conn:
            conn.execute("SET synchronous_commit = off")
            return _copy_file(conn, target, path)

    if workers <= 1 or len(files) <= 1:
        return sum(one(f) for f in files)
    with ThreadPoolExecutor(max_workers=workers) as pool:
        return sum(pool.map(one, files))


def _duckdb_fill(dsn: str, schema: str, table: str, files: list[Path], workers: int) -> int:
    import duckdb

    con = duckdb.connect()
    con.execute(f"SET threads = {max(workers, 1)}")
    con.execute("INSTALL postgres")
    con.execute("LOAD postgres")
    con.execute("ATTACH ? AS pg (TYPE postgres)", [dsn])
    listing = ", ".join("'" + f.as_posix().replace("'", "''") + "'" for f in files)
    con.execute(f'INSERT INTO pg."{schema}"."{table}" SELECT * FROM read_parquet([{listing}])')
    count = con.execute(f'SELECT count(*) FROM pg."{schema}"."{table}"').fetchone()
    con.close()
    return int(count[0]) if count else 0


def _fill(method: str, dsn: str, schema: str, table: str, files: list[Path], workers: int) -> int:
    if method == "copy":
        return _copy_parallel(dsn, _ident(schema, table), files, workers)
    if method == "duckdb":
        return _duckdb_fill(dsn, schema, table, files, workers)
    raise ValueError(f"unknown method {method!r}")


def _source_files(dataset: Path, day: int, source_id: str) -> list[Path]:
    files = sorted((dataset / f"day{day}" / source_id).glob("part-*.parquet"))
    if not files:
        raise FileNotFoundError(f"no parquet for source {source_id} on day {day} in {dataset}")
    return files


def _load_source(
    dsn: str,
    src: Source,
    files: list[Path],
    *,
    mode: str,
    method: str,
    workers: int,
    primary_key: bool,
    unlogged: bool,
    analyze: bool,
) -> dict[str, Any]:
    schema, table = _split_location(src.location)
    stage = f"{table}__load"
    t0 = time.perf_counter()
    arrow_schema = pq.read_schema(files[0])
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema)))
        conn.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(_ident(schema, stage)))
        row = conn.execute(
            "SELECT to_regclass(%s) IS NOT NULL", [f'"{schema}"."{table}"']
        ).fetchone()
        exists = bool(row and row[0])

    if mode == "truncate" and exists:
        with psycopg.connect(dsn) as conn:
            conn.execute("SET LOCAL synchronous_commit = off")
            conn.execute(sql.SQL("TRUNCATE {}").format(_ident(schema, table)))
            rows = sum(_copy_file(conn, _ident(schema, table), f) for f in files)
        t_load = time.perf_counter()
    else:
        with psycopg.connect(dsn, autocommit=True) as conn:
            conn.execute(_ddl(schema, stage, arrow_schema, src.key, unlogged))
        rows = _fill(method, dsn, schema, stage, files, workers)
        t_load = time.perf_counter()
        if primary_key:
            token = int(time.time() * 1000)
            with psycopg.connect(dsn, autocommit=True) as conn:
                conn.execute(
                    sql.SQL("ALTER TABLE {} ADD CONSTRAINT {} PRIMARY KEY ({})").format(
                        _ident(schema, stage),
                        sql.Identifier(f"{table}_pk_{token}"),
                        sql.Identifier(src.key),
                    )
                )
        with psycopg.connect(dsn) as conn:  # transaction : un lecteur voit l'ancien ou le nouveau
            real, tmp = _ident(schema, table), _ident(schema, stage)
            if mode in ("upsert", "append") and exists:
                if mode == "upsert":
                    conn.execute(
                        sql.SQL("DELETE FROM {} t USING {} s WHERE t.{k} = s.{k}").format(
                            real, tmp, k=sql.Identifier(src.key)
                        )
                    )
                conn.execute(sql.SQL("INSERT INTO {} SELECT * FROM {}").format(real, tmp))
                conn.execute(sql.SQL("DROP TABLE {}").format(tmp))
            else:
                conn.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(real))
                conn.execute(
                    sql.SQL("ALTER TABLE {} RENAME TO {}").format(tmp, sql.Identifier(table))
                )
    t_swap = time.perf_counter()
    if analyze:
        with psycopg.connect(dsn, autocommit=True) as conn:
            conn.execute(sql.SQL("ANALYZE {}").format(_ident(schema, table)))
    t_end = time.perf_counter()
    return {
        "table": src.location,
        "mode": mode,
        "rows_loaded": rows,
        "files": len(files),
        "seconds_load": round(t_load - t0, 3),
        "seconds_swap": round(t_swap - t_load, 3),
        "seconds_analyze": round(t_end - t_swap, 3),
        "seconds": round(t_end - t0, 3),
    }


def load_postgres(
    config_or_params: ReferentialConfig | SynthParams | str | Path | None,
    dsn: str,
    dataset_dir: str | Path,
    *,
    day: int = 0,
    sources: list[str] | None = None,
    method: str = "copy",
    workers: int = 8,
    delta_mode: str = "upsert",
    replace_mode: str = "swap",
    primary_key: bool = False,
    unlogged: bool = False,
    analyze: bool = False,
    include_parquet: bool = False,
    parallel_sources: bool = True,
) -> dict[str, Any]:
    """Charge le jour `day` du jeu `dataset_dir` dans les tables PostgreSQL des sources.

    Les sources sont chargées en parallèle (`parallel_sources`), chacune avec `workers`
    connexions COPY (un fichier Parquet par connexion).
    - source snapshot=full : le contenu de la table est remplacé (`replace_mode` : `swap` =
      table chargée en parallèle puis échange atomique ; `truncate` = TRUNCATE + COPY dans une
      seule transaction) ;
    - source snapshot=delta : `delta_mode` = `upsert` (suppression des clés présentes puis
      insertion, une transaction), `append` (insertion) ou `replace` (comme full). Le jour 0
      d'une table inexistante la crée.
    Seules les sources kind=postgres sont chargées (`include_parquet=True` pour toutes).
    """
    if delta_mode not in DELTA_MODES:
        raise ValueError(f"delta_mode must be in {DELTA_MODES}")
    if replace_mode not in ("swap", "truncate"):
        raise ValueError("replace_mode must be 'swap' or 'truncate'")
    dataset = Path(dataset_dir)
    config = _resolve_config(config_or_params, dataset)
    load_manifest(dataset)
    t0 = time.perf_counter()
    todo = []
    for src in config.sources:
        if sources is not None and src.id not in sources:
            continue
        if src.kind != "postgres" and not include_parquet and sources is None:
            continue
        if src.snapshot == "full" or delta_mode == "replace":
            mode = "truncate" if replace_mode == "truncate" else "replace"
        else:
            mode = delta_mode
        todo.append((src, _source_files(dataset, day, src.id), mode))

    def run(item: tuple[Source, list[Path], str]) -> dict[str, Any]:
        src, files, mode = item
        return _load_source(
            dsn,
            src,
            files,
            mode=mode,
            method=method,
            workers=workers,
            primary_key=primary_key,
            unlogged=unlogged,
            analyze=analyze,
        )

    if parallel_sources and len(todo) > 1:
        with ThreadPoolExecutor(max_workers=len(todo)) as pool:
            results = list(pool.map(run, todo))
    else:
        results = [run(item) for item in todo]
    summary = {item[0].id: res for item, res in zip(todo, results, strict=True)}
    return {
        "day": day,
        "method": method,
        "sources": summary,
        "rows": sum(v["rows_loaded"] for v in summary.values()),
        "seconds": round(time.perf_counter() - t0, 3),
    }
