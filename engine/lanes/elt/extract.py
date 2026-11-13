"""Chargement du brut (« EL ») : les lots source sont copiés tels quels dans des fichiers Parquet du
répertoire de travail (seules les colonnes utiles sont lues). Aucune transformation ici.

Pourquoi du Parquet plutôt qu'un schéma `raw` PostgreSQL : écriture colonnaire séquentielle sans
amplification de WAL pour des lignes de 200 colonnes, relecture parallèle native par DuckDB,
lot immuable et rejouable par run-id, et aucune charge d'écriture sur l'état PostgreSQL.
"""

from __future__ import annotations

import glob
import shutil
from pathlib import Path
from typing import Any

import duckdb

from engine.core.config import ReferentialConfig, Source

from .settings import ConfigurationError, Settings, env_secret, format_memory
from .sqlgen import lit, q


def connect_duckdb(settings: Settings, spill: Path) -> duckdb.DuckDBPyConnection:
    spill.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(
        config={
            "memory_limit": format_memory(settings.memory_bytes),
            "threads": settings.threads,
            "temp_directory": str(spill),
        }
    )
    con.execute("SET TimeZone = 'UTC'")
    return con


def needed_columns(config: ReferentialConfig, source: Source) -> list[str]:
    cols = [source.key]
    if source.revision:
        cols.append(source.revision)
    cols += list(config.mapping(source.id).values())
    return list(dict.fromkeys(cols))


def extract_raw(
    config: ReferentialConfig, source: Source, settings: Settings, raw_dir: Path
) -> dict[str, Any]:
    """Copie le lot de la source vers `raw_dir/part-00000.parquet` avec un ordinal `_ord`."""
    if raw_dir.exists():
        shutil.rmtree(raw_dir)
    raw_dir.mkdir(parents=True)
    con = connect_duckdb(settings, raw_dir.parent / "spill-extract")
    try:
        if source.kind == "postgres":
            con.execute("INSTALL postgres")
            con.execute("LOAD postgres")
            dsn = env_secret(source.dsn_env or "")
            con.execute("ATTACH ? AS src (TYPE postgres, READ_ONLY)", [dsn])
            schema, _, table = source.location.rpartition(".")
            relation = f"src.{q(schema or 'public')}.{q(table)}"
        else:
            files = sorted(glob.glob(source.location, recursive=True))
            if not files:
                raise ConfigurationError(f"source {source.id}: no file matches {source.location}")
            listing = ", ".join(lit(Path(f).as_posix()) for f in files)
            relation = f"read_parquet([{listing}], union_by_name = true)"
        have = [r[0] for r in con.execute(f"DESCRIBE SELECT * FROM {relation}").fetchall()]
        wanted = needed_columns(config, source)
        for col in (source.key, source.revision):
            if col and col not in have:
                raise ConfigurationError(f"source {source.id}: column {col!r} not found")
        present = [c for c in wanted if c in have]
        missing = [c for c in wanted if c not in have]
        target = (raw_dir / "part-00000.parquet").as_posix()
        select = ", ".join(q(c) for c in present)
        con.execute(
            f"COPY (SELECT row_number() OVER () - 1 AS _ord, {select} FROM {relation})"
            f" TO {lit(target)} (FORMAT parquet, COMPRESSION zstd)"
        )
        described = con.execute(f"DESCRIBE SELECT * FROM read_parquet({lit(target)})").fetchall()
        rows = con.execute(f"SELECT count(*) FROM read_parquet({lit(target)})").fetchone()
        return {
            "rows": int(rows[0]) if rows else 0,
            "raw_types": {name: dtype for name, dtype, *_ in described},
            "missing_columns": missing,
            "bytes": (raw_dir / "part-00000.parquet").stat().st_size,
        }
    finally:
        con.close()
