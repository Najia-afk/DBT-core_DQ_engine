"""Dialectes SQL (DuckDB pour Parquet, PostgreSQL) et connexions de `check-source`."""

from __future__ import annotations

import os
import re
import shutil
import tempfile
from dataclasses import dataclass, field

from engine.cli import normalizers

TEXT, INT, NUM, BOOL, DATE, TIMESTAMP, OTHER = (
    "text", "int", "num", "bool", "date", "timestamp", "other"
)  # fmt: skip
INT_TYPES = {
    "tinyint", "smallint", "integer", "int", "bigint", "hugeint", "utinyint", "usmallint",
    "uinteger", "ubigint", "uhugeint", "int2", "int4", "int8", "serial", "bigserial",
}  # fmt: skip
NUM_TYPES = {"numeric", "decimal", "real", "double", "float", "float4", "float8", "money"}
TEXT_TYPES = {"varchar", "character", "char", "text", "string", "bpchar", "name", "citext"}

COMPATIBLE = {
    "string": {TEXT, INT, NUM, BOOL, DATE, TIMESTAMP, OTHER},
    "int": {TEXT, INT, NUM},
    "decimal": {TEXT, INT, NUM},
    "date": {TEXT, DATE, TIMESTAMP},
    "timestamp": {TEXT, DATE, TIMESTAMP},
    "bool": {TEXT, BOOL, INT},
}
ORDERABLE = {INT, NUM, DATE, TIMESTAMP}
MEMORY_LIMIT = os.environ.get("MDM_CHECK_SOURCE_MEMORY", "3GB")

# Repli PostgreSQL < 16 (pas de pg_input_is_valid) : approximation par expression régulière.
PG_LEGACY_REGEX = {
    "int": r"^\s*[+-]?[0-9]+\s*$",
    "decimal": r"^\s*[+-]?([0-9]+\.?[0-9]*|\.[0-9]+)([eE][+-]?[0-9]+)?\s*$",
    "date": r"^\s*[0-9]{4}-[0-9]{2}-[0-9]{2}\s*$",
    "timestamp": r"^\s*[0-9]{4}-[0-9]{2}-[0-9]{2}([ T][0-9]{2}:[0-9]{2}(:[0-9]{2}(\.[0-9]+)?)?)?"
    r"\s*(Z|[+-][0-9]{2}(:?[0-9]{2})?)?\s*$",
    "bool": r"^\s*(?i:t|f|true|false|y|n|yes|no|on|off|0|1)\s*$",
}


def family(native: str) -> str:
    base = re.split(r"[ (\[]", native.strip().lower(), maxsplit=1)[0]
    if base in TEXT_TYPES:
        return TEXT
    if base in INT_TYPES:
        return INT
    if base in NUM_TYPES:
        return NUM
    if base in {"boolean", "bool"}:
        return BOOL
    if base == "date":
        return DATE
    if base.startswith("timestamp"):
        return TIMESTAMP
    return OTHER


def quote(ident: str) -> str:
    return '"' + ident.replace('"', '""') + '"'


def literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


class Dialect:
    name = ""
    types = {"string": "", "int": "", "decimal": "", "date": "", "timestamp": "", "bool": ""}
    legacy_cast = False
    approx_distinct = False
    inline_exact = True  # agrégats DISTINCT exacts dans l'agrégation principale
    max_aggs = 1500  # agrégats par requête (au-delà : plusieurs requêtes)

    def source_sql(self, location: str) -> str:
        raise NotImplementedError

    def text(self, expr: str) -> str:
        return f"CAST({expr} AS {self.types['string']})"

    def distinct_count(self, expr: str) -> str:
        return f"count(DISTINCT {expr})"

    def trim(self, expr: str) -> str:
        raise NotImplementedError

    def accents(self, expr: str) -> str:
        raise NotImplementedError

    def regex_replace(self, expr: str, pattern: str, repl: str) -> str:
        return f"regexp_replace({expr}, {literal(pattern)}, {literal(repl)}, 'g')"

    def regex_match(self, expr: str, pattern: str) -> str:
        raise NotImplementedError

    def cast(self, col: str, fam: str, target: str) -> str:
        raise NotImplementedError

    def normalize(self, name: str, expr: str) -> str:
        if name == "trim":
            return self.trim(expr)
        if name in {"lower", "upper"}:
            return f"{name}({expr})"
        if name == "strip_accents":
            return self.accents(expr)
        if name == "collapse_spaces":
            return self.regex_replace(expr, r"\s+", " ")
        if name == "digits_only":
            return self.regex_replace(expr, "[^0-9]", "")
        if name == "alnum_only":
            return self.regex_replace(expr, self.non_alnum, "")
        raise ValueError(name)

    non_alnum = ""


class DuckDialect(Dialect):
    name = "duckdb"
    types = {"string": "VARCHAR", "int": "BIGINT", "decimal": "DOUBLE", "date": "DATE",
             "timestamp": "TIMESTAMP", "bool": "BOOLEAN"}  # fmt: skip
    non_alnum = r"[^\p{L}\p{N}]"
    # count(DISTINCT) exact : un hash par agrégat et par thread, > 2,7 Go dès 400 agrégats
    # sur 500 lignes. HLL : mémoire constante, erreur de l'ordre de 2 %.
    approx_distinct = True
    inline_exact = False

    def distinct_count(self, expr: str) -> str:
        return f"approx_count_distinct({expr})"

    def source_sql(self, location: str) -> str:
        return f"read_parquet({literal(location)}, union_by_name=true)"

    def trim(self, expr: str) -> str:
        return f"trim({expr}, E' \\t\\n\\r')"

    def accents(self, expr: str) -> str:
        return f"strip_accents({expr})"

    def regex_match(self, expr: str, pattern: str) -> str:
        return f"regexp_matches({expr}, {literal(pattern)})"

    def cast(self, col: str, fam: str, target: str) -> str:
        return f"TRY_CAST({col} AS {self.types[target]})"


class PgDialect(Dialect):
    name = "postgres"
    types = {"string": "text", "int": "bigint", "decimal": "numeric", "date": "date",
             "timestamp": "timestamp", "bool": "boolean"}  # fmt: skip
    non_alnum = "[^[:alnum:]]"
    max_aggs = 1600  # PostgreSQL plafonne à 1664 colonnes de sortie

    def __init__(self, server_version: int = 160000) -> None:
        self.legacy_cast = server_version < 160000

    def source_sql(self, location: str) -> str:
        parts = location.split(".")
        if not 1 <= len(parts) <= 2 or not all(parts):
            raise ValueError(f"invalid location {location!r}: expected schema.table")
        return ".".join(quote(p) for p in parts)

    def trim(self, expr: str) -> str:
        return f"btrim({expr}, E' \\t\\n\\r')"

    def accents(self, expr: str) -> str:
        src, dst = literal(normalizers.ACCENTS_FROM), literal(normalizers.ACCENTS_TO)
        return f"translate({expr}, {src}, {dst})"

    def regex_match(self, expr: str, pattern: str) -> str:
        return f"({expr} ~ {literal(pattern)})"

    def valid(self, expr: str, target: str) -> str:
        if self.legacy_cast:
            return f"({expr} ~ {literal(PG_LEGACY_REGEX[target])})"
        return f"pg_input_is_valid({expr}, {literal(self.types[target])})"

    def cast(self, col: str, fam: str, target: str) -> str:
        t = self.types[target]
        if fam == TEXT:
            return f"CASE WHEN {self.valid(f'{col}::text', target)} THEN {col}::{t} END"
        return f"{col}::{t}"


class Backend:
    dialect: Dialect

    def describe(self) -> dict[str, str]:
        raise NotImplementedError

    def row(self, sql: str) -> tuple:
        raise NotImplementedError

    def probe(self, sql: str) -> None:
        self.row(sql)

    def close(self) -> None:
        pass


class DuckBackend(Backend):
    def __init__(self, location: str) -> None:
        import duckdb

        self.dialect = DuckDialect()
        self.src = self.dialect.source_sql(location)
        self.con = duckdb.connect(":memory:")
        # Plafond mémoire explicite ; au-delà, les agrégats DISTINCT débordent sur disque.
        self.spill = tempfile.mkdtemp(prefix="mdm-check-source-")
        self.con.execute(f"SET memory_limit = {literal(MEMORY_LIMIT)}")
        self.con.execute(f"SET temp_directory = {literal(self.spill)}")

    def describe(self) -> dict[str, str]:
        rows = self.con.execute(f"DESCRIBE SELECT * FROM {self.src}").fetchall()
        return {r[0]: r[1] for r in rows}

    def row(self, sql: str) -> tuple:
        return self.con.execute(sql).fetchone()

    def close(self) -> None:
        self.con.close()
        shutil.rmtree(self.spill, ignore_errors=True)


class PgBackend(Backend):
    def __init__(self, location: str, dsn_env: str) -> None:
        import psycopg

        dsn = os.environ.get(dsn_env)
        if not dsn:
            raise ConnectionError(f"environment variable {dsn_env} is not set")
        self.location = location
        self.con = psycopg.connect(dsn, autocommit=True, connect_timeout=10)
        self.con.execute("SET default_transaction_read_only = on")
        version = int(self.con.execute("SHOW server_version_num").fetchone()[0])
        self.dialect = PgDialect(version)
        self.src = self.dialect.source_sql(location)

    def describe(self) -> dict[str, str]:
        rows = self.con.execute(
            "SELECT a.attname, format_type(a.atttypid, a.atttypmod) FROM pg_attribute a "
            "WHERE a.attrelid = to_regclass(%s) AND a.attnum > 0 AND NOT a.attisdropped "
            "ORDER BY a.attnum",
            (self.src,),
        ).fetchall()
        if not rows:
            raise LookupError(f"table or view {self.location!r} not found (or not readable)")
        return dict(rows)

    def row(self, sql: str) -> tuple:
        return self.con.execute(sql).fetchone()

    def close(self) -> None:
        self.con.close()


@dataclass
class Unit:
    inner: dict[str, str]
    aggs: dict[str, str]
    # Agrégats DISTINCT exacts (clé). Sur DuckDB, les mélanger à d'autres agrégats dans une
    # même agrégation fait passer tout le plan par le chemin « distinct » : 12 s → 90 s et
    # 0,7 → 8 Go mesurés sur 190 k lignes × 172 attributs. Ils vont donc dans une sous-requête
    # jointe (même instruction SQL, une seule ligne).
    exact: dict[str, str] = field(default_factory=dict)
