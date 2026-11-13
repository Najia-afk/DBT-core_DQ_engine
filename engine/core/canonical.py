"""Canonical export projections (contract section 4): the interface shared by all lanes.

Every lane exports its state with `export --out DIR` as one Parquet file per table named
`<table>.parquet`, using exactly the schemas built here (`canonical_schemas`). The reference
engine and `engine.core.compare` use the same schemas.

Conventions (normative)
-----------------------
* Node identity: `(source_id, source_key)`; `source_key` is the source key rendered as text
  (`str(value)`, no trimming). Ordering of nodes is binary/code-point order (`COLLATE "C"` in
  PostgreSQL, default in DuckDB/Python): first `source_id`, then `source_key`.
* Pair keys: `match_pair` and `review` store each pair once, ordered so that
  `(left_source_id, left_key) < (right_source_id, right_key)` (see `ordered_pair`). The columns
  are `left_source_id, left_key, right_source_id, right_key` (a single `*_key` column cannot
  identify a node, hence the explicit source columns; the contract's `left_key/right_key`
  are the `(source_id, source_key)` couples).
* `run_id`: opaque text, `[A-Za-z0-9_.:-]{1,128}` (see `canonical_run_id`); compared by
  equality only (chronology = execution order, never the value).
* Ids: `staging_id` / `golden_id` are int64 allocated from a monotonic counter; "oldest" = smallest.
  They are NOT comparable across lanes (comparison is made modulo ids).
* Attribute types: string -> utf8, int -> int64, decimal -> decimal128(18, 4), date -> date32,
  timestamp -> timestamp('us') (naive UTC), bool -> bool. `revision` is timestamp('us').
* `golden_delta.old_value/new_value` are the canonical text (`canonical_text`) of the value;
  `golden.<attr>__src` / `golden_delta.winning_source` is the `source_id` of the winning record.
* `row_hash` = sha256 hex of `row_hash_input` (values of the attributes mapped by the source).

State tables describe the current state; event tables are append-only logs.
State: source_record, match_pair, staging, staging_member, golden, review.
Events: source_delta, staging_event, golden_delta, reject, anomaly.

`review` is a state table: one row per pair currently in `review`; `run_id` is the run that
last changed the pair (informative, not compared).

Extra tables beyond contract section 4 (optional for `compare`): `staging` (staging_id, status
active/retired, created_run) and `anomaly` (run_id, kind, subject, detail) which records
`oversized_block`, `batch_blocked` and `golden_blocked`.
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Iterable, Mapping
from decimal import Decimal
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from engine.core.config import ReferentialConfig

STATE_TABLES = ("source_record", "match_pair", "staging", "staging_member", "golden", "review")
EVENT_TABLES = ("source_delta", "staging_event", "golden_delta", "reject", "anomaly")
TABLES = STATE_TABLES + EVENT_TABLES
OPTIONAL_TABLES = ("staging", "anomaly")
REQUIRED_TABLES = tuple(t for t in TABLES if t not in OPTIONAL_TABLES)

DELTA_KINDS = ("I", "U", "D")
PAIR_DECISIONS = ("match", "review", "none")
REVIEW_REASONS = ("score_band", "contradiction", "multiple_candidates")
STAGING_EVENT_KINDS = ("create", "merge", "split")
GOLDEN_STATUSES = ("published", "blocked", "retired")
REJECT_ACTIONS = ("reject_record", "null_value", "warn")
ANOMALY_KINDS = ("oversized_block", "batch_blocked", "golden_blocked")

_RUN_ID = re.compile(r"^[A-Za-z0-9_.:\-]{1,128}$")
_ARROW_TYPES: dict[str, pa.DataType] = {
    "string": pa.string(),
    "int": pa.int64(),
    "decimal": pa.decimal128(18, 4),
    "date": pa.date32(),
    "timestamp": pa.timestamp("us"),
    "bool": pa.bool_(),
}
Node = tuple[str, str]


def arrow_type(attr_type: str) -> pa.DataType:
    """Arrow type of an attribute type (config `AttrType`)."""
    return _ARROW_TYPES[attr_type]


def canonical_run_id(run_id: str) -> str:
    if not isinstance(run_id, str) or not _RUN_ID.match(run_id):
        raise ValueError(f"invalid run_id {run_id!r}: expected [A-Za-z0-9_.:-]{{1,128}}")
    return run_id


def ordered_pair(left: Node, right: Node) -> tuple[Node, Node]:
    """Canonical order of a pair of nodes (smaller `(source_id, source_key)` first)."""
    if left == right:
        raise ValueError(f"a pair needs two distinct nodes, got {left!r} twice")
    return (left, right) if left < right else (right, left)


def canonical_text(value: Any, attr_type: str) -> str | None:
    """Canonical text of a typed value (used by row_hash and golden_delta)."""
    if value is None:
        return None
    if attr_type == "string":
        return str(value)
    if attr_type == "int":
        return str(int(value))
    if attr_type == "decimal":
        return format(Decimal(value).quantize(Decimal("0.0001")), "f")
    if attr_type == "date":
        return value.isoformat()
    if attr_type == "timestamp":
        return value.strftime("%Y-%m-%dT%H:%M:%S.%f")
    if attr_type == "bool":
        return "true" if value else "false"
    raise ValueError(f"unknown attribute type {attr_type!r}")


def row_hash_input(config: ReferentialConfig, source_id: str, values: Mapping[str, Any]) -> str:
    """Normative hash input: mapped attributes of the source, in config order.

    Each attribute contributes `name US token` where token is `NUL` for null and
    `SOH + canonical_text` otherwise; contributions are joined with RS (U+001E).
    """
    mapped = config.mapping(source_id)
    parts = []
    for attr in config.attributes:
        if attr.name not in mapped:
            continue
        text = canonical_text(values.get(attr.name), attr.type)
        token = "\x00" if text is None else "\x01" + text
        parts.append(f"{attr.name}\x1f{token}")
    return "\x1e".join(parts)


def row_hash(config: ReferentialConfig, source_id: str, values: Mapping[str, Any]) -> str:
    return hashlib.sha256(row_hash_input(config, source_id, values).encode("utf-8")).hexdigest()


def canonical_schemas(config: ReferentialConfig) -> dict[str, pa.Schema]:
    """Arrow schemas of the canonical projections for one referential."""
    s, i64, ts, flag = pa.string(), pa.int64(), pa.timestamp("us"), pa.bool_()

    def f(name: str, typ: pa.DataType, nullable: bool = False) -> pa.Field:
        return pa.field(name, typ, nullable=nullable)

    pair_keys = [
        f("left_source_id", s),
        f("left_key", s),
        f("right_source_id", s),
        f("right_key", s),
    ]
    attr_fields = [f(a.name, arrow_type(a.type), True) for a in config.attributes]
    golden_attr_fields: list[pa.Field] = []
    for a in config.attributes:
        golden_attr_fields.append(f(a.name, arrow_type(a.type), True))
        if config.publication.provenance:
            golden_attr_fields.append(f(f"{a.name}__src", s, True))
    return {
        "source_record": pa.schema(
            [
                f("source_id", s),
                f("source_key", s),
                f("row_hash", s),
                f("revision", ts, True),
                f("status", s),
                f("first_run", s),
                f("last_change_run", s),
                *attr_fields,
            ]
        ),
        "source_delta": pa.schema(
            [
                f("run_id", s),
                f("source_id", s),
                f("source_key", s),
                f("kind", s),
                f("old_hash", s, True),
                f("new_hash", s, True),
            ]
        ),
        "match_pair": pa.schema(
            [
                *pair_keys,
                f("score", pa.float64(), True),
                f("decision", s),
                f("contradiction", flag),
                f("rule_version", s),
            ]
        ),
        "staging": pa.schema([f("staging_id", i64), f("status", s), f("created_run", s)]),
        "staging_member": pa.schema([f("staging_id", i64), f("source_id", s), f("source_key", s)]),
        "staging_event": pa.schema(
            [
                f("run_id", s),
                f("kind", s),
                f("staging_id", i64),
                f("related_id", i64, True),
            ]
        ),
        "golden": pa.schema(
            [f("golden_id", i64), f("status", s), f("stale", flag), *golden_attr_fields]
        ),
        "golden_delta": pa.schema(
            [
                f("run_id", s),
                f("golden_id", i64),
                f("attribute", s),
                f("old_value", s, True),
                f("new_value", s, True),
                f("winning_source", s, True),
            ]
        ),
        "review": pa.schema([f("run_id", s), *pair_keys, f("score", pa.float64()), f("reason", s)]),
        "reject": pa.schema(
            [
                f("run_id", s),
                f("source_id", s),
                f("source_key", s),
                f("attribute", s),
                f("rule", s),
                f("action", s),
            ]
        ),
        "anomaly": pa.schema([f("run_id", s), f("kind", s), f("subject", s), f("detail", s, True)]),
    }


def table_from_rows(
    config: ReferentialConfig, table: str, rows: Iterable[Mapping[str, Any]]
) -> pa.Table:
    """Build a canonical table from dict rows (missing columns become null)."""
    schema = canonical_schemas(config)[table]
    return pa.Table.from_pylist([dict(r) for r in rows], schema=schema)


def empty_tables(config: ReferentialConfig) -> dict[str, pa.Table]:
    return {name: schema.empty_table() for name, schema in canonical_schemas(config).items()}


def write_parquet(tables: Mapping[str, pa.Table], out_dir: str | Path) -> list[Path]:
    """Write each table to `<out_dir>/<table>.parquet`."""
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    written = []
    for name, table in tables.items():
        path = out / f"{name}.parquet"
        pq.write_table(table, path)
        written.append(path)
    return written


def read_parquet_dir(directory: str | Path) -> dict[str, pa.Table]:
    """Read every canonical `<table>.parquet` present in a directory."""
    base = Path(directory)
    return {
        t: pq.read_table(base / f"{t}.parquet") for t in TABLES if (base / f"{t}.parquet").exists()
    }


def check_tables(config: ReferentialConfig, tables: Mapping[str, pa.Table]) -> list[str]:
    """Conformance problems of an export versus the canonical schemas (empty = conform)."""
    problems: list[str] = []
    expected = canonical_schemas(config)
    for name in REQUIRED_TABLES:
        if name not in tables:
            problems.append(f"{name}: table missing")
    for name, table in tables.items():
        schema = expected.get(name)
        if schema is None:
            problems.append(f"{name}: unknown table")
            continue
        have = {f.name: f.type for f in table.schema}
        for fld in schema:
            if fld.name not in have:
                problems.append(f"{name}.{fld.name}: column missing")
            elif _norm_type(have[fld.name]) != _norm_type(fld.type):
                problems.append(f"{name}.{fld.name}: type {have[fld.name]} != {fld.type}")
        for col in have:
            if col not in schema.names:
                problems.append(f"{name}.{col}: unexpected column")
    return problems


def _norm_type(typ: pa.DataType) -> str:
    return str(typ).replace("large_string", "string")


__all__ = [
    "ANOMALY_KINDS",
    "EVENT_TABLES",
    "REQUIRED_TABLES",
    "STATE_TABLES",
    "TABLES",
    "arrow_type",
    "canonical_run_id",
    "canonical_schemas",
    "canonical_text",
    "check_tables",
    "empty_tables",
    "ordered_pair",
    "read_parquet_dir",
    "row_hash",
    "row_hash_input",
    "table_from_rows",
    "write_parquet",
]
