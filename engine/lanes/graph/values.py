"""Vectorised (pyarrow) ingest pipeline: cast, normalise, DQ checks, canonical text, row_hash.

Same semantics as `engine.core.reference.values` / `engine.core.canonical` (the oracle). Each
vectorised step is exact by construction; anything that is not provably identical falls back to
the reference per-value functions on the DISTINCT values of the column (dictionary encoding):

* normalisers: ASCII-only values go through Arrow kernels with explicit ASCII whitespace sets
  (identical to Python for ASCII), the other rows run the reference `normalize_string`;
* regex checks always run Python `re` on the distinct values (RE2 is not Python `re`);
* casts: natural Arrow types are cast in Arrow, everything else uses `cast_value` per distinct
  value.

Known deviation: a decimal `-0` is hashed as `0.0000` (Arrow decimals have no negative zero).
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal
from typing import Any

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc

from engine.core.canonical import arrow_type, canonical_text
from engine.core.config import Attribute, ReferentialConfig
from engine.core.reference.values import (
    CastError,
    cast_value,
    normalize_string,
    to_timestamp,
)

PG_TYPES = {
    "string": "text",
    "int": "bigint",
    "decimal": "numeric(18,4)",
    "date": "date",
    "timestamp": "timestamp(6)",
    "bool": "boolean",
}
_ASCII_WS = " \t\n\r\x0b\x0c\x1c\x1d\x1e\x1f"
_WS_RUN = r"[ \t\n\r\x0b\x0c\x1c-\x1f]+"
_INT64_MIN, _INT64_MAX = -(2**63), 2**63 - 1
_VEC_NORMALIZERS = {
    "trim": lambda a: pc.utf8_trim(a, _ASCII_WS),
    "lower": pc.ascii_lower,
    "upper": pc.ascii_upper,
    "strip_accents": lambda a: a,
    "collapse_spaces": lambda a: pc.replace_substring_regex(a, _WS_RUN, " "),
    "digits_only": lambda a: pc.replace_substring_regex(a, "[^0-9]", ""),
    "alnum_only": lambda a: pc.replace_substring_regex(a, "[^0-9A-Za-z]", ""),
}


@dataclass
class ColumnResult:
    values: pa.Array
    # (row indexes, rule, action) for every failing check / cast
    failures: list[tuple[np.ndarray, str, str]] = field(default_factory=list)


def _chunk(arr: pa.Array | pa.ChunkedArray) -> pa.Array:
    return arr.combine_chunks() if isinstance(arr, pa.ChunkedArray) else arr


def _numpy(arr: pa.Array, fill: Any = 0) -> np.ndarray:
    return pc.fill_null(arr, fill).to_numpy(zero_copy_only=False)


def _python_cast(arr: pa.Array, convert: Any, out_type: pa.DataType) -> tuple[pa.Array, np.ndarray]:
    """Apply `convert(raw)` on distinct values; CastError => null + failure flag."""
    try:
        enc = arr.dictionary_encode()
        uniques = enc.dictionary.to_pylist()
        indices = enc.indices
    except (pa.ArrowNotImplementedError, pa.ArrowInvalid, pa.ArrowTypeError):
        uniques, indices = None, None
    if uniques is None:
        raws = arr.to_pylist()
        out: list[Any] = []
        bad = np.zeros(len(raws), dtype=bool)
        for i, raw in enumerate(raws):
            if raw is None:
                out.append(None)
                continue
            try:
                out.append(convert(raw))
            except CastError:
                out.append(None)
                bad[i] = True
        return pa.array(out, type=out_type), bad
    converted: list[Any] = []
    failed = np.zeros(len(uniques), dtype=bool)
    for i, raw in enumerate(uniques):
        try:
            converted.append(convert(raw))
        except CastError:
            converted.append(None)
            failed[i] = True
    values = pa.array(converted, type=out_type).take(indices)
    if not failed.any():
        return values, np.zeros(len(arr), dtype=bool)
    positions = _numpy(indices, -1)
    bad = np.where(positions >= 0, failed[np.maximum(positions, 0)], False)
    return values, bad


def cast_column(attr: Attribute, arr: pa.Array) -> tuple[pa.Array, np.ndarray | None]:
    """Cast a raw column to the canonical Arrow type; returns (values, cast-failure mask)."""
    typ, target, t = attr.type, arrow_type(attr.type), arr.type
    if pa.types.is_null(t):
        return pa.nulls(len(arr), target), None
    try:
        if typ == "string" and (pa.types.is_string(t) or pa.types.is_large_string(t)):
            return arr.cast(pa.string()), None
        if typ == "int" and pa.types.is_integer(t):
            return arr.cast(pa.int64()), None
        if (
            typ == "decimal"
            and pa.types.is_decimal(t)
            and t.scale <= 4
            and t.precision - t.scale <= 14
        ):
            return arr.cast(target), None
        if typ == "date" and pa.types.is_date32(t):
            return arr, None
        if typ == "timestamp":
            if pa.types.is_timestamp(t) and t.tz is None and t.unit in ("s", "ms", "us"):
                return arr.cast(target), None
            if pa.types.is_date32(t):
                return arr.cast(target), None
        if typ == "bool" and pa.types.is_boolean(t):
            return arr, None
    except (pa.ArrowInvalid, pa.ArrowNotImplementedError):
        pass
    values, bad = _python_cast(arr, lambda raw: cast_value(raw, typ), target)
    return values, bad


def normalize_strings(arr: pa.Array, normalizers: list[str]) -> pa.Array:
    if not normalizers or len(arr) == 0:
        return arr
    ascii_ok = pc.fill_null(pc.string_is_ascii(arr), True)
    out = arr
    for name in normalizers:
        out = _VEC_NORMALIZERS[name](out)
    if not pc.all(ascii_ok).as_py():
        other = pc.invert(ascii_ok)
        raws = pc.filter(arr, other).to_pylist()
        fixed = pa.array([normalize_string(v, normalizers) for v in raws], type=pa.string())
        out = pc.replace_with_mask(out, other, fixed)
    return out


def canonical_text_array(attr_type: str, arr: pa.Array) -> pa.Array:
    """Arrow equivalent of `canonical_text` (null stays null)."""
    if attr_type == "string":
        return arr
    if attr_type in ("int", "decimal", "date"):
        return arr.cast(pa.string())
    if attr_type == "bool":
        return pc.if_else(arr, "true", "false")
    if attr_type == "timestamp":
        # on a timestamp('us') %S already renders "SS.ffffff"
        return pc.strftime(arr, format="%Y-%m-%dT%H:%M:%S")
    raise ValueError(f"unknown attribute type {attr_type!r}")


def _bad_regex(values: pa.Array, pattern: str) -> np.ndarray:
    compiled = re.compile(pattern)
    enc = values.dictionary_encode()
    ok = np.array([compiled.search(u) is not None for u in enc.dictionary.to_pylist()], dtype=bool)
    positions = _numpy(enc.indices, -1)
    if len(ok) == 0:
        return np.zeros(len(values), dtype=bool)
    return np.where(positions >= 0, ~ok[np.maximum(positions, 0)], False)


def _bad_bound(attr: Attribute, values: pa.Array, rule: str, limit: Any) -> np.ndarray:
    valid = pc.is_valid(values).to_numpy(zero_copy_only=False)
    bound = Decimal(str(limit))
    if attr.type == "int" and bound.is_finite():
        if rule == "min":
            edge = int(bound.to_integral_value(rounding=ROUND_CEILING))
            if edge > _INT64_MAX:
                return valid
            if edge < _INT64_MIN:
                return np.zeros(len(values), dtype=bool)
            return valid & _numpy(pc.less(values, edge), False).astype(bool)
        edge = int(bound.to_integral_value(rounding=ROUND_FLOOR))
        if edge < _INT64_MIN:
            return valid
        if edge > _INT64_MAX:
            return np.zeros(len(values), dtype=bool)
        return valid & _numpy(pc.greater(values, edge), False).astype(bool)
    raws = values.to_pylist()
    if rule == "min":
        return np.array([v is not None and not Decimal(v) >= bound for v in raws], dtype=bool)
    return np.array([v is not None and not Decimal(v) <= bound for v in raws], dtype=bool)


def _bad_check(attr: Attribute, values: pa.Array, rule: str, limit: Any) -> np.ndarray:
    n = len(values)
    valid = pc.is_valid(values).to_numpy(zero_copy_only=False)
    if rule == "regex":
        return _bad_regex(values, limit)
    if rule == "in_set":
        texts = canonical_text_array(attr.type, values)
        inside = pc.is_in(texts, value_set=pa.array(list(limit), type=pa.string()))
        return valid & ~_numpy(inside, False).astype(bool)
    if rule == "max_length":
        too_long = pc.greater(pc.utf8_length(values), limit)
        return valid & _numpy(too_long, False).astype(bool)
    if n == 0:
        return np.zeros(0, dtype=bool)
    return _bad_bound(attr, values, rule, limit)


def process_column(attr: Attribute, raw: pa.Array | pa.ChunkedArray) -> ColumnResult:
    """Cast, normalise and check a whole column; same result as `process_attribute` per value."""
    values, bad_cast = cast_column(attr, _chunk(raw))
    failures: list[tuple[np.ndarray, str, str]] = []
    if bad_cast is not None and bad_cast.any():
        failures.append((np.flatnonzero(bad_cast), "cast", "reject_record"))
    if attr.type == "string":
        values = normalize_strings(values, list(attr.normalize))
        values = pc.if_else(pc.equal(values, ""), pa.scalar(None, pa.string()), values)
    for check in attr.checks:
        if len(values) == 0:
            break
        bad = _bad_check(attr, values, check.rule, check.value)
        if bad.any():
            failures.append((np.flatnonzero(bad), check.rule, check.on_fail))
            if check.on_fail == "null_value":
                values = pc.if_else(pa.array(bad), pa.scalar(None, values.type), values)
    return ColumnResult(values, failures)


def string_keys(raw: pa.Array | pa.ChunkedArray) -> pa.Array:
    """`str(raw_key)` (null stays null)."""
    arr = _chunk(raw)
    t = arr.type
    if pa.types.is_string(t) or pa.types.is_large_string(t):
        return arr.cast(pa.string())
    if pa.types.is_integer(t):
        return arr.cast(pa.string())
    if pa.types.is_null(t):
        return pa.nulls(len(arr), pa.string())
    values, _ = _python_cast(arr, str, pa.string())
    return values


def parse_revision(raw: pa.Array | pa.ChunkedArray) -> tuple[pa.Array, np.ndarray]:
    """timestamp('us') revisions and a mask of unparsable values (reference `to_timestamp`)."""
    arr = _chunk(raw)
    t, target = arr.type, pa.timestamp("us")
    if pa.types.is_null(t):
        return pa.nulls(len(arr), target), np.zeros(len(arr), dtype=bool)
    try:
        if pa.types.is_timestamp(t) and t.tz is None and t.unit in ("s", "ms", "us"):
            return arr.cast(target), np.zeros(len(arr), dtype=bool)
        if pa.types.is_date32(t):
            return arr.cast(target), np.zeros(len(arr), dtype=bool)
    except (pa.ArrowInvalid, pa.ArrowNotImplementedError):
        pass
    return _python_cast(arr, to_timestamp, target)


def row_hash_column(
    config: ReferentialConfig, source_id: str, values: dict[str, pa.Array], n: int
) -> list[str]:
    """sha256 hex per row of `engine.core.canonical.row_hash_input` (vectorised input build)."""
    mapped = config.mapping(source_id)
    parts: list[pa.Array] = []
    for attr in config.attributes:
        if attr.name not in mapped:
            continue
        text = canonical_text_array(attr.type, values[attr.name])
        token = pc.binary_join_element_wise(pa.scalar("\x01"), text, "")
        token = pc.fill_null(token, "\x00")
        parts.append(pc.binary_join_element_wise(pa.scalar(f"{attr.name}\x1f"), token, ""))
    if not parts:
        return [hashlib.sha256(b"").hexdigest()] * n
    joined = pc.binary_join_element_wise(*parts, "\x1e")
    return [hashlib.sha256(s.encode("utf-8")).hexdigest() for s in joined.to_pylist()]


def canonical_texts(attr_type: str, values: list[Any]) -> list[str | None]:
    return [canonical_text(v, attr_type) for v in values]
