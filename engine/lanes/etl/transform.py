"""Vectorised transformation of source batches: normalise, cast, DQ checks, row hash.

The hot path works on Arrow arrays (pyarrow.compute / numpy). Semantics are those of the
reference engine (engine.core.reference docstring): every Arrow fast path is exact on its domain
and anything it cannot guarantee falls back to a per-value Python implementation of the same
rule on the affected rows only:

* strings made only of printable ASCII use Arrow kernels; rows holding any other character
  (accents, control characters...) go through Python `str` methods (Unicode case mapping,
  NFD accent stripping, `\\s` and `re` semantics are not those of Arrow/RE2);
* a column whose vectorised cast fails in a batch is re-cast value by value (dirty data only).
"""

from __future__ import annotations

import hashlib
import re
import unicodedata
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from typing import Any

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc

from engine.core.canonical import arrow_type
from engine.core.config import Attribute, Check, ReferentialConfig, Source

KIND_OK, KIND_DQ_REJECTED, KIND_BAD_REVISION = 0, 1, 2
SCOPE_ROW, SCOPE_WINNER = 0, 1  # reject kept always / only when the row wins deduplication
_DECIMAL_STEP = Decimal("0.0001")
_DECIMAL_MAX = Decimal(10) ** 14
_SAFE = r"^[\x20-\x7e]*$"
_ASCII_DIGITS = frozenset("0123456789")
_ASCII_ALNUM = frozenset("0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
_TRUE, _FALSE = {"true", "1"}, {"false", "0"}
_STR = pa.large_string()

META_SCHEMA = pa.schema(
    [
        ("_ord", pa.int64()),
        ("source_key", pa.string()),
        ("revision", pa.timestamp("us")),
        ("kind", pa.int16()),
        ("row_hash", pa.string()),
    ]
)
REJECT_SCHEMA = pa.schema(
    [
        ("_ord", pa.int64()),
        ("source_key", pa.string()),
        ("attribute", pa.string()),
        ("rule", pa.string()),
        ("action", pa.string()),
        ("scope", pa.int16()),
    ]
)


class CastError(ValueError):
    pass


# --------------------------------------------------------------------- scalar (exact) rules


def _strip_accents(value: str) -> str:
    decomposed = unicodedata.normalize("NFD", value)
    return unicodedata.normalize(
        "NFC", "".join(c for c in decomposed if not unicodedata.combining(c))
    )


_PY_NORMALIZERS = {
    "trim": str.strip,
    "lower": str.lower,
    "upper": str.upper,
    "strip_accents": _strip_accents,
    "collapse_spaces": lambda v: re.sub(r"\s+", " ", v),
    "digits_only": lambda v: "".join(c for c in v if c in _ASCII_DIGITS),
    "alnum_only": lambda v: "".join(c for c in v if c in _ASCII_ALNUM),
}


def normalize_py(value: str, normalizers: list[str]) -> str:
    for name in normalizers:
        value = _PY_NORMALIZERS[name](value)
    return value


def _to_decimal(raw: Any) -> Decimal:
    if isinstance(raw, bool):
        raise CastError("bool is not a decimal")
    try:
        value = raw if isinstance(raw, Decimal) else Decimal(str(raw).strip())
    except InvalidOperation as error:
        raise CastError(f"not a decimal: {raw!r}") from error
    if not value.is_finite():
        raise CastError("decimal must be finite")
    value = value.quantize(_DECIMAL_STEP, rounding=ROUND_HALF_UP)
    if abs(value) >= _DECIMAL_MAX:
        raise CastError("decimal out of range")
    return value


def _to_timestamp(raw: Any) -> datetime:
    if isinstance(raw, datetime):
        value = raw
    elif isinstance(raw, date):
        value = datetime(raw.year, raw.month, raw.day)
    elif isinstance(raw, str):
        try:
            value = datetime.fromisoformat(raw.strip())
        except ValueError as error:
            raise CastError(f"not a timestamp: {raw!r}") from error
    else:
        raise CastError(f"not a timestamp: {raw!r}")
    if value.tzinfo is not None:
        value = value.astimezone(UTC).replace(tzinfo=None)
    return value


def cast_scalar(raw: Any, attr_type: str) -> Any:
    """Cast a raw non-null Python value or raise CastError (reference semantics)."""
    if attr_type == "string":
        if isinstance(raw, bool) or not isinstance(raw, (str, int, float, Decimal)):
            raise CastError(f"not a string: {raw!r}")
        return raw if isinstance(raw, str) else str(raw)
    if attr_type == "int":
        if isinstance(raw, bool):
            raise CastError("bool is not an int")
        if isinstance(raw, int):
            return raw
        try:
            value = Decimal(str(raw).strip())
        except InvalidOperation as error:
            raise CastError(f"not an int: {raw!r}") from error
        if not value.is_finite() or value != value.to_integral_value():
            raise CastError(f"not an int: {raw!r}")
        return int(value)
    if attr_type == "decimal":
        return _to_decimal(raw)
    if attr_type == "date":
        if isinstance(raw, datetime):
            return raw.date()
        if isinstance(raw, date):
            return raw
        try:
            return date.fromisoformat(str(raw).strip())
        except ValueError as error:
            raise CastError(f"not a date: {raw!r}") from error
    if attr_type == "timestamp":
        return _to_timestamp(raw)
    if attr_type == "bool":
        if isinstance(raw, bool):
            return raw
        text = str(raw).strip().lower()
        if text in _TRUE:
            return True
        if text in _FALSE:
            return False
        raise CastError(f"not a bool: {raw!r}")
    raise CastError(f"unknown type {attr_type!r}")


def _canonical_py(value: Any, attr_type: str) -> str:
    if attr_type == "string":
        return str(value)
    if attr_type == "int":
        return str(int(value))
    if attr_type == "decimal":
        return format(Decimal(value).quantize(_DECIMAL_STEP), "f")
    if attr_type == "date":
        return value.isoformat()
    if attr_type == "timestamp":
        return value.strftime("%Y-%m-%dT%H:%M:%S.%f")
    return "true" if value else "false"


def check_py(attr: Attribute, check: Check, value: Any) -> bool:
    """One check on one non-null value (reference semantics)."""
    rule, limit = check.rule, check.value
    if rule == "regex":
        return re.search(limit, value) is not None
    if rule == "in_set":
        return _canonical_py(value, attr.type) in limit
    if rule == "max_length":
        return len(value) <= limit
    bound = Decimal(str(limit))
    number = Decimal(value)
    return number >= bound if rule == "min" else number <= bound


# ------------------------------------------------------------------------ canonical text


def canon_text(arr: pa.Array, attr_type: str) -> pa.Array:
    """Vectorised `engine.core.canonical.canonical_text` (large_string, null stays null)."""
    if attr_type == "string":
        out = arr
    elif attr_type in {"int", "decimal", "date"}:
        out = pc.cast(arr, pa.string())
    elif attr_type == "bool":
        out = pc.if_else(arr, pa.scalar("true"), pa.scalar("false"))
    elif attr_type == "timestamp":
        micros = pc.fill_null(pc.cast(arr, pa.int64()), 0).to_numpy(zero_copy_only=False)
        text = micros.astype("datetime64[us]").astype("U26")
        out = pa.array(text, mask=pc.is_null(arr).to_numpy(zero_copy_only=False))
    else:
        raise ValueError(f"unknown attribute type {attr_type!r}")
    return pc.cast(out, _STR)


def sha256_hex(joined: pa.Array) -> list[str]:
    """sha256 hex of each element of a (large_)string array, without materialising strings."""
    joined = pc.cast(joined, _STR)
    if isinstance(joined, pa.ChunkedArray):
        joined = joined.combine_chunks()
    n = len(joined)
    if n == 0:
        return []
    buffers = joined.buffers()
    offsets = np.frombuffer(buffers[1], dtype=np.int64)[joined.offset : joined.offset + n + 1]
    data = memoryview(buffers[2])
    bounds = offsets.tolist()
    digest = hashlib.sha256
    return [digest(data[a:b]).hexdigest() for a, b in zip(bounds, bounds[1:], strict=False)]


def row_hashes(attrs: list[Attribute], values: dict[str, pa.Array], n: int) -> pa.Array:
    """`engine.core.canonical.row_hash` of every row (attributes mapped by the source)."""
    args: list[Any] = []
    for i, attr in enumerate(attrs):
        text = canon_text(values[attr.name], attr.type)
        token = pc.if_else(
            pc.is_null(text),
            pa.scalar("\x00", _STR),
            pc.binary_join_element_wise(pa.scalar("\x01", _STR), text, pa.scalar("", _STR)),
        )
        args.append(pa.scalar(("\x1e" if i else "") + attr.name + "\x1f", _STR))
        args.append(token)
    if not args:
        return pa.array([hashlib.sha256(b"").hexdigest()] * n, pa.string())
    joined = pc.binary_join_element_wise(*args, pa.scalar("", _STR))
    return pa.array(sha256_hex(joined), pa.string())


# --------------------------------------------------------------------------------- casting


def _plain(arr: pa.Array) -> pa.Array:
    if isinstance(arr, pa.ChunkedArray):
        arr = arr.combine_chunks()
    if pa.types.is_dictionary(arr.type):
        arr = arr.dictionary_decode()
    return arr


def _fast_cast(arr: pa.Array, attr_type: str) -> pa.Array:
    """Exact vectorised cast or an exception (caller falls back to the scalar path)."""
    t = arr.type
    is_str = pa.types.is_string(t) or pa.types.is_large_string(t)
    is_int = pa.types.is_integer(t)
    is_dec = pa.types.is_decimal(t)
    is_ts = pa.types.is_timestamp(t)
    if attr_type == "string":
        if is_str or is_int or (is_dec and t.scale <= 6):
            return pc.cast(arr, pa.string())
    elif attr_type == "int":
        if is_int or is_str or is_dec:
            return pc.cast(arr, pa.int64())
    elif attr_type == "decimal":
        if is_int or is_str or is_dec:
            return pc.cast(arr, arrow_type("decimal"))
    elif attr_type == "date":
        if pa.types.is_date32(t):
            return arr
        if is_str:
            return pc.cast(arr, pa.date32())
        if is_ts and t.tz is None:
            return pc.cast(arr, pa.date32(), safe=False)
    elif attr_type == "timestamp":
        if is_ts:
            return pc.cast(arr, arrow_type("timestamp"), safe=False)
        if pa.types.is_date32(t) or is_str:
            return pc.cast(arr, arrow_type("timestamp"))
    elif attr_type == "bool":
        if pa.types.is_boolean(t):
            return arr
        if is_str:
            text = pc.ascii_lower(pc.utf8_trim(arr, characters=" "))
            known = pc.is_in(text, value_set=pa.array(sorted(_TRUE | _FALSE)))
            if pc.any(pc.and_(pc.is_valid(text), pc.invert(known))).as_py():
                raise pa.ArrowInvalid("not a bool")
            return pc.is_in(text, value_set=pa.array(sorted(_TRUE)))
    raise pa.ArrowNotImplementedError("no vectorised path")


def cast_column(arr: pa.Array, attr_type: str) -> tuple[pa.Array, np.ndarray]:
    """Cast a raw column to the canonical Arrow type; returns (values, failed mask).

    Null raw values stay null and are not failures.
    """
    arr = _plain(arr)
    n = len(arr)
    target = arrow_type(attr_type)
    if arr.null_count == n:
        return pa.nulls(n, target), np.zeros(n, dtype=bool)
    try:
        out = _fast_cast(arr, attr_type)
        if out.null_count != arr.null_count:
            raise pa.ArrowInvalid("null count changed")
        if attr_type == "string":
            out = pc.cast(out, pa.string())
        return out, np.zeros(n, dtype=bool)
    except (pa.ArrowInvalid, pa.ArrowNotImplementedError, pa.ArrowTypeError, OverflowError):
        pass
    values: list[Any] = []
    failed = np.zeros(n, dtype=bool)
    for i, raw in enumerate(arr.to_pylist()):
        if raw is None:
            values.append(None)
            continue
        try:
            values.append(cast_scalar(raw, attr_type))
        except CastError:
            values.append(None)
            failed[i] = True
    return pa.array(values, target), failed


# ------------------------------------------------------------------------- normalisation


def _unsafe_mask(arr: pa.Array) -> pa.Array:
    """True where the string holds anything but printable ASCII (needs the Python path)."""
    return pc.fill_null(pc.invert(pc.match_substring_regex(arr, _SAFE)), False)


_FAST_OPS = {
    "trim": lambda a: pc.utf8_trim(a, characters=" "),
    "lower": pc.ascii_lower,
    "upper": pc.ascii_upper,
    "strip_accents": lambda a: a,
    "collapse_spaces": lambda a: pc.replace_substring_regex(a, " +", " "),
    "digits_only": lambda a: pc.replace_substring_regex(a, "[^0-9]", ""),
    "alnum_only": lambda a: pc.replace_substring_regex(a, "[^0-9A-Za-z]", ""),
}


def normalize_column(arr: pa.Array, normalizers: list[str]) -> pa.Array:
    """Normalise a string column; empty results become null."""
    out = arr
    if normalizers:
        fast = arr
        for name in normalizers:
            fast = _FAST_OPS[name](fast)
        unsafe = _unsafe_mask(arr)
        if pc.any(unsafe).as_py():
            idx = np.flatnonzero(unsafe.to_numpy(zero_copy_only=False))
            py = [normalize_py(v, normalizers) for v in arr.take(pa.array(idx)).to_pylist()]
            fast = pc.replace_with_mask(fast, unsafe, pa.array(py, arr.type))
        out = fast
    return pc.if_else(pc.equal(out, ""), pa.scalar(None, out.type), out)


# ---------------------------------------------------------------------------------- checks


def _bound_scale(bound: Decimal) -> int | None:
    exponent = bound.as_tuple().exponent
    if not isinstance(exponent, int) or not bound.is_finite():
        return None
    scale = max(-exponent, 0)
    return scale if scale <= 18 and len(bound.as_tuple().digits) + max(exponent, 0) <= 18 else None


def _py_passes(attr: Attribute, check: Check, arr: pa.Array) -> pa.Array:
    return pa.array(
        [None if v is None else check_py(attr, check, v) for v in arr.to_pylist()], pa.bool_()
    )


def check_pass(attr: Attribute, check: Check, arr: pa.Array) -> pa.Array:
    """Boolean array: the check passes (null where the value is null)."""
    rule, limit = check.rule, check.value
    if rule == "max_length":
        return pc.less_equal(pc.utf8_length(arr), pa.scalar(limit))
    if rule == "in_set":
        text = canon_text(arr, attr.type)
        return pc.is_in(text, value_set=pa.array(list(limit), _STR))
    if rule == "regex":
        unsafe = _unsafe_mask(arr)
        try:
            ok = pc.match_substring_regex(arr, str(limit))
        except pa.ArrowInvalid:
            return _py_passes(attr, check, arr)
        if pc.any(unsafe).as_py():
            idx = np.flatnonzero(unsafe.to_numpy(zero_copy_only=False))
            sub = _py_passes(attr, check, arr.take(pa.array(idx)))
            ok = pc.replace_with_mask(ok, unsafe, sub)
        return ok
    bound = Decimal(str(limit))
    scale = _bound_scale(bound)
    if scale is None:
        return _py_passes(attr, check, arr)
    scale = max(scale, 4 if attr.type == "decimal" else 0)
    wide = pc.cast(arr, pa.decimal128(38, scale))
    scalar = pa.scalar(bound, pa.decimal128(38, scale))
    return pc.greater_equal(wide, scalar) if rule == "min" else pc.less_equal(wide, scalar)


# -------------------------------------------------------------------------- batch pipeline


@dataclass
class Batch:
    rows: int  # source rows read
    meta: pa.Table  # META_SCHEMA, every row with a usable key
    values: pa.Table  # `_ord` + mapped attributes, rows of kind OK only
    rejects: pa.Table  # REJECT_SCHEMA
    key_null: int = 0


def _empty_rejects() -> list[pa.Table]:
    return []


def _reject_piece(
    ordinals: np.ndarray, keys: pa.Array, attribute: str, rule: str, action: str, scope: int
) -> pa.Table:
    n = len(ordinals)
    return pa.table(
        {
            "_ord": pa.array(ordinals, pa.int64()),
            "source_key": keys,
            "attribute": pa.repeat(pa.scalar(attribute, pa.string()), n),
            "rule": pa.repeat(pa.scalar(rule, pa.string()), n),
            "action": pa.repeat(pa.scalar(action, pa.string()), n),
            "scope": pa.array(np.full(n, scope, dtype=np.int16), pa.int16()),
        },
        schema=REJECT_SCHEMA,
    )


def to_key(arr: pa.Array) -> pa.Array:
    """`str(raw key)`; null/empty stay null/empty (caller rejects them)."""
    arr = _plain(arr)
    t = arr.type
    if pa.types.is_string(t) or pa.types.is_large_string(t) or pa.types.is_integer(t):
        return pc.cast(arr, pa.string())
    return pa.array([None if v is None else str(v) for v in arr.to_pylist()], pa.string())


class SourceTransform:
    """Turns raw batches of one source into stage-ready Arrow tables."""

    def __init__(self, cfg: ReferentialConfig, source_id: str) -> None:
        self.cfg = cfg
        self.source: Source = cfg.source(source_id)
        self.mapping = cfg.mapping(source_id)
        self.attrs = [a for a in cfg.attributes if a.name in self.mapping]
        wanted = [self.source.key]
        if self.source.revision:
            wanted.append(self.source.revision)
        wanted += [self.mapping[a.name] for a in self.attrs]
        self.columns = list(dict.fromkeys(wanted))

    def _process_attribute(
        self, attr: Attribute, raw: pa.Array, ordinals: np.ndarray, keys: pa.Array
    ) -> tuple[pa.Array, np.ndarray, list[pa.Table]]:
        pieces: list[pa.Table] = []
        rejected = np.zeros(len(ordinals), dtype=bool)
        value, failed = cast_column(raw, attr.type)
        if failed.any():
            idx = np.flatnonzero(failed)
            pieces.append(
                _reject_piece(
                    ordinals[idx], keys.take(pa.array(idx)), attr.name, "cast", "reject_record",
                    SCOPE_WINNER,
                )
            )  # fmt: skip
            rejected |= failed
        if attr.type == "string":
            value = normalize_column(value, list(attr.normalize))
        active = ~failed
        for check in attr.checks:
            passes = pc.fill_null(check_pass(attr, check, value), True)
            valid = pc.is_valid(value).to_numpy(zero_copy_only=False)
            fail = active & valid & ~passes.to_numpy(zero_copy_only=False)
            if not fail.any():
                continue
            idx = np.flatnonzero(fail)
            pieces.append(
                _reject_piece(
                    ordinals[idx], keys.take(pa.array(idx)), attr.name, check.rule, check.on_fail,
                    SCOPE_WINNER,
                )
            )  # fmt: skip
            if check.on_fail == "reject_record":
                rejected |= fail
            elif check.on_fail == "null_value":
                value = pc.if_else(pa.array(fail), pa.scalar(None, value.type), value)
                active = active & ~fail
        return value, rejected, pieces

    def _revision(self, raw: pa.Array) -> tuple[pa.Array, np.ndarray]:
        value, failed = cast_column(raw, "timestamp")
        return value, failed

    def run(self, raw: pa.Table, ordinal0: int) -> Batch:
        n = raw.num_rows
        ordinals = np.arange(ordinal0, ordinal0 + n, dtype=np.int64)
        key = to_key(raw.column(self.source.key))
        usable = pc.fill_null(pc.and_(pc.is_valid(key), pc.not_equal(key, "")), False)
        usable_np = usable.to_numpy(zero_copy_only=False)
        pieces: list[pa.Table] = []
        key_null = int(n - usable_np.sum())
        if key_null:
            idx = np.flatnonzero(~usable_np)
            empty = pa.array([""] * key_null, pa.string())
            pieces.append(_reject_piece(ordinals[idx], empty, "", "key_null", "reject_record", 0))
            raw = raw.filter(usable)
            key = key.filter(usable)
            ordinals = ordinals[usable_np]
        m = len(ordinals)
        if self.source.revision:
            revision, bad_rev = self._revision(raw.column(self.source.revision))
        else:
            revision, bad_rev = pa.nulls(m, arrow_type("timestamp")), np.zeros(m, dtype=bool)
        if bad_rev.any():
            idx = np.flatnonzero(bad_rev)
            pieces.append(
                _reject_piece(
                    ordinals[idx], key.take(pa.array(idx)), "", "revision", "reject_record",
                    SCOPE_ROW,
                )
            )  # fmt: skip
        values: dict[str, pa.Array] = {}
        record_rejected = np.zeros(m, dtype=bool)
        for attr in self.attrs:
            value, rejected, attr_pieces = self._process_attribute(
                attr, raw.column(self.mapping[attr.name]), ordinals, key
            )
            values[attr.name] = value
            record_rejected |= rejected
            pieces += attr_pieces
        kind = np.zeros(m, dtype=np.int16)
        kind[record_rejected] = KIND_DQ_REJECTED
        kind[bad_rev] = KIND_BAD_REVISION
        ok = kind == KIND_OK
        hashes = row_hashes(self.attrs, values, m)
        hashes = pc.if_else(pa.array(ok), hashes, pa.scalar(None, pa.string()))
        meta = pa.table(
            {
                "_ord": pa.array(ordinals, pa.int64()),
                "source_key": key,
                "revision": revision,
                "kind": pa.array(kind, pa.int16()),
                "row_hash": hashes,
            },
            schema=META_SCHEMA,
        )
        ok_mask = pa.array(ok)
        value_table = pa.table(
            {"_ord": pa.array(ordinals, pa.int64()), **{k: v for k, v in values.items()}}
        ).filter(ok_mask)
        rejects = pa.concat_tables(pieces) if pieces else REJECT_SCHEMA.empty_table()
        return Batch(n, meta, value_table, rejects, key_null)
