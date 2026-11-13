"""Normalizers, type casting and DQ checks of the reference engine (pure Python, per value)."""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from typing import Any

from engine.core.canonical import canonical_text
from engine.core.config import Attribute

_DECIMAL_STEP = Decimal("0.0001")
_DECIMAL_MAX = Decimal(10) ** 14
_ASCII_DIGITS = frozenset("0123456789")
_ASCII_ALNUM = frozenset("0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
_TRUE = {"true", "1"}
_FALSE = {"false", "0"}


class CastError(ValueError):
    pass


def strip_accents(value: str) -> str:
    decomposed = unicodedata.normalize("NFD", value)
    kept = "".join(c for c in decomposed if not unicodedata.combining(c))
    return unicodedata.normalize("NFC", kept)


_NORMALIZERS = {
    "trim": str.strip,
    "lower": str.lower,
    "upper": str.upper,
    "strip_accents": strip_accents,
    "collapse_spaces": lambda v: re.sub(r"\s+", " ", v),
    "digits_only": lambda v: "".join(c for c in v if c in _ASCII_DIGITS),
    "alnum_only": lambda v: "".join(c for c in v if c in _ASCII_ALNUM),
}


def normalize_string(value: str, normalizers: list[str]) -> str:
    for name in normalizers:
        value = _NORMALIZERS[name](value)
    return value


def to_decimal(raw: Any) -> Decimal:
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
        raise CastError("decimal out of range for decimal(18,4)")
    return value


def to_timestamp(raw: Any) -> datetime:
    """Naive UTC datetime with microsecond precision."""
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


def cast_value(raw: Any, attr_type: str) -> Any:
    """Cast a raw non-null value to its canonical Python type or raise CastError."""
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
        return to_decimal(raw)
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
        return to_timestamp(raw)
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


@dataclass(frozen=True)
class Failure:
    rule: str
    action: str


def _check_passes(attr: Attribute, rule: str, limit: Any, value: Any) -> bool:
    if rule == "regex":
        return re.search(limit, value) is not None
    if rule == "in_set":
        return canonical_text(value, attr.type) in limit
    if rule == "max_length":
        return len(value) <= limit
    bound = Decimal(str(limit))
    number = Decimal(value)
    return number >= bound if rule == "min" else number <= bound


def process_attribute(attr: Attribute, raw: Any) -> tuple[Any, list[Failure]]:
    """Normalise, cast and check one raw value.

    Returns the final value (None if null or nulled by a `null_value` check) and every failure.
    A string that is empty after normalisation is null. Checks never run on null values.
    A cast failure is a `reject_record` failure with rule `cast`.
    """
    if raw is None:
        return None, []
    try:
        value = cast_value(raw, attr.type)
    except CastError:
        return None, [Failure("cast", "reject_record")]
    if attr.type == "string":
        value = normalize_string(value, list(attr.normalize))
        if value == "":
            return None, []
    failures: list[Failure] = []
    for check in attr.checks:
        if _check_passes(attr, check.rule, check.value, value):
            continue
        failures.append(Failure(check.rule, check.on_fail))
        if check.on_fail == "null_value":
            return None, failures
    return value, failures
