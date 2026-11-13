"""Vectorised ingest pipeline == reference per-value semantics (no Docker needed)."""

from __future__ import annotations

import random
from datetime import UTC, date, datetime, timedelta, timezone
from decimal import Decimal

import pyarrow as pa
import pytest

from engine.core.canonical import arrow_type, canonical_text, row_hash
from engine.core.config import Attribute
from engine.core.reference.values import process_attribute
from engine.lanes.graph import values as V
from tests.core.reference.ref_helpers import make_config

RNG = random.Random(7)
WORDS = [
    "Jean",
    " jean ",
    "JEAN-PIERRE",
    "Éloïse",
    "eloise",
    "  multiple   spaces  ",
    "tab\tx",
    "",
    " ",
    "ß",
    "İstanbul",
    "Ǆ",
    "a\u00a0b",
    "x\x1fy",
    "\x1cz\x1d",
    "straße",
    "日本語",
    "ÀÉÎÕÜ",
    "O'Neil",
    "john@example.com",
    "bad@",
    "a@b.c",
    "FR",
    "fr ",
    "XX-1234",
    "ab-1234",
    "12345",
    "a" * 13,
    "é" * 12,
    "é" * 13,
    "line\nbreak",
    "ok\n",
    "\u2003em\u2003",
]


def attr(**kw) -> Attribute:
    base = {"name": "x", "sources": [{"source": "crm", "column": "c"}]}
    return Attribute.model_validate({**base, **kw})


def rand_value(kind: str):
    r = RNG.random()
    if r < 0.12:
        return None
    if kind == "string":
        r2 = RNG.random()
        if r2 < 0.8:
            return RNG.choice(WORDS)
        if r2 < 0.9:
            return RNG.choice([1, 2.5, Decimal("3.10"), 10**20])
        return RNG.choice([True, b"x"[0:0] or "z"])
    if kind == "int":
        return RNG.choice([0, 1, -3, 7, 2**40, 5.0, "12", "x", 2.5, Decimal("4"), True, " 9 "])
    if kind == "decimal":
        return RNG.choice(
            [
                Decimal("1.23456"),
                Decimal("0"),
                Decimal("-0.00005"),
                3,
                2.5,
                "7.125",
                "abc",
                Decimal("NaN"),
                Decimal("1E+20"),
                True,
                Decimal("0.00004"),
                Decimal("12.34995"),
            ]
        )
    if kind == "date":
        return RNG.choice(
            [
                date(2020, 2, 29),
                datetime(1960, 5, 4, 23, 59),
                "1999-12-31",
                "bad",
                5,
                date(1, 1, 1),
                " 2000-01-01 ",
            ]
        )
    if kind == "timestamp":
        return RNG.choice(
            [
                datetime(2020, 1, 1, 12, 0, 0, 123456),
                date(2021, 5, 5),
                "2022-03-04T05:06:07",
                datetime(1950, 1, 1, 0, 0, 0, 5),
                datetime(2020, 1, 1, tzinfo=timezone(timedelta(hours=2))),
                "nope",
                3,
                datetime(2020, 1, 1, tzinfo=UTC),
            ]
        )
    return RNG.choice([True, False, "true", "0", "FALSE", " 1 ", "yes", 1, 0, 2])


CASES = [
    ("string", {"normalize": ["trim", "lower", "strip_accents", "collapse_spaces"]}),
    ("string", {"normalize": ["upper", "alnum_only"]}),
    ("string", {"normalize": ["digits_only", "trim"]}),
    (
        "string",
        {
            "normalize": ["strip_accents", "lower"],
            "checks": [{"rule": "max_length", "value": 12, "on_fail": "reject_record"}],
        },
    ),
    (
        "string",
        {
            "normalize": ["trim", "lower"],
            "checks": [
                {"rule": "regex", "value": r"^[^@\s]+@[^@\s]+\.[^@\s]+$", "on_fail": "null_value"},
                {"rule": "max_length", "value": 5, "on_fail": "warn"},
            ],
        },
    ),
    (
        "string",
        {
            "normalize": ["trim", "upper"],
            "checks": [
                {"rule": "in_set", "value": ["FR", "BE", "XX-1234"], "on_fail": "warn"},
                {"rule": "regex", "value": r"^[A-Z]{2}-[0-9]{4}$", "on_fail": "reject_record"},
            ],
        },
    ),
    ("string", {}),
    (
        "int",
        {
            "checks": [
                {"rule": "min", "value": 0, "on_fail": "reject_record"},
                {"rule": "max", "value": 100.5, "on_fail": "null_value"},
            ]
        },
    ),
    ("int", {"checks": [{"rule": "in_set", "value": ["1", "7"], "on_fail": "warn"}]}),
    (
        "decimal",
        {
            "checks": [
                {"rule": "min", "value": -0.0001, "on_fail": "null_value"},
                {"rule": "max", "value": 10, "on_fail": "warn"},
            ]
        },
    ),
    ("decimal", {}),
    (
        "date",
        {"checks": [{"rule": "in_set", "value": ["2020-02-29", "1999-12-31"], "on_fail": "warn"}]},
    ),
    ("timestamp", {}),
    ("bool", {}),
]


def raw_arrays(kind: str, values: list):
    """The same raw values under several Arrow representations."""
    yield pa.array(values, type=pa.object_()) if False else None
    try:
        yield pa.array(values)
    except (pa.ArrowInvalid, pa.ArrowTypeError, OverflowError):
        return


def natural_array(kind: str, values: list):
    typed = {
        "string": pa.string(),
        "int": pa.int64(),
        "date": pa.date32(),
        "timestamp": pa.timestamp("us"),
        "bool": pa.bool_(),
    }.get(kind)
    clean = []
    for v in values:
        ok = (
            (kind == "string" and isinstance(v, str))
            or (kind == "int" and isinstance(v, int) and not isinstance(v, bool) and abs(v) < 2**62)
            or (kind == "date" and type(v) is date)
            or (kind == "timestamp" and type(v) is datetime and v.tzinfo is None)
            or (kind == "bool" and isinstance(v, bool))
            or (
                kind == "decimal"
                and isinstance(v, Decimal)
                and v.is_finite()
                and abs(v) < 10**13
                and v == v.quantize(Decimal("0.0001"))
            )
        )
        clean.append(v if ok or v is None else None)
    if kind == "decimal":
        return pa.array(clean, type=pa.decimal128(18, 4)), clean
    return pa.array(clean, type=typed), clean


def reference(a: Attribute, values: list):
    out, fails = [], []
    for i, raw in enumerate(values):
        v, f = process_attribute(a, raw)
        out.append(v)
        fails += [(i, x.rule, x.action) for x in f]
    return out, sorted(fails)


def check(a: Attribute, arr: pa.Array, values: list):
    res = V.process_column(a, arr)
    want, want_fail = reference(a, values)
    got = res.values.to_pylist()
    assert res.values.type == arrow_type(a.type)
    got_fail = sorted((int(i), rule, action) for idx, rule, action in res.failures for i in idx)
    assert got_fail == want_fail
    for i, (g, w) in enumerate(zip(got, want, strict=True)):
        # a cast failure is a reject_record: the value is irrelevant then
        if any(f[0] == i and f[1] == "cast" for f in want_fail):
            continue
        assert g == w, (i, values[i], g, w)


@pytest.mark.parametrize(("kind", "extra"), CASES)
def test_object_path_matches_reference(kind, extra):
    a = attr(type=kind, **extra)
    values = [rand_value(kind) for _ in range(400)]
    try:
        arr = pa.array(values)
    except (pa.ArrowInvalid, pa.ArrowTypeError, OverflowError):
        arr = pa.array([str(v) if v is not None else None for v in values])
        values = [str(v) if v is not None else None for v in values]
    check(a, arr, values)


@pytest.mark.parametrize(("kind", "extra"), CASES)
def test_natural_arrow_types_match_reference(kind, extra):
    a = attr(type=kind, **extra)
    values = [rand_value(kind) for _ in range(600)]
    arr, clean = natural_array(kind, values)
    check(a, arr, clean)


def test_decimal_ranges_and_scales_use_reference_rounding():
    a = attr(type="decimal", checks=[{"rule": "min", "value": 0, "on_fail": "reject_record"}])
    values = [
        Decimal("0.00005"),
        Decimal("1.99995"),
        Decimal("-1.23456"),
        None,
        Decimal("99999999999999"),
    ]
    check(a, pa.array(values), values)


def test_canonical_text_and_hash_match_reference():
    cfg = make_config()
    cols = {
        "string": ["é", None, "x y"],
        "int": [5, -2, None],
        "decimal": [Decimal("12.5"), Decimal("-0.0500"), None],
        "date": [date(2020, 1, 2), None, date(1999, 12, 31)],
        "timestamp": [
            datetime(2020, 1, 1, 1, 2, 3, 4),
            datetime(1950, 6, 7, 8, 9, 10, 999999),
            None,
        ],
        "bool": [True, False, None],
    }
    for kind, values in cols.items():
        arr = pa.array(values, type=arrow_type(kind))
        got = V.canonical_text_array(kind, arr).to_pylist()
        assert got == [canonical_text(v, kind) for v in values]
    for source_id in ("crm", "erp", "web"):
        mapped = cfg.mapping(source_id)
        rows = []
        for i in range(6):
            rows.append(
                {
                    "last_name": f"n{i}" if i % 3 else None,
                    "first_name": "jean" if i % 2 else None,
                    "birth_date": date(1980, 1, 1 + i) if i % 4 else None,
                    "email": f"a{i}@x.fr" if i % 5 else None,
                    "country": "FR" if i % 2 else None,
                    "credit_limit": Decimal(i) / 4 if i % 3 else None,
                }
            )
        arrays = {
            a.name: pa.array([r[a.name] for r in rows], type=arrow_type(a.type))
            for a in cfg.attributes
        }
        got = V.row_hash_column(cfg, source_id, arrays, len(rows))
        assert got == [row_hash(cfg, source_id, r) for r in rows]
        assert mapped  # sanity


def test_utf8_slice_and_length_are_code_points():
    import pyarrow.compute as pc

    words = ["éàü😀abc", "日本語テキスト", "ab", ""]
    arr = pa.array(words)
    assert pc.utf8_length(arr).to_pylist() == [len(w) for w in words]
    assert pc.utf8_slice_codeunits(arr, 0, 4).to_pylist() == [w[:4] for w in words]
