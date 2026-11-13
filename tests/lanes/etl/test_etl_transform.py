"""Vectorised transform of the ETL lane vs the per-value reference semantics (no database)."""

from __future__ import annotations

import random
from collections import Counter
from datetime import date, datetime
from decimal import Decimal

import pyarrow as pa
import pyarrow.compute as pc
import pytest

from engine.core.canonical import row_hash
from engine.core.config import ReferentialConfig
from engine.core.reference.values import process_attribute
from engine.lanes.etl.transform import SourceTransform, canon_text

RAW = {
    "referential": "t",
    "version": 1,
    "state": {"dsn_env": "X", "schema": "mdm_t"},
    "sources": [
        {"id": "s1", "kind": "parquet", "location": "/x/*.parquet", "key": "id", "revision": "rev"}
    ],
    "attributes": [
        {
            "name": "name_s",
            "nullable": False,
            "normalize": ["trim", "lower", "strip_accents", "collapse_spaces"],
            "sources": [{"source": "s1", "column": "c_name"}],
            "checks": [
                {"rule": "max_length", "value": 8, "on_fail": "reject_record"},
                {"rule": "regex", "value": "^[a-z ]+$", "on_fail": "null_value"},
            ],
        },
        {
            "name": "code",
            "normalize": ["trim", "upper", "alnum_only"],
            "sources": [{"source": "s1", "column": "c_code"}],
            "checks": [{"rule": "in_set", "value": ["AB1", "CD2"], "on_fail": "warn"}],
        },
        {
            "name": "digits",
            "normalize": ["digits_only"],
            "sources": [{"source": "s1", "column": "c_digits"}],
        },
        {
            "name": "n",
            "type": "int",
            "sources": [{"source": "s1", "column": "c_n"}],
            "checks": [
                {"rule": "min", "value": 0, "on_fail": "reject_record"},
                {"rule": "max", "value": 1000, "on_fail": "null_value"},
            ],
        },
        {
            "name": "amt",
            "type": "decimal",
            "sources": [{"source": "s1", "column": "c_amt"}],
            "checks": [
                {"rule": "min", "value": 0.5, "on_fail": "reject_record"},
                {"rule": "max", "value": 99999.9999, "on_fail": "warn"},
            ],
        },
        {"name": "d", "type": "date", "sources": [{"source": "s1", "column": "c_d"}]},
        {"name": "ts", "type": "timestamp", "sources": [{"source": "s1", "column": "c_ts"}]},
        {"name": "flag", "type": "bool", "sources": [{"source": "s1", "column": "c_flag"}]},
        {
            "name": "email",
            "normalize": ["trim", "lower", "collapse_spaces"],
            "sources": [{"source": "s1", "column": "c_email"}],
            "checks": [
                {"rule": "regex", "value": r"^[^@\s]+@[^@\s]+\.[^@\s]+$", "on_fail": "warn"}
            ],
        },
    ],
    "matching": {
        "version": "t",
        "blocking": [{"name": "b", "parts": [{"attr": "name_s", "transform": "prefix", "n": 3}]}],
        "comparators": [{"attr": "name_s", "method": "levenshtein", "weight": 1.0}],
    },
}
CFG = ReferentialConfig.model_validate(RAW)

STRINGS = [
    "Dupont",
    "  DUPONT  ",
    chr(201) + "lo" + chr(239) + "se",
    "na" + chr(239) + "ve  caf" + chr(233),
    "",
    "   ",
    "a\tb",
    "tab\tgap",
    "x" * 20,
    "AB 1",
    "ab1",
    "1-2-3",
    "\u0661\u0662\u0663",
    "\u0130stanbul",
    "\u0391\u03a3",
    "a@b.com",
    "bad@",
    " A@B.CO ",
    "emoji\U0001f600",
    "a\x0bb",
    "line\nbreak",
    "Z" * 8,
    "ab cd",
    "AB  CD",
    "CD2",
    "cd-2",
    "\u00a0pad\u00a0",
    "e\u0301cole",
    "A@b.c\n",
    "x y@z.fr",
    "\x1fctl",
    "9",
    "0",
    "-5",
    "1000",
    "1001",
    "12.0",
    " 7 ",
    "1e3",
    "0.4999",
    "0.5",
    "1.23455",
    "99999.99995",
    "2024-01-01",
    "2024-1-1",
    "20240101",
    "2024-01-01T10:20:30",
    "2024-01-01 10:20:30.5",
    "true",
    " FALSE ",
    "1",
    "0",
    "yes",
    "abc",
    "",
]


def rand_strings(rng: random.Random, n: int) -> list[str | None]:
    out: list[str | None] = []
    for _ in range(n):
        r = rng.random()
        if r < 0.1:
            out.append(None)
        elif r < 0.7:
            out.append(rng.choice(STRINGS))
        else:
            alphabet = "abcXYZ  \t09-@.\u00e9\u00df"
            out.append("".join(rng.choice(alphabet) for _ in range(rng.randint(0, 12))))
    return out


def rand_col(rng: random.Random, n: int, kind: str) -> pa.Array:
    if kind == "str":
        return pa.array(rand_strings(rng, n), pa.string())
    if kind == "int":
        return pa.array([rng.choice([None, -3, 0, 5, 999, 1000, 1001, 2**40]) for _ in range(n)])
    if kind == "dec":
        vals = [None, Decimal("0.5"), Decimal("0.49"), Decimal("12.345678"), Decimal("1E+5"),
                Decimal("99999.9999"), Decimal("123456789012.5"), Decimal("-1")]  # fmt: skip
        return pa.array([rng.choice(vals) for _ in range(n)], pa.decimal128(24, 6))
    if kind == "float":
        return pa.array([rng.choice([None, 0.1, 2.5, 1e16, 3.0, -1.5]) for _ in range(n)])
    if kind == "date":
        return pa.array(
            [rng.choice([None, date(2020, 2, 29), date(1999, 12, 31)]) for _ in range(n)]
        )
    if kind == "ts":
        vals = [None, datetime(2024, 1, 1, 10, 20, 30, 123456), datetime(1960, 5, 4, 1, 2, 3, 5)]
        return pa.array([rng.choice(vals) for _ in range(n)], pa.timestamp("us"))
    if kind == "bool":
        return pa.array([rng.choice([None, True, False]) for _ in range(n)])
    raise ValueError(kind)


KINDS = {
    "c_name": ["str"],
    "c_code": ["str", "int"],
    "c_digits": ["str", "int"],
    "c_n": ["int", "str", "dec", "float"],
    "c_amt": ["dec", "str", "int", "float"],
    "c_d": ["date", "str", "ts"],
    "c_ts": ["ts", "str", "date"],
    "c_flag": ["bool", "str", "int"],
    "c_email": ["str"],
}


@pytest.mark.parametrize("seed", range(6))
def test_transform_matches_reference(seed: int) -> None:
    rng = random.Random(seed)
    n = 400
    transform = SourceTransform(CFG, "s1")
    cols = {name: rand_col(rng, n, rng.choice(kinds)) for name, kinds in KINDS.items()}
    keys = [None if i % 97 == 0 else (i // 2 if i % 3 else str(i)) for i in range(n)]
    revs = [rng.choice([None, "2024-01-01T00:00:00", "2024-02-01", "bad"]) for _ in range(n)]
    table = pa.table(
        {"id": pa.array([str(k) if k is not None else None for k in keys]),
         "rev": pa.array(revs, pa.string()), **cols}
    )  # fmt: skip
    batch = transform.run(table, 1000)

    raw_by_col = {name: col.to_pylist() for name, col in cols.items()}
    key_py = table.column("id").to_pylist()
    usable = [i for i, k in enumerate(key_py) if k not in (None, "")]
    assert batch.meta.num_rows == len(usable)
    assert batch.meta.column("_ord").to_pylist() == [1000 + i for i in usable]
    meta_kind = batch.meta.column("kind").to_pylist()
    hashes = batch.meta.column("row_hash").to_pylist()
    expected_rejects: Counter[tuple] = Counter()
    for pos, i in enumerate(usable):
        raw_rev = table.column("rev").to_pylist()[i]
        bad_rev = raw_rev == "bad"
        if bad_rev:
            expected_rejects[(1000 + i, "", "revision", "reject_record")] += 1
        values: dict = {}
        rejected = False
        for attr in CFG.attributes:
            column = {a.name: a for a in CFG.attributes}[attr.name].sources[0].column
            value, failures = process_attribute(attr, raw_by_col[column][i])
            values[attr.name] = value
            for f in failures:
                expected_rejects[(1000 + i, attr.name, f.rule, f.action)] += 1
                rejected = rejected or f.action == "reject_record"
        kind = 2 if bad_rev else (1 if rejected else 0)
        assert meta_kind[pos] == kind, (i, kind)
        if kind == 0:
            assert hashes[pos] == row_hash(CFG, "s1", values), i
            out_row = batch.values.filter(pc.equal(batch.values["_ord"], 1000 + i))
            got = out_row.to_pylist()[0]
            for name, value in values.items():
                assert got[name] == value, (i, name, got[name], value)
        else:
            assert hashes[pos] is None
    got_rejects: Counter[tuple] = Counter()
    for r in batch.rejects.to_pylist():
        if r["rule"] == "key_null":
            continue
        got_rejects[(r["_ord"], r["attribute"], r["rule"], r["action"])] += 1
    for k in list(expected_rejects):
        # duplicate-key rejects are not produced here; compare record/attribute rejects only
        assert got_rejects[k] == expected_rejects[k], k
    assert set(got_rejects) == set(expected_rejects)
    assert batch.key_null == n - len(usable)


def test_canon_text_matches_reference() -> None:
    from engine.core.canonical import canonical_text

    cases = {
        "int": ([None, 0, -7, 2**40], pa.int64()),
        "decimal": (
            [None, Decimal("0"), Decimal("-0.5"), Decimal("12.3456")],
            pa.decimal128(18, 4),
        ),
        "date": ([None, date(2020, 2, 29), date(1, 1, 1)], pa.date32()),
        "timestamp": (
            [None, datetime(2024, 1, 1, 0, 0, 0), datetime(1960, 5, 4, 1, 2, 3, 5)],
            pa.timestamp("us"),
        ),
        "bool": ([None, True, False], pa.bool_()),
        "string": ([None, "a", ""], pa.string()),
    }
    for attr_type, (values, arrow) in cases.items():
        got = canon_text(pa.array(values, arrow), attr_type).to_pylist()
        assert got == [canonical_text(v, attr_type) for v in values], attr_type
