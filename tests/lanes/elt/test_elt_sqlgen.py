"""Parité des fragments SQL générés (DuckDB) avec la sémantique de la référence Python.

Ces tests n'ont besoin ni de PostgreSQL ni de dbt : ils évaluent les expressions de
`engine.lanes.elt.sqlgen` dans une base DuckDB en mémoire et les comparent aux fonctions de
`engine.core.reference` / `engine.core.canonical`.
"""

from __future__ import annotations

import itertools
import random
from datetime import date, datetime
from decimal import Decimal

import duckdb
import pytest

from engine.core.canonical import row_hash
from engine.core.config import Attribute, ReferentialConfig
from engine.core.reference.matching import compare_values, score_pair
from engine.core.reference.values import process_attribute
from engine.lanes.elt.dbtplugin import register_udfs
from engine.lanes.elt.sqlgen import (
    Spec,
    cast_sql,
    check_pass_sql,
    comparator_sim_sql,
    normalize_sql,
    q,
    raw_class,
    row_hash_sql,
)

# Casse « simple » de DuckDB vs casse complète de Python : ß, ligatures, İ, sigma final ne sont pas
# couverts (écart documenté dans le README).
CORPUS = [
    "Dupont",
    "  Éléonore  ",
    "ÀÉÎÕÜ ÿ ñ ç",
    "ǅungla",
    "a\u00a0b",
    "a\u2003b\u3000",
    "\t x \n",
    "x\x1cy",
    "a  b   c",
    "12-34 ab_c",
    "ñandú",
    "Ça va?",
    "日本語",
    "e\u0301",
    "é",
    "ǆ",
    "ı",
    "x\u200by",
    "\u0085a\u0085",
    "",
    "   ",
    "O'Brien-Smith",
    "ÅNGSTRÖM",
    "tab\tsep",
    "0123",
]
NORMALIZERS = [
    "trim",
    "lower",
    "upper",
    "strip_accents",
    "collapse_spaces",
    "digits_only",
    "alnum_only",
]


@pytest.fixture(scope="module")
def con() -> duckdb.DuckDBPyConnection:
    c = duckdb.connect()
    c.execute("SET TimeZone = 'UTC'")
    register_udfs(c)
    return c


def scalar(con: duckdb.DuckDBPyConnection, expr: str, *params: object) -> object:
    return con.execute(f"SELECT {expr}", list(params)).fetchone()[0]


@pytest.mark.parametrize(
    "combo", [c for n in (1, 2, 3) for c in itertools.permutations(NORMALIZERS, n)]
)
def test_normalizers_match_reference(con, combo):
    from engine.core.reference.values import normalize_string

    expr = normalize_sql("CAST(? AS VARCHAR)", list(combo))
    for value in CORPUS:
        want = normalize_string(value, list(combo)) or None
        assert scalar(con, expr, value) == want, (combo, value)


def _attr(typ: str, **kw) -> Attribute:
    return Attribute.model_validate(
        {"name": "x", "type": typ, "sources": [{"source": "s", "column": "c"}], **kw}
    )


CAST_CASES: list[tuple[str, str, list]] = [
    ("VARCHAR", "string", ["abc", " x ", "", "12"]),
    ("VARCHAR", "int", ["12", " 7 ", "-3", "1.0", "1.5", "abc", "1e3", "", "9999999999"]),
    ("VARCHAR", "decimal", ["12.34", " 0.00005 ", "1.23455", "-1.23455", "abc", "1e2", "NaN"]),
    ("VARCHAR", "date", ["2020-02-29", "2021-02-29", " 1999-12-31 ", "31/12/1999", "abc"]),
    (
        "VARCHAR",
        "timestamp",
        [
            "2020-01-02T03:04:05.123456",
            "2020-01-02 03:04:05",
            "2020-01-02T03:04:05+02:00",
            "2020-01-02",
            "abc",
        ],
    ),
    ("VARCHAR", "bool", ["true", "FALSE", " 1 ", "0", "yes", "", "True"]),
    ("BIGINT", "string", [1, -5, 0]),
    ("BIGINT", "int", [1, -5, 0, 2**40]),
    ("BIGINT", "decimal", [1, -5, 12345]),
    ("BIGINT", "bool", [1, 0, 2]),
    ("BIGINT", "date", [1]),
    ("DECIMAL(18,4)", "int", [Decimal("5.0000"), Decimal("5.5000")]),
    ("DECIMAL(18,4)", "decimal", [Decimal("5.1234"), Decimal("-0.0001")]),
    ("DECIMAL(18,4)", "string", [Decimal("5.1234")]),
    ("DATE", "date", [date(2020, 1, 31)]),
    ("DATE", "timestamp", [date(2020, 1, 31)]),
    ("DATE", "string", [date(2020, 1, 31)]),
    ("TIMESTAMP", "timestamp", [datetime(2020, 1, 2, 3, 4, 5, 123456)]),
    ("TIMESTAMP", "date", [datetime(2020, 1, 2, 23, 59, 59)]),
    ("BOOLEAN", "bool", [True, False]),
    ("BOOLEAN", "string", [True]),
    ("BOOLEAN", "int", [True]),
]


@pytest.mark.parametrize("duck_type,target,values", CAST_CASES)
def test_casts_match_reference(con, duck_type, target, values):
    cls = raw_class(duck_type)
    expr, can_fail = cast_sql("v", cls, target)
    attr = _attr(target)
    for raw in values:
        want, failures = process_attribute(attr, raw)
        failed = any(f.rule == "cast" for f in failures)
        got = scalar(con, f"{expr} FROM (SELECT CAST(? AS {duck_type}) AS v)", raw)
        if target == "string" and got is not None:
            got = got or None  # la normalisation (vide -> NULL) est testée à part
            want = want or None
        assert (got is None) == (want is None) or failed, (duck_type, target, raw, got, want)
        if failed:
            assert can_fail and got is None, (duck_type, target, raw, got)
        elif want is not None:
            assert got == want, (duck_type, target, raw, got, want)


CHECK_VALUES = {
    "string": [
        "abc",
        "a@b.fr",
        "bad@",
        "FR-1234",
        "fr-1234",
        "x" * 12,
        "x" * 13,
        "é" * 12,
        "日本語",
    ],
    "int": [-1, 0, 5, 10, 11],
    "decimal": [
        Decimal("-0.0001"),
        Decimal("0"),
        Decimal("0.5"),
        Decimal("10"),
        Decimal("10.0001"),
    ],
}


@pytest.mark.parametrize(
    "typ,check",
    [
        ("string", {"rule": "regex", "value": r"^[^@\s]+@[^@\s]+\.[^@\s]+$"}),
        ("string", {"rule": "regex", "value": r"[0-9]{4}"}),
        ("string", {"rule": "regex", "value": r"^[A-Z]{2}-[0-9]{4}$"}),
        ("string", {"rule": "in_set", "value": ["abc", "FR-1234", "日本語"]}),
        ("string", {"rule": "max_length", "value": 12}),
        ("int", {"rule": "min", "value": 0}),
        ("int", {"rule": "max", "value": 10}),
        ("int", {"rule": "in_set", "value": ["5", "10"]}),
        ("decimal", {"rule": "min", "value": 0}),
        ("decimal", {"rule": "max", "value": 10}),
        ("decimal", {"rule": "min", "value": 0.5}),
        ("decimal", {"rule": "in_set", "value": ["0.5000", "10.0000"]}),
    ],
)
def test_checks_match_reference(con, typ, check):
    attr = _attr(typ, checks=[{**check, "on_fail": "warn"}])
    sql_type = {"string": "VARCHAR", "int": "BIGINT", "decimal": "DECIMAL(18,4)"}[typ]
    expr = check_pass_sql(attr, attr.checks[0], "v")
    for value in CHECK_VALUES[typ]:
        _, failures = process_attribute(attr, value)
        want_pass = not failures
        got = scalar(con, f"{expr} FROM (SELECT CAST(? AS {sql_type}) AS v)", value)
        assert got is want_pass, (check, value, got, want_pass)


def _mini_config() -> ReferentialConfig:
    sources = [
        {"id": "a", "kind": "parquet", "location": "/x/*.parquet", "key": "k", "revision": "r"}
    ]

    def attr(name: str, typ: str, **kw) -> dict:
        return {
            "name": name,
            "type": typ,
            "sources": [{"source": "a", "column": name.upper()}],
            **kw,
        }

    return ReferentialConfig.model_validate(
        {
            "referential": "t",
            "version": 1,
            "state": {"dsn_env": "X", "schema": "s"},
            "sources": sources,
            "attributes": [
                attr("s", "string"),
                attr("i", "int"),
                attr("d", "decimal"),
                attr("dt", "date"),
                attr("ts", "timestamp"),
                attr("b", "bool"),
            ],
            "matching": {
                "version": "v",
                "blocking": [{"name": "k", "parts": [{"attr": "s"}]}],
                "comparators": [{"attr": "s", "method": "exact", "weight": 1.0}],
            },
        }
    )


def test_row_hash_matches_canonical(con):
    cfg = _mini_config()
    expr = row_hash_sql(cfg, "a", lambda a: f"t.{q(a.name)}")
    rng = random.Random(5)
    words = ["", "a", "é", "日本", "x\x1fy", "x\x1ey", "Σ", "0", "ligne\nbreak", "z" * 40]
    for _ in range(300):
        values = {
            "s": rng.choice([*words, None]),
            "i": rng.choice([None, 0, -7, 2**40]),
            "d": rng.choice([None, Decimal("0"), Decimal("-0.5"), Decimal("12345.6789")]),
            "dt": rng.choice([None, date(2020, 2, 29), date(1999, 1, 1)]),
            "ts": rng.choice([None, datetime(2020, 1, 2, 3, 4, 5, 6), datetime(2020, 1, 1)]),
            "b": rng.choice([None, True, False]),
        }
        con.execute(
            "CREATE OR REPLACE TEMP TABLE t AS SELECT CAST(? AS VARCHAR) AS s,"
            " CAST(? AS BIGINT) AS i, CAST(? AS DECIMAL(18,4)) AS d, CAST(? AS DATE) AS dt,"
            " CAST(? AS TIMESTAMP) AS ts, CAST(? AS BOOLEAN) AS b",
            list(values.values()),
        )
        assert scalar(con, f"{expr} FROM t") == row_hash(cfg, "a", values), values


def test_levenshtein_and_scores_match_reference(con):
    rng = random.Random(11)
    alphabet = "abcdeé日 "
    for _ in range(200):
        a = "".join(rng.choice(alphabet) for _ in range(rng.randint(1, 9)))
        b = "".join(rng.choice(alphabet) for _ in range(rng.randint(1, 9)))
        got = scalar(
            con,
            comparator_sim_sql("levenshtein", "a", "b")
            + " FROM (SELECT ?::VARCHAR a, ?::VARCHAR b)",
            a,
            b,
        )
        assert got == compare_values("levenshtein", a, b), (a, b)


def test_weighted_score_and_contradiction_match_reference(con):
    cfg = ReferentialConfig.model_validate(
        {
            "referential": "t",
            "version": 1,
            "state": {"dsn_env": "X", "schema": "s"},
            "sources": [{"id": "a", "kind": "parquet", "location": "/x", "key": "k"}],
            "attributes": [
                {"name": "n", "sources": [{"source": "a", "column": "n"}]},
                {"name": "f", "sources": [{"source": "a", "column": "f"}]},
                {"name": "bd", "type": "date", "sources": [{"source": "a", "column": "bd"}]},
            ],
            "matching": {
                "version": "v",
                "blocking": [{"name": "k", "parts": [{"attr": "n"}]}],
                "comparators": [
                    {"attr": "n", "method": "levenshtein", "weight": 0.4},
                    {"attr": "f", "method": "levenshtein", "weight": 0.3},
                    {"attr": "bd", "method": "exact", "weight": 0.2},
                    {"attr": "bd", "method": "date_year", "weight": 0.1},
                ],
                "contradictions": [{"attr": "bd"}],
                "min_evidence": 0.5,
            },
        }
    )
    spec = Spec(cfg)
    active, total, contra = spec.score_sql("a", "b")
    min_ev = spec.min_evidence
    rng = random.Random(3)
    names = ["dupont", "dupond", "martin", None, "durand", "josé", "jose", "zoë", "日本"]
    dates = [date(1980, 5, 1), date(1980, 7, 2), date(1975, 1, 1), None]
    for _ in range(300):
        a = {"n": rng.choice(names), "f": rng.choice(names), "bd": rng.choice(dates)}
        b = {"n": rng.choice(names), "f": rng.choice(names), "bd": rng.choice(dates)}
        con.execute(
            "CREATE OR REPLACE TEMP TABLE p AS SELECT CAST(? AS VARCHAR) n, CAST(? AS VARCHAR) f,"
            " CAST(? AS DATE) bd, CAST(? AS VARCHAR) n2, CAST(? AS VARCHAR) f2,"
            " CAST(? AS DATE) bd2",
            [a["n"], a["f"], a["bd"], b["n"], b["f"], b["bd"]],
        )
        sql = (
            f"WITH s AS (SELECT ({active}) AS w, ({total}) AS t, ({contra}) AS c FROM"
            " (SELECT n, f, bd FROM p) a, (SELECT n2 AS n, f2 AS f, bd2 AS bd FROM p) b)"
            f" SELECT CASE WHEN w = 0.0 OR w < {min_ev} THEN NULL ELSE t / w END, c FROM s"
        )
        score, contradiction = con.execute(sql).fetchone()
        want_score, want_contra = score_pair(cfg.matching, a, b)
        assert contradiction == want_contra
        if want_score is None:
            assert score is None
        else:
            assert score == pytest.approx(want_score, abs=1e-12)
