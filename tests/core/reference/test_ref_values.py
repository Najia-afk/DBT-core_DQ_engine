from datetime import date, datetime
from decimal import Decimal

import pytest
from rapidfuzz.distance import Levenshtein

from engine.core.config import Attribute
from engine.core.reference.matching import compare_values, levenshtein_distance
from engine.core.reference.values import CastError, cast_value, normalize_string, process_attribute


def attr(**kw):
    base = {"name": "x", "sources": [{"source": "s", "column": "c"}]}
    return Attribute.model_validate({**base, **kw})


@pytest.mark.parametrize(
    ("normalizers", "raw", "expected"),
    [
        (["trim"], "  a b \t", "a b"),
        (["lower"], "ÀBc", "àbc"),
        (["upper"], "àbc", "ÀBC"),
        (["strip_accents"], "Éloïse Çà", "Eloise Ca"),
        (["collapse_spaces"], " a \t\n b  ", " a b "),
        (["digits_only"], "+33 (0)1-23.45 ٣", "33012345"),
        (["alnum_only"], "A-1_b é!", "A1b"),
        (["trim", "lower", "strip_accents", "collapse_spaces"], "  ÉLOÏSE   Dû ", "eloise du"),
        (["upper", "alnum_only"], "x-y 9", "XY9"),
    ],
)
def test_normalizers(normalizers, raw, expected):
    assert normalize_string(raw, normalizers) == expected


def test_normalizers_apply_in_declared_order():
    assert normalize_string(" A ", ["lower", "trim"]) == "a"
    assert normalize_string("a-b", ["alnum_only", "upper"]) == "AB"


@pytest.mark.parametrize(
    ("attr_type", "raw", "expected"),
    [
        ("string", 12, "12"),
        ("int", "42", 42),
        ("int", 7.0, 7),
        ("decimal", "1.23456", Decimal("1.2346")),
        ("decimal", 2, Decimal("2.0000")),
        ("decimal", 0.1, Decimal("0.1000")),
        ("date", "2020-02-29", date(2020, 2, 29)),
        ("date", datetime(2020, 2, 29, 13), date(2020, 2, 29)),
        ("timestamp", "2020-02-29T13:00:00+02:00", datetime(2020, 2, 29, 11)),
        ("timestamp", date(2020, 1, 2), datetime(2020, 1, 2)),
        ("bool", "TRUE", True),
        ("bool", 0, False),
    ],
)
def test_casts(attr_type, raw, expected):
    assert cast_value(raw, attr_type) == expected


@pytest.mark.parametrize(
    ("attr_type", "raw"),
    [
        ("int", "4.5"),
        ("int", True),
        ("int", "abc"),
        ("decimal", "nan"),
        ("decimal", "1e20"),
        ("date", "31/12/2020"),
        ("timestamp", 12),
        ("bool", "maybe"),
        ("string", True),
    ],
)
def test_cast_errors(attr_type, raw):
    with pytest.raises(CastError):
        cast_value(raw, attr_type)


def test_checks_pass_and_fail_by_rule():
    regex = attr(checks=[{"rule": "regex", "value": "^a+$", "on_fail": "warn"}])
    assert process_attribute(regex, "aaa") == ("aaa", [])
    value, failures = process_attribute(regex, "ab")
    assert value == "ab" and [(f.rule, f.action) for f in failures] == [("regex", "warn")]

    in_set = attr(type="int", checks=[{"rule": "in_set", "value": ["1", "2"]}])
    assert process_attribute(in_set, 2)[1] == []
    assert process_attribute(in_set, 3)[1][0].rule == "in_set"

    bounded = attr(
        type="decimal", checks=[{"rule": "min", "value": 0}, {"rule": "max", "value": 10}]
    )
    assert process_attribute(bounded, "0")[1] == []
    assert process_attribute(bounded, "10")[1] == []
    assert [f.rule for f in process_attribute(bounded, "-0.0001")[1]] == ["min"]
    assert [f.rule for f in process_attribute(bounded, "10.0001")[1]] == ["max"]

    short = attr(checks=[{"rule": "max_length", "value": 3}])
    assert process_attribute(short, "abc")[1] == []
    assert process_attribute(short, "abcd")[1][0].rule == "max_length"


def test_null_value_stops_later_checks_and_null_is_never_checked():
    a = attr(
        checks=[
            {"rule": "max_length", "value": 2, "on_fail": "null_value"},
            {"rule": "regex", "value": "^z", "on_fail": "reject_record"},
        ]
    )
    value, failures = process_attribute(a, "abc")
    assert value is None and [f.rule for f in failures] == ["max_length"]
    assert process_attribute(a, None) == (None, [])
    assert process_attribute(attr(normalize=["trim"]), "   ") == (None, [])


def test_cast_failure_is_reject_record():
    value, failures = process_attribute(attr(type="int"), "x")
    assert value is None and [(f.rule, f.action) for f in failures] == [("cast", "reject_record")]


def test_levenshtein_matches_rapidfuzz_and_known_values():
    assert levenshtein_distance("kitten", "sitting") == 3
    assert levenshtein_distance("", "abc") == 3
    pairs = [("dupont", "dupond"), ("martin", "marten"), ("a", "b"), ("été", "ete"), ("abc", "")]
    for a, b in pairs:
        assert levenshtein_distance(a, b) == Levenshtein.distance(a, b)
    assert compare_values("levenshtein", "dupont", "dupond") == pytest.approx(1 - 1 / 6)
    assert compare_values("levenshtein", "ab", "abcd") == pytest.approx(0.5)


def test_comparators_exact_and_date_year():
    assert compare_values("exact", "a", "a") == 1.0
    assert compare_values("exact", Decimal("1.5"), Decimal("1.50")) == 1.0
    assert compare_values("exact", 1, 2) == 0.0
    assert compare_values("date_year", date(2020, 1, 1), date(2020, 12, 31)) == 1.0
    assert compare_values("date_year", datetime(2020, 1, 1), datetime(2021, 1, 1)) == 0.0
