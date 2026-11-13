"""Vectorised scoring / blocking / survivorship == reference functions (no Docker needed)."""

from __future__ import annotations

import copy
import random
from datetime import date, datetime, timedelta

import numpy as np
import pyarrow as pa
import pytest
from graph_data_base import BASE

from engine.core.canonical import arrow_type
from engine.core.config import ReferentialConfig
from engine.core.reference.matching import block_label, block_values, score_pair
from engine.core.reference.model import Record
from engine.core.reference.survive import survive_attribute
from engine.lanes.graph.matching import block_identities, label_of, score_pairs
from engine.lanes.graph.survive import survive

RNG = random.Random(11)
NAMES = [
    "dupont",
    "dupond",
    "durand",
    "éloïse",
    "eloise",
    "日本語",
    "x",
    "ab",
    "abc",
    "smith",
    "smyth",
    "o'neil",
    "straße",
    "strasse",
    "a" * 30,
    "a" * 29 + "b",
]


def config(**matching) -> ReferentialConfig:
    raw = copy.deepcopy(BASE)
    raw["matching"]["comparators"] = [
        {"attr": "last_name", "method": "levenshtein", "weight": 0.3},
        {"attr": "first_name", "method": "levenshtein", "weight": 0.25},
        {"attr": "birth_date", "method": "exact", "weight": 0.2},
        {"attr": "birth_date", "method": "date_year", "weight": 0.1},
        {"attr": "email", "method": "exact", "weight": 0.15},
    ]
    raw["matching"].update(matching)
    return ReferentialConfig.model_validate(raw)


def random_values() -> dict:
    def maybe(value, p=0.2):
        return None if RNG.random() < p else value

    return {
        "last_name": maybe(RNG.choice(NAMES), 0.05),
        "first_name": maybe(RNG.choice(NAMES)),
        "birth_date": maybe(
            date(1980 + RNG.randrange(3), 1 + RNG.randrange(2), 1 + RNG.randrange(2))
        ),
        "email": maybe(RNG.choice(["a@x.fr", "b@x.fr", "c@y.fr"]), 0.5),
    }


@pytest.mark.parametrize("min_evidence", [0.5, 0.1, 0.9])
def test_pair_scores_decisions_inputs_match_the_reference(min_evidence) -> None:
    cfg = config(min_evidence=min_evidence)
    m = cfg.matching
    n = 600
    left = [random_values() for _ in range(n)]
    right = [dict(v) if RNG.random() < 0.3 else random_values() for v in left]
    right = [random_values() if RNG.random() < 0.5 else r for r in right]

    def columns(rows):
        return {
            a.name: pa.array([r.get(a.name) for r in rows], type=arrow_type(a.type))
            for a in cfg.attributes
        }

    score, has, contra, elig = score_pairs(m, columns(left), columns(right), n)
    for i in range(n):
        want_score, want_contra = score_pair(m, left[i], right[i])
        assert bool(contra[i]) == want_contra
        assert bool(has[i]) == (want_score is not None)
        if want_score is not None:
            assert score[i] == want_score  # same float operations, same order
            assert bool(elig[i]) == (want_score >= m.accept - 1e-9 and not want_contra)


def test_block_identities_match_the_reference_labels() -> None:
    cfg = config()
    rows = [random_values() for _ in range(300)]
    table = pa.table(
        {
            a.name: pa.array([r.get(a.name) for r in rows], type=arrow_type(a.type))
            for a in cfg.attributes
        }
    )
    got = block_identities(cfg, table)
    for row, idents in zip(rows, got, strict=True):
        want = {block_label(cfg, name, value) for name, value in block_values(cfg, row).items()}
        assert {label_of(i) for i in idents} == want
    assert len({i for idents in got for i in idents}) == len(
        {(i) for idents in got for i in idents}
    )


def test_block_identity_is_unambiguous_when_values_contain_the_separator() -> None:
    cfg = config()
    rows = [
        {"last_name": "a|b", "birth_date": date(2000, 1, 1)},
        {"last_name": "a", "birth_date": date(2000, 1, 1)},
    ]
    table = pa.table(
        {
            a.name: pa.array([r.get(a.name) for r in rows], type=arrow_type(a.type))
            for a in cfg.attributes
        }
    )
    first, second = block_identities(cfg, table)
    assert first != second and len(first) == 1


@pytest.mark.parametrize("seed", range(3))
def test_survivorship_matches_the_reference(seed) -> None:
    rng = random.Random(seed)
    cfg = config()
    sources = ["crm", "erp", "web"]
    stagings = 60
    members: list[Record] = []
    ordinal: list[int] = []
    for s in range(stagings):
        for k in range(rng.randrange(1, 6)):
            sid = rng.choice(sources)
            rev = rng.choice([None, datetime(2024, 1, 1) + timedelta(days=rng.randrange(4))])
            values = {
                a.name: (None if rng.random() < 0.3 else rng.choice(["a", "b", "c"]))
                if a.type == "string"
                else (None if rng.random() < 0.3 else date(2000, 1, 1 + rng.randrange(3)))
                if a.type == "date"
                else None
                for a in cfg.attributes
            }
            members.append(
                Record(sid, f"k{s}-{k}-{rng.randrange(3)}", "h", rev, "active", "r", "r", values)
            )
            ordinal.append(s)
    table = pa.table(
        {
            "_source_id": [m.source_id for m in members],
            "_source_key": [m.source_key for m in members],
            "_revision": pa.array([m.revision for m in members], type=pa.timestamp("us")),
            **{
                a.name: pa.array([m.values[a.name] for m in members], type=arrow_type(a.type))
                for a in cfg.attributes
            },
        }
    )
    values, srcs = survive(cfg, table, np.array(ordinal, dtype=np.int64), stagings)
    for s in range(stagings):
        group = [m for m, o in zip(members, ordinal, strict=True) if o == s]
        for a in cfg.attributes:
            want, want_src = survive_attribute(a, group)
            assert values[a.name][s].as_py() == want, (s, a.name)
            assert srcs[a.name][s].as_py() == want_src, (s, a.name)
