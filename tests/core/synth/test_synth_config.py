from __future__ import annotations

import pytest

from engine.core.config import load_config
from engine.core.synth import SynthParams, build_plan, write_config
from engine.core.synth.plan import ALIASES

PARAM_SETS = [
    {"attributes": 12, "sources": 3},
    {"attributes": 50, "sources": 3, "kinds": ("postgres", "parquet", "postgres")},
    {"attributes": 200, "sources": 3, "snapshots": ("full", "delta", "full")},
    {"attributes": 20, "sources": 1},
    {"attributes": 40, "sources": 5, "kinds": ("parquet",), "seed": 9},
    {
        "attributes": 30,
        "sources": 4,
        "type_mix": {"string": 1.0, "int": 1.0},
        "nullable_share": 0.0,
        "check_share": 1.0,
        "most_recent_share": 0.5,
    },
]


@pytest.mark.parametrize("raw", PARAM_SETS)
def test_config_is_valid_and_loadable(tmp_path, raw):
    params = SynthParams(nodes=100, workers=1, **raw)
    cfg = write_config(params, tmp_path / "c.yml", data_root=tmp_path)
    assert load_config(tmp_path / "c.yml") == cfg
    assert len(cfg.attributes) == params.attributes
    assert len(cfg.sources) == params.sources
    assert cfg.state.dsn_env == "MDM_STATE_DSN"
    for i, s in enumerate(cfg.sources):
        assert s.kind == params.kind_of(i)
        assert s.snapshot == params.snapshot_of(i)
        assert (s.dsn_env is not None) == (s.kind == "postgres")
        if s.dsn_env:
            assert s.dsn_env.startswith("MDM_SRC_") and s.dsn_env.endswith("_DSN")


def test_mapping_uses_other_names_than_targets(tmp_path):
    params = SynthParams(nodes=100, attributes=50, sources=3, workers=1)
    cfg = write_config(params, tmp_path / "c.yml")
    for s in cfg.sources:
        mapping = cfg.mapping(s.id)
        assert mapping, s.id
        assert all(col != name for name, col in mapping.items())
        assert len(set(mapping.values())) == len(mapping)
        assert s.key not in mapping.values() and s.revision not in mapping.values()
        assert s.key not in mapping and s.revision not in mapping
    assert cfg.mapping("crm")["last_name"] == ALIASES["last_name"][0]
    assert cfg.mapping("erp")["last_name"] == ALIASES["last_name"][1]
    keys = {s.key for s in cfg.sources}
    assert len(keys) == 3


def test_source_priority_orders_differ_between_attributes(tmp_path):
    cfg = write_config(
        SynthParams(nodes=100, attributes=50, sources=3, workers=1), tmp_path / "c.yml"
    )
    orders = {tuple(sc.source for sc in a.sources) for a in cfg.attributes}
    assert len(orders) >= 4
    for name in ("last_name", "first_name", "birth_date", "email"):
        assert {sc.source for sc in cfg.attribute(name).sources} == {"crm", "erp", "web"}


def test_type_mix_checks_and_matching(tmp_path):
    params = SynthParams(nodes=100, attributes=200, sources=3, workers=1)
    cfg = write_config(params, tmp_path / "c.yml")
    assert {a.type for a in cfg.attributes} == {
        "string", "int", "decimal", "date", "timestamp", "bool"
    }  # fmt: skip
    rules = {c.rule for a in cfg.attributes for c in a.checks}
    assert rules == {"regex", "in_set", "min", "max", "max_length"}
    fails = {c.on_fail for a in cfg.attributes for c in a.checks}
    assert fails == {"reject_record", "null_value", "warn"}
    assert any(not a.nullable for a in cfg.attributes) and any(a.nullable for a in cfg.attributes)
    assert any(a.strategy == "most_recent" for a in cfg.attributes)
    m = cfg.matching
    assert 3 <= len(m.comparators) <= 5
    assert {c.method for c in m.comparators} == {"exact", "levenshtein", "date_year"}
    assert len(m.blocking) == 2
    assert [c.attr for c in m.contradictions] == ["birth_date"]
    assert cfg.attribute("birth_date").type == "date"


def test_plan_is_deterministic_and_seed_sensitive():
    a = build_plan(SynthParams(nodes=10, attributes=60, seed=3)).config
    b = build_plan(SynthParams(nodes=10, attributes=60, seed=3)).config
    c = build_plan(SynthParams(nodes=10, attributes=60, seed=4)).config
    assert a == b
    assert a != c


@pytest.mark.parametrize(
    "raw",
    [
        {"attributes": 5},
        {"sources": 0},
        {"kinds": ("mysql",)},
        {"snapshots": ("full", "full")},
        {"overlap": 0.0},
        {"null_rate": 1.5},
    ],
)
def test_invalid_params_are_rejected(raw):
    with pytest.raises(ValueError):
        SynthParams(nodes=10, **raw)
