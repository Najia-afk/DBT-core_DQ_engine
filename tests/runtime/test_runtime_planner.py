"""Planificateur : init? -> ingest[source] -> reconcile -> export? (sans Docker)."""

from __future__ import annotations

from pathlib import Path

import pytest
from runtime_support import write_config

from engine.core.config import load_config
from engine.runtime.planner import PlanError, build_plan


def _plan(tmp_path: Path, declared=("a", "b", "c"), **params):
    cfg = load_config(write_config(tmp_path, "ref", declared))
    return build_plan(cfg, params, profiles={"small", "medium", "large"})


def test_default_plan_is_ingests_then_single_reconcile(tmp_path: Path) -> None:
    plan = _plan(tmp_path)
    assert [s.step for s in plan] == ["ingest:a", "ingest:b", "ingest:c", "reconcile"]
    assert all(s.depends_on == () for s in plan[:3])
    assert plan[3].depends_on == ("ingest:a", "ingest:b", "ingest:c")
    assert {s.kind for s in plan} == {"ingest", "reconcile"}


def test_init_and_export_are_optional_and_chained(tmp_path: Path) -> None:
    plan = _plan(tmp_path, include_init=True, include_export=True, export_out="out")
    by_name = {s.step: s for s in plan}
    assert [s.step for s in plan][0] == "init" and [s.step for s in plan][-1] == "export"
    assert by_name["ingest:a"].depends_on == ("init",)
    assert by_name["export"].depends_on == ("reconcile",)
    assert len(plan) == 6


def test_source_subset_keeps_declared_order(tmp_path: Path) -> None:
    plan = _plan(tmp_path, **{"sources": ["c", "a"]})
    assert [s.step for s in plan] == ["ingest:a", "ingest:c", "reconcile"]


def test_profiles_timeout_and_attempts(tmp_path: Path) -> None:
    plan = _plan(
        tmp_path, profile_by_kind={"reconcile": "large"}, timeout_seconds=9, max_attempts=2
    )
    assert plan[0].profile == "small" and plan[-1].profile == "large"
    assert {s.timeout_seconds for s in plan} == {9} and {s.max_attempts for s in plan} == {2}


@pytest.mark.parametrize(
    "params",
    [
        {"sources": ["zzz"]},
        {"sources": []},
        {"include_export": True},
        {"profile": "gigantic"},
        {"bogus": 1},
        {"profile_by_kind": {"nope": "small"}},
        {"max_attempts": 0},
    ],
)
def test_invalid_params_are_rejected(tmp_path: Path, params) -> None:
    with pytest.raises(PlanError):
        _plan(tmp_path, **params)
