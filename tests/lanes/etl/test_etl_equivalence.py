"""Equivalence of the ETL lane with the reference engine (modulo ids), day0 -> day1 -> day2."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
import yaml
from etl_support import HEAVY_DELTA, Lab, make_dataset

from engine.core.canonical import check_tables
from engine.core.synth import DeltaParams


def patch_config(ds: Path, edit: Callable[[dict[str, Any]], None]) -> None:
    path = ds / "config.yml"
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    edit(raw)
    path.write_text(yaml.safe_dump(raw, sort_keys=False, allow_unicode=True), encoding="utf-8")


def tiny_blocks(raw: dict[str, Any]) -> None:
    raw["matching"]["max_block_size"] = 3


def no_provenance(raw: dict[str, Any]) -> None:
    raw["publication"] = {"provenance": False}


def small_batches(raw: dict[str, Any]) -> None:
    raw["runtime"]["batch_rows"] = 1000  # several batches per source


def strict_ratio(raw: dict[str, Any]) -> None:
    for source in raw["sources"]:
        source["full_snapshot_min_ratio"] = 0.97


CASES: dict[str, dict[str, Any]] = {
    "small": {},
    "large_multi_batch": {"nodes": 4000, "attributes": 30, "seed": 31, "patch": small_batches},
    "attrs50": {"nodes": 1200, "attributes": 50, "seed": 9},
    "tiny_blocks": {"nodes": 600, "attributes": 14, "seed": 3, "patch": tiny_blocks},
    "parquet_no_provenance": {
        "nodes": 500,
        "attributes": 16,
        "seed": 21,
        "kinds": ("parquet", "parquet", "parquet"),
        "snapshots": ("full", "delta", "delta"),
        "patch": no_provenance,
    },
    "completeness_gate": {
        "nodes": 500,
        "attributes": 12,
        "seed": 4,
        "patch": strict_ratio,
        "delta": DeltaParams(update_pct=5, new_pct=2, delete_pct=6, broken_pct=1),
    },
}


@pytest.mark.parametrize("case", sorted(CASES))
def test_equivalence_three_days(
    case: str, tmp_path: Path, pg_db: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    spec = dict(CASES[case])
    patch = spec.pop("patch", None)
    delta = spec.pop("delta", HEAVY_DELTA)
    ds = make_dataset(tmp_path, days=2, delta=delta, **spec)
    if patch:
        patch_config(ds, patch)
    lab = Lab(ds, pg_db, monkeypatch, tmp_path / "out")
    lab.lane("init")
    for day in range(3):
        lab.lane_day(day)
        lab.ref_day(day)
        report = lab.compare()
        assert report.ok, f"{case} day {day}: {report.format()}"
    tables = lab.export("final")
    assert check_tables(lab.cfg, tables) == []
    assert tables["golden_delta"].num_rows > 0
    if case == "small":
        events = set(tables["staging_event"].column("kind").to_pylist())
        assert {"create", "merge", "split"} <= events
        assert tables["reject"].num_rows > 0
    if case == "tiny_blocks":
        assert "oversized_block" in set(tables["anomaly"].column("kind").to_pylist())
    if case == "completeness_gate":
        assert "batch_blocked" in set(tables["anomaly"].column("kind").to_pylist())
