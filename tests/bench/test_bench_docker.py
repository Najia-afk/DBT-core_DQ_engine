"""Intégration Docker : un run ETL de 2 000 nœuds de bout en bout via le banc."""

from __future__ import annotations

import json
import os
import shutil
from pathlib import Path

import pytest

from engine.bench import report
from engine.bench.dataset import DatasetSpec
from engine.bench.guards import Limits
from engine.bench.pg import PgContainer, PgTuning, stray_bench_containers, unique_tag
from engine.bench.scenario import RunSpec, run_lane_bench

pytestmark = pytest.mark.skipif(
    not os.environ.get("MDM_TEST_DOCKER") or shutil.which("docker") is None,
    reason="MDM_TEST_DOCKER non défini (ou docker absent)",
)

TINY_PG = PgTuning(
    memory="2g", shm_size="256m", shared_buffers="256MB", work_mem="16MB",
    maintenance_work_mem="128MB", effective_cache_size="1GB", max_wal_size="2GB",
    min_wal_size="256MB",
)  # fmt: skip


def test_etl_2k_nodes_end_to_end_through_the_harness(tmp_path: Path) -> None:
    tag = unique_tag()
    spec = RunSpec(
        lane="etl",
        dataset=DatasetSpec(
            nodes=2000,
            attributes=20,
            seed=3,
            workers=2,
            deltas=({"update_pct": 5.0, "new_pct": 1.0, "delete_pct": 0.5, "broken_pct": 0.2},),
        ),  # fmt: skip
        name="test-etl-2k",
        root=tmp_path / "root",
        out_dir=tmp_path / "out",
        tuning=TINY_PG,
        limits=Limits(min_free_disk_gb=1.0, max_step_seconds=600, max_total_ram_gb=28.0),
        export="final",
        tag=tag,
    )
    result = run_lane_bench(spec)

    assert result["status"] == "ok", result.get("abort_reason") or result["phases"][-1]
    phases = {(p["day"], p["phase"]): p for p in result["phases"]}
    assert set(phases) == {
        (0, "init"), (0, "ingest"), (0, "reconcile"),
        (1, "ingest"), (1, "reconcile"), (1, "export"),
    }  # fmt: skip
    ingest0 = phases[(0, "ingest")]
    assert ingest0["rows"] > 2000 and ingest0["rows_per_s"] > 0 and ingest0["rss_peak_mb"] > 10
    assert phases[(0, "reconcile")]["delta_nodes"] and phases[(0, "reconcile")]["wall_s"] > 0
    assert phases[(0, "reconcile")]["pg_db_mb"] > 5 and phases[(0, "reconcile")]["wal_mb"] > 0
    assert phases[(1, "export")]["export_mb"] > 0
    assert result["config"]["hash"] and result["machine"]["cpu_threads"]
    assert result["totals"]["total_peak_mb"] > 0
    assert any("ingest" in c for c in result["commands"])

    written = json.loads((spec.out_dir / "test-etl-2k.json").read_text(encoding="utf-8"))
    assert written["name"] == "test-etl-2k"
    results_md = report.write_results(spec.out_dir)
    assert "test-etl-2k" not in results_md.read_text(encoding="utf-8")  # préfixe test : ignoré

    # le conteneur et les données du run sont supprimés
    assert PgContainer("etl", tag=tag).name not in stray_bench_containers()
    assert not (spec.root / "run-test-etl-2k").exists()
