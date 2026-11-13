"""Recette n°1 : équivalence de la voie ELT avec la référence sur des jeux générés (jour 0, 1, 2)
et propriété incrémental == à froid sur la voie elle-même (PostgreSQL : MDM_TEST_DOCKER=1)."""

from __future__ import annotations

import psycopg
import pytest
from elt_helpers import DELTA_MIX, Harness, make_synth, with_schema

from engine.core.canonical import read_parquet_dir
from engine.core.compare import compare_exports
from engine.core.synth import DeltaParams
from engine.lanes.elt.lane import Lane
from engine.lanes.elt.settings import Settings

SCENARIOS = {
    # ~50 attributs, source postgres complète + deux sources parquet (une en delta)
    "synth50": dict(nodes=500, attributes=50, seed=11, delta=DELTA_MIX),
    # 4 sources, deux postgres (une en delta), deux parquet ; mises à jour/suppressions plus fortes
    "synth20_four_sources": dict(
        nodes=400,
        attributes=20,
        sources=4,
        seed=5,
        kinds=("parquet", "postgres", "postgres", "parquet"),
        snapshots=("full", "full", "delta", "delta"),
        delta=DeltaParams(
            update_pct=15, new_pct=6, delete_pct=5, broken_pct=3, merge_pct=20, split_pct=20
        ),
    ),
}


@pytest.mark.parametrize("name", list(SCENARIOS))
async def test_equivalence_with_reference_and_cold_run(name, elt_env, pg_dsn, work_dir):
    cfg, dataset = make_synth(work_dir, schema="mdm_incremental", **SCENARIOS[name])
    h = Harness(cfg, dataset, pg_dsn, work_dir)
    for day in range(3):
        await h.run_day(day)
        report = h.compare()
        assert report.ok, f"day {day}: {report.format()}"
    golden = h.export()["golden"]
    assert golden.num_rows > 0

    # propriété n°1 sur la voie : l'état incrémental == un reconcile à froid sur les mêmes
    # enregistrements actifs (source_record copié, tout en attente), comme `recompute_from_scratch`
    cold_cfg = with_schema(cfg, "mdm_cold")
    cold = Lane(cold_cfg, Settings.from_env(cold_cfg))
    cold.reset()
    cold.init()
    with psycopg.connect(pg_dsn) as conn:
        conn.execute(
            "INSERT INTO mdm_cold.source_record SELECT * FROM mdm_incremental.source_record"
            " WHERE status = 'active'"
        )
        conn.execute(
            "INSERT INTO mdm_cold.pending_delta (source_id, source_key)"
            " SELECT source_id, source_key FROM mdm_cold.source_record"
        )
        conn.commit()
    result = await cold.reconcile("cold")
    assert result["counters"]["delta_nodes"] > 0
    out = work_dir / "cold_export"
    cold.export(out)
    report = compare_exports(h.export(), read_parquet_dir(out), state_only=True)
    assert report.ok, report.format()


async def test_keep_none_pairs_matches_strict_reference(elt_env, pg_dsn, work_dir, monkeypatch):
    """Avec MDM_ELT_KEEP_NONE_PAIRS=1 les paires `none` sont aussi persistées : égalité stricte."""
    monkeypatch.setenv("MDM_ELT_KEEP_NONE_PAIRS", "1")
    cfg, dataset = make_synth(
        work_dir, nodes=300, attributes=20, seed=2, delta=DELTA_MIX, schema="mdm_none_pairs"
    )
    h = Harness(cfg, dataset, pg_dsn, work_dir)
    for day in range(3):
        await h.run_day(day)
        report = h.compare(include_none_pairs=True)
        assert report.ok, f"day {day}: {report.format()}"
    pairs = h.export()["match_pair"].to_pylist()
    assert any(p["decision"] == "none" for p in pairs)
