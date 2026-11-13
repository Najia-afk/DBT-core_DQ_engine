"""Recette n°5 : injection de pannes (kill pendant reconcile/ingest). L'état publié précédent reste
intact et un nouveau lancement converge vers le résultat de la référence (PostgreSQL requis)."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from pathlib import Path

import psycopg
import pytest
from elt_helpers import DELTA_MIX, Harness, make_synth, write_config
from test_elt_operations import STATE_TABLES, fingerprint, run_cli

from engine.core.canonical import read_parquet_dir
from engine.core.compare import compare_exports
from engine.lanes.elt.dbtrun import kill_tree


def work_tables(dsn: str, schema: str) -> list[str]:
    with psycopg.connect(dsn) as conn:
        rows = conn.execute(
            "SELECT table_name FROM information_schema.tables"
            " WHERE table_schema = %s AND table_name LIKE '\\_w%%'",
            [schema],
        ).fetchall()
    return [r[0] for r in rows]


def pending(dsn: str, schema: str) -> int:
    with psycopg.connect(dsn) as conn:
        return conn.execute(f'SELECT count(*) FROM "{schema}".pending_delta').fetchone()[0]


@pytest.mark.parametrize(
    "point", ["reconcile_after_dbt", "reconcile_mid_apply", "reconcile_before_commit"]
)
async def test_kill_during_reconcile_keeps_previous_publication(point, elt_env, pg_dsn, work_dir):
    cfg, dataset = make_synth(work_dir, nodes=300, attributes=20, seed=12, days=1, delta=DELTA_MIX)
    h = Harness(cfg, dataset, pg_dsn, work_dir)
    await h.run_day(0)
    await h.ingest_day(1)  # delta ingéré, reconcile en attente
    schema = cfg.state.schema_name
    config = write_config(cfg, work_dir / "config.yml")
    before = fingerprint(pg_dsn, schema, STATE_TABLES + ["run_log"])
    todo = pending(pg_dsn, schema)
    assert todo > 0

    crashed = run_cli(
        ["reconcile", "--config", str(config), "--run-id", "day1"], env={"MDM_ELT_FAULT": point}
    )
    assert crashed.returncode != 0 and crashed.stdout.strip() == ""  # mort brutale, pas de JSON
    # état publié strictement identique ; le delta reste en attente ; reste des tables de travail
    assert fingerprint(pg_dsn, schema, STATE_TABLES + ["run_log"]) == before
    assert pending(pg_dsn, schema) == todo
    assert work_tables(pg_dsn, schema)

    # relance : converge vers le résultat de la référence, tables de travail nettoyées
    done = run_cli(["reconcile", "--config", str(config), "--run-id", "day1"])
    summary = json.loads(done.stdout.strip().splitlines()[-1])
    assert done.returncode == 0 and summary["status"] == "ok", done.stdout + done.stderr
    want = h.ref.reconcile("day1")
    assert summary["counters"]["delta_nodes"] == want.delta_nodes
    assert summary["counters"]["golden_changes"] == want.golden_changes
    h.assert_equivalent()
    assert pending(pg_dsn, schema) == 0
    assert not work_tables(pg_dsn, schema)


async def test_kill_dbt_process_tree_during_reconcile(elt_env, pg_dsn, work_dir):
    cfg, dataset = make_synth(work_dir, nodes=300, attributes=20, seed=13, days=1, delta=DELTA_MIX)
    h = Harness(cfg, dataset, pg_dsn, work_dir)
    await h.run_day(0)
    await h.ingest_day(1)
    schema = cfg.state.schema_name
    config = write_config(cfg, work_dir / "config.yml")
    before = fingerprint(pg_dsn, schema, STATE_TABLES + ["run_log"])

    proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "engine.lanes.elt",
            "reconcile",
            "--config",
            str(config),
            "--run-id",
            "day1",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
    )
    base = [p for p in Path(os.environ["MDM_ELT_WORKDIR"]).iterdir() if p.is_dir()][0]
    log = base / "runs" / "day1" / "reconcile" / "dbt" / "dbt.out.log"
    deadline = time.time() + 120
    while time.time() < deadline and not (log.exists() and "Concurrency" in log.read_text("utf-8")):
        assert proc.poll() is None, "reconcile ended before the kill"
        time.sleep(0.1)
    kill_tree(proc.pid)  # tue le CLI et le dbt (arbre complet), comme un kill -9 de l'hôte
    proc.wait(timeout=30)
    assert proc.returncode != 0
    assert fingerprint(pg_dsn, schema, STATE_TABLES + ["run_log"]) == before

    done = run_cli(["reconcile", "--config", str(config), "--run-id", "day1"])
    assert done.returncode == 0, done.stdout + done.stderr
    h.ref.reconcile("day1")
    h.assert_equivalent()


async def test_kill_during_ingest_merge_leaves_state_untouched(elt_env, pg_dsn, work_dir):
    cfg, dataset = make_synth(work_dir, nodes=300, attributes=20, seed=14, days=1, delta=DELTA_MIX)
    h = Harness(cfg, dataset, pg_dsn, work_dir)
    await h.run_day(0)
    h.stage(1)
    schema = cfg.state.schema_name
    config = write_config(cfg, work_dir / "config.yml")
    before = fingerprint(pg_dsn, schema, STATE_TABLES + ["run_log"])
    for point in ("ingest_after_dbt", "ingest_before_commit"):
        crashed = run_cli(
            ["ingest", "--config", str(config), "--source", "crm", "--run-id", "day1"],
            env={"MDM_ELT_FAULT": point},
        )
        assert crashed.returncode != 0
        assert fingerprint(pg_dsn, schema, STATE_TABLES + ["run_log"]) == before
        assert "elt_test_only" not in crashed.stdout + crashed.stderr
    # aucun secret dans les journaux dbt conservés après la panne
    for path in Path(os.environ["MDM_ELT_WORKDIR"]).rglob("*"):
        if path.is_file() and path.suffix in {".log", ".yml", ".sql", ".json"}:
            assert "elt_test_only" not in path.read_text("utf-8", errors="ignore"), path
    # relance (ingest + reconcile) : converge vers la référence
    for sid in [s.id for s in cfg.sources]:
        assert (
            run_cli(
                ["ingest", "--config", str(config), "--source", sid, "--run-id", "day1"]
            ).returncode
            == 0
        )
    assert run_cli(["reconcile", "--config", str(config), "--run-id", "day1"]).returncode == 0
    from elt_helpers import source_rows

    for s in cfg.sources:
        h.ref.ingest(s.id, source_rows(cfg, dataset, 1, s.id), "day1")
    h.ref.reconcile("day1")
    out = work_dir / "final"
    h.lane.export(out)
    report = compare_exports(h.ref.export(), read_parquet_dir(out), compare_hash=True)
    assert report.ok, report.format()
