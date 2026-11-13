"""Opérations de la voie ELT : idempotence, preuve « delta seulement », ingest parallèle,
contrat CLI (PostgreSQL requis : MDM_TEST_DOCKER=1)."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from typing import Any

import psycopg
from elt_helpers import Harness, make_synth, write_config

from engine.core.synth import DeltaParams, stage_live

STATE_TABLES = [
    "source_record",
    "source_delta",
    "pending_delta",
    "block_index",
    "match_pair",
    "staging",
    "staging_member",
    "staging_event",
    "golden",
    "golden_delta",
    "reject",
    "anomaly",
]


def fingerprint(dsn: str, schema: str, tables: list[str] | None = None) -> dict[str, tuple]:
    """(nombre de lignes, empreinte indépendante de l'ordre) de chaque table d'état."""
    out = {}
    with psycopg.connect(dsn) as conn:
        for table in tables or STATE_TABLES:
            row = conn.execute(
                f"SELECT count(*), coalesce(sum(hashtextextended(t::text, 0)::numeric), 0)"
                f' FROM "{schema}"."{table}" t'
            ).fetchone()
            out[table] = tuple(row)
    return out


def xmins(dsn: str, schema: str, table: str, key: str) -> dict[Any, str]:
    with psycopg.connect(dsn) as conn:
        rows = conn.execute(f'SELECT {key}, xmin::text FROM "{schema}"."{table}"').fetchall()
    return {r[0]: r[1] for r in rows}


def pair_stamps(dsn: str, schema: str) -> dict[Any, str]:
    with psycopg.connect(dsn) as conn:
        rows = conn.execute(
            "SELECT left_source_id || chr(31) || left_key || chr(31) || right_source_id"
            f' || chr(31) || right_key, xmin::text FROM "{schema}".match_pair'
        ).fetchall()
    return {r[0]: r[1] for r in rows}


def changed(before: dict, after: dict) -> int:
    return sum(1 for k, v in after.items() if before.get(k) != v)


async def test_replay_and_unchanged_batches_are_noops(elt_env, pg_dsn, work_dir):
    cfg, dataset = make_synth(work_dir, nodes=300, attributes=20, seed=4, days=0)
    h = Harness(cfg, dataset, pg_dsn, work_dir)
    await h.run_day(0)
    schema = cfg.state.schema_name
    reference = fingerprint(pg_dsn, schema)
    runs = fingerprint(pg_dsn, schema, ["run_log"])

    # même run-id : rien n'est refait (replayed), l'état et le journal sont strictement identiques
    ids = [s.id for s in cfg.sources]
    replay = await h.lane.ingest(ids, "day0")
    assert all(r["replayed"] for r in [await h.lane.ingest([i], "day0") for i in ids])
    assert replay["counters"]
    again = await h.lane.reconcile("day0")
    assert again["replayed"] is True
    assert fingerprint(pg_dsn, schema) == reference
    assert fingerprint(pg_dsn, schema, ["run_log"]) == runs

    # nouveau run-id, mêmes données : aucun delta, aucune réconciliation, aucune écriture
    stamps = {
        t: xmins(pg_dsn, schema, t, "golden_id" if t == "golden" else "source_key")
        for t in ("golden", "source_record")
    }
    result = await h.lane.ingest(ids, "day0-again")
    for sid in ids:
        c = result["counters"][sid]
        assert c["inserted"] == c["updated"] == c["deleted"] == 0 and c["unchanged"] > 0
    empty = await h.lane.reconcile("day0-again")
    assert empty["counters"]["delta_nodes"] == 0 and "dbt" not in empty["seconds"]
    # (les rejets DQ de chaque lot sont journalisés à chaque run : seule la table `reject` change)
    quiet = [t for t in STATE_TABLES if t != "reject"]
    assert fingerprint(pg_dsn, schema, quiet) == {
        k: v for k, v in reference.items() if k != "reject"
    }
    for table, before in stamps.items():
        after = xmins(pg_dsn, schema, table, "golden_id" if table == "golden" else "source_key")
        assert before == after, f"{table} rewritten by a no-op run"
    h.assert_equivalent(
        state_only=True
    )  # rien n'a changé d'état (les événements de rejet diffèrent)


async def test_reconcile_touches_only_the_delta(elt_env, pg_dsn, work_dir):
    cfg, dataset = make_synth(
        work_dir,
        nodes=2000,
        attributes=20,
        seed=8,
        days=1,
        delta=DeltaParams(
            update_pct=1, new_pct=0.5, delete_pct=0.5, broken_pct=0.2, merge_pct=10, split_pct=10
        ),
    )
    h = Harness(cfg, dataset, pg_dsn, work_dir)
    await h.run_day(0)
    schema = cfg.state.schema_name
    gold0 = xmins(pg_dsn, schema, "golden", "golden_id")
    sm0 = xmins(pg_dsn, schema, "staging_member", "source_key")
    st0 = xmins(pg_dsn, schema, "staging", "staging_id")
    pair0 = pair_stamps(pg_dsn, schema)
    with psycopg.connect(pg_dsn) as conn:
        active = conn.execute(f"SELECT count(*) FROM {schema}.source_record").fetchone()[0]

    result = await h.run_day(1)
    c = result["counters"]
    h.assert_equivalent()

    gold1 = xmins(pg_dsn, schema, "golden", "golden_id")
    sm1 = xmins(pg_dsn, schema, "staging_member", "source_key")
    st1 = xmins(pg_dsn, schema, "staging", "staging_id")
    pair1 = pair_stamps(pg_dsn, schema)
    # compteurs == lignes réellement réécrites dans PostgreSQL (xmin change à chaque écriture)
    assert changed(gold0, gold1) == c["written_golden"]
    assert c["written_staging"] <= changed(st0, st1) < 0.3 * len(st1)
    assert changed(sm0, sm1) <= c["written_staging_member"]
    assert changed(pair0, pair1) <= c["written_match_pair"]
    # le travail est proportionnel au delta, pas au référentiel
    total_golden = len(gold1)
    assert c["delta_nodes"] < 0.1 * active
    assert c["read_source_record"] < 0.4 * active
    assert c["read_golden"] < 0.3 * total_golden
    assert changed(gold0, gold1) < 0.3 * total_golden
    assert sum(1 for k in gold0 if gold0[k] == gold1.get(k)) > 0.7 * len(gold0)
    assert sum(1 for k in sm0 if sm0[k] == sm1.get(k)) > 0.7 * len(sm0)
    assert sum(1 for k in pair0 if pair0[k] == pair1.get(k)) > 0.7 * len(pair0)


async def test_parallel_ingest_uses_separate_duckdb_files(elt_env, pg_dsn, work_dir):
    cfg, dataset = make_synth(work_dir, nodes=300, attributes=20, seed=6, days=0)
    h = Harness(cfg, dataset, pg_dsn, work_dir)
    h.stage(0)
    ids = [s.id for s in cfg.sources]
    result = await h.lane.ingest(ids, "p0")
    per_source = result["per_source_seconds"]
    assert set(per_source) == set(ids)
    # les ingests tournent simultanément : le total est inférieur à la somme des durées par source
    assert result["seconds"]["total"] < 0.8 * sum(v["total"] for v in per_source.values())
    assert all(result["counters"][s]["inserted"] > 0 for s in ids)


def run_cli(args: list[str], env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "engine.lanes.elt", *args],
        capture_output=True,
        text=True,
        env={**os.environ, **(env or {}), "PYTHONDONTWRITEBYTECODE": "1"},
        timeout=600,
    )


def one_json_line(proc: subprocess.CompletedProcess[str]) -> dict:
    lines = [ln for ln in proc.stdout.splitlines() if ln.strip()]
    assert len(lines) == 1, proc.stdout + proc.stderr
    return json.loads(lines[0])


def test_cli_contract(elt_env, pg_dsn, work_dir):
    cfg, dataset = make_synth(
        work_dir, nodes=200, attributes=10, seed=9, days=0, kinds=("parquet", "parquet", "parquet")
    )
    config = write_config(cfg, work_dir / "config.yml")
    stage_live(dataset, 0)
    out = work_dir / "export"
    first = run_cli(["init", "--config", str(config)])
    assert first.returncode == 0 and one_json_line(first)["status"] == "ok"
    assert run_cli(["init", "--config", str(config)]).returncode == 0  # idempotent
    ing = run_cli(["ingest", "--config", str(config), "--source", "crm", "--run-id", "r1"])
    summary = one_json_line(ing)
    assert ing.returncode == 0 and summary["command"] == "ingest"
    assert summary["counters"]["inserted"] > 0 and "dbt" in summary["seconds"]
    for sid in ("erp", "web"):
        assert (
            run_cli(
                ["ingest", "--config", str(config), "--source", sid, "--run-id", "r1"]
            ).returncode
            == 0
        )
    replay = one_json_line(
        run_cli(["ingest", "--config", str(config), "--source", "crm", "--run-id", "r1"])
    )
    assert replay["replayed"] is True and replay["counters"]["inserted"] > 0
    rec = run_cli(["reconcile", "--config", str(config), "--run-id", "r1"])
    rsum = one_json_line(rec)
    assert rec.returncode == 0 and rsum["counters"]["stagings"] > 0
    assert {"render", "dbt", "apply", "total"} <= set(rsum["seconds"])
    exp = run_cli(["export", "--config", str(config), "--out", str(out)])
    esum = one_json_line(exp)
    assert exp.returncode == 0 and esum["counters"]["golden"] == rsum["counters"]["stagings"]
    assert (out / "golden.parquet").exists() and (out / "match_pair.parquet").exists()

    # échec : variable d'environnement manquante -> code != 0 et ligne JSON d'erreur
    bad = run_cli(["init", "--config", str(config)], env={"MDM_STATE_DSN": ""})
    assert bad.returncode != 0 and one_json_line(bad)["status"] == "error"
    unknown = run_cli(["ingest", "--config", str(config), "--source", "nope", "--run-id", "x"])
    assert unknown.returncode != 0 and one_json_line(unknown)["status"] == "error"
    # secrets : jamais dans la sortie
    assert "elt_test_only" not in (ing.stdout + ing.stderr + rec.stdout + rec.stderr)

    reset = run_cli(["reset", "--config", str(config)])
    assert reset.returncode == 0 and one_json_line(reset)["counters"]["dropped"] is True
    with psycopg.connect(pg_dsn) as conn:
        left = conn.execute(
            "SELECT count(*) FROM information_schema.schemata WHERE schema_name = %s",
            [cfg.state.schema_name],
        ).fetchone()
    assert left == (0,)
    assert run_cli(["reset", "--config", str(config)]).returncode == 0  # idempotent
