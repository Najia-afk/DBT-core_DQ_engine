"""Lane-level properties: from-scratch equivalence, idempotent replay, delta-only work, crashes."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
from pathlib import Path
from typing import Any

import psycopg
import pytest
import yaml
from etl_support import Lab, make_dataset, read_day

from engine.core.compare import compare_exports
from engine.core.synth import DeltaParams

ROOT = Path(__file__).resolve().parents[3]


def cli(lab: Lab, *argv: str, fault: str | None = None) -> tuple[int, dict[str, Any], str]:
    """Run the lane as a separate process (so that it can be killed by a fault point)."""
    env = {**os.environ, "PYTHONDONTWRITEBYTECODE": "1", "PYTHONPATH": str(ROOT)}
    env[lab.cfg.state.dsn_env] = lab.dsn
    for s in lab.cfg.sources:
        if s.dsn_env:
            env[s.dsn_env] = lab.dsn
    if fault:
        env["MDM_ETL_FAULT"] = fault
    proc = subprocess.run(
        [
            sys.executable, "-m", "engine.lanes.etl", argv[0],
            "--config", str(lab.cfg_path), *argv[1:],
        ],
        capture_output=True, text=True, env=env, cwd=ROOT, timeout=240, check=False,
    )  # fmt: skip
    lines = [ln for ln in proc.stdout.splitlines() if ln.startswith("{")]
    return proc.returncode, (json.loads(lines[0]) if lines else {}), proc.stderr


@pytest.fixture
def lab(tmp_path: Path, pg_db: str, monkeypatch: pytest.MonkeyPatch) -> Lab:
    ds = make_dataset(tmp_path, days=2, nodes=500, attributes=14, seed=8)
    lab = Lab(ds, pg_db, monkeypatch, tmp_path / "out")
    lab.lane("init")
    return lab


def state_tables(lab: Lab) -> dict[str, Any]:
    return lab.export("snap")


def same_table(a: Any, b: Any) -> bool:
    """Equality independent of the physical row order (exports have no ORDER BY)."""
    keys = [(name, "ascending") for name in a.column_names]
    return a.sort_by(keys).equals(b.sort_by(keys))


def same_tables(a: dict[str, Any], b: dict[str, Any]) -> bool:
    return set(a) == set(b) and all(same_table(a[name], b[name]) for name in a)


def test_init_is_idempotent_and_versioned(lab: Lab) -> None:
    again = lab.lane("init")
    assert again["counts"]["migrations_applied"] == []
    assert again["counts"]["version"] == 1
    assert lab.sql("SELECT version FROM schema_version") == [(1,)]
    query = (
        "SELECT column_name FROM information_schema.columns"
        " WHERE table_schema = %s AND table_name = 'source_record'"
    )
    cols = {r[0] for r in lab.sql(query, [lab.cfg.state.schema_name])}
    assert {a.name for a in lab.cfg.attributes} <= cols  # one column per attribute


def test_incremental_equals_from_scratch(lab: Lab, tmp_path: Path) -> None:
    for day in range(3):
        lab.lane_day(day)
    incremental = lab.export("inc")
    cold_cfg = tmp_path / "cold.yml"
    raw = yaml.safe_load(lab.cfg_path.read_text(encoding="utf-8"))
    raw["state"]["schema"] = "mdm_cold"
    cold_cfg.write_text(yaml.safe_dump(raw, sort_keys=False), encoding="utf-8")
    cold = Lab.__new__(Lab)
    cold.__dict__.update({**lab.__dict__, "cfg_path": cold_cfg})
    from engine.core.config import load_config

    cold.cfg = load_config(cold_cfg)
    cold.lane("init")
    cols = ", ".join(a.name for a in lab.cfg.attributes)
    with psycopg.connect(lab.dsn, autocommit=True) as conn:
        conn.execute(
            "INSERT INTO mdm_cold.source_record (source_id, source_key, row_hash, revision,"
            f" status, first_run, last_change_run, {cols}) SELECT source_id, source_key, row_hash,"
            f" revision, 'active', 'scratch', 'scratch', {cols}"
            f" FROM {lab.cfg.state.schema_name}.source_record WHERE status = 'active'"
        )
        conn.execute("INSERT INTO mdm_cold.pending_node SELECT node_id FROM mdm_cold.source_record")
    cold.lane("reconcile", "--run-id", "scratch")
    report = compare_exports(incremental, cold.export("cold"), state_only=True)
    assert report.ok, report.format()
    assert incremental["golden"].num_rows > 50


def test_replay_same_run_id_is_a_noop(lab: Lab) -> None:
    lab.lane_day(0)
    lab.lane_day(1)
    before = state_tables(lab)
    replay = lab.lane_day(1)
    assert all(summary["counts"].get("replayed") for summary in replay.values())
    assert same_tables(before, state_tables(lab))


def test_replay_same_data_new_run_id_produces_no_delta(lab: Lab) -> None:
    lab.lane_day(0)
    lab.lane_day(1)
    before = state_tables(lab)
    for source in lab.cfg.sources:
        summary = lab.lane("ingest", "--source", source.id, "--run-id", "again")
        counts = summary["counts"]
        assert (counts["inserted"], counts["updated"], counts["deleted"]) == (0, 0, 0)
    assert lab.lane("reconcile", "--run-id", "again")["counts"]["delta_nodes"] == 0
    after = state_tables(lab)
    for name in ("source_record", "source_delta", "match_pair", "staging", "staging_member",
                 "staging_event", "golden", "golden_delta", "review"):  # fmt: skip
        assert same_table(before[name], after[name]), name


def xmins(lab: Lab, table: str, key: str) -> dict[Any, str]:
    return {r[0]: r[1] for r in lab.sql(f"SELECT {key}, xmin::text FROM {table}")}


def test_delta_only_work(tmp_path: Path, pg_db: str, monkeypatch: pytest.MonkeyPatch) -> None:
    quiet = DeltaParams(update_pct=1.5, new_pct=0.6, delete_pct=0.4, broken_pct=0.2,
                        merge_pct=10, split_pct=10)  # fmt: skip
    ds = make_dataset(tmp_path, days=1, delta=quiet, nodes=3000, attributes=20, seed=12)
    lab = Lab(ds, pg_db, monkeypatch, tmp_path / "out")
    lab.lane("init")
    lab.lane_day(0)
    pairs_sql = "SELECT left_id, right_id, xmin::text FROM match_pair"
    pairs = lab.sql(pairs_sql)
    tables = {
        "golden": xmins(lab, "golden", "golden_id"),
        "staging_member": xmins(lab, "staging_member", "node_id"),
        "staging": xmins(lab, "staging", "staging_id"),
        "source_record": xmins(lab, "source_record", "node_id"),
    }
    pair_xmin = {(r[0], r[1]): r[2] for r in pairs}
    ingests = lab.lane_day(1)
    counts = ingests["reconcile"]["counts"]
    ingest_changes = sum(
        ingests[s.id]["counts"][k]
        for s in lab.cfg.sources
        for k in ("inserted", "updated", "deleted")
    )
    total_stagings = counts["stagings_total"]
    assert 0 < counts["delta_nodes"] == ingest_changes
    assert counts["stagings_read"] < 0.2 * total_stagings, counts
    assert counts["affected_nodes"] < 0.3 * len(tables["source_record"]), counts
    assert counts["member_rows_read"] < 0.3 * len(tables["staging_member"]), counts
    assert counts["record_rows_read_wide"] < 0.3 * len(tables["source_record"]), counts
    # rows physically rewritten (new tuple version => new xmin) are bounded by the counters
    for name, key in (("golden", "golden_id"), ("staging_member", "node_id"),
                      ("source_record", "node_id")):  # fmt: skip
        now = xmins(lab, name, key)
        rewritten = {k for k, x in now.items() if tables[name].get(k) != x}
        limit = {
            "golden": counts["golden_rows_written"] + counts["stagings_retired"],
            "staging_member": counts["member_rows_written"],
            "source_record": ingest_changes,
        }[name]
        assert len(rewritten) <= limit, (name, len(rewritten), limit)
        assert len(rewritten) < 0.3 * len(now), (name, len(rewritten), len(now))
    now_pairs = {(r[0], r[1]): r[2] for r in lab.sql(pairs_sql)}
    rewritten_pairs = {k for k, x in now_pairs.items() if pair_xmin.get(k) != x}
    assert len(rewritten_pairs) <= counts["pairs_inserted"] + counts["pairs_updated"]
    assert len(rewritten_pairs) < 0.3 * len(now_pairs)
    stable = sum(
        1 for k, x in xmins(lab, "golden", "golden_id").items() if tables["golden"].get(k) == x
    )
    assert stable > 0.7 * total_stagings


def test_kill_during_reconcile_then_rerun_converges(lab: Lab) -> None:
    lab.lane_day(0)
    lab.ref_day(0)
    lab.load_sources(1)
    for source in lab.cfg.sources:
        assert cli(lab, "ingest", "--source", source.id, "--run-id", "d1")[0] == 0
    stable = state_tables(lab)
    pending = lab.sql("SELECT count(*) FROM pending_node")[0][0]
    assert pending > 0
    for point in ("reconcile:after_blocks", "reconcile:after_scoring", "reconcile:after_pairs",
                  "reconcile:before_commit"):  # fmt: skip
        code, summary, err = cli(lab, "reconcile", "--run-id", "d1", fault=point)
        assert code == 97 and "fault injected" in err
        after = state_tables(lab)
        for name in ("match_pair", "staging", "staging_member", "golden", "golden_delta",
                     "staging_event", "review", "anomaly"):  # fmt: skip
            assert same_table(stable[name], after[name]), (point, name)
        assert lab.sql("SELECT count(*) FROM pending_node")[0][0] == pending
        assert lab.sql("SELECT status FROM run_log WHERE step = 'reconcile' AND run_id = 'd1'") == [
            ("running",)
        ]
    code, summary, _ = cli(lab, "reconcile", "--run-id", "d1")
    assert code == 0 and summary["status"] == "ok"
    for source in lab.cfg.sources:
        lab.ref.ingest(source.id, read_day(lab.ds, 1, source.id), "d1")
    lab.ref.reconcile("d1")
    report = lab.compare()
    assert report.ok, report.format()


@pytest.mark.parametrize("point", ["ingest:after_batch", "ingest:before_apply"])
def test_kill_during_ingest_then_rerun_converges(lab: Lab, point: str) -> None:
    lab.lane_day(0)
    lab.ref_day(0)
    lab.load_sources(1)
    before = state_tables(lab)
    code, _, err = cli(lab, "ingest", "--source", "crm", "--run-id", "d1", fault=point)
    assert code == 97
    assert same_tables(before, state_tables(lab))  # nothing applied
    for source in lab.cfg.sources:
        assert cli(lab, "ingest", "--source", source.id, "--run-id", "d1")[0] == 0
    assert cli(lab, "reconcile", "--run-id", "d1")[0] == 0
    lab.ref_day(1)
    report = lab.compare()
    assert report.ok, report.format()


def test_parallel_ingests_of_several_sources(lab: Lab) -> None:
    lab.load_sources(0)
    results: dict[str, tuple[int, dict[str, Any], str]] = {}

    def run(source: str) -> None:
        results[source] = cli(lab, "ingest", "--source", source, "--run-id", "d0")

    threads = [threading.Thread(target=run, args=(s.id,)) for s in lab.cfg.sources]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=240)
    assert all(code == 0 for code, _, _ in results.values()), results
    assert cli(lab, "reconcile", "--run-id", "d0")[0] == 0
    lab.ref_day(0)
    report = lab.compare()
    assert report.ok, report.format()


def test_failures_are_reported(lab: Lab, monkeypatch: pytest.MonkeyPatch) -> None:
    code, summary, err = cli(lab, "ingest", "--source", "nope", "--run-id", "x1")
    assert code == 1 and summary["status"] == "failed" and "unknown source" in summary["error"]
    code, summary, _ = cli(lab, "ingest", "--source", "crm", "--run-id", "bad run id")
    assert code == 1 and summary["status"] == "failed"
    # source table missing: failure, run logged as failed, nothing applied
    lab.load_sources(0)
    with psycopg.connect(lab.dsn, autocommit=True) as conn:
        conn.execute("DROP TABLE public.crm_customer")
    code, summary, _ = cli(lab, "ingest", "--source", "crm", "--run-id", "x2")
    assert code == 1 and "crm" in summary["error"]
    assert lab.sql("SELECT status FROM run_log WHERE run_id = 'x2'") == [("failed",)]
    assert lab.sql("SELECT count(*) FROM source_record")[0][0] == 0
    env_name = lab.cfg.sources[0].dsn_env
    monkeypatch.delenv(env_name)
    summary = lab.lane("ingest", "--source", "crm", "--run-id", "x3", ok=False)
    assert summary["status"] == "failed" and env_name in summary["error"]
    monkeypatch.setenv(
        "MDM_STATE_DSN", "host=127.0.0.1 port=1 dbname=none user=x connect_timeout=2"
    )
    assert lab.lane("reconcile", "--run-id", "x4", ok=False)["status"] == "failed"


def test_batch_retry_on_connection_loss(
    tmp_path: Path, pg_db: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    from engine.lanes.etl import ingest as ingest_module

    ds = make_dataset(tmp_path, days=0, nodes=3000, attributes=10, seed=2)
    raw = yaml.safe_load((ds / "config.yml").read_text(encoding="utf-8"))
    raw["runtime"]["batch_rows"] = 1000
    (ds / "config.yml").write_text(yaml.safe_dump(raw, sort_keys=False), encoding="utf-8")
    lab = Lab(ds, pg_db, monkeypatch, tmp_path / "out")
    lab.lane("init")
    lab.load_sources(0)
    original = ingest_module._stage_batch
    calls = {"n": 0}

    def flaky(conn: Any, tf: Any, batch: Any) -> int:
        calls["n"] += 1
        if calls["n"] == 2:  # second batch: the connection dies before the work
            conn.execute("SELECT pg_terminate_backend(pg_backend_pid())")
        return original(conn, tf, batch)

    monkeypatch.setattr(ingest_module, "_stage_batch", flaky)
    monkeypatch.setattr("engine.lanes.etl.db.RETRY_BASE_S", 0.01)
    summary = lab.lane("ingest", "--source", "crm", "--run-id", "d0")
    assert summary["counts"]["batches"] >= 2 and summary["counts"]["retries"] >= 1
    monkeypatch.setattr(ingest_module, "_stage_batch", original)
    for source in ("erp", "web"):
        lab.lane("ingest", "--source", source, "--run-id", "d0")
    lab.lane("reconcile", "--run-id", "d0")
    lab.ref_day(0)
    report = lab.compare()
    assert report.ok, report.format()


def test_reserved_column_name_is_refused(
    tmp_path: Path, pg_db: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    ds = make_dataset(tmp_path, days=0, nodes=60, attributes=8, seed=1)
    raw = yaml.safe_load((ds / "config.yml").read_text(encoding="utf-8"))
    raw["attributes"][-1]["name"] = "revision"  # accepted by the config, physical column name
    (ds / "config.yml").write_text(yaml.safe_dump(raw, sort_keys=False), encoding="utf-8")
    lab = Lab(ds, pg_db, monkeypatch, tmp_path / "out")
    summary = lab.lane("init", ok=False)
    assert summary["status"] == "failed" and "collides" in summary["error"]


def test_reset_drops_state(lab: Lab) -> None:
    lab.lane_day(0)
    assert lab.lane("reset")["counts"]["dropped"] is True
    assert lab.lane("reset")["counts"]["dropped"] is False
    lab.lane("init")
    assert lab.sql("SELECT count(*) FROM source_record") == [(0,)]
    assert lab.lane("ingest", "--source", "crm", "--run-id", "again0")["counts"]["inserted"] > 0
