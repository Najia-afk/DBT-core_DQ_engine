"""Kill the lane in the middle of a transaction: nothing is published, a rerun converges."""

from __future__ import annotations

import copy

import pytest
from graph_data_base import BASE, crm, erp, web
from graph_support import Harness

from engine.core.compare import compare_exports
from engine.core.reference import ReferenceEngine

DAY0 = {
    "crm": [
        crm(1, "Dupont", em="a@x.fr"),
        crm(2, "Martin", bd="1970-01-01"),
        crm(3, "Durand"),
        crm(4, "Dupond"),
        crm(5, "Bernard", fn="paul"),
    ],
    "erp": [
        erp(10, "dupont"),
        erp(11, "martin", dn="1971-01-01"),
        erp(12, "Durand", plafond="12.5"),
        erp(13, "bernard", prenom="paul"),
    ],
    "web": [web(100, "DUPONT", mail="a@x.fr"), web(101, "Durant")],
}
DAY1 = {
    "crm": [
        crm(1, "Martin"),
        crm(2, "Martin", bd="1970-01-01"),
        crm(3, "Durand"),
        crm(4, "Dupont"),
        crm(6, "Nouveau"),
    ],
    "erp": [
        erp(10, "dupont"),
        erp(11, "martin", dn="1971-01-01"),
        erp(12, "Durant", plafond="12.5"),
        erp(13, "bernard", prenom="paul"),
    ],
    "web": [web(100, "Dupont", mail="a@x.fr", rev="2025-01-01")],
}
POINTS = [
    "reconcile_after_index",
    "reconcile_after_pairs",
    "reconcile_after_decisions",
    "reconcile_after_recluster",
    "reconcile_after_survive",
    "reconcile_before_commit",
]


def build(server, tmp, name: str) -> tuple[Harness, ReferenceEngine]:
    raw = copy.deepcopy(BASE)
    raw["state"] = {"dsn_env": "X", "schema": f"mdm_{name}"}
    h = Harness(server, raw, tmp, name=name)
    h.cli("init")
    ref = ReferenceEngine(h.cfg)
    for run, data, replace in (("d0", DAY0, True),):
        for sid, rows in data.items():
            h.load_source(sid, rows, replace if sid != "web" else False)
            h.cli("ingest", "--source", sid, "--run-id", run)
            ref.ingest(sid, h.rows[sid], run)
        h.cli("reconcile", "--run-id", run)
        ref.reconcile(run)
    return h, ref


def tables_equal(a: dict, b: dict) -> bool:
    return all(a[name].equals(b[name]) for name in a)


@pytest.mark.parametrize("point", POINTS)
def test_kill_during_reconcile_leaves_state_untouched_and_rerun_converges(
    age_server, tmp_path, point
) -> None:
    h, ref = build(age_server, tmp_path, f"kill_{point[-8:]}")
    for sid, rows in DAY1.items():
        h.load_source(sid, rows, sid != "web")
        h.cli("ingest", "--source", sid, "--run-id", "d1")
        ref.ingest(sid, h.rows[sid], "d1")
    before = h.export_tables("before")
    pending = h.sql(f'SELECT count(*) FROM "{h.cfg.state.schema_name}".pending')[0][0]
    assert pending > 0

    killed = h.cli("reconcile", "--run-id", "d1", env={"MDM_GRAPH_FAIL_AT": point}, check=False)
    assert killed["_returncode"] == 137
    assert killed["_stdout_lines"] == 0
    assert h.sql(f'SELECT count(*) FROM "{h.cfg.state.schema_name}".pending')[0][0] == pending
    done = h.sql(
        f'SELECT count(*) FROM "{h.cfg.state.schema_name}".run_log'
        " WHERE run_id = 'd1' AND step = 'reconcile'"
    )
    assert done[0][0] == 0
    assert tables_equal(before, h.export_tables("after_kill")), "a killed reconcile leaked state"

    summary = h.cli("reconcile", "--run-id", "d1")
    assert summary["status"] == "ok" and summary["replayed"] is False
    ref.reconcile("d1")
    report = compare_exports(h.export_tables("converged"), ref.export(), include_none_pairs=True)
    assert report.ok, report.format()
    assert h.sql(f'SELECT count(*) FROM "{h.cfg.state.schema_name}".pending')[0][0] == 0


def test_kill_during_ingest_publishes_nothing_and_rerun_converges(age_server, tmp_path) -> None:
    h, ref = build(age_server, tmp_path, "killing")
    h.load_source("crm", DAY1["crm"], True)
    before = h.export_tables("before")
    killed = h.cli(
        "ingest",
        "--source",
        "crm",
        "--run-id",
        "d1",
        env={"MDM_GRAPH_FAIL_AT": "ingest_before_commit"},
        check=False,
    )
    assert killed["_returncode"] == 137
    assert tables_equal(before, h.export_tables("after_kill"))
    vertices = h.sql(f'SELECT count(*) FROM "{h.cfg.state.schema_name}_g"."Record"')[0][0]
    assert vertices == before["source_record"].num_rows
    again = h.cli("ingest", "--source", "crm", "--run-id", "d1")
    assert again["replayed"] is False and again["counts"]["updated"] > 0
    ref.ingest("crm", h.rows["crm"], "d1")
    h.cli("reconcile", "--run-id", "d1")
    ref.reconcile("d1")
    report = compare_exports(h.export_tables("converged"), ref.export(), include_none_pairs=True)
    assert report.ok, report.format()


def test_a_failing_ingest_exits_non_zero_without_side_effects(age_server, tmp_path) -> None:
    h, _ = build(age_server, tmp_path, "failing")
    before = h.export_tables("before")
    h.rows["crm"] = []
    import psycopg

    with psycopg.connect(h.env[h.cfg.source("crm").dsn_env], autocommit=True) as conn:
        conn.execute("ALTER TABLE t.crm RENAME COLUMN ln TO ln_gone")
    res = h.cli("ingest", "--source", "crm", "--run-id", "d1", check=False)
    assert res["_returncode"] == 2 and res["status"] == "error"
    assert "ln" in res["error"]
    assert tables_equal(before, h.export_tables("after"))
