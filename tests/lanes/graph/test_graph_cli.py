"""CLI contract (docs/MDM_CONTRACT.md section 6): JSON line, exit codes, idempotence, secrets."""

from __future__ import annotations

import copy
import json
import subprocess
import sys

from graph_data_base import BASE, crm, erp, web
from graph_support import PASSWORD, ROOT, Harness

from engine.core.canonical import TABLES, check_tables


def make(server, tmp, name: str) -> Harness:
    raw = copy.deepcopy(BASE)
    raw["state"] = {"dsn_env": "X", "schema": f"mdm_{name}"}
    h = Harness(server, raw, tmp, name=name)
    h.load_source("crm", [crm(1, "Dupont"), crm(2, "Martin")], True)
    h.load_source("erp", [erp(10, "dupont"), erp(11, "martin", dn="1971-01-01")], True)
    h.load_source("web", [web(100, "DUPONT")], False)
    return h


def test_commands_print_one_json_line_and_are_idempotent(age_server, tmp_path) -> None:
    h = make(age_server, tmp_path, "cli1")
    first = h.cli("init")
    second = h.cli("init")
    assert first["_stdout_lines"] == second["_stdout_lines"] == 1
    assert first["graph_created"] is True and second["graph_created"] is False
    for sid in ("crm", "erp", "web"):
        res = h.cli("ingest", "--source", sid, "--run-id", "r1")
        assert res["_stdout_lines"] == 1
        assert res["lane"] == "graph" and res["command"] == "ingest" and res["run_id"] == "r1"
        assert res["status"] == "ok" and res["counts"]["inserted"] > 0 and "read" in res["seconds"]
    rec = h.cli("reconcile", "--run-id", "r1")
    assert rec["counts"]["delta_nodes"] == 5 and rec["counts"]["stagings"] > 0
    assert {"index", "pairs", "decide", "recluster", "survive"} <= set(rec["seconds"])
    out = tmp_path / "parquet_out"
    exported = h.cli("export", "--out", str(out))
    assert exported["_stdout_lines"] == 1
    assert {p.stem for p in out.glob("*.parquet")} == set(TABLES)
    tables = h.export_tables("again")
    assert check_tables(h.cfg, tables) == []
    # replays of the same run id change nothing
    again = h.cli("reconcile", "--run-id", "r1")
    assert again["replayed"] is True
    assert h.cli("ingest", "--source", "crm", "--run-id", "r1")["replayed"] is True
    assert h.export_tables("replayed")["golden"].equals(tables["golden"])


def test_failures_exit_non_zero_with_a_json_error(age_server, tmp_path) -> None:
    h = make(age_server, tmp_path, "cli2")
    h.cli("init")
    no_env = h.cli("reconcile", "--run-id", "r1", env={h.state_env: ""}, check=False)
    assert no_env["_returncode"] == 2 and "not set" in no_env["error"]
    unknown = h.cli("ingest", "--source", "nope", "--run-id", "r1", check=False)
    assert unknown["_returncode"] == 2 and "unknown source" in unknown["error"]
    bad_run = h.cli("reconcile", "--run-id", "bad run id!", check=False)
    assert bad_run["_returncode"] == 1 and bad_run["status"] == "error"
    proc = subprocess.run(
        [
            sys.executable,
            "-B",
            "-m",
            "engine.lanes.graph",
            "export",
            "--out",
            str(tmp_path / "x"),
            "--config",
            "nope.yml",
        ],
        capture_output=True,
        text=True,
        cwd=ROOT,
        env=h.environ(),
        timeout=120,
        check=False,
    )
    assert proc.returncode == 2
    assert json.loads(proc.stdout.strip().splitlines()[-1])["status"] == "error"


def test_reset_drops_the_state_and_init_recreates_it(age_server, tmp_path) -> None:
    h = make(age_server, tmp_path, "cli3")
    h.cli("init")
    h.cli("ingest", "--source", "crm", "--run-id", "r1")
    schema = h.cfg.state.schema_name
    assert h.sql("SELECT count(*) FROM pg_namespace WHERE nspname = %s", (schema,))[0][0] == 1
    gone = h.cli("reset")
    assert gone["graph_dropped"] is True
    assert (
        h.sql(
            "SELECT count(*) FROM pg_namespace WHERE nspname IN (%s, %s)", (schema, f"{schema}_g")
        )[0][0]
        == 0
    )
    assert h.cli("reset")["graph_dropped"] is False
    h.cli("init")
    res = h.cli("ingest", "--source", "crm", "--run-id", "r1")
    assert res["replayed"] is False and res["counts"]["inserted"] == 2


def test_attribute_changes_require_a_reset(age_server, tmp_path) -> None:
    h = make(age_server, tmp_path, "cli4")
    h.cli("init")
    raw = copy.deepcopy(h.raw)
    raw["attributes"].append({"name": "extra", "sources": [{"source": "crm", "column": "ct"}]})
    changed = h.tmp / "changed.yml"
    import yaml

    changed.write_text(yaml.safe_dump(raw), encoding="utf-8")
    res = h.cli("init", config=changed, check=False)
    assert res["_returncode"] == 2 and "reset" in res["error"]


def test_secrets_stay_in_the_environment(age_server, tmp_path) -> None:
    h = make(age_server, tmp_path, "cli5")
    assert PASSWORD not in h.config_path.read_text(encoding="utf-8")
    h.cli("init")
    for sid in ("crm", "erp", "web"):
        res = h.cli("ingest", "--source", sid, "--run-id", "r1")
        assert PASSWORD not in json.dumps(res)
    rec = h.cli("reconcile", "--run-id", "r1")
    assert PASSWORD not in json.dumps(rec)
    bad = h.cli(
        "reconcile",
        "--run-id",
        "r2",
        env={h.state_env: h.env[h.state_env].replace(PASSWORD, "hunter2")},
        check=False,
    )
    assert bad["_returncode"] != 0 and "hunter2" not in json.dumps(bad)


def test_sources_can_be_ingested_in_parallel_then_reconciled_once(age_server, tmp_path) -> None:
    from concurrent.futures import ThreadPoolExecutor

    from engine.core.compare import compare_exports
    from engine.core.reference import ReferenceEngine

    h = make(age_server, tmp_path, "cli6")
    rows = {
        "crm": [crm(i, f"Name{i % 40}", fn=f"f{i % 7}") for i in range(1, 400)],
        "erp": [erp(1000 + i, f"name{i % 40}", prenom=f"f{i % 7}") for i in range(1, 350)],
        "web": [web(5000 + i, f"NAME{i % 40}", given=f"f{i % 7}") for i in range(1, 300)],
    }
    for sid, data in rows.items():
        h.load_source(sid, data, sid != "web")
    h.cli("init")
    with ThreadPoolExecutor(max_workers=3) as pool:
        results = list(pool.map(lambda s: h.cli("ingest", "--source", s, "--run-id", "p1"), rows))
    assert all(r["counts"]["inserted"] > 0 for r in results)
    h.cli("reconcile", "--run-id", "p1")
    ref = ReferenceEngine(h.cfg)
    for sid in rows:
        ref.ingest(sid, h.rows[sid], "p1")
    ref.reconcile("p1")
    report = compare_exports(h.export_tables("par"), ref.export(), include_none_pairs=True)
    assert report.ok, report.format()
