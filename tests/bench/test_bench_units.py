"""Jeux de données, étapes, résumés de lanes et rapport (sans Docker)."""

from __future__ import annotations

import json
from pathlib import Path

from engine.bench import report
from engine.bench.concurrency import analyze, max_concurrency, peak_overlapping_sum
from engine.bench.dataset import (
    DatasetSpec,
    day_rows,
    delta_args,
    stage_live,
    synth_commands,
)
from engine.bench.scenario import find_int, trim_summary
from engine.bench.steps import LADDER, equivalence_dataset


def test_dataset_key_is_stable_and_ignores_workers() -> None:
    a = DatasetSpec(nodes=1000, attributes=20, deltas=({"update_pct": 1.0},))
    b = DatasetSpec(nodes=1000, attributes=20, deltas=({"update_pct": 1.0},), workers=2)
    c = DatasetSpec(nodes=1000, attributes=20, deltas=({"update_pct": 2.0},))
    assert a.key() == b.key()
    assert a.key() != c.key()
    assert a.days == 1


def test_synth_commands_one_generate_then_one_next_day_per_day(tmp_path: Path) -> None:
    spec = LADDER["L1"].dataset()
    cmds = synth_commands(spec, tmp_path / "ds")
    assert len(cmds) == 1 + 3
    gen = " ".join(cmds[0])
    assert "--nodes 100000" in gen and "--attributes 50" in gen and "--seed 42" in gen
    assert "--kinds parquet,parquet,parquet" in gen and "--snapshots full,full,delta" in gen
    assert "--days 0" in gen
    updates = [c[c.index("--update-pct") + 1] for c in cmds[1:]]
    assert updates == ["1.0", "5.0", "20.0"]
    assert all("next-day" in c for c in cmds[1:])


def test_delta_args_fill_defaults() -> None:
    args = delta_args({"update_pct": 7})
    assert args[args.index("--update-pct") + 1] == "7"
    assert "--merge-pct" in args and "--broken-pct" in args


def test_ladder_matches_the_ticket() -> None:
    assert (LADDER["L1"].nodes, LADDER["L1"].attributes) == (100_000, 50)
    assert LADDER["L1"].update_pcts == (1.0, 5.0, 20.0)
    assert (LADDER["L2"].nodes, LADDER["L2"].attributes, LADDER["L2"].update_pcts) == (
        300_000, 200, (1.0, 5.0),
    )  # fmt: skip
    assert (LADDER["L3"].nodes, LADDER["L3"].attributes) == (1_000_000, 200)
    ds = LADDER["L1"].dataset()
    assert ds.snapshots == ("full", "full", "delta") and set(ds.kinds) == {"parquet"}
    eq = equivalence_dataset()
    assert eq.nodes <= 10_000 and eq.days == 3


def make_fake_dataset(root: Path) -> Path:
    ds = root / "ds"
    sources = [
        {"id": "crm", "kind": "parquet", "snapshot": "full"},
        {"id": "web", "kind": "parquet", "snapshot": "delta"},
        {"id": "erp", "kind": "postgres", "snapshot": "full"},
    ]
    days = {
        "0": {"sources": {"crm": {"rows": 10}, "web": {"rows": 7}, "erp": {"rows": 5}}},
        "1": {"sources": {"crm": {"rows": 10}, "web": {"rows": 2}, "erp": {"rows": 5}}},
    }
    ds.mkdir()
    (ds / "manifest.json").write_text(json.dumps({"sources": sources, "days": days}))
    for day in (0, 1):
        for sid in ("crm", "web"):
            part = ds / f"day{day}" / sid
            part.mkdir(parents=True)
            (part / "part-00000.parquet").write_bytes(b"x" * (day + 1))
    return ds


def test_stage_live_replaces_both_full_and_delta_sources(tmp_path: Path) -> None:
    ds = make_fake_dataset(tmp_path)
    assert stage_live(ds, 0) == {"crm": 1, "web": 1}
    assert stage_live(ds, 1) == {"crm": 1, "web": 1}
    for sid in ("crm", "web"):
        names = [p.name for p in (ds / "live" / sid).iterdir()]
        assert names == ["day1-part-00000.parquet"]  # le jour 0 n'est plus visible
    assert not (ds / "live" / "erp").exists()  # source postgres : pas de parquet


def test_day_rows(tmp_path: Path) -> None:
    manifest = json.loads((make_fake_dataset(tmp_path) / "manifest.json").read_text())
    assert day_rows(manifest, 1) == {"crm": 10, "web": 2, "erp": 5}


def test_find_int_and_trim_summary() -> None:
    summary = {
        "status": "ok",
        "counters": {"delta_nodes": 42, "dbt_models": {"a": 1.0, "b": 9.0, "c": 5.0}},
        "seconds": {"dbt": 1.0},
        "peak_rss_mb": 88.5,
        "noise": [1, 2, 3],
    }
    assert find_int(summary, ("delta_nodes",)) == 42
    assert find_int({"counts": {"delta_nodes": True}}, ("delta_nodes",)) is None
    assert find_int(None, ("delta_nodes",)) is None
    trimmed = trim_summary(summary)
    assert trimmed is not None
    assert "dbt_models" not in trimmed["counters"]
    assert list(trimmed["dbt_top_models_s"]) == ["b", "c", "a"]
    assert trimmed["peak_rss_mb"] == 88.5 and "noise" not in trimmed
    assert trim_summary(None) is None


def test_concurrency_helpers() -> None:
    assert max_concurrency([(0, 5), (1, 6), (2, 3), (7, 8)]) == 3
    assert (
        max_concurrency([(0, 1), (1, 2)]) == 1
    )  # fin et début au même instant : pas de chevauchement
    assert peak_overlapping_sum([(0, 5, 100), (1, 6, 200), (7, 8, 900)]) == 900
    assert peak_overlapping_sum([(0, 5, 100), (1, 6, 200)]) == 300


def test_analyze_detects_serialization_and_overlap() -> None:
    runs = [
        {"run_id": "a", "referential": "r1", "status": "succeeded", "started": 0, "ended": 10},
        {"run_id": "b", "referential": "r1", "status": "succeeded", "started": 10.5, "ended": 20},
        {"run_id": "c", "referential": "r2", "status": "succeeded", "started": 1, "ended": 15},
    ]
    steps = [
        {"run_id": "a", "step": "init", "started": 0, "ended": 3, "peak_rss_mib": 100},
        {"run_id": "c", "step": "init", "started": 1, "ended": 4, "peak_rss_mib": 300},
        {"run_id": "b", "step": "init", "started": 10.5, "ended": 12, "peak_rss_mib": 50},
    ]
    result = analyze(runs, steps)
    assert result["all_succeeded"] is True
    assert result["run_concurrency_max"] == 2
    assert result["referential_concurrency_max"] == 2
    assert result["step_concurrency_max"] == 2
    assert result["sum_of_concurrent_step_peaks_mib"] == 400
    assert result["same_referential_serialization"] == [
        {"referential": "r1", "gap_s": 0.5, "ok": True}
    ]
    bad = [dict(runs[0]), {**runs[1], "started": 5.0}]
    assert analyze(bad, [])["same_referential_serialization"][0]["ok"] is False


def test_empty_queue_is_not_successful_concurrency_evidence() -> None:
    assert analyze([], [])["all_succeeded"] is False


def sample_run() -> dict:
    return {
        "schema": 1, "name": "etl-L1", "lane": "etl", "status": "ok", "abort_reason": None,
        "machine": {
            "date": "2026-11-13T04:00:00+01:00", "os": "Windows", "cpu_threads": 32,
            "ram_gb": 63.7, "python": "3.12", "duckdb": "2.0", "pyarrow": "26", "psycopg": "3.3",
            "dbt-core": "1.12", "dbt-duckdb": "1.11", "docker_server": "29",
        },
        "pg_server": "16.4", "pg_image": "postgres:16.4-bookworm",
        "config": {
            "hash": "abc123", "nodes": 100000, "attributes": 50, "sources": 3,
            "kinds": ["parquet"], "snapshots": ["full"], "seed": 42,
            "deltas": [{"update_pct": 1.0, "new_pct": 0.5, "delete_pct": 0.2, "broken_pct": 0.1}],
            "days": 1, "ingest_mode": "parallel", "batch_rows": "config (50000)",
            "elt_memory_limit": None, "elt_threads": None, "etl_workers": 8,
            "pg_tuning": {"memory": "12g"}, "limits": {},
        },
        "dataset": {"bytes": 2 * 1024**3, "gen_wall_s": 12.0, "gen_rss_peak_mb": 900},
        "commands": ["python -m engine.lanes.etl init --config C"],
        "phases": [
            {
                "day": 0, "phase": "ingest", "wall_s": 7.2, "rows": 190009, "rows_per_s": 26390.0,
                "rss_peak_mb": 811.3, "container_mem_peak_mb": 503.1,
                "container_cpu_peak_pct": 150.0, "pg_db_mb": 148.4, "pgdata_mb": 300.0,
                "pgdata_mb_peak": 320.0, "wal_mb": 120.0, "workdir_mb": 0.0, "status": "ok",
            }
        ],
        "totals": {
            "phases_wall_s": 7.2, "total_peak_mb": 1314.4, "pg_db_mb_final": 148.4,
            "pgdata_peak_mb": 320.0, "host_used_peak_gb": 27.0, "host_used_baseline_gb": 25.0,
        },
    }  # fmt: skip


def test_report_tables_and_details() -> None:
    run = sample_run()
    overview = report.overview_table([run])
    assert "etl-L1" in overview and "100 000" in overview and "abc123" in overview
    phases = report.phase_table(run)
    assert "26 390.0" in phases and "811.3" in phases and "320.0" in phases
    details = report.run_details(run)
    assert "commandes exactes" in details and "python -m engine.lanes.etl init" in details
    assert "upd 1.0% new 0.5%" in details


def test_splice_is_idempotent_and_preserves_handwritten_text() -> None:
    text = f"# titre\n\ntexte à moi\n\n{report.START}\nancien\n{report.END}\n\npied de page\n"
    once = report.splice(text, "NOUVEAU")
    assert "texte à moi" in once and "pied de page" in once and "ancien" not in once
    assert report.splice(once, "NOUVEAU") == once
    added = report.splice("# sans marqueurs", "BLOC")
    assert report.START in added and "BLOC" in added


def test_write_results_from_json_files(tmp_path: Path) -> None:
    runs = tmp_path / "runs"
    runs.mkdir()
    (runs / "etl-L1.json").write_text(json.dumps(sample_run()), encoding="utf-8")
    (runs / "smoke-etl.json").write_text(json.dumps({"ignored": True}), encoding="utf-8")
    path = report.write_results(tmp_path)
    text = path.read_text(encoding="utf-8")
    assert "etl-L1" in text and "ignored" not in text
    path.write_text("# intro manuelle\n" + text, encoding="utf-8")
    report.write_results(tmp_path)
    assert path.read_text(encoding="utf-8").startswith("# intro manuelle")
