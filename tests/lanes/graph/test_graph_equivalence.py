"""Graph lane == reference engine on generated day0 -> day1 -> day2 datasets (needs Docker)."""

from __future__ import annotations

import glob
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import yaml
from graph_data import Customers
from graph_support import Harness

from engine.core.canonical import check_tables, read_parquet_dir
from engine.core.compare import compare_exports
from engine.core.reference import ReferenceEngine
from engine.core.synth import DeltaParams, SynthParams, generate
from engine.lanes.graph.export import export
from engine.lanes.graph.scratch import replay_active

ROOT = Path(__file__).resolve().parents[3]


@dataclass
class Scenario:
    h: Harness
    ref: ReferenceEngine
    ingests: list[dict[str, dict]] = field(default_factory=list)
    reconciles: list[dict] = field(default_factory=list)
    reports: list[Any] = field(default_factory=list)
    tables: list[dict[str, pa.Table]] = field(default_factory=list)
    problems: list[list[str]] = field(default_factory=list)

    def feed(self, run: str, batches: dict[str, tuple[Any, bool]]) -> None:
        for sid, (rows, replace) in batches.items():
            self.h.load_source(sid, rows, replace)
        counts = {}
        for src in self.h.cfg.sources:
            if src.id in batches:
                counts[src.id] = self.h.ingest(src.id, run)["counts"]
                self.ref.ingest(src.id, self.h.rows[src.id], run)
        self.ingests.append(counts)
        self.reconciles.append(self.h.reconcile(run)["counts"])
        self.ref.reconcile(run)
        tables = self.h.export_tables(f"export_{run}")
        self.tables.append(tables)
        self.problems.append(check_tables(self.h.cfg, tables))
        self.reports.append(compare_exports(tables, self.ref.export(), include_none_pairs=True))


def synth_batches(ds: Path, day: int, params: SynthParams) -> dict[str, tuple[Any, bool]]:
    out = {}
    for i in range(params.sources):
        sid = ["crm", "erp", "web"][i]
        files = sorted(glob.glob(str(ds / f"day{day}" / sid / "part-*.parquet")))
        table = pa.concat_tables([pq.read_table(f) for f in files])
        out[sid] = (table, params.snapshot_of(i) == "full")
    return out


@pytest.fixture(scope="module")
def synth_scenario(age_server, tmp_path_factory) -> Scenario:
    tmp = tmp_path_factory.mktemp("synth")
    params = SynthParams(
        nodes=450,
        attributes=30,
        sources=3,
        seed=5,
        workers=1,
        kinds=("postgres", "postgres", "parquet"),
        snapshots=("full", "full", "delta"),
    )
    dp = DeltaParams(
        update_pct=10, new_pct=5, delete_pct=4, broken_pct=3, merge_pct=15, split_pct=15
    )
    ds = tmp / "ds"
    generate(params, ds, days=2, delta=dp)
    raw = yaml.safe_load((ds / "config.yml").read_text(encoding="utf-8"))
    raw["state"]["schema"] = "mdm_synth"
    h = Harness(age_server, raw, tmp, name="synth")
    h.init()
    sc = Scenario(h, ReferenceEngine(h.cfg))
    for day in range(3):
        sc.feed(f"d{day}", synth_batches(ds, day, params))
    return sc


@pytest.fixture(scope="module")
def customer_scenario(age_server, tmp_path_factory) -> Scenario:
    tmp = tmp_path_factory.mktemp("customer")
    raw = yaml.safe_load(
        (ROOT / "configs" / "example" / "customer.yml").read_text(encoding="utf-8")
    )
    raw["state"]["schema"] = "mdm_customer_t"
    for src in raw["sources"]:
        src["full_snapshot_min_ratio"] = 0.8
    data = Customers(seed=3)
    h = Harness(age_server, raw, tmp, name="cust")
    h.init()
    sc = Scenario(h, ReferenceEngine(h.cfg))
    sc.feed("d0", data.initial())
    sc.feed("d1", data.day())
    sc.feed("d2", data.day(update=0.12, delete=0.06, new=0.08))
    return sc


@pytest.fixture(params=["synth", "customer"], scope="module")
def scenario(request, synth_scenario, customer_scenario) -> Scenario:
    return synth_scenario if request.param == "synth" else customer_scenario


def test_equivalence_with_reference_every_day(scenario: Scenario) -> None:
    assert len(scenario.reports) == 3
    for day, report in enumerate(scenario.reports):
        assert report.ok, f"day {day}: {report.format()}"
    for problems in scenario.problems:
        assert problems == []


def test_scenario_exercises_the_interesting_cases(scenario: Scenario) -> None:
    final = scenario.tables[-1]
    reconciles = scenario.reconciles
    assert sum(r.get("merges", 0) for r in reconciles) > 0
    assert sum(r.get("splits", 0) for r in reconciles) > 0
    assert final["match_pair"].num_rows > 0
    assert final["review"].num_rows > 0
    assert final["reject"].num_rows > 0
    assert any(c["deleted"] > 0 for day in scenario.ingests for c in day.values())
    assert any(c["rejected_records"] > 0 for day in scenario.ingests for c in day.values())
    decisions = set(final["match_pair"]["decision"].to_pylist())
    assert {"match", "review"} <= decisions
    statuses = set(final["golden"]["status"].to_pylist())
    assert "published" in statuses and "retired" in statuses
    kinds = {r["kind"] for r in final["source_delta"].to_pylist()}
    assert kinds == {"I", "U", "D"}


def test_reference_incremental_equals_scratch(scenario: Scenario) -> None:
    scratch = scenario.ref.recompute_from_scratch()
    report = compare_exports(
        scenario.ref.export(), scratch, state_only=True, include_none_pairs=True
    )
    assert report.ok, report.format()


def test_lane_incremental_equals_lane_scratch(scenario: Scenario) -> None:
    h = scenario.h
    cold_cfg = h.cfg.model_copy(deep=True)
    cold_cfg.state.schema_name = f"{h.cfg.state.schema_name}_cold"
    replay_active(h.cfg, h.env[h.state_env], cold_cfg)
    out = h.tmp / f"export_{cold_cfg.state.schema_name}"
    export(cold_cfg, h.env[h.state_env], str(out))
    report = compare_exports(
        scenario.tables[-1], read_parquet_dir(out), state_only=True, include_none_pairs=True
    )
    assert report.ok, report.format()


def test_delta_is_a_small_fraction_of_the_state(scenario: Scenario) -> None:
    total = scenario.tables[-1]["source_record"].num_rows
    day1 = scenario.reconciles[1]
    assert 0 < day1["delta_nodes"] < total * 0.4
    assert day1["candidate_pairs"] < scenario.reconciles[0]["candidate_pairs"]
    assert day1["affected_nodes"] < total
    assert day1["golden_rows_written"] <= day1["stagings"]
    assert day1["survive_member_rows"] <= day1["affected_nodes"]


def test_xmin_proof_untouched_rows_are_not_rewritten(scenario: Scenario) -> None:
    """One source row changes: only its cluster's rows may be rewritten (PostgreSQL xmin)."""
    h = scenario.h

    def snapshot() -> dict[str, dict]:
        g = h.cfg.state.schema_name
        graph = f"{g}_g"
        return {
            "golden": dict(h.sql(f'SELECT golden_id, xmin::text FROM "{g}".golden')),
            "member": dict(h.sql(f'SELECT start_id::text, xmin::text FROM "{graph}"."MEMBER_OF"')),
            "match": dict(h.sql(f'SELECT id::text, xmin::text FROM "{graph}"."MATCH"')),
            "block": dict(h.sql(f'SELECT id::text, xmin::text FROM "{graph}"."IN_BLOCK"')),
            "record": dict(h.sql(f'SELECT id::text, xmin::text FROM "{graph}"."Record"')),
            "source": dict(h.sql(f'SELECT _vid::text, xmin::text FROM "{g}".source_record')),
        }

    before = snapshot()
    src = "crm" if "crm" in h.rows else next(iter(h.rows))
    rows = [dict(r) for r in h.rows[src]]
    cfg_src = h.cfg.source(src)
    victim = next(r for r in rows if r.get(cfg_src.key) is not None)
    name_col = next(
        c for c in h.cfg.mapping(src).values() if "name" in c.lower() or c in {"ln", "nom"}
    )
    victim[name_col] = "zzupdated"
    run = "x_one_change"
    h.load_source(src, rows, replace=True)
    h.ingest(src, run)
    counts = h.reconcile(run)["counts"]
    scenario.ref.ingest(src, h.rows[src], run)
    scenario.ref.reconcile(run)
    after = snapshot()
    assert 1 <= counts["delta_nodes"] <= 1
    assert counts["rescored_nodes"] == 1
    assert counts["affected_nodes"] <= 6
    for table, old in before.items():
        rewritten = sum(1 for k, v in old.items() if after[table].get(k) != v)
        total = max(1, len(old))
        assert rewritten <= max(8, total * 0.02), f"{table}: {rewritten}/{total} rows rewritten"
    report = compare_exports(
        h.export_tables("export_xmin"), scenario.ref.export(), include_none_pairs=True
    )
    assert report.ok, report.format()


def test_graph_structure_is_consistent(scenario: Scenario) -> None:
    from engine.core.reference.matching import block_label
    from engine.lanes.graph.matching import label_of

    h = scenario.h
    s = h.cfg.state.schema_name
    g = f"{s}_g"
    one = lambda query: h.sql(query)[0][0]  # noqa: E731
    vprop = "ag_catalog.agtype_access_operator(properties, '\"v\"'::agtype)"
    for label in ("Record", "Block", "Staging", "Golden"):
        bad = one(
            f'SELECT count(*) FROM "{g}"."{label}"'
            f" WHERE ({vprop})::text::bigint <> id::text::bigint"
        )
        assert bad == 0, label
    assert one(f'SELECT count(*) FROM "{g}"."Record"') == one(
        f'SELECT count(*) FROM "{s}".source_record'
    )
    for label in ("IN_BLOCK", "MATCH", "REVIEW", "MEMBER_OF", "PUBLISHED_AS"):
        dangling = one(
            f'SELECT count(*) FROM "{g}"."{label}" e WHERE NOT EXISTS'
            f' (SELECT 1 FROM "{g}"._ag_label_vertex v WHERE v.id = e.start_id)'
            f' OR NOT EXISTS (SELECT 1 FROM "{g}"._ag_label_vertex v WHERE v.id = e.end_id)'
        )
        assert dangling == 0, label
    assert one(f'SELECT count(*) - count(DISTINCT (start_id, end_id)) FROM "{g}"."MATCH"') == 0
    misoriented = one(
        f'SELECT count(*) FROM "{g}"."MATCH" e'
        f' JOIN "{s}".source_record a ON a._vid = e.start_id::text::bigint'
        f' JOIN "{s}".source_record b ON b._vid = e.end_id::text::bigint'
        ' WHERE a._source_id COLLATE "C" > b._source_id OR (a._source_id = b._source_id'
        ' AND a._source_key COLLATE "C" >= b._source_key)'
    )
    assert misoriented == 0
    active = one(f"SELECT count(*) FROM \"{s}\".source_record WHERE _status = 'active'")
    assert one(f'SELECT count(*) FROM "{g}"."MEMBER_OF"') == active
    assert one(f'SELECT count(DISTINCT start_id) FROM "{g}"."MEMBER_OF"') == active
    assert one(f'SELECT count(*) FROM "{g}"."PUBLISHED_AS"') == one(
        f'SELECT count(*) FROM "{s}".golden'
    )
    assert one(f'SELECT count(*) FROM "{g}"."Staging"') == one(
        f'SELECT count(*) FROM "{s}".staging'
    )
    assert one(f'SELECT count(*) FROM "{s}".pending') == 0
    tables = h.export_tables("export_structure")
    reviews = sum(1 for d in tables["match_pair"]["decision"].to_pylist() if d == "review")
    assert one(f'SELECT count(*) FROM "{g}"."REVIEW"') == reviews == tables["review"].num_rows

    access = "ag_catalog.agtype_access_operator(k.properties, '\"bk\"'::agtype)"
    sizes = dict(
        h.sql(
            f'SELECT ({access})::text, count(*) FROM "{g}"."IN_BLOCK" e JOIN "{g}"."Block" k'
            " ON k.id = e.end_id GROUP BY 1"
        )
    )
    cfg = h.cfg
    expected = {block_label(cfg, *b): len(m) for b, m in scenario.ref._index.items()}
    assert {label_of(bk): n for bk, n in sizes.items()} == expected
    limit = cfg.matching.max_block_size
    flags = h.sql(
        f'SELECT ({access.replace("bk", "o")})::text, count(*) FROM "{g}"."Block" k GROUP BY 1'
    )
    assert {f for f, _ in flags} <= {"true", "false"}
    big = sum(1 for n in expected.values() if n > limit)
    assert sum(n for f, n in flags if f == "true") == big


def test_replay_of_the_same_run_ids_is_a_noop(scenario: Scenario) -> None:
    h = scenario.h
    before = h.export_tables("export_replay_before")
    for src in h.cfg.sources:
        res = h.ingest(src.id, "d0")
        assert res["replayed"] is True and res["counts"]["rows_read"] > 0
    res = h.reconcile("d0")
    assert res["replayed"] is True
    after = h.export_tables("export_replay_after")
    for name, table in before.items():
        assert table.equals(after[name]), name


def test_same_data_with_a_new_run_id_produces_no_delta(scenario: Scenario) -> None:
    h = scenario.h
    for src in h.cfg.sources:
        if src.snapshot == "full":
            counts = h.ingest(src.id, "again")["counts"]
            assert counts["inserted"] == counts["updated"] == counts["deleted"] == 0
    res = h.reconcile("again")
    assert res["counts"]["delta_nodes"] == 0
    assert "edges_MATCH" not in res["counts"]
    assert res["counts"].get("golden_rows_written", 0) == 0
