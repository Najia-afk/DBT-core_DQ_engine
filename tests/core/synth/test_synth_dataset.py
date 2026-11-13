from __future__ import annotations

import hashlib
import re
from pathlib import Path

import duckdb
import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest

from engine.core.config import ReferentialConfig, load_config
from engine.core.synth import (
    DeltaParams,
    SynthParams,
    generate,
    generate_delta,
    load_manifest,
    stage_live,
)
from engine.core.synth.cli import main as cli_main

SMALL = {
    "nodes": 3000,
    "attributes": 30,
    "sources": 3,
    "seed": 7,
    "workers": 1,
    "kinds": ("postgres", "parquet", "parquet"),
    "snapshots": ("full", "full", "delta"),
}
DELTA = DeltaParams(update_pct=4, new_pct=2, delete_pct=1, broken_pct=1)


@pytest.fixture(scope="module")
def small_params() -> SynthParams:
    return SynthParams(**SMALL)


@pytest.fixture(scope="module")
def small_dataset(tmp_path_factory: pytest.TempPathFactory, small_params: SynthParams) -> Path:
    out = tmp_path_factory.mktemp("synth") / "ds"
    generate(small_params, out, days=2, delta=DELTA)
    return out


def read_source(ds: Path, day: int, source: str) -> pa.Table:
    files = sorted((ds / f"day{day}" / source).glob("part-*.parquet"))
    return pa.concat_tables([pq.read_table(f) for f in files])


def digest(root: Path) -> dict[str, str]:
    out = {}
    for f in sorted(root.rglob("*")):
        if f.is_file() and f.name != "manifest.json" and f.suffix != ".yml":
            out[f.relative_to(root).as_posix()] = hashlib.sha256(f.read_bytes()).hexdigest()
    return out


def test_layout_and_manifest(small_dataset, small_params):
    ds = small_dataset
    cfg = load_config(ds / "config.yml")
    manifest = load_manifest(ds)
    assert set(manifest["days"]) == {"0", "1", "2"}
    for day in range(3):
        for s in cfg.sources:
            assert list((ds / f"day{day}" / s.id).glob("part-*.parquet"))
        assert (ds / "truth" / f"day{day}_entities.parquet").exists()
        assert (ds / "truth" / f"day{day}_violations.parquet").exists()
    assert not (ds / "truth" / "day0_changes.parquet").exists()
    assert (ds / "truth" / "day1_changes.parquet").exists()
    assert manifest["params"]["seed"] == small_params.seed
    # parquet : location pointe vers live/<source>/
    web = cfg.source("web")
    assert web.kind == "parquet" and web.location.endswith("/live/web/*.parquet")


def test_source_columns_follow_config_mapping(small_dataset):
    cfg = load_config(small_dataset / "config.yml")
    for day in (0, 1):
        for s in cfg.sources:
            table = read_source(small_dataset, day, s.id)
            mapping = cfg.mapping(s.id)
            assert table.column_names == [s.key, s.revision, *mapping.values()]
            assert not set(mapping) & set(table.column_names)  # aucun nom cible
            types = {a.name: a.type for a in cfg.attributes}
            for name, col in mapping.items():
                t = table.schema.field(col).type
                expected = {
                    "string": pa.types.is_string,
                    "int": pa.types.is_int64,
                    "decimal": pa.types.is_decimal,
                    "date": pa.types.is_date32,
                    "timestamp": pa.types.is_timestamp,
                    "bool": pa.types.is_boolean,
                }[types[name]]
                assert expected(t), (name, t)
            assert len(set(table.column(s.key).to_pylist())) == table.num_rows


def test_truth_is_separate_and_complete(small_dataset):
    ds = small_dataset
    cfg = load_config(ds / "config.yml")
    truth = pq.read_table(ds / "truth" / "day0_entities.parquet")
    assert truth.column_names == ["source_id", "source_key", "entity_id", "alive"]
    for s in cfg.sources:
        table = read_source(ds, 0, s.id)
        assert not any("entit" in c.lower() or "truth" in c.lower() for c in table.column_names)
        mine = truth.filter(pc.equal(truth["source_id"], s.id))
        keys = pc.cast(table.column(s.key), pa.string())
        assert set(mine["source_key"].to_pylist()) == set(keys.to_pylist())
    # chaque entité observée dans 1..S sources, doublons intra-source présents
    con = duckdb.connect()
    con.register("t", truth)
    per_entity = con.execute(
        "select count(distinct source_id) c from t group by entity_id"
    ).fetchall()
    counts = {c for (c,) in per_entity}
    assert counts == {1, 2, 3}
    assert len(per_entity) == SMALL["nodes"]
    dups = con.execute(
        "select count(*) from (select source_id, entity_id from t group by all having count(*) > 1)"
    ).fetchall()[0][0]
    assert dups > 0


def test_day0_rates_nulls_and_nullability(small_dataset, small_params):
    cfg = load_config(small_dataset / "config.yml")
    for s in cfg.sources:
        table = read_source(small_dataset, 0, s.id)
        rows = table.num_rows
        for a in cfg.attributes:
            if a.name not in cfg.mapping(s.id):
                continue
            nulls = table.column(cfg.mapping(s.id)[a.name]).null_count
            if not a.nullable:
                assert nulls == 0, a.name
        assert table.column(cfg.mapping(s.id)["email"]).null_count > 0
        assert rows > SMALL["nodes"] * 0.5


def test_determinism_same_seed_same_bytes(tmp_path, small_dataset, small_params):
    again = tmp_path / "again"
    generate(small_params, again, days=2, delta=DELTA)
    assert digest(again) == digest(small_dataset)
    other = tmp_path / "other"
    generate(SynthParams(**{**SMALL, "seed": 8}), other, days=1, delta=DELTA)
    assert (
        digest(other)["day0/crm/part-00000.parquet"]
        != digest(small_dataset)["day0/crm/part-00000.parquet"]
    )


def test_determinism_independent_of_workers_and_part_size(tmp_path, small_dataset):
    parallel = tmp_path / "par"
    generate(SynthParams(**{**SMALL, "workers": 2}), parallel, days=1, delta=DELTA)
    con = duckdb.connect()
    for s in ("crm", "erp", "web"):
        for day in (0, 1):
            a = f"{small_dataset.as_posix()}/day{day}/{s}/*.parquet"
            b = f"{parallel.as_posix()}/day{day}/{s}/*.parquet"
            q = f"(select * from read_parquet('{a}') except select * from read_parquet('{b}'))"
            assert con.execute(f"select count(*) from {q}").fetchall()[0][0] == 0
            q = f"(select * from read_parquet('{b}') except select * from read_parquet('{a}'))"
            assert con.execute(f"select count(*) from {q}").fetchall()[0][0] == 0
    split = tmp_path / "split"
    generate(SynthParams(**{**SMALL, "part_rows": 400}), split, days=1, delta=DELTA)
    assert len(list((split / "day0" / "crm").glob("*.parquet"))) > 3
    a = f"{small_dataset.as_posix()}/day1/crm/*.parquet"
    b = f"{split.as_posix()}/day1/crm/*.parquet"
    q = f"(select * from read_parquet('{a}') except select * from read_parquet('{b}'))"
    assert con.execute(f"select count(*) from {q}").fetchall()[0][0] == 0


def test_delta_percentages_are_respected(small_dataset):
    manifest = load_manifest(small_dataset)
    for day in ("1", "2"):
        for sid, info in manifest["days"][day]["sources"].items():
            c = info["changes"]
            prev = c["previous_active"]
            snapshot = next(s for s in manifest["sources"] if s["id"] == sid)["snapshot"]
            assert c["insert"] == round(prev * DELTA.new_pct / 100)
            upd = c["update"] + c["merge"] + c["split"]
            assert upd == round(prev * DELTA.update_pct / 100)
            assert c["broken"] == round(prev * DELTA.broken_pct / 100)
            if snapshot == "full":
                assert c["delete"] == round(prev * DELTA.delete_pct / 100)
                assert info["rows"] == c["active"]
            else:
                assert c["delete"] == 0
                assert info["rows"] == c["insert"] + upd + c["broken"]
            assert c["active"] == prev + c["insert"] - c["delete"]
            assert c["merge"] <= round(upd * DELTA.merge_pct / 100)
            assert c["split"] <= round(upd * DELTA.split_pct / 100)
            assert c["merge"] + c["split"] > 0 or day == "2"
    changes = pq.read_table(small_dataset / "truth" / "day1_changes.parquet")
    con = duckdb.connect()
    con.register("changes", changes)
    counted = con.execute("select change, count(*) n from changes group by 1").fetchall()
    total = {
        k: sum(manifest["days"]["1"]["sources"][s]["changes"][k] for s in ("crm", "erp", "web"))
        for k in ("insert", "update", "merge", "split", "delete", "broken")
    }
    assert dict(counted) == {k: v for k, v in total.items() if v}


def test_unchanged_rows_are_identical_and_changed_rows_move(small_dataset):
    ds = small_dataset
    cfg = load_config(ds / "config.yml")
    changes = pq.read_table(ds / "truth" / "day1_changes.parquet")
    con = duckdb.connect()
    for s in ("crm", "erp"):  # snapshots full
        src = cfg.source(s)
        changed = changes.filter(pc.equal(changes["source_id"], s))
        keys = pc.cast(changed["source_key"], pa.string())
        d0, d1 = read_source(ds, 0, s), read_source(ds, 1, s)
        key0 = pc.cast(d0[src.key], pa.string())
        key1 = pc.cast(d1[src.key], pa.string())
        stable0 = d0.filter(pc.invert(pc.is_in(key0, value_set=keys)))
        stable1 = d1.filter(pc.invert(pc.is_in(key1, value_set=keys)))
        assert stable0.num_rows > 0.8 * d0.num_rows
        con.register("a", stable0)
        con.register("b", stable1)
        assert stable0.num_rows == stable1.num_rows
        assert (
            con.execute("select count(*) from (select * from a except select * from b)").fetchall()[
                0
            ][0]
            == 0
        )
        assert (
            con.execute("select count(*) from (select * from b except select * from a)").fetchall()[
                0
            ][0]
            == 0
        )
        # lignes mises à jour : révision strictement plus grande et valeurs différentes
        upd = changed.filter(pc.equal(changed["change"], "update"))["source_key"]
        u0 = d0.filter(pc.is_in(key0, value_set=upd))
        u1 = d1.filter(pc.is_in(key1, value_set=upd))
        assert u0.num_rows == u1.num_rows == len(upd)
        con.register("u0", u0.sort_by(src.key))
        con.register("u1", u1.sort_by(src.key))
        newer = con.execute(
            f"select count(*) from u0 join u1 using ({src.key}) "
            f"where u1.{src.revision} > u0.{src.revision}"
        ).fetchall()[0][0]
        assert newer == u0.num_rows
        differ = con.execute(
            "select count(*) from (select * from u1 except select * from u0)"
        ).fetchall()[0][0]
        assert differ >= 0.95 * u0.num_rows


def test_delta_source_only_contains_changed_rows(small_dataset):
    ds = small_dataset
    web1 = read_source(ds, 1, "web")
    changes = pq.read_table(ds / "truth" / "day1_changes.parquet")
    mine = changes.filter(pc.equal(changes["source_id"], "web"))
    expected = {
        k
        for k, c in zip(mine["source_key"].to_pylist(), mine["change"].to_pylist(), strict=True)
        if c != "delete"
    }
    got = {str(k) if not isinstance(k, str) else k for k in web1["account_id"].to_pylist()}
    assert got == expected
    # un delta ne touche pas les lignes inchangées : leurs clés absentes
    assert web1.num_rows < 0.2 * read_source(ds, 0, "web").num_rows


def test_deleted_rows_are_absent_from_full_snapshot_and_marked_dead(small_dataset):
    ds = small_dataset
    changes = pq.read_table(ds / "truth" / "day1_changes.parquet")
    deleted = changes.filter(pc.equal(changes["change"], "delete"))
    assert deleted.num_rows > 0
    truth = pq.read_table(ds / "truth" / "day1_entities.parquet")
    dead = truth.filter(pc.invert(truth["alive"]))
    assert set(dead["source_key"].to_pylist()) >= set(deleted["source_key"].to_pylist())
    for sid, col in (("crm", "customer_id"), ("erp", "client_no")):
        gone = set(deleted.filter(pc.equal(deleted["source_id"], sid))["source_key"].to_pylist())
        keys = {str(k) for k in read_source(ds, 1, sid)[col].to_pylist()}
        assert not gone & keys


def test_merge_and_split_change_truth_entities(small_dataset):
    ds = small_dataset
    ch = pq.read_table(ds / "truth" / "day1_changes.parquet")
    e0 = pq.read_table(ds / "truth" / "day0_entities.parquet")
    e1 = pq.read_table(ds / "truth" / "day1_entities.parquet")
    con = duckdb.connect()
    con.register("ch", ch)
    con.register("e0", e0)
    con.register("e1", e1)
    for kind in ("merge", "split"):
        rows = con.execute(
            "select e0.entity_id, e1.entity_id, ch.source_id, ch.source_key from ch "
            "join e0 using (source_id, source_key) join e1 using (source_id, source_key) "
            f"where ch.change = '{kind}'"
        ).fetchall()
        assert rows, kind
        assert all(before != after for before, after, *_ in rows)
    max_before = con.execute("select max(entity_id) from e0").fetchall()[0][0]
    split_after = con.execute(
        "select e1.entity_id from ch join e1 using (source_id, source_key) where change='split'"
    ).fetchall()
    assert all(e > max_before for (e,) in split_after)
    # une scission sépare une ligne d'une entité qui en gardait au moins une autre
    left = con.execute(
        "select count(*) from ch join e0 using (source_id, source_key) where change='split' and "
        "e0.entity_id in (select entity_id from e0 where alive group by 1 having count(*) > 1)"
    ).fetchall()[0][0]
    assert left == len(split_after)
    merged = con.execute(
        "select e1.entity_id from ch join e1 using (source_id, source_key) where change='merge'"
    ).fetchall()
    existing = {e for (e,) in con.execute("select entity_id from e0").fetchall()}
    assert all(e in existing for (e,) in merged)


CHECKS = {
    "regex": lambda col, v: pc.invert(pc.match_substring_regex(col, v)),
    "max_length": lambda col, v: pc.greater(pc.utf8_length(col), v),
    "in_set": lambda col, v: pc.invert(pc.is_in(col, value_set=pa.array(v, pa.string()))),
    "min": lambda col, v: pc.less(pc.cast(col, pa.float64()), v),
    "max": lambda col, v: pc.greater(pc.cast(col, pa.float64()), v),
}


def independent_violations(cfg: ReferentialConfig, table: pa.Table, source: str) -> set[tuple]:
    src = cfg.source(source)
    keys = pc.cast(table[src.key], pa.string()).to_pylist()
    found = set()
    for attr in cfg.attributes:
        col_name = cfg.mapping(source).get(attr.name)
        if col_name is None:
            continue
        col = table[col_name]
        for chk in attr.checks:
            bad = pc.fill_null(CHECKS[chk.rule](col, chk.value), False).to_pylist()
            found |= {(k, attr.name, chk.rule) for k, b in zip(keys, bad, strict=True) if b}
    return found


@pytest.mark.parametrize("day", [0, 1, 2])
def test_dq_violations_match_the_oracle(small_dataset, day):
    cfg = load_config(small_dataset / "config.yml")
    oracle = pq.read_table(small_dataset / "truth" / f"day{day}_violations.parquet")
    expected = {
        (r["source_key"], r["attribute"], r["rule"], r["source_id"]) for r in oracle.to_pylist()
    }
    assert expected, "le jeu doit contenir des violations"
    found = set()
    for s in cfg.sources:
        table = read_source(small_dataset, day, s.id)
        found |= {(*v, s.id) for v in independent_violations(cfg, table, s.id)}
    assert found == expected
    fails = {r["on_fail"] for r in oracle.to_pylist()}
    assert "reject_record" in fails and len(fails) >= 2


def test_broken_rows_carry_a_reject_record_violation(small_dataset):
    ch = pq.read_table(small_dataset / "truth" / "day1_changes.parquet")
    viol = pq.read_table(small_dataset / "truth" / "day1_violations.parquet")
    broken = {
        (s, k)
        for s, k, c in zip(
            ch["source_id"].to_pylist(),
            ch["source_key"].to_pylist(),
            ch["change"].to_pylist(),
            strict=True,
        )
        if c == "broken"
    }
    rejected = {
        (r["source_id"], r["source_key"])
        for r in viol.to_pylist()
        if r["on_fail"] == "reject_record"
    }
    assert broken and broken <= rejected


def test_conflicts_between_sources_and_identity_noise(small_dataset):
    ds = small_dataset
    cfg = load_config(ds / "config.yml")
    truth = pq.read_table(ds / "truth" / "day0_entities.parquet")
    con = duckdb.connect()
    for s in ("crm", "erp"):
        t = read_source(ds, 0, s)
        t = t.append_column("__key", pc.cast(t[cfg.source(s).key], pa.string()))
        con.register(f"t_{s}", t)
    con.register("truth", truth)
    ln_crm, ln_erp = cfg.mapping("crm")["last_name"], cfg.mapping("erp")["last_name"]
    rows = con.execute(
        f'select c."{ln_crm}", e."{ln_erp}" from t_crm c '
        "join truth tc on tc.source_id='crm' and tc.source_key = c.__key "
        "join truth te on te.source_id='erp' and te.entity_id = tc.entity_id "
        "join t_erp e on te.source_key = e.__key"
    ).fetchall()
    assert len(rows) > 500
    same = sum(1 for a, b in rows if a == b)
    assert 0.6 < same / len(rows) < 1.0  # accord majoritaire, bruit minoritaire
    pattern = re.compile(r"^\s|\s$")
    assert any(pattern.search(a or "") for a, _ in rows) or any(
        pattern.search(b or "") for _, b in rows
    )


def test_stage_live_for_parquet_sources(tmp_path, small_dataset):
    ds = tmp_path / "ds"
    generate(SynthParams(**SMALL), ds, days=2, delta=DELTA)
    stage_live(ds, 0)
    stage_live(ds, 1)
    erp = list((ds / "live" / "erp").glob("*.parquet"))
    web = list((ds / "live" / "web").glob("*.parquet"))
    assert {f.name.split("-")[0] for f in erp} == {"day1"}  # full : remplacé
    assert {f.name.split("-")[0] for f in web} == {"day0", "day1"}  # delta : cumulé
    assert not (ds / "live" / "crm").exists()  # source postgres


def test_next_day_extends_a_dataset_and_rejects_gaps(tmp_path):
    ds = tmp_path / "ds"
    generate(SynthParams(**{**SMALL, "nodes": 800}), ds, days=1, delta=DELTA)
    info = generate_delta(ds, None, DeltaParams(update_pct=10))
    assert info["day"] == 2
    with pytest.raises(ValueError):
        generate_delta(ds, 5, DeltaParams())
    with pytest.raises(FileExistsError):
        generate(SynthParams(**{**SMALL, "nodes": 800}), ds)


def test_cli_generate_and_config(tmp_path, capsys):
    out = tmp_path / "cli"
    rc = cli_main(
        ["generate", "--nodes", "500", "--attributes", "20", "--sources", "2", "--seed", "3",
         "--out", str(out), "--days", "1", "--update-pct", "5", "--workers", "1",
         "--kinds", "parquet,postgres"]
    )  # fmt: skip
    assert rc == 0
    import json

    summary = json.loads(capsys.readouterr().out)
    assert len(summary["days"]) == 2 and summary["bytes"] > 0 and summary["seconds"] > 0
    cfg = load_config(out / "config.yml")
    assert [s.kind for s in cfg.sources] == ["parquet", "postgres"]
    assert cli_main(["config", "--attributes", "12", "--out", str(tmp_path / "x.yml")]) == 0
    assert load_config(tmp_path / "x.yml").attributes[0].name == "last_name"


def test_numeric_columns_hold_values(small_dataset):
    table = read_source(small_dataset, 0, "crm")
    np_ok = False
    for field in table.schema:
        if pa.types.is_decimal(field.type):
            vals = table[field.name].drop_null()
            assert pc.min(vals).as_py() is not None
            np_ok = True
        if pa.types.is_boolean(field.type):
            assert table[field.name].drop_null().unique().to_pylist()
    assert np_ok
    ints = [f.name for f in table.schema if pa.types.is_int64(f.type)]
    assert np.all([table[c].null_count < table.num_rows for c in ints])
