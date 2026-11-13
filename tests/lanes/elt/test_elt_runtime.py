"""Briques de la voie ELT testables sans PostgreSQL : exécution dbt asynchrone (délai, arbre de
processus), composantes connexes sur de gros clusters, génération du projet (200 attributs),
réglages, extraction du brut."""

from __future__ import annotations

import json
import os
import random
import shutil
import time
from pathlib import Path

import duckdb
import psutil
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from engine.core.config import load_config
from engine.core.synth import SynthParams, build_config
from engine.lanes.elt import dbtrun
from engine.lanes.elt.dbtrun import DbtError, run_dbt
from engine.lanes.elt.extract import extract_raw
from engine.lanes.elt.render import TEMPLATES, render_project
from engine.lanes.elt.settings import (
    ConfigurationError,
    Settings,
    format_memory,
    parse_memory,
    split_dsn,
)
from engine.lanes.elt.sqlgen import Spec


@pytest.fixture
def work() -> Path:
    base = Path(os.environ.get("MDM_TEST_WORKDIR", ".mdm_test_work")).resolve()
    base.mkdir(parents=True, exist_ok=True)
    (base / ".gitignore").write_text("*\n", encoding="utf-8")
    path = base / f"rt-{os.getpid()}-{random.randrange(10**9)}"
    path.mkdir()
    yield path
    shutil.rmtree(path, ignore_errors=True)


def settings(work: Path, **kw) -> Settings:
    base = dict(
        workdir=work,
        threads=2,
        memory_bytes=1024**3,
        dbt_timeout_s=None,
        keep_none_pairs=False,
        fault=None,
    )
    return Settings(**{**base, **kw})


async def test_dbt_timeout_kills_the_whole_process_tree(work, monkeypatch):
    pid_file = work / "child.pid"
    launcher = (
        "import os, subprocess, sys, time;"
        "c = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(120)']);"
        f"open({str(pid_file)!r}, 'w').write(str(c.pid));"
        "time.sleep(120)"
    )
    monkeypatch.setattr(dbtrun, "DBT_LAUNCHER", launcher)
    started = time.perf_counter()
    with pytest.raises(DbtError, match="timed out"):
        await run_dbt(work, work / "w", {}, {}, timeout_s=4)
    assert time.perf_counter() - started < 30
    child = int(pid_file.read_text())
    deadline = time.time() + 10
    while psutil.pid_exists(child) and time.time() < deadline:
        time.sleep(0.2)
    assert not psutil.pid_exists(child), "grandchild survived the timeout"


async def test_dbt_failure_is_reported_with_the_log(work, monkeypatch):
    monkeypatch.setattr(dbtrun, "DBT_LAUNCHER", "print('boom from dbt'); raise SystemExit(3)")
    with pytest.raises(DbtError, match="exit 3") as info:
        await run_dbt(work, work / "w", {}, {}, timeout_s=30)
    assert "boom from dbt" in str(info.value)
    assert (work / "w" / "dbt.out.log").read_text(encoding="utf-8").strip() == "boom from dbt"


def _components_project(root: Path) -> Path:
    project = root / "project"
    (project / "models").mkdir(parents=True)
    shutil.copytree(TEMPLATES / "macros", project / "macros")
    shutil.copy(TEMPLATES / "dbt_project.yml", project / "dbt_project.yml")
    (project / "profiles.yml").write_text(
        "mdm_elt:\n  target: run\n  outputs:\n    run:\n      type: duckdb\n"
        "      path: \"{{ env_var('MDM_DUCKDB_PATH') }}\"\n      threads: 1\n"
        "      settings: {memory_limit: '1GB', threads: 2}\n",
        encoding="utf-8",
    )
    n_chain, n_cliques, n_iso = 60_000, 3_000, 5_000
    (project / "models" / "r_nodes.sql").write_text(
        f"select cast(range + 1 as bigint) as nid from range({n_chain + n_cliques * 4 + n_iso})",
        encoding="utf-8",
    )
    # chaîne de 60 000 nœuds dans un ordre pseudo-aléatoire (diamètre maximal), 3 000 cliques de
    # 4 nœuds et des nœuds isolés : pas d'explosion même sur un très long chemin
    (project / "models" / "r_edges.sql").write_text(
        "with perm as (select cast(range + 1 as bigint) as i,"
        f" cast((range * 40009) % {n_chain} + 1 as bigint) as nid from range({n_chain})),"
        " chain as (select a.nid as a, b.nid as b from perm a join perm b on b.i = a.i + 1),"
        f" cl as (select cast({n_chain} + g * 4 + x as bigint) as a,"
        f" cast({n_chain} + g * 4 + y as bigint) as b"
        f" from range({n_cliques}) t(g), range(1, 5) u(x), range(1, 5) v(y) where x < y),"
        " e as (select a, b from chain union all select a, b from cl)"
        " select a, b from e union all select b, a from e",
        encoding="utf-8",
    )
    (project / "models" / "r_comp.sql").write_text(
        "{{ components(ref('r_nodes'), ref('r_edges'), 'r_comp_lab') }}", encoding="utf-8"
    )
    return project


async def test_connected_components_on_large_clusters(work):
    project = _components_project(work)
    db = work / "cc.duckdb"
    started = time.perf_counter()
    await run_dbt(project, work / "dbt", {"MDM_DUCKDB_PATH": str(db)}, {}, timeout_s=300)
    assert time.perf_counter() - started < 120
    con = duckdb.connect(str(db), read_only=True)
    labels = dict(con.execute("select nid, lab from r_comp").fetchall())
    edges = con.execute("select a, b from r_edges").fetchall()
    nodes = [r[0] for r in con.execute("select nid from r_nodes").fetchall()]
    con.close()
    parent = {n: n for n in nodes}

    def find(x: int) -> int:
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    for a, b in edges:
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[max(ra, rb)] = min(ra, rb)
    assert len(labels) == len(nodes)
    assert all(labels[n] == find(n) for n in nodes)  # label = plus petit nid de la composante
    assert len({labels[n] for n in nodes}) == 1 + 3_000 + 5_000


def test_200_attributes_generate_columnar_models_without_self_joins(work):
    cfg = build_config(SynthParams(nodes=10, attributes=200, sources=3))
    spec = Spec(cfg)
    project = render_project(spec, settings(work), work / "p", "reconcile", {}, work)
    survive = (project / "models" / "r_gold_new.sql").read_text(encoding="utf-8").lower()
    assert survive.count("filter (where") == 400 and " join " not in survive
    assert survive.count("group by m.staging_id") == 1
    golden = (project / "models" / "r_gold_up.sql").read_text(encoding="utf-8").lower()
    assert golden.count(" join ") == 1  # une seule jointure golden nouveau / ancien
    models = {p.name for p in (project / "models").glob("*.sql")}
    assert len(models) > 60
    # déterministe : deux rendus identiques
    again = render_project(spec, settings(work), work / "p2", "reconcile", {}, work)
    for name in models:
        assert (project / "models" / name).read_text() == (again / "models" / name).read_text()


async def test_dbt_plugin_registers_codepoint_levenshtein(work):
    spec = Spec(build_config(SynthParams(nodes=10, attributes=10, sources=3)))
    project = render_project(spec, settings(work), work / "p", "reconcile", {}, work)
    for model in (project / "models").glob("*.sql"):
        model.unlink()
    (project / "models" / "lev.sql").write_text(
        "select mdm_lev_cp('éc', 'd') as d, mdm_lev_cp('a', null) as n,"
        " levenshtein('éc', 'd') as b",
        encoding="utf-8",
    )
    profile = project / "profiles.yml"
    profile.write_text(
        profile.read_text(encoding="utf-8").split("      attach:")[0], encoding="utf-8"
    )
    db = work / "lev.duckdb"
    await run_dbt(project, work / "w", {"MDM_DUCKDB_PATH": str(db)}, {}, timeout_s=120)
    con = duckdb.connect(str(db), read_only=True)
    # code points : 2 ; octets (levenshtein natif de DuckDB) : 3
    assert con.execute("select d, n, b from lev").fetchone() == (2, None, 3)
    con.close()


async def test_generated_projects_parse_with_dbt(work):
    cfg = build_config(SynthParams(nodes=10, attributes=30, sources=3))
    spec = Spec(cfg)
    from engine.lanes.elt.ingest import build_context

    raw_types = {c: "VARCHAR" for s in cfg.sources for c in cfg.mapping(s.id).values()}
    raw_types |= {s.key: "BIGINT" for s in cfg.sources} | {
        s.revision: "TIMESTAMP" for s in cfg.sources
    }
    env = {"MDM_DUCKDB_PATH": str(work / "p.duckdb"), "MDM_PG_CONN": "dbname=x"}
    ctx = build_context(spec, cfg.sources[0], raw_types, "x/*.parquet")
    for kind, context in (("ingest", ctx), ("reconcile", {})):
        project = render_project(spec, settings(work), work / kind, kind, context, work)
        result = await run_dbt(
            project,
            work / f"w-{kind}",
            env,
            {"run_id": "r", "raw_glob": "x/*.parquet"},
            120,
            command="parse",
        )
        assert result.returncode == 0


def test_resource_limits_are_enforced():
    assert parse_memory("4GB") == 4 * 1024**3 and parse_memory("512 MB") == 512 * 1024**2
    assert parse_memory("6GB") == 6 * 1024**3
    for bad in ("7GB", "64GB", "lots", "-1GB", ""):
        with pytest.raises(ConfigurationError):
            parse_memory(bad)
    assert format_memory(parse_memory("1GB")) == "1024MB"
    s = Settings(Path("."), 8, 6 * 1024**3, None, False, None).share(3)
    assert s.threads == 2 and s.memory_bytes == 2 * 1024**3
    os.environ["MDM_ELT_THREADS"] = "32"
    try:
        assert Settings.from_env().threads == 8
    finally:
        del os.environ["MDM_ELT_THREADS"]


def test_dsn_secret_is_split_from_the_connection_string():
    conn, env = split_dsn("postgresql://user:s3cr3t@db.example:5433/mdm?sslmode=require")
    assert "s3cr3t" not in conn and env == {"PGPASSWORD": "s3cr3t"}
    assert "dbname=mdm" in conn and "port=5433" in conn and "sslmode=require" in conn


def test_extract_raw_copies_only_needed_columns_with_an_ordinal(work):
    cfg = load_config(Path("configs/example/customer.yml"))
    web = cfg.source("web")
    data = work / "web"
    data.mkdir()
    pq.write_table(
        pa.table(
            {
                "account_id": [1, 2, 3],
                "event_ts": ["2024-01-01"] * 3,
                "surname": ["a", "b", "c"],
                "given_name": ["x", "y", "z"],
                "email": ["e1", "e2", "e3"],
                "noise": [0, 0, 0],
            }
        ),
        data / "p1.parquet",
    )
    cfg2 = cfg.model_copy(deep=True)
    cfg2.sources[2] = web.model_copy(update={"location": (data / "*.parquet").as_posix()})
    info = extract_raw(cfg2, cfg2.source("web"), settings(work), work / "raw")
    assert info["rows"] == 3 and info["missing_columns"] == []
    cols = set(info["raw_types"])
    assert cols == {"_ord", "account_id", "event_ts", "surname", "given_name", "email"}
    ords = pq.read_table(work / "raw" / "part-00000.parquet")["_ord"].to_pylist()
    assert ords == [0, 1, 2]
    # clé absente : erreur explicite ; aucun fichier : erreur explicite
    cfg3 = cfg2.model_copy(deep=True)
    cfg3.sources[2] = cfg2.source("web").model_copy(update={"key": "nope"})
    with pytest.raises(ConfigurationError, match="nope"):
        extract_raw(cfg3, cfg3.source("web"), settings(work), work / "raw2")
    cfg4 = cfg2.model_copy(deep=True)
    cfg4.sources[2] = web.model_copy(update={"location": (work / "none" / "*.parquet").as_posix()})
    with pytest.raises(ConfigurationError, match="no file"):
        extract_raw(cfg4, cfg4.source("web"), settings(work), work / "raw3")
    _ = json
