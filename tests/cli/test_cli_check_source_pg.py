"""check-source contre un vrai PostgreSQL éphémère (Docker) : MDM_TEST_DOCKER=1 requis."""

from __future__ import annotations

import os
import secrets
import shutil
import subprocess
import time
from collections.abc import Iterator

import pyarrow.parquet as pq
import pytest
import yaml

from engine.cli import checksource
from engine.cli.sqlbackends import PgBackend
from engine.core.config import ReferentialConfig

IMAGE = "postgres:16.4-bookworm"
PASSWORD = "mdm2-cli-test"


def _docker(*args: str, timeout: int = 120) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["docker", *args], capture_output=True, text=True, timeout=timeout, check=False
    )


@pytest.fixture(scope="module")
def pg_dsn() -> Iterator[str]:
    if os.environ.get("MDM_TEST_DOCKER") != "1":
        pytest.skip("set MDM_TEST_DOCKER=1 to run the PostgreSQL check-source tests (Docker)")
    if shutil.which("docker") is None or _docker("info", timeout=30).returncode != 0:
        pytest.skip("docker is not available")
    import psycopg

    name = f"mdm2-cli-pg-{secrets.token_hex(4)}"
    started = _docker(
        "run", "-d", "--name", name, "--memory", "3g", "--memory-swap", "3g",
        "-e", f"POSTGRES_PASSWORD={PASSWORD}", "-p", "127.0.0.1::5432", IMAGE,
        timeout=600,
    )  # fmt: skip
    if started.returncode != 0:
        pytest.skip(f"cannot start {IMAGE}: {started.stderr.strip()[:200]}")
    try:
        port = int(
            _docker("port", name, "5432/tcp").stdout.strip().splitlines()[0].rsplit(":", 1)[1]
        )
        dsn = f"host=127.0.0.1 port={port} dbname=postgres user=postgres password={PASSWORD}"
        deadline, streak = time.monotonic() + 90, 0
        while time.monotonic() < deadline and streak < 3:
            try:
                with psycopg.connect(dsn, connect_timeout=2) as con:
                    con.execute("SELECT 1")
                streak += 1
            except psycopg.Error:
                streak = 0
            time.sleep(0.5)
        assert streak == 3, "postgres did not start"
        yield dsn
    finally:
        _docker("rm", "-f", name)


def _load_crm(dsn: str, data_dir) -> None:
    import psycopg

    table = pq.read_table(data_dir / "crm.parquet")
    with psycopg.connect(dsn, autocommit=True) as con:
        con.execute(
            'CREATE TABLE public.crm (customer_id int, updated_at timestamp, "LASTNAME" text, '
            '"EMAIL" text, "BIRTHDATE" text)'
        )
        with con.cursor() as cur:
            cur.executemany(
                "INSERT INTO public.crm VALUES (%s, %s, %s, %s, %s)",
                list(zip(*(table.column(c).to_pylist() for c in table.column_names), strict=True)),
            )


def test_postgres_report_matches_parquet(cli_json, config_path, data_dir, pg_dsn, monkeypatch):
    _, parquet = cli_json("check-source", config_path, "--source", "crm")
    _load_crm(pg_dsn, data_dir)
    monkeypatch.setenv("MDM_TEST_PG_DSN", pg_dsn)
    raw = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    raw["sources"][0].update(kind="postgres", location="public.crm", dsn_env="MDM_TEST_PG_DSN")
    config_path.write_text(yaml.safe_dump(raw, sort_keys=False), encoding="utf-8")
    code, pg = cli_json("check-source", config_path, "--source", "crm")
    assert code == 0 and pg["verdict"] == parquet["verdict"] == "WARN", pg["findings"]
    assert pg["distinct_approximate"] is False
    for field in ("rows", "key", "rows_rejected_by_checks"):
        assert pg[field] == parquet[field], field
    for a, b in zip(pg["attributes"], parquet["attributes"], strict=True):
        for field in ("null_count", "cast_failures", "checks"):
            assert a[field] == b[field], (a["attribute"], field)
        for field in ("distinct_before", "distinct_after"):  # DuckDB : estimation HLL (~2 %)
            if a[field] is not None:
                assert b[field] == pytest.approx(a[field], rel=0.05), (a["attribute"], field)


def test_wide_postgres_source_is_a_single_query(pg_dsn, monkeypatch):
    import psycopg

    n_attrs, rows = 200, 2000
    cols = ", ".join(f"c{k} text" for k in range(n_attrs))
    values = ", ".join("' V' || (i % 7) || ' '" for _ in range(n_attrs))
    with psycopg.connect(pg_dsn, autocommit=True) as con:
        con.execute(f"CREATE TABLE public.wide (id int, {cols})")
        con.execute(f"INSERT INTO public.wide SELECT i, {values} FROM generate_series(1, {rows}) i")
    monkeypatch.setenv("MDM_TEST_PG_DSN", pg_dsn)
    attrs = [
        {
            "name": f"a{k}", "normalize": ["trim", "lower"],
            "sources": [{"source": "s", "column": f"c{k}"}],
            "checks": [{"rule": "max_length", "value": 3, "on_fail": "warn"}],
        }
        for k in range(n_attrs)
    ]  # fmt: skip
    cfg = ReferentialConfig.model_validate({
        "referential": "w", "version": 1, "state": {"dsn_env": "X", "schema": "w"},
        "sources": [{"id": "s", "kind": "postgres", "location": "public.wide", "key": "id",
                     "dsn_env": "MDM_TEST_PG_DSN"}],
        "attributes": attrs,
        "matching": {"version": "1", "blocking": [{"name": "b", "parts": [{"attr": "a0"}]}],
                     "comparators": [{"attr": "a0", "method": "exact", "weight": 1}]},
    })  # fmt: skip
    backend = PgBackend("public.wide", "MDM_TEST_PG_DSN")
    calls = []
    original = backend.row
    backend.row = lambda sql: calls.append(sql) or original(sql)
    try:
        report = checksource.check_source(cfg, "s", backend=backend)
    finally:
        backend.close()
    assert report["rows"] == rows and len(report["attributes"]) == n_attrs, report["findings"]
    assert len(calls) == 1
    assert report["attributes"][0]["distinct_before"] == 7
