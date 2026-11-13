"""Test support for the graph lane: disposable AGE container, source loading, reference feeding."""

from __future__ import annotations

import copy
import json
import os
import secrets
import shutil
import subprocess
import sys
import time
from collections.abc import Iterator
from decimal import Decimal
from pathlib import Path
from typing import Any

import psycopg
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import yaml

from engine.core.config import ReferentialConfig

IMAGE = "apache/age:release_PG16_1.6.0"
PASSWORD = "mdm2-graph-test-only"
ROOT = Path(__file__).resolve().parents[3]


def docker(*args: str, timeout: int = 180) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["docker", *args], capture_output=True, text=True, timeout=timeout, check=False
    )


class AgeServer:
    """A throw-away AGE server container (`mdm2-graph-*`, 6 GB cap) or an external DSN base."""

    def __init__(self, name: str, host: str, port: int) -> None:
        self.name, self.host, self.port = name, host, port

    def dsn(self, db: str) -> str:
        return (
            f"host={self.host} port={self.port} dbname={db} user=mdm password={PASSWORD}"
            " connect_timeout=10"
        )

    def create_database(self, prefix: str) -> str:
        db = f"{prefix}_{secrets.token_hex(3)}"
        with psycopg.connect(self.dsn("postgres"), autocommit=True) as conn:
            conn.execute(f'CREATE DATABASE "{db}"')
        return db


def start_container(extra: list[str] | None = None, tmpfs: str = "1g") -> AgeServer:
    name = f"mdm2-graph-{secrets.token_hex(3)}"
    args = [
        "run",
        "-d",
        "--rm",
        "--pull",
        "never",
        "--name",
        name,
        "--memory",
        "6g",
        "--memory-swap",
        "6g",
        "--shm-size",
        "512m",
        "-e",
        "POSTGRES_USER=mdm",
        "-e",
        f"POSTGRES_PASSWORD={PASSWORD}",
        "-e",
        "POSTGRES_DB=postgres",
        "-p",
        "127.0.0.1::5432",
    ]
    if tmpfs:
        args += ["--tmpfs", f"/var/lib/postgresql/data:rw,size={tmpfs}"]
    args += [
        IMAGE,
        "postgres",
        "-c",
        "shared_buffers=256MB",
        "-c",
        "synchronous_commit=off",
        "-c",
        "fsync=off",
        "-c",
        "max_connections=60",
        *(extra or []),
    ]
    started = docker(*args)
    if started.returncode != 0:
        raise RuntimeError(f"docker run failed: {started.stderr.strip()}")
    try:
        port_line = ""
        for _ in range(30):
            out = docker("port", name, "5432/tcp").stdout.strip()
            if out:
                port_line = out.splitlines()[0]
                break
            time.sleep(0.5)
        port = int(port_line.rsplit(":", 1)[1])
        server = AgeServer(name, "127.0.0.1", port)
        for _ in range(120):
            try:
                psycopg.connect(server.dsn("postgres"), autocommit=True).close()
                return server
            except psycopg.OperationalError:
                time.sleep(0.5)
        raise RuntimeError("AGE container did not become ready")
    except BaseException:
        docker("rm", "-f", name)
        raise


def stop_container(server: AgeServer) -> None:
    docker("rm", "-f", "-v", server.name)


def require_docker() -> None:
    if not os.environ.get("MDM_TEST_DOCKER"):
        pytest.skip("MDM_TEST_DOCKER not set: AGE tests skipped")
    if shutil.which("docker") is None:
        pytest.skip("docker not found")
    try:
        if docker("info", timeout=30).returncode != 0:
            pytest.skip("docker daemon unavailable")
    except subprocess.TimeoutExpired:
        pytest.skip("docker daemon unavailable")
    if docker("image", "inspect", IMAGE).returncode != 0:
        pytest.skip(f"image {IMAGE} not available locally")


PG_TYPES = [
    (pa.types.is_boolean, "boolean"),
    (pa.types.is_integer, "bigint"),
    (pa.types.is_floating, "double precision"),
    (pa.types.is_decimal, "numeric"),
    (pa.types.is_date, "date"),
    (pa.types.is_timestamp, "timestamp"),
]


def pg_type(t: pa.DataType) -> str:
    for test, name in PG_TYPES:
        if test(t):
            return name
    return "text"


class Harness:
    """One referential: config, state DB, source DB, parquet dir, lane runner, reference."""

    def __init__(
        self,
        server: AgeServer,
        raw_cfg: dict[str, Any],
        tmp: Path,
        name: str = "h",
        keep_env: bool = False,
    ) -> None:
        self.server = server
        self.tmp = tmp
        self.state_db = server.create_database(f"st_{name}")
        self.src_db = server.create_database(f"src_{name}")
        raw = copy.deepcopy(raw_cfg)
        self.state_env = raw["state"]["dsn_env"] if keep_env else f"MDM_TEST_STATE_{name.upper()}"
        raw["state"] = {"dsn_env": self.state_env, "schema": raw["state"]["schema"]}
        self.env = {self.state_env: server.dsn(self.state_db)}
        for src in raw["sources"]:
            if src["kind"] == "postgres":
                env = (
                    src["dsn_env"]
                    if keep_env
                    else (f"MDM_TEST_SRC_{name.upper()}_{src['id'].upper()}")
                )
                src["dsn_env"] = env
                self.env[env] = server.dsn(self.src_db)
            elif not keep_env:
                directory = tmp / "parquet" / src["id"]
                directory.mkdir(parents=True, exist_ok=True)
                src["location"] = (directory / "*.parquet").as_posix()
        self.raw = raw
        self.config_path = tmp / f"config_{name}.yml"
        self.config_path.write_text(yaml.safe_dump(raw, sort_keys=False), encoding="utf-8")
        self.cfg = ReferentialConfig.model_validate(raw)
        self._files = 0
        self.rows: dict[str, list[dict]] = {s.id: [] for s in self.cfg.sources}

    # ------------------------------------------------------------------ sources

    def load_source(
        self, source_id: str, rows: list[dict] | pa.Table, replace: bool = True
    ) -> None:
        """Make `rows` the content of the source (full) or one more batch (delta/append)."""
        src = self.cfg.source(source_id)
        table = rows if isinstance(rows, pa.Table) else pa.Table.from_pylist(rows)
        loaded = table.to_pylist()
        self.rows[source_id] = loaded if replace else [*self.rows[source_id], *loaded]
        if src.kind == "parquet":
            directory = Path(src.location).parent
            if replace:
                for old in directory.glob("*.parquet"):
                    old.unlink()
            self._files += 1
            pq.write_table(table, directory / f"part-{self._files:05d}.parquet")
            return
        schema, name = src.location.split(".")
        cols = ", ".join(f'"{f.name}" {pg_type(f.type)}' for f in table.schema)
        with psycopg.connect(self.env[src.dsn_env], autocommit=True) as conn:
            conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
            if replace:
                conn.execute(f'DROP TABLE IF EXISTS "{schema}"."{name}"')
            conn.execute(f'CREATE TABLE IF NOT EXISTS "{schema}"."{name}" ({cols})')
            names = ", ".join(f'"{n}"' for n in table.column_names)
            with (
                conn.cursor() as cur,
                cur.copy(f'COPY "{schema}"."{name}" ({names}) FROM STDIN') as cp,
            ):
                for row in zip(*[c.to_pylist() for c in table.columns], strict=True):
                    cp.write_row(row)

    # --------------------------------------------------------------------- lane

    def environ(self) -> dict[str, str]:
        return {**os.environ, **self.env, "PYTHONDONTWRITEBYTECODE": "1"}

    def cli(
        self,
        *args: str,
        env: dict[str, str] | None = None,
        check: bool = True,
        config: Path | None = None,
    ) -> dict:
        """Run the lane CLI in a subprocess (what an orchestrator does)."""
        proc = subprocess.run(
            [
                sys.executable,
                "-B",
                "-m",
                "engine.lanes.graph",
                *args,
                "--config",
                str(config or self.config_path),
            ],
            capture_output=True,
            text=True,
            cwd=ROOT,
            env={**self.environ(), **(env or {})},
            timeout=600,
            check=False,
        )
        if check and proc.returncode != 0:
            raise AssertionError(f"lane failed ({proc.returncode}): {proc.stdout}\n{proc.stderr}")
        lines = [ln for ln in proc.stdout.splitlines() if ln.strip()]
        out = json.loads(lines[-1]) if lines else {}
        out["_returncode"] = proc.returncode
        out["_stdout_lines"] = len(lines)
        return out

    def run_inproc(self, command: str, *args: str) -> dict:
        from engine.lanes.graph.cli import run

        old = {k: os.environ.get(k) for k in self.env}
        os.environ.update(self.env)
        try:
            return run([command, "--config", str(self.config_path), *args])
        finally:
            for k, v in old.items():
                if v is None:
                    os.environ.pop(k, None)
                else:
                    os.environ[k] = v

    def init(self) -> dict:
        return self.run_inproc("init")

    def ingest(self, source_id: str, run_id: str) -> dict:
        return self.run_inproc("ingest", "--source", source_id, "--run-id", run_id)

    def reconcile(self, run_id: str, *extra: str) -> dict:
        return self.run_inproc("reconcile", "--run-id", run_id, *extra)

    def export_tables(self, name: str = "export") -> dict[str, pa.Table]:
        from engine.core.canonical import read_parquet_dir

        out = self.tmp / name
        shutil.rmtree(out, ignore_errors=True)
        self.run_inproc("export", "--out", str(out))
        return read_parquet_dir(out)

    def sql(self, query: str, params: tuple = ()) -> list[tuple]:
        from engine.lanes.graph.db import connect

        conn = connect(self.env[self.state_env])
        try:
            return conn.execute(query, params).fetchall()
        finally:
            conn.close()


def dec(value: str) -> Decimal:
    return Decimal(value)


@pytest.fixture(scope="session")
def age_server() -> Iterator[AgeServer]:
    require_docker()
    server = start_container()
    try:
        yield server
    finally:
        stop_container(server)
