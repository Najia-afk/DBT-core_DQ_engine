"""Fixtures: a throw-away PostgreSQL (Docker, MDM_TEST_DOCKER=1) and one database per test.

`MDM_ETL_TEST_ADMIN_DSN` (libpq DSN of a superuser on an existing server) skips the container
start, for local iterations; the container otherwise is `mdm2-etl-pg-<hex>` (6 GB, no swap) and
is removed at the end of the session.
"""

from __future__ import annotations

import os
import secrets
import shutil
import subprocess
import time
from collections.abc import Iterator

import psycopg
import pytest

IMAGE = "postgres:16.4-bookworm"
PASSWORD = "mdm2-etl-test"


def _docker(*args: str, timeout: int = 120) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["docker", *args], capture_output=True, text=True, timeout=timeout, check=False
    )


def _dsn(port: int, dbname: str) -> str:
    return f"host=127.0.0.1 port={port} dbname={dbname} user=postgres password={PASSWORD}"


@pytest.fixture(scope="session")
def pg_admin_dsn() -> Iterator[str]:
    external = os.environ.get("MDM_ETL_TEST_ADMIN_DSN")
    if external:
        yield external
        return
    if not os.environ.get("MDM_TEST_DOCKER"):
        pytest.skip("MDM_TEST_DOCKER non défini : tests PostgreSQL ignorés")
    if shutil.which("docker") is None:
        pytest.skip("docker introuvable")
    try:
        if _docker("info", timeout=30).returncode != 0:
            pytest.skip("démon Docker indisponible")
    except subprocess.TimeoutExpired:
        pytest.skip("démon Docker indisponible")
    name = f"mdm2-etl-pg-{secrets.token_hex(4)}"
    started = _docker(
        "run", "-d", "--name", name, "--memory", "6g", "--memory-swap", "6g", "--shm-size", "512m",
        "-e", f"POSTGRES_PASSWORD={PASSWORD}", "-p", "127.0.0.1::5432", IMAGE,
        "-c", "max_connections=100", "-c", "fsync=off", "-c", "synchronous_commit=off",
        "-c", "shared_buffers=256MB", "-c", "work_mem=32MB", "-c", "max_wal_size=2GB",
        timeout=600,
    )  # fmt: skip
    if started.returncode != 0:
        pytest.skip(f"impossible de démarrer {IMAGE}: {started.stderr.strip()[:200]}")
    try:
        mapping = _docker("port", name, "5432/tcp").stdout.strip().splitlines()[0]
        port = int(mapping.rsplit(":", 1)[1])
        deadline = time.monotonic() + 90
        streak = 0
        while time.monotonic() < deadline and streak < 3:
            try:
                with psycopg.connect(_dsn(port, "postgres"), connect_timeout=2) as conn:
                    conn.execute("SELECT 1")
                streak += 1
            except psycopg.Error:
                streak = 0
            time.sleep(0.5)
        if streak < 3:
            pytest.fail("postgres ne démarre pas")
        yield _dsn(port, "postgres")
    finally:
        _docker("rm", "-f", "-v", name)  # -v: also drops the anonymous data volume


@pytest.fixture
def pg_db(pg_admin_dsn: str) -> Iterator[str]:
    """DSN of an empty database private to the test (dropped afterwards)."""
    name = f"etl_{secrets.token_hex(4)}"
    with psycopg.connect(pg_admin_dsn, autocommit=True) as admin:
        admin.execute(f'CREATE DATABASE "{name}"')
    dsn = psycopg.conninfo.make_conninfo(pg_admin_dsn, dbname=name)
    try:
        yield dsn
    finally:
        with psycopg.connect(pg_admin_dsn, autocommit=True) as admin:
            admin.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
