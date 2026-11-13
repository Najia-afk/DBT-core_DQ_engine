"""Fixtures : PostgreSQL jetable (Docker) pour les tests du runtime."""

from __future__ import annotations

import os
import secrets
import shutil
import subprocess
import time
from collections.abc import Iterator
from pathlib import Path

import psycopg
import pytest

IMAGE = "postgres:16.4-bookworm"
PASSWORD = "mdm2-runtime-test"


def _docker(*args: str, timeout: int = 120) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["docker", *args], capture_output=True, text=True, timeout=timeout, check=False
    )


@pytest.fixture(scope="session")
def pg_server() -> Iterator[tuple[str, int]]:
    """Démarre un conteneur postgres sur un port libre aléatoire ; le supprime en fin de session."""
    if not os.environ.get("MDM_TEST_DOCKER"):
        pytest.skip("MDM_TEST_DOCKER non défini : tests PostgreSQL ignorés")
    if shutil.which("docker") is None:
        pytest.skip("docker introuvable")
    try:
        if _docker("info", timeout=30).returncode != 0:
            pytest.skip("démon Docker indisponible")
    except subprocess.TimeoutExpired:
        pytest.skip("démon Docker indisponible")

    name = f"mdm2-runtime-pg-{secrets.token_hex(4)}"
    started = _docker(
        "run",
        "-d",
        "--name",
        name,
        "--memory",
        "3g",
        "--memory-swap",
        "3g",
        "-e",
        f"POSTGRES_PASSWORD={PASSWORD}",
        "-p",
        "127.0.0.1::5432",
        IMAGE,
        "-c",
        "max_connections=200",
        "-c",
        "fsync=off",
        timeout=600,
    )
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
        yield ("127.0.0.1", port)
    finally:
        _docker("rm", "-f", name)


def _dsn(port: int, dbname: str) -> str:
    return f"host=127.0.0.1 port={port} dbname={dbname} user=postgres password={PASSWORD}"


@pytest.fixture
def meta_dsn(pg_server: tuple[str, int]) -> Iterator[str]:
    """Base pg-meta vierge propre à chaque test."""
    _, port = pg_server
    dbname = f"meta_{secrets.token_hex(4)}"
    with psycopg.connect(_dsn(port, "postgres"), autocommit=True) as admin:
        admin.execute(f'CREATE DATABASE "{dbname}"')
    try:
        yield _dsn(port, dbname)
    finally:
        with psycopg.connect(_dsn(port, "postgres"), autocommit=True) as admin:
            admin.execute(f'DROP DATABASE IF EXISTS "{dbname}" WITH (FORCE)')


@pytest.fixture
def fake_dir(tmp_path: Path) -> Path:
    path = tmp_path / "fake"
    path.mkdir()
    return path


@pytest.fixture
def cfg_dir(tmp_path: Path) -> Path:
    path = tmp_path / "configs"
    path.mkdir()
    return path
