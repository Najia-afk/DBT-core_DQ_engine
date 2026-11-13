"""Fixtures de la voie ELT : PostgreSQL éphémère (Docker) activé par MDM_TEST_DOCKER=1.

Le conteneur est nommé `mdm2-elt-*`, limité à 6 Go (mémoire et swap) / 512 Mo de shm, et supprimé en
fin de session. Chaque test travaille dans sa propre base (`CREATE DATABASE`) : aucune collision
entre tables sources ou schémas d'état.
"""

from __future__ import annotations

import os
import shutil
import socket
import subprocess
import time
import uuid
from collections.abc import Iterator
from pathlib import Path

import psycopg
import pytest

IMAGE = os.environ.get("MDM_TEST_PG_IMAGE", "postgres:16.4-bookworm")
PASSWORD = "elt_test_only"


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _docker(*args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["docker", *args], capture_output=True, text=True, check=check, timeout=180
    )


@pytest.fixture(scope="session")
def pg_server() -> Iterator[str]:
    """DSN (sans nom de base) d'un PostgreSQL jetable."""
    if os.environ.get("MDM_TEST_DOCKER") != "1":
        pytest.skip("set MDM_TEST_DOCKER=1 to run the ELT lane integration tests (needs Docker)")
    name = f"mdm2-elt-test-{uuid.uuid4().hex[:8]}"
    port = _free_port()
    _docker(
        "run",
        "-d",
        "--name",
        name,
        "--memory",
        "6g",
        "--memory-swap",
        "6g",
        "--shm-size",
        "512m",
        "-e",
        f"POSTGRES_PASSWORD={PASSWORD}",
        "-p",
        f"127.0.0.1:{port}:5432",
        IMAGE,
        "-c",
        "max_connections=100",
    )
    base = f"postgresql://postgres:{PASSWORD}@127.0.0.1:{port}"
    try:
        deadline = time.time() + 90
        while True:
            try:
                with psycopg.connect(f"{base}/postgres", connect_timeout=3) as conn:
                    conn.execute("SELECT 1")
                break
            except psycopg.OperationalError:
                if time.time() > deadline:
                    raise
                time.sleep(1)
        yield base
    finally:
        _docker("rm", "-f", name, check=False)


@pytest.fixture
def pg_dsn(pg_server: str) -> Iterator[str]:
    """DSN d'une base vide propre au test."""
    db = f"t_{uuid.uuid4().hex[:10]}"
    with psycopg.connect(f"{pg_server}/postgres", autocommit=True) as conn:
        conn.execute(f'CREATE DATABASE "{db}"')
    try:
        yield f"{pg_server}/{db}"
    finally:
        with psycopg.connect(f"{pg_server}/postgres", autocommit=True) as conn:
            conn.execute(f'DROP DATABASE IF EXISTS "{db}" WITH (FORCE)')


@pytest.fixture
def work_dir() -> Iterator[Path]:
    """Répertoire de travail du test, sous le répertoire courant (jamais dans un dossier temporaire
    système), supprimé en fin de test."""
    base = Path(os.environ.get("MDM_TEST_WORKDIR", ".mdm_test_work")).resolve()
    base.mkdir(parents=True, exist_ok=True)
    (base / ".gitignore").write_text("*\n", encoding="utf-8")
    path = base / uuid.uuid4().hex[:10]
    path.mkdir()
    try:
        yield path
    finally:
        shutil.rmtree(path, ignore_errors=True)


@pytest.fixture
def elt_env(monkeypatch: pytest.MonkeyPatch, pg_dsn: str, work_dir: Path) -> dict[str, str]:
    """Variables d'environnement d'une exécution ELT (ressources bornées pour les tests)."""
    env = {
        "MDM_STATE_DSN": pg_dsn,
        "MDM_SRC_CRM_DSN": pg_dsn,
        "MDM_SRC_ERP_DSN": pg_dsn,
        "MDM_SRC_WEB_DSN": pg_dsn,
        "MDM_SRC_POS_DSN": pg_dsn,
        "CRM_DSN": pg_dsn,
        "ERP_DSN": pg_dsn,
        "MDM_ELT_WORKDIR": str(work_dir / "work"),
        "MDM_ELT_THREADS": "2",
        "MDM_ELT_MEMORY_LIMIT": "1GB",
        "MDM_ELT_DBT_TIMEOUT_S": "600",
    }
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    monkeypatch.delenv("MDM_ELT_FAULT", raising=False)
    monkeypatch.delenv("MDM_ELT_KEEP_NONE_PAIRS", raising=False)
    return env
