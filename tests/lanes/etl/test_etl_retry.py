"""Retries, timeouts and error classification of the database layer."""

from __future__ import annotations

import copy

import psycopg
import pytest
from test_etl_scenarios import CONFIG

from engine.core.config import ReferentialConfig
from engine.lanes.etl import db
from engine.lanes.etl.db import LaneError, Pg


@pytest.fixture
def pg(pg_db: str, monkeypatch: pytest.MonkeyPatch) -> Pg:
    monkeypatch.setattr(db, "RETRY_BASE_S", 0.01)
    raw = copy.deepcopy(CONFIG)
    raw["runtime"] = {"max_attempts": 3}
    return Pg(ReferentialConfig.model_validate(raw), dsn=pg_db)


def test_transient_error_is_retried_on_a_new_connection(pg: Pg) -> None:
    attempts: list[int] = []

    def work(conn: psycopg.Connection) -> int:
        attempts.append(conn.info.backend_pid)
        if len(attempts) == 1:
            conn.execute("SELECT pg_terminate_backend(pg_backend_pid())")  # connection killed
        return conn.execute("SELECT 41 + 1").fetchone()[0]

    assert pg.run(work, "unit") == 42
    assert len(attempts) == 2 and attempts[0] != attempts[1]
    assert pg.retries == 1


def test_permanent_error_is_not_retried(pg: Pg) -> None:
    calls = []

    def work(conn: psycopg.Connection) -> None:
        calls.append(1)
        conn.execute("SELECT * FROM table_that_does_not_exist")

    with pytest.raises(psycopg.errors.UndefinedTable):
        pg.run(work, "unit")
    assert len(calls) == 1


def test_retries_are_bounded_and_reported(pg: Pg) -> None:
    calls = []

    def work(conn: psycopg.Connection) -> None:
        calls.append(1)
        conn.execute("SELECT pg_terminate_backend(pg_backend_pid())")
        conn.execute("SELECT 1")

    with pytest.raises(LaneError, match="failed after 3 attempts"):
        pg.run(work, "unit")
    assert len(calls) == 3


def test_lock_timeout_is_set_on_connections(pg: Pg) -> None:
    conn = pg.conn()
    assert conn.execute("SHOW lock_timeout").fetchone()[0] in {"1min", "60s"}
    assert conn.execute("SHOW statement_timeout").fetchone()[0] == "2h"
    assert conn.execute("SHOW TimeZone").fetchone()[0] == "UTC"


def test_statement_timeout_fails_without_retry(pg: Pg, monkeypatch: pytest.MonkeyPatch) -> None:
    calls = []

    def work(conn: psycopg.Connection) -> None:
        calls.append(1)
        conn.execute("SET statement_timeout = 200")
        conn.execute("SELECT pg_sleep(5)")

    with pytest.raises(psycopg.errors.QueryCanceled):
        pg.run(work, "unit")
    assert len(calls) == 1
