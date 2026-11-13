"""PostgreSQL access: connections with explicit timeouts, retries, locks, run log, fault hook."""

from __future__ import annotations

import hashlib
import json
import os
import sys
import time
from collections.abc import Callable
from typing import Any, TypeVar

import psycopg
from psycopg import errors as pgerr

from engine.core.config import ReferentialConfig

T = TypeVar("T")
LOCK_TIMEOUT_S = float(os.environ.get("MDM_ETL_LOCK_TIMEOUT_S", "60"))
RETRY_BASE_S = float(os.environ.get("MDM_ETL_RETRY_BASE_S", "1.0"))
_TRANSIENT = (
    pgerr.SerializationFailure,
    pgerr.DeadlockDetected,
    pgerr.LockNotAvailable,
    pgerr.AdminShutdown,
    pgerr.CrashShutdown,
    pgerr.CannotConnectNow,
    pgerr.ConnectionFailure,
    pgerr.ConnectionException,
    BrokenPipeError,
    ConnectionError,
)


class LaneError(RuntimeError):
    """Failure with a message meant for the operator (exit code != 0)."""


def env_value(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise LaneError(f"environment variable {name} is not set (secrets come from the env)")
    return value


def fault(point: str) -> None:
    """Test hook: `MDM_ETL_FAULT=<point>` kills the process at that point (simulated crash)."""
    if os.environ.get("MDM_ETL_FAULT") == point:
        sys.stderr.write(f"fault injected at {point}\n")
        sys.stderr.flush()
        os._exit(97)


def is_transient(error: BaseException) -> bool:
    if isinstance(error, _TRANSIENT):
        return True
    if isinstance(error, psycopg.OperationalError):
        return error.sqlstate is None  # connection lost on the client side
    return isinstance(error, psycopg.InterfaceError) and "closed" in str(error).lower()


def retrying(fn: Callable[[], T], attempts: int, label: str, on_retry: Callable[[], None]) -> T:
    """Run `fn`; retry transient database errors with exponential backoff."""
    for attempt in range(1, attempts + 1):
        try:
            return fn()
        except Exception as error:
            if not is_transient(error) or attempt == attempts:
                if is_transient(error):
                    raise LaneError(
                        f"{label}: failed after {attempts} attempts: {error}"
                    ) from error
                raise
            on_retry()
            time.sleep(RETRY_BASE_S * 2 ** (attempt - 1))
    raise AssertionError("unreachable")


def advisory_key(schema: str, name: str) -> int:
    digest = hashlib.sha256(f"{schema}:{name}".encode()).digest()
    return int.from_bytes(digest[:8], "big", signed=True)


class Pg:
    """Lazy connection to the state database (re-opened after a connection error)."""

    def __init__(self, cfg: ReferentialConfig, *, dsn: str | None = None) -> None:
        self.cfg = cfg
        self.dsn = dsn or env_value(cfg.state.dsn_env)
        self.schema = cfg.state.schema_name
        self.retries = 0
        self._conn: psycopg.Connection[Any] | None = None

    def conn(self) -> psycopg.Connection[Any]:
        if self._conn is None or self._conn.closed or self._conn.broken:
            self.reset()
            self._conn = open_connection(self.cfg, self.dsn, schema=self.schema)
        return self._conn

    def reset(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            except psycopg.Error:
                pass
        self._conn = None

    def note_retry(self) -> None:
        self.retries += 1
        self.reset()

    def run(self, fn: Callable[[psycopg.Connection[Any]], T], label: str) -> T:
        """`fn(conn)` with retries on transient errors (fn must be a self-contained unit)."""
        return retrying(
            lambda: fn(self.conn()), self.cfg.runtime.max_attempts, label, self.note_retry
        )

    def close(self) -> None:
        self.reset()


def open_connection(
    cfg: ReferentialConfig, dsn: str, *, schema: str | None = None, read_only: bool = False
) -> psycopg.Connection[Any]:
    step_ms = cfg.runtime.step_timeout_minutes * 60_000
    options = [
        f"-c lock_timeout={int(LOCK_TIMEOUT_S * 1000)}",
        f"-c statement_timeout={step_ms}",
        f"-c idle_in_transaction_session_timeout={step_ms}",
        "-c TimeZone=UTC",
    ]
    if schema:
        options.append(f"-c search_path={schema}")
    if read_only:
        options.append("-c default_transaction_read_only=on")
    return psycopg.connect(
        dsn,
        autocommit=True,
        options=" ".join(options),
        application_name=f"mdm-etl-{cfg.referential}",
        connect_timeout=15,
    )


# ------------------------------------------------------------------------------- run log


def run_begin(conn: psycopg.Connection[Any], cfg: ReferentialConfig, run_id: str, step: str):
    """Mark a step as running; return the stored counts if the step already succeeded."""
    row = conn.execute(
        "SELECT status, counts FROM run_log WHERE run_id = %s AND step = %s", [run_id, step]
    ).fetchone()
    if row is not None and row[0] == "ok":
        return row[1] or {}
    conn.execute(
        "INSERT INTO run_log (run_id, referential, step, status, started_at)"
        " VALUES (%s, %s, %s, 'running', now())"
        " ON CONFLICT (run_id, step) DO UPDATE SET status = 'running', started_at = now(),"
        " ended_at = NULL, error = NULL, counts = NULL",
        [run_id, cfg.referential, step],
    )
    return None


def run_finish(
    conn: psycopg.Connection[Any], run_id: str, step: str, status: str, counts: dict[str, Any]
) -> None:
    conn.execute(
        "UPDATE run_log SET status = %s, ended_at = now(), counts = %s::jsonb"
        " WHERE run_id = %s AND step = %s",
        [status, json.dumps(counts), run_id, step],
    )


def run_fail(conn: psycopg.Connection[Any], run_id: str, step: str, error: str) -> None:
    conn.execute(
        "UPDATE run_log SET status = 'failed', ended_at = now(), error = %s"
        " WHERE run_id = %s AND step = %s",
        [error[:2000], run_id, step],
    )
