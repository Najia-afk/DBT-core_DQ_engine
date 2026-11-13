"""Accès pg-meta : file, runs, steps, tentatives, groupes d'écriture (psycopg synchrone).

Toutes les opérations sont des transactions courtes ; aucune transaction n'est ouverte pendant
l'exécution d'un step. L'ordre de verrouillage est toujours : ligne `runs` puis lignes `steps`.
Les transitions faites par un propriétaire de tentative sont « fencées » par (owner, generation).
"""

from __future__ import annotations

import hashlib
import json
import logging
import random
import shutil
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from psycopg import Connection
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import ConnectionPool

from engine.core.config import load_config
from engine.runtime.admission import (
    AdmissionError,
    Candidate,
    IdempotencyConflict,
    Running,
    check_submission,
    select_next,
)
from engine.runtime.executor import LaneResult
from engine.runtime.planner import build_plan
from engine.runtime.settings import RuntimeSettings

SCHEMA_FILE = Path(__file__).with_name("schema.sql")
log = logging.getLogger("engine.runtime")
LOCK_MIGRATE = 7_282_001
LOCK_SUBMIT = 7_282_002
LOCK_ADMISSION = 7_282_003

RUN_STATES = (
    "queued",
    "running",
    "waiting_resource",
    "succeeded",
    "succeeded_with_warnings",
    "failed",
    "cancel_requested",
    "cancelled",
    "needs_attention",
)
TERMINAL_RUN_STATES = frozenset({"succeeded", "succeeded_with_warnings", "failed", "cancelled"})
ACTIVE_RUN_STATES = ("queued", "running", "waiting_resource", "cancel_requested")
ERROR_TEXT_LIMIT = 4000

# Issue de la lane -> (type d'erreur, tentative réessayable par défaut)
OUTCOME_KINDS = {
    "failed": "exit_nonzero",
    "timeout": "timeout",
    "rss_exceeded": "rss_exceeded",
    "shutdown": "lost",
    "lost": "lost",
    "invalid_summary": "contract_violation",
    "engine_error": "engine_error",
}


@dataclass(frozen=True)
class SubmitResult:
    run_id: str
    created: bool
    status: str


@dataclass(frozen=True)
class Claim:
    run_id: str
    step: str
    kind: str
    source_id: str | None
    referential: str
    lane: str
    config_path: str
    params: dict[str, Any]
    profile: str
    attempt: int
    generation: int
    timeout_seconds: int
    owner: str
    host: str


def _epoch(value: datetime | None) -> float:
    return value.timestamp() if value is not None else 0.0


def _truncate(text: str | None) -> str | None:
    if text is None:
        return None
    return text if len(text) <= ERROR_TEXT_LIMIT else text[-ERROR_TEXT_LIMIT:]


def _jsonable(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat()
    if isinstance(value, dict):
        return {k: _jsonable(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_jsonable(v) for v in value]
    return value


def request_hash(referential: str, lane: str, config_path: str, config_bytes: bytes, params: dict):
    body = {
        "referential": referential,
        "lane": lane,
        "config_path": config_path,
        "config_sha256": hashlib.sha256(config_bytes).hexdigest(),
        "params": params,
    }
    return hashlib.sha256(json.dumps(body, sort_keys=True, default=str).encode()).hexdigest()


class RuntimeStore:
    def __init__(self, dsn: str, settings: RuntimeSettings | None = None) -> None:
        self.dsn = dsn
        self.settings = settings or RuntimeSettings()
        self.pool = ConnectionPool(
            dsn,
            min_size=1,
            max_size=self.settings.host.max_running_steps + 8,
            kwargs={"row_factory": dict_row, "options": "-c search_path=mdm_runtime,public"},
            open=False,
            timeout=30,
            name="mdm-runtime",
        )

    def open(self) -> None:
        self.pool.open(wait=True, timeout=30)

    def close(self) -> None:
        self.pool.close()

    def __enter__(self) -> RuntimeStore:
        self.open()
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # ------------------------------------------------------------------ schéma

    def migrate(self) -> None:
        sql = SCHEMA_FILE.read_text(encoding="utf-8")
        with self.pool.connection() as conn:
            conn.execute("SELECT pg_advisory_xact_lock(%s)", (LOCK_MIGRATE,))
            conn.execute(sql)

    # -------------------------------------------------------------- soumission

    def submit_run(
        self,
        config_path: str | Path,
        *,
        lane: str = "etl",
        idempotency_key: str | None = None,
        params: dict[str, Any] | None = None,
    ) -> SubmitResult:
        """Persiste la demande (sans l'exécuter). 429 -> AdmissionError, 409 -> conflit."""
        params = dict(params or {})
        path = Path(config_path).resolve()
        config = load_config(path)
        plan = build_plan(config, params, profiles=set(self.settings.profiles))
        key = idempotency_key or f"auto-{uuid.uuid4().hex}"
        digest = request_hash(config.referential, lane, str(path), path.read_bytes(), params)
        run_id = f"run-{datetime.now(UTC):%Y%m%d%H%M%S}-{uuid.uuid4().hex[:8]}"
        with self.pool.connection() as conn:
            conn.execute("SELECT pg_advisory_xact_lock(%s)", (LOCK_SUBMIT,))
            # La recherche d'une demande existante précède le contrôle de plafond.
            existing = conn.execute(
                "SELECT run_id, request_hash, status FROM runs"
                " WHERE referential = %s AND idempotency_key = %s",
                (config.referential, key),
            ).fetchone()
            if existing:
                if existing["request_hash"] != digest:
                    raise IdempotencyConflict(
                        f"idempotency key {key!r} already used with different content"
                        f" (run {existing['run_id']})"
                    )
                return SubmitResult(existing["run_id"], False, existing["status"])
            counts = conn.execute(
                "SELECT count(*) AS total, count(*) FILTER (WHERE referential = %s) AS ref"
                " FROM runs WHERE status IN ('queued', 'waiting_resource')",
                (config.referential,),
            ).fetchone()
            check_submission(counts["total"], counts["ref"], self.settings)
            conn.execute(
                "INSERT INTO runs (run_id, referential, lane, config_path, idempotency_key,"
                " request_hash, params) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                (run_id, config.referential, lane, str(path), key, digest, Jsonb(params)),
            )
            with conn.cursor() as cur:
                cur.executemany(
                    "INSERT INTO steps (run_id, step, ordinal, kind, source_id, depends_on,"
                    " profile, max_attempts, timeout_seconds, ready_at)"
                    " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s,"
                    " CASE WHEN cardinality(%s::text[]) = 0 THEN now() END)",
                    [
                        (
                            run_id,
                            s.step,
                            i,
                            s.kind,
                            s.source_id,
                            list(s.depends_on),
                            s.profile,
                            s.max_attempts,
                            s.timeout_seconds,
                            list(s.depends_on),
                        )
                        for i, s in enumerate(plan)
                    ],
                )
        return SubmitResult(run_id, True, "queued")

    # ------------------------------------------------------------ consultation

    def get_run(self, run_id: str) -> dict[str, Any] | None:
        with self.pool.connection() as conn:
            run = conn.execute("SELECT * FROM runs WHERE run_id = %s", (run_id,)).fetchone()
            if run is None:
                return None
            steps = conn.execute(
                "SELECT * FROM steps WHERE run_id = %s ORDER BY ordinal, step", (run_id,)
            ).fetchall()
            attempts = conn.execute(
                "SELECT * FROM attempts WHERE run_id = %s ORDER BY step, attempt", (run_id,)
            ).fetchall()
            group = conn.execute(
                "SELECT run_id FROM write_groups WHERE group_name = %s", (run["referential"],)
            ).fetchone()
        run["steps"] = steps
        run["attempts"] = attempts
        run["holds_write_group"] = bool(group and group["run_id"] == run_id)
        return _jsonable(run)

    def list_runs(self, statuses: list[str] | None = None, limit: int = 100) -> list[dict]:
        with self.pool.connection() as conn:
            rows = conn.execute(
                "SELECT run_id, referential, lane, status, status_reason, queued_at, started_at,"
                " ended_at FROM runs WHERE (%s::text[] IS NULL OR status = ANY(%s::text[]))"
                " ORDER BY queued_at DESC, seq DESC LIMIT %s",
                (statuses, statuses, limit),
            ).fetchall()
        return [_jsonable(r) for r in rows]

    def has_inflight(self) -> bool:
        with self.pool.connection() as conn:
            row = conn.execute(
                "SELECT EXISTS (SELECT 1 FROM steps s JOIN runs r USING (run_id)"
                " WHERE s.status IN ('running', 'retry_wait')"
                " AND r.status IN ('queued', 'running', 'waiting_resource', 'cancel_requested'))"
                " AS busy"
            ).fetchone()
        return bool(row["busy"])

    # ------------------------------------------------------------------ workers

    def register_worker(self, worker_id: str, host: str, pid: int, pid_started: float) -> None:
        with self.pool.connection() as conn:
            conn.execute(
                "INSERT INTO workers (worker_id, host, pid, pid_started) VALUES (%s, %s, %s, %s)",
                (worker_id, host, pid, pid_started),
            )

    def worker_heartbeat(self, worker_id: str) -> None:
        with self.pool.connection() as conn:
            conn.execute(
                "UPDATE workers SET heartbeat_at = now() WHERE worker_id = %s", (worker_id,)
            )

    def stop_worker(self, worker_id: str) -> None:
        with self.pool.connection() as conn:
            conn.execute("UPDATE workers SET stopped_at = now() WHERE worker_id = %s", (worker_id,))

    # -------------------------------------------------------------------- claim

    def claim_next(self, worker_id: str, host: str) -> Claim | None:
        """Réserve atomiquement le prochain step admissible ; transaction courte."""
        free_gib = None
        if self.settings.host.min_free_disk_gib > 0:
            free_gib = shutil.disk_usage(self.settings.host.work_dir).free / 2**30
        for _ in range(8):
            outcome = self._claim_once(worker_id, host, free_gib)
            if outcome != "retry":
                return outcome
        return None

    def _claim_once(self, worker_id: str, host: str, free_gib: float | None):
        s = self.settings
        with self.pool.connection() as conn:
            # Sérialise les décisions d'admission : les budgets sont par hôte, pas par worker.
            conn.execute("SELECT pg_advisory_xact_lock(%s)", (LOCK_ADMISSION,))
            rows = conn.execute(
                "SELECT s.run_id, s.step, s.profile, coalesce(s.ready_at, r.queued_at) AS ready_at,"
                " r.referential, r.queued_at, r.seq, g.run_id AS group_holder,"
                " extract(epoch FROM now()) AS now_epoch"
                " FROM steps s JOIN runs r USING (run_id)"
                " LEFT JOIN write_groups g ON g.group_name = r.referential"
                " WHERE s.status IN ('pending', 'retry_wait')"
                " AND r.status IN ('queued', 'running', 'waiting_resource')"
                " AND (s.not_before IS NULL OR s.not_before <= now())"
                " AND (g.run_id IS NULL OR g.run_id = s.run_id)"
                " AND NOT EXISTS (SELECT 1 FROM steps d WHERE d.run_id = s.run_id"
                "   AND d.step = ANY (s.depends_on) AND d.status <> 'succeeded')"
                " ORDER BY r.queued_at, r.seq, s.ordinal LIMIT 1000"
            ).fetchall()
            if not rows:
                return None
            now = float(rows[0]["now_epoch"])
            running = [
                Running(r["referential"], r["profile"])
                for r in conn.execute(
                    "SELECT r.referential, s.profile FROM steps s JOIN runs r USING (run_id)"
                    " WHERE s.status IN ('running', 'suspect') AND s.host = %s",
                    (host,),
                ).fetchall()
            ]
            rotation = {
                r["referential"]: _epoch(r["last_claim_at"])
                for r in conn.execute("SELECT * FROM referential_rotation").fetchall()
            }
            candidates = [
                Candidate(
                    r["run_id"],
                    r["step"],
                    r["referential"],
                    r["profile"],
                    _epoch(r["ready_at"]),
                    (_epoch(r["queued_at"]), r["seq"]),
                    r["group_holder"],
                )
                for r in rows
            ]
            decision = select_next(
                candidates, running, s, now=now, free_disk_gib=free_gib, last_served=rotation
            )
            if decision.refused:
                for cand, reason in decision.refused:
                    self._fail_unadmitted(conn, cand, reason)
                return "retry"
            self._mark_waiting(conn, decision)
            chosen = decision.chosen
            log.debug(
                "claim: %d candidates, %d running, chosen=%s blocked=%s",
                len(candidates),
                len(running),
                chosen and chosen.key,
                decision.blocked,
            )
            if chosen is None:
                return None
            claim = self._take(conn, chosen, worker_id, host)
            return claim if claim is not None else "retry"

    def _take(self, conn: Connection, cand: Candidate, worker_id: str, host: str):
        run = conn.execute(
            "SELECT * FROM runs WHERE run_id = %s AND status IN"
            " ('queued', 'running', 'waiting_resource') FOR UPDATE SKIP LOCKED",
            (cand.run_id,),
        ).fetchone()
        if run is None:
            return None
        step = conn.execute(
            "SELECT * FROM steps WHERE run_id = %s AND step = %s"
            " AND status IN ('pending', 'retry_wait') FOR UPDATE SKIP LOCKED",
            (cand.run_id, cand.step),
        ).fetchone()
        if step is None:
            return None
        held = conn.execute(
            "INSERT INTO write_groups (group_name, run_id) VALUES (%s, %s)"
            " ON CONFLICT (group_name) DO UPDATE SET group_name = excluded.group_name"
            " WHERE write_groups.run_id = excluded.run_id RETURNING run_id",
            (run["referential"], run["run_id"]),
        ).fetchone()
        if held is None:
            return None
        attempt, generation = step["attempt"] + 1, step["generation"] + 1
        conn.execute(
            "UPDATE steps SET status = 'running', attempt = %s, generation = %s, owner = %s,"
            " host = %s, pid = NULL, pid_started = NULL, heartbeat_at = now(),"
            " started_at = now(), ended_at = NULL, not_before = NULL, wait_reason = NULL,"
            " error = NULL, error_kind = NULL WHERE run_id = %s AND step = %s",
            (attempt, generation, worker_id, host, cand.run_id, cand.step),
        )
        conn.execute(
            "INSERT INTO attempts (run_id, step, attempt, generation, owner, host)"
            " VALUES (%s, %s, %s, %s, %s, %s)",
            (cand.run_id, cand.step, attempt, generation, worker_id, host),
        )
        conn.execute(
            "UPDATE runs SET status = 'running', status_reason = NULL,"
            " started_at = coalesce(started_at, now()) WHERE run_id = %s",
            (cand.run_id,),
        )
        conn.execute(
            "INSERT INTO referential_rotation (referential, last_claim_at) VALUES (%s, now())"
            " ON CONFLICT (referential) DO UPDATE SET last_claim_at = excluded.last_claim_at",
            (run["referential"],),
        )
        return Claim(
            run_id=run["run_id"],
            step=step["step"],
            kind=step["kind"],
            source_id=step["source_id"],
            referential=run["referential"],
            lane=run["lane"],
            config_path=run["config_path"],
            params=run["params"],
            profile=step["profile"],
            attempt=attempt,
            generation=generation,
            timeout_seconds=step["timeout_seconds"],
            owner=worker_id,
            host=host,
        )

    def _mark_waiting(self, conn: Connection, decision) -> None:
        by_run: dict[str, str] = {}
        chosen_run = decision.chosen.run_id if decision.chosen else None
        for (run_id, step), reason in decision.blocked.items():
            conn.execute(
                "UPDATE steps SET wait_reason = %s WHERE run_id = %s AND step = %s"
                " AND wait_reason IS DISTINCT FROM %s",
                (reason, run_id, step, reason),
            )
            if run_id != chosen_run:
                by_run.setdefault(run_id, reason)
        for run_id, reason in by_run.items():
            conn.execute(
                "UPDATE runs SET status = 'waiting_resource', status_reason = %s"
                " WHERE run_id = %s AND status IN ('queued', 'running')"
                " AND NOT EXISTS (SELECT 1 FROM steps WHERE run_id = runs.run_id"
                "   AND status IN ('running', 'suspect'))",
                (reason, run_id),
            )

    def _fail_unadmitted(self, conn: Connection, cand: Candidate, reason: str) -> None:
        conn.execute("SELECT 1 FROM runs WHERE run_id = %s FOR UPDATE", (cand.run_id,))
        conn.execute(
            "UPDATE steps SET status = 'failed', ended_at = now(),"
            " error_kind = 'capacity_exceeded',"
            " error = %s WHERE run_id = %s AND step = %s AND status IN ('pending', 'retry_wait')",
            (reason, cand.run_id, cand.step),
        )
        self._skip_remaining(conn, cand.run_id)
        self._refresh_run(conn, cand.run_id)

    # ------------------------------------------------------- tentative en cours

    def record_pid(self, claim: Claim, pid: int, pid_started: float) -> bool:
        with self.pool.connection() as conn:
            row = conn.execute(
                "UPDATE steps SET pid = %s, pid_started = %s WHERE run_id = %s AND step = %s"
                " AND generation = %s AND owner = %s AND status = 'running' RETURNING 1 AS ok",
                (pid, pid_started, claim.run_id, claim.step, claim.generation, claim.owner),
            ).fetchone()
            if row:
                conn.execute(
                    "UPDATE attempts SET pid = %s WHERE run_id = %s AND step = %s AND attempt = %s",
                    (pid, claim.run_id, claim.step, claim.attempt),
                )
        return row is not None

    def heartbeat(self, claim: Claim, peak_rss_mib: int) -> str:
        """'ok' | 'cancel' | 'fenced' (ancien propriétaire : ne doit plus rien valider)."""
        with self.pool.connection() as conn:
            row = conn.execute(
                "UPDATE steps s SET heartbeat_at = now(),"
                " peak_rss_mib = GREATEST(coalesce(s.peak_rss_mib, 0), %s)"
                " FROM runs r WHERE r.run_id = s.run_id AND s.run_id = %s AND s.step = %s"
                " AND s.generation = %s AND s.owner = %s AND s.status = 'running'"
                " RETURNING r.status AS run_status",
                (peak_rss_mib, claim.run_id, claim.step, claim.generation, claim.owner),
            ).fetchone()
        if row is None:
            return "fenced"
        return "cancel" if row["run_status"] == "cancel_requested" else "ok"

    # ----------------------------------------------------------- fin de tentative

    def _backoff(self, attempt: int) -> float:
        r = self.settings.retry
        delay = min(r.backoff_max_seconds, r.backoff_base_seconds * 2 ** (attempt - 1))
        return max(0.0, delay * (1 + random.uniform(-r.jitter, r.jitter)))

    def finish_step(self, claim: Claim, res: LaneResult) -> bool:
        """Valide l'issue d'une tentative. False si l'appelant est fencé (ancienne génération)."""
        s = self.settings
        with self.pool.connection() as conn:
            run = conn.execute(
                "SELECT status FROM runs WHERE run_id = %s FOR UPDATE", (claim.run_id,)
            ).fetchone()
            step = conn.execute(
                "SELECT attempt, max_attempts FROM steps WHERE run_id = %s AND step = %s"
                " AND generation = %s AND owner = %s AND status = 'running' FOR UPDATE",
                (claim.run_id, claim.step, claim.generation, claim.owner),
            ).fetchone()
            if run is None or step is None:
                return False
            error = res.error
            if error and res.stderr_tail.strip():
                error = f"{error}\n--- stderr tail ---\n{res.stderr_tail.strip()}"
            kind = OUTCOME_KINDS.get(res.outcome, res.outcome)
            delay: float | None = None
            if res.outcome == "succeeded":
                status = "succeeded"
            elif res.outcome == "cancelled":
                status = "cancelled"
            elif res.outcome == "kill_unconfirmed":
                status = "suspect"
            else:
                retry = (
                    res.retryable
                    and kind in s.retry.retry_on
                    and res.exit_code not in s.retry.non_retryable_exit_codes
                    and step["attempt"] < step["max_attempts"]
                    and run["status"] != "cancel_requested"
                )
                if retry:
                    status, delay = "retry_wait", self._backoff(step["attempt"])
                else:
                    status = "cancelled" if run["status"] == "cancel_requested" else "failed"
            self._end_attempt(
                conn,
                claim.run_id,
                claim.step,
                claim.generation,
                claim.owner,
                status=status,
                outcome=res.outcome,
                error_kind=None if status == "succeeded" else kind,
                error=error,
                summary=res.summary,
                exit_code=res.exit_code,
                peak=res.peak_rss_mib,
                delay=delay,
            )
            if status == "succeeded":
                self._release_dependents(conn, claim.run_id)
            elif status == "failed":
                self._skip_remaining(conn, claim.run_id)
            self._refresh_run(conn, claim.run_id)
        return True

    def _end_attempt(
        self,
        conn: Connection,
        run_id: str,
        step: str,
        generation: int,
        owner: str | None,
        *,
        status: str,
        outcome: str,
        error_kind: str | None,
        error: str | None,
        summary: dict | None,
        exit_code: int | None,
        peak: int,
        delay: float | None,
        bump_generation: bool = False,
    ) -> int | None:
        row = conn.execute(
            "UPDATE steps SET status = %(status)s,"
            " ended_at = CASE WHEN %(status)s IN ('retry_wait', 'suspect') THEN NULL"
            "   ELSE now() END,"
            " error = %(error)s, error_kind = %(kind)s, summary = %(summary)s,"
            " peak_rss_mib = GREATEST(coalesce(peak_rss_mib, 0), %(peak)s),"
            " generation = generation + %(bump)s,"
            " not_before = CASE WHEN %(delay)s::float8 IS NULL THEN NULL"
            "   ELSE now() + make_interval(secs => %(delay)s::float8) END,"
            " ready_at = CASE WHEN %(delay)s::float8 IS NULL THEN ready_at"
            "   ELSE now() + make_interval(secs => %(delay)s::float8) END,"
            " wait_reason = NULL"
            " WHERE run_id = %(run_id)s AND step = %(step)s AND generation = %(gen)s"
            " AND status = 'running' AND (%(owner)s::text IS NULL OR owner = %(owner)s::text)"
            " RETURNING attempt",
            {
                "status": status,
                "error": _truncate(error),
                "kind": error_kind,
                "summary": Jsonb(summary) if summary is not None else None,
                "peak": peak,
                "bump": 1 if bump_generation else 0,
                "delay": delay,
                "run_id": run_id,
                "step": step,
                "gen": generation,
                "owner": owner,
            },
        ).fetchone()
        if row is None:
            return None
        conn.execute(
            "UPDATE attempts SET ended_at = CASE WHEN %s = 'suspect' THEN NULL ELSE now() END,"
            " outcome = %s, exit_code = %s, error = %s, summary = %s,"
            " peak_rss_mib = GREATEST(coalesce(peak_rss_mib, 0), %s)"
            " WHERE run_id = %s AND step = %s AND attempt = %s",
            (
                status,
                outcome,
                exit_code,
                _truncate(error),
                Jsonb(summary) if summary is not None else None,
                peak,
                run_id,
                step,
                row["attempt"],
            ),
        )
        return row["attempt"]

    def _release_dependents(self, conn: Connection, run_id: str) -> None:
        conn.execute(
            "UPDATE steps s SET ready_at = now() WHERE s.run_id = %s AND s.status = 'pending'"
            " AND s.ready_at IS NULL AND NOT EXISTS (SELECT 1 FROM steps d"
            " WHERE d.run_id = s.run_id AND d.step = ANY (s.depends_on)"
            " AND d.status <> 'succeeded')",
            (run_id,),
        )

    def _skip_remaining(self, conn: Connection, run_id: str) -> None:
        conn.execute(
            "UPDATE steps SET status = 'skipped', ended_at = now(), error_kind = 'upstream_failed',"
            " wait_reason = NULL WHERE run_id = %s AND status IN ('pending', 'retry_wait')",
            (run_id,),
        )

    def _refresh_run(self, conn: Connection, run_id: str) -> str:
        """Recalcule l'état du run ; l'appelant détient le verrou de la ligne `runs`."""
        run = conn.execute(
            "SELECT status, referential FROM runs WHERE run_id = %s", (run_id,)
        ).fetchone()
        if run["status"] in TERMINAL_RUN_STATES or run["status"] == "needs_attention":
            return run["status"]
        steps = conn.execute(
            "SELECT step, status, summary, error_kind FROM steps WHERE run_id = %s", (run_id,)
        ).fetchall()
        states = {r["status"] for r in steps}
        reason: str | None = None
        if "suspect" in states:
            new = "needs_attention"
            suspects = [r["step"] for r in steps if r["status"] == "suspect"]
            reason = f"attempt state unconfirmed for {suspects}; write group kept blocked"
        elif "running" in states:
            return run["status"]
        elif states <= {"succeeded"}:
            warned = any(_has_warnings(r["summary"]) for r in steps)
            new = "succeeded_with_warnings" if warned else "succeeded"
        elif run["status"] == "cancel_requested":
            conn.execute(
                "UPDATE steps SET status = 'cancelled', ended_at = now(), wait_reason = NULL"
                " WHERE run_id = %s AND status IN ('pending', 'retry_wait')",
                (run_id,),
            )
            new, reason = "cancelled", "cancelled on request"
        elif "failed" in states:
            self._skip_remaining(conn, run_id)
            failed = [r for r in steps if r["status"] == "failed"]
            new = "failed"
            reason = ", ".join(f"{r['step']}: {r['error_kind']}" for r in failed)
        elif states & {"pending", "retry_wait"}:
            return run["status"]
        else:
            new, reason = "failed", f"inconsistent step states {sorted(states)}"
        terminal = new in TERMINAL_RUN_STATES
        conn.execute(
            "UPDATE runs SET status = %s, status_reason = %s,"
            " ended_at = CASE WHEN %s THEN now() ELSE ended_at END WHERE run_id = %s",
            (new, reason, terminal, run_id),
        )
        if terminal:
            conn.execute(
                "DELETE FROM write_groups WHERE group_name = %s AND run_id = %s",
                (run["referential"], run_id),
            )
        return new

    # ------------------------------------------------------------------ annulation

    def cancel_run(self, run_id: str) -> str:
        """Demande l'annulation ; confirmée (`cancelled`) seulement quand plus rien ne tourne."""
        with self.pool.connection() as conn:
            run = conn.execute(
                "SELECT status FROM runs WHERE run_id = %s FOR UPDATE", (run_id,)
            ).fetchone()
            if run is None:
                raise KeyError(run_id)
            if run["status"] in TERMINAL_RUN_STATES or run["status"] in {
                "needs_attention",
                "cancel_requested",
            }:
                return run["status"]
            conn.execute(
                "UPDATE runs SET status = 'cancel_requested', cancel_requested_at = now(),"
                " status_reason = 'cancellation requested' WHERE run_id = %s",
                (run_id,),
            )
            conn.execute(
                "UPDATE steps SET status = 'cancelled', ended_at = now(), wait_reason = NULL"
                " WHERE run_id = %s AND status IN ('pending', 'retry_wait')",
                (run_id,),
            )
            return self._refresh_run(conn, run_id)

    def resolve_attention(self, run_id: str, *, confirmed_stopped: bool) -> str:
        """Décision opérateur : l'arrêt est confirmé, le run échoue et libère son groupe."""
        if not confirmed_stopped:
            raise ValueError("the operator must confirm that the attempt processes are stopped")
        with self.pool.connection() as conn:
            run = conn.execute(
                "SELECT status, referential FROM runs WHERE run_id = %s FOR UPDATE", (run_id,)
            ).fetchone()
            if run is None:
                raise KeyError(run_id)
            if run["status"] != "needs_attention":
                return run["status"]
            conn.execute(
                "UPDATE steps SET status = 'failed', ended_at = now(),"
                " error_kind = 'operator_resolved' WHERE run_id = %s"
                " AND status IN ('suspect', 'running')",
                (run_id,),
            )
            self._skip_remaining(conn, run_id)
            conn.execute(
                "UPDATE runs SET status = 'failed', ended_at = now(),"
                " status_reason = 'resolved by operator after needs_attention' WHERE run_id = %s",
                (run_id,),
            )
            conn.execute(
                "DELETE FROM write_groups WHERE group_name = %s AND run_id = %s",
                (run["referential"], run_id),
            )
        return "failed"

    # ------------------------------------------------------------ reprise / reaper

    def foreign_running_steps(self, worker_id: str) -> list[dict[str, Any]]:
        with self.pool.connection() as conn:
            return conn.execute(
                "SELECT s.run_id, s.step, s.kind, s.source_id, s.generation, s.owner, s.host,"
                " s.pid, s.pid_started, extract(epoch FROM now() - s.heartbeat_at) AS age,"
                " w.host AS w_host, w.pid AS w_pid, w.pid_started AS w_started,"
                " w.stopped_at IS NOT NULL AS w_stopped, w.worker_id IS NOT NULL AS w_known"
                " FROM steps s LEFT JOIN workers w ON w.worker_id = s.owner"
                " WHERE s.status = 'running' AND s.owner IS DISTINCT FROM %s",
                (worker_id,),
            ).fetchall()

    def mark_lost(self, run_id: str, step: str, generation: int, reason: str) -> bool:
        """Arrêt confirmé : la tentative est perdue ; la génération est incrémentée (fencing)."""
        with self.pool.connection() as conn:
            run = conn.execute(
                "SELECT status FROM runs WHERE run_id = %s FOR UPDATE", (run_id,)
            ).fetchone()
            cur = conn.execute(
                "SELECT attempt, max_attempts FROM steps WHERE run_id = %s AND step = %s"
                " AND generation = %s AND status = 'running' FOR UPDATE",
                (run_id, step, generation),
            ).fetchone()
            if run is None or cur is None:
                return False
            retry = (
                "lost" in self.settings.retry.retry_on
                and cur["attempt"] < cur["max_attempts"]
                and run["status"] != "cancel_requested"
            )
            if retry:
                status, delay = "retry_wait", self._backoff(cur["attempt"])
            else:
                status = "cancelled" if run["status"] == "cancel_requested" else "failed"
                delay = None
            self._end_attempt(
                conn,
                run_id,
                step,
                generation,
                None,
                status=status,
                outcome="lost",
                error_kind="lost",
                error=reason,
                summary=None,
                exit_code=None,
                peak=0,
                delay=delay,
                bump_generation=True,
            )
            if status == "failed":
                self._skip_remaining(conn, run_id)
            self._refresh_run(conn, run_id)
        return True

    def mark_suspect(self, run_id: str, step: str, generation: int, reason: str) -> bool:
        """Heartbeat perdu / arrêt non confirmé : tentative suspecte, run needs_attention."""
        with self.pool.connection() as conn:
            conn.execute("SELECT 1 FROM runs WHERE run_id = %s FOR UPDATE", (run_id,))
            done = self._end_attempt(
                conn,
                run_id,
                step,
                generation,
                None,
                status="suspect",
                outcome="suspect",
                error_kind="suspect",
                error=reason,
                summary=None,
                exit_code=None,
                peak=0,
                delay=None,
                bump_generation=True,
            )
            if done is None:
                return False
            self._refresh_run(conn, run_id)
        return True


def _has_warnings(summary: Any) -> bool:
    if not isinstance(summary, dict):
        return False
    if summary.get("status") == "blocked" or summary.get("blocked") is True:
        return True
    warnings = summary.get("warnings")
    if isinstance(warnings, (int, float)):
        warned = warnings > 0
    else:
        warned = bool(warnings)
    return warned or any(
        _has_warnings(value) for value in summary.values() if isinstance(value, dict)
    )


__all__ = [
    "ACTIVE_RUN_STATES",
    "RUN_STATES",
    "TERMINAL_RUN_STATES",
    "AdmissionError",
    "Claim",
    "IdempotencyConflict",
    "RuntimeStore",
    "SubmitResult",
]
