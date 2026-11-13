"""RuntimeEngine : boucle asyncio de claim, exécution supervisée, reprise sur incident.

Les accès à pg-meta passent par psycopg synchrone dans des threads (`asyncio.to_thread`) : sous
Windows, psycopg async exige une boucle Selector alors que les sous-processus asyncio exigent la
boucle Proactor.
"""

from __future__ import annotations

import asyncio
import logging
import os
import socket
import time
import uuid
from typing import Any

import psutil

from engine.runtime.executor import (
    LaneResult,
    build_argv,
    find_lane_processes,
    kill_tree,
    process_alive,
    run_lane,
)
from engine.runtime.settings import RuntimeSettings
from engine.runtime.store import Claim, RuntimeStore, SubmitResult

log = logging.getLogger("engine.runtime")
COMMIT_RETRIES = 5


class RuntimeEngine:
    def __init__(
        self,
        dsn: str | None = None,
        settings: RuntimeSettings | None = None,
        *,
        store: RuntimeStore | None = None,
        worker_id: str | None = None,
    ) -> None:
        self.settings = settings or (store.settings if store else RuntimeSettings())
        if store is None:
            dsn = dsn or os.environ.get("MDM_META_DSN")
            if not dsn:
                raise ValueError("pg-meta DSN required (argument or MDM_META_DSN)")
            store = RuntimeStore(dsn, self.settings)
        self.store = store
        self.host = socket.gethostname()
        self.worker_id = worker_id or f"{self.host}-{os.getpid()}-{uuid.uuid4().hex[:6]}"
        self._tasks: set[asyncio.Task[None]] = set()
        self._completed = 0
        self._stop = asyncio.Event()
        self._started = False
        self._last_reap = 0.0
        self._last_beat = 0.0

    # ----------------------------------------------------------- API synchrone

    def submit_run(self, config_path: str, **kwargs: Any) -> SubmitResult:
        return self.store.submit_run(config_path, **kwargs)

    def get_run(self, run_id: str) -> dict[str, Any] | None:
        return self.store.get_run(run_id)

    def cancel_run(self, run_id: str) -> str:
        return self.store.cancel_run(run_id)

    # --------------------------------------------------------------- cycle de vie

    async def start(self) -> None:
        if self._started:
            return
        await asyncio.to_thread(self.store.open)
        await asyncio.to_thread(self.store.migrate)
        created = psutil.Process().create_time()
        await asyncio.to_thread(
            self.store.register_worker, self.worker_id, self.host, os.getpid(), created
        )
        self._started = True
        await asyncio.to_thread(self.reap)  # reprise sur incident avant tout nouveau départ

    async def stop(self) -> None:
        """Arrêt propre : plus de départs ; les steps en cours sont terminés puis relâchés."""
        self._stop.set()
        if self._tasks:
            await asyncio.gather(*list(self._tasks), return_exceptions=True)
        if self._started:
            await asyncio.to_thread(self.store.stop_worker, self.worker_id)
            await asyncio.to_thread(self.store.close)
            self._started = False

    def request_stop(self) -> None:
        self._stop.set()

    async def __aenter__(self) -> RuntimeEngine:
        await self.start()
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.stop()

    async def run_forever(self) -> None:
        await self.start()
        try:
            await self._loop(until_idle=False, max_seconds=None)
        finally:
            await self.stop()

    async def run_until_idle(self, max_seconds: float | None = None) -> None:
        """Traite la file jusqu'à ce que plus rien ne soit en cours ni réessayable."""
        await self.start()
        await self._loop(until_idle=True, max_seconds=max_seconds)

    # ------------------------------------------------------------------- boucle

    async def _loop(self, *, until_idle: bool, max_seconds: float | None) -> None:
        s = self.settings
        began = time.monotonic()
        while not self._stop.is_set():
            if max_seconds is not None and time.monotonic() - began > max_seconds:
                raise TimeoutError(f"engine not idle after {max_seconds}s")
            now = time.monotonic()
            if now - self._last_reap >= s.scheduler.reaper_interval_seconds:
                self._last_reap = now
                await asyncio.to_thread(self.reap)
            if now - self._last_beat >= s.heartbeat.interval_seconds:
                self._last_beat = now
                await asyncio.to_thread(self.store.worker_heartbeat, self.worker_id)
            launched = False
            completed_before = self._completed
            while not self._stop.is_set():
                # l'admission en base (budgets par hôte) décide ; pas de plafond local
                claim = await asyncio.to_thread(self.store.claim_next, self.worker_id, self.host)
                if claim is None:
                    break
                launched = True
                task = asyncio.create_task(
                    self._run_step(claim), name=f"{claim.run_id}/{claim.step}"
                )
                self._tasks.add(task)
                task.add_done_callback(self._task_done)
            # Un step terminé pendant le claim a pu libérer des ressources : ne pas conclure.
            settled = not launched and not self._tasks and self._completed == completed_before
            if until_idle and settled and not await asyncio.to_thread(self.store.has_inflight):
                return
            if self._tasks:
                await asyncio.wait(
                    set(self._tasks),
                    timeout=s.scheduler.poll_interval_seconds,
                    return_when=asyncio.FIRST_COMPLETED,
                )
            else:
                await asyncio.sleep(s.scheduler.poll_interval_seconds)

    # ----------------------------------------------------------------- un step

    def _task_done(self, task: asyncio.Task[None]) -> None:
        self._tasks.discard(task)
        self._completed += 1

    async def _run_step(self, claim: Claim) -> None:
        s = self.settings
        profile = s.profiles[claim.profile]
        argv = build_argv(
            s.lane.command,
            python=s.lane.python,
            lane=claim.lane,
            command=claim.kind,
            config=claim.config_path,
            run_id=claim.run_id,
            source=claim.source_id,
            out=claim.params.get("export_out") if claim.kind == "export" else None,
        )
        env = {
            **os.environ,
            **s.lane.env,
            "MDM_RUN_ID": claim.run_id,
            "MDM_STEP": claim.step,
            "MDM_ATTEMPT": str(claim.attempt),
            "MDM_PROFILE": claim.profile,
            "MDM_THREADS": str(profile.threads),
            "MDM_PROCESS_MEMORY_MIB": str(profile.process_memory_mib),
            "MDM_DUCKDB_MEMORY_MIB": str(profile.duckdb_memory_mib),
        }

        async def started(pid: int, created: float) -> bool:
            return await asyncio.to_thread(self.store.record_pid, claim, pid, created)

        async def beat(peak: int) -> str:
            try:
                return await asyncio.to_thread(self.store.heartbeat, claim, peak)
            except Exception:
                log.exception("heartbeat failed for %s/%s", claim.run_id, claim.step)
                return "ok"

        try:
            result = await run_lane(
                argv,
                env=env,
                cwd=s.lane.cwd,
                timeout_seconds=claim.timeout_seconds,
                rss_limit_mib=profile.process_memory_mib,
                rss_interval=s.monitor.rss_interval_seconds,
                heartbeat_interval=s.heartbeat.interval_seconds,
                kill_grace=s.monitor.kill_grace_seconds,
                stderr_tail_bytes=s.monitor.stderr_tail_bytes,
                stdout_tail_bytes=s.monitor.stdout_tail_bytes,
                on_started=started,
                heartbeat=beat,
                stop_event=self._stop,
            )
        except Exception as error:
            log.exception("engine error on %s/%s", claim.run_id, claim.step)
            result = LaneResult("engine_error", error=f"engine error: {error!r}", retryable=False)
        if result.outcome == "fenced":
            log.warning("%s/%s: stale owner fenced, no state committed", claim.run_id, claim.step)
            return
        for attempt in range(COMMIT_RETRIES):
            try:
                if not await asyncio.to_thread(self.store.finish_step, claim, result):
                    log.warning("%s/%s: finish rejected (fenced)", claim.run_id, claim.step)
                return
            except Exception:
                log.exception("cannot commit %s/%s (try %d)", claim.run_id, claim.step, attempt)
                await asyncio.sleep(min(2.0, 0.2 * (attempt + 1)))
        log.error("%s/%s: outcome lost; heartbeat expiry will flag it", claim.run_id, claim.step)

    # ------------------------------------------------------------ reprise / reaper

    def reap(self) -> None:
        """Détecte les tentatives orphelines ou à heartbeat expiré (exécuté dans un thread).

        - propriétaire mort sur cet hôte : l'arbre de processus restant est tué ; si l'arrêt est
          confirmé la tentative est « perdue » (réessai borné), sinon suspecte ;
        - heartbeat expiré sans preuve de mort du propriétaire (autre hôte, worker vivant) :
          tentative suspecte et run `needs_attention` ; le groupe d'écriture reste bloqué.
        """
        timeout = self.settings.heartbeat.timeout_seconds
        for row in self.store.foreign_running_steps(self.worker_id):
            run_id, step, gen = row["run_id"], row["step"], row["generation"]
            same_host = row["host"] == self.host
            owner_dead = (
                same_host
                and row["w_known"]
                and (row["w_stopped"] or not process_alive(row["w_pid"], row["w_started"]))
            )
            if owner_dead:
                if self._stop_leftovers(row):
                    log.warning("%s/%s: owner %s dead, attempt lost", run_id, step, row["owner"])
                    self.store.mark_lost(
                        run_id, step, gen, f"owner {row['owner']} died; process tree stopped"
                    )
                else:
                    self.store.mark_suspect(
                        run_id, step, gen, "owner died; leftover process not confirmed stopped"
                    )
            elif row["age"] is not None and row["age"] > timeout:
                self.store.mark_suspect(
                    run_id,
                    step,
                    gen,
                    f"heartbeat lost for {row['age']:.0f}s (owner {row['owner']} on {row['host']});"
                    " stop of the attempt cannot be confirmed",
                )

    def _stop_leftovers(self, row: dict[str, Any]) -> bool:
        confirmed = True
        pid = row["pid"]
        if pid is not None:
            confirmed &= kill_tree(pid, created=row["pid_started"])
        for proc in find_lane_processes(row["run_id"], row["kind"], row["source_id"]):
            confirmed &= kill_tree(proc.pid)
        return confirmed
