"""Exécution non bloquante d'une lane en sous-processus, supervisée (timeout, RSS, annulation)."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import subprocess
import sys
import time
from collections import deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

import psutil

log = logging.getLogger("engine.runtime")

MIB = 1024 * 1024


@dataclass
class LaneResult:
    # succeeded | failed | timeout | rss_exceeded | cancelled | fenced | invalid_summary |
    # kill_unconfirmed | engine_error | shutdown
    outcome: str
    exit_code: int | None = None
    summary: dict[str, Any] | None = None
    error: str | None = None
    stderr_tail: str = ""
    peak_rss_mib: int = 0
    pid: int | None = None
    retryable: bool = True


# Rappel de heartbeat : reçoit le pic RSS (MiB) et renvoie 'ok' | 'cancel' | 'fenced'.
HeartbeatFn = Callable[[int], Awaitable[str]]
StartedFn = Callable[[int, float], Awaitable[bool]]


def build_argv(
    template: list[str],
    *,
    python: str | None,
    lane: str,
    command: str,
    config: str,
    run_id: str,
    source: str | None = None,
    out: str | None = None,
) -> list[str]:
    values = {"python": python or sys.executable, "lane": lane}
    argv = [token.format(**values) for token in template]
    argv += [command, "--config", config]
    if source:
        argv += ["--source", source]
    if out:
        argv += ["--out", out]
    argv += ["--run-id", run_id]
    return argv


def process_alive(pid: int | None, created: float | None) -> bool:
    """Vrai si le processus existe encore (et n'est pas un pid recyclé)."""
    if pid is None:
        return False
    try:
        proc = psutil.Process(pid)
        if created is not None and abs(proc.create_time() - created) > 1.0:
            return False
        return proc.status() != psutil.STATUS_ZOMBIE
    except psutil.NoSuchProcess:
        return False
    except psutil.AccessDenied:
        return True


def tree_rss_mib(root: psutil.Process) -> int:
    total = 0
    try:
        procs = [root, *root.children(recursive=True)]
    except psutil.NoSuchProcess:
        return 0
    for p in procs:
        try:
            total += p.memory_info().rss
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    return total // MIB


def kill_tree(pid: int, *, grace: float = 0.0, created: float | None = None) -> bool:
    """Termine le processus et tous ses descendants ; True si l'arrêt est confirmé."""
    try:
        root = psutil.Process(pid)
        if created is not None and abs(root.create_time() - created) > 1.0:
            return True  # pid recyclé : le processus visé n'existe plus
        procs = [root, *root.children(recursive=True)]
    except psutil.NoSuchProcess:
        return True
    except psutil.AccessDenied:
        return False
    if grace > 0:
        for p in procs:
            try:
                p.terminate()
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
        _, procs = psutil.wait_procs(procs, timeout=grace)
    for _ in range(5):
        for p in list(procs):
            try:
                procs += [c for c in p.children(recursive=True) if c not in procs]
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
        for p in procs:
            try:
                p.kill()
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
        _, procs = psutil.wait_procs(procs, timeout=2.0)
        if not procs:
            return True
    return False


def find_lane_processes(run_id: str, kind: str, source_id: str | None) -> list[psutil.Process]:
    """Orphelins d'une tentative dont le pid n'a pas été enregistré (recherche par argv)."""
    found = []
    me = os.getpid()
    for proc in psutil.process_iter(["pid", "cmdline"]):
        argv = proc.info.get("cmdline") or []
        if proc.info["pid"] == me or kind not in argv or "--run-id" not in argv:
            continue
        if argv[argv.index("--run-id") + 1 :][:1] != [run_id]:
            continue
        has_source = "--source" in argv
        if source_id is None and has_source:
            continue
        if source_id is not None and (
            not has_source or argv[argv.index("--source") + 1 :][:1] != [source_id]
        ):
            continue
        found.append(proc)
    return found


def parse_summary(stdout: str) -> dict[str, Any] | None:
    """La synthèse est la dernière ligne non vide de stdout ; elle doit être un objet JSON."""
    for line in reversed(stdout.strip().splitlines()):
        line = line.strip()
        if not line:
            continue
        try:
            value = json.loads(line)
        except ValueError:
            return None
        return value if isinstance(value, dict) else None
    return None


async def _drain(stream: asyncio.StreamReader | None, limit: int) -> str:
    chunks: deque[bytes] = deque()
    size = 0
    while stream is not None:
        data = await stream.read(65536)
        if not data:
            break
        chunks.append(data)
        size += len(data)
        while size - len(chunks[0]) >= limit:
            size -= len(chunks.popleft())
    return b"".join(chunks)[-limit:].decode("utf-8", errors="replace")


async def _collect(task: asyncio.Task[str]) -> str:
    try:
        return await asyncio.wait_for(task, timeout=5)
    except (TimeoutError, asyncio.CancelledError):
        return ""


def _failure_message(exit_code: int | None, summary: dict[str, Any] | None, stderr: str) -> str:
    detail = (summary or {}).get("error")
    if not detail:
        lines = stderr.strip().splitlines()
        detail = lines[-1] if lines else "no output"
    return f"exit code {exit_code}: {detail}"


async def run_lane(
    argv: list[str],
    *,
    env: dict[str, str],
    cwd: str | None,
    timeout_seconds: float,
    rss_limit_mib: int,
    rss_interval: float,
    heartbeat_interval: float,
    kill_grace: float,
    stderr_tail_bytes: int,
    stdout_tail_bytes: int,
    on_started: StartedFn,
    heartbeat: HeartbeatFn,
    stop_event: asyncio.Event | None = None,
) -> LaneResult:
    """Lance la lane et la surveille ; toute issue normale est un LaneResult."""
    kwargs: dict[str, Any] = {}
    if sys.platform == "win32":
        kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
    else:
        kwargs["start_new_session"] = True
    try:
        proc = await asyncio.create_subprocess_exec(
            *argv,
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=env,
            cwd=cwd,
            **kwargs,
        )
    except OSError as error:
        return LaneResult("engine_error", error=f"cannot spawn lane: {error}", retryable=False)

    pid = proc.pid
    out_task = asyncio.create_task(_drain(proc.stdout, stdout_tail_bytes))
    err_task = asyncio.create_task(_drain(proc.stderr, stderr_tail_bytes))
    wait_task = asyncio.ensure_future(proc.wait())
    started = time.monotonic()
    peak = 0
    outcome: str | None = None
    reason = ""
    unconfirmed = False
    try:
        try:
            created = psutil.Process(pid).create_time()
        except psutil.NoSuchProcess:
            created = 0.0
        if not await on_started(pid, created):
            outcome, reason = "fenced", "attempt generation superseded before start"
        next_beat = started + heartbeat_interval
        while outcome is None and not wait_task.done():
            await asyncio.wait({wait_task}, timeout=rss_interval)
            if wait_task.done():
                break
            now = time.monotonic()
            try:
                rss = await asyncio.to_thread(tree_rss_mib, psutil.Process(pid))
            except psutil.NoSuchProcess:
                rss = 0
            peak = max(peak, rss)
            if rss > rss_limit_mib:
                outcome = "rss_exceeded"
                reason = f"RSS {rss} MiB exceeded the profile budget of {rss_limit_mib} MiB"
            elif now - started > timeout_seconds:
                outcome = "timeout"
                reason = f"step timeout after {timeout_seconds:.0f}s"
            elif stop_event is not None and stop_event.is_set():
                outcome, reason = "shutdown", "worker shutting down"
            elif now >= next_beat:
                next_beat = now + heartbeat_interval
                state = await heartbeat(peak)
                if state == "fenced":
                    outcome, reason = "fenced", "attempt generation superseded (stale owner)"
                elif state == "cancel":
                    outcome, reason = "cancelled", "run cancellation requested"
        if outcome is not None:
            graceful = outcome in {"cancelled", "shutdown"}
            confirmed = await asyncio.to_thread(
                kill_tree, pid, grace=kill_grace if graceful else 0.0
            )
            unconfirmed = not confirmed
    except BaseException:
        await asyncio.to_thread(kill_tree, pid)
        raise
    finally:
        try:
            await asyncio.wait_for(asyncio.shield(wait_task), timeout=5)
        except (TimeoutError, asyncio.CancelledError):
            pass
        stdout = await _collect(out_task)
        stderr = await _collect(err_task)

    summary = parse_summary(stdout)
    exit_code = proc.returncode
    base = {"exit_code": exit_code, "stderr_tail": stderr, "peak_rss_mib": peak, "pid": pid}
    if unconfirmed:
        error = f"{reason}; process tree {pid} could not be confirmed stopped"
        return LaneResult("kill_unconfirmed", error=error, **base)
    if outcome is not None:
        return LaneResult(outcome, error=reason, summary=summary, **base)
    if exit_code == 0:
        if summary is None:
            error = "lane exited 0 without a JSON summary line on stdout"
            return LaneResult("invalid_summary", error=error, retryable=False, **base)
        return LaneResult("succeeded", summary=summary, **base)
    retryable = bool(summary.get("retryable", True)) if summary else True
    error = _failure_message(exit_code, summary, stderr)
    return LaneResult("failed", summary=summary, error=error, retryable=retryable, **base)
