"""Exécution de dbt en sous-processus asynchrone : délai maximal, arrêt de l'arbre de processus,
capture des journaux. La boucle d'événements n'est jamais bloquée."""

from __future__ import annotations

import asyncio
import json
import os
import sys
import time
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import psutil

DBT_LAUNCHER = "import sys; from dbt.cli.main import cli; sys.exit(cli())"


@dataclass
class DbtResult:
    returncode: int
    seconds: float
    log_path: Path
    models: list[dict[str, Any]] = field(default_factory=list)
    tail: str = ""

    @property
    def model_seconds(self) -> dict[str, float]:
        return {m["name"]: m["seconds"] for m in self.models}


class DbtError(RuntimeError):
    def __init__(self, message: str, result: DbtResult | None = None) -> None:
        super().__init__(message)
        self.result = result


def kill_tree(pid: int) -> None:
    """Tue le processus et tous ses descendants (dbt lance parfois des workers)."""
    try:
        parent = psutil.Process(pid)
    except psutil.NoSuchProcess:
        return
    procs = [*parent.children(recursive=True), parent]
    for proc in procs:
        try:
            proc.kill()
        except psutil.NoSuchProcess:
            pass
    psutil.wait_procs(procs, timeout=10)


def _read_run_results(target: Path) -> list[dict[str, Any]]:
    path = target / "run_results.json"
    if not path.exists():
        return []
    raw = json.loads(path.read_text(encoding="utf-8"))
    return [
        {
            "name": r["unique_id"].rsplit(".", 1)[-1],
            "status": r["status"],
            "seconds": round(float(r.get("execution_time") or 0.0), 3),
            "message": r.get("message") or "",
        }
        for r in raw.get("results", [])
    ]


async def run_dbt(
    project: Path,
    work: Path,
    env: dict[str, str],
    variables: dict[str, Any],
    timeout_s: float | None,
    command: str = "run",
) -> DbtResult:
    """Lance `dbt <command>` sur le projet généré ; lève DbtError sur échec ou délai dépassé."""
    work.mkdir(parents=True, exist_ok=True)
    target, logs = work / "target", work / "logs"
    log_path = work / "dbt.out.log"
    args = [
        sys.executable,
        "-c",
        DBT_LAUNCHER,
        command,
        "--project-dir",
        str(project),
        "--profiles-dir",
        str(project),
        "--target-path",
        str(target),
        "--log-path",
        str(logs),
        "--no-use-colors",
        "--vars",
        json.dumps(variables),
    ]
    full_env = {
        **os.environ,
        **env,
        "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
        "DBT_VERSION_CHECK": "false",
        "PYTHONUTF8": "1",
        "PYTHONDONTWRITEBYTECODE": "1",
    }
    started = time.perf_counter()
    tail: deque[str] = deque(maxlen=60)
    proc = await asyncio.create_subprocess_exec(
        *args,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
        env=full_env,
        cwd=str(project),
    )

    async def pump() -> None:
        assert proc.stdout is not None
        with log_path.open("w", encoding="utf-8") as log:
            async for raw in proc.stdout:
                line = raw.decode("utf-8", errors="replace").rstrip()
                log.write(line + "\n")
                tail.append(line)

    reader = asyncio.create_task(pump())
    try:
        await asyncio.wait_for(proc.wait(), timeout=timeout_s)
    except TimeoutError:
        kill_tree(proc.pid)
        await reader
        raise DbtError(f"dbt timed out after {timeout_s:.0f}s (process tree killed)") from None
    except asyncio.CancelledError:
        kill_tree(proc.pid)
        raise
    await reader
    result = DbtResult(
        proc.returncode or 0,
        round(time.perf_counter() - started, 3),
        log_path,
        _read_run_results(target),
        "\n".join(tail),
    )
    if result.returncode != 0:
        failed = [f"{m['name']}: {m['message']}" for m in result.models if m["status"] == "error"]
        detail = "; ".join(failed) if failed else result.tail[-1500:]
        raise DbtError(f"dbt failed (exit {result.returncode}): {detail}", result)
    return result
