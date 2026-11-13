"""Exécution de commandes surveillées (journaux sur disque, synthèse JSON de la lane)."""

from __future__ import annotations

import json
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

from .guards import Monitor

PYTHON = sys.executable


@dataclass
class StepResult:
    label: str
    argv: list[str]
    returncode: int
    wall_s: float
    summary: dict | None = None
    stderr_tail: str = ""
    killed: bool = False
    extra: dict = field(default_factory=dict)

    @property
    def ok(self) -> bool:
        return self.returncode == 0 and not self.killed


def last_json_line(text: str) -> dict | None:
    for line in reversed(text.splitlines()):
        line = line.strip()
        if line.startswith("{") and line.endswith("}"):
            try:
                value = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(value, dict):
                return value
    return None


def tail(path: Path, size: int = 3000) -> str:
    try:
        data = path.read_bytes()
    except OSError:
        return ""
    return data[-size:].decode("utf-8", errors="replace")


def start_command(
    argv: list[str], *, env: dict[str, str], cwd: Path, log_dir: Path, label: str
) -> tuple[subprocess.Popen, Path, Path]:
    log_dir.mkdir(parents=True, exist_ok=True)
    out_path, err_path = log_dir / f"{label}.out", log_dir / f"{label}.err"
    with out_path.open("wb") as out, err_path.open("wb") as err:
        proc = subprocess.Popen(  # noqa: S603 - arguments construits par le banc
            argv, env=env, cwd=cwd, stdout=out, stderr=err, stdin=subprocess.DEVNULL
        )
    return proc, out_path, err_path


def run_command(
    argv: list[str],
    *,
    env: dict[str, str],
    cwd: Path,
    log_dir: Path,
    label: str,
    monitor: Monitor,
) -> StepResult:
    """Lance `argv`, l'enregistre dans le moniteur, attend sa fin ; stdout/stderr dans log_dir."""
    t0 = time.perf_counter()
    proc, out_path, err_path = start_command(argv, env=env, cwd=cwd, log_dir=log_dir, label=label)
    monitor.add_root(proc.pid)
    code = proc.wait()
    wall = time.perf_counter() - t0
    summary = last_json_line(out_path.read_text(encoding="utf-8", errors="replace"))
    return StepResult(
        label=label,
        argv=argv,
        returncode=code,
        wall_s=wall,
        summary=summary,
        stderr_tail=tail(err_path),
        killed=monitor.abort_reason is not None,
    )
