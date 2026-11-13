"""Configuration du runtime (hôte, profils, ordonnancement), distincte de la config référentiel.

Les clés suivent SPECIFICATIONS §5.1 ; voir engine/runtime/README.md pour la liste complète.
"""

from __future__ import annotations

from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, model_validator

RetryKind = Literal["exit_nonzero", "timeout", "rss_exceeded", "lost"]


class Strict(BaseModel):
    model_config = ConfigDict(extra="forbid")


class Host(Strict):
    ram_gib: float = Field(default=16, gt=0)
    job_memory_budget_mib: int = Field(default=6144, ge=1)
    max_running_steps: int = Field(default=2, ge=1)
    max_threads: int = Field(default=32, ge=1)
    max_queued_runs: int = Field(default=2000, ge=1)
    min_free_disk_gib: float = Field(default=30, ge=0)
    work_dir: str = "."  # volume dont l'espace libre est contrôlé à l'admission


class Profile(Strict):
    process_memory_mib: int = Field(ge=1)
    duckdb_memory_mib: int = Field(ge=1)
    threads: int = Field(ge=1)


def _default_profiles() -> dict[str, Profile]:
    return {
        "small": Profile(process_memory_mib=2048, duckdb_memory_mib=1024, threads=1),
        "medium": Profile(process_memory_mib=4096, duckdb_memory_mib=2048, threads=1),
        "large": Profile(process_memory_mib=6144, duckdb_memory_mib=4096, threads=2),
    }


class ReferentialLimits(Strict):
    max_running_steps: int = Field(default=3, ge=1)  # bornes les ingests parallèles
    max_queued_runs: int = Field(default=200, ge=1)


class Scheduler(Strict):
    poll_interval_seconds: float = Field(default=0.5, gt=0)
    aging_seconds: float = Field(default=300, ge=0)
    reaper_interval_seconds: float = Field(default=5, gt=0)
    retry_after_seconds: int = Field(default=30, ge=1)


class Heartbeat(Strict):
    interval_seconds: float = Field(default=5, gt=0)
    timeout_seconds: float = Field(default=30, gt=0)

    @model_validator(mode="after")
    def _check(self) -> Heartbeat:
        if self.timeout_seconds <= self.interval_seconds:
            raise ValueError("heartbeat timeout must be greater than the interval")
        return self


class Monitor(Strict):
    rss_interval_seconds: float = Field(default=1.0, gt=0)
    kill_grace_seconds: float = Field(default=5.0, ge=0)
    stderr_tail_bytes: int = Field(default=4096, ge=256)
    stdout_tail_bytes: int = Field(default=65536, ge=1024)


class Retry(Strict):
    backoff_base_seconds: float = Field(default=5, ge=0)
    backoff_max_seconds: float = Field(default=300, ge=0)
    jitter: float = Field(default=0.1, ge=0, le=1)
    retry_on: list[RetryKind] = Field(default_factory=lambda: ["exit_nonzero", "lost"])
    non_retryable_exit_codes: list[int] = Field(default_factory=lambda: [2])


class Lane(Strict):
    # Préfixe de la commande ; le moteur ajoute : <command> --config C [--source S] [--out D]
    # --run-id R. Jetons remplacés : {python}, {lane}.
    command: list[str] = Field(default_factory=lambda: ["{python}", "-m", "engine.lanes.{lane}"])
    python: str | None = None  # défaut : sys.executable
    env: dict[str, str] = Field(default_factory=dict)
    cwd: str | None = None


class RuntimeSettings(Strict):
    host: Host = Field(default_factory=Host)
    profiles: dict[str, Profile] = Field(default_factory=_default_profiles)
    referential: ReferentialLimits = Field(default_factory=ReferentialLimits)
    scheduler: Scheduler = Field(default_factory=Scheduler)
    heartbeat: Heartbeat = Field(default_factory=Heartbeat)
    monitor: Monitor = Field(default_factory=Monitor)
    retry: Retry = Field(default_factory=Retry)
    lane: Lane = Field(default_factory=Lane)

    @model_validator(mode="after")
    def _check(self) -> RuntimeSettings:
        if not self.profiles:
            raise ValueError("at least one profile is required")
        return self


def load_runtime_settings(path: str | Path | None) -> RuntimeSettings:
    if path is None:
        return RuntimeSettings()
    try:
        raw = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError) as error:
        raise ValueError(f"cannot read runtime config {path}: {error}") from error
    if raw is None:
        raw = {}
    if not isinstance(raw, dict):
        raise ValueError(f"runtime config {path} must be a YAML mapping")
    return RuntimeSettings.model_validate(raw)
