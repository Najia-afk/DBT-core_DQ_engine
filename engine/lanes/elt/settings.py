"""Réglages de la voie ELT (variables d'environnement, limites de ressources, DSN)."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path

from psycopg.conninfo import conninfo_to_dict, make_conninfo

from engine.core.config import ReferentialConfig

MAX_MEMORY_BYTES = 6 * 1024**3
MAX_THREADS = 8
_SIZE = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*(MB|GB|MIB|GIB)\s*$", re.IGNORECASE)


class ConfigurationError(RuntimeError):
    pass


def parse_memory(text: str) -> int:
    """Octets d'une limite DuckDB ('4GB', '512MB') ; refuse > 6 Go (contrainte de la machine)."""
    match = _SIZE.match(text)
    if not match:
        raise ConfigurationError(f"invalid memory limit {text!r} (expected e.g. 4GB or 512MB)")
    unit = match.group(2).upper()
    size = float(match.group(1)) * (1024**3 if unit.endswith("GB") or unit == "GIB" else 1024**2)
    if size > MAX_MEMORY_BYTES:
        raise ConfigurationError(f"memory limit {text!r} exceeds the 6GB cap")
    return int(size)


def format_memory(size: int) -> str:
    return f"{max(size // 1024**2, 64)}MB"


@dataclass(frozen=True)
class Settings:
    workdir: Path
    threads: int
    memory_bytes: int
    dbt_timeout_s: float | None
    keep_none_pairs: bool
    fault: str | None
    keep_work: bool = False

    @classmethod
    def from_env(cls, config: ReferentialConfig | None = None) -> Settings:
        env = os.environ
        threads = min(int(env.get("MDM_ELT_THREADS", "4")), MAX_THREADS)
        if threads < 1:
            raise ConfigurationError("MDM_ELT_THREADS must be >= 1")
        timeout = env.get("MDM_ELT_DBT_TIMEOUT_S")
        if timeout is None and config is not None:
            timeout_s: float | None = config.runtime.step_timeout_minutes * 60.0
        else:
            timeout_s = float(timeout) if timeout else None
        return cls(
            workdir=Path(env.get("MDM_ELT_WORKDIR", ".mdm_elt_work")).resolve(),
            threads=threads,
            memory_bytes=parse_memory(env.get("MDM_ELT_MEMORY_LIMIT", "4GB")),
            dbt_timeout_s=timeout_s,
            keep_none_pairs=env.get("MDM_ELT_KEEP_NONE_PAIRS", "0") == "1",
            fault=env.get("MDM_ELT_FAULT") or None,
            keep_work=env.get("MDM_ELT_KEEP_WORK", "0") == "1",
        )

    def share(self, jobs: int) -> Settings:
        """Part de ressources d'un job parmi `jobs` jobs concurrents."""
        jobs = max(jobs, 1)
        return Settings(
            self.workdir,
            max(self.threads // jobs, 1),
            max(self.memory_bytes // jobs, 256 * 1024**2),
            self.dbt_timeout_s,
            self.keep_none_pairs,
            self.fault,
            self.keep_work,
        )

    def referential_dir(self, config: ReferentialConfig) -> Path:
        path = self.workdir / f"{config.referential}-{config.state.schema_name}"
        path.mkdir(parents=True, exist_ok=True)
        marker = self.workdir / ".gitignore"
        if not marker.exists():
            marker.write_text("*\n", encoding="utf-8")
        return path


def state_dsn(config: ReferentialConfig) -> str:
    return env_secret(config.state.dsn_env)


def env_secret(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise ConfigurationError(f"environment variable {name} is not set")
    return value


def split_dsn(dsn: str) -> tuple[str, dict[str, str]]:
    """(chaîne de connexion libpq SANS mot de passe, variables PG* contenant le secret)."""
    params = {k: str(v) for k, v in conninfo_to_dict(dsn).items() if v is not None}
    env: dict[str, str] = {}
    password = params.pop("password", None)
    if password is not None:
        env["PGPASSWORD"] = password
    return make_conninfo(**params), env
