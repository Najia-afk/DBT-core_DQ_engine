"""Conteneur PostgreSQL jetable d'un banc (`mdm2-bench-*` uniquement : jamais d'autre conteneur)."""

from __future__ import annotations

import secrets
import subprocess
import time
from dataclasses import dataclass, field

import psycopg

PREFIX = "mdm2-bench-"
PG_IMAGE = "postgres:16.4-bookworm"
AGE_IMAGE = "apache/age:release_PG16_1.6.0"
IMAGES = {"etl": PG_IMAGE, "elt": PG_IMAGE, "graph": AGE_IMAGE, "meta": PG_IMAGE}
PASSWORD = "mdm2-bench-local-only"
USER = "mdm"
DB = "bench"


def docker(*args: str, timeout: int = 600) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["docker", *args], capture_output=True, text=True, timeout=timeout, check=False
    )


def assert_bench_name(name: str) -> None:
    if not name.startswith(PREFIX):
        raise ValueError(f"refus de toucher un conteneur hors {PREFIX}* : {name!r}")


@dataclass(frozen=True)
class PgTuning:
    """Réglages PostgreSQL pour une boîte de 12 Go ; `synchronous_commit` est publié au rapport."""

    memory: str = "12g"
    shm_size: str = "1g"
    shared_buffers: str = "3GB"
    work_mem: str = "64MB"
    maintenance_work_mem: str = "1GB"
    effective_cache_size: str = "8GB"
    max_wal_size: str = "16GB"
    min_wal_size: str = "2GB"
    checkpoint_timeout: str = "30min"
    synchronous_commit: str = "off"
    max_connections: int = 100

    def settings(self) -> list[str]:
        pairs = {
            "shared_buffers": self.shared_buffers,
            "work_mem": self.work_mem,
            "maintenance_work_mem": self.maintenance_work_mem,
            "effective_cache_size": self.effective_cache_size,
            "max_wal_size": self.max_wal_size,
            "min_wal_size": self.min_wal_size,
            "checkpoint_timeout": self.checkpoint_timeout,
            "synchronous_commit": self.synchronous_commit,
            "max_connections": str(self.max_connections),
        }
        out: list[str] = []
        for key, value in pairs.items():
            out += ["-c", f"{key}={value}"]
        return out

    def describe(self) -> dict[str, str | int]:
        return {k: getattr(self, k) for k in self.__dataclass_fields__}


SMALL_TUNING = PgTuning(
    memory="3g",
    shm_size="256m",
    shared_buffers="512MB",
    work_mem="32MB",
    maintenance_work_mem="256MB",
    effective_cache_size="2GB",
    max_wal_size="4GB",
    min_wal_size="512MB",
)


def run_args(name: str, image: str, tuning: PgTuning) -> list[str]:
    """Arguments de `docker run` : mémoire et mémoire+swap égales, port local aléatoire."""
    assert_bench_name(name)
    return [
        "run", "-d", "--pull", "never", "--name", name,
        "--memory", tuning.memory, "--memory-swap", tuning.memory,
        "--shm-size", tuning.shm_size,
        "-e", f"POSTGRES_USER={USER}", "-e", f"POSTGRES_PASSWORD={PASSWORD}",
        "-e", f"POSTGRES_DB={DB}",
        "-p", "127.0.0.1::5432",
        image, "postgres", *tuning.settings(),
    ]  # fmt: skip


@dataclass
class PgContainer:
    lane: str
    tuning: PgTuning = field(default_factory=PgTuning)
    tag: str = ""
    name: str = ""
    port: int = 0

    def __post_init__(self) -> None:
        if not self.name:
            suffix = f"-{self.tag}" if self.tag else ""
            self.name = f"{PREFIX}{self.lane}{suffix}"

    @property
    def image(self) -> str:
        return IMAGES[self.lane]

    def dsn(self, db: str = DB) -> str:
        return (
            f"host=127.0.0.1 port={self.port} dbname={db} user={USER} "
            f"password={PASSWORD} connect_timeout=10"
        )

    def start(self, timeout: float = 180) -> PgContainer:
        self.remove()
        res = docker(*run_args(self.name, self.image, self.tuning))
        if res.returncode != 0:
            raise RuntimeError(f"docker run a échoué : {res.stderr.strip()}")
        try:
            for _ in range(60):
                out = docker("port", self.name, "5432/tcp").stdout.strip()
                if out:
                    self.port = int(out.splitlines()[0].rsplit(":", 1)[1])
                    break
                time.sleep(0.5)
            deadline = time.monotonic() + timeout
            streak = 0
            while time.monotonic() < deadline and streak < 3:
                try:
                    psycopg.connect(self.dsn(), connect_timeout=2).close()
                    streak += 1
                except psycopg.Error:
                    streak = 0
                time.sleep(0.5)
            if streak < 3:
                raise RuntimeError("PostgreSQL ne démarre pas")
        except BaseException:
            self.remove()
            raise
        return self

    def remove(self) -> None:
        assert_bench_name(self.name)
        docker("rm", "-f", "-v", self.name)

    def exists(self) -> bool:
        return bool(docker("ps", "-aq", "--filter", f"name=^{self.name}$").stdout.strip())

    def version(self) -> str:
        with psycopg.connect(self.dsn(), autocommit=True) as conn:
            return str(conn.execute("SHOW server_version").fetchone()[0])  # type: ignore[index]

    def db_bytes(self) -> int:
        with psycopg.connect(self.dsn(), autocommit=True) as conn:
            row = conn.execute("SELECT pg_database_size(current_database())").fetchone()
            return int(row[0])  # type: ignore[index]

    def wal_lsn(self) -> int:
        with psycopg.connect(self.dsn(), autocommit=True) as conn:
            row = conn.execute("SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), '0/0')").fetchone()
            return int(row[0])  # type: ignore[index]

    def pgdata_bytes(self) -> int:
        """Taille du répertoire de données (WAL inclus), mesurée dans le conteneur."""
        out = docker("exec", self.name, "du", "-sk", "/var/lib/postgresql/data").stdout.split()
        return int(out[0]) * 1024 if out else 0

    def scalar(self, query: str) -> object:
        with psycopg.connect(self.dsn(), autocommit=True) as conn:
            row = conn.execute(query).fetchone()
            return row[0] if row else None


def stray_bench_containers() -> list[str]:
    out = docker("ps", "-a", "--filter", f"name={PREFIX}", "--format", "{{.Names}}").stdout
    return [n for n in out.split() if n.startswith(PREFIX)]


def unique_tag() -> str:
    return secrets.token_hex(2)
