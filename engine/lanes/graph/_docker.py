"""Disposable AGE container helpers for the smoke / bench tools (never touches other containers)."""

from __future__ import annotations

import secrets
import subprocess
import threading
import time
from dataclasses import dataclass, field

import psycopg

IMAGE = "apache/age:release_PG16_1.6.0"
PASSWORD = "mdm2-graph-local-only"
MEMORY = "6g"


def docker(*args: str, timeout: int = 300) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["docker", *args], capture_output=True, text=True, timeout=timeout, check=False
    )


@dataclass
class Container:
    name: str
    port: int
    peak_bytes: int = 0
    samples: list[int] = field(default_factory=list)
    _stop: threading.Event = field(default_factory=threading.Event)
    _thread: threading.Thread | None = None

    def dsn(self, db: str = "postgres") -> str:
        return f"host=127.0.0.1 port={self.port} dbname={db} user=mdm password={PASSWORD}"

    def stats(self) -> int:
        out = docker("stats", "--no-stream", "--format", "{{.MemUsage}}", self.name).stdout
        used = out.split("/")[0].strip()
        for unit, mult in (("GiB", 2**30), ("MiB", 2**20), ("KiB", 2**10), ("B", 1)):
            if used.endswith(unit):
                return int(float(used[: -len(unit)]) * mult)
        return 0

    def start_watch(self) -> None:
        self._stop.clear()

        def loop() -> None:
            while not self._stop.is_set():
                try:
                    value = self.stats()
                except Exception:  # noqa: BLE001 - monitoring must not break the run
                    value = 0
                self.samples.append(value)
                self.peak_bytes = max(self.peak_bytes, value)

        self._thread = threading.Thread(target=loop, daemon=True)
        self._thread.start()

    def stop_watch(self) -> int:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=30)
        return self.peak_bytes

    def remove(self) -> None:
        docker("rm", "-f", "-v", self.name)


def start(tag: str, extra: list[str] | None = None, tmpfs: str | None = None) -> Container:
    """`mdm2-graph-<tag>-xxxx`, 6 GB RAM+swap cap, 512 MB shm, random localhost port."""
    name = f"mdm2-graph-{tag}-{secrets.token_hex(2)}"
    args = [
        "run",
        "-d",
        "--rm",
        "--pull",
        "never",
        "--name",
        name,
        "--memory",
        MEMORY,
        "--memory-swap",
        MEMORY,
        "--shm-size",
        "512m",
        "-e",
        "POSTGRES_USER=mdm",
        "-e",
        f"POSTGRES_PASSWORD={PASSWORD}",
        "-e",
        "POSTGRES_DB=postgres",
        "-p",
        "127.0.0.1::5432",
    ]
    if tmpfs:
        args += ["--tmpfs", f"/var/lib/postgresql/data:rw,size={tmpfs}"]
    args += [
        IMAGE,
        "postgres",
        "-c",
        "shared_buffers=1GB",
        "-c",
        "max_wal_size=4GB",
        "-c",
        "max_connections=60",
        *(extra or []),
    ]
    res = docker(*args)
    if res.returncode != 0:
        raise RuntimeError(f"docker run failed: {res.stderr.strip()}")
    try:
        port = 0
        for _ in range(40):
            out = docker("port", name, "5432/tcp").stdout.strip()
            if out:
                port = int(out.splitlines()[0].rsplit(":", 1)[1])
                break
            time.sleep(0.5)
        container = Container(name, port)
        for _ in range(180):
            try:
                psycopg.connect(container.dsn(), autocommit=True).close()
                return container
            except psycopg.OperationalError:
                time.sleep(0.5)
        raise RuntimeError("container did not become ready")
    except BaseException:
        docker("rm", "-f", "-v", name)
        raise
