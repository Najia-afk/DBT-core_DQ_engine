"""Banc d'une voie : conteneur PostgreSQL, jours 0..N, mesures par phase, JSON du run."""

from __future__ import annotations

import hashlib
import json
import os
import platform
import shutil
import subprocess
import threading
import time
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from importlib import metadata
from pathlib import Path
from typing import Any

import psutil

from .dataset import (
    DatasetSpec,
    day_rows,
    dir_bytes,
    ensure_dataset,
    load_manifest,
    load_pg_command,
    prepare_config,
    stage_live,
)
from .guards import Limits, Monitor, free_disk_gb
from .pg import PgContainer, PgTuning, docker
from .procs import PYTHON, StepResult, run_command

REPO_ROOT = Path(__file__).resolve().parents[2]
MIB = 1024 * 1024
DEFAULT_INGEST_MODE = {"etl": "parallel", "elt": "native", "graph": "sequential"}
NODE_KEYS = ("delta_nodes", "nodes_delta", "pending_nodes", "pending", "delta")
KEEP_SUMMARY = ("status", "replayed", "counts", "counters", "seconds", "total_seconds")


@dataclass
class RunSpec:
    lane: str
    dataset: DatasetSpec
    name: str
    root: Path
    out_dir: Path
    ingest_mode: str | None = None
    batch_rows: int | None = None
    elt_memory: str = "6GB"
    elt_threads: int = 8
    etl_workers: int = 8
    tuning: PgTuning = field(default_factory=PgTuning)
    limits: Limits = field(default_factory=Limits)
    export: str = "none"  # none | final | each
    export_root: Path | None = None
    keep_dataset: bool = False
    keep_container: bool = False
    tag: str = ""
    extra_env: dict[str, str] = field(default_factory=dict)

    def mode(self) -> str:
        return self.ingest_mode or DEFAULT_INGEST_MODE[self.lane]


def trim_summary(summary: dict | None) -> dict | None:
    """Garde les compteurs et durées ; écarte les gros blocs (modèles dbt, listes)."""
    if summary is None:
        return None
    out = {k: summary[k] for k in KEEP_SUMMARY if k in summary}
    counters = out.get("counters")
    if isinstance(counters, dict) and isinstance(counters.get("dbt_models"), dict):
        models = counters["dbt_models"]
        out["counters"] = {k: v for k, v in counters.items() if k != "dbt_models"}
        top = sorted(models.items(), key=lambda kv: kv[1], reverse=True)[:5]
        out["dbt_top_models_s"] = dict(top)
    if "error" in summary:
        out["error"] = str(summary["error"])[:500]
    for key, value in summary.items():
        if key not in out and isinstance(value, int | float | bool) and key != "wall_seconds":
            out[key] = value
    return out


def find_int(summary: dict | None, keys: tuple[str, ...]) -> int | None:
    """Premier compteur entier parmi `keys`, cherché dans counts/counters puis à la racine."""
    if not summary:
        return None
    for holder in (summary.get("counts"), summary.get("counters"), summary):
        if isinstance(holder, dict):
            for key in keys:
                value = holder.get(key)
                if isinstance(value, int) and not isinstance(value, bool):
                    return value
    return None


def machine_info() -> dict[str, Any]:
    def version(pkg: str) -> str | None:
        try:
            return metadata.version(pkg)
        except metadata.PackageNotFoundError:
            return None

    docker_v = docker("version", "--format", "{{.Server.Version}}", timeout=30).stdout.strip()
    docker_mem = docker("info", "--format", "{{.MemTotal}}", timeout=30).stdout.strip()
    return {
        "date": datetime.now(UTC).astimezone().isoformat(timespec="seconds"),
        "os": platform.platform(),
        "cpu_threads": os.cpu_count(),
        "ram_gb": round(psutil.virtual_memory().total / 1024**3, 1),
        "python": platform.python_version(),
        "duckdb": version("duckdb"),
        "pyarrow": version("pyarrow"),
        "psycopg": version("psycopg"),
        "numpy": version("numpy"),
        "rapidfuzz": version("rapidfuzz"),
        "dbt-core": version("dbt-core"),
        "dbt-duckdb": version("dbt-duckdb"),
        "docker_server": docker_v,
        "docker_vm_mem_gb": round(int(docker_mem) / 1024**3, 1) if docker_mem.isdigit() else None,
        "git_head": subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True,
            text=True,
            cwd=REPO_ROOT,
            check=False,
        ).stdout.strip(),
    }


def config_hash(spec: RunSpec) -> str:
    payload = {
        "lane": spec.lane,
        "dataset": spec.dataset.key(),
        "mode": spec.mode(),
        "batch_rows": spec.batch_rows,
        "elt": [spec.elt_memory, spec.elt_threads],
        "etl_workers": spec.etl_workers,
        "tuning": spec.tuning.describe(),
    }
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()[:12]


def lane_argv(lane: str, command: str, config: Path, *extra: str) -> list[str]:
    return [PYTHON, "-m", f"engine.lanes.{lane}", command, "--config", str(config), *extra]


def pretty(argv: list[str]) -> str:
    return " ".join(["python", *[a for a in argv[1:]]])


class LaneBench:
    """Un run complet. Utilisable comme contexte : le conteneur est supprimé à la sortie."""

    def __init__(self, spec: RunSpec, *, log: Callable[[str], None] = print) -> None:
        self.spec = spec
        self.log = log
        tag = spec.tag
        self.pg = PgContainer(spec.lane, spec.tuning, tag=tag)
        self.run_dir = spec.root / f"run-{spec.name}"
        self.work_dir = self.run_dir / "work"
        self.log_dir = self.run_dir / "logs"
        self.phases: list[dict[str, Any]] = []
        self.commands: list[str] = []
        self.result: dict[str, Any] = {}
        self.status = "ok"
        self.host_baseline_gb = round(psutil.virtual_memory().used / 1024**3, 2)
        self.abort_reason: str | None = None
        self.ds: Path | None = None
        self.config: Path | None = None
        self.exports: dict[str, str] = {}
        self._manifest: dict[str, Any] = {}

    # -- environnement --------------------------------------------------------------
    def env(self) -> dict[str, str]:
        s = self.spec
        env = {
            **os.environ,
            "PYTHONDONTWRITEBYTECODE": "1",
            "PYTHONUNBUFFERED": "1",
            "MDM_STATE_DSN": self.pg.dsn(),
        }
        if s.lane == "elt":
            env["MDM_ELT_WORKDIR"] = str(self.work_dir)
            env["MDM_ELT_MEMORY_LIMIT"] = s.elt_memory
            env["MDM_ELT_THREADS"] = str(s.elt_threads)
        if s.lane == "etl":
            env["MDM_ETL_WORKERS"] = str(s.etl_workers)
        env.update(s.extra_env)
        for src in self._manifest.get("sources", []):
            if src["kind"] == "postgres":
                env[f"MDM_SRC_{src['id'].upper()}_DSN"] = self.pg.dsn()
        return env

    def probes(self) -> dict[str, float]:
        out = {"workdir_mb": dir_bytes(self.work_dir) / MIB}
        out["pgdata_mb"] = self.pg.pgdata_bytes() / MIB
        return out

    # -- phases ---------------------------------------------------------------------
    def phase(
        self,
        day: int,
        name: str,
        steps: list[tuple[str, list[str]]],
        *,
        parallel: bool = False,
        rows: int | None = None,
        env: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        env = env or self.env()
        wal0 = self.pg.wal_lsn()
        results: list[StepResult] = []
        lock = threading.Lock()
        t0 = time.perf_counter()
        with Monitor(
            self.spec.limits,
            container=self.pg.name,
            disk_path=str(self.spec.root),
            probes=self.probes,
        ) as mon:

            def one(label: str, argv: list[str]) -> None:
                res = run_command(
                    argv, env=env, cwd=REPO_ROOT, log_dir=self.log_dir,
                    label=f"d{day}-{label}", monitor=mon,
                )  # fmt: skip
                with lock:
                    results.append(res)

            if parallel and len(steps) > 1:
                threads = [threading.Thread(target=one, args=s) for s in steps]
                for t in threads:
                    t.start()
                for t in threads:
                    t.join()
            else:
                for label, argv in steps:
                    one(label, argv)
                    if not results[-1].ok:
                        break
            wall = time.perf_counter() - t0
        self.commands += [pretty(a) for _, a in steps]
        failed = [r for r in results if not r.ok]
        rec: dict[str, Any] = {
            "day": day,
            "phase": name,
            "wall_s": round(wall, 2),
            "rows": rows,
            "rows_per_s": round(rows / wall, 1) if rows and wall > 0 else None,
            **mon.summary(),
            "pg_db_mb": round(self.pg.db_bytes() / MIB, 1),
            "pgdata_mb": round(self.pg.pgdata_bytes() / MIB, 1),
            "wal_mb": round((self.pg.wal_lsn() - wal0) / MIB, 1),
            "workdir_mb": round(dir_bytes(self.work_dir) / MIB, 1),
            "steps": [
                {
                    "label": r.label,
                    "wall_s": round(r.wall_s, 2),
                    "rc": r.returncode,
                    "summary": trim_summary(r.summary),
                }
                for r in sorted(results, key=lambda r: r.label)
            ],
            "status": "failed" if failed else "ok",
        }
        if mon.abort_reason:
            rec["status"] = "aborted"
            self.abort_reason = mon.abort_reason
        elif failed:
            rec["error"] = (failed[0].summary or {}).get("error") or failed[0].stderr_tail[-800:]
        if name == "reconcile" and results and results[0].summary:
            nodes = find_int(results[0].summary, NODE_KEYS)
            rec["delta_nodes"] = nodes
            if nodes and wall > 0:
                rec["rows"] = nodes
                rec["rows_per_s"] = round(nodes / wall, 1)
        self.phases.append(rec)
        self.log(
            f"[{self.spec.name}] day{day} {name}: {rec['status']} {rec['wall_s']}s "
            f"rows={rec['rows']} rss={rec['rss_peak_mb']}MB pg={rec['container_mem_peak_mb']}MB "
            f"db={rec['pg_db_mb']}MB free={rec['disk_free_min_gb']}GB"
        )
        if rec["status"] != "ok":
            self.status = rec["status"]
        return rec

    def day(self, day: int, *, init: bool = False) -> bool:
        s = self.spec
        assert self.ds is not None and self.config is not None
        staged = stage_live(self.ds, day)
        if s.dataset.uses_postgres_sources():
            load = [load_pg_command(self.ds, "MDM_STATE_DSN", day)]
            if not self.phase(day, "load-pg", [("load-pg", load[0])])["status"] == "ok":
                return False
        rows = day_rows(self._manifest, day)
        if init:
            rec = self.phase(day, "init", [("init", lane_argv(s.lane, "init", self.config))])
            if rec["status"] != "ok":
                return False
        run_id = f"d{day}"
        sources = [src["id"] for src in self._manifest["sources"]]
        ingest_rows = sum(rows.values())
        mode = s.mode()
        if mode == "native" and s.lane == "elt":
            extra = [x for sid in sources for x in ("--source", sid)]
            steps = [
                ("ingest", lane_argv(s.lane, "ingest", self.config, *extra, "--run-id", run_id))
            ]
        else:
            steps = [
                (f"ingest-{sid}", lane_argv(s.lane, "ingest", self.config, "--source", sid,
                                            "--run-id", run_id))
                for sid in sources
            ]  # fmt: skip
        rec = self.phase(day, "ingest", steps, parallel=mode == "parallel", rows=ingest_rows)
        rec["staged_files"] = staged
        if rec["status"] != "ok":
            return False
        rec = self.phase(
            day,
            "reconcile",
            [("reconcile", lane_argv(s.lane, "reconcile", self.config, "--run-id", run_id))],
        )
        if rec["status"] != "ok":
            return False
        if s.export == "each" or (s.export == "final" and day == s.dataset.days):
            return self.export(day)
        return True

    def export(self, day: int) -> bool:
        s = self.spec
        assert self.config is not None
        base = s.export_root or (s.root / "exports" / s.name)
        out = base / f"day{day}"
        shutil.rmtree(out, ignore_errors=True)
        out.mkdir(parents=True)
        rec = self.phase(
            day, "export", [("export", lane_argv(s.lane, "export", self.config, "--out", str(out)))]
        )
        rec["export_mb"] = round(dir_bytes(out) / MIB, 1)
        self.exports[str(day)] = str(out)
        return rec["status"] == "ok"

    # -- run ------------------------------------------------------------------------
    def execute(self) -> dict[str, Any]:
        s = self.spec
        s.out_dir.mkdir(parents=True, exist_ok=True)
        s.root.mkdir(parents=True, exist_ok=True)
        t_start = time.perf_counter()
        info: dict[str, Any] = {}
        try:
            if free_disk_gb(str(s.root)) < s.limits.min_free_disk_gb:
                raise RuntimeError("disk_free_below_min avant démarrage")
            self.ds, info = ensure_dataset(s.dataset, s.root, limits=s.limits, env=dict(os.environ))
            self._manifest = load_manifest(self.ds)
            self.work_dir.mkdir(parents=True, exist_ok=True)
            self.config = prepare_config(self.ds, self.run_dir, batch_rows=s.batch_rows)
            self.pg.start()
            for day in range(s.dataset.days + 1):
                if not self.day(day, init=day == 0):
                    break
        except BaseException as exc:  # noqa: BLE001 - le JSON du run doit toujours être écrit
            self.status = "error"
            self.abort_reason = f"{type(exc).__name__}: {exc}"[:500]
        finally:
            self.result = self.build_result(info, time.perf_counter() - t_start)
            self.cleanup()
        path = s.out_dir / f"{s.name}.json"
        path.write_text(json.dumps(self.result, indent=1, default=str), encoding="utf-8")
        return self.result

    def build_result(self, dataset_info: dict[str, Any], wall: float) -> dict[str, Any]:
        s = self.spec
        phases = self.phases
        return {
            "schema": 1,
            "name": s.name,
            "lane": s.lane,
            "status": self.status,
            "abort_reason": self.abort_reason,
            "machine": machine_info(),
            "pg_server": self.pg.version() if self.pg.port and self.pg.exists() else None,
            "pg_image": self.pg.image,
            "config": {
                "hash": config_hash(s),
                "nodes": s.dataset.nodes,
                "attributes": s.dataset.attributes,
                "sources": s.dataset.sources,
                "kinds": list(s.dataset.kinds),
                "snapshots": list(s.dataset.snapshots),
                "seed": s.dataset.seed,
                "deltas": list(s.dataset.deltas),
                "days": s.dataset.days,
                "ingest_mode": s.mode(),
                "batch_rows": s.batch_rows or "config (50000)",
                "elt_memory_limit": s.elt_memory if s.lane == "elt" else None,
                "elt_threads": s.elt_threads if s.lane == "elt" else None,
                "etl_workers": s.etl_workers if s.lane == "etl" else None,
                "pg_tuning": s.tuning.describe(),
                "limits": asdict(s.limits),
            },
            "dataset": dataset_info,
            "commands": self.commands,
            "phases": phases,
            "exports": self.exports,
            "totals": {
                "wall_s": round(wall, 1),
                "phases_wall_s": round(sum(p["wall_s"] for p in phases), 1),
                "total_peak_mb": max((p["total_peak_mb"] for p in phases), default=0),
                "rss_peak_mb": max((p["rss_peak_mb"] for p in phases), default=0),
                "container_mem_peak_mb": max(
                    (p["container_mem_peak_mb"] or 0 for p in phases), default=0
                ),
                "host_used_peak_gb": max((p["host_used_peak_gb"] for p in phases), default=0),
                "host_used_baseline_gb": self.host_baseline_gb,
                "pgdata_peak_mb": max(
                    (max(p["pgdata_mb"], p.get("pgdata_mb_peak", 0)) for p in phases), default=0
                ),
                "pg_db_mb_final": phases[-1]["pg_db_mb"] if phases else None,
                "disk_free_min_gb": min((p["disk_free_min_gb"] for p in phases), default=None),
            },
        }

    def keep_failure_logs(self) -> None:
        dest = self.spec.out_dir / "logs" / self.spec.name
        if self.log_dir.exists():
            shutil.copytree(self.log_dir, dest, dirs_exist_ok=True)
        for pattern in ("*.log", "run_results.json"):
            for path in self.work_dir.rglob(pattern):
                if path.stat().st_size < 3 * MIB:
                    target = dest / "work" / path.relative_to(self.work_dir)
                    target.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(path, target)

    def cleanup(self) -> None:
        s = self.spec
        if not s.keep_container:
            try:
                self.pg.remove()
            except Exception:  # noqa: BLE001
                pass
        if self.status != "ok":
            self.keep_failure_logs()
        shutil.rmtree(self.run_dir, ignore_errors=True)
        if self.ds is not None and not s.keep_dataset:
            shutil.rmtree(self.ds, ignore_errors=True)
            shutil.rmtree(self.ds.parent / f"logs-{s.dataset.key()}", ignore_errors=True)


def run_lane_bench(spec: RunSpec) -> dict[str, Any]:
    return LaneBench(spec).execute()
