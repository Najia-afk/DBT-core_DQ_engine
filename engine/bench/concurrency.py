"""Concurrence : N référentiels réels via engine.runtime (pg-meta en conteneur)."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import time
from pathlib import Path
from typing import Any

import psycopg
import yaml

from .dataset import DatasetSpec, ensure_dataset, stage_live
from .guards import Limits, Monitor
from .pg import PgContainer, PgTuning
from .procs import PYTHON
from .sampler import docker_stats
from .scenario import REPO_ROOT, machine_info

MIB = 1024 * 1024
LANE_TUNING = PgTuning(
    memory="4g", shm_size="256m", shared_buffers="1GB", work_mem="32MB",
    maintenance_work_mem="256MB", effective_cache_size="2GB", max_wal_size="4GB",
    min_wal_size="512MB",
)  # fmt: skip
META_TUNING = PgTuning(
    memory="1g", shm_size="128m", shared_buffers="128MB", work_mem="8MB",
    maintenance_work_mem="64MB", effective_cache_size="512MB", max_wal_size="1GB",
    min_wal_size="80MB", max_connections=60,
)  # fmt: skip
PROFILE_MIB = 3072
BUDGET_MIB = 9216


def max_concurrency(intervals: list[tuple[float, float]]) -> int:
    points = sorted([(a, 1) for a, _ in intervals] + [(b, -1) for _, b in intervals])
    current = best = 0
    for _, delta in points:
        current += delta
        best = max(best, current)
    return best


def peak_overlapping_sum(spans: list[tuple[float, float, float]]) -> float:
    """Pic de la somme des valeurs des intervalles (début, fin, valeur) simultanément actifs."""
    points = sorted([(a, v) for a, _, v in spans] + [(b, -v) for _, b, v in spans])
    current = best = 0.0
    for _, delta in points:
        current += delta
        best = max(best, current)
    return best


def analyze(runs: list[dict[str, Any]], steps: list[dict[str, Any]]) -> dict[str, Any]:
    """Synthèse de concurrence à partir des lignes pg-meta (temps en secondes epoch)."""
    done = [s for s in steps if s.get("started") is not None and s.get("ended") is not None]
    spans = [(s["started"], s["ended"]) for s in done]
    run_spans = [(r["started"], r["ended"]) for r in runs if r["started"] and r["ended"]]
    ref_spans: dict[str, list[tuple[float, float]]] = {}
    by_run = {r["run_id"]: r for r in runs}
    for s in done:
        ref_spans.setdefault(by_run[s["run_id"]]["referential"], []).append(
            (s["started"], s["ended"])
        )
    per_ref = [(min(a for a, _ in v), max(b for _, b in v)) for v in ref_spans.values()]
    serialized: list[dict[str, Any]] = []
    by_ref: dict[str, list[dict[str, Any]]] = {}
    for r in sorted(runs, key=lambda r: r["started"] or 0):
        by_ref.setdefault(r["referential"], []).append(r)
    for ref, items in by_ref.items():
        for a, b in zip(items, items[1:], strict=False):
            if a["ended"] and b["started"]:
                serialized.append(
                    {"referential": ref, "gap_s": round(b["started"] - a["ended"], 2),
                     "ok": b["started"] >= a["ended"]}
                )  # fmt: skip
    peak_sum = peak_overlapping_sum(
        [(s["started"], s["ended"], float(s.get("peak_rss_mib") or 0)) for s in done]
    )
    starts = [r["started"] for r in runs if r["started"]]
    ends = [r["ended"] for r in runs if r["ended"]]
    return {
        "runs": len(runs),
        "all_succeeded": bool(runs)
        and all(str(r["status"]).startswith("succeeded") for r in runs),
        "statuses": sorted({str(r["status"]) for r in runs}),
        "wall_s": round(max(ends) - min(starts), 2) if starts and ends else None,
        "step_concurrency_max": max_concurrency(spans),
        "run_concurrency_max": max_concurrency(run_spans),
        "referential_concurrency_max": max_concurrency(per_ref),
        "same_referential_serialization": serialized,
        "step_peak_rss_mib_max": max((s.get("peak_rss_mib") or 0 for s in done), default=0),
        "sum_of_concurrent_step_peaks_mib": round(peak_sum, 1),
    }


def fetch_rows(dsn: str, key_prefix: str) -> tuple[list[dict], list[dict]]:
    ts = "EXTRACT(EPOCH FROM {c})"
    with psycopg.connect(dsn, autocommit=True, row_factory=psycopg.rows.dict_row) as conn:
        runs = conn.execute(
            f"SELECT run_id, referential, status, status_reason, "
            f"{ts.format(c='started_at')} AS started, "
            f"{ts.format(c='ended_at')} AS ended, {ts.format(c='queued_at')} AS queued "
            "FROM mdm_runtime.runs WHERE idempotency_key LIKE %s ORDER BY seq",
            (key_prefix + "%",),
        ).fetchall()
        steps = conn.execute(
            f"SELECT s.run_id, s.step, s.status, s.error, s.error_kind, "
            f"{ts.format(c='s.started_at')} AS started, "
            f"{ts.format(c='s.ended_at')} AS ended, s.peak_rss_mib, s.attempt "
            "FROM mdm_runtime.steps s JOIN mdm_runtime.runs r USING (run_id) "
            "WHERE r.idempotency_key LIKE %s",
            (key_prefix + "%",),
        ).fetchall()
    for row in [*runs, *steps]:
        for k in ("started", "ended", "queued"):
            if k in row and row[k] is not None:
                row[k] = float(row[k])
    return runs, steps


def runtime_yaml(path: Path, *, host_steps: int, lane: str, work: Path) -> None:
    lane_env = {"PYTHONDONTWRITEBYTECODE": "1"}
    if lane == "elt":
        lane_env |= {"MDM_ELT_WORKDIR": str(work), "MDM_ELT_MEMORY_LIMIT": "1GB",
                     "MDM_ELT_THREADS": "2"}  # fmt: skip
    if lane == "etl":
        lane_env["MDM_ETL_WORKERS"] = "2"
    data = {
        "host": {
            "ram_gib": 16, "job_memory_budget_mib": BUDGET_MIB, "max_running_steps": host_steps,
            "max_threads": 32, "min_free_disk_gib": 25, "work_dir": str(path.parent),
        },
        "profiles": {
            "small": {"process_memory_mib": PROFILE_MIB, "duckdb_memory_mib": 1024, "threads": 2},
            "medium": {"process_memory_mib": PROFILE_MIB, "duckdb_memory_mib": 1024, "threads": 2},
            "large": {"process_memory_mib": PROFILE_MIB, "duckdb_memory_mib": 1024, "threads": 2},
        },
        "referential": {"max_running_steps": 3},
        "scheduler": {"poll_interval_seconds": 0.5},
        "lane": {"python": PYTHON, "env": lane_env, "cwd": str(REPO_ROOT)},
    }  # fmt: skip
    path.write_text(yaml.safe_dump(data, sort_keys=False), encoding="utf-8")


def runtime_cmd(meta_dsn: str, cfg: Path, *args: str) -> list[str]:
    return [PYTHON, "-m", "engine.runtime", "--dsn", meta_dsn, "--runtime-config", str(cfg), *args]


def state_counts(state: PgContainer, refs: list[str]) -> dict[str, dict[str, int]]:
    out: dict[str, dict[str, int]] = {}
    for ref in refs:
        out[ref] = {}
        for table in ("source_record", "golden", "match_pair"):
            count = state.scalar(f'SELECT count(*) FROM "mdm_{ref}"."{table}"')
            if count is None:
                raise RuntimeError(f"missing row count for {ref}.{table}")
            out[ref][table] = int(count)
    return out


def run_worker_phase(
    label: str,
    *,
    env: dict[str, str],
    meta: PgContainer,
    state: PgContainer,
    cfg: Path,
    configs: dict[str, Path],
    lane: str,
    submissions: list[tuple[str, str]],
    limits: Limits,
    log_dir: Path,
) -> dict[str, Any]:
    """Soumet (référentiel, clé), lance un worker jusqu'à file vide, mesure son arbre."""
    for ref, key in submissions:
        res = subprocess.run(
            runtime_cmd(meta.dsn(), cfg, "submit", "--config", str(configs[ref]), "--lane", lane,
                        "--key", key, "--include-init"),
            env=env, cwd=REPO_ROOT, capture_output=True, text=True, check=False,
        )  # fmt: skip
        if res.returncode != 0:
            raise RuntimeError(f"submit {key} a échoué : {res.stdout} {res.stderr[-400:]}")
    log_dir.mkdir(parents=True, exist_ok=True)
    out = (log_dir / f"{label}.worker.out").open("wb")
    err = (log_dir / f"{label}.worker.err").open("wb")
    with Monitor(limits, container=state.name, disk_path=str(log_dir)) as mon:
        t0 = time.perf_counter()
        proc = subprocess.Popen(  # noqa: S603
            runtime_cmd(meta.dsn(), cfg, "worker", "--until-idle", "--max-seconds", "3000"),
            env=env, cwd=REPO_ROOT, stdout=out, stderr=err, stdin=subprocess.DEVNULL,
        )  # fmt: skip
        mon.add_root(proc.pid)
        code = proc.wait()
        worker_wall = time.perf_counter() - t0
    out.close()
    err.close()
    prefix = label.split("-")[0] + "-"
    runs, steps = fetch_rows(meta.dsn(), prefix)
    summary = analyze(runs, steps)
    summary |= {
        "label": label,
        "worker_exit": code,
        "worker_wall_s": round(worker_wall, 2),
        "monitor": mon.summary(),
        "worker_tree_rss_peak_mib": round(float(mon.summary()["rss_peak_mb"]), 1),
        "state_container_mem_peak_mib": mon.summary()["container_mem_peak_mb"],
        "meta_container_mem_mib_after": round(docker_stats(meta.name).get("mem", 0) / MIB, 1),
    }  # fmt: skip
    return {"summary": summary, "runs": runs, "steps": steps}


def run_concurrency(args: argparse.Namespace) -> int:
    lane: str = args.lane
    root: Path = args.root
    n = args.referentials
    limits = Limits(args.min_free_disk_gb, args.max_step_minutes * 60.0, args.max_total_ram_gb)
    refs = [f"r{i}" for i in range(1, n + 1)]
    work = root / f"conc-{lane}"
    shutil.rmtree(work, ignore_errors=True)
    work.mkdir(parents=True)
    state = PgContainer(lane, LANE_TUNING, tag="rt")
    meta = PgContainer("meta", META_TUNING)
    datasets: dict[str, Path] = {}
    configs: dict[str, Path] = {}
    payload: dict[str, Any] = {"lane": lane, "machine": machine_info(), "phases": {}}
    status = 0
    try:
        for i, ref in enumerate(refs, start=1):
            spec = DatasetSpec(nodes=args.nodes, attributes=args.attributes, seed=100 + i,
                               referential=ref, workers=4)  # fmt: skip
            ds, _ = ensure_dataset(spec, root, limits=limits, env=dict(os.environ))
            stage_live(ds, 0)
            datasets[ref], configs[ref] = ds, ds / "config.yml"
        state.start()
        meta.start()
        env = {**os.environ, "PYTHONDONTWRITEBYTECODE": "1", "MDM_STATE_DSN": state.dsn(),
               "MDM_META_DSN": meta.dsn()}  # fmt: skip
        common = dict(env=env, meta=meta, state=state, configs=configs, lane=lane, limits=limits,
                      log_dir=work / "logs")  # fmt: skip
        seq_cfg, conc_cfg = work / "rt-seq.yml", work / "rt-conc.yml"
        runtime_yaml(seq_cfg, host_steps=1, lane=lane, work=work / "elt")
        runtime_yaml(conc_cfg, host_steps=3, lane=lane, work=work / "elt")

        seq = run_worker_phase("seq-baseline", cfg=seq_cfg,
                               submissions=[(r, f"seq-{r}") for r in refs], **common)  # fmt: skip
        payload["phases"]["sequential_host_slots_1"] = seq
        payload["counts_seq"] = state_counts(state, refs)
        for ref in refs:
            subprocess.run([PYTHON, "-m", f"engine.lanes.{lane}", "reset", "--config",
                            str(configs[ref])], env=env, cwd=REPO_ROOT, capture_output=True,
                           check=False)  # fmt: skip
        shutil.rmtree(work / "elt", ignore_errors=True)

        conc = run_worker_phase("conc-parallel", cfg=conc_cfg,
                                submissions=[(r, f"conc-{r}") for r in refs], **common)  # fmt: skip
        payload["phases"]["concurrent_host_slots_3"] = conc
        payload["counts_conc"] = state_counts(state, refs)

        ser = run_worker_phase(
            "ser-same-ref", cfg=conc_cfg,
            submissions=[(refs[0], "ser-a"), (refs[0], "ser-b"), (refs[1], "ser-c")], **common,
        )  # fmt: skip
        payload["phases"]["same_referential"] = ser

        s, c = seq["summary"], conc["summary"]
        payload["summary"] = {
            "lane": lane,
            "referentials": n,
            "nodes": args.nodes,
            "attributes": args.attributes,
            "sequential_wall_s": s["wall_s"],
            "concurrent_wall_s": c["wall_s"],
            "speedup": round(s["wall_s"] / c["wall_s"], 2) if s["wall_s"] and c["wall_s"] else None,
            "concurrent_step_concurrency_max": c["step_concurrency_max"],
            "concurrent_run_concurrency_max": c["run_concurrency_max"],
            "all_succeeded": all(x["summary"]["all_succeeded"] for x in (seq, conc, ser)),
            "same_ref_serialization": ser["summary"]["same_referential_serialization"],
            "state_equal_seq_vs_conc": payload["counts_seq"] == payload["counts_conc"],
            "budget_mib": BUDGET_MIB,
            "profile_process_mib": PROFILE_MIB,
            "worker_tree_rss_peak_mib_concurrent": c["worker_tree_rss_peak_mib"],
            "sum_of_concurrent_step_peaks_mib": c["sum_of_concurrent_step_peaks_mib"],
            "step_peak_rss_mib_max": c["step_peak_rss_mib_max"],
            "state_container_mem_peak_mib": c["monitor"]["container_mem_peak_mb"],
            "total_peak_mib_concurrent": c["monitor"]["total_peak_mb"],
            "within_budget": c["worker_tree_rss_peak_mib"] <= BUDGET_MIB,
        }  # fmt: skip
        checks = payload["summary"]
        valid = (
            checks["all_succeeded"]
            and checks["state_equal_seq_vs_conc"]
            and checks["within_budget"]
            and checks["concurrent_step_concurrency_max"] > 1
            and bool(checks["same_ref_serialization"])
            and all(item["ok"] for item in checks["same_ref_serialization"])
        )
        status = 0 if valid else 1
        payload["summary"]["validated"] = valid
    except BaseException as exc:  # noqa: BLE001
        payload["error"] = f"{type(exc).__name__}: {exc}"[:800]
        payload["summary"] = {"error": payload["error"]}
        status = 1
    finally:
        state.remove()
        meta.remove()
        for ds in datasets.values():
            shutil.rmtree(ds, ignore_errors=True)
        shutil.rmtree(work, ignore_errors=True)
    target = args.out / "concurrency"
    target.mkdir(parents=True, exist_ok=True)
    (target / f"{lane}-{n}x{args.nodes // 1000}k.json").write_text(
        json.dumps(payload, indent=1, default=str), encoding="utf-8"
    )
    print(json.dumps(payload["summary"], default=str))
    return status
