"""Équivalence : voies contre ReferenceEngine (petite taille), voie contre voie (grande)."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import threading
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

import psutil

from .dataset import ensure_dataset, load_manifest
from .guards import Limits
from .procs import PYTHON, last_json_line
from .sampler import MIB, Sampler, tree_rss_bytes
from .scenario import REPO_ROOT, RunSpec, run_lane_bench
from .steps import LADDER, equivalence_dataset

MAX_DETAILS = 20


def compare_dirs(a: Path, b: Path, json_path: Path | None, use_hash: bool, memory: str) -> int:
    """Compare deux exports Parquet ; DuckDB (créé par compare_exports) est borné en mémoire."""
    import duckdb

    from engine.core.canonical import read_parquet_dir
    from engine.core.compare import compare_exports

    real_connect = duckdb.connect

    def bounded(*args: Any, **kwargs: Any) -> Any:
        con = real_connect(*args, **kwargs)
        con.execute(f"SET memory_limit='{memory}'")
        con.execute("SET threads=8")
        return con

    duckdb.connect = bounded  # type: ignore[assignment]
    t0 = time.perf_counter()
    try:
        report = compare_exports(read_parquet_dir(a), read_parquet_dir(b), compare_hash=use_hash)
    finally:
        duckdb.connect = real_connect
    payload = {
        "a": str(a),
        "b": str(b),
        "ok": report.ok,
        "totals": report.totals,
        "compared": {k: list(v) for k, v in report.compared.items()},
        "details": [str(d) for d in report.divergences[:MAX_DETAILS]],
        "compare_hash": use_hash,
        "seconds": round(time.perf_counter() - t0, 1),
    }
    if json_path:
        json_path.parent.mkdir(parents=True, exist_ok=True)
        json_path.write_text(json.dumps(payload, indent=1), encoding="utf-8")
    print(json.dumps(payload))
    return 0 if report.ok else 1


def compare_subprocess(a: Path, b: Path, *, use_hash: bool = True, memory: str = "6GB") -> dict:
    """Lance `compare` dans un process dédié et mesure son pic RSS."""
    argv = [PYTHON, "-m", "engine.bench", "compare", "--a", str(a), "--b", str(b),
            "--memory", memory, *(["--hash"] if use_hash else [])]  # fmt: skip
    proc = subprocess.Popen(  # noqa: S603
        argv, cwd=REPO_ROOT, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
    )
    sampler = Sampler(lambda: {"rss": tree_rss_bytes([proc.pid])}, 0.5, False).start()
    out, err = proc.communicate()
    sampler.stop()
    payload = last_json_line(out) or {"ok": False, "error": err[-800:]}
    payload["rss_peak_mb"] = round(sampler.peak("rss") / MIB, 1)
    return payload


def run_reference(ds: Path, out: Path) -> dict[str, Any]:
    """ReferenceEngine sur tous les jours ; un export Parquet par jour. Retourne temps et RSS."""
    import pyarrow.parquet as pq

    from engine.core.canonical import write_parquet
    from engine.core.config import load_config
    from engine.core.reference import ReferenceEngine

    cfg = load_config(ds / "config.yml")
    ref = ReferenceEngine(cfg)
    manifest = load_manifest(ds)
    sampler = Sampler(lambda: {"rss": psutil.Process().memory_info().rss}, 1.0).start()
    days: list[dict[str, Any]] = []
    for day in range(len(manifest["days"])):
        t0 = time.perf_counter()
        for src in cfg.sources:
            files = sorted((ds / f"day{day}" / src.id).glob("part-*.parquet"))
            rows = [r for f in files for r in pq.read_table(f).to_pylist()]
            ref.ingest(src.id, rows, f"d{day}")
        ref.reconcile(f"d{day}")
        tables = ref.export()
        shutil.rmtree(out / f"day{day}", ignore_errors=True)
        write_parquet(tables, out / f"day{day}")
        days.append({"day": day, "seconds": round(time.perf_counter() - t0, 1)})
    sampler.stop()
    return {"days": days, "rss_peak_mb": round(sampler.peak("rss") / MIB, 1)}


def _short(report: dict) -> dict:
    return {
        k: report.get(k)
        for k in ("ok", "totals", "details", "seconds", "rss_peak_mb", "error", "compared")
    }


def _write(out: Path, name: str, payload: dict) -> None:
    target = out / "equivalence"
    target.mkdir(parents=True, exist_ok=True)
    (target / f"{name}.json").write_text(json.dumps(payload, indent=1, default=str), "utf-8")


def reference_stage(args: argparse.Namespace, make_spec: Callable[..., RunSpec]) -> int:
    root: Path = args.root
    dspec = equivalence_dataset(args.nodes, args.attributes, args.days)
    limits = Limits(args.min_free_disk_gb, args.max_step_minutes * 60.0, args.max_total_ram_gb)
    ds, info = ensure_dataset(dspec, root, limits=limits, env=dict(os.environ))
    ref_dir = root / "exports" / f"ref-{dspec.key()}"
    ref_info = run_reference(ds, ref_dir)
    print(json.dumps({"reference": ref_info}), flush=True)
    payload: dict[str, Any] = {
        "dataset": {"nodes": args.nodes, "attributes": args.attributes, "days": args.days,
                    "key": dspec.key(), "info": info},
        "reference": ref_info,
        "lanes": {},
    }  # fmt: skip
    failures = 0
    for lane in args.lanes:
        args.export = "each"
        spec = make_spec(args, lane, dspec, f"equiv-{lane}-{args.nodes // 1000}k")
        spec.keep_dataset = True
        spec.export_root = root / "exports" / spec.name
        result = run_lane_bench(spec)
        entry: dict[str, Any] = {"status": result["status"], "days": {}}
        if result["status"] == "ok":
            for day in range(args.days + 1):
                rep = compare_subprocess(ref_dir / f"day{day}", spec.export_root / f"day{day}")
                entry["days"][str(day)] = _short(rep)
                failures += 0 if rep.get("ok") else 1
                print(json.dumps({"lane": lane, "day": day, "ok": rep.get("ok"),
                                  "totals": rep.get("totals")}), flush=True)  # fmt: skip
        else:
            entry["abort_reason"] = result["abort_reason"]
            failures += 1
        payload["lanes"][lane] = entry
        shutil.rmtree(spec.export_root, ignore_errors=True)
    shutil.rmtree(ref_dir, ignore_errors=True)
    shutil.rmtree(ds, ignore_errors=True)
    _write(args.out, f"reference-{args.nodes // 1000}k", payload)
    return 1 if failures else 0


def lanes_stage(args: argparse.Namespace, make_spec: Callable[..., RunSpec]) -> int:
    step = LADDER[args.step]
    root: Path = args.root
    exports: dict[str, Path] = {}
    runs: dict[str, dict] = {}
    for lane in args.lanes:
        final = root / "exports" / f"{lane}-{step.name}" / f"day{len(step.update_pcts)}"
        if not final.exists():
            args.export = "final"
            spec = make_spec(args, lane, step.dataset(), f"{lane}-{step.name}")
            spec.export_root = root / "exports" / spec.name
            runs[lane] = {
                k: v for k, v in run_lane_bench(spec).items() if k in ("status", "totals")
            }
            if runs[lane]["status"] != "ok":
                continue
        exports[lane] = final
    payload: dict[str, Any] = {"step": step.name, "nodes": step.nodes, "runs": runs, "pairs": {}}
    payload["attributes"] = step.attributes
    failures = len(args.lanes) - len(exports)
    names = [lane for lane in args.lanes if lane in exports]
    for i, left in enumerate(names):
        for right in names[i + 1 :]:
            for use_hash in (False, True):
                suffix = "+row_hash" if use_hash else ""
                rep = compare_subprocess(exports[left], exports[right], use_hash=use_hash)
                payload["pairs"][f"{left}-vs-{right}{suffix}"] = _short(rep)
                failures += 0 if rep.get("ok") else 1
                print(json.dumps({"pair": f"{left}-{right}{suffix}", "ok": rep.get("ok"),
                                  "totals": rep.get("totals")}), flush=True)  # fmt: skip
    _write(args.out, f"lanes-{step.name}", payload)
    for lane in names:
        shutil.rmtree(exports[lane].parent, ignore_errors=True)
    return 1 if failures else 0


def run_equivalence(args: argparse.Namespace, make_spec: Callable[..., RunSpec]) -> int:
    threading.current_thread().name = "equiv"
    if args.mode == "reference":
        return reference_stage(args, make_spec)
    return lanes_stage(args, make_spec)
