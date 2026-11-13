"""Garde-fous du banc : règles d'abandon (disque, durée, mémoire) et moniteur de phase."""

from __future__ import annotations

import subprocess
import sys
import time
from pathlib import Path

import pytest

from engine.bench.guards import GIB, Limits, Monitor, evaluate, free_disk_gb
from engine.bench.pg import PgContainer, PgTuning, assert_bench_name, run_args


def ok_kwargs(**over: float) -> dict[str, float]:
    base = {"free_disk_gb": 80.0, "elapsed_s": 10.0, "total_ram_gb": 5.0}
    return {**base, **over}


def test_defaults_match_the_study_rules() -> None:
    limits = Limits()
    assert limits.min_free_disk_gb == 25.0
    assert limits.max_step_seconds == 90 * 60
    assert limits.max_total_ram_gb == 27.0  # 28 Go imposés, 1 Go de marge d'échantillonnage


def test_evaluate_ok_and_each_rule() -> None:
    limits = Limits()
    assert evaluate(limits, **ok_kwargs()) is None
    assert "disk_free_below_min" in (evaluate(limits, **ok_kwargs(free_disk_gb=24.9)) or "")
    assert evaluate(limits, **ok_kwargs(free_disk_gb=25.0)) is None
    assert "step_timeout" in (evaluate(limits, **ok_kwargs(elapsed_s=5401)) or "")
    assert evaluate(limits, **ok_kwargs(elapsed_s=5400)) is None
    assert "ram_budget" in (evaluate(limits, **ok_kwargs(total_ram_gb=27.1)) or "")


def test_evaluate_priority_disk_before_time_before_ram() -> None:
    limits = Limits()
    worst = ok_kwargs(free_disk_gb=1, elapsed_s=99999, total_ram_gb=99)
    assert "disk" in (evaluate(limits, **worst) or "")
    assert "step_timeout" in (evaluate(limits, **ok_kwargs(elapsed_s=99999, total_ram_gb=99)) or "")


def sleeper() -> subprocess.Popen:
    return subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])


def test_monitor_kills_on_step_timeout(tmp_path: Path) -> None:
    limits = Limits(min_free_disk_gb=0, max_step_seconds=0.5, max_total_ram_gb=1000)
    proc = sleeper()
    try:
        with Monitor(limits, disk_path=str(tmp_path), guard_interval=0.1) as mon:
            mon.add_root(proc.pid)
            proc.wait(timeout=20)
        assert mon.abort_reason is not None
        assert mon.abort_reason.startswith("step_timeout")
        assert mon.summary()["abort_reason"] == mon.abort_reason
    finally:
        if proc.poll() is None:
            proc.kill()


def test_monitor_kills_on_disk_guard(tmp_path: Path) -> None:
    huge = free_disk_gb(str(tmp_path)) + 1000
    limits = Limits(min_free_disk_gb=huge, max_step_seconds=1000, max_total_ram_gb=1000)
    proc = sleeper()
    try:
        with Monitor(limits, disk_path=str(tmp_path), guard_interval=0.1) as mon:
            mon.add_root(proc.pid)
            proc.wait(timeout=20)
        assert (mon.abort_reason or "").startswith("disk_free_below_min")
    finally:
        if proc.poll() is None:
            proc.kill()


def test_monitor_ram_budget_includes_container(tmp_path: Path) -> None:
    limits = Limits(min_free_disk_gb=0, max_step_seconds=1000, max_total_ram_gb=20)
    proc = sleeper()
    fake = {"mem": 30 * GIB, "cpu": 250.0}
    try:
        with Monitor(
            limits,
            container="mdm2-bench-fake",
            disk_path=str(tmp_path),
            guard_interval=0.1,
            stats_fn=lambda _name: fake,
        ) as mon:
            mon.add_root(proc.pid)
            proc.wait(timeout=20)
        assert (mon.abort_reason or "").startswith("ram_budget")
        summary = mon.summary()
        assert summary["container_mem_peak_mb"] == 30 * 1024
        assert summary["container_cpu_peak_pct"] == 250.0
        assert summary["total_peak_mb"] >= 30 * 1024
    finally:
        if proc.poll() is None:
            proc.kill()


def test_monitor_normal_run_reports_peaks(tmp_path: Path) -> None:
    limits = Limits(min_free_disk_gb=0)
    proc = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(1.5)"])
    with Monitor(limits, disk_path=str(tmp_path), guard_interval=0.2) as mon:
        mon.add_root(proc.pid)
        proc.wait(timeout=20)
        time.sleep(0.2)
    summary = mon.summary()
    assert mon.abort_reason is None
    assert summary["rss_peak_mb"] > 5
    assert summary["container_mem_peak_mb"] is None
    assert summary["host_used_peak_gb"] > 0


def test_probes_are_reported_as_peaks(tmp_path: Path) -> None:
    values = iter([{"workdir_mb": 3.0}, {"workdir_mb": 11.0}, {"workdir_mb": 2.0}])

    def probes() -> dict[str, float]:
        return next(values, {"workdir_mb": 1.0})

    with Monitor(
        Limits(min_free_disk_gb=0), disk_path=str(tmp_path), probes=probes, probe_interval=0.05
    ) as mon:
        time.sleep(0.4)
    assert mon.summary()["workdir_mb_peak"] == 11.0


def test_only_bench_containers_can_be_touched() -> None:
    assert_bench_name("mdm2-bench-etl")
    with pytest.raises(ValueError):
        assert_bench_name("mdm-s0-02-postgres-1")
    with pytest.raises(ValueError):
        PgContainer("etl", name="other-container").remove()


def test_container_naming_and_limits() -> None:
    pg = PgContainer("graph")
    assert pg.name == "mdm2-bench-graph"
    assert pg.image == "apache/age:release_PG16_1.6.0"
    assert PgContainer("elt", tag="ab12").name == "mdm2-bench-elt-ab12"
    args = run_args(pg.name, pg.image, PgTuning())
    joined = " ".join(args)
    assert "--memory 12g --memory-swap 12g" in joined
    assert "--shm-size 1g" in joined
    for setting in ("shared_buffers=3GB", "work_mem=64MB", "max_wal_size=16GB",
                    "synchronous_commit=off"):  # fmt: skip
        assert setting in joined
    assert args[args.index("--name") + 1] == "mdm2-bench-graph"
