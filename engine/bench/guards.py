"""Garde-fous : disque libre, durée d'une étape, budget mémoire total ; moniteur de phase."""

from __future__ import annotations

import shutil
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass

from .sampler import (
    MIB,
    Sampler,
    combined_peak,
    docker_stats,
    host_memory,
    kill_tree,
    tree_rss_breakdown,
)

GIB = 1024**3


@dataclass(frozen=True)
class Limits:
    min_free_disk_gb: float = 25.0
    max_step_seconds: float = 90 * 60.0
    max_total_ram_gb: float = 27.0


def evaluate(
    limits: Limits, *, free_disk_gb: float, elapsed_s: float, total_ram_gb: float
) -> str | None:
    """Motif d'arrêt (None si tout va bien). L'ordre fixe la priorité : disque, durée, mémoire."""
    if free_disk_gb < limits.min_free_disk_gb:
        return f"disk_free_below_min: {free_disk_gb:.1f} GB < {limits.min_free_disk_gb:.1f} GB"
    if elapsed_s > limits.max_step_seconds:
        return f"step_timeout: {elapsed_s / 60:.1f} min > {limits.max_step_seconds / 60:.0f} min"
    if total_ram_gb > limits.max_total_ram_gb:
        return f"ram_budget: {total_ram_gb:.1f} GB > {limits.max_total_ram_gb:.1f} GB"
    return None


def free_disk_gb(path: str) -> float:
    return shutil.disk_usage(path).free / GIB


class Monitor:
    """Contexte d'une phase : échantillonne RSS (arbre), conteneur, hôte ; applique les limites."""

    def __init__(
        self,
        limits: Limits,
        *,
        container: str | None = None,
        disk_path: str = ".",
        guard_interval: float = 0.5,
        stats_fn: Callable[[str], dict[str, float]] = docker_stats,
        probes: Callable[[], dict[str, float]] | None = None,
        probe_interval: float = 5.0,
    ) -> None:
        self.limits = limits
        self.container = container
        self.disk_path = disk_path
        self.guard_interval = guard_interval
        self.roots: set[int] = set()
        self.abort_reason: str | None = None
        self.min_free_gb = free_disk_gb(disk_path)
        self.elapsed = 0.0
        self._t0 = 0.0
        self._stop = threading.Event()
        self._guard = threading.Thread(target=self._guard_loop, daemon=True)
        self.peak_breakdown: list[dict[str, float | str]] = []
        self._best_total = 0.0
        self._tree = Sampler(self._tree_sample, 0.5, False)
        self._host = Sampler(host_memory, 2.0)
        self._docker = (
            Sampler(lambda: stats_fn(container), 1.0, False) if container is not None else None
        )
        self._probes = Sampler(probes, probe_interval, False) if probes else None

    def add_root(self, pid: int) -> None:
        self.roots.add(pid)

    def _tree_sample(self) -> dict[str, float]:
        rows = tree_rss_breakdown(list(self.roots))
        total = float(sum(r[2] for r in rows))
        if total > self._best_total:
            self._best_total = total
            self.peak_breakdown = [
                {"pid": pid, "cmd": cmd, "rss_mb": round(rss / MIB, 1)}
                for pid, cmd, rss in rows[:5]
            ]
        return {"rss": total}

    def _total_gb(self) -> float:
        rss = self._tree.series.get("rss")
        mem = self._docker.series.get("mem") if self._docker else None
        total = (rss.points[-1][1] if rss and rss.points else 0.0) + (
            mem.points[-1][1] if mem and mem.points else 0.0
        )
        return total / GIB

    def _guard_loop(self) -> None:
        while not self._stop.wait(self.guard_interval):
            free = free_disk_gb(self.disk_path)
            self.min_free_gb = min(self.min_free_gb, free)
            reason = evaluate(
                self.limits,
                free_disk_gb=free,
                elapsed_s=time.monotonic() - self._t0,
                total_ram_gb=self._total_gb(),
            )
            if reason and self.abort_reason is None:
                self.abort_reason = reason
                for pid in list(self.roots):
                    kill_tree(pid)

    def __enter__(self) -> Monitor:
        self._t0 = time.monotonic()
        self._tree.start()
        self._host.start()
        for sampler in (self._docker, self._probes):
            if sampler:
                sampler.start()
        self._guard.start()
        return self

    def __exit__(self, *exc: object) -> None:
        self.elapsed = time.monotonic() - self._t0
        self._stop.set()
        self._guard.join(timeout=10)
        for sampler in (self._tree, self._host, self._docker, self._probes):
            if sampler:
                sampler.stop()
        self.min_free_gb = min(self.min_free_gb, free_disk_gb(self.disk_path))

    def summary(self) -> dict[str, float | str | None]:
        series = [s for s in (self._tree.series.get("rss"),) if s]
        if self._docker and self._docker.series.get("mem"):
            series.append(self._docker.series["mem"])
        probes = {
            f"{k}_peak": round(v.peak, 1)
            for k, v in (self._probes.series if self._probes else {}).items()
        }
        return {
            **probes,
            "rss_peak_mb": round(self._tree.peak("rss") / MIB, 1),
            "container_mem_peak_mb": round(self._docker.peak("mem") / MIB, 1)
            if self._docker
            else None,
            "container_cpu_peak_pct": round(self._docker.peak("cpu"), 1) if self._docker else None,
            "total_peak_mb": round(combined_peak(*series) / MIB, 1),
            "host_used_peak_gb": round(self._host.peak("used") / GIB, 2),
            "disk_free_min_gb": round(self.min_free_gb, 1),
            "abort_reason": self.abort_reason,
            "rss_peak_processes": self.peak_breakdown,
        }
