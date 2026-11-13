"""Échantillonneurs de ressources : arbre de processus (psutil), conteneur (docker stats), hôte."""

from __future__ import annotations

import json
import subprocess
import threading
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field

import psutil

MIB = 1024 * 1024

_UNITS = {
    "B": 1,
    "KB": 10**3,
    "MB": 10**6,
    "GB": 10**9,
    "KIB": 1024,
    "MIB": 1024**2,
    "GIB": 1024**3,
}


def parse_size(text: str) -> int:
    """'1.5GiB' -> octets (unités docker : B, kB, MB, GB, KiB, MiB, GiB)."""
    text = text.strip()
    for unit in sorted(_UNITS, key=len, reverse=True):
        if text.upper().endswith(unit):
            number = text[: -len(unit)].strip()
            return int(float(number) * _UNITS[unit])
    return int(float(text))


def parse_docker_stats(line: str) -> tuple[int, float]:
    """Ligne `docker stats --format '{{json .}}'` -> (octets utilisés, % CPU)."""
    row = json.loads(line)
    used = parse_size(row["MemUsage"].split("/")[0])
    cpu = float(row["CPUPerc"].strip().rstrip("%") or 0)
    return used, cpu


def descendants(pid: int) -> list[psutil.Process]:
    try:
        root = psutil.Process(pid)
        return [root, *root.children(recursive=True)]
    except psutil.Error:
        return []


def tree_rss_breakdown(pids: Iterable[int]) -> list[tuple[int, str, int]]:
    """[(pid, nom, RSS)] décroissant des processus de l'arbre (pour le diagnostic d'un pic)."""
    seen: dict[int, psutil.Process] = {}
    for pid in pids:
        for proc in descendants(pid):
            seen.setdefault(proc.pid, proc)
    rows = []
    for proc in seen.values():
        try:
            rows.append((proc.pid, " ".join(proc.cmdline()[1:4])[:60] or proc.name(),
                         proc.memory_info().rss))  # fmt: skip
        except psutil.Error:
            continue
    return sorted(rows, key=lambda r: r[2], reverse=True)


def tree_rss_bytes(pids: Iterable[int]) -> int:
    """Somme des RSS (working set sous Windows) des processus `pids` et de leurs descendants."""
    seen: dict[int, psutil.Process] = {}
    for pid in pids:
        for proc in descendants(pid):
            seen.setdefault(proc.pid, proc)
    total = 0
    for proc in seen.values():
        try:
            total += proc.memory_info().rss
        except psutil.Error:
            continue
    return total


def kill_tree(pid: int) -> None:
    for proc in reversed(descendants(pid)):
        try:
            proc.kill()
        except psutil.Error:
            continue


@dataclass
class Series:
    """Série temporelle (t monotone, valeur) avec pic."""

    points: list[tuple[float, float]] = field(default_factory=list)

    def add(self, t: float, value: float) -> None:
        self.points.append((t, value))

    @property
    def peak(self) -> float:
        return max((v for _, v in self.points), default=0.0)

    def at(self, t: float) -> float:
        """Dernière valeur échantillonnée au plus tard à `t` (0 si aucune)."""
        value = 0.0
        for ts, v in self.points:
            if ts > t:
                break
            value = v
        return value


def combined_peak(*series: Series) -> float:
    """Pic de la somme des séries, chaque série étant tenue à sa dernière valeur connue."""
    times = sorted({t for s in series for t, _ in s.points})
    best = 0.0
    for t in times:
        best = max(best, sum(s.at(t) for s in series))
    return best


class Sampler:
    """Appelle `fn() -> dict[str, float]` toutes les `interval` secondes dans un thread."""

    def __init__(
        self,
        fn: Callable[[], dict[str, float]],
        interval: float = 1.0,
        final_sample: bool = True,
    ) -> None:
        self.fn = fn
        self.interval = interval
        self.final_sample = final_sample
        self.series: dict[str, Series] = {}
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._loop, daemon=True)

    def _sample(self) -> None:
        try:
            values = self.fn()
        except Exception:  # noqa: BLE001 - la mesure ne doit jamais casser le run
            return
        now = time.monotonic()
        for key, value in values.items():
            self.series.setdefault(key, Series()).add(now, float(value))

    def _loop(self) -> None:
        while not self._stop.is_set():
            self._sample()
            self._stop.wait(self.interval)

    def start(self) -> Sampler:
        self._thread.start()
        return self

    def stop(self) -> None:
        self._stop.set()
        self._thread.join(timeout=30)
        if self.final_sample:
            self._sample()

    def peak(self, key: str) -> float:
        return self.series[key].peak if key in self.series else 0.0


def docker_stats(name: str) -> dict[str, float]:
    out = subprocess.run(
        ["docker", "stats", "--no-stream", "--format", "{{json .}}", name],
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    ).stdout.strip()
    if not out:
        return {}
    used, cpu = parse_docker_stats(out.splitlines()[0])
    return {"mem": used, "cpu": cpu}


def host_memory() -> dict[str, float]:
    return {"used": psutil.virtual_memory().used}
