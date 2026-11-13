"""Échantillonneurs du banc : unités, séries, arbre de processus (sans Docker)."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time

import psutil
import pytest

from engine.bench.sampler import (
    Sampler,
    Series,
    combined_peak,
    kill_tree,
    parse_docker_stats,
    parse_size,
    tree_rss_bytes,
)


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("512B", 512),
        ("1KiB", 1024),
        ("1.5MiB", 1572864),
        ("2GiB", 2 * 1024**3),
        ("12.5MB", 12_500_000),
        ("3kB", 3000),
        ("0B", 0),
    ],
)
def test_parse_size(text: str, expected: int) -> None:
    assert parse_size(text) == expected


def test_parse_docker_stats() -> None:
    line = json.dumps({"MemUsage": "1.25GiB / 12GiB", "CPUPerc": "187.50%"})
    used, cpu = parse_docker_stats(line)
    assert used == int(1.25 * 1024**3)
    assert cpu == 187.5


def test_series_peak_and_at() -> None:
    s = Series()
    for t, v in [(1.0, 5.0), (2.0, 9.0), (3.0, 2.0)]:
        s.add(t, v)
    assert s.peak == 9.0
    assert s.at(0.5) == 0.0
    assert s.at(2.5) == 9.0
    assert Series().peak == 0.0


def test_combined_peak_holds_last_value() -> None:
    a, b = Series(), Series()
    for t, v in [(1.0, 10.0), (3.0, 1.0)]:
        a.add(t, v)
    for t, v in [(2.0, 7.0), (4.0, 0.0)]:
        b.add(t, v)
    # t=2 : a=10 (tenu), b=7 -> 17 ; t=3 : 1+7 ; t=4 : 1+0
    assert combined_peak(a, b) == 17.0
    assert combined_peak() == 0.0


def test_sampler_collects_and_tracks_peak() -> None:
    values = iter([{"x": 1.0}, {"x": 8.0}, {"x": 3.0}])

    def fn() -> dict[str, float]:
        try:
            return next(values)
        except StopIteration:
            return {"x": 0.0}

    sampler = Sampler(fn, interval=0.01).start()
    time.sleep(0.2)
    sampler.stop()
    assert sampler.peak("x") == 8.0
    assert sampler.peak("absent") == 0.0


def test_sampler_survives_failing_probe() -> None:
    def boom() -> dict[str, float]:
        raise RuntimeError("probe down")

    sampler = Sampler(boom, interval=0.01).start()
    time.sleep(0.05)
    sampler.stop()
    assert sampler.series == {}


def test_tree_rss_counts_children_and_kill_tree() -> None:
    code = "import subprocess,sys,time; subprocess.Popen([sys.executable,'-c','import time; time.sleep(30)']); time.sleep(30)"  # noqa: E501
    parent = subprocess.Popen([sys.executable, "-c", code])
    try:
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline and len(psutil.Process(parent.pid).children()) < 1:
            time.sleep(0.1)
        alone = psutil.Process(parent.pid).memory_info().rss
        assert tree_rss_bytes([parent.pid]) > alone
        assert tree_rss_bytes([os.getpid()]) > 0
        assert tree_rss_bytes([2**22 + 12345]) == 0
        kill_tree(parent.pid)
        parent.wait(timeout=15)
        assert parent.returncode is not None
    finally:
        if parent.poll() is None:
            parent.kill()
