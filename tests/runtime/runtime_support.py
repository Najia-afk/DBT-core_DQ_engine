"""Aides partagées des tests du runtime (config référentiel, réglages rapides, journaux)."""

from __future__ import annotations

import json
import sys
import time
from pathlib import Path
from typing import Any

from engine.runtime.settings import RuntimeSettings

FAKE_LANE = Path(__file__).with_name("fake_lane.py")
REPO_ROOT = Path(__file__).resolve().parents[2]


def write_config(
    directory: Path,
    referential: str,
    sources: tuple[str, ...] = ("s1",),
    *,
    max_attempts: int = 3,
    profile: str = "small",
    extra_text: str = "",
) -> Path:
    """Config référentiel valide (engine.core.config) ; le fichier s'appelle <referentiel>.yml."""
    source_lines = "\n".join(
        f"  - {{id: {s}, kind: parquet, location: /data/{s}/*.parquet, key: id, snapshot: delta}}"
        for s in sources
    )
    columns = ", ".join(f"{{source: {s}, column: name}}" for s in sources)
    text = f"""referential: {referential}
version: 1
state: {{dsn_env: MDM_STATE_DSN, schema: mdm_{referential}}}
sources:
{source_lines}
attributes:
  - name: last_name
    sources: [{columns}]
matching:
  version: v1
  blocking: [{{name: ln, parts: [{{attr: last_name}}]}}]
  comparators: [{{attr: last_name, method: exact, weight: 1.0}}]
runtime: {{profile: {profile}, max_attempts: {max_attempts}, step_timeout_minutes: 1}}
{extra_text}"""
    path = directory / f"{referential}.yml"
    path.write_text(text, encoding="utf-8")
    return path


def make_settings(fake_dir: Path, **overrides: Any) -> RuntimeSettings:
    """Réglages rapides pour tests ; `overrides` fusionne section par section."""
    data: dict[str, Any] = {
        "host": {
            "job_memory_budget_mib": 4000,
            "max_running_steps": 4,
            "max_queued_runs": 100,
            "min_free_disk_gib": 0,
        },
        "profiles": {
            "small": {"process_memory_mib": 400, "duckdb_memory_mib": 100, "threads": 1},
            "medium": {"process_memory_mib": 800, "duckdb_memory_mib": 200, "threads": 1},
            "large": {"process_memory_mib": 1600, "duckdb_memory_mib": 400, "threads": 2},
        },
        "referential": {"max_running_steps": 3, "max_queued_runs": 50},
        "scheduler": {
            "poll_interval_seconds": 0.05,
            "aging_seconds": 300,
            "reaper_interval_seconds": 0.3,
            "retry_after_seconds": 7,
        },
        "heartbeat": {"interval_seconds": 0.2, "timeout_seconds": 1.5},
        "monitor": {"rss_interval_seconds": 0.1, "kill_grace_seconds": 1},
        "retry": {"backoff_base_seconds": 0.2, "backoff_max_seconds": 1, "jitter": 0},
        "lane": {
            "command": ["{python}", str(FAKE_LANE)],
            "python": sys.executable,
            "env": {"FAKE_LANE_DIR": str(fake_dir)},
        },
    }
    for section, value in overrides.items():
        if isinstance(value, dict) and isinstance(data.get(section), dict):
            data[section].update(value)
        else:
            data[section] = value
    return RuntimeSettings.model_validate(data)


def write_scenario(fake_dir: Path, scenario: dict[str, Any]) -> None:
    (fake_dir / "scenario.json").write_text(json.dumps(scenario), encoding="utf-8")


def _records(directory: Path) -> list[str]:
    if not directory.exists():
        return []
    return [p.read_text(encoding="utf-8") for p in sorted(directory.iterdir())]


def read_events(fake_dir: Path) -> list[dict[str, Any]]:
    return [json.loads(text) for text in _records(fake_dir / "events")]


def read_effects(fake_dir: Path) -> list[str]:
    return _records(fake_dir / "effects")


def first(events: list[dict], ev: str, run_id: str, step: str, attempt: int | None = None):
    return min(
        e["t"]
        for e in events
        if e["ev"] == ev
        and e["run_id"] == run_id
        and e["step"] == step
        and (attempt is None or e["attempt"] == attempt)
    )


def last(events: list[dict], ev: str, run_id: str, step: str) -> float:
    return max(
        e["t"] for e in events if e["ev"] == ev and e["run_id"] == run_id and e["step"] == step
    )


def intervals(events: list[dict]) -> list[tuple[float, float, dict]]:
    """(début, fin, événement start) de chaque tentative ayant démarré puis terminé."""
    ends = {(e["run_id"], e["step"], e["attempt"]): e["t"] for e in events if e["ev"] == "end"}
    out = []
    for e in events:
        if e["ev"] == "start":
            end = ends.get((e["run_id"], e["step"], e["attempt"]))
            if end is not None:
                out.append((e["t"], end, e))
    return out


def max_concurrency(spans: list[tuple[float, float, dict]]) -> int:
    points = sorted([(a, 1) for a, _, _ in spans] + [(b, -1) for _, b, _ in spans])
    current = best = 0
    for _, delta in points:
        current += delta
        best = max(best, current)
    return best


def wait_for(predicate, timeout: float = 30.0, interval: float = 0.1, message: str = ""):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(interval)
    raise AssertionError(f"timeout waiting for condition {message}")
