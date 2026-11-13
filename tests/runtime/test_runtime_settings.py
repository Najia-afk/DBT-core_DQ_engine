"""Réglages du runtime (sans Docker)."""

from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from engine.runtime.settings import RuntimeSettings, load_runtime_settings


def test_defaults_follow_the_16_gib_profile_of_the_specification() -> None:
    s = RuntimeSettings()
    assert s.host.job_memory_budget_mib == 6144 and s.host.max_running_steps == 2
    assert s.profiles["large"].process_memory_mib == 6144 and s.profiles["large"].threads == 2
    assert s.lane.command == ["{python}", "-m", "engine.lanes.{lane}"]


def test_yaml_loading_and_strictness(tmp_path: Path) -> None:
    path = tmp_path / "rt.yml"
    path.write_text(
        "host: {max_running_steps: 5}\nretry: {retry_on: [timeout]}\n", encoding="utf-8"
    )
    s = load_runtime_settings(path)
    assert s.host.max_running_steps == 5 and s.retry.retry_on == ["timeout"]
    assert load_runtime_settings(None) == RuntimeSettings()
    path.write_text("host: {max_runing_steps: 5}\n", encoding="utf-8")
    with pytest.raises(ValidationError):
        load_runtime_settings(path)
    with pytest.raises(ValueError):
        load_runtime_settings(tmp_path / "missing.yml")


def test_heartbeat_timeout_must_exceed_interval() -> None:
    with pytest.raises(ValidationError):
        RuntimeSettings.model_validate({"heartbeat": {"interval_seconds": 5, "timeout_seconds": 5}})
