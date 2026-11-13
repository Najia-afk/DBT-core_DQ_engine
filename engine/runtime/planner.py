"""Plan d'un run : init? -> ingest[source] (parallèles) -> reconcile -> export?"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from engine.core.config import ReferentialConfig

STEP_KINDS = ("init", "ingest", "reconcile", "export")
PLAN_PARAMS = {
    "include_init",
    "include_export",
    "export_out",
    "sources",
    "profile",
    "profile_by_kind",
    "timeout_seconds",
    "max_attempts",
}


class PlanError(ValueError):
    pass


@dataclass(frozen=True)
class StepSpec:
    step: str
    kind: str
    source_id: str | None
    depends_on: tuple[str, ...]
    profile: str
    timeout_seconds: int
    max_attempts: int


def step_name(kind: str, source_id: str | None = None) -> str:
    return f"{kind}:{source_id}" if source_id else kind


def validate_params(params: dict[str, Any]) -> None:
    unknown = set(params) - PLAN_PARAMS
    if unknown:
        raise PlanError(f"unknown run params: {sorted(unknown)}")


def build_plan(
    config: ReferentialConfig,
    params: dict[str, Any] | None = None,
    *,
    profiles: set[str] | None = None,
) -> list[StepSpec]:
    """Plan déterministe ; toutes les ingestions précèdent l'unique reconcile."""
    params = params or {}
    validate_params(params)
    default_profile = params.get("profile") or config.runtime.profile
    by_kind: dict[str, str] = params.get("profile_by_kind") or {}
    bad_kinds = set(by_kind) - set(STEP_KINDS)
    if bad_kinds:
        raise PlanError(f"profile_by_kind: unknown step kinds {sorted(bad_kinds)}")
    timeout = params.get("timeout_seconds")
    timeout = int(config.runtime.step_timeout_minutes * 60 if timeout is None else timeout)
    max_attempts = params.get("max_attempts")
    max_attempts = int(config.runtime.max_attempts if max_attempts is None else max_attempts)
    if timeout < 1 or max_attempts < 1:
        raise PlanError("timeout_seconds and max_attempts must be >= 1")

    declared = [s.id for s in config.sources]
    selected = params.get("sources")
    if selected is None:
        selected = declared
    else:
        unknown = [s for s in selected if s not in declared]
        if unknown or not selected or len(set(selected)) != len(selected):
            raise PlanError(f"invalid sources selection {selected!r}; declared: {declared}")
        selected = [s for s in declared if s in selected]

    def profile_of(kind: str) -> str:
        profile = by_kind.get(kind, default_profile)
        if profiles is not None and profile not in profiles:
            raise PlanError(f"unknown profile {profile!r}; known: {sorted(profiles)}")
        return profile

    def spec(kind: str, source_id: str | None, deps: tuple[str, ...]) -> StepSpec:
        return StepSpec(
            step_name(kind, source_id),
            kind,
            source_id,
            deps,
            profile_of(kind),
            timeout,
            max_attempts,
        )

    plan: list[StepSpec] = []
    upstream: tuple[str, ...] = ()
    if params.get("include_init"):
        plan.append(spec("init", None, ()))
        upstream = ("init",)
    ingests = [spec("ingest", s, upstream) for s in selected]
    plan.extend(ingests)
    plan.append(spec("reconcile", None, tuple(i.step for i in ingests)))
    if params.get("include_export"):
        if not params.get("export_out"):
            raise PlanError("export_out is required when include_export is set")
        plan.append(spec("export", None, ("reconcile",)))
    return plan
