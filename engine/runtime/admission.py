"""Admission et ordonnancement : logique pure, sans base ni processus.

Un step démarre seulement si l'hôte a le budget mémoire, un slot, des threads, de l'espace
disque libre et si le plafond du référentiel n'est pas atteint. L'ordre est une rotation entre
référentiels (le moins récemment servi d'abord) avec vieillissement : une demande vieille de
`aging_seconds` passe devant et, si elle ne tient pas encore, bloque les départs de petits steps
(réservation de priorité d'accès, jamais de jetons partiels).
"""

from __future__ import annotations

from dataclasses import dataclass, field

from engine.runtime.settings import RuntimeSettings

NEVER = float("-inf")


class AdmissionError(Exception):
    """Équivalent d'un HTTP 429 : plafond de file atteint ; réessayer après `retry_after` s."""

    def __init__(self, reason: str, retry_after: int) -> None:
        super().__init__(f"{reason} (retry after {retry_after}s)")
        self.reason = reason
        self.retry_after = retry_after


class IdempotencyConflict(Exception):
    """Équivalent d'un HTTP 409 : même clé d'idempotence, contenu différent."""


@dataclass(frozen=True)
class Candidate:
    run_id: str
    step: str
    referential: str
    profile: str
    ready_at: float  # epoch secondes
    run_order: tuple[float, int]  # (queued_at, seq) du run
    group_holder: str | None = None  # run détenant le groupe d'écriture du référentiel

    @property
    def key(self) -> tuple[str, str]:
        return (self.run_id, self.step)


@dataclass(frozen=True)
class Running:
    referential: str
    profile: str


@dataclass
class Decision:
    chosen: Candidate | None = None
    refused: list[tuple[Candidate, str]] = field(default_factory=list)  # jamais admissibles
    blocked: dict[tuple[str, str], str] = field(default_factory=dict)
    reserved_for: Candidate | None = None  # tête vieillie qui draine les départs


def check_submission(queued_runs: int, referential_queued_runs: int, s: RuntimeSettings) -> None:
    if queued_runs >= s.host.max_queued_runs:
        raise AdmissionError(
            f"queue full ({queued_runs}/{s.host.max_queued_runs} runs)",
            s.scheduler.retry_after_seconds,
        )
    if referential_queued_runs >= s.referential.max_queued_runs:
        raise AdmissionError(
            f"referential queue full ({referential_queued_runs}/{s.referential.max_queued_runs})",
            s.scheduler.retry_after_seconds,
        )


def capacity_refusal(profile: str, s: RuntimeSettings) -> str | None:
    """Besoin supérieur à la capacité configurée : refus explicite, pas d'attente infinie."""
    p = s.profiles.get(profile)
    if p is None:
        return f"unknown profile {profile!r}"
    if p.process_memory_mib > s.host.job_memory_budget_mib:
        return (
            f"profile {profile} needs {p.process_memory_mib} MiB > job budget "
            f"{s.host.job_memory_budget_mib} MiB"
        )
    if p.threads > s.host.max_threads:
        return f"profile {profile} needs {p.threads} threads > host limit {s.host.max_threads}"
    return None


def check_fit(
    cand: Candidate,
    running: list[Running],
    s: RuntimeSettings,
    free_disk_gib: float | None,
) -> tuple[str, bool] | None:
    """None si le step peut démarrer, sinon (motif, global). `global`: ressource d'hôte."""
    p = s.profiles[cand.profile]
    if len(running) >= s.host.max_running_steps:
        return (f"max_running_steps={s.host.max_running_steps} reached", True)
    used_mem = sum(s.profiles[r.profile].process_memory_mib for r in running)
    if used_mem + p.process_memory_mib > s.host.job_memory_budget_mib:
        return (
            f"memory budget: {used_mem}+{p.process_memory_mib} MiB > "
            f"{s.host.job_memory_budget_mib} MiB",
            True,
        )
    used_threads = sum(s.profiles[r.profile].threads for r in running)
    if used_threads + p.threads > s.host.max_threads:
        return (f"thread budget: {used_threads}+{p.threads} > {s.host.max_threads}", True)
    if free_disk_gib is not None and free_disk_gib < s.host.min_free_disk_gib:
        return (f"free disk {free_disk_gib:.1f} GiB < {s.host.min_free_disk_gib} GiB", True)
    ref_running = sum(1 for r in running if r.referential == cand.referential)
    if ref_running >= s.referential.max_running_steps:
        return (f"referential max_running_steps={s.referential.max_running_steps}", False)
    return None


def _eligible_runs(candidates: list[Candidate]) -> list[Candidate]:
    """Un seul run actif par référentiel : le détenteur du groupe, sinon le plus ancien."""
    allowed: dict[str, str] = {}
    for c in candidates:
        if c.group_holder is not None:
            allowed[c.referential] = c.group_holder
    oldest: dict[str, Candidate] = {}
    for c in candidates:
        if c.referential not in allowed:
            best = oldest.get(c.referential)
            if best is None or c.run_order < best.run_order:
                oldest[c.referential] = c
    for ref, c in oldest.items():
        allowed[ref] = c.run_id
    return [c for c in candidates if allowed[c.referential] == c.run_id]


def order_candidates(
    candidates: list[Candidate], *, now: float, aging_seconds: float, last_served: dict[str, float]
) -> list[Candidate]:
    def sort_key(c: Candidate) -> tuple:
        aged = now - c.ready_at >= aging_seconds
        if aged:
            return (0, c.ready_at, last_served.get(c.referential, NEVER), c.run_order, c.step)
        return (1, last_served.get(c.referential, NEVER), c.ready_at, c.run_order, c.step)

    return sorted(candidates, key=sort_key)


def select_next(
    candidates: list[Candidate],
    running: list[Running],
    s: RuntimeSettings,
    *,
    now: float,
    free_disk_gib: float | None = None,
    last_served: dict[str, float] | None = None,
) -> Decision:
    decision = Decision()
    pool: list[Candidate] = []
    for c in _eligible_runs(candidates):
        reason = capacity_refusal(c.profile, s)
        if reason:
            decision.refused.append((c, reason))
        else:
            pool.append(c)
    ordered = order_candidates(
        pool, now=now, aging_seconds=s.scheduler.aging_seconds, last_served=last_served or {}
    )
    for c in ordered:
        if decision.reserved_for is not None:
            decision.blocked[c.key] = f"waiting for aged step {decision.reserved_for.step} to start"
            continue
        misfit = check_fit(c, running, s, free_disk_gib)
        if misfit is None:
            decision.chosen = c
            return decision
        reason, is_global = misfit
        decision.blocked[c.key] = reason
        if is_global and now - c.ready_at >= s.scheduler.aging_seconds:
            decision.reserved_for = c
    return decision
