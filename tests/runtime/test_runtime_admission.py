"""Admission, rotation et vieillissement : logique pure (sans Docker, sans processus)."""

from __future__ import annotations

import pytest

from engine.runtime.admission import (
    AdmissionError,
    Candidate,
    Running,
    capacity_refusal,
    check_fit,
    check_submission,
    select_next,
)
from engine.runtime.settings import RuntimeSettings


def settings(**host) -> RuntimeSettings:
    base = {"max_running_steps": 4, "min_free_disk_gib": 0}
    base.update(host)
    return RuntimeSettings.model_validate({"host": base})


def cand(
    ref: str, run: str, step: str = "s", profile: str = "small", ready_at: float = 0.0, order=0
):
    return Candidate(run, step, ref, profile, ready_at, (float(order), order))


# ------------------------------------------------------------------ plafonds de file


def test_submission_caps_raise_429_like_error_with_retry_after() -> None:
    s = RuntimeSettings.model_validate(
        {
            "host": {"max_queued_runs": 3},
            "referential": {"max_queued_runs": 2},
            "scheduler": {"retry_after_seconds": 11},
        }
    )
    check_submission(2, 1, s)
    with pytest.raises(AdmissionError) as global_cap:
        check_submission(3, 0, s)
    assert global_cap.value.retry_after == 11 and "queue full" in global_cap.value.reason
    with pytest.raises(AdmissionError) as ref_cap:
        check_submission(1, 2, s)
    assert "referential" in ref_cap.value.reason


# ------------------------------------------------------------------- budgets d'hôte


def test_medium_and_small_coexist_but_large_reserves_the_whole_budget() -> None:
    s = RuntimeSettings()  # budget 6144 MiB, 2 slots (SPEC §5.1)
    assert check_fit(cand("a", "r1", profile="small"), [Running("b", "medium")], s, None) is None
    large = cand("a", "r1", profile="large")
    assert check_fit(large, [], s, None) is None
    assert check_fit(large, [Running("b", "small")], s, None) is not None
    assert check_fit(cand("a", "r1", profile="medium"), [Running("b", "medium")], s, None)


def test_slots_threads_disk_and_referential_cap() -> None:
    s = RuntimeSettings.model_validate(
        {
            "host": {"max_running_steps": 2, "max_threads": 2, "min_free_disk_gib": 10},
            "referential": {"max_running_steps": 1},
        }
    )
    c = cand("a", "r1")
    assert check_fit(c, [Running("x", "small"), Running("y", "small")], s, 50)[1] is True
    assert "free disk" in check_fit(c, [], s, 5)[0]
    assert check_fit(c, [], s, 50) is None
    reason, is_global = check_fit(c, [Running("a", "small")], s, 50)
    assert "referential" in reason and is_global is False
    big = cand("a", "r1", profile="large")  # 2 threads
    wide = settings(max_threads=2, job_memory_budget_mib=20000)
    assert "thread" in check_fit(big, [Running("z", "small")], wide, None)[0]


def test_need_above_capacity_is_refused_explicitly() -> None:
    s = settings(job_memory_budget_mib=3000)
    assert "needs 4096" in capacity_refusal("medium", s)
    assert capacity_refusal("small", s) is None
    assert "unknown profile" in capacity_refusal("nope", s)
    decision = select_next([cand("a", "r1", profile="large")], [], s, now=0)
    assert decision.chosen is None and len(decision.refused) == 1


# ------------------------------------------------------- un run actif par référentiel


def test_only_one_run_per_referential_is_eligible() -> None:
    s = settings()
    older, newer = cand("a", "run-old", "x", order=1), cand("a", "run-new", "x", order=2)
    decision = select_next([newer, older], [], s, now=10)
    assert decision.chosen == older
    held = Candidate("run-new", "y", "a", "small", 0.0, (2.0, 2), group_holder="run-new")
    assert select_next([older, held], [], s, now=10).chosen == held


# --------------------------------------------------------------- rotation / vieillissement


def test_rotation_serves_least_recently_served_referential_first() -> None:
    s = settings()
    pool = [cand("a", "ra", f"s{i}", ready_at=i, order=0) for i in range(5)]
    pool.append(cand("b", "rb", "only", ready_at=100, order=1))
    chosen = select_next(pool, [], s, now=101, last_served={"a": 90.0}).chosen
    assert chosen.referential == "b"
    chosen = select_next(pool, [], s, now=101, last_served={"a": 90.0, "b": 95.0}).chosen
    assert chosen.referential == "a"


def test_small_jobs_backfill_until_the_large_one_is_aged_then_the_host_drains() -> None:
    s = RuntimeSettings.model_validate(
        {
            "host": {"max_running_steps": 3, "min_free_disk_gib": 0},
            "scheduler": {"aging_seconds": 60},
        }
    )
    running = [Running("x", "small")]
    large = cand("big", "rl", profile="large", ready_at=0, order=0)
    small = cand("sm", "rs", profile="small", ready_at=50, order=1)
    young = select_next([large, small], running, s, now=30, last_served={"sm": 1.0})
    assert young.chosen == small  # le gros n'est pas encore vieilli : rétro-remplissage
    aged = select_next([large, small], running, s, now=61, last_served={"sm": 1.0})
    assert aged.chosen is None and aged.reserved_for == large
    assert "aged" in aged.blocked[small.key]
    assert select_next([large, small], [], s, now=61).chosen == large


# -------------------------------------------------------- simulation d'absence de famine


def simulate(s: RuntimeSettings, horizon: int, small_refs: int = 3):
    """Flux continu de petits steps (durées 20/27/34), un gros prêt dès t=0 : ses départs."""
    running: list[tuple[int, str, Running]] = []
    waiting: dict[str, Candidate] = {}
    last_served: dict[str, float] = {}
    starts: dict[str, list[int]] = {}
    for i in range(small_refs):
        ref = f"small{i}"
        waiting[ref] = cand(ref, f"r-{ref}", "j0", "small", ready_at=0, order=i)
    waiting["big"] = cand("big", "r-big", "j0", "large", ready_at=0, order=99)
    counters = dict.fromkeys(waiting, 0)
    for t in range(horizon):
        running = [r for r in running if r[0] > t]
        while True:
            decision = select_next(
                list(waiting.values()),
                [r[2] for r in running],
                s,
                now=float(t),
                last_served=last_served,
            )
            if decision.chosen is None:
                break
            c = decision.chosen
            duration = 40 if c.referential == "big" else 20 + 7 * int(c.referential[-1])
            running.append((t + duration, "", Running(c.referential, c.profile)))
            starts.setdefault(c.referential, []).append(t)
            last_served[c.referential] = float(t)
            if c.referential == "big":
                del waiting["big"]
            else:
                counters[c.referential] += 1
                waiting[c.referential] = cand(
                    c.referential,
                    f"r-{c.referential}",
                    f"j{counters[c.referential]}",
                    "small",
                    ready_at=float(t),
                    order=int(c.referential[-1]),
                )
    return starts


def test_large_job_does_not_starve_under_continuous_small_load() -> None:
    s = RuntimeSettings.model_validate(
        {
            "host": {"max_running_steps": 3, "min_free_disk_gib": 0},
            "scheduler": {"aging_seconds": 60},
        }
    )
    starts = simulate(s, horizon=400)
    assert "big" in starts
    # bornes : vieillissement + durée max des petits qui occupent l'hôte
    assert starts["big"][0] <= 60 + 34 + 1


def test_without_aging_the_same_load_would_starve_the_large_job() -> None:
    s = RuntimeSettings.model_validate(
        {
            "host": {"max_running_steps": 3, "min_free_disk_gib": 0},
            "scheduler": {"aging_seconds": 10**9},
        }
    )
    assert "big" not in simulate(s, horizon=400)  # prouve que le test précédent est discriminant


def test_rotation_shares_slots_between_referentials() -> None:
    s = RuntimeSettings.model_validate({"host": {"max_running_steps": 1, "min_free_disk_gib": 0}})
    waiting = {
        "a": [cand("a", "ra", f"s{i}", ready_at=0, order=0) for i in range(30)],
        "b": [cand("b", "rb", f"s{i}", ready_at=0, order=1) for i in range(30)],
        "c": [cand("c", "rc", "s0", ready_at=0, order=2)],
    }
    served: dict[str, float] = {}
    order: list[str] = []
    for t in range(12):
        pool = [lst[0] for lst in waiting.values() if lst]
        chosen = select_next(pool, [], s, now=float(t), last_served=served).chosen
        order.append(chosen.referential)
        served[chosen.referential] = float(t)
        waiting[chosen.referential].pop(0)
    assert order[:3] == ["a", "b", "c"]  # chacun servi avant que a ne repasse
    assert order.count("a") >= 4 and order.count("b") >= 4
