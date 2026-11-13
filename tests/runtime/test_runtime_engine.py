"""Tests du runtime contre un vrai PostgreSQL (MDM_TEST_DOCKER=1) avec une fausse lane."""

from __future__ import annotations

import asyncio
import json
import subprocess
import sys
import threading
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

import psutil
import psycopg
import pytest
import yaml
from runtime_support import (
    REPO_ROOT,
    first,
    intervals,
    last,
    make_settings,
    max_concurrency,
    read_effects,
    read_events,
    write_config,
    write_scenario,
)

from engine.runtime import (
    AdmissionError,
    IdempotencyConflict,
    RuntimeEngine,
    RuntimeSettings,
    RuntimeStore,
)
from engine.runtime.executor import LaneResult


class Env:
    def __init__(self, dsn: str, fake_dir: Path, cfg_dir: Path) -> None:
        self.dsn, self.fake_dir, self.cfg_dir = dsn, fake_dir, cfg_dir
        self._stores: list[RuntimeStore] = []
        self.scenario({})

    def settings(self, **overrides: Any) -> RuntimeSettings:
        return make_settings(self.fake_dir, **overrides)

    def store(self, settings: RuntimeSettings | None = None) -> RuntimeStore:
        store = RuntimeStore(self.dsn, settings or self.settings())
        store.open()
        store.migrate()
        self._stores.append(store)
        return store

    def config(self, ref: str, sources: tuple[str, ...] = ("s1",), **kw: Any) -> Path:
        return write_config(self.cfg_dir, ref, sources, **kw)

    def scenario(self, scenario: dict[str, Any]) -> None:
        write_scenario(self.fake_dir, scenario)

    def events(self) -> list[dict[str, Any]]:
        return read_events(self.fake_dir)

    async def drain(self, settings: RuntimeSettings | None = None, max_seconds: float = 90) -> None:
        async with RuntimeEngine(self.dsn, settings or self.settings()) as engine:
            await engine.run_until_idle(max_seconds=max_seconds)

    def close(self) -> None:
        for store in self._stores:
            store.close()


@pytest.fixture
def env(meta_dsn: str, fake_dir: Path, cfg_dir: Path):
    environment = Env(meta_dsn, fake_dir, cfg_dir)
    yield environment
    environment.close()


def step_of(run: dict, name: str) -> dict:
    return next(s for s in run["steps"] if s["step"] == name)


def outcomes(run: dict, name: str) -> list[str]:
    return [a["outcome"] for a in run["attempts"] if a["step"] == name]


async def until(predicate: Callable[[], Any], timeout: float = 30.0, what: str = "") -> Any:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = await asyncio.to_thread(predicate)
        if value:
            return value
        await asyncio.sleep(0.1)
    raise AssertionError(f"timeout waiting for {what}")


def pid_dead(pid: int) -> bool:
    try:
        return psutil.Process(pid).status() == psutil.STATUS_ZOMBIE
    except psutil.NoSuchProcess:
        return True


# --------------------------------------------------------------------- migration


def test_migration_is_idempotent_and_concurrent(env: Env) -> None:
    stores = [RuntimeStore(env.dsn, env.settings()) for _ in range(4)]
    for s in stores:
        s.open()
    errors: list[BaseException] = []

    def migrate(store: RuntimeStore) -> None:
        try:
            store.migrate()
        except BaseException as error:  # noqa: BLE001
            errors.append(error)

    threads = [threading.Thread(target=migrate, args=(s,)) for s in stores]
    [t.start() for t in threads]
    [t.join() for t in threads]
    stores[0].migrate()
    for s in stores:
        s.close()
    assert errors == []
    with psycopg.connect(env.dsn) as conn:
        tables = {
            r[0]
            for r in conn.execute(
                "SELECT table_name FROM information_schema.tables"
                " WHERE table_schema = 'mdm_runtime'"
            )
        }
    assert {"runs", "steps", "attempts", "write_groups", "workers"} <= tables


# ----------------------------------------------------------------- ordonnancement


@pytest.mark.parametrize(
    "summary",
    [{"counts": {"blocked": True}}, {"status": "blocked"}, {"counters": {"s1": {"blocked": True}}}],
)
async def test_blocked_ingest_run_finishes_with_warnings(env: Env, summary: dict) -> None:
    env.scenario({"ingest:s1": {"summary": summary}})
    store = env.store()
    sub = store.submit_run(env.config("blocked_source"))
    await env.drain()
    assert store.get_run(sub.run_id)["status"] == "succeeded_with_warnings"


async def test_plan_runs_in_order_and_ingests_precede_reconcile(env: Env) -> None:
    env.scenario({"default": {"sleep": 0.3}, "reconcile": {"warnings": 2}})
    store = env.store()
    cfg = env.config("orders", ("s1", "s2", "s3"))
    sub = store.submit_run(
        cfg, params={"include_init": True, "include_export": True, "export_out": "out"}
    )
    assert sub.created and sub.status == "queued"
    await env.drain()
    run = store.get_run(sub.run_id)
    assert run["status"] == "succeeded_with_warnings"
    assert all(s["status"] == "succeeded" for s in run["steps"])
    ev, rid = env.events(), sub.run_id
    assert last(ev, "end", rid, "init") <= first(ev, "start", rid, "ingest:s1")
    latest_ingest = max(last(ev, "end", rid, f"ingest:s{i}") for i in (1, 2, 3))
    assert latest_ingest <= first(ev, "start", rid, "reconcile")
    assert last(ev, "end", rid, "reconcile") <= first(ev, "start", rid, "export")
    assert not run["holds_write_group"]
    assert len(read_effects(env.fake_dir)) == 6


async def test_parallel_ingests_are_bounded(env: Env) -> None:
    env.scenario({"ingest:s1": {"sleep": 0.8}, "default": {"sleep": 0.8}})
    settings = env.settings(referential={"max_running_steps": 2})
    store = env.store(settings)
    sub = store.submit_run(env.config("bounded", ("s1", "s2", "s3", "s4")))
    await env.drain(settings)
    assert store.get_run(sub.run_id)["status"] == "succeeded"
    ingests = [x for x in intervals(env.events()) if x[2]["step"].startswith("ingest")]
    assert max_concurrency(ingests) == 2


async def test_referentials_run_concurrently_but_runs_of_one_referential_serialize(
    env: Env,
) -> None:
    env.scenario({"default": {"sleep": 0.7}})
    store = env.store()
    a1 = store.submit_run(env.config("alpha"), idempotency_key="a1")
    a2 = store.submit_run(env.config("alpha"), idempotency_key="a2")
    b1 = store.submit_run(env.config("beta"), idempotency_key="b1")
    await env.drain()
    ev = env.events()
    for sub in (a1, a2, b1):
        assert store.get_run(sub.run_id)["status"] == "succeeded"
    # alpha : le second run ne démarre qu'après la fin complète du premier (même groupe d'écriture)
    assert last(ev, "end", a1.run_id, "reconcile") <= first(ev, "start", a2.run_id, "ingest:s1")
    # beta tourne en même temps qu'alpha
    assert first(ev, "start", b1.run_id, "ingest:s1") < last(ev, "end", a1.run_id, "reconcile")
    alpha = [x for x in intervals(ev) if x[2]["referential"] == "alpha"]
    assert max_concurrency(alpha) == 1


# ---------------------------------------------------------------------- reprises


async def test_retry_with_backoff_then_success(env: Env) -> None:
    env.scenario({"ingest:s1": {"fail_times": 2}})
    store = env.store()
    sub = store.submit_run(env.config("retry", max_attempts=3))
    await env.drain()
    run = store.get_run(sub.run_id)
    assert run["status"] == "succeeded"
    ingest = step_of(run, "ingest:s1")
    assert ingest["attempt"] == 3 and ingest["status"] == "succeeded"
    assert outcomes(run, "ingest:s1") == ["failed", "failed", "succeeded"]
    ev = env.events()
    gap = first(ev, "start", sub.run_id, "ingest:s1", 2) - first(
        ev, "start", sub.run_id, "ingest:s1", 1
    )
    assert gap >= 0.2  # backoff exponentiel
    assert len([e for e in read_effects(env.fake_dir) if "ingest" in e]) == 1


async def test_retry_exhaustion_fails_the_run_and_skips_downstream(env: Env) -> None:
    env.scenario({"ingest:s1": {"fail_times": 9}})
    store = env.store()
    sub = store.submit_run(env.config("exhaust", max_attempts=2))
    await env.drain()
    run = store.get_run(sub.run_id)
    assert run["status"] == "failed"
    ingest = step_of(run, "ingest:s1")
    assert ingest["status"] == "failed" and ingest["attempt"] == 2
    assert "exit code 1" in ingest["error"] and "scripted failure" in ingest["error"]
    assert step_of(run, "reconcile")["status"] == "skipped"
    assert not run["holds_write_group"]


async def test_non_retryable_failure_is_not_retried(env: Env) -> None:
    env.scenario({"ingest:s1": {"fail_times": 9, "retryable": False}})
    store = env.store()
    sub = store.submit_run(env.config("fatal", max_attempts=5))
    await env.drain()
    run = store.get_run(sub.run_id)
    assert run["status"] == "failed" and step_of(run, "ingest:s1")["attempt"] == 1


async def test_failure_of_one_ingest_lets_siblings_finish_but_blocks_reconcile(env: Env) -> None:
    env.scenario({"ingest:s1": {"fail_times": 9, "retryable": False}, "ingest:s2": {"sleep": 0.6}})
    store = env.store()
    sub = store.submit_run(env.config("sib", ("s1", "s2")))
    await env.drain()
    run = store.get_run(sub.run_id)
    assert run["status"] == "failed"
    assert step_of(run, "ingest:s2")["status"] == "succeeded"
    assert step_of(run, "reconcile")["status"] == "skipped"


# ------------------------------------------------------------ supervision des processus


async def test_timeout_kills_the_process_tree(env: Env) -> None:
    env.scenario({"ingest:s1": {"hang": True}})
    store = env.store()
    sub = store.submit_run(env.config("hang"), params={"timeout_seconds": 1})
    started = time.monotonic()
    await env.drain()
    assert time.monotonic() - started < 20
    run = store.get_run(sub.run_id)
    ingest = step_of(run, "ingest:s1")
    assert run["status"] == "failed" and ingest["status"] == "failed"
    assert ingest["error_kind"] == "timeout" and "timeout" in ingest["error"]
    assert pid_dead(ingest["pid"])
    assert all(pid_dead(e["pid"]) for e in env.events())


async def test_rss_over_profile_budget_kills_and_explains(env: Env) -> None:
    env.scenario({"ingest:s1": {"alloc_mb": 900, "sleep": 30}})
    store = env.store()
    sub = store.submit_run(env.config("hungry", profile="small"))  # budget small = 400 MiB
    await env.drain()
    run = store.get_run(sub.run_id)
    ingest = step_of(run, "ingest:s1")
    assert run["status"] == "failed" and ingest["error_kind"] == "rss_exceeded"
    assert "exceeded the profile budget of 400 MiB" in ingest["error"]
    assert ingest["peak_rss_mib"] > 400
    assert all(pid_dead(e["pid"]) for e in env.events())
    assert read_effects(env.fake_dir) == []


async def test_cancel_running_run_terminates_the_tree(env: Env) -> None:
    env.scenario({"ingest:s1": {"sleep": 30}})
    store = env.store()
    sub = store.submit_run(env.config("cancelme"))
    engine = RuntimeEngine(env.dsn, env.settings())
    task = asyncio.create_task(engine.run_until_idle(max_seconds=60))
    try:
        await until(
            lambda: (
                (r := store.get_run(sub.run_id))
                and step_of(r, "ingest:s1")["status"] == "running"
                and step_of(r, "ingest:s1")["pid"]
            ),
            what="step running",
        )
        assert store.cancel_run(sub.run_id) == "cancel_requested"
        await asyncio.wait_for(task, 30)
    finally:
        await engine.stop()
    run = store.get_run(sub.run_id)
    assert run["status"] == "cancelled"
    assert step_of(run, "ingest:s1")["status"] == "cancelled"
    assert step_of(run, "reconcile")["status"] == "cancelled"
    assert outcomes(run, "ingest:s1") == ["cancelled"]
    assert pid_dead(step_of(run, "ingest:s1")["pid"])
    assert not run["holds_write_group"] and read_effects(env.fake_dir) == []


async def test_graceful_shutdown_releases_the_step_for_retry(env: Env) -> None:
    env.scenario({"ingest:s1": {"sleep": 30}})
    store = env.store()
    sub = store.submit_run(env.config("shutdown"))
    engine = RuntimeEngine(env.dsn, env.settings())
    task = asyncio.create_task(engine.run_forever())
    await until(
        lambda: (
            (r := store.get_run(sub.run_id))
            and step_of(r, "ingest:s1")["status"] == "running"
            and step_of(r, "ingest:s1")["pid"]
        ),
        what="step running",
    )
    engine.request_stop()
    await asyncio.wait_for(task, 30)
    run = store.get_run(sub.run_id)
    assert step_of(run, "ingest:s1")["status"] == "retry_wait"
    assert outcomes(run, "ingest:s1") == ["shutdown"]
    assert run["status"] == "running" and run["holds_write_group"]
    assert all(pid_dead(e["pid"]) for e in env.events())
    env.scenario({})
    await env.drain()
    run = store.get_run(sub.run_id)
    assert run["status"] == "succeeded"
    assert outcomes(run, "ingest:s1") == ["shutdown", "succeeded"]


def test_cancel_of_a_queued_run_is_immediate(env: Env) -> None:
    store = env.store()
    sub = store.submit_run(env.config("queuedcancel"))
    assert store.cancel_run(sub.run_id) == "cancelled"
    run = store.get_run(sub.run_id)
    assert {s["status"] for s in run["steps"]} == {"cancelled"}
    assert store.cancel_run(sub.run_id) == "cancelled"  # idempotent
    with pytest.raises(KeyError):
        store.cancel_run("run-unknown")


# ------------------------------------------------------- reprise sur incident / fencing


async def test_crash_recovery_kills_orphan_and_runs_each_step_once(
    env: Env, tmp_path: Path
) -> None:
    env.scenario({"ingest:s1": {"sleep": 6}})
    settings = env.settings()
    store = env.store()
    sub = store.submit_run(env.config("crash"))
    runtime_yml = tmp_path / "runtime.yml"
    runtime_yml.write_text(yaml.safe_dump(settings.model_dump(mode="json")), encoding="utf-8")
    log = open(tmp_path / "worker.log", "wb")  # noqa: SIM115
    worker = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "engine.runtime",
            "--dsn",
            env.dsn,
            "--runtime-config",
            str(runtime_yml),
            "worker",
        ],
        cwd=REPO_ROOT,
        stdout=log,
        stderr=log,
    )
    try:
        await until(
            lambda: (
                (r := store.get_run(sub.run_id))
                and step_of(r, "ingest:s1")["status"] == "running"
                and step_of(r, "ingest:s1")["pid"]
                and env.events()
            ),
            timeout=40,
            what="worker started the step",
        )
        with psycopg.connect(env.dsn) as conn:
            worker_pid = conn.execute(
                "SELECT pid FROM mdm_runtime.workers ORDER BY started_at DESC LIMIT 1"
            ).fetchone()[0]
        orphan_pid = env.events()[0]["pid"]
        psutil.Process(worker_pid).kill()  # le worker meurt, pas son sous-processus
        worker.kill()
        assert psutil.pid_exists(orphan_pid), "the lane process should survive its worker"
        await env.drain()
    finally:
        worker.kill()
        log.close()
    run = store.get_run(sub.run_id)
    assert run["status"] == "succeeded"
    assert outcomes(run, "ingest:s1") == ["lost", "succeeded"]
    assert step_of(run, "ingest:s1")["generation"] == 3  # claim 1, perte (fencing) 2, claim 3
    assert pid_dead(orphan_pid)
    effects = read_effects(env.fake_dir)
    assert sorted(e.split()[1] for e in effects) == ["ingest:s1", "reconcile"]  # aucun double effet
    starts = [e for e in env.events() if e["ev"] == "start" and e["step"] == "ingest:s1"]
    assert len(starts) == 2 and len([e for e in env.events() if e["ev"] == "end"]) == 2


def test_stale_owner_is_fenced_after_attempt_is_lost(env: Env) -> None:
    store = env.store()
    sub = store.submit_run(env.config("fence"))
    store.register_worker("w-old", "hostx", 1, 0.0)
    old = store.claim_next("w-old", "hostx")
    assert old and old.generation == 1 and old.step == "ingest:s1"
    assert store.heartbeat(old, 10) == "ok"
    assert store.mark_lost(old.run_id, old.step, old.generation, "owner presumed dead")
    # l'ancien propriétaire ne peut plus rien valider
    assert store.heartbeat(old, 10) == "fenced"
    assert not store.record_pid(old, 1, 0.0)
    assert not store.finish_step(old, LaneResult("succeeded", summary={"x": 1}))
    run = store.get_run(sub.run_id)
    assert step_of(run, "ingest:s1")["status"] == "retry_wait"
    assert run["status"] == "running" and run["holds_write_group"]
    time.sleep(0.4)  # backoff
    new = store.claim_next("w-new", "hostx")
    assert new and new.attempt == 2 and new.generation == 3
    assert store.finish_step(new, LaneResult("succeeded", summary={"ok": True}))
    assert not store.finish_step(new, LaneResult("succeeded", summary={"ok": True}))


async def test_lost_heartbeat_marks_needs_attention_and_keeps_the_write_group(env: Env) -> None:
    store = env.store()
    cfg = env.config("ghost")
    first_run = store.submit_run(cfg, idempotency_key="one")
    second_run = store.submit_run(cfg, idempotency_key="two")
    store.register_worker("w-remote", "another-host", 99999999, 0.0)
    claim = store.claim_next("w-remote", "another-host")
    assert claim and claim.run_id == first_run.run_id
    with psycopg.connect(env.dsn) as conn:
        conn.execute(
            "UPDATE mdm_runtime.steps SET heartbeat_at = now() - interval '1 hour'"
            " WHERE run_id = %s",
            (first_run.run_id,),
        )
    await env.drain()
    run = store.get_run(first_run.run_id)
    assert run["status"] == "needs_attention"
    assert step_of(run, "ingest:s1")["status"] == "suspect"
    assert "heartbeat lost" in step_of(run, "ingest:s1")["error"]
    assert run["holds_write_group"], "the write group must not be released silently"
    other = store.get_run(second_run.run_id)
    assert other["status"] == "queued" and other["attempts"] == []
    assert env.events() == []
    with pytest.raises(ValueError):
        store.resolve_attention(first_run.run_id, confirmed_stopped=False)
    assert store.resolve_attention(first_run.run_id, confirmed_stopped=True) == "failed"
    await env.drain()
    assert store.get_run(second_run.run_id)["status"] == "succeeded"


# ----------------------------------------------------- soumission, admission, équité


def test_idempotent_submission(env: Env) -> None:
    store = env.store()
    cfg = env.config("idem")
    one = store.submit_run(cfg, idempotency_key="k", params={"include_init": True})
    again = store.submit_run(cfg, idempotency_key="k", params={"include_init": True})
    assert (one.created, again.created) == (True, False) and one.run_id == again.run_id
    with pytest.raises(IdempotencyConflict):
        store.submit_run(cfg, idempotency_key="k", params={"include_init": False})
    with pytest.raises(IdempotencyConflict):
        store.submit_run(cfg, idempotency_key="k", lane="elt", params={"include_init": True})
    cfg.write_text(cfg.read_text(encoding="utf-8") + "# modifiée\n", encoding="utf-8")
    with pytest.raises(IdempotencyConflict):
        store.submit_run(cfg, idempotency_key="k", params={"include_init": True})
    other = store.submit_run(env.config("idem2"), idempotency_key="k")  # clé par référentiel
    assert other.created and other.run_id != one.run_id
    assert len(store.list_runs()) == 2


def test_concurrent_submissions_with_one_key_create_a_single_run(env: Env) -> None:
    store = env.store()
    cfg = env.config("race")
    results: list[Any] = []

    def go() -> None:
        results.append(store.submit_run(cfg, idempotency_key="same"))

    threads = [threading.Thread(target=go) for _ in range(8)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    assert sum(r.created for r in results) == 1 and len({r.run_id for r in results}) == 1


async def test_admission_refuses_when_queue_is_full_but_existing_keys_still_resolve(
    env: Env,
) -> None:
    settings = env.settings(host={"max_queued_runs": 2})
    store = env.store(settings)
    first_run = store.submit_run(env.config("q1"), idempotency_key="k1")
    store.submit_run(env.config("q2"), idempotency_key="k2")
    with pytest.raises(AdmissionError) as refused:
        store.submit_run(env.config("q3"), idempotency_key="k3")
    assert refused.value.retry_after == 7 and "queue full" in refused.value.reason
    # la recherche d'une demande existante précède le contrôle de plafond
    assert store.submit_run(env.config("q1"), idempotency_key="k1").run_id == first_run.run_id
    await env.drain(settings)
    assert store.submit_run(env.config("q3"), idempotency_key="k3").created


def test_per_referential_queue_cap(env: Env) -> None:
    store = env.store(env.settings(referential={"max_queued_runs": 1}))
    cfg = env.config("capped")
    store.submit_run(cfg, idempotency_key="1")
    with pytest.raises(AdmissionError):
        store.submit_run(cfg, idempotency_key="2")
    assert store.submit_run(env.config("free"), idempotency_key="1").created


async def test_need_above_capacity_fails_explicitly(env: Env) -> None:
    settings = env.settings(host={"job_memory_budget_mib": 1000})
    store = env.store(settings)
    sub = store.submit_run(env.config("toobig"), params={"profile": "large"})
    await env.drain(settings)
    run = store.get_run(sub.run_id)
    assert run["status"] == "failed"
    assert step_of(run, "ingest:s1")["error_kind"] == "capacity_exceeded"
    assert "needs 1600 MiB" in step_of(run, "ingest:s1")["error"]
    assert env.events() == []


async def test_waiting_resource_is_visible_then_everything_completes(env: Env) -> None:
    env.scenario({"default": {"sleep": 1.0}})
    settings = env.settings(host={"max_running_steps": 1})
    store = env.store(settings)
    one = store.submit_run(env.config("wr1"))
    two = store.submit_run(env.config("wr2"))
    engine = RuntimeEngine(env.dsn, settings)
    task = asyncio.create_task(engine.run_until_idle(max_seconds=60))
    try:
        waiting = await until(
            lambda: [
                r
                for r in (store.get_run(one.run_id), store.get_run(two.run_id))
                if r["status"] == "waiting_resource"
            ],
            what="a run in waiting_resource",
        )
        assert "max_running_steps=1" in waiting[0]["status_reason"]
        await asyncio.wait_for(task, 60)
    finally:
        await engine.stop()
    assert store.get_run(one.run_id)["status"] == "succeeded"
    assert store.get_run(two.run_id)["status"] == "succeeded"


async def test_rotation_prevents_a_big_referential_from_monopolizing_the_host(env: Env) -> None:
    env.scenario({"default": {"sleep": 0.25}})
    settings = env.settings(host={"max_running_steps": 1})
    store = env.store(settings)
    big = store.submit_run(env.config("zbig", tuple(f"s{i}" for i in range(6))))
    small = store.submit_run(env.config("asmall"))
    await env.drain(settings)
    starts = sorted((e for e in env.events() if e["ev"] == "start"), key=lambda e: e["t"])
    order = [e["referential"] for e in starts]
    assert order[0] == "zbig" and order[1] == "asmall", order  # pas 6 steps de zbig d'abord
    assert store.get_run(big.run_id)["status"] == "succeeded"
    assert store.get_run(small.run_id)["status"] == "succeeded"


# ------------------------------------------------------------------------------ CLI


def test_cli_submit_worker_status(env: Env, tmp_path: Path) -> None:
    env.store().close()  # migration
    env.scenario({"default": {"sleep": 0.1}})
    runtime_yml = tmp_path / "runtime.yml"
    runtime_yml.write_text(yaml.safe_dump(env.settings().model_dump(mode="json")), encoding="utf-8")
    cfg = env.config("clirun", ("s1", "s2"))
    base = [
        sys.executable,
        "-m",
        "engine.runtime",
        "--dsn",
        env.dsn,
        "--runtime-config",
        str(runtime_yml),
    ]

    def cli(*args: str) -> tuple[int, dict]:
        done = subprocess.run(
            [*base, *args], cwd=REPO_ROOT, capture_output=True, text=True, timeout=120
        )
        return done.returncode, json.loads(done.stdout.strip().splitlines()[-1])

    code, created = cli("submit", "--config", str(cfg), "--key", "cli-1", "--include-init")
    assert code == 0 and created["created"] is True
    code, again = cli("submit", "--config", str(cfg), "--key", "cli-1", "--include-init")
    assert code == 0 and again["created"] is False and again["run_id"] == created["run_id"]
    code, conflict = cli("submit", "--config", str(cfg), "--key", "cli-1")
    assert code == 4 and conflict["error"] == "idempotency_conflict"
    code, _ = cli("worker", "--until-idle", "--max-seconds", "90")
    assert code == 0
    code, status = cli("status", "--run-id", created["run_id"])
    assert code == 0 and status["status"] == "succeeded" and len(status["steps"]) == 4
    code, listing = cli("status")
    assert code == 0 and listing["runs"][0]["run_id"] == created["run_id"]
    code, missing = cli("cancel", "--run-id", "run-nope")
    assert code == 5 and missing["error"] == "not_found"
