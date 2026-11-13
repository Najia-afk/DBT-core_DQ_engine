"""Incremental equivalence (acceptance property n°1) on seeded random deltas.

J1 then random deltas (inserts, updates that merge or split, deletions in full snapshots,
DQ rejects, duplicate keys, oversized blocks, blocked goldens) must give the same state as a
cold recompute from the current source state, modulo ids.
"""

import random
from collections import Counter
from functools import lru_cache

import pytest
from ref_helpers import check_incremental, make_config, rows

from engine.core.reference import ReferenceEngine

SOURCES = ("crm", "erp", "web")
COLUMNS = {
    "crm": {"ln": "ln", "fn": "fn", "bd": "bd", "em": "em", "ct": "ct"},
    "erp": {"ln": "nom", "fn": "prenom", "bd": "dn", "ct": "pays", "pl": "plafond"},
    "web": {"ln": "surname", "fn": "given", "em": "mail"},
}
LAST = ["dupont", "dupond", "durand", "martin", "martinez", "bernard", "petit", "moreau"]
FIRST = ["jean", "jeanne", "paul", "pierre", "marie", "anne"]
BIRTH = ["1980-05-01", "1980-06-15", "1981-01-01", "1979-12-31"]
EMAIL = [None, None, "p0@x.fr", "p1@x.fr", "p2@x.fr", "p3@x.fr", "not-an-email"]
COUNTRY = ["FR", "BE", "DE", "ZZ", None]
PLAFOND = [None, None, "10", "250.5", "999"]
INVALID = {
    "ln": "x" * 15,
    "bd": "garbage",
    "pl": "-3",
}


def typo(rng: random.Random, text: str) -> str:
    i = rng.randrange(len(text))
    return text[:i] + rng.choice("abcdeghnrt") + text[i + 1 :]


def fresh_fields(rng: random.Random, source: str) -> dict:
    fields = {
        "ln": rng.choice(LAST),
        "fn": rng.choice(FIRST),
        "bd": rng.choice(BIRTH),
        "em": rng.choice(EMAIL),
        "ct": rng.choice(COUNTRY),
        "pl": rng.choice(PLAFOND),
    }
    if rng.random() < 0.15:
        fields["ln"] = typo(rng, fields["ln"])
    if rng.random() < 0.05:
        fields["ln"] = None
    return {k: v for k, v in fields.items() if k in COLUMNS[source]}


def to_row(source: str, key: str, rec: dict) -> dict:
    row = {"id": key, "rev": rec["rev"]}
    for logical, column in COLUMNS[source].items():
        row[column] = rec["f"].get(logical)
    return row


def new_record(rng: random.Random, source: str, base: dict | None = None) -> dict:
    fields = fresh_fields(rng, source)
    if base is not None:
        for k in ("ln", "fn", "bd", "em"):
            if k in fields and k in base:
                fields[k] = base[k]
    return {"f": fields, "rev": f"2024-{rng.randint(1, 12):02d}-{rng.randint(1, 28):02d}"}


def mutate(rng: random.Random, source: str, rec: dict, world: dict) -> None:
    fields = rec["f"]
    roll = rng.random()
    donors = [r["f"] for s in SOURCES for r in world[s].values() if r is not rec]
    if roll < 0.35 and donors:
        donor = rng.choice(donors)
        for k in ("ln", "fn", "bd", "em"):
            if k in fields and k in donor:
                fields[k] = donor[k]
    elif roll < 0.9:
        k = rng.choice(list(fields))
        fields[k] = fresh_fields(rng, source)[k]
    else:
        k = rng.choice([k for k in INVALID if k in fields])
        fields[k] = INVALID[k]
    rec["rev"] = f"2024-{rng.randint(1, 12):02d}-{rng.randint(1, 28):02d}"


def build_world(rng: random.Random) -> dict:
    world: dict[str, dict[str, dict]] = {s: {} for s in SOURCES}
    counter = 0
    for _ in range(24):
        base = fresh_fields(rng, "crm")
        for source in SOURCES:
            for _ in range(rng.choice([0, 1, 1, 1, 2])):
                counter += 1
                world[source][f"{source[0]}{counter}"] = new_record(rng, source, base)
    return world


def evolve(rng: random.Random, world: dict, graveyard: dict) -> dict[str, list[dict]]:
    """Apply a random delta to the world; return the batch of each source."""
    changed: dict[str, set[str]] = {s: set() for s in SOURCES}
    for source in SOURCES:
        recs = world[source]
        for key in list(recs):
            roll = rng.random()
            if roll < 0.12 and source != "web":
                graveyard[(source, key)] = recs.pop(key)
            elif roll < 0.42:
                mutate(rng, source, recs[key], world)
                changed[source].add(key)
        for (s, key), rec in list(graveyard.items()):
            if s == source and rng.random() < 0.3:
                recs[key] = graveyard.pop((s, key))
                changed[source].add(key)
        for _ in range(rng.randint(0, 5)):
            key = f"{source[0]}n{rng.randrange(10**6)}"
            recs[key] = new_record(rng, source)
            changed[source].add(key)
    batches = {}
    for source in SOURCES:
        keys = sorted(world[source]) if source != "web" else sorted(changed[source])
        batch = [to_row(source, k, world[source][k]) for k in keys if k in world[source]]
        if batch and rng.random() < 0.5:
            dup = dict(rng.choice(batch))
            dup["rev"] = "2023-01-01"
            dup[COLUMNS[source]["ln"]] = "stale"
            batch.insert(rng.randrange(len(batch) + 1), dup)
        batches[source] = batch
    return batches


@lru_cache(maxsize=None)
def simulate(seed: int, max_block: int) -> Counter:
    rng = random.Random(seed)
    config = make_config(matching__max_block_size=max_block)
    engine = ReferenceEngine(config)
    world = build_world(rng)
    graveyard: dict = {}
    stats: Counter = Counter()
    for round_no in range(4):
        if round_no == 0:
            batches = {s: [to_row(s, k, r) for k, r in sorted(world[s].items())] for s in SOURCES}
        else:
            batches = evolve(rng, world, graveyard)
        run_id = f"run{round_no}"
        ordered = list(SOURCES)
        rng.shuffle(ordered)
        for source in ordered:
            engine.ingest(source, batches[source], run_id)
            if rng.random() < 0.2:
                engine.reconcile(run_id)
        engine.reconcile(run_id)
        check_incremental(engine)

        replay = f"{run_id}b"
        for source in ordered:
            engine.ingest(source, batches[source], replay)
        engine.reconcile(replay)
        assert [d for d in rows(engine, "source_delta") if d["run_id"] == replay] == []

    for e in rows(engine, "staging_event"):
        stats[e["kind"]] += 1
        if e["kind"] == "merge":
            assert e["staging_id"] < e["related_id"]
        if e["kind"] == "split":
            assert e["staging_id"] < e["related_id"]
    stats.update(f"anomaly:{a['kind']}" for a in rows(engine, "anomaly"))
    stats.update(f"reject:{r['action']}" for r in rows(engine, "reject"))
    stats.update(f"reject:{r['rule']}" for r in rows(engine, "reject"))
    stats.update(f"golden:{g['status']}" for g in rows(engine, "golden"))
    stats.update(f"golden_stale:{g['stale']}" for g in rows(engine, "golden"))
    stats.update(f"delta:{d['kind']}" for d in rows(engine, "source_delta"))
    stats.update(f"pair:{p['decision']}" for p in rows(engine, "match_pair"))
    stats.update(f"review:{r['reason']}" for r in rows(engine, "review"))
    return stats


SEEDS = range(14)


@pytest.mark.parametrize("seed", SEEDS)
@pytest.mark.parametrize("max_block", [50, 3])
def test_incremental_equals_from_scratch(seed, max_block):
    simulate(seed, max_block)


def test_the_generated_deltas_exercise_every_rule():
    total: Counter = Counter()
    for seed in SEEDS:
        for max_block in (50, 3):
            total.update(simulate(seed, max_block))
    expected = [
        "merge",
        "split",
        "create",
        "anomaly:oversized_block",
        "anomaly:golden_blocked",
        "reject:reject_record",
        "reject:null_value",
        "reject:warn",
        "reject:duplicate_key",
        "reject:cast",
        "reject:max_length",
        "reject:min",
        "golden:published",
        "golden:blocked",
        "golden:retired",
        "golden_stale:True",
        "delta:I",
        "delta:U",
        "delta:D",
        "pair:match",
        "pair:review",
        "pair:none",
        "review:score_band",
        "review:contradiction",
        "review:multiple_candidates",
    ]
    missing = [name for name in expected if not total[name]]
    assert not missing, (missing, dict(total))
