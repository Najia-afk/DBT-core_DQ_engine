from ref_helpers import (
    check_incremental,
    crm,
    erp,
    golden_by_member,
    make_config,
    partition,
    rows,
    run,
    staging_id,
    web,
)

from engine.core.reference import ReferenceEngine


def events(engine, run_id):
    return [
        (e["kind"], e["staging_id"], e["related_id"])
        for e in rows(engine, "staging_event")
        if e["run_id"] == run_id
    ]


def statuses(engine):
    return {s["staging_id"]: s["status"] for s in rows(engine, "staging")}


def test_new_nodes_create_stagings_with_increasing_ids():
    engine = ReferenceEngine(make_config())
    run(engine, "r1", crm=[crm(1, "dupont"), crm(2, "martin", bd="1970-01-01")])
    assert events(engine, "r1") == [("create", 1, None), ("create", 2, None)]
    assert [(s["staging_id"], s["created_run"]) for s in rows(engine, "staging")] == [
        (1, "r1"),
        (2, "r1"),
    ]


def test_merge_keeps_the_oldest_id_and_redirects_the_other():
    engine = ReferenceEngine(make_config())
    run(engine, "r1", web=[web("w", "dupont", mail="e@x.fr")])
    run(engine, "r2", crm=[crm(1, "dupont")], erp=[erp("A", "dupont", plafond="5")])
    assert staging_id(engine, "web", "w") == 1 and staging_id(engine, "crm", "1") == 2
    run(engine, "r3", crm=[crm(1, "dupont", em="e@x.fr")])
    assert events(engine, "r3") == [("merge", 1, 2)]
    assert partition(engine) == {frozenset({("web", "w"), ("crm", "1"), ("erp", "A")})}
    assert staging_id(engine, "erp", "A") == 1
    assert statuses(engine) == {1: "active", 2: "retired"}
    golden = {g["golden_id"]: g for g in rows(engine, "golden")}
    assert golden[2]["status"] == "retired" and golden[1]["status"] == "published"
    assert golden[1]["credit_limit"] is not None
    new = [d for d in rows(engine, "golden_delta") if d["run_id"] == "r3"]
    assert {d["golden_id"] for d in new} == {1}
    assert {d["attribute"] for d in new} >= {"credit_limit"}
    check_incremental(engine)


def test_split_larger_part_keeps_the_id_and_the_other_gets_a_new_one():
    engine = ReferenceEngine(make_config())
    run(
        engine,
        "r1",
        crm=[crm(1, "dupont", em="e@x.fr")],
        erp=[erp("A", "dupont")],
        web=[web("w", "dupont", mail="e@x.fr")],
    )
    assert len(partition(engine)) == 1 and staging_id(engine, "web", "w") == 1
    run(engine, "r2", crm=[crm(1, "dupont", em=None)])
    assert partition(engine) == {
        frozenset({("crm", "1"), ("erp", "A")}),
        frozenset({("web", "w")}),
    }
    assert events(engine, "r2") == [("split", 1, 2)]
    assert staging_id(engine, "crm", "1") == 1 and staging_id(engine, "web", "w") == 2
    assert statuses(engine) == {1: "active", 2: "active"}
    check_incremental(engine)


def test_split_tie_is_resolved_by_the_smallest_node():
    engine = ReferenceEngine(make_config())
    run(engine, "r1", crm=[crm(1, "dupont")], erp=[erp("A", "dupont")])
    run(engine, "r2", erp=[erp("A", "zzzzzz")])
    assert events(engine, "r2") == [("split", 1, 2)]
    assert staging_id(engine, "crm", "1") == 1 and staging_id(engine, "erp", "A") == 2


def test_unrelated_change_keeps_ids_and_writes_no_staging_event():
    engine = ReferenceEngine(make_config())
    run(engine, "r1", crm=[crm(1, "dupont")], erp=[erp("A", "dupont")])
    ids = golden_by_member(engine)
    run(engine, "r2", crm=[crm(1, "dupont", em="a@b.fr")])
    assert events(engine, "r2") == []
    assert {k: v["golden_id"] for k, v in golden_by_member(engine).items()} == {
        k: v["golden_id"] for k, v in ids.items()
    }


def test_retired_when_last_member_deleted_and_ids_are_not_reused():
    engine = ReferenceEngine(make_config(sources__0__full_snapshot_min_ratio=0.5))
    run(engine, "r1", crm=[crm(1, "dupont"), crm(2, "martin", bd="1970-01-01")])
    run(engine, "r2", crm=[crm(2, "martin", bd="1970-01-01")])
    assert statuses(engine) == {1: "retired", 2: "active"}
    assert {g["golden_id"]: g["status"] for g in rows(engine, "golden")} == {
        1: "retired",
        2: "published",
    }
    assert [m["staging_id"] for m in rows(engine, "staging_member")] == [2]
    run(engine, "r3", crm=[crm(1, "dupont"), crm(2, "martin", bd="1970-01-01")])
    assert staging_id(engine, "crm", "1") == 3
    assert events(engine, "r3") == [("create", 3, None)]
    check_incremental(engine)


def test_rejected_record_triggers_no_reconciliation():
    engine = ReferenceEngine(make_config())
    run(engine, "r1", crm=[crm(1, "dupont")], erp=[erp("A", "dupont")])
    engine.ingest("erp", [erp("A", "dupont", plafond="-1")], "r2")
    result = engine.reconcile("r2")
    assert result.delta_nodes == 0 and events(engine, "r2") == []


def test_reconcile_without_pending_delta_is_a_noop():
    engine = ReferenceEngine(make_config())
    run(engine, "r1", crm=[crm(1, "dupont")])
    before = engine.export()
    engine.reconcile("r2")
    assert all(engine.export()[k].equals(before[k]) for k in before)


def test_several_ingests_before_one_reconcile_use_the_net_state():
    engine = ReferenceEngine(make_config())
    engine.ingest("crm", [crm(1, "dupont")], "r1")
    engine.ingest("erp", [erp("A", "dupont")], "r1")
    engine.ingest("web", [web("w", "dupont", mail="e@x.fr")], "r1")
    engine.ingest("crm", [crm(1, "dupont", em="e@x.fr")], "r1")
    engine.reconcile("r1")
    assert len(partition(engine)) == 1
    check_incremental(engine)


def test_review_pairs_never_merge_and_keep_their_run():
    engine = ReferenceEngine(make_config())
    run(engine, "r1", crm=[crm(1, "dupont")], erp=[erp("A", "dupond")])
    run(engine, "r2", crm=[crm(1, "dupont", em="a@b.fr")])
    (review,) = rows(engine, "review")
    assert review["run_id"] == "r1" and len(partition(engine)) == 2
    run(engine, "r3", erp=[erp("A", "duponts")])
    (review,) = rows(engine, "review")
    assert review["run_id"] == "r3"
