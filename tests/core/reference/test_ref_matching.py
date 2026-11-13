import pytest
from ref_helpers import check_incremental, crm, erp, make_config, partition, rows, run, web

from engine.core.reference import ReferenceEngine


def pairs(engine, include_none=False):
    return {
        ((p["left_source_id"], p["left_key"]), (p["right_source_id"], p["right_key"])): p
        for p in rows(engine, "match_pair")
        if include_none or p["decision"] != "none"
    }


def decision(engine, left, right):
    return pairs(engine, True)[(left, right)]["decision"]


def test_identical_records_across_sources_match_and_merge():
    engine = ReferenceEngine(make_config())
    run(engine, "r1", crm=[crm(1, "dupont")], erp=[erp("A", "dupont")])
    (pair,) = pairs(engine).values()
    assert pair["score"] == pytest.approx(1.0) and pair["decision"] == "match"
    assert pair["rule_version"] == "t-1" and pair["contradiction"] is False
    assert partition(engine) == {frozenset({("crm", "1"), ("erp", "A")})}
    assert rows(engine, "review") == []


def test_pair_key_is_ordered_by_source_then_key_in_binary_order():
    engine = ReferenceEngine(make_config())
    run(engine, "r1", crm=[crm(2, "dupont"), crm(10, "dupont")], erp=[erp("A", "dupont")])
    for left, right in pairs(engine, True):
        assert left < right
    assert (("crm", "10"), ("crm", "2")) in pairs(engine, True)


def test_levenshtein_score_in_band_goes_to_review_without_merging():
    engine = ReferenceEngine(make_config())
    run(engine, "r1", crm=[crm(1, "dupont")], erp=[erp("A", "dupond")])
    expected = (0.4 * (1 - 1 / 6) + 0.3 + 0.2) / 0.9
    (review,) = rows(engine, "review")
    assert review["score"] == pytest.approx(expected) and review["reason"] == "score_band"
    assert len(partition(engine)) == 2


def test_low_score_is_stored_as_none():
    engine = ReferenceEngine(make_config())
    run(engine, "r1", crm=[crm(1, "dupont", fn="jean")], erp=[erp("A", "dupond", prenom="xavier")])
    (pair,) = pairs(engine, True).values()
    assert pair["decision"] == "none" and pair["score"] < 0.8
    assert rows(engine, "review") == []


def test_contradiction_blocks_the_merge_and_goes_to_review():
    config = make_config(matching__contradictions=[{"attr": "country"}])
    engine = ReferenceEngine(config)
    run(engine, "r1", crm=[crm(1, "dupont", ct="FR")], erp=[erp("A", "dupont", pays="BE")])
    (pair,) = pairs(engine).values()
    assert pair["score"] == pytest.approx(1.0) and pair["contradiction"] is True
    (review,) = rows(engine, "review")
    assert review["reason"] == "contradiction"
    assert len(partition(engine)) == 2


def test_default_contradiction_on_birth_date():
    engine = ReferenceEngine(make_config(matching__review=0.7))
    run(
        engine,
        "r1",
        crm=[crm(1, "dupont", bd="1980-05-01")],
        erp=[erp("A", "dupont", dn="1980-06-01")],
    )
    (pair,) = pairs(engine).values()
    assert pair["contradiction"] is True and pair["decision"] == "review"


def test_null_side_is_ignored_and_not_a_contradiction():
    engine = ReferenceEngine(make_config())
    run(engine, "r1", crm=[crm(1, "dupont", bd="1980-05-01")], web=[web("w", "dupont", mail=None)])
    # no common block (web has no birth_date, no email): nothing to compare
    assert pairs(engine, True) == {}
    run(engine, "r2", crm=[crm(1, "dupont", em="a@b.fr")], web=[web("w", "dupont", mail="a@b.fr")])
    (pair,) = pairs(engine).values()
    assert pair["score"] == pytest.approx(1.0) and pair["contradiction"] is False
    assert pair["decision"] == "match"


def test_min_evidence_boundary_and_below():
    # active weight: last_name 0.4 + email 0.1 = 0.5
    rows_crm = [crm(1, "dupont", fn=None, bd=None, em="a@b.fr")]
    rows_web = [web("w", "dupont", given=None, mail="a@b.fr")]
    engine = ReferenceEngine(make_config())
    run(engine, "r1", crm=rows_crm, web=rows_web)
    assert next(iter(pairs(engine).values()))["decision"] == "match"

    strict = ReferenceEngine(make_config(matching__min_evidence=0.6))
    run(strict, "r1", crm=rows_crm, web=rows_web)
    (pair,) = pairs(strict, True).values()
    assert pair["score"] is None and pair["decision"] == "none"
    assert len(partition(strict)) == 2


def test_blocking_keys_are_united():
    # crm/erp share ln+year only, crm/web share the email only
    engine = ReferenceEngine(make_config())
    run(
        engine,
        "r1",
        crm=[crm(1, "dupont", em="a@b.fr")],
        erp=[erp("A", "dupont")],
        web=[web("w", "dupont", mail="a@b.fr")],
    )
    assert set(pairs(engine, True)) == {
        (("crm", "1"), ("erp", "A")),
        (("crm", "1"), ("web", "w")),
    }
    assert partition(engine) == {frozenset({("crm", "1"), ("erp", "A"), ("web", "w")})}


def test_two_candidates_in_one_source_go_to_review():
    engine = ReferenceEngine(make_config())
    run(
        engine,
        "r1",
        crm=[crm(1, "dupont", ct="FR"), crm(2, "dupont", ct="FR")],
        erp=[erp("A", "dupont")],
    )
    assert decision(engine, ("crm", "1"), ("erp", "A")) == "review"
    assert decision(engine, ("crm", "2"), ("erp", "A")) == "review"
    assert {r["reason"] for r in rows(engine, "review")} == {"multiple_candidates"}
    # the crm pair itself is unambiguous: it merges
    assert decision(engine, ("crm", "1"), ("crm", "2")) == "match"
    assert partition(engine) == {frozenset({("crm", "1"), ("crm", "2")}), frozenset({("erp", "A")})}


def test_removing_the_competing_candidate_promotes_review_to_match():
    engine = ReferenceEngine(make_config())
    run(engine, "r1", erp=[erp("A", "dupont")], crm=[crm(1, "dupont")])
    assert decision(engine, ("crm", "1"), ("erp", "A")) == "match"
    # a second crm candidate appears: the untouched pair 1-A must switch to review
    run(engine, "r2", crm=[crm(1, "dupont"), crm(2, "dupont", bd="1980-05-01")])
    assert decision(engine, ("crm", "1"), ("erp", "A")) == "review"
    assert partition(engine) == {frozenset({("crm", "1"), ("crm", "2")}), frozenset({("erp", "A")})}
    check_incremental(engine)
    run(engine, "r3", crm=[crm(1, "dupont")])
    assert decision(engine, ("crm", "1"), ("erp", "A")) == "match"
    check_incremental(engine)


def test_date_year_comparator():
    config = make_config(
        matching__comparators=[
            {"attr": "last_name", "method": "levenshtein", "weight": 0.5},
            {"attr": "birth_date", "method": "date_year", "weight": 0.5},
        ],
        matching__contradictions=[],
    )
    engine = ReferenceEngine(config)
    run(
        engine,
        "r1",
        crm=[crm(1, "dupont", bd="1980-01-01")],
        erp=[erp("A", "dupont", dn="1980-12-31")],
    )
    (pair,) = pairs(engine).values()
    assert pair["score"] == pytest.approx(1.0) and pair["decision"] == "match"


def test_oversized_block_is_not_expanded_and_not_truncated():
    config = make_config(
        matching__max_block_size=3,
        matching__blocking=[
            {
                "name": "ln_year",
                "parts": [
                    {"attr": "last_name", "transform": "prefix", "n": 3},
                    {"attr": "birth_date", "transform": "year"},
                ],
            }
        ],
    )
    engine = ReferenceEngine(config)
    run(engine, "r1", crm=[crm(k, "dupont") for k in range(4)])
    assert pairs(engine, True) == {}
    assert len(partition(engine)) == 4
    (anomaly,) = rows(engine, "anomaly")
    assert (anomaly["kind"], anomaly["subject"]) == ("oversized_block", "ln_year=dup|1980")
    assert anomaly["detail"] == "size=4 max=3"


def test_block_at_the_limit_is_expanded_entirely():
    config = make_config(matching__max_block_size=3)
    engine = ReferenceEngine(config)
    run(engine, "r1", crm=[crm(k, "dupont") for k in range(3)])
    assert len(pairs(engine, True)) == 3
    assert rows(engine, "anomaly") == []


def test_block_growing_over_the_limit_drops_pairs_of_untouched_members():
    engine = ReferenceEngine(
        make_config(matching__max_block_size=3, sources__0__full_snapshot_min_ratio=0.3)
    )
    run(engine, "r1", crm=[crm(0, "dupont")], erp=[erp("A", "dupont")], web=[web("w", "other")])
    assert len(partition(engine)) == 2
    # two more members enter the block: size 4 > 3, existing pair 0-A must disappear
    run(engine, "r2", crm=[crm(0, "dupont"), crm(1, "dupont"), crm(2, "dupont")])
    assert pairs(engine, True) == {}
    assert len(partition(engine)) == 5
    check_incremental(engine)
    # and shrinking back below the limit expands it again
    run(engine, "r3", crm=[crm(0, "dupont")])
    assert decision(engine, ("crm", "0"), ("erp", "A")) == "match"
    check_incremental(engine)


def test_null_blocking_part_generates_no_block():
    engine = ReferenceEngine(make_config())
    run(engine, "r1", crm=[crm(1, "dupont", bd=None)], erp=[erp("A", "dupont", dn=None)])
    assert pairs(engine, True) == {}


def test_only_pairs_of_the_delta_are_rescored_and_unrelated_stagings_untouched():
    engine = ReferenceEngine(make_config())
    run(
        engine,
        "r1",
        crm=[crm(1, "dupont"), crm(2, "martin", bd="1970-01-01")],
        erp=[erp("A", "dupont"), erp("B", "martin", dn="1970-01-01")],
    )
    events_before = len(rows(engine, "staging_event"))
    deltas_before = len(rows(engine, "golden_delta"))
    batch = [crm(1, "dupont", em="x@y.fr"), crm(2, "martin", bd="1970-01-01")]
    engine.ingest("crm", batch, "r2")
    result = engine.reconcile("r2")
    assert result.delta_nodes == 1 and result.affected_nodes == 2
    assert len(rows(engine, "staging_event")) == events_before
    new = rows(engine, "golden_delta")[deltas_before:]
    assert [d["attribute"] for d in new] == ["email"]
    assert partition(engine) == {
        frozenset({("crm", "1"), ("erp", "A")}),
        frozenset({("crm", "2"), ("erp", "B")}),
    }
