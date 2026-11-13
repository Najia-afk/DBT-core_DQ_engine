from datetime import date
from decimal import Decimal

from ref_helpers import assert_conform, check_incremental, crm, erp, make_config, rows, run, web

from engine.core.reference import ReferenceEngine


def golden_of(engine, source_id, key):
    sid = next(
        m["staging_id"]
        for m in rows(engine, "staging_member")
        if (m["source_id"], m["source_key"]) == (source_id, key)
    )
    return next(g for g in rows(engine, "golden") if g["golden_id"] == sid)


def test_priority_follows_declared_source_order():
    engine = ReferenceEngine(make_config())
    run(engine, "r1", crm=[crm(1, "dupont", fn="jean")], erp=[erp("A", "dupont", prenom="johnny")])
    g = golden_of(engine, "crm", "1")
    assert (g["first_name"], g["first_name__src"]) == ("jean", "crm")
    assert (g["credit_limit"], g["credit_limit__src"]) == (None, None)


def test_priority_falls_through_null_values():
    engine = ReferenceEngine(make_config())
    run(
        engine,
        "r1",
        crm=[crm(1, "dupont", fn=None, ct=None)],
        erp=[erp("A", "dupont", prenom="johnny", pays="BE", plafond="3")],
    )
    g = golden_of(engine, "crm", "1")
    assert (g["first_name"], g["first_name__src"]) == ("johnny", "erp")
    assert (g["country"], g["country__src"]) == ("BE", "erp")
    assert (g["birth_date"], g["birth_date__src"]) == (date(1980, 5, 1), "crm")
    assert g["credit_limit"] == Decimal("3.0000")


def test_priority_inside_a_source_takes_greatest_revision_then_smallest_key():
    engine = ReferenceEngine(make_config())
    run(
        engine,
        "r1",
        crm=[
            crm(1, "dupont", ct="FR", rev="2024-01-01"),
            crm(2, "dupont", ct="BE", rev="2024-02-01"),
        ],
    )
    g = golden_of(engine, "crm", "1")
    assert len(rows(engine, "staging_member")) == 2
    assert g["country"] == "BE"

    tie = ReferenceEngine(make_config())
    run(
        tie,
        "r1",
        crm=[
            crm(2, "dupont", ct="BE", rev="2024-02-01"),
            crm(1, "dupont", ct="FR", rev="2024-02-01"),
        ],
    )
    assert golden_of(tie, "crm", "1")["country"] == "FR"


def email_cluster(crm2_rev, web_rev):
    """crm1~crm2 (same person) and web w linked to crm2 through the shared email b@x.fr."""
    engine = ReferenceEngine(make_config(matching__accept=0.9))
    run(
        engine,
        "r1",
        crm=[
            crm(1, "dupont", em="a@x.fr", rev="2024-01-01"),
            crm(2, "dupont", em="b@x.fr", rev=crm2_rev),
        ],
        web=[web("w", "dupont", mail="b@x.fr", rev=web_rev)],
    )
    assert len({m["staging_id"] for m in rows(engine, "staging_member")}) == 1
    return engine


def test_most_recent_uses_revision():
    engine = email_cluster("2024-06-01", "2024-09-01")
    g = golden_of(engine, "crm", "1")
    assert (g["email"], g["email__src"]) == ("b@x.fr", "web")
    older = email_cluster("2024-12-01", "2024-09-01")
    assert golden_of(older, "crm", "1")["email__src"] == "crm"
    # the older crm1 value never wins against newer ones
    assert golden_of(older, "crm", "1")["email"] == "b@x.fr"


def test_revision_alone_is_not_a_change_for_most_recent():
    engine = email_cluster("2024-06-01", "2024-09-01")
    run(
        engine,
        "r2",
        crm=[
            crm(1, "dupont", em="a@x.fr", rev="2024-01-01"),
            crm(2, "dupont", em="b@x.fr", rev="2025-01-01"),
        ],
    )
    assert golden_of(engine, "crm", "1")["email__src"] == "web"


def test_most_recent_tie_goes_to_smallest_source_then_key():
    engine = email_cluster("2024-06-01", "2024-06-01")
    assert golden_of(engine, "crm", "1")["email__src"] == "crm"


def test_most_recent_null_revision_is_oldest():
    engine = email_cluster("2020-01-01", None)
    assert golden_of(engine, "crm", "1")["email__src"] == "crm"


def test_golden_delta_records_old_new_winner_and_run():
    engine = ReferenceEngine(make_config())
    run(engine, "r1", crm=[crm(1, "dupont", fn="jean")])
    run(engine, "r2", crm=[crm(1, "dupont", fn="paul")])
    deltas = [d for d in rows(engine, "golden_delta") if d["attribute"] == "first_name"]
    assert [(d["run_id"], d["old_value"], d["new_value"], d["winning_source"]) for d in deltas] == [
        ("r1", None, "jean", "crm"),
        ("r2", "jean", "paul", "crm"),
    ]
    first = [d for d in rows(engine, "golden_delta") if d["run_id"] == "r1"]
    assert {d["attribute"] for d in first} == {"last_name", "first_name", "birth_date", "country"}
    birth = next(d for d in first if d["attribute"] == "birth_date")
    assert birth["new_value"] == "1980-05-01"


def test_unchanged_golden_is_not_rewritten_and_same_value_new_source_updates_provenance():
    engine = ReferenceEngine(make_config(sources__0__full_snapshot_min_ratio=0.5))
    run(
        engine,
        "r1",
        crm=[crm(1, "dupont"), crm(9, "zzz", bd="1999-01-01")],
        erp=[erp("A", "dupont")],
    )
    assert golden_of(engine, "crm", "1")["first_name__src"] == "crm"
    count = len(rows(engine, "golden_delta"))
    run(engine, "r2", crm=[crm(9, "zzz", bd="1999-01-01")])
    g = golden_of(engine, "erp", "A")
    assert (g["first_name"], g["first_name__src"]) == ("jean", "erp")
    assert len(rows(engine, "golden_delta")) == count
    check_incremental(engine)


def test_non_nullable_attribute_blocks_golden_and_keeps_the_old_version_stale():
    engine = ReferenceEngine(make_config())
    run(engine, "r1", crm=[crm(1, None, fn="jean")])
    g = golden_of(engine, "crm", "1")
    assert (g["status"], g["stale"], g["first_name"]) == ("blocked", False, None)
    assert rows(engine, "golden_delta") == []
    assert [(a["kind"], a["subject"]) for a in rows(engine, "anomaly")] == [
        ("golden_blocked", "last_name")
    ]

    run(engine, "r2", crm=[crm(1, "dupont", fn="jean")])
    g = golden_of(engine, "crm", "1")
    assert (g["status"], g["stale"], g["last_name"]) == ("published", False, "dupont")

    run(engine, "r3", crm=[crm(1, None, fn="paul")])
    g = golden_of(engine, "crm", "1")
    assert (g["status"], g["stale"]) == ("blocked", True)
    assert (g["last_name"], g["first_name"]) == ("dupont", "jean")
    assert not [d for d in rows(engine, "golden_delta") if d["run_id"] == "r3"]

    run(engine, "r4", crm=[crm(1, "martin", fn="paul")])
    g = golden_of(engine, "crm", "1")
    assert (g["status"], g["stale"], g["last_name"], g["first_name"]) == (
        "published",
        False,
        "martin",
        "paul",
    )
    r4 = {
        d["attribute"]: (d["old_value"], d["new_value"])
        for d in rows(engine, "golden_delta")
        if d["run_id"] == "r4"
    }
    assert r4 == {"last_name": ("dupont", "martin"), "first_name": ("jean", "paul")}
    check_incremental(engine)


def test_non_nullable_attribute_is_checked_on_the_merged_staging():
    engine = ReferenceEngine(make_config(matching__min_evidence=0.4))
    run(engine, "r1", crm=[crm(1, None, em="e@x.fr")], web=[web("w", "dupont", mail="e@x.fr")])
    g = golden_of(engine, "crm", "1")
    assert g["status"] == "published" and g["last_name"] == "dupont"
    assert g["last_name__src"] == "web"


def test_provenance_disabled_drops_the_src_columns():
    engine = ReferenceEngine(make_config(publication__provenance=False))
    run(engine, "r1", crm=[crm(1, "dupont")])
    names = rows(engine, "golden")[0].keys()
    assert not [n for n in names if n.endswith("__src")]
    assert_conform(engine)
