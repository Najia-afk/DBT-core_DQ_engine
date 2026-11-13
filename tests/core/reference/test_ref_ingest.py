from datetime import date, datetime
from decimal import Decimal

import pytest
from ref_helpers import assert_conform, crm, erp, make_config, rows, run, web

from engine.core.reference import ReferenceEngine


def deltas(engine, run_id=None):
    return [
        (d["run_id"], d["source_id"], d["source_key"], d["kind"])
        for d in rows(engine, "source_delta")
        if run_id in (None, d["run_id"])
    ]


def records(engine):
    return {(r["source_id"], r["source_key"]): r for r in rows(engine, "source_record")}


def test_insert_normalises_and_casts():
    engine = ReferenceEngine(make_config())
    run(engine, "r1", crm=[crm(7, "  DÉPONT   Jr ", fn=" Éloïse ", em=" A@B.fr ", ct=" fr ")])
    rec = records(engine)[("crm", "7")]
    assert rec["last_name"] == "depont jr"
    assert rec["first_name"] == "eloise"
    assert rec["email"] == "a@b.fr"
    assert rec["country"] == "FR"
    assert rec["birth_date"] == date(1980, 5, 1)
    assert rec["revision"] == datetime(2024, 1, 1)
    assert rec["status"] == "active" and rec["first_run"] == "r1"
    assert rec["credit_limit"] is None
    assert_conform(engine)


def test_erp_decimal_is_quantised():
    engine = ReferenceEngine(make_config())
    run(engine, "r1", erp=[erp("A", "martin", plafond="12.34567")])
    assert records(engine)[("erp", "A")]["credit_limit"] == Decimal("12.3457")


def test_replaying_the_same_batch_creates_no_delta():
    engine = ReferenceEngine(make_config())
    batch = [crm(1, "dupont"), crm(2, "martin")]
    run(engine, "r1", crm=batch)
    before = {k: engine.export()[k] for k in ("source_record", "golden", "staging_member")}
    result = engine.ingest("crm", batch, "r2")
    engine.reconcile("r2")
    assert (result.inserted, result.updated, result.deleted, result.unchanged) == (0, 0, 0, 2)
    assert deltas(engine, "r2") == []
    for name, table in before.items():
        assert engine.export()[name].equals(table)
    assert rows(engine, "staging_event") == [
        e for e in rows(engine, "staging_event") if e["run_id"] == "r1"
    ]


def test_update_emits_u_with_old_and_new_hash():
    engine = ReferenceEngine(make_config())
    run(engine, "r1", crm=[crm(1, "dupont")])
    run(engine, "r2", crm=[crm(1, "dupont", fn="paul")])
    first, second = rows(engine, "source_delta")
    assert (first["kind"], first["old_hash"]) == ("I", None)
    assert second["kind"] == "U" and second["old_hash"] == first["new_hash"]
    assert second["new_hash"] != first["new_hash"]
    assert records(engine)[("crm", "1")]["last_change_run"] == "r2"


def test_revision_alone_is_not_a_change():
    engine = ReferenceEngine(make_config())
    run(engine, "r1", crm=[crm(1, "dupont", rev="2024-01-01")])
    run(engine, "r2", crm=[crm(1, "dupont", rev="2024-06-01")])
    assert deltas(engine, "r2") == []
    assert records(engine)[("crm", "1")]["revision"] == datetime(2024, 1, 1)


def test_full_snapshot_missing_key_is_logically_deleted_then_reactivated():
    engine = ReferenceEngine(make_config())
    run(engine, "r1", crm=[crm(1, "dupont"), crm(2, "martin")])
    run(engine, "r2", crm=[crm(1, "dupont")])
    assert deltas(engine, "r2") == [("r2", "crm", "2", "D")]
    gone = records(engine)[("crm", "2")]
    assert gone["status"] == "retired" and gone["last_change_run"] == "r2"
    d = rows(engine, "source_delta")[-1]
    assert d["old_hash"] == gone["row_hash"] and d["new_hash"] is None
    run(engine, "r3", crm=[crm(1, "dupont"), crm(2, "martin")])
    assert deltas(engine, "r3") == [("r3", "crm", "2", "I")]
    assert records(engine)[("crm", "2")]["status"] == "active"
    assert records(engine)[("crm", "2")]["first_run"] == "r1"


def test_delta_snapshot_never_deletes():
    engine = ReferenceEngine(make_config())
    run(engine, "r1", web=[web("a", "dupont"), web("b", "martin")])
    run(engine, "r2", web=[web("a", "dupont")])
    assert deltas(engine, "r2") == []


def test_failed_completeness_check_blocks_the_whole_batch():
    engine = ReferenceEngine(make_config())
    run(engine, "r1", crm=[crm(k, f"name{k}") for k in range(10)])
    result = engine.ingest("crm", [crm(0, "changed"), crm(100, "new")], "r2")
    engine.reconcile("r2")
    assert result.blocked and result.deleted == 0 and result.inserted == 0
    assert deltas(engine, "r2") == []
    assert all(r["status"] == "active" for r in records(engine).values())
    assert records(engine)[("crm", "0")]["last_name"] == "name0"
    assert [(a["kind"], a["subject"]) for a in rows(engine, "anomaly")] == [
        ("batch_blocked", "crm")
    ]
    # a complete batch right after is accepted (the ratio is on the active keys)
    batch = [crm(k, f"name{k}") for k in range(9)]
    assert not engine.ingest("crm", batch, "r3").blocked


def test_completeness_ratio_counts_rejected_keys_as_present():
    engine = ReferenceEngine(make_config(sources__0__full_snapshot_min_ratio=1.0))
    run(engine, "r1", crm=[crm(1, "dupont"), crm(2, "martin")])
    result = engine.ingest("crm", [crm(1, "dupont"), crm(2, "x" * 50)], "r2")
    assert not result.blocked and result.rejected == 1 and result.deleted == 0


def test_duplicate_key_keeps_greatest_revision_then_last_row():
    engine = ReferenceEngine(make_config())
    batch = [
        crm(1, "old", rev="2024-03-01"),
        crm(1, "newest", rev="2024-05-01"),
        crm(1, "stale", rev="2024-04-01"),
        crm(2, "first", rev="2024-01-01"),
        crm(2, "last", rev="2024-01-01"),
    ]
    engine.ingest("crm", batch, "r1")
    recs = records(engine)
    assert recs[("crm", "1")]["last_name"] == "newest"
    assert recs[("crm", "2")]["last_name"] == "last"
    dup = [r for r in rows(engine, "reject") if r["rule"] == "duplicate_key"]
    assert sorted(r["source_key"] for r in dup) == ["1", "1", "2"]
    assert all(r["action"] == "reject_record" and r["attribute"] == "" for r in dup)


def test_reject_record_keeps_last_valid_version_and_emits_no_delta():
    engine = ReferenceEngine(make_config())
    run(engine, "r1", erp=[erp("A", "martin", plafond="10")])
    result = engine.ingest("erp", [erp("A", "martin", plafond="-5")], "r2")
    engine.reconcile("r2")
    assert result.rejected == 1 and deltas(engine, "r2") == []
    rec = records(engine)[("erp", "A")]
    assert rec["status"] == "active" and rec["credit_limit"] == Decimal("10")
    assert [(r["attribute"], r["rule"], r["action"]) for r in rows(engine, "reject")] == [
        ("credit_limit", "min", "reject_record")
    ]
    assert rows(engine, "staging_event") == [
        e for e in rows(engine, "staging_event") if e["run_id"] == "r1"
    ]


def test_rejected_new_key_creates_nothing():
    engine = ReferenceEngine(make_config())
    run(engine, "r1", crm=[crm(1, "a-name-longer-than-twelve")])
    assert rows(engine, "source_record") == [] and rows(engine, "source_delta") == []
    assert rows(engine, "golden") == []


def test_null_value_nulls_the_attribute_and_logs():
    engine = ReferenceEngine(make_config())
    run(engine, "r1", crm=[crm(1, "dupont", em="not-an-email")])
    assert records(engine)[("crm", "1")]["email"] is None
    (r,) = rows(engine, "reject")
    assert (r["attribute"], r["rule"], r["action"]) == ("email", "regex", "null_value")


def test_warn_keeps_the_value_and_logs():
    engine = ReferenceEngine(make_config())
    run(engine, "r1", crm=[crm(1, "dupont", ct="zz")])
    assert records(engine)[("crm", "1")]["country"] == "ZZ"
    (r,) = rows(engine, "reject")
    assert (r["attribute"], r["rule"], r["action"]) == ("country", "in_set", "warn")


def test_all_failures_are_logged_and_one_reject_record_rejects():
    engine = ReferenceEngine(make_config())
    result = engine.ingest(
        "erp", [erp("A", "x" * 20, plafond="-1", pays="zz", dn="2020-01-01")], "r1"
    )
    assert result.rejected == 1
    logged = {(r["attribute"], r["rule"], r["action"]) for r in rows(engine, "reject")}
    assert logged == {
        ("last_name", "max_length", "reject_record"),
        ("credit_limit", "min", "reject_record"),
        ("country", "in_set", "warn"),
    }


def test_cast_error_rejects_the_record():
    engine = ReferenceEngine(make_config())
    engine.ingest("crm", [crm(1, "dupont", bd="not-a-date")], "r1")
    assert [(r["attribute"], r["rule"]) for r in rows(engine, "reject")] == [("birth_date", "cast")]
    assert rows(engine, "source_record") == []


def test_null_key_and_bad_revision_are_rejected():
    engine = ReferenceEngine(make_config())
    engine.ingest("crm", [crm(None, "a"), crm(2, "b", rev="whenever"), crm(3, "c", rev=None)], "r1")
    assert sorted((r["source_key"], r["rule"]) for r in rows(engine, "reject")) == [
        ("", "key_null"),
        ("2", "revision"),
    ]
    assert records(engine)[("crm", "3")]["revision"] is None


def test_empty_after_normalisation_is_null():
    engine = ReferenceEngine(make_config())
    run(engine, "r1", crm=[crm(1, "dupont", fn="   ")])
    assert records(engine)[("crm", "1")]["first_name"] is None


def test_unknown_source_and_bad_run_id():
    engine = ReferenceEngine(make_config())
    with pytest.raises(ValueError):
        engine.ingest("nope", [], "r1")
    with pytest.raises(ValueError):
        engine.ingest("crm", [], "bad run id")
