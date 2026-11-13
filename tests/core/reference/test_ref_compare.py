import time
from datetime import date
from decimal import Decimal

import pyarrow as pa
import pytest
from ref_helpers import crm, erp, make_config, run, web

from engine.core.canonical import REQUIRED_TABLES, canonical_schemas
from engine.core.compare import compare_exports
from engine.core.reference import ReferenceEngine


def scenario() -> dict[str, pa.Table]:
    """History with a merge, a split, a deletion, rejects, a blocked golden and a review."""
    engine = ReferenceEngine(make_config(sources__0__full_snapshot_min_ratio=0.3))
    run(
        engine,
        "r1",
        crm=[
            crm(1, "dupont"),
            crm(2, None, bd="1970-01-01"),
            crm(3, "petit", bd="1960-01-01"),
            crm(4, "moreau", bd="1950-02-02"),
        ],
        erp=[
            erp("A", "dupont", plafond="5"),
            erp("B", "durand", dn="1990-01-01", pays="ZZ"),
            erp("C", "x" * 20),
            erp("D", "dupons", prenom="xavier"),
            erp("F", "moreau", dn="1950-02-02"),
        ],
        web=[web("w", "dupont", mail="e@x.fr")],
    )
    run(
        engine,
        "r2",
        crm=[
            crm(1, "dupont", em="e@x.fr"),
            crm(2, None, bd="1970-01-01"),
            crm(3, "petit", bd="1960-01-01"),
            crm(4, "moreau", bd="1950-02-02"),
        ],
    )
    run(
        engine,
        "r3",
        crm=[
            crm(1, "dupont", em=None),
            crm(2, None, bd="1970-01-01"),
            crm(3, None, bd="1960-01-01"),
            crm(4, "moreau", bd="1950-02-02"),
        ],
        erp=[
            erp("A", "dupond", plafond="6"),
            erp("D", "dupons", prenom="xavier"),
            erp("F", "moreau", dn="1950-02-02"),
        ],
    )
    return engine.export()


def edit(tables, name, fn):
    out = dict(tables)
    table = tables[name]
    out[name] = pa.Table.from_pylist(fn(table.to_pylist()), schema=table.schema)
    return out


def remap_ids(tables, mapping):
    def ids(rows, *cols):
        for r in rows:
            for c in cols:
                if r[c] is not None:
                    r[c] = mapping(r[c])
        return rows

    out = edit(tables, "staging", lambda r: ids(r, "staging_id"))
    out = edit(out, "staging_member", lambda r: ids(r, "staging_id"))
    out = edit(out, "staging_event", lambda r: ids(r, "staging_id", "related_id"))
    out = edit(out, "golden", lambda r: ids(r, "golden_id"))
    return edit(out, "golden_delta", lambda r: ids(r, "golden_id"))


def categories(report):
    return set(report.totals)


def test_identical_exports_are_equivalent():
    a = scenario()
    report = compare_exports(a, scenario())
    assert report.ok, report.format()
    assert report.totals == {} and report.format() == "exports equivalent"
    assert report.compared["golden"] == (a["golden"].num_rows, a["golden"].num_rows)
    assert compare_exports(a, scenario(), include_none_pairs=True, compare_hash=True).ok


def test_scenario_really_has_history_to_compare():
    a = scenario()
    kinds = {e["kind"] for e in a["staging_event"].to_pylist()}
    assert kinds == {"create", "merge", "split"}
    statuses = {g["status"] for g in a["golden"].to_pylist()}
    assert statuses == {"published", "blocked", "retired"}
    assert a["reject"].num_rows and a["review"].num_rows
    assert {p["decision"] for p in a["match_pair"].to_pylist()} == {"match", "review", "none"}
    assert "retired" in {r["status"] for r in a["source_record"].to_pylist()}


def test_ids_are_not_compared():
    a = scenario()
    b = remap_ids(a, lambda i: 1000 - 7 * i)
    assert compare_exports(a, b).ok
    assert compare_exports(b, a).ok


def test_partition_difference_is_located():
    a = scenario()
    ids = {
        (m["source_id"], m["source_key"]): m["staging_id"] for m in a["staging_member"].to_pylist()
    }
    target = ids[("crm", "3")]

    def move(rows):
        for r in rows:
            if (r["source_id"], r["source_key"]) == ("erp", "D"):
                r["staging_id"] = target
        return rows

    report = compare_exports(a, edit(a, "staging_member", move), state_only=True)
    groups = [d for d in report.by_category("partition") if d.kind == "different_grouping"]
    assert groups
    assert any("crm:3" in d.key or "erp:B" in d.key for d in groups)


def test_node_missing_from_one_side_is_reported():
    a = scenario()
    b = edit(a, "staging_member", lambda rows: [r for r in rows if r["source_key"] != "w"])
    report = compare_exports(a, b, state_only=True)
    only_a = [d for d in report.by_category("partition") if d.kind == "node_only_in_a"]
    assert [d.key for d in only_a] == ["web:w"]
    assert "invariant" in categories(report)


def test_golden_value_status_and_provenance_differences():
    a = scenario()
    sid = next(
        m["staging_id"]
        for m in a["staging_member"].to_pylist()
        if (m["source_id"], m["source_key"]) == ("erp", "A")
    )

    def tweak(rows):
        for r in rows:
            if r["golden_id"] == sid:
                r["first_name"] = "zzz"
                r["country__src"] = "web"
                r["credit_limit"] = Decimal("99")
        return rows

    report = compare_exports(a, edit(a, "golden", tweak), state_only=True)
    kinds = {d.kind for d in report.by_category("golden")}
    assert kinds == {"different:first_name", "different:country__src", "different:credit_limit"}
    detail = next(d for d in report.divergences if d.kind == "different:first_name")
    assert "a='jean'" in detail.detail and "b='zzz'" in detail.detail

    def unpublish(rows):
        for r in rows:
            if r["golden_id"] == sid:
                r["status"] = "blocked"
        return rows

    report = compare_exports(a, edit(a, "golden", unpublish), state_only=True)
    assert [d.kind for d in report.by_category("golden")] == ["different:status"]


def test_blocked_golden_is_compared_by_status_only():
    a = scenario()
    blocked = [g for g in a["golden"].to_pylist() if g["status"] == "blocked"]
    assert blocked

    def tweak(rows):
        for r in rows:
            if r["status"] == "blocked":
                r["first_name"], r["stale"] = "whatever", not r["stale"]
        return rows

    assert compare_exports(a, edit(a, "golden", tweak), state_only=True).ok


def test_retired_goldens_are_ignored_in_state_but_counted_in_history():
    a = scenario()
    b = edit(a, "golden", lambda rows: [r for r in rows if r["status"] != "retired"])
    assert compare_exports(a, b, state_only=True).ok
    report = compare_exports(a, b)
    assert [d.kind for d in report.by_category("golden")] == ["retired_count"]


def test_pair_scores_use_a_tolerance_and_review_is_compared():
    a = scenario()
    assert a["match_pair"].num_rows

    def shift(delta):
        def apply(rows):
            for r in rows:
                if r["score"] is not None:
                    r["score"] += delta
            return rows

        return apply

    close = edit(edit(a, "match_pair", shift(1e-10)), "review", shift(1e-10))
    assert compare_exports(a, close, include_none_pairs=True).ok
    far = edit(edit(a, "match_pair", shift(1e-6)), "review", shift(1e-6))
    report = compare_exports(a, far, include_none_pairs=True)
    assert {"match_pair", "review"} <= categories(report)
    assert compare_exports(a, far, score_tol=1e-3, include_none_pairs=True).ok


def test_missing_review_pair_and_decision_change():
    a = scenario()
    b = edit(a, "review", lambda rows: rows[1:])
    report = compare_exports(a, b)
    assert [d.kind for d in report.by_category("review")] == ["only_in_a"]

    def flip(rows):
        rows[0]["decision"] = "review" if rows[0]["decision"] != "review" else "match"
        return rows

    report = compare_exports(a, edit(a, "match_pair", flip), include_none_pairs=True)
    assert [d.kind for d in report.by_category("match_pair")] == ["different:decision"]


def test_none_pairs_are_skipped_unless_requested():
    a = scenario()
    b = edit(a, "match_pair", lambda rows: [r for r in rows if r["decision"] != "none"])
    assert compare_exports(a, b).ok
    assert not compare_exports(a, b, include_none_pairs=True).ok


def test_reject_multiset_difference_counts_rows():
    a = scenario()
    assert a["reject"].num_rows >= 2
    b = edit(a, "reject", lambda rows: rows[1:] + [dict(rows[0], rule="other")])
    report = compare_exports(a, b)
    kinds = sorted(d.kind for d in report.by_category("reject"))
    assert kinds == ["only_in_a", "only_in_b"]
    assert report.totals["reject"] == 2
    assert compare_exports(a, b, state_only=True).ok


def test_duplicate_reject_rows_are_counted_not_collapsed():
    a = scenario()
    b = edit(a, "reject", lambda rows: rows + [dict(rows[0])])
    report = compare_exports(a, b)
    (div,) = report.by_category("reject")
    assert div.kind == "only_in_b" and div.detail.startswith("1 row")


def test_golden_delta_multiset_modulo_ids_and_content():
    a = scenario()
    assert compare_exports(a, remap_ids(a, lambda i: i + 40)).ok
    b = edit(a, "golden_delta", lambda rows: rows[:-1] + [dict(rows[-1], new_value="changed")])
    report = compare_exports(a, b)
    assert {d.kind for d in report.by_category("golden_delta")} == {"only_in_a", "only_in_b"}
    c = edit(
        a, "golden_delta", lambda rows: [dict(r, run_id="rX") if r is rows[0] else r for r in rows]
    )
    assert not compare_exports(a, c).ok


def test_golden_delta_follows_merge_redirects():
    a = scenario()
    merges = [e for e in a["staging_event"].to_pylist() if e["kind"] == "merge"]
    assert merges
    absorbed = merges[0]["related_id"]
    deltas = [d for d in a["golden_delta"].to_pylist() if d["golden_id"] == absorbed]
    assert deltas
    # the same history attributed to the surviving id instead of the absorbed one is equivalent
    b = edit(
        a,
        "golden_delta",
        lambda rows: [
            dict(r, golden_id=merges[0]["staging_id"]) if r["golden_id"] == absorbed else r
            for r in rows
        ],
    )
    assert compare_exports(a, b).ok


def test_source_record_and_source_delta_differences():
    a = scenario()

    def tweak(rows):
        rows[0]["last_name"] = "changed"
        rows[1]["revision"] = None
        return rows

    report = compare_exports(a, edit(a, "source_record", tweak), state_only=True)
    assert {d.kind for d in report.by_category("source_record")} >= {"different:last_name"}
    c = edit(a, "source_delta", lambda rows: rows[:-1])
    report = compare_exports(a, c)
    assert [d.kind for d in report.by_category("source_delta")] == ["only_in_a"]
    assert compare_exports(a, c, state_only=True).ok
    hashed = edit(a, "source_delta", lambda rows: [dict(r, new_hash="x" * 64) for r in rows])
    assert compare_exports(a, hashed).ok
    assert not compare_exports(a, hashed, compare_hash=True).ok


def test_retired_source_records_only_matter_in_history():
    a = scenario()
    b = edit(a, "source_record", lambda rows: [r for r in rows if r["status"] == "active"])
    assert compare_exports(a, b, state_only=True).ok
    assert not compare_exports(a, b).ok


def test_schema_problems_are_reported():
    a = scenario()
    b = dict(a)
    del b["reject"]
    b["golden"] = a["golden"].drop_columns(["email"])
    b["review"] = a["review"].set_column(
        a["review"].schema.get_field_index("score"),
        "score",
        pa.array([str(s) for s in a["review"]["score"].to_pylist()], pa.string()),
    )
    report = compare_exports(a, b)
    kinds = {(d.kind, d.key) for d in report.by_category("schema")}
    assert ("missing_table", "reject") in kinds
    assert ("missing_column", "golden.email") in kinds
    assert ("type", "review.score") in kinds
    optional_only = {k: v for k, v in a.items() if k in REQUIRED_TABLES}
    assert compare_exports(a, optional_only).ok


def test_large_string_columns_are_not_a_type_difference():
    a = scenario()
    b = dict(a)
    b["reject"] = pa.Table.from_pylist(
        a["reject"].to_pylist(),
        schema=pa.schema([pa.field(f.name, pa.large_string()) for f in a["reject"].schema]),
    )
    assert compare_exports(a, b).ok


def test_detail_cap_does_not_cap_totals():
    a = scenario()
    b = edit(a, "source_record", lambda rows: [dict(r, last_name="zz") for r in rows])
    report = compare_exports(a, b, state_only=True, max_details=1)
    assert len([d for d in report.by_category("source_record")]) == 1
    assert report.totals["source_record"] > 1
    with pytest.raises(AssertionError, match="source_record"):
        report.assert_equal()


def test_node_in_two_stagings_is_an_invariant_violation():
    a = scenario()
    first = a["staging_member"].to_pylist()[0]
    b = edit(
        a, "staging_member", lambda rows: rows + [dict(first, staging_id=first["staging_id"] + 99)]
    )
    report = compare_exports(a, b, state_only=True)
    assert any(d.kind == "b:node_in_several_stagings" for d in report.by_category("invariant"))


# ----------------------------------------------------------------------------- scale


def synthetic(n: int, *, reverse_ids: bool = False, change: int = 0) -> dict[str, pa.Table]:
    """n source nodes in n/2 stagings; `change` goldens get another first_name."""
    config = make_config()
    schemas = canonical_schemas(config)
    st = n // 2
    sids = [i // 2 + 1 for i in range(n)]
    gid = [(st + 1 - s) if reverse_ids else s for s in sids]
    keys = [str(i) for i in range(n)]
    gids = [(st + 1 - s) if reverse_ids else s for s in range(1, st + 1)]
    names = [f"name{i % 997}" for i in range(st)]
    first = [f"fn{i % 31}" if i >= change else "changed" for i in range(st)]
    revs = [None] * n

    def table(name, columns):
        return pa.Table.from_pydict(columns, schema=schemas[name])

    record_cols = {
        "source_id": ["crm"] * n,
        "source_key": keys,
        "row_hash": ["h"] * n,
        "revision": revs,
        "status": ["active"] * n,
        "first_run": ["r1"] * n,
        "last_change_run": ["r1"] * n,
        "last_name": [names[s - 1] for s in sids],
        "first_name": [first[s - 1] for s in sids],
        "birth_date": [date(1980, 1, 1)] * n,
        "email": [None] * n,
        "country": ["FR"] * n,
        "credit_limit": [Decimal("1.5")] * n,
    }
    golden_cols = {
        "golden_id": gids,
        "status": ["published"] * st,
        "stale": [False] * st,
        "last_name": names,
        "last_name__src": ["crm"] * st,
        "first_name": first,
        "first_name__src": ["crm"] * st,
        "birth_date": [date(1980, 1, 1)] * st,
        "birth_date__src": ["crm"] * st,
        "email": [None] * st,
        "email__src": [None] * st,
        "country": ["FR"] * st,
        "country__src": ["crm"] * st,
        "credit_limit": [Decimal("1.5")] * st,
        "credit_limit__src": ["crm"] * st,
    }
    pair_cols = {
        "left_source_id": ["crm"] * st,
        "left_key": keys[0::2],
        "right_source_id": ["crm"] * st,
        "right_key": keys[1::2],
    }
    tables = {
        "source_record": table("source_record", record_cols),
        "source_delta": table(
            "source_delta",
            {
                "run_id": ["r1"] * n,
                "source_id": ["crm"] * n,
                "source_key": keys,
                "kind": ["I"] * n,
                "old_hash": [None] * n,
                "new_hash": ["h"] * n,
            },
        ),
        "match_pair": table(
            "match_pair",
            {
                **pair_cols,
                "score": [1.0] * st,
                "decision": ["match"] * st,
                "contradiction": [False] * st,
                "rule_version": ["v"] * st,
            },
        ),
        "staging_member": table(
            "staging_member", {"staging_id": gid, "source_id": ["crm"] * n, "source_key": keys}
        ),
        "staging_event": table(
            "staging_event",
            {
                "run_id": ["r1"] * st,
                "kind": ["create"] * st,
                "staging_id": gids,
                "related_id": [None] * st,
            },
        ),
        "golden": table("golden", golden_cols),
        "golden_delta": table(
            "golden_delta",
            {
                "run_id": ["r1"] * st,
                "golden_id": gids,
                "attribute": ["last_name"] * st,
                "old_value": [None] * st,
                "new_value": names,
                "winning_source": ["crm"] * st,
            },
        ),
        "review": table(
            "review",
            {
                "run_id": ["r1"] * 10,
                "left_source_id": ["crm"] * 10,
                "left_key": keys[:10],
                "right_source_id": ["erp"] * 10,
                "right_key": keys[:10],
                "score": [0.9] * 10,
                "reason": ["score_band"] * 10,
            },
        ),
        "reject": table(
            "reject",
            {
                "run_id": ["r1"],
                "source_id": ["crm"],
                "source_key": ["x"],
                "attribute": [""],
                "rule": ["duplicate_key"],
                "action": ["reject_record"],
            },
        ),
    }
    assert set(tables) == set(REQUIRED_TABLES)
    return tables


def test_compare_scales_to_200k_nodes_and_finds_the_needle():
    n = 200_000
    a = synthetic(n)
    b = synthetic(n, reverse_ids=True, change=3)
    start = time.perf_counter()
    report = compare_exports(a, b)
    elapsed = time.perf_counter() - start
    assert report.totals == {"golden": 3, "source_record": 6}
    assert {d.kind for d in report.divergences} == {"different:first_name"}
    assert elapsed < 60, elapsed
    same = compare_exports(a, synthetic(n, reverse_ids=True))
    assert same.ok, same.format()
