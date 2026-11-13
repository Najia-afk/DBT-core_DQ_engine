"""Hand-written scenarios: DQ rules, odd strings, completeness, oversized blocks, blocked golden."""

from __future__ import annotations

import copy
from datetime import date, datetime
from decimal import Decimal
from typing import Any

import pytest
from graph_data_base import BASE, crm, erp, web
from graph_support import Harness

from engine.core.canonical import check_tables
from engine.core.compare import compare_exports
from engine.core.reference import ReferenceEngine


class Run:
    def __init__(self, server, tmp, name: str, **matching: Any) -> None:
        raw = copy.deepcopy(BASE)
        raw["state"] = {"dsn_env": "X", "schema": f"mdm_{name}"}
        raw["matching"].update(matching)
        self.h = Harness(server, raw, tmp, name=name)
        self.h.init()
        self.ref = ReferenceEngine(self.h.cfg)
        self.mode = "hop"
        self.results: list[dict] = []

    def step(self, run: str, **batches: tuple[list[dict], bool]) -> dict:
        """Load, ingest into both engines, reconcile both, assert full equivalence."""
        for sid, (rows, replace) in batches.items():
            self.h.load_source(sid, rows, replace)
        counts = {}
        for src in self.h.cfg.sources:
            if src.id in batches:
                res = self.h.ingest(src.id, run)
                counts[src.id] = res["counts"]
                ref_res = self.ref.ingest(src.id, self.h.rows[src.id], run)
                assert res["counts"]["inserted"] == ref_res.inserted
                assert res["counts"]["updated"] == ref_res.updated
                assert res["counts"]["deleted"] == ref_res.deleted
                assert res["counts"]["unchanged"] == ref_res.unchanged
                assert res["counts"]["blocked"] == ref_res.blocked
        rec = self.h.reconcile(run, "--components", self.mode)
        self.ref.reconcile(run)
        tables = self.h.export_tables(f"export_{run}")
        assert check_tables(self.h.cfg, tables) == []
        report = compare_exports(tables, self.ref.export(), include_none_pairs=True)
        assert report.ok, f"{run}: {report.format()}"
        out = {"ingest": counts, "reconcile": rec["counts"], "tables": tables}
        self.results.append(out)
        return out


@pytest.fixture(params=["hop", "varlen"])
def mode(request) -> str:
    return request.param


def test_dq_dedup_revisions_reactivation_and_blocked_batches(age_server, tmp_path, mode) -> None:
    r = Run(age_server, tmp_path, f"edge_{mode}", max_block_size=50)
    r.mode = mode
    r.step(
        "r1",
        crm=(
            [
                crm(1, "Dupont", em="a@x.fr"),
                crm(2, "Martin", bd="1970-01-01"),
                crm(3, "Durand"),
                crm(4, "Dupond"),
                crm(5, "x" * 20),
                crm(6, "Zed", em="not-an-email"),
            ],
            True,
        ),
        erp=(
            [
                erp(10, "dupont"),
                erp(11, "martin", dn="1971-01-01"),
                erp(12, "Durand", plafond="12.5"),
                erp(13, "neg", plafond="-3"),
                erp(14, "castfail", plafond="abc"),
            ],
            True,
        ),
        web=([web(100, "DUPONT", mail="a@x.fr"), web(101, "Durant")], False),
    )
    # identity change merges/splits, deletion, same-key duplicates, bad revision, null key
    out = r.step(
        "r2",
        crm=(
            [
                crm(1, "Martin"),
                crm(2, "Martin", bd="1970-01-01"),
                crm(4, "Dupond"),
                crm(4, "Dupont", rev="2025-01-01"),
                crm(4, "Older", rev="2020-01-01"),
                crm(7, "Rev", rev="not a date"),
                crm(None, "NoKey"),
                crm("", "Empty"),
                crm(8, "Same", rev="2024-06-01"),
                crm(8, "Same2", rev="2024-06-01"),
            ],
            True,
        ),
        erp=(
            [
                erp(10, "dupont"),
                erp(11, "martin", dn="1971-01-01"),
                erp(13, "negfixed", plafond="3"),
            ],
            True,
        ),
        web=([web(100, "Dupont", mail="a@x.fr", rev="2025-02-01"), web(102, "New")], False),
    )
    assert out["ingest"]["crm"]["deleted"] > 0
    # a deleted key comes back (reactivation = I), a full snapshot below the ratio is blocked
    r.step(
        "r3",
        crm=(
            [
                crm(3, "Durand"),
                crm(1, "Martin"),
                crm(2, "Martin", bd="1970-01-01"),
                crm(4, "Dupont", rev="2025-01-01"),
                crm(8, "Same2", rev="2024-06-01"),
            ],
            True,
        ),
    )
    blocked = r.step("r4", crm=([crm(1, "Martin")], True))
    assert blocked["ingest"]["crm"]["blocked"] is True
    assert blocked["reconcile"]["delta_nodes"] == 0
    anomalies = blocked["tables"]["anomaly"].to_pylist()
    assert any(a["kind"] == "batch_blocked" for a in anomalies)
    r.step(
        "r5",
        erp=(
            [
                erp(10, "dupont"),
                erp(11, "martin", dn="1971-01-01"),
                erp(13, "negfixed", plafond="3"),
            ],
            True,
        ),
    )


def test_odd_strings_and_keys_round_trip_through_copy_and_cypher(age_server, tmp_path) -> None:
    r = Run(age_server, tmp_path, "odd", max_block_size=50)
    odd = [
        'O"Neil',
        "back\\slash",
        "new\nline",
        "tab\tx",
        "ünï©ödé",
        "日本語",
        "q'uote",
        "$$dollar$$",
        "semi;colon",
        "{brace}",
        "pct%s",
        "emoji😀",
    ]
    crm_rows = [
        crm(f'k"{i}\\\n\u00e9', name, fn=name, em=f"{i}@x.fr") for i, name in enumerate(odd)
    ]
    erp_rows = [erp(f"e'{i}$$", name.lower()) for i, name in enumerate(odd)]
    r.step("o1", crm=(crm_rows, True), erp=(erp_rows, True), web=([web(1, 'O"Neil')], False))
    crm_rows[0] = crm(crm_rows[0]["id"], "Changed")
    r.step("o2", crm=(crm_rows, True), erp=(erp_rows, True))


def test_oversized_blocks_are_logged_not_expanded_and_flip_back(age_server, tmp_path) -> None:
    r = Run(age_server, tmp_path, "over", max_block_size=3)
    crowd = [crm(i, "Smith", fn=f"john{i}") for i in range(1, 7)]
    out = r.step("b1", crm=(crowd[:2], True), erp=([erp(10, "smith")], True))
    assert out["tables"]["match_pair"].num_rows > 0
    out = r.step("b2", crm=(crowd, True), erp=([erp(10, "smith")], True))
    kinds = [a["kind"] for a in out["tables"]["anomaly"].to_pylist()]
    assert "oversized_block" in kinds
    # shrinking the block flips it back: its members are rescored and pairs reappear
    out = r.step("b3", crm=(crowd[:4], True), erp=([erp(10, "smith")], True))
    out = r.step("b4", crm=(crowd[:2], True), erp=([erp(10, "smith")], True))
    assert out["reconcile"].get("flipped_blocks", 0) >= 1
    r.step("b5", crm=(crowd[:2] + [crm(20, "Smyth", fn="john1")], True))


def test_golden_blocked_then_published_and_retired(age_server, tmp_path) -> None:
    r = Run(age_server, tmp_path, "blk", max_block_size=50)
    out = r.step(
        "g1",
        crm=([crm(1, None, fn="ann"), crm(2, "Okay", fn="bob")], True),
        erp=([erp(10, None, prenom="ann")], True),
    )
    assert any(a["kind"] == "golden_blocked" for a in out["tables"]["anomaly"].to_pylist())
    out = r.step("g2", crm=([crm(1, "Fixed", fn="ann"), crm(2, "Okay", fn="bob")], True))
    golden = {g["status"] for g in out["tables"]["golden"].to_pylist()}
    assert "published" in golden
    r.step("g3", crm=([crm(2, "Okay", fn="bob")], True))
    r.step("g4", crm=([crm(1, "Fixed", fn="ann"), crm(2, "Okay", fn="bob")], True))


def test_most_recent_priority_ties_and_provenance(age_server, tmp_path) -> None:
    r = Run(age_server, tmp_path, "surv", max_block_size=50)
    r.step(
        "s1",
        crm=(
            [
                crm(1, "Same", em="old@x.fr", rev="2024-01-01"),
                crm(2, "Same", em="mid@x.fr", rev="2024-03-01", ct="BE"),
            ],
            True,
        ),
        erp=([erp(10, "same", pays="DE", rev="2024-02-01")], True),
        web=(
            [
                web(100, "SAME", mail="new@x.fr", rev="2024-05-01"),
                web(101, "Same", mail="tie@x.fr", rev="2024-05-01"),
            ],
            False,
        ),
    )
    r.step(
        "s2",
        crm=(
            [
                crm(1, "Same", em="old@x.fr", rev="2024-01-01"),
                crm(2, "Same", em="mid@x.fr", rev="2024-03-01", ct="BE", fn="other"),
            ],
            True,
        ),
    )


def test_typed_values_dates_decimals_timestamps(age_server, tmp_path) -> None:
    r = Run(age_server, tmp_path, "typed", max_block_size=50)
    rows = [
        {
            "id": 1,
            "ln": "Typed",
            "fn": "a",
            "bd": date(1980, 5, 1),
            "em": None,
            "ct": "FR",
            "rev": datetime(2024, 1, 1, 12, 30, 15, 123456),
        },
        {
            "id": 2,
            "ln": "Typed",
            "fn": "a",
            "bd": date(1980, 5, 2),
            "em": None,
            "ct": "FR",
            "rev": datetime(2024, 1, 2),
        },
    ]
    erps = [
        {
            "id": 10,
            "nom": "typed",
            "prenom": "a",
            "dn": date(1980, 5, 1),
            "pays": "FR",
            "plafond": Decimal("1234.56785"),
            "rev": datetime(2024, 1, 1),
        },
        {
            "id": 11,
            "nom": "other",
            "prenom": "b",
            "dn": date(1990, 1, 1),
            "pays": "FR",
            "plafond": Decimal("5"),
            "rev": datetime(2024, 1, 1),
        },
    ]
    r.step("t1", crm=(rows, True), erp=(erps, True))
