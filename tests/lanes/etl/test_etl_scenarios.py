"""Hand-written scenarios (parquet sources) checked against the reference after every step."""

from __future__ import annotations

import copy
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import yaml
from etl_support import Lab

SOURCES = {"crm": "id", "erp": "id", "web": "id"}
LEFEVRE = "Lef\u00e8vre"
ELOISE = "\u00c9lo\u00efse"
CONFIG: dict[str, Any] = {
    "referential": "scn",
    "version": 1,
    "state": {"dsn_env": "MDM_STATE_DSN", "schema": "mdm_scn"},
    "sources": [
        {"id": "crm", "kind": "parquet", "location": "@/crm/*.parquet", "key": "id",
         "revision": "rev", "snapshot": "full", "full_snapshot_min_ratio": 0.4},
        {"id": "erp", "kind": "parquet", "location": "@/erp/*.parquet", "key": "id",
         "revision": "rev", "snapshot": "full", "full_snapshot_min_ratio": 0.4},
        {"id": "web", "kind": "parquet", "location": "@/web/*.parquet", "key": "id",
         "revision": "rev", "snapshot": "delta"},
    ],
    "attributes": [
        {"name": "last_name", "nullable": False,
         "normalize": ["trim", "lower", "strip_accents", "collapse_spaces"],
         "sources": [{"source": "crm", "column": "ln"}, {"source": "erp", "column": "nom"},
                     {"source": "web", "column": "surname"}],
         "checks": [{"rule": "max_length", "value": 12, "on_fail": "reject_record"}]},
        {"name": "first_name", "normalize": ["trim", "lower", "strip_accents"],
         "sources": [{"source": "crm", "column": "fn"}, {"source": "erp", "column": "prenom"},
                     {"source": "web", "column": "given"}]},
        {"name": "birth_date", "type": "date",
         "sources": [{"source": "crm", "column": "bd"}, {"source": "erp", "column": "dn"}]},
        {"name": "email", "normalize": ["trim", "lower"], "strategy": "most_recent",
         "sources": [{"source": "web", "column": "mail"}, {"source": "crm", "column": "em"}],
         "checks": [{"rule": "regex", "value": "^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$",
                     "on_fail": "null_value"}]},
        {"name": "credit_limit", "type": "decimal",
         "sources": [{"source": "erp", "column": "plafond"}],
         "checks": [{"rule": "min", "value": 0, "on_fail": "reject_record"}]},
    ],
    "matching": {
        "version": "scn-1",
        "blocking": [{"name": "ln", "parts": [{"attr": "last_name", "transform": "prefix", "n": 3},
                                               {"attr": "birth_date", "transform": "year"}]},
                     {"name": "email", "parts": [{"attr": "email"}]}],
        "max_block_size": 50,
        "comparators": [{"attr": "last_name", "method": "levenshtein", "weight": 0.4},
                        {"attr": "first_name", "method": "levenshtein", "weight": 0.3},
                        {"attr": "birth_date", "method": "exact", "weight": 0.2},
                        {"attr": "email", "method": "exact", "weight": 0.1}],
        "contradictions": [{"attr": "birth_date"}],
    },
}  # fmt: skip


def crm(key: Any, ln: str | None, fn: str = "jean", bd: str | None = "1980-05-01",
        em: str | None = None, rev: str | None = "2024-01-01") -> dict:  # fmt: skip
    return {"id": key, "ln": ln, "fn": fn, "bd": bd, "em": em, "rev": rev}


def erp(key: Any, nom: str | None, prenom: str = "jean", dn: str | None = "1980-05-01",
        plafond: Any = None, rev: str | None = "2024-01-01") -> dict:  # fmt: skip
    return {"id": key, "nom": nom, "prenom": prenom, "dn": dn, "plafond": plafond, "rev": rev}


def web(key: Any, surname: str | None, given: str = "jean", mail: str | None = None,
        rev: str | None = "2024-01-01") -> dict:  # fmt: skip
    return {"id": key, "surname": surname, "given": given, "mail": mail, "rev": rev}


SCHEMAS = {
    "crm": pa.schema([("id", pa.string()), ("ln", pa.string()), ("fn", pa.string()),
                      ("bd", pa.string()), ("em", pa.string()), ("rev", pa.string())]),
    "erp": pa.schema([("id", pa.string()), ("nom", pa.string()), ("prenom", pa.string()),
                      ("dn", pa.string()), ("plafond", pa.string()), ("rev", pa.string())]),
    "web": pa.schema([("id", pa.string()), ("surname", pa.string()), ("given", pa.string()),
                      ("mail", pa.string()), ("rev", pa.string())]),
}  # fmt: skip

STEPS: list[dict[str, list[dict]]] = [
    # 1: a crm/erp/web triple of the same person, one lone record, one multi-candidate setup
    {"crm": [crm("c1", "Dupont"), crm("c2", "Martin", "pierre", "1975-02-03"),
             crm("c3", LEFEVRE, ELOISE, "1990-07-07", "elo@x.fr")],
     "erp": [erp("e1", "DUPONT", plafond="1000.5"), erp("e2", "martin", "pierre", "1975-02-03"),
             erp("e3", "Lefevre", "Eloise", "1990-07-07", plafond="12.34567")],
     "web": [web("w1", "dupont", mail="j@x.fr", rev="2024-03-01")]},
    # 2: update that splits (c1 gets another identity), e2 retired by absence, bad revision
    {"crm": [crm("c1", "Durand", "paul", "1970-01-01", rev="2024-02-01"),
             crm("c2", "Martin", "pierre", "1975-02-03"), crm("c3", LEFEVRE, ELOISE,
             "1990-07-07", "elo@x.fr"), crm("c4", "Zed", rev="garbage")],
     "erp": [erp("e1", "DUPONT", plafond="1000.5"), erp("e3", "Lefevre", "Eloise", "1990-07-07",
             plafond="12.34567")],
     "web": [web("w2", "Zed", mail="not-mail", rev="2024-03-02")]},
    # 3: e2 comes back identical (reactivation), same-hash replay, revision-only change, dup keys
    {"crm": [crm("c1", "Durand", "paul", "1970-01-01", rev="2024-05-05"),
             crm("c2", "Martin", "pierre", "1975-02-03"), crm("c3", LEFEVRE, ELOISE,
             "1990-07-07", "elo@x.fr"), crm("c5", "Martin", "pierre", "1975-02-03")],
     "erp": [erp("e1", "DUPONT", plafond="1000.5"), erp("e2", "martin", "pierre", "1975-02-03"),
             erp("e3", "Lefevre", "Eloise", "1990-07-07", plafond="12.34567"),
             erp("e1", "Dupond", plafond="1", rev="2024-01-01")],
     "web": [web("w2", "Zed", mail="z@x.fr", rev="2024-03-02"), web("w1", "dupont", mail="j@x.fr",
             rev="2024-03-01"), web("w1", "older", rev="2023-01-01"), web(None, "nokey")]},
    # 4: blocked golden (non-nullable last_name only known by one erp record) then repaired
    {"crm": [crm("c1", "Durand", "paul", "1970-01-01"), crm("c3", LEFEVRE, ELOISE,
             "1990-07-07", "elo@x.fr"), crm("c2", "Martin", "pierre", "1975-02-03"),
             crm("c5", "Martin", "pierre", "1975-02-03")],
     "erp": [erp("e1", "DUPONT", plafond="-3"), erp("e2", "martin", "pierre", "1975-02-03"),
             erp("e3", None, "Eloise", "1990-07-07"), erp("e9", None, "ghost", "1950-01-01")],
     "web": []},
    {"crm": [crm("c1", "Durand", "paul", "1970-01-01"), crm("c3", LEFEVRE, ELOISE,
             "1990-07-07", "elo@x.fr"), crm("c2", "Martin", "pierre", "1975-02-03"),
             crm("c5", "Martin", "pierre", "1975-02-03")],
     "erp": [erp("e1", "DUPONT"), erp("e2", "martin", "pierre", "1975-02-03"),
             erp("e3", "Lefevre", "Eloise", "1990-07-07"),
             erp("e9", "Ghost", "ghost", "1950-01-01")],
     "web": [web("w3", "toolongsurname1", mail="w@x.fr")]},
]  # fmt: skip


@pytest.fixture
def scenario_lab(tmp_path: Path, pg_db: str, monkeypatch: pytest.MonkeyPatch) -> Lab:
    raw = copy.deepcopy(CONFIG)
    for source in raw["sources"]:
        source["location"] = source["location"].replace("@", tmp_path.as_posix())
    (tmp_path / "scn.yml").write_text(yaml.safe_dump(raw, sort_keys=False), encoding="utf-8")
    return Lab(tmp_path, pg_db, monkeypatch, tmp_path / "out", "scn.yml")


def publish(lab: Lab, step: dict[str, list[dict]]) -> None:
    for source, rows in step.items():
        folder = lab.ds / source
        folder.mkdir(exist_ok=True)
        for old in folder.glob("*.parquet"):
            old.unlink()
        stringified = [{k: None if v is None else str(v) for k, v in r.items()} for r in rows]
        pq.write_table(
            pa.Table.from_pylist(stringified, schema=SCHEMAS[source]), folder / "p.parquet"
        )


def test_scenarios_match_reference(scenario_lab: Lab) -> None:
    lab = scenario_lab
    lab.lane("init")
    anomalies: set[str] = set()
    for index, step in enumerate(STEPS, start=1):
        publish(lab, step)
        run = f"r{index}"
        for source in SOURCES:
            lab.lane("ingest", "--source", source, "--run-id", run)
            lab.ref.ingest(source, step[source], run)
        lab.lane("reconcile", "--run-id", run)
        lab.ref.reconcile(run)
        report = lab.compare()
        assert report.ok, f"step {index}: {report.format()}"
        anomalies |= set(lab.export("cur")["anomaly"].column("kind").to_pylist())
    tables = lab.export("final")
    assert "golden_blocked" in anomalies
    assert {"merge", "split", "create"} <= set(tables["staging_event"].column("kind").to_pylist())
    reasons = {r["reason"] for r in tables["review"].to_pylist()}
    assert reasons  # review pairs exist
    rejects = {(r["rule"], r["action"]) for r in tables["reject"].to_pylist()}
    assert {("revision", "reject_record"), ("key_null", "reject_record"),
            ("duplicate_key", "reject_record"), ("min", "reject_record"),
            ("max_length", "reject_record"), ("regex", "null_value")} <= rejects  # fmt: skip
    kinds = {r["kind"] for r in tables["source_delta"].to_pylist()}
    assert kinds == {"I", "U", "D"}
