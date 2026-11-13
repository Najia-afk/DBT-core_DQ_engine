"""Small three-source config + row builders (same shape as the reference engine tests)."""

from __future__ import annotations

from typing import Any

BASE: dict[str, Any] = {
    "referential": "customer",
    "version": 1,
    "state": {"dsn_env": "X", "schema": "mdm_test"},
    "sources": [
        {
            "id": "crm",
            "kind": "postgres",
            "location": "t.crm",
            "dsn_env": "A",
            "key": "id",
            "revision": "rev",
            "snapshot": "full",
            "full_snapshot_min_ratio": 0.5,
        },
        {
            "id": "erp",
            "kind": "postgres",
            "location": "t.erp",
            "dsn_env": "B",
            "key": "id",
            "revision": "rev",
            "snapshot": "full",
            "full_snapshot_min_ratio": 0.5,
        },
        {
            "id": "web",
            "kind": "parquet",
            "location": "/w/*.parquet",
            "key": "id",
            "revision": "rev",
            "snapshot": "delta",
        },
    ],
    "attributes": [
        {
            "name": "last_name",
            "nullable": False,
            "normalize": ["trim", "lower", "strip_accents", "collapse_spaces"],
            "sources": [
                {"source": "crm", "column": "ln"},
                {"source": "erp", "column": "nom"},
                {"source": "web", "column": "surname"},
            ],
            "checks": [{"rule": "max_length", "value": 12, "on_fail": "reject_record"}],
        },
        {
            "name": "first_name",
            "normalize": ["trim", "lower", "strip_accents"],
            "sources": [
                {"source": "crm", "column": "fn"},
                {"source": "erp", "column": "prenom"},
                {"source": "web", "column": "given"},
            ],
        },
        {
            "name": "birth_date",
            "type": "date",
            "sources": [{"source": "crm", "column": "bd"}, {"source": "erp", "column": "dn"}],
        },
        {
            "name": "email",
            "normalize": ["trim", "lower"],
            "strategy": "most_recent",
            "sources": [{"source": "web", "column": "mail"}, {"source": "crm", "column": "em"}],
            "checks": [
                {"rule": "regex", "value": r"^[^@\s]+@[^@\s]+\.[^@\s]+$", "on_fail": "null_value"}
            ],
        },
        {
            "name": "country",
            "normalize": ["trim", "upper"],
            "sources": [{"source": "crm", "column": "ct"}, {"source": "erp", "column": "pays"}],
            "checks": [{"rule": "in_set", "value": ["FR", "BE", "DE"], "on_fail": "warn"}],
        },
        {
            "name": "credit_limit",
            "type": "decimal",
            "sources": [{"source": "erp", "column": "plafond"}],
            "checks": [{"rule": "min", "value": 0, "on_fail": "reject_record"}],
        },
    ],
    "matching": {
        "version": "t-1",
        "blocking": [
            {
                "name": "ln_year",
                "parts": [
                    {"attr": "last_name", "transform": "prefix", "n": 3},
                    {"attr": "birth_date", "transform": "year"},
                ],
            },
            {"name": "email", "parts": [{"attr": "email"}]},
        ],
        "max_block_size": 50,
        "comparators": [
            {"attr": "last_name", "method": "levenshtein", "weight": 0.4},
            {"attr": "first_name", "method": "levenshtein", "weight": 0.3},
            {"attr": "birth_date", "method": "exact", "weight": 0.2},
            {"attr": "email", "method": "exact", "weight": 0.1},
        ],
        "contradictions": [{"attr": "birth_date"}],
        "accept": 0.95,
        "review": 0.80,
        "min_evidence": 0.5,
    },
    "publication": {"provenance": True},
}


def _key(key: Any) -> Any:
    return str(key) if isinstance(key, int) else key


def crm(
    key: Any,
    ln: str | None,
    fn: str | None = "jean",
    bd: str | None = "1980-05-01",
    em: str | None = None,
    ct: str | None = "FR",
    rev: str | None = "2024-01-01",
) -> dict:
    return {"id": _key(key), "ln": ln, "fn": fn, "bd": bd, "em": em, "ct": ct, "rev": rev}


def erp(
    key: Any,
    nom: str | None,
    prenom: str | None = "jean",
    dn: str | None = "1980-05-01",
    pays: str | None = "FR",
    plafond: Any = None,
    rev: str | None = "2024-01-01",
) -> dict:
    return {
        "id": _key(key),
        "nom": nom,
        "prenom": prenom,
        "dn": dn,
        "pays": pays,
        "plafond": plafond,
        "rev": rev,
    }


def web(
    key: Any,
    surname: str | None,
    given: str | None = "jean",
    mail: str | None = None,
    rev: str | None = "2024-01-01",
) -> dict:
    return {"id": _key(key), "surname": surname, "given": given, "mail": mail, "rev": rev}
