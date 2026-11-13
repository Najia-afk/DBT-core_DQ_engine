"""Scénarios « sales » (clés nulles, doublons, valeurs invalides, lots bloqués, blocs trop gros,
fusions/scissions, réapparitions) comparés à la référence à chaque étape.

Les données sont des valeurs brutes texte (dates, décimaux, révisions en chaînes) : cela exerce les
conversions SQL depuis VARCHAR. Un générateur pseudo-aléatoire à graine fixe produit les lots.
"""

from __future__ import annotations

import copy
import random
from pathlib import Path
from typing import Any

import pytest
from elt_helpers import RowsHarness

from engine.core.config import ReferentialConfig

BASE: dict[str, Any] = {
    "referential": "customer",
    "version": 1,
    "state": {"dsn_env": "MDM_STATE_DSN", "schema": "mdm_scenarios"},
    "sources": [
        {
            "id": "crm",
            "kind": "parquet",
            "location": "",
            "key": "id",
            "revision": "rev",
            "snapshot": "full",
            "full_snapshot_min_ratio": 0.5,
        },
        {
            "id": "erp",
            "kind": "parquet",
            "location": "",
            "key": "id",
            "revision": "rev",
            "snapshot": "full",
            "full_snapshot_min_ratio": 0.5,
        },
        {
            "id": "web",
            "kind": "parquet",
            "location": "",
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
        {"name": "vip", "type": "bool", "sources": [{"source": "crm", "column": "vip"}]},
        {
            "name": "visits",
            "type": "int",
            "sources": [{"source": "web", "column": "visits"}],
            "checks": [{"rule": "max", "value": 1000, "on_fail": "null_value"}],
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

LAST = [
    "dupont",
    "dupond",
    "Dupont",
    " DUPONT ",
    "durand",
    "martin",
    "martins",
    "Éléonore",
    "eleonore",
    "bernard",
    "x" * 13,
    "ab",
    "a  b",
    None,
    "",
    "   ",
]
FIRST = [
    "jean",
    "Jean",
    "jeanne",
    "marie",
    "Marie-Éve",
    "Zoë",
    "zoe",
    "josé",
    "jose",
    None,
    "",
    "paul",
]
DATES = ["1980-05-01", "1980-05-02", "1980-11-30", "1975-01-01", "notadate", None, "2021-02-29"]
MAILS = ["a@b.fr", "A@B.FR ", "c@d.com", "bad", None, "", "e@f.org"]
COUNTRY = ["FR", " fr ", "BE", "XX", None, "de"]
LIMITS = ["10.5", "0", "-1", "abc", None, "1e2", "100.12345"]
REVS = ["2024-01-0{n}T10:00:00", "2024-01-0{n}", "2024-01-0{n}T10:00:00+02:00"]
VIP = ["true", "false", "1", "0", "maybe", None]
VISITS = [5, 7, 2000, None, 0]


class Universe:
    """Évolue des lignes brutes de trois sources ; produit un lot complet ou delta par étape."""

    def __init__(self, seed: int) -> None:
        self.rng = random.Random(seed)
        self.rows: dict[str, dict[int, dict]] = {"crm": {}, "erp": {}, "web": {}}
        self.next_key = {"crm": 100, "erp": 200, "web": 300}

    def _row(self, sid: str, step: int) -> dict:
        r, n = self.rng, step
        rev = r.choice(REVS).format(n=n)
        if r.random() < 0.05:
            rev = r.choice(["garbage", None])
        base = {"id": None, "rev": rev}
        if sid == "crm":
            base |= {
                "ln": r.choice(LAST),
                "fn": r.choice(FIRST),
                "bd": r.choice(DATES),
                "em": r.choice(MAILS),
                "ct": r.choice(COUNTRY),
                "vip": r.choice(VIP),
            }
        elif sid == "erp":
            base |= {
                "nom": r.choice(LAST),
                "prenom": r.choice(FIRST),
                "dn": r.choice(DATES),
                "pays": r.choice(COUNTRY),
                "plafond": r.choice(LIMITS),
            }
        else:
            base |= {
                "surname": r.choice(LAST),
                "given": r.choice(FIRST),
                "mail": r.choice(MAILS),
                "visits": r.choice(VISITS),
            }
        return base

    def _new(self, sid: str, step: int) -> dict:
        key = self.next_key[sid]
        self.next_key[sid] += 1
        row = self._row(sid, step)
        row["id"] = key
        # peu de valeurs « sales » : la plupart des lignes sont valides pour produire des matches
        if self.rng.random() < 0.8:
            for col, good in (("ln", "dupont"), ("nom", "dupont"), ("surname", "dupont")):
                if col in row and self.rng.random() < 0.7:
                    row[col] = self.rng.choice(["dupont", "durand", "martin", "bernard"])
            for col in ("bd", "dn"):
                if col in row and self.rng.random() < 0.6:
                    row[col] = self.rng.choice(DATES[:4])
            if row["rev"] in {"garbage", None}:
                row["rev"] = f"2024-01-0{step}T10:00:00"
        self.rows[sid][key] = row
        return row

    def step(self, step: int) -> dict[str, list[dict]]:
        r = self.rng
        batches: dict[str, list[dict]] = {}
        for sid, rows in self.rows.items():
            changed: list[dict] = []
            if step == 1:
                for _ in range(40):
                    changed.append(self._new(sid, step))
            else:
                keys = list(rows)
                for key in r.sample(keys, k=min(len(keys), 8)):
                    fresh = self._row(sid, step)
                    fresh["id"] = key
                    old = rows[key]
                    for col in fresh:  # mise à jour partielle : quelques champs seulement
                        if col != "id" and r.random() < 0.35:
                            old[col] = fresh[col]
                    old["rev"] = f"2024-01-0{step}T11:00:00"
                    changed.append(old)
                for _ in range(5):
                    changed.append(self._new(sid, step))
                if rows and sid != "web":  # suppression : clés retirées du snapshot complet
                    for key in r.sample(list(rows), k=min(len(rows), r.choice([0, 2, 4]))):
                        rows.pop(key)
            if sid == "web":
                batch = list(changed)
            else:
                batch = [dict(row) for row in rows.values()]
            # doublons de clé, clé nulle, clé vide, ligne identique répétée
            if batch and r.random() < 0.8:
                dup = dict(r.choice(batch))
                dup["rev"] = r.choice(["2024-01-01T00:00:00", f"2024-01-0{step}T23:00:00", None])
                batch.append(dup)
            if batch and r.random() < 0.5:
                batch.append({**r.choice(batch), "id": None})
            if r.random() < 0.3 and sid != "web":
                batch = batch[: max(1, len(batch) // 5)]  # lot tronqué : complétude insuffisante
            batches[sid] = batch if batch else [self._row(sid, step) | {"id": 9999}]
        return batches


def scenario_config(root: Path, **changes: Any) -> ReferentialConfig:
    raw = copy.deepcopy(BASE)
    for s in raw["sources"]:
        s["location"] = (root / s["id"] / "*.parquet").as_posix()
    for dotted, value in changes.items():
        target = raw
        *path, last = dotted.split("__")
        for part in path:
            target = target[int(part) if part.isdigit() else part]
        target[last] = value
    return ReferentialConfig.model_validate(raw)


@pytest.mark.parametrize(
    "seed,changes",
    [
        (1, {}),
        (2, {"matching__max_block_size": 3}),
        (
            3,
            {
                "matching__accept": 0.9,
                "matching__min_evidence": 0.7,
                "publication__provenance": False,
                # first_name garde ses accents : levenshtein sur points de code non ASCII
                "attributes__1__normalize": ["trim", "lower"],
            },
        ),
    ],
)
async def test_dirty_data_scenarios_match_reference(seed, changes, elt_env, work_dir):
    cfg = scenario_config(work_dir / "data", **changes)
    h = RowsHarness(cfg, work_dir / "data")
    universe = Universe(seed)
    for step in range(1, 6):
        batches = universe.step(step)
        await h.step(f"s{step}", **batches)
        h.assert_equivalent()
    exported = h.export()
    assert exported["source_record"].num_rows > 0
    assert exported["golden"].num_rows > 0
    kinds = {r["kind"] for r in exported["anomaly"].to_pylist()}
    rules = {r["rule"] for r in exported["reject"].to_pylist()}
    events = {r["kind"] for r in exported["staging_event"].to_pylist()}
    # le jeu exerce bien les chemins difficiles (sinon l'équivalence ne prouverait rien)
    assert {"golden_blocked", "batch_blocked"} <= kinds, kinds
    assert {"key_null", "duplicate_key", "cast", "revision", "regex", "max_length"} & rules
    assert {"split", "create"} <= events, events
    if not changes.get("matching__max_block_size"):
        assert "merge" in events, events
    assert exported["review"].num_rows > 0
    if changes.get("matching__max_block_size") == 3:
        assert "oversized_block" in kinds
