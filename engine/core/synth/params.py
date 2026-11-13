"""Paramètres du générateur synthétique (sérialisables en JSON, déterministes via `seed`)."""

from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass, field, fields
from datetime import date
from typing import Any

DEFAULT_TYPE_MIX: dict[str, float] = {
    "string": 0.55,
    "int": 0.15,
    "decimal": 0.12,
    "date": 0.08,
    "timestamp": 0.04,
    "bool": 0.06,
}
MAX_DAYS = 60
MAX_SOURCES = 16
RATE_FIELDS = (
    "nullable_share",
    "null_rate",
    "overlap",
    "dup_rate",
    "noise_rate",
    "dup_noise_rate",
    "violation_rate",
    "conflict_rate",
    "contradiction_rate",
    "check_share",
    "coverage",
    "most_recent_share",
    "attr_change_rate",
)


def _per_source(values: tuple[str, ...], i: int, default: str) -> str:
    if not values:
        return default
    return values[i] if len(values) > 1 else values[0]


@dataclass
class SynthParams:
    nodes: int = 10_000  # nombre d'entités golden (vérité) ; les lignes source en découlent
    attributes: int = 50
    sources: int = 3
    seed: int = 1
    referential: str = "customer"
    kinds: tuple[str, ...] = ()  # par source : postgres|parquet (vide = tout postgres)
    snapshots: tuple[str, ...] = ()  # par source : full|delta (vide = tout full)
    type_mix: dict[str, float] = field(default_factory=lambda: dict(DEFAULT_TYPE_MIX))
    nullable_share: float = 0.8  # part des attributs génériques nullable
    null_rate: float = 0.03  # taux de nuls moyen des attributs nullable
    overlap: float = 0.6  # proba qu'une entité soit observée dans chaque source (>= 1 garantie)
    dup_rate: float = 0.02  # doublons intra-source (autre clé, même entité)
    noise_rate: float = 0.08  # bruit (typo/casse/accent/espaces) sur les attributs d'identité
    dup_noise_rate: float = 0.35  # idem pour les lignes doublons
    violation_rate: float = 0.0005  # part des valeurs violant un contrôle DQ
    conflict_rate: float = 0.02  # valeur différente entre sources (attributs non identité)
    contradiction_rate: float = 0.01  # date de naissance / email contradictoires entre sources
    check_share: float = 0.25  # part des attributs génériques portant un contrôle
    coverage: float = 0.8  # proba qu'un attribut générique soit mappé dans une source donnée
    most_recent_share: float = 0.1
    attr_change_rate: float = 0.15  # part des attributs modifiés par une mise à jour de ligne
    name_pool: int = 20_000
    pg_schema: str = "public"
    start_date: str = "2025-01-01"
    part_rows: int = 50_000
    compression: str = "zstd"
    workers: int = 0  # 0 = automatique (nb de threads machine, plafonné à 32)

    def __post_init__(self) -> None:
        self.kinds = tuple(self.kinds)
        self.snapshots = tuple(self.snapshots)
        self.validate()

    def validate(self) -> None:
        if self.nodes < 1:
            raise ValueError("nodes must be >= 1")
        if self.attributes < 6:
            raise ValueError("attributes must be >= 6 (4 identity attributes + country + 1)")
        if not 1 <= self.sources <= MAX_SOURCES:
            raise ValueError(f"sources must be in 1..{MAX_SOURCES}")
        for name, values, allowed in (
            ("kinds", self.kinds, {"postgres", "parquet"}),
            ("snapshots", self.snapshots, {"full", "delta"}),
        ):
            if values and len(values) not in (1, self.sources):
                raise ValueError(f"{name} must have 1 or {self.sources} entries")
            if not set(values) <= allowed:
                raise ValueError(f"{name} must be in {sorted(allowed)}")
        if not set(self.type_mix) <= set(DEFAULT_TYPE_MIX) or sum(self.type_mix.values()) <= 0:
            raise ValueError(f"type_mix keys must be in {sorted(DEFAULT_TYPE_MIX)}")
        for name in RATE_FIELDS:
            if not 0 <= getattr(self, name) <= 1:
                raise ValueError(f"{name} must be in [0, 1]")
        if self.overlap <= 0:
            raise ValueError("overlap must be > 0")
        if self.part_rows < 1:
            raise ValueError("part_rows must be >= 1")
        date.fromisoformat(self.start_date)

    def kind_of(self, i: int) -> str:
        return _per_source(self.kinds, i, "postgres")

    def snapshot_of(self, i: int) -> str:
        return _per_source(self.snapshots, i, "full")

    def n_workers(self) -> int:
        return self.workers if self.workers > 0 else min(32, os.cpu_count() or 1)

    def to_json(self) -> str:
        return json.dumps(asdict(self), sort_keys=True)

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> SynthParams:
        known = {f.name for f in fields(cls)}
        return cls(**{k: v for k, v in raw.items() if k in known})

    @classmethod
    def from_json(cls, text: str) -> SynthParams:
        return cls.from_dict(json.loads(text))


@dataclass
class DeltaParams:
    """Composition d'un jour k >= 1. Les pourcentages portent sur les lignes actives de J-1."""

    update_pct: float = 3.0
    new_pct: float = 1.0
    delete_pct: float = 0.5  # sources snapshot=full uniquement (une source delta ne retire rien)
    broken_pct: float = 0.2  # lignes dont une valeur viole un contrôle reject_record
    merge_pct: float = 5.0  # % des mises à jour dont l'identité devient celle d'une autre entité
    split_pct: float = 5.0  # % des mises à jour dont l'identité devient une nouvelle entité
    new_existing_share: float = 0.3  # part des nouvelles lignes observant une entité existante

    def __post_init__(self) -> None:
        for name in ("update_pct", "new_pct", "delete_pct", "broken_pct"):
            if not 0 <= getattr(self, name) <= 100:
                raise ValueError(f"{name} must be in [0, 100]")
        if self.update_pct + self.delete_pct + self.broken_pct > 100:
            raise ValueError("update + delete + broken must not exceed 100%")
        for name in ("merge_pct", "split_pct"):
            if not 0 <= getattr(self, name) <= 100:
                raise ValueError(f"{name} must be in [0, 100]")
        if self.merge_pct + self.split_pct > 100:
            raise ValueError("merge_pct + split_pct must not exceed 100")
        if not 0 <= self.new_existing_share <= 1:
            raise ValueError("new_existing_share must be in [0, 1]")
