"""Configuration d'un référentiel MDM : seule source de vérité des trois voies.

Voir docs/MDM_CONTRACT.md. Aucune évaluation de code arbitraire : toutes les options sont des
listes fermées.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, model_validator

IDENT = r"^[a-z][a-z0-9_]{0,62}$"

AttrType = Literal["string", "int", "decimal", "date", "timestamp", "bool"]
Normalizer = Literal[
    "trim", "lower", "upper", "strip_accents", "collapse_spaces", "digits_only", "alnum_only"
]
Comparator = Literal["exact", "levenshtein", "date_year"]
OnFail = Literal["reject_record", "null_value", "warn"]
BlockTransform = Literal["exact", "prefix", "year"]

COMPARATORS_BY_TYPE: dict[str, set[str]] = {
    "string": {"exact", "levenshtein"},
    "int": {"exact"},
    "decimal": {"exact"},
    "bool": {"exact"},
    "date": {"exact", "date_year"},
    "timestamp": {"exact", "date_year"},
}


class Strict(BaseModel):
    model_config = ConfigDict(extra="forbid")


class Source(Strict):
    id: str = Field(pattern=IDENT)
    kind: Literal["postgres", "parquet"]
    location: str = Field(min_length=1)  # schema.table ou chemin/glob Parquet
    dsn_env: str | None = None  # nom de la variable d'environnement (postgres uniquement)
    key: str = Field(min_length=1)  # colonne source de la clé
    revision: str | None = None  # colonne ordonnant les versions d'une clé
    snapshot: Literal["full", "delta"] = "full"
    full_snapshot_min_ratio: float = Field(default=0.9, gt=0, le=1)

    @model_validator(mode="after")
    def _check(self) -> Source:
        if self.kind == "postgres" and not self.dsn_env:
            raise ValueError(f"source {self.id}: dsn_env is required for postgres")
        if self.kind == "parquet" and self.dsn_env:
            raise ValueError(f"source {self.id}: dsn_env is not allowed for parquet")
        return self


class SourceColumn(Strict):
    source: str
    column: str = Field(min_length=1)


class Check(Strict):
    rule: Literal["regex", "in_set", "min", "max", "max_length"]
    value: str | int | float | list[str]
    on_fail: OnFail = "reject_record"

    @model_validator(mode="after")
    def _check(self) -> Check:
        if self.rule == "regex":
            if not isinstance(self.value, str):
                raise ValueError("regex value must be a string")
            re.compile(self.value)
        elif self.rule == "in_set":
            if not isinstance(self.value, list) or not self.value:
                raise ValueError("in_set value must be a non-empty list")
        elif isinstance(self.value, (str, list)) or isinstance(self.value, bool):
            raise ValueError(f"{self.rule} value must be a number")
        return self


class Attribute(Strict):
    name: str = Field(pattern=IDENT)
    type: AttrType = "string"
    nullable: bool = True
    normalize: list[Normalizer] = Field(default_factory=list)
    # L'ordre de cette liste est la priorité de survie.
    sources: list[SourceColumn] = Field(min_length=1)
    strategy: Literal["priority", "most_recent"] = "priority"
    checks: list[Check] = Field(default_factory=list)

    @model_validator(mode="after")
    def _check(self) -> Attribute:
        if self.type != "string" and self.normalize:
            raise ValueError(f"{self.name}: normalize only applies to string attributes")
        ids = [s.source for s in self.sources]
        if len(set(ids)) != len(ids):
            raise ValueError(f"{self.name}: a source can appear only once")
        if len(set(self.normalize)) != len(self.normalize):
            raise ValueError(f"{self.name}: duplicate normalizers")
        for chk in self.checks:
            numeric = self.type in {"int", "decimal"}
            if chk.rule in {"min", "max"} and not numeric:
                raise ValueError(f"{self.name}: {chk.rule} needs a numeric attribute")
            if chk.rule in {"regex", "max_length"} and self.type != "string":
                raise ValueError(f"{self.name}: {chk.rule} needs a string attribute")
        return self


class BlockPart(Strict):
    attr: str
    transform: BlockTransform = "exact"
    n: int | None = Field(default=None, ge=1, le=32)  # prefix uniquement

    @model_validator(mode="after")
    def _check(self) -> BlockPart:
        if (self.transform == "prefix") != (self.n is not None):
            raise ValueError("n is required for (and only for) the prefix transform")
        return self


class BlockingKey(Strict):
    name: str = Field(pattern=IDENT)
    parts: list[BlockPart] = Field(min_length=1)


class ComparatorRule(Strict):
    attr: str
    method: Comparator
    weight: float = Field(gt=0, le=1)


class Contradiction(Strict):
    attr: str


class Matching(Strict):
    version: str = Field(min_length=1)
    blocking: list[BlockingKey] = Field(min_length=1)
    max_block_size: int = Field(default=500, ge=2)
    comparators: list[ComparatorRule] = Field(min_length=1)
    contradictions: list[Contradiction] = Field(default_factory=list)
    accept: float = Field(default=0.95, gt=0, le=1)
    review: float = Field(default=0.80, gt=0, le=1)
    min_evidence: float = Field(default=0.5, gt=0, le=1)

    @model_validator(mode="after")
    def _check(self) -> Matching:
        if self.review >= self.accept:
            raise ValueError("review must be lower than accept")
        if abs(sum(c.weight for c in self.comparators) - 1.0) > 1e-9:
            raise ValueError("comparator weights must sum to 1")
        names = [b.name for b in self.blocking]
        if len(set(names)) != len(names):
            raise ValueError("blocking key names must be unique")
        return self


class Publication(Strict):
    provenance: bool = True  # colonne <attr>__src par attribut


class Runtime(Strict):
    profile: Literal["small", "medium", "large"] = "medium"
    batch_rows: int = Field(default=50_000, ge=1_000)
    max_attempts: int = Field(default=3, ge=1, le=10)
    step_timeout_minutes: int = Field(default=120, ge=1)


class State(Strict):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    dsn_env: str  # base d'état/publication du référentiel
    schema_name: str = Field(alias="schema", pattern=IDENT)


class ReferentialConfig(Strict):
    referential: str = Field(pattern=IDENT)
    version: int = Field(ge=1)
    state: State
    sources: list[Source] = Field(min_length=1)
    attributes: list[Attribute] = Field(min_length=1)
    matching: Matching
    publication: Publication = Field(default_factory=Publication)
    runtime: Runtime = Field(default_factory=Runtime)

    @model_validator(mode="after")
    def _check(self) -> ReferentialConfig:
        source_ids = [s.id for s in self.sources]
        if len(set(source_ids)) != len(source_ids):
            raise ValueError("source ids must be unique")
        names = [a.name for a in self.attributes]
        if len(set(names)) != len(names):
            raise ValueError("attribute names must be unique")
        attrs = {a.name: a for a in self.attributes}
        reserved = {
            "golden_id", "staging_id", "status", "stale", "revision", "source_id",
            "source_key", "row_hash", "first_run", "last_change_run", "run_id",
            "node_id", "published_once",
        }  # fmt: skip
        for a in self.attributes:
            if a.name in reserved or a.name.endswith("__src"):
                raise ValueError(f"attribute name {a.name!r} is reserved")
            if len(a.name) > 58:  # <name>__src must fit the 63-char PostgreSQL identifier limit
                raise ValueError(f"attribute name {a.name!r} is longer than 58 characters")
            for sc in a.sources:
                if sc.source not in source_ids:
                    raise ValueError(f"{a.name}: unknown source {sc.source!r}")
                if a.strategy == "most_recent" and self.source(sc.source).revision is None:
                    raise ValueError(
                        f"{a.name}: most_recent needs a revision column on {sc.source}"
                    )
        for s in self.sources:
            if not any(sc.source == s.id for a in self.attributes for sc in a.sources):
                raise ValueError(f"source {s.id} is not mapped by any attribute")
        m = self.matching
        for comp in m.comparators:
            attr = attrs.get(comp.attr)
            if attr is None:
                raise ValueError(f"comparator on unknown attribute {comp.attr!r}")
            if comp.method not in COMPARATORS_BY_TYPE[attr.type]:
                raise ValueError(f"{comp.attr}: {comp.method} is not valid for {attr.type}")
        for c in m.contradictions:
            if c.attr not in attrs:
                raise ValueError(f"contradiction on unknown attribute {c.attr!r}")
        for key in m.blocking:
            for part in key.parts:
                attr = attrs.get(part.attr)
                if attr is None:
                    raise ValueError(f"blocking {key.name}: unknown attribute {part.attr!r}")
                if part.transform == "prefix" and attr.type != "string":
                    raise ValueError(f"blocking {key.name}: prefix needs a string attribute")
                if part.transform == "year" and attr.type not in {"date", "timestamp"}:
                    raise ValueError(f"blocking {key.name}: year needs a date attribute")
        return self

    def attribute(self, name: str) -> Attribute:
        return next(a for a in self.attributes if a.name == name)

    def source(self, source_id: str) -> Source:
        return next(s for s in self.sources if s.id == source_id)

    def mapping(self, source_id: str) -> dict[str, str]:
        """attribut cible -> colonne source, pour une source."""
        return {
            a.name: sc.column for a in self.attributes for sc in a.sources if sc.source == source_id
        }


def load_config(path: str | Path) -> ReferentialConfig:
    try:
        raw = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError) as error:
        raise ValueError(f"cannot read config {path}: {error}") from error
    if not isinstance(raw, dict):
        raise ValueError(f"config {path} must be a YAML mapping")
    return ReferentialConfig.model_validate(raw)
