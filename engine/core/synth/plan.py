"""Plan de génération : attributs, sources, mapping et config YAML valide (ReferentialConfig)."""

from __future__ import annotations

from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import Any

import numpy as np
import yaml

from engine.core.config import ReferentialConfig

from .params import DEFAULT_TYPE_MIX, SynthParams

SOURCE_IDS = ["crm", "erp", "web", "pos", "app", "partner", "legacy", "social"]
KEY_COLS = ["customer_id", "client_no", "account_id"]
REV_COLS = ["updated_at", "modified_at", "event_ts"]
ALIASES: dict[str, list[str]] = {
    "last_name": ["LASTNAME", "nom", "surname"],
    "first_name": ["FIRSTNAME", "prenom", "given_name"],
    "birth_date": ["BIRTHDATE", "date_naissance", "dob"],
    "email": ["MAIL", "courriel", "email_address"],
    "country": ["COUNTRY", "pays", "country_code"],
}
COUNTRIES = ["FR", "BE", "DE", "ES", "IT", "GB"]
CATEGORIES = ["alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta", "theta", "iota"]
ON_FAIL_CYCLE = ["reject_record", "null_value", "warn", "reject_record"]
EMAIL_REGEX = r"^[^@\s]+@[^@\s]+\.[^@\s]+$"
CODE_REGEX = r"^[A-Z]{2}-[0-9]{4}$"
NAME_NORMALIZE = ["trim", "lower", "strip_accents", "collapse_spaces"]
PREFIX = {
    "word": "txt",
    "phrase": "note",
    "code": "code",
    "category": "cat",
    "int": "qty",
    "decimal": "amt",
    "date": "dt",
    "timestamp": "ts",
    "bool": "flag",
}
STRING_KINDS = ["word", "phrase", "code", "category"]
IDENTITY_KINDS = ("last_name", "first_name", "birth_date", "email")
ATTR_TYPE_OF_KIND = {
    "last_name": "string",
    "first_name": "string",
    "email": "string",
    "country": "string",
    "birth_date": "date",
    "word": "string",
    "phrase": "string",
    "code": "string",
    "category": "string",
}
DEFAULT_NORMALIZE = {
    "word": ["trim", "lower"],
    "phrase": ["trim", "lower", "collapse_spaces"],
    "code": ["trim", "upper"],
    "category": ["trim", "lower"],
}
NO_SPEC: dict[str, Any] = {"lo": 0, "span": 1, "values": ()}


@dataclass(frozen=True)
class AttrSpec:
    idx: int
    name: str
    type: str
    kind: str
    identity: bool
    nullable: bool
    normalize: tuple[str, ...]
    strategy: str
    checks: tuple[dict[str, Any], ...]
    sources: tuple[tuple[int, str], ...]  # (indice source, colonne) dans l'ordre de priorité
    null_rate: float
    lo: int = 0
    span: int = 1
    values: tuple[str, ...] = ()


@dataclass(frozen=True)
class SourceSpec:
    idx: int
    id: str
    kind: str
    snapshot: str
    key_col: str
    rev_col: str
    location: str
    dsn_env: str | None
    key_is_str: bool
    key_prefix: str
    key_base: int
    columns: tuple[tuple[int, str], ...]  # (indice attribut, colonne) dans l'ordre des attributs


@dataclass
class Plan:
    params: SynthParams
    attrs: list[AttrSpec]
    sources: list[SourceSpec]
    config: dict[str, Any] = field(default_factory=dict)

    @property
    def nonidentity(self) -> list[int]:
        return [a.idx for a in self.attrs if not a.identity]

    def broken_candidates(self, s: int) -> np.ndarray:
        """Attributs mappés par la source `s` portant un contrôle reject_record."""
        out = [
            a.idx
            for a in self.attrs
            if any(i == s for i, _ in a.sources)
            and any(c["on_fail"] == "reject_record" for c in a.checks)
        ]
        return np.array(out, dtype=np.int64)


def _largest_remainder(total: int, mix: dict[str, float]) -> dict[str, int]:
    weight = sum(mix.values())
    raw = {k: total * v / weight for k, v in mix.items()}
    counts = {k: int(v) for k, v in raw.items()}
    rest = total - sum(counts.values())
    for k in sorted(raw, key=lambda x: (counts[x] - raw[x], x))[:rest]:
        counts[k] += 1
    return counts


def _column_name(attr_name: str, src_idx: int) -> str:
    named = ALIASES.get(attr_name)
    if named is not None:
        base = named[src_idx % len(named)]
        return base if src_idx < len(named) else f"{base}_{src_idx}"
    style = src_idx % 3
    if style == 0:
        col = attr_name.upper()
    elif style == 1:
        col = attr_name.replace("_", "")
    else:
        col = f"{attr_name}_val"
    return col if src_idx < 3 else f"{col}_{src_idx}"


def _source_spec(p: SynthParams, i: int, columns: tuple[tuple[int, str], ...]) -> SourceSpec:
    sid = SOURCE_IDS[i] if i < len(SOURCE_IDS) else f"src_{i:02d}"
    suffix = "" if i < 3 else f"_{i}"
    kind = p.kind_of(i)
    location = (
        f"{p.pg_schema}.{sid}_{p.referential}" if kind == "postgres" else f"live/{sid}/*.parquet"
    )
    return SourceSpec(
        idx=i,
        id=sid,
        kind=kind,
        snapshot=p.snapshot_of(i),
        key_col=KEY_COLS[i % 3] + suffix,
        rev_col=REV_COLS[i % 3] + suffix,
        location=location,
        dsn_env=f"MDM_SRC_{sid.upper()}_DSN" if kind == "postgres" else None,
        key_is_str=i % 2 == 1,
        key_prefix=f"{sid.upper()}-",
        key_base=10_000_000 * (i + 1),
        columns=columns,
    )


def _generic_checks(kind: str, spec: dict[str, Any], n_fail: int) -> tuple[dict[str, Any], ...]:
    fail = ON_FAIL_CYCLE[n_fail % len(ON_FAIL_CYCLE)]
    if kind in {"word", "phrase"}:
        return ({"rule": "max_length", "value": 40, "on_fail": fail},)
    if kind == "code":
        return ({"rule": "regex", "value": CODE_REGEX, "on_fail": fail},)
    if kind == "category":
        return ({"rule": "in_set", "value": list(spec["values"]), "on_fail": fail},)
    if kind == "int":
        other = ON_FAIL_CYCLE[(n_fail + 1) % len(ON_FAIL_CYCLE)]
        return (
            {"rule": "min", "value": 0, "on_fail": fail},
            {"rule": "max", "value": spec["span"], "on_fail": other},
        )
    if kind == "decimal":
        return ({"rule": "min", "value": 0, "on_fail": fail},)
    return ()


def _generic_spec(rng: np.random.Generator, kind: str) -> dict[str, Any]:
    spec: dict[str, Any] = dict(NO_SPEC)
    if kind == "int":
        spec["span"] = int(rng.integers(1_000, 1_000_000))
    elif kind == "decimal":
        spec["span"] = int(rng.integers(1_000, 100_000))  # montant max ; centimes = span * 100
    elif kind == "category":
        spec["values"] = tuple(CATEGORIES[: int(rng.integers(4, len(CATEGORIES) + 1))])
    elif kind == "date":
        spec.update(lo=10_957, span=9_000)  # 2000-01-01 + 9000 jours
    elif kind == "timestamp":
        spec.update(lo=1_577_836_800, span=5 * 365 * 86_400)  # secondes depuis l'epoch
    return spec


def _generic_kinds(p: SynthParams, rng: np.random.Generator) -> list[str]:
    mix = {k: p.type_mix.get(k, 0.0) for k in DEFAULT_TYPE_MIX}
    counts = _largest_remainder(p.attributes - 5, mix)
    kinds = [STRING_KINDS[i % len(STRING_KINDS)] for i in range(counts["string"])]
    for t in ("int", "decimal", "date", "timestamp", "bool"):
        kinds += [t] * counts[t]
    return [kinds[i] for i in rng.permutation(len(kinds))]


class _Builder:
    def __init__(self, p: SynthParams, rng: np.random.Generator) -> None:
        self.p = p
        self.rng = rng
        self.attrs: list[AttrSpec] = []
        self.n_fail = 0

    def add(
        self,
        name: str,
        kind: str,
        *,
        spec: dict[str, Any] | None = None,
        checks: tuple[dict[str, Any], ...] | None = None,
        normalize: list[str] | None = None,
        nullable: bool = True,
    ) -> None:
        p, rng = self.p, self.rng
        generic = kind in PREFIX
        spec = spec or _generic_spec(rng, kind)
        if checks is None:
            checks = ()
            if generic and rng.random() < p.check_share:
                checks = _generic_checks(kind, spec, self.n_fail)
                self.n_fail += len(checks)
        strategy = "priority"
        if generic:
            nullable = bool(rng.random() < p.nullable_share)
            mapped = [s for s in range(p.sources) if rng.random() < p.coverage]
            mapped = mapped or [int(rng.integers(p.sources))]
            mapped = [int(s) for s in rng.permutation(mapped)]
            if rng.random() < p.most_recent_share:
                strategy = "most_recent"
        else:
            mapped = [int(s) for s in rng.permutation(p.sources)]
        null_rate = 0.0
        if nullable:
            null_rate = p.null_rate * float(rng.choice([0.25, 0.5, 1.0, 1.5, 2.0]))
        if kind == "email":
            null_rate = p.null_rate * 2
        if normalize is None:
            normalize = DEFAULT_NORMALIZE.get(kind, [])
        self.attrs.append(
            AttrSpec(
                idx=len(self.attrs),
                name=name,
                type=ATTR_TYPE_OF_KIND.get(kind, kind),
                kind=kind,
                identity=kind in IDENTITY_KINDS,
                nullable=nullable,
                normalize=tuple(normalize),
                strategy=strategy,
                checks=tuple(checks),
                sources=tuple((s, _column_name(name, s)) for s in mapped),
                null_rate=null_rate,
                lo=spec["lo"],
                span=spec["span"],
                values=tuple(spec["values"]),
            )
        )


def _build_attrs(p: SynthParams, rng: np.random.Generator) -> list[AttrSpec]:
    b = _Builder(p, rng)
    kinds = _generic_kinds(p, rng)
    b.add(
        "last_name",
        "last_name",
        nullable=False,
        normalize=NAME_NORMALIZE,
        checks=({"rule": "max_length", "value": 120, "on_fail": "reject_record"},),
        spec=NO_SPEC,
    )
    b.add("first_name", "first_name", normalize=NAME_NORMALIZE[:3], checks=(), spec=NO_SPEC)
    b.add("birth_date", "birth_date", checks=(), spec={**NO_SPEC, "lo": -10_957, "span": 24_000})
    b.add(
        "email",
        "email",
        normalize=["trim", "lower"],
        checks=({"rule": "regex", "value": EMAIL_REGEX, "on_fail": "null_value"},),
        spec=NO_SPEC,
    )
    b.add(
        "country",
        "country",
        nullable=False,
        normalize=["trim", "upper"],
        checks=({"rule": "in_set", "value": list(COUNTRIES), "on_fail": "warn"},),
        spec={**NO_SPEC, "values": tuple(COUNTRIES)},
    )
    for i, kind in enumerate(kinds, start=5):
        b.add(f"{PREFIX[kind]}_{i:03d}", kind)
    return b.attrs


def build_plan(params: SynthParams) -> Plan:
    rng = np.random.default_rng([params.seed, 0xC0F1])
    attrs = _build_attrs(params, rng)
    sources = []
    for s in range(params.sources):
        cols = tuple((a.idx, col) for a in attrs for i, col in a.sources if i == s)
        sources.append(_source_spec(params, s, cols))
    plan = Plan(params=params, attrs=attrs, sources=sources)
    plan.config = build_config_dict(plan)
    return plan


@lru_cache(maxsize=4)
def plan_from_json(params_json: str) -> Plan:
    return build_plan(SynthParams.from_json(params_json))


def build_config_dict(plan: Plan, data_root: str | Path | None = None) -> dict[str, Any]:
    p = plan.params
    sources = []
    for s in plan.sources:
        location = s.location
        if s.kind == "parquet" and data_root is not None:
            location = (Path(data_root).resolve() / "live" / s.id / "*.parquet").as_posix()
        entry: dict[str, Any] = {"id": s.id, "kind": s.kind, "location": location}
        if s.dsn_env:
            entry["dsn_env"] = s.dsn_env
        entry.update(key=s.key_col, revision=s.rev_col, snapshot=s.snapshot)
        sources.append(entry)
    attributes = []
    for a in plan.attrs:
        item: dict[str, Any] = {"name": a.name, "type": a.type, "nullable": a.nullable}
        if a.normalize:
            item["normalize"] = list(a.normalize)
        item["sources"] = [{"source": plan.sources[i].id, "column": c} for i, c in a.sources]
        if a.strategy != "priority":
            item["strategy"] = a.strategy
        if a.checks:
            item["checks"] = [dict(c) for c in a.checks]
        attributes.append(item)
    matching = {
        "version": "synth-match-1",
        "blocking": [
            {
                "name": "ln_year",
                "parts": [
                    {"attr": "last_name", "transform": "prefix", "n": 4},
                    {"attr": "birth_date", "transform": "year"},
                ],
            },
            {"name": "email", "parts": [{"attr": "email"}]},
        ],
        "max_block_size": 500,
        "comparators": [
            {"attr": "last_name", "method": "levenshtein", "weight": 0.30},
            {"attr": "first_name", "method": "levenshtein", "weight": 0.25},
            {"attr": "birth_date", "method": "exact", "weight": 0.20},
            {"attr": "birth_date", "method": "date_year", "weight": 0.10},
            {"attr": "email", "method": "exact", "weight": 0.15},
        ],
        "contradictions": [{"attr": "birth_date"}],
        "accept": 0.95,
        "review": 0.80,
        "min_evidence": 0.5,
    }
    return {
        "referential": p.referential,
        "version": 1,
        "state": {"dsn_env": "MDM_STATE_DSN", "schema": f"mdm_{p.referential}"},
        "sources": sources,
        "attributes": attributes,
        "matching": matching,
        "publication": {"provenance": True},
        "runtime": {
            "profile": "large" if p.nodes >= 500_000 else "medium",
            "batch_rows": 50_000,
            "max_attempts": 3,
            "step_timeout_minutes": 120,
        },
    }


def build_config(params: SynthParams, data_root: str | Path | None = None) -> ReferentialConfig:
    plan = build_plan(params)
    return ReferentialConfig.model_validate(build_config_dict(plan, data_root))


def write_config(
    params: SynthParams, path: str | Path, data_root: str | Path | None = None
) -> ReferentialConfig:
    """Écrit le YAML et le valide en le rechargeant avec `engine.core.config.load_config`."""
    from engine.core.config import load_config

    plan = build_plan(params)
    text = yaml.safe_dump(
        build_config_dict(plan, data_root), sort_keys=False, allow_unicode=True, width=100
    )
    Path(path).write_text(text, encoding="utf-8")
    return load_config(path)
