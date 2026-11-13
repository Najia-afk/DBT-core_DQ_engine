"""Lint : ce que Pydantic ne peut pas dire (config valide mais probablement mal pensée)."""

from __future__ import annotations

import yaml

from engine.cli import normalizers
from engine.cli.issues import Issue, fmt_path
from engine.cli.yamlio import line_of
from engine.core.config import ReferentialConfig

# Attributs dont la valeur change dans le temps : `priority` y fige la valeur d'une vieille source.
VOLATILE_HINTS = (
    "email", "mail", "phone", "tel", "mobile", "address", "adresse", "street", "city", "zip",
    "postal", "status", "iban", "job", "title", "employer", "name_used", "preferred",
)  # fmt: skip
MANY_ATTRIBUTES = 200


def roles(cfg: ReferentialConfig) -> dict[str, list[str]]:
    out: dict[str, list[str]] = {}
    m = cfg.matching
    for key in m.blocking:
        for part in key.parts:
            out.setdefault(part.attr, []).append(f"blocking:{key.name}")
    for comp in m.comparators:
        out.setdefault(comp.attr, []).append(f"comparator:{comp.method}")
    for c in m.contradictions:
        out.setdefault(c.attr, []).append("contradiction")
    return out


def lint(cfg: ReferentialConfig, root: yaml.Node | None = None) -> list[Issue]:
    found: list[tuple[str, tuple, str, str]] = []  # (niveau, loc, code, message)
    n_sources = len(cfg.sources)
    all_ids = {s.id for s in cfg.sources}

    def fed(attr) -> set[str]:
        return {sc.source for sc in attr.sources}

    for i, a in enumerate(cfg.attributes):
        providers = fed(a)
        if not a.nullable and providers != all_ids:
            missing = sorted(all_ids - providers)
            only = " (it is fed by a single source)" if len(providers) == 1 else ""
            found.append((
                "warning", ("attributes", i, "sources"), "nonnull-partial-coverage",
                f"'{a.name}' is nullable: false but not fed by {', '.join(missing)}{only}. "
                f"Records of {', '.join(missing)} carry no value for it; a golden whose "
                "contributing sources are all null/absent for it is BLOCKED (not published, "
                "previous version kept as stale)."
            ))  # fmt: skip
        volatile = any(h in a.name.lower() for h in VOLATILE_HINTS)
        with_revision = all(cfg.source(sc.source).revision for sc in a.sources)
        if a.strategy == "priority" and len(a.sources) >= 2 and volatile and with_revision:
            found.append((
                "warning", ("attributes", i, "strategy"), "maybe-most-recent",
                f"'{a.name}' looks like data that changes over time but uses strategy: priority "
                f"(a stale value from '{a.sources[0].source}' always beats newer ones). "
                "If the freshest value should win, use strategy: most_recent."
            ))  # fmt: skip
        for j, chk in enumerate(a.checks):
            loc = ("attributes", i, "checks", j)
            if chk.on_fail == "null_value" and not a.nullable:
                found.append((
                    "warning", loc + ("on_fail",), "null-on-nonnullable",
                    f"on_fail: null_value on non-nullable '{a.name}': an invalid value becomes "
                    "null and can block the golden if no other source provides a value."
                ))  # fmt: skip
            if chk.rule == "in_set" and a.normalize and isinstance(chk.value, list):
                bad = [v for v in chk.value if normalizers.apply(list(a.normalize), v) != v]
                if bad:
                    shown = ", ".join(
                        f"'{v}' → '{normalizers.apply(list(a.normalize), v)}'" for v in bad[:3]
                    )
                    found.append((
                        "warning", loc + ("value",), "in-set-not-normalized",
                        f"in_set values are compared after normalisation "
                        f"({', '.join(a.normalize)}) but some listed values would not survive "
                        f"it: {shown}. They can never match."
                    ))  # fmt: skip

    for i, s in enumerate(cfg.sources):
        n_attrs = len(cfg.mapping(s.id))
        if n_attrs < 2:
            found.append((
                "warning", ("sources", i), "source-few-attributes",
                f"source '{s.id}' feeds only {n_attrs} attribute: almost nothing to match or "
                "enrich with. Is the mapping complete?"
            ))  # fmt: skip
        if not s.revision:
            found.append((
                "warning", ("sources", i), "source-no-revision",
                f"source '{s.id}' has no revision column: duplicate keys in a batch are resolved "
                "by file/row order, and most_recent cannot use this source."
            ))  # fmt: skip
        if s.snapshot == "delta" and s.full_snapshot_min_ratio != 0.9:
            found.append((
                "info", ("sources", i, "full_snapshot_min_ratio"), "ratio-ignored",
                f"full_snapshot_min_ratio only applies to snapshot: full ('{s.id}' is delta)."
            ))  # fmt: skip

    m = cfg.matching
    for i, comp in enumerate(m.comparators):
        attr = cfg.attribute(comp.attr)
        providers = fed(attr)
        if n_sources > 1 and len(providers) == 1:
            found.append((
                "warning", ("matching", "comparators", i, "attr"), "comparator-single-source",
                f"'{comp.attr}' is only fed by '{next(iter(providers))}': records from the "
                f"{n_sources - 1} other source(s) have no value, so this comparator is ignored "
                "for them."
            ))  # fmt: skip
    if all(cfg.attribute(c.attr).nullable for c in m.comparators):
        found.append((
            "warning", ("matching", "comparators"), "all-comparators-nullable",
            "every compared attribute is nullable: a pair where they are all null has no "
            f"evidence (active weight 0 < min_evidence {m.min_evidence}) and gets no decision."
        ))  # fmt: skip

    blocking_all_nullable = True
    for i, key in enumerate(m.blocking):
        key_nullable = False
        for j, part in enumerate(key.parts):
            attr = cfg.attribute(part.attr)
            if not attr.nullable:
                continue
            key_nullable = True
            loc = ("matching", "blocking", i, "parts", j, "attr")
            missing = all_ids - fed(attr)
            if missing:
                found.append((
                    "warning", loc, "blocking-sparse",
                    f"blocking key '{key.name}' uses nullable '{part.attr}' which is not fed by "
                    f"{', '.join(sorted(missing))}: those records cannot produce this key."
                ))  # fmt: skip
            else:
                found.append((
                    "info", loc, "blocking-nullable",
                    f"blocking key '{key.name}' uses nullable '{part.attr}': records where it is "
                    "null cannot produce this key. Measure the null rate with check-source."
                ))  # fmt: skip
        blocking_all_nullable &= key_nullable
    if blocking_all_nullable:
        found.append((
            "warning", ("matching", "blocking"), "blocking-all-nullable",
            "every blocking key depends on a nullable attribute: a record with nulls everywhere "
            "has no candidate at all. Prefer at least one key on a non-nullable attribute."
        ))  # fmt: skip

    if len(cfg.attributes) >= MANY_ATTRIBUTES:
        found.append((
            "info", ("attributes",), "many-attributes",
            f"{len(cfg.attributes)} attributes: ingestion hashes and normalises every mapped "
            "column of every row, so cost grows linearly with the attribute count. Drop "
            "attributes nobody uses for matching or publication."
        ))  # fmt: skip

    order = {"warning": 0, "info": 1}
    found.sort(key=lambda f: order[f[0]])
    return [
        Issue(level, code, fmt_path(loc), msg, line_of(root, loc))
        for level, loc, code, msg in found
    ]
