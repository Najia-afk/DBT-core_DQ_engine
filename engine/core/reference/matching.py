"""Blocking, comparators, pair scoring and decisions (contract section 3)."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from engine.core.canonical import canonical_text
from engine.core.config import Matching, ReferentialConfig

EPS = 1e-9
BlockValue = tuple[Any, ...]


def levenshtein_distance(a: str, b: str) -> int:
    if a == b:
        return 0
    previous = list(range(len(b) + 1))
    for i, ca in enumerate(a, start=1):
        current = [i]
        for j, cb in enumerate(b, start=1):
            current.append(min(previous[j] + 1, current[j - 1] + 1, previous[j - 1] + (ca != cb)))
        previous = current
    return previous[-1]


def compare_values(method: str, a: Any, b: Any) -> float:
    if method == "exact":
        return 1.0 if a == b else 0.0
    if method == "levenshtein":
        return 1.0 - levenshtein_distance(a, b) / max(len(a), len(b))
    if method == "date_year":
        return 1.0 if a.year == b.year else 0.0
    raise ValueError(f"unknown comparator {method!r}")


def block_values(config: ReferentialConfig, values: Mapping[str, Any]) -> dict[str, BlockValue]:
    """Blocking key name -> block value; a key with a null part is not generated."""
    result: dict[str, BlockValue] = {}
    for key in config.matching.blocking:
        parts: list[Any] = []
        for part in key.parts:
            value = values.get(part.attr)
            if value is None:
                break
            if part.transform == "prefix":
                value = value[: part.n]
            elif part.transform == "year":
                value = value.year
            parts.append(value)
        else:
            result[key.name] = tuple(parts)
    return result


def block_label(config: ReferentialConfig, name: str, value: BlockValue) -> str:
    key = next(k for k in config.matching.blocking if k.name == name)
    texts = []
    for part, item in zip(key.parts, value, strict=True):
        if part.transform == "year":
            texts.append(str(item))
        elif part.transform == "prefix":
            texts.append(item)
        else:
            texts.append(canonical_text(item, config.attribute(part.attr).type))
    return f"{name}=" + "|".join(texts)


def score_pair(
    matching: Matching, a: Mapping[str, Any], b: Mapping[str, Any]
) -> tuple[float | None, bool]:
    """(score or None when evidence < min_evidence, contradiction)."""
    active = 0.0
    total = 0.0
    for comp in matching.comparators:
        va, vb = a.get(comp.attr), b.get(comp.attr)
        if va is None or vb is None:
            continue
        total += comp.weight * compare_values(comp.method, va, vb)
        active += comp.weight
    contradiction = any(
        a.get(c.attr) is not None and b.get(c.attr) is not None and a[c.attr] != b[c.attr]
        for c in matching.contradictions
    )
    if active == 0 or active < matching.min_evidence - EPS:
        return None, contradiction
    return total / active, contradiction


def is_eligible(matching: Matching, score: float | None, contradiction: bool) -> bool:
    return score is not None and score >= matching.accept - EPS and not contradiction


def decide(
    matching: Matching,
    score: float | None,
    contradiction: bool,
    eligible_left: int,
    eligible_right: int,
) -> tuple[str, str]:
    """(decision, reason) of a pair.

    `eligible_left` = number of eligible pairs from the left node to nodes of the RIGHT node's
    source (and vice versa for `eligible_right`): a node is ambiguous only when it has several
    eligible candidates in the same source.
    """
    if score is None:
        return "none", ""
    if is_eligible(matching, score, contradiction):
        if eligible_left == 1 and eligible_right == 1:
            return "match", ""
        return "review", "multiple_candidates"
    if score >= matching.review - EPS:
        return "review", "contradiction" if contradiction else "score_band"
    return "none", ""
