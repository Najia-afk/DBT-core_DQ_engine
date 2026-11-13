"""Blocking keys and pair scoring (contract section 3), vectorised with pyarrow / numpy / rapidfuzz.

Candidate pairs come from the graph (blocks); this module only turns attribute values into
Block identities and scores pairs. Scores replicate `engine.core.reference.matching.score_pair`
operation for operation (same float additions in declared comparator order).
"""

from __future__ import annotations

import json

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
from rapidfuzz.distance import Levenshtein
from rapidfuzz.process import cpdist

from engine.core.config import Matching, ReferentialConfig

from .values import canonical_text_array

EPS = 1e-9


def block_attrs(cfg: ReferentialConfig) -> list[str]:
    seen: list[str] = []
    for key in cfg.matching.blocking:
        for part in key.parts:
            if part.attr not in seen:
                seen.append(part.attr)
    return seen


def scoring_attrs(cfg: ReferentialConfig) -> list[str]:
    seen: list[str] = []
    for item in (*cfg.matching.comparators, *cfg.matching.contradictions):
        if item.attr not in seen:
            seen.append(item.attr)
    return seen


def block_identities(cfg: ReferentialConfig, table: pa.Table) -> list[list[str]]:
    """Per row: the block identities (JSON `[key_name, part texts...]`) the node belongs to.

    A key with a null part is not generated. The texts are those of the reference
    `block_label`, so `label_of` renders the reference anomaly subject.
    """
    n = table.num_rows
    per_row: list[list[str]] = [[] for _ in range(n)]
    for key in cfg.matching.blocking:
        texts: list[pa.Array] = []
        for part in key.parts:
            arr = table[part.attr]
            if part.transform == "prefix":
                texts.append(pc.utf8_slice_codeunits(arr, 0, part.n))
            elif part.transform == "year":
                texts.append(pc.year(arr).cast(pa.string()))
            else:
                texts.append(canonical_text_array(cfg.attribute(part.attr).type, arr))
        columns = [t.to_pylist() for t in texts]
        for i, parts in enumerate(zip(*columns, strict=True)):
            if None not in parts:
                per_row[i].append(json.dumps([key.name, *parts], ensure_ascii=False))
    return per_row


def label_of(identity: str) -> str:
    name, *texts = json.loads(identity)
    return f"{name}=" + "|".join(texts)


def score_pairs(
    matching: Matching, left: dict[str, pa.Array], right: dict[str, pa.Array], n: int
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """(score, has_score, contradiction, eligible) per pair; `left/right` aligned per pair."""
    total = np.zeros(n)
    active = np.zeros(n)
    for comp in matching.comparators:
        a, b = left[comp.attr], right[comp.attr]
        valid = pc.and_(pc.is_valid(a), pc.is_valid(b)).to_numpy(zero_copy_only=False)
        s = np.zeros(n)
        if comp.method == "exact":
            s = pc.fill_null(pc.equal(a, b), False).to_numpy(zero_copy_only=False).astype(float)
        elif comp.method == "date_year":
            same = pc.equal(pc.year(a), pc.year(b))
            s = pc.fill_null(same, False).to_numpy(zero_copy_only=False).astype(float)
        else:
            idx = np.flatnonzero(valid)
            if len(idx):
                ia = pa.array(idx)
                sa, sb = a.take(ia), b.take(ia)
                dist = cpdist(
                    sa.to_pylist(), sb.to_pylist(), scorer=Levenshtein.distance, workers=-1
                )
                longest = np.maximum(
                    pc.utf8_length(sa).to_numpy(zero_copy_only=False),
                    pc.utf8_length(sb).to_numpy(zero_copy_only=False),
                )
                s[idx] = 1.0 - dist / longest
        total += np.where(valid, comp.weight * s, 0.0)
        active += np.where(valid, comp.weight, 0.0)
    contradiction = np.zeros(n, dtype=bool)
    for c in matching.contradictions:
        a, b = left[c.attr], right[c.attr]
        both = pc.and_(pc.is_valid(a), pc.is_valid(b)).to_numpy(zero_copy_only=False)
        differ = pc.fill_null(pc.not_equal(a, b), False).to_numpy(zero_copy_only=False)
        contradiction |= both & differ
    has = (active != 0) & ~(active < matching.min_evidence - EPS)
    score = np.where(has, total / np.where(active == 0, 1.0, active), np.nan)
    eligible = has & (score >= matching.accept - EPS) & ~contradiction
    return score, has, contradiction, eligible
