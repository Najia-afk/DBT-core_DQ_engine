"""Blocking keys and pair scoring (vectorised; Levenshtein with rapidfuzz)."""

from __future__ import annotations

import os

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
from rapidfuzz import process
from rapidfuzz.distance import Levenshtein

from engine.core.config import Matching, ReferentialConfig

from .transform import canon_text

EPS = 1e-9
WORKERS = int(os.environ.get("MDM_ETL_WORKERS", str(min(8, os.cpu_count() or 1))))
SEP = "\x1f"
BLOCK_SCHEMA = pa.schema([("node_id", pa.int64()), ("bname", pa.string()), ("bvalue", pa.string())])


def match_attributes(cfg: ReferentialConfig) -> list[str]:
    """Attributes read by blocking and scoring, in config order."""
    m = cfg.matching
    used = {p.attr for k in m.blocking for p in k.parts}
    used |= {c.attr for c in m.comparators} | {c.attr for c in m.contradictions}
    return [a.name for a in cfg.attributes if a.name in used]


def block_keys(cfg: ReferentialConfig, table: pa.Table) -> pa.Table:
    """(node_id, bname, bvalue) of the active nodes; a key with a null part is not generated."""
    pieces: list[pa.Table] = []
    for key in cfg.matching.blocking:
        texts: list[pa.Array] = []
        valid = np.ones(table.num_rows, dtype=bool)
        for part in key.parts:
            arr = table.column(part.attr).combine_chunks()
            valid &= pc.is_valid(arr).to_numpy(zero_copy_only=False)
            if part.transform == "prefix":
                text = pc.cast(pc.utf8_slice_codeunits(arr, 0, part.n), pa.large_string())
            elif part.transform == "year":
                text = pc.cast(pc.cast(pc.year(arr), pa.string()), pa.large_string())
            else:
                text = canon_text(arr, cfg.attribute(part.attr).type)
            texts.append(text)
        joined = pc.binary_join_element_wise(*texts, pa.scalar(SEP, pa.large_string()))
        mask = pa.array(valid)
        n = int(valid.sum())
        pieces.append(
            pa.table(
                {
                    "node_id": table.column("node_id").filter(mask),
                    "bname": pa.repeat(pa.scalar(key.name, pa.string()), n),
                    "bvalue": pc.cast(joined.filter(mask), pa.string()),
                },
                schema=BLOCK_SCHEMA,
            )
        )
    return pa.concat_tables(pieces) if pieces else BLOCK_SCHEMA.empty_table()


def _np(arr: pa.Array | pa.ChunkedArray) -> np.ndarray:
    return arr.to_numpy(zero_copy_only=False)


def score_pairs(
    matching: Matching, left: pa.Table, right: pa.Table
) -> tuple[np.ndarray, np.ndarray]:
    """(score with NaN for 'no decision', contradiction) of aligned left/right attribute rows."""
    n = left.num_rows
    total = np.zeros(n)
    active = np.zeros(n)
    for comp in matching.comparators:
        va = left.column(comp.attr).combine_chunks()
        vb = right.column(comp.attr).combine_chunks()
        valid = _np(pc.and_(pc.is_valid(va), pc.is_valid(vb)))
        if not valid.any():
            continue
        if comp.method == "exact":
            sim = _np(pc.fill_null(pc.equal(va, vb), False)).astype(np.float64)
        elif comp.method == "date_year":
            sim = _np(pc.fill_null(pc.equal(pc.year(va), pc.year(vb)), False)).astype(np.float64)
        else:
            sim = np.zeros(n)
            mask = pa.array(valid)
            sa, sb = va.filter(mask), vb.filter(mask)
            dist = np.asarray(
                process.cpdist(
                    sa.to_pylist(), sb.to_pylist(), scorer=Levenshtein.distance, workers=WORKERS
                ),
                dtype=np.float64,
            )
            longest = np.maximum(_np(pc.utf8_length(sa)), _np(pc.utf8_length(sb)))
            sim[valid] = 1.0 - dist / longest
        total += np.where(valid, comp.weight * sim, 0.0)
        active += np.where(valid, comp.weight, 0.0)
    contradiction = np.zeros(n, dtype=bool)
    for item in matching.contradictions:
        va = left.column(item.attr).combine_chunks()
        vb = right.column(item.attr).combine_chunks()
        both = _np(pc.and_(pc.is_valid(va), pc.is_valid(vb)))
        contradiction |= both & _np(pc.fill_null(pc.not_equal(va, vb), False))
    enough = (active > 0) & (active >= matching.min_evidence - EPS)
    score = np.full(n, np.nan)
    np.divide(total, active, out=score, where=enough)
    return score, contradiction
