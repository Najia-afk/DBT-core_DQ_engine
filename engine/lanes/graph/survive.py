"""Survivorship (contract section 2, rule 5), vectorised over all members of the affected stagings.

Semantics of `engine.core.reference.survive`: `priority` = first declared source having a non-null
member value (ties inside that source: greatest revision, null oldest, then smallest node);
`most_recent` = among members of mapped sources with a value, greatest revision then smallest node.
"""

from __future__ import annotations

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc

from engine.core.config import ReferentialConfig


def survive(
    cfg: ReferentialConfig, members: pa.Table, ordinal: np.ndarray, n_stagings: int
) -> tuple[dict[str, pa.Array], dict[str, pa.Array]]:
    """Winning value and source per staging ordinal for every attribute.

    `members` has `_source_id`, `_source_key`, `_revision` and one column per attribute;
    `ordinal[i]` is the 0-based staging ordinal of member row i.
    """
    m = members.num_rows
    sid, key = members["_source_id"], members["_source_key"]
    order = pc.sort_indices(
        pa.table({"s": sid, "k": key}), sort_keys=[("s", "ascending"), ("k", "ascending")]
    ).to_numpy()
    node_rank = np.empty(m, dtype=np.int64)
    node_rank[order] = np.arange(m)
    revision = members["_revision"].combine_chunks()
    has_rev = pc.is_valid(revision).to_numpy(zero_copy_only=False)
    rev_us = pc.fill_null(revision.cast(pa.int64()), 0).to_numpy(zero_copy_only=False)
    no_rev = (~has_rev).astype(np.int64)
    neg_rev = -rev_us
    values: dict[str, pa.Array] = {}
    sources: dict[str, pa.Array] = {}
    sid_arr = sid.combine_chunks()
    for attr in cfg.attributes:
        col = members[attr.name].combine_chunks()
        valid = pc.is_valid(col).to_numpy(zero_copy_only=False)
        mapped = [sc.source for sc in attr.sources]
        pos = pc.index_in(sid_arr, value_set=pa.array(mapped, type=pa.string()))
        rank = pc.fill_null(pos, -1).to_numpy(zero_copy_only=False).astype(np.int64)
        cand = np.flatnonzero(valid & (rank >= 0))
        src_rank = rank[cand] if attr.strategy == "priority" else np.zeros(len(cand), np.int64)
        picked = np.full(n_stagings, -1, dtype=np.int64)
        if len(cand):
            sel = np.lexsort(
                (node_rank[cand], neg_rev[cand], no_rev[cand], src_rank, ordinal[cand])
            )
            ordered = cand[sel]
            ords = ordinal[ordered]
            first = np.ones(len(ordered), dtype=bool)
            first[1:] = ords[1:] != ords[:-1]
            picked[ords[first]] = ordered[first]
        take = pa.array(np.where(picked < 0, 0, picked), mask=picked < 0, type=pa.int64())
        values[attr.name] = col.take(take) if m else pa.nulls(n_stagings, col.type)
        sources[attr.name] = sid_arr.take(take) if m else pa.nulls(n_stagings, pa.string())
    return values, sources
