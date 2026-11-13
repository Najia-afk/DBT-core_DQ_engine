"""Survivorship of the stagings (vectorised) and golden comparison with the published state."""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc

from engine.core.config import ReferentialConfig

from .transform import canon_text

_INF = np.iinfo(np.int64).max


@dataclass
class Winners:
    sids: np.ndarray  # staging ids, ascending
    values: dict[str, pa.Array]
    sources: dict[str, pa.Array]


def survive(cfg: ReferentialConfig, members: pa.Table, sid_of_row: np.ndarray) -> Winners:
    """Best value of every attribute for every staging of a chunk.

    `members`: `source_id`, `revision` and one column per attribute (active members of the
    stagings of the chunk); `sid_of_row`: staging id of each member. Candidate order is the
    contract's: first declared source holding a value (strategy `priority`) then greatest
    revision (null = oldest) then smallest (source_id, source_key); `most_recent` ignores the
    source order. The key `(source position, tie-break position)` is reduced per staging.
    """
    m = members.num_rows
    order = np.argsort(sid_of_row, kind="stable")
    sid_sorted = sid_of_row[order]
    starts = np.flatnonzero(np.r_[True, sid_sorted[1:] != sid_sorted[:-1]])
    sids = sid_sorted[starts]
    best_first = pc.sort_indices(
        members,
        sort_keys=[
            ("revision", "descending"),
            ("source_id", "ascending"),
            ("source_key", "ascending"),
        ],
    ).to_numpy()
    tie = np.empty(m, dtype=np.int64)
    tie[best_first] = np.arange(m, dtype=np.int64)
    row_of_tie = best_first.astype(np.int64)
    source_ids = members.column("source_id").combine_chunks()
    codes = pc.index_in(
        source_ids, value_set=pa.array([s.id for s in cfg.sources], pa.string())
    ).to_numpy(zero_copy_only=False)
    values: dict[str, pa.Array] = {}
    sources: dict[str, pa.Array] = {}
    for attr in cfg.attributes:
        column = members.column(attr.name).combine_chunks()
        valid = pc.is_valid(column).to_numpy(zero_copy_only=False)
        position = np.full(len(cfg.sources), _INF // (m + 1), dtype=np.int64)
        for i, sc in enumerate(attr.sources):
            position[[s.id for s in cfg.sources].index(sc.source)] = i
        pos = position[codes]
        mapped = pos < _INF // (m + 1)
        if attr.strategy == "priority":
            key = pos * m + tie
        else:
            key = tie
        key = np.where(valid & mapped, key, _INF)
        best = np.minimum.reduceat(key[order], starts)
        has = best < _INF
        tie_pos = best % m if attr.strategy == "priority" else best
        winner = row_of_tie[np.where(has, tie_pos, 0)]
        take = pa.array(winner, mask=~has)
        values[attr.name] = column.take(take)
        sources[attr.name] = source_ids.take(take)
    return Winners(sids, values, sources)


def changed_mask(old: pa.Array, new: pa.Array) -> np.ndarray:
    """Null-aware inequality of two aligned arrays of the same type."""
    old_null = pc.is_null(old).to_numpy(zero_copy_only=False)
    new_null = pc.is_null(new).to_numpy(zero_copy_only=False)
    equal = pc.fill_null(pc.equal(old, new), False).to_numpy(zero_copy_only=False)
    return (old_null != new_null) | (~old_null & ~new_null & ~equal)


def delta_rows(
    run_id: str,
    attr_name: str,
    attr_type: str,
    golden_ids: np.ndarray,
    old: pa.Array,
    new: pa.Array,
    winning: pa.Array,
    mask: np.ndarray,
) -> pa.Table:
    idx = np.flatnonzero(mask)
    take = pa.array(idx)
    n = len(idx)
    return pa.table(
        {
            "run_id": pa.repeat(pa.scalar(run_id, pa.string()), n),
            "golden_id": pa.array(golden_ids[idx], pa.int64()),
            "attribute": pa.repeat(pa.scalar(attr_name, pa.string()), n),
            "old_value": pc.cast(canon_text(old.take(take), attr_type), pa.string()),
            "new_value": pc.cast(canon_text(new.take(take), attr_type), pa.string()),
            "winning_source": winning.take(take),
        }
    )
