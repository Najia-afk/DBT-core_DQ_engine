"""Connected components and staging id assignment over the affected set (numpy only)."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field

import numpy as np


def connected_components(n: int, u: np.ndarray, v: np.ndarray) -> np.ndarray:
    """Union-find by hooking + pointer jumping; the label of a component is its smallest index."""
    parent = np.arange(n, dtype=np.int64)
    if len(u) == 0:
        return parent
    u = np.asarray(u, dtype=np.int64)
    v = np.asarray(v, dtype=np.int64)
    while True:
        pu, pv = parent[u], parent[v]
        diff = pu != pv
        if not diff.any():
            return parent
        high = np.maximum(pu, pv)[diff]
        low = np.minimum(pu, pv)[diff]
        np.minimum.at(parent, high, low)
        while True:
            jumped = parent[parent]
            if np.array_equal(jumped, parent):
                break
            parent = jumped


@dataclass
class StagingPlan:
    """Result of the id assignment for the components of the affected set."""

    comp_sid: np.ndarray  # staging id of each component (component order = smallest node)
    new_ids: list[int] = field(default_factory=list)
    events: list[tuple[str, int, int | None]] = field(default_factory=list)
    retired: list[int] = field(default_factory=list)


def assign_staging_ids(
    comp_of_node: np.ndarray,
    n_comps: int,
    old_sid: np.ndarray,
    old_ids: np.ndarray,
    allocate: Callable[[int], list[int]],
) -> StagingPlan:
    """Reference semantics of staging id stability.

    `comp_of_node`: component index of each ACTIVE node of the affected set; `old_sid`: its
    current staging id (-1 if none); `old_ids`: every staging id touched; `allocate(k)` returns
    k fresh ids. A component keeps the smallest id among the stagings whose heir it is (the
    component holding most of their members, tie: smallest component); the other ids are merged
    into it. A component that is nobody's heir gets a new id: `split` when it holds members of
    an old staging (parent = the old staging with most members in it), else `create`.
    """
    plan = StagingPlan(comp_sid=np.full(n_comps, -1, dtype=np.int64))
    has_old = old_sid >= 0
    oc, os_ = comp_of_node[has_old], old_sid[has_old]
    heir_comp: dict[int, int] = {}
    parent_of: dict[int, int] = {}
    if len(oc):
        pairs = np.stack([oc, os_], axis=1)
        uniq, counts = np.unique(pairs, axis=0, return_counts=True)
        u_comp, u_sid = uniq[:, 0], uniq[:, 1]
        order = np.lexsort((u_comp, -counts, u_sid))  # heir: most members, then smallest component
        sids, comps = u_sid[order], u_comp[order]
        first = np.r_[True, sids[1:] != sids[:-1]]
        heir_comp = dict(zip(sids[first].tolist(), comps[first].tolist(), strict=True))
        order = np.lexsort((u_sid, -counts, u_comp))  # parent: most members, then smallest id
        sids, comps = u_sid[order], u_comp[order]
        first = np.r_[True, comps[1:] != comps[:-1]]
        parent_of = dict(zip(comps[first].tolist(), sids[first].tolist(), strict=True))
    heirs: dict[int, list[int]] = {}
    for sid, comp in sorted(heir_comp.items()):
        heirs.setdefault(comp, []).append(sid)
    fresh = [c for c in range(n_comps) if c not in heirs]
    ids = iter(allocate(len(fresh))) if fresh else iter(())
    for comp in range(n_comps):
        if comp in heirs:
            keep, *absorbed = heirs[comp]
            plan.comp_sid[comp] = keep
            plan.events += [("merge", keep, other) for other in absorbed]
            continue
        sid = next(ids)
        plan.comp_sid[comp] = sid
        plan.new_ids.append(sid)
        if comp in parent_of:
            plan.events.append(("split", parent_of[comp], sid))
        else:
            plan.events.append(("create", sid, None))
    kept = set(plan.comp_sid.tolist())
    plan.retired = sorted(set(old_ids.tolist()) - kept)
    return plan
