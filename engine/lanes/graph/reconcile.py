"""RECONCILE (contract section 2, steps 2 and 3) on the graph, driven by the delta only.

Graph model (AGE): Record -IN_BLOCK-> Block, Record -MATCH- Record (every candidate pair, with
score/decision), Record -REVIEW- Record, Record -MEMBER_OF-> Staging -PUBLISHED_AS-> Golden.

What runs where (also listed in README "not graph"):
* openCypher: block membership/size, candidate pairs (2-hop through Block), eligible-candidate
  counts, adjacency of accepted MATCH edges, staging membership, affected set expansion.
* SQL on the AGE label tables: bulk COPY of vertices/edges, deletes/updates by id.
* Python/Arrow: scoring, decision rule, union-find labelling, heirs/merge/split ids,
  survivorship, golden diff.
Everything of one reconcile is one PostgreSQL transaction (atomic publication).
"""

from __future__ import annotations

import json
import time
from collections import Counter, defaultdict
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc

from engine.core.canonical import arrow_type, canonical_run_id
from engine.core.reference.matching import decide

from . import values as V
from .db import ENTRY_MASK, LABEL_BITS, Ctx, LaneError, connect, q
from .ingest import copy_arrow, fail_point
from .matching import (
    block_attrs,
    block_identities,
    label_of,
    score_pairs,
    scoring_attrs,
)
from .survive import survive

PAIR_KINDS = ("int", "int", "int", "float", "bool", "bool", "str", "str", "str")
PAIR_EDGE = "m:MATCH"
PAIR_RETURN = "RETURN id(m), start_id(m), end_id(m), m.s, m.c, m.e, m.d, m.r, m.run"


@dataclass
class PairRec:
    eid: int | None
    score: float | None
    contra: bool
    elig: bool
    d: str
    r: str | None
    run: str

    def sig(self) -> tuple[Any, ...]:
        return (self.d, self.r or "", self.score, self.contra)


class Nodes:
    """(source_id, source_key, active) of record vertices, fetched on demand."""

    def __init__(self, ctx: Ctx) -> None:
        self.ctx = ctx
        self.info: dict[int, tuple[str, str, bool]] = {}

    def need(self, vids: Any) -> None:
        missing = [v for v in set(vids) if v not in self.info]
        for start in range(0, len(missing), 100_000):
            part = missing[start : start + 100_000]
            rows = self.ctx.conn.execute(
                f"SELECT _vid, _source_id, _source_key, _status = 'active'"
                f" FROM {self.ctx.t('source_record')} WHERE _vid = ANY(%s::bigint[])",
                (part,),
            ).fetchall()
            for vid, sid, key, active in rows:
                self.info[vid] = (sid, key, active)
        unknown = [v for v in missing if v not in self.info]
        if unknown:
            raise LaneError(f"graph vertices without source_record: {unknown[:5]}")

    def key(self, vid: int) -> tuple[str, str]:
        info = self.info[vid]
        return (info[0], info[1])

    def active(self, vid: int) -> bool:
        return self.info[vid][2]


def fetch_table(
    ctx: Ctx, sql: str, params: tuple, names: list[str], types: list[pa.DataType]
) -> pa.Table:
    rows = ctx.conn.execute(sql, params).fetchall()
    columns = list(zip(*rows, strict=True)) if rows else [()] * len(names)
    return pa.table(
        {n: pa.array(list(c), type=t) for n, c, t in zip(names, columns, types, strict=True)}
    )


def fetch_attrs(
    ctx: Ctx, vids: list[int], attrs: list[str], extra: tuple[str, ...] = ()
) -> pa.Table:
    """Typed payload of records (sorted by vid), columns `_vid`, `extra`..., attributes."""
    cfg = ctx.cfg
    types = {a.name: arrow_type(a.type) for a in cfg.attributes}
    extra_types = {
        "_source_id": pa.string(),
        "_source_key": pa.string(),
        "_revision": pa.timestamp("us"),
    }
    names = ["_vid", *extra, *attrs]
    arrow = [pa.int64(), *[extra_types[e] for e in extra], *[types[a] for a in attrs]]
    if not vids:
        return pa.table({n: pa.array([], type=t) for n, t in zip(names, arrow, strict=True)})
    cols = ", ".join(q(n) for n in names)
    return fetch_table(
        ctx,
        f"SELECT {cols} FROM {ctx.t('source_record')} WHERE _vid = ANY(%s::bigint[]) ORDER BY _vid",
        (vids,),
        names,
        arrow,
    )


@contextmanager
def phase(ctx: Ctx, name: str):
    t0 = time.perf_counter()
    try:
        yield
    finally:
        ctx.timings[name] = round(ctx.timings.get(name, 0.0) + time.perf_counter() - t0, 4)


def table_empty(ctx: Ctx, label: str) -> bool:
    return ctx.conn.execute(f"SELECT NOT EXISTS (SELECT 1 FROM {ctx.lt(label)})").fetchone()[0]


def pair_json(
    score: float | None, contra: bool, elig: bool, d: str, r: str | None, run: str
) -> str:
    s = "null" if score is None else repr(float(score))
    reason = "" if not r else f',"r":"{r}"'
    return (
        f'{{"s":{s},"c":{"true" if contra else "false"},"e":{"true" if elig else "false"},'
        f'"d":"{d}"{reason},"run":"{run}"}}'
    )


def review_json(score: float | None, r: str, run: str) -> str:
    s = "null" if score is None else repr(float(score))
    return f'{{"s":{s},"r":"{r}","run":"{run}"}}'


# --------------------------------------------------------------------------- reconcile


def reconcile(cfg: Any, run_id: str, dsn: str, cc_mode: str = "hop") -> dict[str, Any]:
    run_id = canonical_run_id(run_id)
    conn = connect(dsn)
    ctx = Ctx(cfg, conn)
    started = datetime.now(UTC)
    try:
        ctx.load_labels()
        with conn.transaction():
            ctx.lock(exclusive=True)
            prior = ctx.done_counts(run_id, "reconcile")
            if prior is not None:
                return _summary(run_id, prior, {}, replayed=True)
            t0 = time.perf_counter()
            counts = _reconcile_tx(ctx, run_id, cc_mode)
            counts.update({k: v for k, v in sorted(ctx.counters.items())})
            ctx.timings["total"] = round(time.perf_counter() - t0, 4)
            ctx.log_step(run_id, "reconcile", "done", started, counts)
            fail_point("reconcile_before_commit")
        return _summary(run_id, counts, ctx.timings)
    finally:
        conn.close()


def _summary(run_id: str, counts: dict, timings: dict, *, replayed: bool = False) -> dict[str, Any]:
    return {
        "lane": "graph",
        "command": "reconcile",
        "run_id": run_id,
        "status": "ok",
        "replayed": replayed,
        "counts": counts,
        "seconds": timings,
    }


def _reconcile_tx(ctx: Ctx, run_id: str, cc_mode: str) -> dict[str, Any]:
    delta = sorted(r[0] for r in ctx.conn.execute(f"SELECT vid FROM {ctx.t('pending')}").fetchall())
    counts: dict[str, Any] = {"delta_nodes": len(delta)}
    if not delta:
        return counts
    nodes = Nodes(ctx)
    with phase(ctx, "index"):
        nodes.need(delta)
        rescore = update_index(ctx, run_id, delta, nodes)
    fail_point("reconcile_after_index")
    counts["rescored_nodes"] = len(rescore)
    with phase(ctx, "pairs"):
        removed, created, changed = rebuild_pairs(ctx, run_id, rescore, nodes)
    counts["pairs"] = len(created)
    counts["pairs_removed"] = len(removed)
    fail_point("reconcile_after_pairs")
    with phase(ctx, "decide"):
        touched = decide_pairs(ctx, run_id, removed, created, changed, rescore, nodes)
    fail_point("reconcile_after_decisions")
    with phase(ctx, "recluster"):
        kept = recluster(ctx, run_id, delta, touched, nodes, counts, cc_mode)
    fail_point("reconcile_after_recluster")
    with phase(ctx, "survive"):
        publish(ctx, run_id, kept, counts)
    fail_point("reconcile_after_survive")
    ctx.temp_ids("_done", delta)
    ctx.conn.execute(
        f"DELETE FROM {ctx.t('pending')} p USING _done d WHERE p.vid = d.v::text::bigint"
    )
    return counts


# ------------------------------------------------------------------- 1. blocking index


def lookup_blocks(ctx: Ctx, identities: list[str]) -> dict[str, int]:
    if not identities:
        return {}
    ctx.conn.execute("DROP TABLE IF EXISTS _bk")
    ctx.conn.execute("CREATE TEMP TABLE _bk (bk text) ON COMMIT DROP")
    with ctx.conn.cursor() as cur, cur.copy("COPY _bk FROM STDIN") as cp:
        for ident in identities:
            cp.write_row((ident,))
    access = """ag_catalog.agtype_access_operator(b.properties, '"bk"'::agtype)"""
    rows = ctx.conn.execute(
        f"SELECT ({access})::text::jsonb #>> '{{}}', b.id::text::bigint FROM {ctx.lt('Block')} b"
        f" JOIN _bk t ON {access} = to_jsonb(t.bk)::text::agtype"
    ).fetchall()
    return {bk: vid for bk, vid in rows}


def update_index(ctx: Ctx, run_id: str, delta: list[int], nodes: Nodes) -> set[int]:
    cfg, limit = ctx.cfg, ctx.cfg.matching.max_block_size
    active_delta = [v for v in delta if nodes.active(v)]
    table = fetch_attrs(ctx, active_delta, block_attrs(cfg))
    idents = block_identities(cfg, table)
    new_ident = {v: lst for v, lst in zip(table["_vid"].to_pylist(), idents, strict=True) if lst}
    all_idents = sorted({b for lst in new_ident.values() for b in lst})
    block_vid = lookup_blocks(ctx, all_idents)
    missing = [b for b in all_idents if b not in block_vid]
    created_blocks: set[int] = set()
    if missing:
        vids = ctx.alloc("Block", len(missing))
        props = [
            json.dumps(
                {"v": int(v), "bk": b, "o": False}, ensure_ascii=False, separators=(",", ":")
            )
            for v, b in zip(vids, missing, strict=True)
        ]
        ctx.copy_vertices("Block", vids, props)
        for v, b in zip(vids.tolist(), missing, strict=True):
            block_vid[b] = v
            created_blocks.add(v)
    ident_of = {v: b for b, v in block_vid.items()}

    old: dict[int, set[int]] = defaultdict(set)
    if not table_empty(ctx, "IN_BLOCK"):
        rows = ctx.cypher_in(
            "MATCH (r:Record)-[:IN_BLOCK]->(b:Block) WHERE r.v IN @IDS@ RETURN r.v, b.v",
            delta,
            ("int", "int"),
        )
        for r, b in rows:
            old[r].add(b)
    new = {v: {block_vid[b] for b in new_ident.get(v, ())} for v in delta}
    touched: set[int] = set()
    entered: set[int] = set()
    for v in delta:
        touched |= old[v] | new[v]
        entered |= new[v]
    before = {b: 0 for b in touched}
    existing = [b for b in touched if b not in created_blocks]
    if existing and not table_empty(ctx, "IN_BLOCK"):
        for b, n in ctx.cypher_in(
            "MATCH (b:Block)<-[:IN_BLOCK]-(x:Record) WHERE b.v IN @IDS@ RETURN b.v, count(x)",
            existing,
            ("int", "int"),
        ):
            before[b] = n
    after = dict(before)
    remove = [(v, b) for v in delta for b in old[v] - new[v]]
    add = [(v, b) for v in delta for b in new[v] - old[v]]
    for _, b in remove:
        after[b] -= 1
    for _, b in add:
        after[b] += 1
    ctx.delete_edges_by_pair("IN_BLOCK", remove)
    if add:
        ctx.copy_edges(
            "IN_BLOCK",
            np.array([a for a, _ in add], dtype=np.int64),
            np.array([b for _, b in add], dtype=np.int64),
            ["{}"] * len(add),
        )
    flipped = {b for b in touched if (before[b] > limit) != (after[b] > limit)}
    if flipped:
        ctx.temp_ids("_flip", [(b, after[b] > limit) for b in flipped], "v graphid, o boolean")
        ctx.conn.execute(
            f"UPDATE {ctx.lt('Block')} k SET properties = k.properties ||"
            " jsonb_build_object('o', f.o)::text::agtype FROM _flip f WHERE k.id = f.v"
        )
    rescore = set(delta)
    if flipped:
        rows = ctx.cypher_in(
            "MATCH (b:Block)<-[:IN_BLOCK]-(x:Record) WHERE b.v IN @IDS@ RETURN x.v",
            sorted(flipped),
            ("int",),
        )
        rescore |= {r[0] for r in rows}
        ctx.count("flipped_blocks", len(flipped))
    oversized = [b for b in entered | flipped if after[b] > limit]
    unknown = [b for b in oversized if b not in ident_of]
    if unknown:
        for b, ident in ctx.cypher_in(
            "MATCH (b:Block) WHERE b.v IN @IDS@ RETURN b.v, b.bk", unknown, ("int", "str")
        ):
            ident_of[b] = ident
    rows = sorted((label_of(ident_of[b]), after[b]) for b in oversized)
    if rows:
        with ctx.conn.cursor() as cur, cur.copy(f"COPY {ctx.t('anomaly')} FROM STDIN") as cp:
            for label, size in rows:
                cp.write_row((run_id, "oversized_block", label, f"size={size} max={limit}"))
    ctx.count("blocks_touched", len(touched))
    ctx.count("oversized_blocks_logged", len(rows))
    return rescore


# --------------------------------------------------------------------- 2-3. pairs


def _order_pairs(nodes: Nodes, a: np.ndarray, b: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """Orient pairs (smaller `(source_id, source_key)` first) and drop duplicates."""
    if len(a) == 0:
        return a, b
    u = np.unique(np.concatenate([a, b]))
    nodes.need(u.tolist())
    order = sorted(range(len(u)), key=lambda i: nodes.key(int(u[i])))
    rank = np.empty(len(u), dtype=np.int64)
    rank[order] = np.arange(len(u))
    ia, ib = np.searchsorted(u, a), np.searchsorted(u, b)
    swap = rank[ia] > rank[ib]
    lo = np.where(swap, ib, ia)
    hi = np.where(swap, ia, ib)
    code = lo.astype(np.int64) * len(u) + hi
    _, first = np.unique(code, return_index=True)
    return u[lo[first]], u[hi[first]]


def read_pairs(ctx: Ctx, vids: list[int]) -> dict[tuple[int, int], PairRec]:
    out: dict[tuple[int, int], PairRec] = {}
    if not vids or table_empty(ctx, "MATCH"):
        return out
    for eid, s, e, score, c, el, d, r, run in ctx.cypher_both(
        PAIR_EDGE, PAIR_RETURN, vids, PAIR_KINDS
    ):
        out[(s, e)] = PairRec(eid, score, c, el, d, r, run)
    return out


def rebuild_pairs(
    ctx: Ctx, run_id: str, rescore: set[int], nodes: Nodes
) -> tuple[dict[tuple[int, int], PairRec], dict[tuple[int, int], PairRec], set[int]]:
    cfg = ctx.cfg
    matching = cfg.matching
    r_sorted = sorted(rescore)
    removed = read_pairs(ctx, r_sorted)
    changed = set(rescore)
    for s, e in removed:
        changed.add(s)
        changed.add(e)
    ctx.delete_edges_by_ids("MATCH", [p.eid for p in removed.values()])
    ctx.delete_edges_by_pair("REVIEW", [k for k, p in removed.items() if p.d == "review"])
    ctx.count("pairs_dropped", len(removed))

    rows = ctx.cypher_in(
        "MATCH (r:Record)-[:IN_BLOCK]->(b:Block {o: false})<-[:IN_BLOCK]-(o:Record)"
        " WHERE r.v IN @IDS@ RETURN r.v, o.v",
        r_sorted,
        ("int", "int"),
    )
    a = np.fromiter((r[0] for r in rows), dtype=np.int64, count=len(rows))
    b = np.fromiter((r[1] for r in rows), dtype=np.int64, count=len(rows))
    keep = a != b
    lo, hi = _order_pairs(nodes, a[keep], b[keep])
    n = len(lo)
    ctx.count("candidate_pairs", n)
    created: dict[tuple[int, int], PairRec] = {}
    if n == 0:
        return removed, created, changed
    ends = sorted(set(lo.tolist()) | set(hi.tolist()))
    attrs = scoring_attrs(cfg)
    table = fetch_attrs(ctx, ends, attrs)
    vids = table["_vid"].to_numpy()
    li, ri = pa.array(np.searchsorted(vids, lo)), pa.array(np.searchsorted(vids, hi))
    left = {name: table[name].combine_chunks().take(li) for name in attrs}
    right = {name: table[name].combine_chunks().take(ri) for name in attrs}
    score, has, contra, elig = score_pairs(matching, left, right, n)
    score_list = score.tolist()
    props = [
        pair_json(
            score_list[i] if has[i] else None, bool(contra[i]), bool(elig[i]), "none", None, run_id
        )
        for i in range(n)
    ]
    eids = ctx.copy_edges("MATCH", lo, hi, props)
    for i, (s, e, eid) in enumerate(zip(lo.tolist(), hi.tolist(), eids.tolist(), strict=True)):
        created[(s, e)] = PairRec(
            eid,
            score_list[i] if has[i] else None,
            bool(contra[i]),
            bool(elig[i]),
            "none",
            None,
            run_id,
        )
        changed.add(s)
        changed.add(e)
    return removed, created, changed


# ------------------------------------------------------------------- 4. decisions


def decide_pairs(
    ctx: Ctx,
    run_id: str,
    removed: dict[tuple[int, int], PairRec],
    created: dict[tuple[int, int], PairRec],
    changed: set[int],
    rescore: set[int],
    nodes: Nodes,
) -> set[int]:
    matching = ctx.cfg.matching
    others = {
        k: p
        for k, p in read_pairs(ctx, sorted(changed - rescore)).items()
        if k[0] not in rescore and k[1] not in rescore
    }
    redo: dict[tuple[int, int], PairRec] = {**others, **created}
    involved = sorted({v for k in redo for v in k})
    nodes.need(involved)
    counts: dict[tuple[int, str], int] = {}
    if involved:
        for vid, source, n in ctx.cypher_both(
            "m:MATCH {e: true}", "RETURN a.v, b.s, count(m)", involved, ("int", "str", "int")
        ):
            counts[(vid, source)] = counts.get((vid, source), 0) + n
    touched: set[int] = set()
    update_ids: list[int] = []
    update_props: list[str] = []
    review_del: list[tuple[int, int]] = []
    review_new: list[tuple[int, int, str]] = []
    for key in sorted(redo):
        pair = redo[key]
        left, right = key
        is_new = key in created
        prev = removed.get(key) if is_new else PairRec(*_clone(pair))
        prev_sig = None if prev is None else prev.sig()
        d, reason = decide(
            matching,
            pair.score,
            pair.contra,
            counts.get((left, nodes.info[right][0]), 0),
            counts.get((right, nodes.info[left][0]), 0),
        )
        pair.d, pair.r = d, reason or None
        pair.run = (prev.run if prev is not None else run_id) if prev_sig == pair.sig() else run_id
        if (prev is not None and prev.d == "match") != (d == "match"):
            touched.update(key)
        review_exists = (not is_new) and prev is not None and prev.d == "review"
        if is_new:
            if (d, pair.r, pair.run) != ("none", None, run_id):
                update_ids.append(pair.eid)
                update_props.append(
                    pair_json(pair.score, pair.contra, pair.elig, d, pair.r, pair.run)
                )
            if d == "review":
                review_new.append((left, right, review_json(pair.score, reason, pair.run)))
        elif prev.sig() != pair.sig() or prev.run != pair.run:
            update_ids.append(pair.eid)
            update_props.append(pair_json(pair.score, pair.contra, pair.elig, d, pair.r, pair.run))
            if review_exists:
                review_del.append(key)
            if d == "review":
                review_new.append((left, right, review_json(pair.score, reason, pair.run)))
    for key, old in removed.items():
        if key not in created and old.d == "match":
            touched.update(key)
    ctx.update_edge_props("MATCH", update_ids, update_props)
    ctx.delete_edges_by_pair("REVIEW", review_del)
    if review_new:
        ctx.copy_edges(
            "REVIEW",
            np.array([s for s, _, _ in review_new], dtype=np.int64),
            np.array([e for _, e, _ in review_new], dtype=np.int64),
            [p for _, _, p in review_new],
        )
    ctx.count("pairs_decided", len(redo))
    return touched


def _clone(p: PairRec) -> tuple[Any, ...]:
    return (p.eid, p.score, p.contra, p.elig, p.d, p.r, p.run)


# ------------------------------------------------------------------- 5. clusters


class _Union:
    def __init__(self, items: list[int]) -> None:
        self.parent = {i: i for i in items}

    def find(self, x: int) -> int:
        root = x
        while self.parent[root] != root:
            root = self.parent[root]
        while self.parent[x] != root:
            self.parent[x], x = root, self.parent[x]
        return root

    def union(self, a: int, b: int) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.parent[ra] = rb


def match_components(ctx: Ctx, active: list[int], affected: set[int], mode: str) -> list[list[int]]:
    """Connected components of accepted MATCH edges among the active affected nodes."""
    if mode == "varlen":
        edge, tail = ":MATCH*1..8 {d: 'match'}", "RETURN DISTINCT a.v, b.v"
    else:
        edge, tail = "m:MATCH {d: 'match'}", "RETURN a.v, b.v"
    uf = _Union(active)
    if not table_empty(ctx, "MATCH"):
        for a, b in ctx.cypher_both(edge, tail, active, ("int", "int")):
            if b not in affected:
                raise RuntimeError(f"invariant broken: {a} matches {b} outside the affected set")
            uf.union(a, b)
    groups: dict[int, list[int]] = defaultdict(list)
    for v in active:
        groups[uf.find(v)].append(v)
    return list(groups.values())


def recluster(
    ctx: Ctx,
    run_id: str,
    delta: list[int],
    match_touched: set[int],
    nodes: Nodes,
    counts: dict[str, Any],
    cc_mode: str,
) -> list[tuple[int, list[int]]]:
    seeds = set(delta) | match_touched
    nodes.need(seeds)
    seed_list = sorted(seeds)
    old_of: dict[int, int] = {}
    members_of: dict[int, set[int]] = {}
    if not table_empty(ctx, "MEMBER_OF"):
        for r, s in ctx.cypher_in(
            "MATCH (r:Record)-[:MEMBER_OF]->(s:Staging) WHERE r.v IN @IDS@ RETURN r.v, s.v",
            seed_list,
            ("int", "int"),
        ):
            old_of[r] = s & ENTRY_MASK
        old_ids = sorted(set(old_of.values()))
        staging_label = ctx.labels["Staging"]
        for s, m in ctx.cypher_in(
            "MATCH (s:Staging)<-[:MEMBER_OF]-(m:Record) WHERE s.v IN @IDS@ RETURN s.v, m.v",
            [(staging_label << LABEL_BITS) | i for i in old_ids],
            ("int", "int"),
        ):
            members_of.setdefault(s & ENTRY_MASK, set()).add(m)
            old_of[m] = s & ENTRY_MASK
    old_ids = sorted(members_of)
    affected = set(seeds)
    for members in members_of.values():
        affected |= members
    counts["affected_nodes"] = len(affected)
    nodes.need(affected)
    active = sorted((v for v in affected if nodes.active(v)), key=nodes.key)
    components = match_components(ctx, active, affected, cc_mode)
    components = [sorted(c, key=nodes.key) for c in components]
    components.sort(key=lambda c: nodes.key(c[0]))

    overlaps = [Counter(old_of[n] for n in comp if n in old_of) for comp in components]
    best: dict[int, tuple[int, int]] = {}
    for index, overlap in enumerate(overlaps):
        for sid, c in overlap.items():
            if sid not in best or c > best[sid][0]:
                best[sid] = (c, index)
    heirs: dict[int, list[int]] = defaultdict(list)
    for sid in sorted(best):
        heirs[best[sid][1]].append(sid)
    fresh = [i for i in range(len(components)) if not heirs[i]]
    new_vids = ctx.alloc("Staging", len(fresh))
    new_ids = iter((new_vids & ENTRY_MASK).tolist())
    staging_label = ctx.labels["Staging"]
    events: list[tuple[str, int, int | None]] = []
    assigned: list[int] = []
    new_staging: list[int] = []
    merges = splits = creates = 0
    for index, overlap in enumerate(overlaps):
        candidates = heirs[index]
        if candidates:
            sid = candidates[0]
            for other in candidates[1:]:
                events.append(("merge", sid, other))
                merges += 1
        else:
            sid = next(new_ids)
            new_staging.append(sid)
            if overlap:
                parent = min(overlap, key=lambda s: (-overlap[s], s))
                events.append(("split", parent, sid))
                splits += 1
            else:
                events.append(("create", sid, None))
                creates += 1
        assigned.append(sid)
    kept = set(assigned)
    retired = sorted(set(old_ids) - kept)

    if new_staging:
        vids = np.array([(staging_label << LABEL_BITS) | s for s in new_staging], dtype=np.int64)
        ctx.copy_vertices("Staging", vids, [f'{{"v":{v}}}' for v in vids.tolist()])
        with ctx.conn.cursor() as cur, cur.copy(f"COPY {ctx.t('staging')} FROM STDIN") as cp:
            for s in new_staging:
                cp.write_row((s, "active", run_id))
    if retired:
        ctx.conn.execute(
            f"UPDATE {ctx.t('staging')} SET status = 'retired'"
            " WHERE staging_id = ANY(%s::bigint[])",
            (retired,),
        )
    if events:
        with ctx.conn.cursor() as cur, cur.copy(f"COPY {ctx.t('staging_event')} FROM STDIN") as cp:
            for kind, sid, related in events:
                cp.write_row((run_id, kind, sid, related))

    new_of = {n: sid for comp, sid in zip(components, assigned, strict=True) for n in comp}
    drop = [n for n, s in old_of.items() if new_of.get(n) != s]
    ctx.delete_edges_by_start("MEMBER_OF", drop)
    add = [(n, s) for n, s in new_of.items() if old_of.get(n) != s]
    if add:
        ctx.copy_edges(
            "MEMBER_OF",
            np.array([n for n, _ in add], dtype=np.int64),
            np.array([(staging_label << LABEL_BITS) | s for _, s in add], dtype=np.int64),
            ["{}"] * len(add),
        )
    counts.update(
        stagings=len(kept), merges=merges, splits=splits, creates=creates, retired=len(retired)
    )
    counts["retired_ids"] = retired
    return sorted(zip(assigned, components, strict=True))


# ------------------------------------------------------------------- 6. survive/publish


def _differs(old: pa.Array, new: pa.Array) -> np.ndarray:
    neq = pc.fill_null(pc.not_equal(old, new), False)
    one_null = pc.xor(pc.is_null(old), pc.is_null(new))
    return pc.or_(neq, one_null).to_numpy(zero_copy_only=False)


def publish(
    ctx: Ctx, run_id: str, kept: list[tuple[int, list[int]]], counts: dict[str, Any]
) -> None:
    cfg = ctx.cfg
    retired = counts.pop("retired_ids", [])
    prov = cfg.publication.provenance
    names = [a.name for a in cfg.attributes]
    golden_cols = [
        n for a in cfg.attributes for n in ([a.name, f"{a.name}__src"] if prov else [a.name])
    ]
    counts["golden_rows_written"] = 0
    counts["golden_changes"] = 0
    if retired:
        cur = ctx.conn.execute(
            f"UPDATE {ctx.t('golden')} SET status = 'retired', stale = false"
            " WHERE golden_id = ANY(%s::bigint[]) AND (status <> 'retired' OR stale)",
            (retired,),
        )
        ctx.count("golden_retired", cur.rowcount)
    if not kept:
        return
    k = len(kept)
    sids = [s for s, _ in kept]
    ordinal_of = {v: i for i, (_, members) in enumerate(kept) for v in members}
    all_vids = sorted(ordinal_of)
    members = fetch_attrs(ctx, all_vids, names, ("_source_id", "_source_key", "_revision"))
    ordinal = np.fromiter(
        (ordinal_of[v] for v in members["_vid"].to_pylist()), dtype=np.int64, count=members.num_rows
    )
    ctx.count("survive_member_rows", members.num_rows)
    new_vals, new_src = survive(cfg, members, ordinal, k)

    types = [pa.int64(), pa.string(), pa.bool_(), pa.bool_()]
    cols = ["golden_id", "status", "stale", "_published_once"]
    for a in cfg.attributes:
        cols.append(a.name)
        types.append(arrow_type(a.type))
        if prov:
            cols.append(f"{a.name}__src")
            types.append(pa.string())
    old = fetch_table(
        ctx,
        f"SELECT {', '.join(q(c) for c in cols)} FROM {ctx.t('golden')}"
        " WHERE golden_id = ANY(%s::bigint[]) ORDER BY golden_id",
        (sids,),
        cols,
        types,
    )
    pos = {g: i for i, g in enumerate(old["golden_id"].to_pylist())}
    old_idx = np.array([pos.get(s, -1) for s in sids], dtype=np.int64)
    exists = old_idx >= 0
    take = pa.array(np.where(exists, old_idx, 0), mask=~exists, type=pa.int64())
    old_status = old["status"].combine_chunks().take(take)
    old_stale = pc.fill_null(old["stale"].combine_chunks().take(take), False).to_numpy(
        zero_copy_only=False
    )
    old_pub = pc.fill_null(old["_published_once"].combine_chunks().take(take), False).to_numpy(
        zero_copy_only=False
    )
    blocked = np.zeros(k, dtype=bool)
    blocked_attrs: list[list[str]] = [[] for _ in range(k)]
    for a in cfg.attributes:
        if not a.nullable:
            isnull = pc.is_null(new_vals[a.name]).to_numpy(zero_copy_only=False)
            blocked |= isnull
            for i in np.flatnonzero(isnull).tolist():
                blocked_attrs[i].append(a.name)
    ok = ~blocked
    deltas: list[tuple[Any, ...]] = []
    changed_any = np.zeros(k, dtype=bool)
    out: dict[str, pa.Array] = {}
    for a in cfg.attributes:
        old_v = old[a.name].combine_chunks().take(take)
        diff = _differs(old_v, new_vals[a.name]) & ok
        changed_any |= diff
        idx = np.flatnonzero(diff)
        if len(idx):
            ia = pa.array(idx)
            old_t = V.canonical_text_array(a.type, old_v.take(ia)).to_pylist()
            new_t = V.canonical_text_array(a.type, new_vals[a.name].take(ia)).to_pylist()
            win = new_src[a.name].take(ia).to_pylist()
            for g, o, n_, w in zip(np.array(sids)[idx].tolist(), old_t, new_t, win, strict=True):
                deltas.append((run_id, g, a.name, o, n_, w))
        keep_old = pa.array(blocked)
        out[a.name] = pc.if_else(keep_old, old_v, new_vals[a.name])
        if prov:
            old_s = old[f"{a.name}__src"].combine_chunks().take(take)
            changed_any |= _differs(old_s, new_src[a.name]) & ok
            out[f"{a.name}__src"] = pc.if_else(keep_old, old_s, new_src[a.name])
    counts["golden_changes"] = len(deltas)
    published = pa.array(["published"] * k)
    status = pc.if_else(pa.array(blocked), pa.scalar("blocked"), published)
    stale = np.where(blocked, old_pub, False)
    pub_once = old_pub | ok
    old_status_np = np.array(old_status.to_pylist(), dtype=object)
    need = (
        ~exists
        | (ok & (changed_any | (old_status_np != "published") | old_stale | ~old_pub))
        | (blocked & ((old_status_np != "blocked") | (old_stale != stale)))
    )
    need_idx = np.flatnonzero(need)
    counts["golden_rows_written"] = int(len(need_idx))
    if len(need_idx):
        sel = pa.array(need_idx)
        table = pa.table(
            {
                "golden_id": pa.array(np.array(sids, dtype=np.int64)).take(sel),
                "status": status.take(sel),
                "stale": pa.array(stale).take(sel),
                "_published_once": pa.array(pub_once).take(sel),
                **{c: out[c].take(sel) for c in golden_cols},
            }
        )
        ctx.conn.execute(
            "CREATE TEMP TABLE _g_up ON COMMIT DROP AS SELECT * FROM "
            f"{ctx.t('golden')} WITH NO DATA"
        )
        copy_arrow(ctx, "_g_up", [q(c) for c in table.column_names], table)
        sets = ", ".join(
            f"{q(c)} = EXCLUDED.{q(c)}" for c in table.column_names if c != "golden_id"
        )
        ctx.conn.execute(
            f"INSERT INTO {ctx.t('golden')} SELECT * FROM _g_up ON CONFLICT (golden_id)"
            f" DO UPDATE SET {sets}"
        )
        fresh = [int(s) for s, e in zip(sids, exists, strict=True) if not e]
        if fresh:
            glabel, slabel = ctx.labels["Golden"], ctx.labels["Staging"]
            gv = np.array([(glabel << LABEL_BITS) | s for s in fresh], dtype=np.int64)
            sv = np.array([(slabel << LABEL_BITS) | s for s in fresh], dtype=np.int64)
            ctx.copy_vertices("Golden", gv, [f'{{"v":{v}}}' for v in gv.tolist()])
            ctx.copy_edges("PUBLISHED_AS", sv, gv, ["{}"] * len(fresh))
    anomalies = [
        (run_id, "golden_blocked", ",".join(blocked_attrs[i]), f"golden_id={sids[i]}")
        for i in np.flatnonzero(blocked).tolist()
    ]
    for table_name, rows in (("golden_delta", deltas), ("anomaly", anomalies)):
        if rows:
            with ctx.conn.cursor() as cur, cur.copy(f"COPY {ctx.t(table_name)} FROM STDIN") as cp:
                for row in rows:
                    cp.write_row(row)
