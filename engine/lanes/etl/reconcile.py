"""Reconcile (contract steps 2 and 3): blocking, scoring, decisions, stagings, golden.

Everything runs in ONE PostgreSQL transaction holding an exclusive advisory lock: readers of the
state tables see the previous published state until COMMIT and the new one after (MVCC), a crash
or an error rolls everything back and a rerun converges. Work is bounded by the delta: pairs are
rebuilt only around the delta nodes, stagings only for the affected set (delta nodes + members of
their current stagings + endpoints of pairs whose `match` status changed).
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any

import numpy as np
import psycopg
import pyarrow as pa
import pyarrow.compute as pc

from engine.core.canonical import arrow_type, canonical_run_id
from engine.core.config import ReferentialConfig

from .arrowio import copy_in, copy_out, fetch_table, ident, rebatch
from .cluster import assign_staging_ids, connected_components
from .db import LaneError, Pg, advisory_key, fault, run_begin, run_fail, run_finish
from .matching import EPS, block_keys, match_attributes, score_pairs
from .schema import require_initialised
from .survive import Winners, changed_mask, delta_rows, survive

_DEC = pa.array(["none", "review", "match"])
_REASON = pa.array(["", "multiple_candidates", "contradiction", "score_band"])
_DEC_NAMES = np.array(["none", "review", "match"], dtype=object)
_REASON_NAMES = np.array(["", "multiple_candidates", "contradiction", "score_band"], dtype=object)
_PAIR_COLS = (
    "p.left_id, p.right_id, p.score, p.contradiction, p.eligible, p.decision, p.reason, p.run_id"
)


def _pair_schema(extra: list[tuple[str, pa.DataType]] | None = None) -> pa.Schema:
    return pa.schema(
        [
            ("left_id", pa.int64()),
            ("right_id", pa.int64()),
            ("score", pa.float64()),
            ("contradiction", pa.bool_()),
            ("eligible", pa.bool_()),
            ("decision", pa.string()),
            ("reason", pa.string()),
            ("run_id", pa.string()),
            *(extra or []),
        ]
    )


def _np(arr: pa.Array | pa.ChunkedArray) -> np.ndarray:
    return arr.to_numpy(zero_copy_only=False)


def _codes(arr: pa.Array | pa.ChunkedArray, values: pa.Array) -> np.ndarray:
    return _np(pc.index_in(arr, value_set=values)).astype(np.int64)


@dataclass
class Pairs:
    left: np.ndarray
    right: np.ndarray
    score: np.ndarray
    contradiction: np.ndarray
    eligible: np.ndarray
    lsrc: np.ndarray
    rsrc: np.ndarray

    @staticmethod
    def empty() -> Pairs:
        e = np.empty(0, dtype=np.int64)
        return Pairs(e, e, np.empty(0), np.empty(0, bool), np.empty(0, bool), e, e)

    def __len__(self) -> int:
        return len(self.left)

    @staticmethod
    def concat(parts: list[Pairs]) -> Pairs:
        if not parts:
            return Pairs.empty()
        return Pairs(*(np.concatenate([getattr(p, f) for p in parts]) for f in _FIELDS))


_FIELDS = ("left", "right", "score", "contradiction", "eligible", "lsrc", "rsrc")


class Reconciler:
    def __init__(self, conn: psycopg.Connection[Any], cfg: ReferentialConfig, run_id: str) -> None:
        self.c = conn
        self.cfg = cfg
        self.run_id = run_id
        self.m = cfg.matching
        self.source_ids = pa.array([s.id for s in cfg.sources], pa.string())
        self.n_src = len(cfg.sources)
        self.counts: dict[str, Any] = {}
        self.seconds: dict[str, float] = {}
        self._t = time.perf_counter()

    # ------------------------------------------------------------------ helpers

    def tick(self, phase: str) -> None:
        now = time.perf_counter()
        self.seconds[phase] = round(self.seconds.get(phase, 0.0) + now - self._t, 3)
        self._t = now

    def add(self, name: str, value: int) -> None:
        self.counts[name] = self.counts.get(name, 0) + int(value)

    def temp_ids(self, name: str, ids: np.ndarray) -> None:
        self.c.execute(f"CREATE TEMP TABLE {name} (node_id bigint PRIMARY KEY) ON COMMIT DROP")
        copy_in(self.c, name, pa.table({"node_id": pa.array(np.unique(ids), pa.int64())}))
        self.c.execute(f"ANALYZE {name}")

    def attr_fields(self, names: list[str], prefix: str = "") -> list[tuple[str, pa.DataType]]:
        return [(f"{prefix}{n}", arrow_type(self.cfg.attribute(n).type)) for n in names]

    # ---------------------------------------------------------------------- run

    def run(self) -> dict[str, Any]:
        c, cfg = self.c, self.cfg
        c.execute(
            "SELECT pg_advisory_xact_lock(%s)", [advisory_key(cfg.state.schema_name, "state")]
        )
        c.execute("CREATE TEMP TABLE t_delta ON COMMIT DROP AS SELECT node_id FROM pending_node")
        c.execute("ALTER TABLE t_delta ADD PRIMARY KEY (node_id)")
        c.execute("ANALYZE t_delta")
        n_delta = c.execute("SELECT count(*) FROM t_delta").fetchone()[0]
        self.counts["delta_nodes"] = n_delta
        self.counts["stagings_total"] = c.execute(
            "SELECT count(*) FROM staging WHERE status = 'active'"
        ).fetchone()[0]
        self.tick("setup")
        if n_delta == 0:
            return self.finish()
        attrs = match_attributes(cfg)
        schema = pa.schema(
            [("node_id", pa.int64()), ("status", pa.string()), *self.attr_fields(attrs)]
        )
        cols = "".join(f", r.{ident(a)}" for a in attrs)
        delta = fetch_table(
            c,
            f"SELECT r.node_id, r.status{cols} FROM source_record r JOIN t_delta d USING (node_id)",
            schema,
        )
        self.add("record_rows_read_match_attrs", delta.num_rows)
        active = delta.filter(pc.equal(delta["status"], "active"))
        new_blocks = block_keys(cfg, active)
        self.tick("read_delta")
        self.update_blocks(new_blocks)
        fault("reconcile:after_blocks")
        self.tick("blocks")
        removed = self.drop_pairs()
        candidates = self.score_candidates(attrs)
        self.tick("score")
        fault("reconcile:after_scoring")
        touched_match = self.decide_and_store(removed, candidates)
        self.tick("decide")
        fault("reconcile:after_pairs")
        self.recluster(touched_match)
        fault("reconcile:before_commit")
        c.execute("DELETE FROM pending_node WHERE node_id IN (SELECT node_id FROM t_delta)")
        return self.finish()

    def finish(self) -> dict[str, Any]:
        self.counts["seconds"] = self.seconds
        run_finish(self.c, self.run_id, "reconcile", "ok", {**self.counts})
        return self.counts

    # ------------------------------------------------------------- blocking index

    def update_blocks(self, new_blocks: pa.Table) -> None:
        c, limit = self.c, self.m.max_block_size
        c.execute(
            'CREATE TEMP TABLE t_newblock (node_id bigint, bname text COLLATE "C",'
            ' bvalue text COLLATE "C") ON COMMIT DROP'
        )
        copy_in(c, "t_newblock", new_blocks)
        c.execute(
            "CREATE TEMP TABLE t_touched ON COMMIT DROP AS"
            " SELECT bname, bvalue FROM block_index WHERE node_id IN (SELECT node_id FROM t_delta)"
            " UNION SELECT bname, bvalue FROM t_newblock"
        )
        count_sql = (
            "SELECT t.bname, t.bvalue, (SELECT count(*) FROM block_index b"
            " WHERE b.bname = t.bname AND b.bvalue = t.bvalue) AS n FROM t_touched t"
        )
        c.execute(f"CREATE TEMP TABLE t_before ON COMMIT DROP AS {count_sql}")
        c.execute("DELETE FROM block_index WHERE node_id IN (SELECT node_id FROM t_delta)")
        c.execute("INSERT INTO block_index SELECT node_id, bname, bvalue FROM t_newblock")
        c.execute(f"CREATE TEMP TABLE t_after ON COMMIT DROP AS {count_sql}")
        c.execute(
            "CREATE TEMP TABLE t_flipped ON COMMIT DROP AS SELECT a.bname, a.bvalue, a.n"
            " FROM t_after a JOIN t_before b USING (bname, bvalue) WHERE (b.n > %s) <> (a.n > %s)",
            [limit, limit],
        )
        anomalies = c.execute(
            "INSERT INTO anomaly (run_id, kind, subject, detail)"
            " SELECT %s, 'oversized_block', a.bname || '=' || replace(a.bvalue, chr(31), '|'),"
            " 'size=' || a.n || ' max=' || %s::text FROM t_after a WHERE a.n > %s AND ("
            " EXISTS (SELECT 1 FROM t_newblock x WHERE x.bname = a.bname AND x.bvalue = a.bvalue)"
            " OR EXISTS (SELECT 1 FROM t_flipped f"
            " WHERE f.bname = a.bname AND f.bvalue = a.bvalue))",
            [self.run_id, limit, limit],
        ).rowcount
        self.add("oversized_block_anomalies", anomalies)
        c.execute(
            "CREATE TEMP TABLE t_r ON COMMIT DROP AS SELECT node_id FROM t_delta UNION"
            " SELECT b.node_id FROM block_index b JOIN t_flipped f"
            " ON f.bname = b.bname AND f.bvalue = b.bvalue"
        )
        c.execute("ALTER TABLE t_r ADD PRIMARY KEY (node_id)")
        c.execute("ANALYZE t_r")
        self.counts["rescored_nodes"] = c.execute("SELECT count(*) FROM t_r").fetchone()[0]

    # ---------------------------------------------------------------------- pairs

    def drop_pairs(self) -> pa.Table:
        c = self.c
        c.execute(
            "CREATE TEMP TABLE t_removed ON COMMIT DROP AS SELECT * FROM match_pair"
            " WHERE left_id IN (SELECT node_id FROM t_r) OR right_id IN (SELECT node_id FROM t_r)"
        )
        c.execute(
            "DELETE FROM match_pair p USING t_removed r"
            " WHERE p.left_id = r.left_id AND p.right_id = r.right_id"
        )
        removed = fetch_table(
            c,
            "SELECT left_id, right_id, score, contradiction, eligible, decision, reason, run_id"
            " FROM t_removed",
            _pair_schema(),
        )
        self.add("pairs_removed", removed.num_rows)
        return removed

    def score_candidates(self, attrs: list[str]) -> Pairs:
        c, m = self.c, self.m
        limit = m.max_block_size
        c.execute(
            "CREATE TEMP TABLE t_okblocks ON COMMIT DROP AS SELECT t.bname, t.bvalue FROM"
            " (SELECT DISTINCT b.bname, b.bvalue FROM block_index b JOIN t_r r USING (node_id)) t"
            " WHERE (SELECT count(*) FROM block_index x WHERE x.bname = t.bname"
            " AND x.bvalue = t.bvalue) <= %s",
            [limit],
        )
        c.execute("ANALYZE t_okblocks")
        c.execute(
            "CREATE TEMP TABLE t_cand ON COMMIT DROP AS SELECT DISTINCT"
            " least(a.node_id, b.node_id) AS lo, greatest(a.node_id, b.node_id) AS hi"
            " FROM t_r r JOIN block_index a ON a.node_id = r.node_id"
            " JOIN t_okblocks k ON k.bname = a.bname AND k.bvalue = a.bvalue"
            " JOIN block_index b ON b.bname = a.bname AND b.bvalue = a.bvalue"
            " AND b.node_id <> a.node_id"
        )
        c.execute("ANALYZE t_cand")
        wire = pa.schema(
            [
                ("lo", pa.int64()),
                ("hi", pa.int64()),
                ("lsrc", pa.string()),
                ("rsrc", pa.string()),
                *self.attr_fields(attrs, "l_"),
                *self.attr_fields(attrs, "r_"),
            ]
        )
        left_cols = ", ".join(f"a.{ident(n)}" for n in attrs)
        right_cols = ", ".join(f"b.{ident(n)}" for n in attrs)
        query = (
            f"SELECT c.lo, c.hi, a.source_id, b.source_id, {left_cols}, {right_cols}"
            " FROM t_cand c JOIN source_record a ON a.node_id = c.lo"
            " JOIN source_record b ON b.node_id = c.hi"
        )
        parts: list[Pairs] = []
        n_candidates = 0
        stream = copy_out(c, query, wire)
        try:
            for table in rebatch((pa.Table.from_batches([b]) for b in stream), 200_000):
                n_candidates += table.num_rows
                left = pa.table({n: table.column(f"l_{n}") for n in attrs})
                right = pa.table({n: table.column(f"r_{n}") for n in attrs})
                score, contradiction = score_pairs(m, left, right)
                known = ~np.isnan(score)
                with np.errstate(invalid="ignore"):
                    eligible = known & (score >= m.accept - EPS) & ~contradiction
                    band = known & (score >= m.review - EPS)
                keep = eligible | band
                if not keep.any():
                    continue
                parts.append(
                    Pairs(
                        _np(table.column("lo"))[keep],
                        _np(table.column("hi"))[keep],
                        score[keep],
                        contradiction[keep],
                        eligible[keep],
                        _codes(table.column("lsrc"), self.source_ids)[keep],
                        _codes(table.column("rsrc"), self.source_ids)[keep],
                    )
                )
        finally:
            stream.close()
        self.add("record_rows_read_match_attrs", 2 * n_candidates)
        self.counts["candidate_pairs"] = n_candidates
        return Pairs.concat(parts)

    def _count_eligible(
        self, left: np.ndarray, right: np.ndarray, lsrc: np.ndarray, rsrc: np.ndarray
    ) -> tuple[np.ndarray, np.ndarray]:
        s = self.n_src
        keys = np.concatenate([left * s + rsrc, right * s + lsrc])
        return np.unique(keys, return_counts=True)

    def decide_and_store(self, removed: pa.Table, new: Pairs) -> np.ndarray:
        """Decide every pair touching a changed node, write the differences, return the nodes
        whose `match` edges changed."""
        c, m = self.c, self.m
        self.counts["pairs_stored"] = len(new)
        old_keys = (_np(removed["left_id"]) << 32) | _np(removed["right_id"])
        changed = np.unique(
            np.concatenate(
                [
                    _np(removed["left_id"]),
                    _np(removed["right_id"]),
                    new.left,
                    new.right,
                    self._column("t_r"),
                ]
            )
        )
        self.temp_ids("t_changed", changed)
        src_join = (
            " JOIN source_record ls ON ls.node_id = p.left_id"
            " JOIN source_record rs ON rs.node_id = p.right_id"
        )
        adj_schema = _pair_schema([("lsrc", pa.string()), ("rsrc", pa.string())])
        adjacent = fetch_table(
            c,
            f"SELECT {_PAIR_COLS}, ls.source_id, rs.source_id FROM match_pair p{src_join}"
            " WHERE p.left_id IN (SELECT node_id FROM t_changed)"
            " OR p.right_id IN (SELECT node_id FROM t_changed)",
            adj_schema,
        )
        self.add("pairs_read", adjacent.num_rows)
        adj = Pairs(
            _np(adjacent["left_id"]),
            _np(adjacent["right_id"]),
            _np(pc.fill_null(adjacent["score"], np.nan)),
            _np(adjacent["contradiction"]),
            _np(adjacent["eligible"]),
            _codes(adjacent["lsrc"], self.source_ids),
            _codes(adjacent["rsrc"], self.source_ids),
        )
        redo = Pairs.concat([new, adj])
        is_new = np.r_[np.ones(len(new), bool), np.zeros(len(adj), bool)]
        # eligible pairs around the endpoints of the pairs to decide: the counts of the decision
        ends = np.unique(np.concatenate([redo.left, redo.right]))
        self.temp_ids("t_ends", ends)
        context = fetch_table(
            c,
            f"SELECT p.left_id, p.right_id, ls.source_id, rs.source_id FROM match_pair p{src_join}"
            " WHERE p.eligible AND (p.left_id IN (SELECT node_id FROM t_ends)"
            " OR p.right_id IN (SELECT node_id FROM t_ends))",
            pa.schema(
                [
                    ("left_id", pa.int64()),
                    ("right_id", pa.int64()),
                    ("lsrc", pa.string()),
                    ("rsrc", pa.string()),
                ]
            ),
        )
        self.add("pairs_read", context.num_rows)
        el = new.eligible
        ctx_l = np.concatenate([_np(context["left_id"]), new.left[el]])
        ctx_r = np.concatenate([_np(context["right_id"]), new.right[el]])
        ctx_ls = np.concatenate([_codes(context["lsrc"], self.source_ids), new.lsrc[el]])
        ctx_rs = np.concatenate([_codes(context["rsrc"], self.source_ids), new.rsrc[el]])
        uniq, cnt = self._count_eligible(ctx_l, ctx_r, ctx_ls, ctx_rs)

        def count(node: np.ndarray, src: np.ndarray) -> np.ndarray:
            key = node * self.n_src + src
            if len(uniq) == 0:
                return np.zeros(len(key), dtype=np.int64)
            pos = np.minimum(np.searchsorted(uniq, key), len(uniq) - 1)
            return np.where(uniq[pos] == key, cnt[pos], 0)

        n_left, n_right = count(redo.left, redo.rsrc), count(redo.right, redo.lsrc)
        known = ~np.isnan(redo.score)
        with np.errstate(invalid="ignore"):
            band = known & (redo.score >= m.review - EPS) & ~redo.eligible
        match = redo.eligible & (n_left == 1) & (n_right == 1)
        multiple = redo.eligible & ~match
        dec = np.where(match, 2, np.where(multiple | band, 1, 0))
        reason = np.where(multiple, 1, np.where(band, np.where(redo.contradiction, 2, 3), 0))
        # previous state of each pair: stored row (existing) or the removed row (re-created)
        prev_dec = np.full(len(redo), -1, dtype=np.int64)
        prev_reason = np.zeros(len(redo), dtype=np.int64)
        prev_score = np.full(len(redo), np.nan)
        prev_contra = np.zeros(len(redo), dtype=bool)
        prev_run = np.full(len(redo), "", dtype=object)
        if len(adj):
            sl = slice(len(new), None)
            prev_dec[sl] = _codes(adjacent["decision"], _DEC)
            prev_reason[sl] = _codes(adjacent["reason"], _REASON)
            prev_score[sl] = redo.score[sl]
            prev_contra[sl] = adj.contradiction
            prev_run[sl] = _np(adjacent["run_id"])
        if len(new) and removed.num_rows:
            order = np.argsort(old_keys)
            sorted_keys = old_keys[order]
            new_keys = (new.left << 32) | new.right
            pos = np.minimum(np.searchsorted(sorted_keys, new_keys), len(sorted_keys) - 1)
            found = sorted_keys[pos] == new_keys
            src = order[pos[found]]
            idx = np.flatnonzero(found)
            prev_dec[idx] = _codes(removed["decision"].take(pa.array(src)), _DEC)
            prev_reason[idx] = _codes(removed["reason"].take(pa.array(src)), _REASON)
            prev_score[idx] = _np(pc.fill_null(removed["score"].take(pa.array(src)), np.nan))
            prev_contra[idx] = _np(removed["contradiction"].take(pa.array(src)))
            prev_run[idx] = _np(removed["run_id"].take(pa.array(src)))
        has_prev = prev_dec >= 0
        same_score = (prev_score == redo.score) | (np.isnan(prev_score) & np.isnan(redo.score))
        same = (
            has_prev
            & (prev_dec == dec)
            & (prev_reason == reason)
            & same_score
            & (prev_contra == redo.contradiction)
        )
        run_col = np.where(same, prev_run, self.run_id).astype(object)
        before_match = prev_dec == 2
        flips = before_match != (dec == 2)
        touched = np.concatenate([redo.left[flips], redo.right[flips]])
        # removed `match` pairs that were not re-created
        if removed.num_rows:
            was_match = _np(pc.equal(removed["decision"], "match"))
            keys_new = np.sort((new.left << 32) | new.right)
            gone = was_match & ~np.isin(old_keys, keys_new)
            touched = np.concatenate(
                [touched, _np(removed["left_id"])[gone], _np(removed["right_id"])[gone]]
            )
        # writes: new pairs in full, stored pairs only when their decision data changed
        out_schema = _pair_schema()

        def table(rows: np.ndarray) -> pa.Table:
            score = redo.score[rows]
            return pa.table(
                {
                    "left_id": pa.array(redo.left[rows], pa.int64()),
                    "right_id": pa.array(redo.right[rows], pa.int64()),
                    "score": pa.array(score, pa.float64(), mask=np.isnan(score)),
                    "contradiction": pa.array(redo.contradiction[rows]),
                    "eligible": pa.array(redo.eligible[rows]),
                    "decision": pa.array(_DEC_NAMES[dec[rows]], pa.string()),
                    "reason": pa.array(_REASON_NAMES[reason[rows]], pa.string()),
                    "run_id": pa.array(run_col[rows], pa.string()),
                },
                schema=out_schema,
            )

        insert_rows = np.flatnonzero(is_new & (dec > 0))
        copy_in(c, "match_pair", table(insert_rows))
        update_rows = np.flatnonzero(~is_new & ((prev_dec != dec) | (prev_reason != reason)))
        if len(update_rows):
            c.execute("CREATE TEMP TABLE t_dec (LIKE match_pair) ON COMMIT DROP")
            copy_in(c, "t_dec", table(update_rows))
            c.execute(
                "UPDATE match_pair p SET decision = d.decision, reason = d.reason,"
                " run_id = d.run_id"
                " FROM t_dec d WHERE p.left_id = d.left_id AND p.right_id = d.right_id"
            )
        self.add("pairs_inserted", len(insert_rows))
        self.add("pairs_updated", len(update_rows))
        return np.unique(touched)

    def _column(self, table: str) -> np.ndarray:
        rows = self.c.execute(f"SELECT node_id FROM {table}").fetchall()
        return np.fromiter((r[0] for r in rows), dtype=np.int64, count=len(rows))

    # ------------------------------------------------------------------ stagings

    def recluster(self, match_touched: np.ndarray) -> None:
        c = self.c
        self.temp_ids("t_mt", match_touched)
        c.execute(
            "CREATE TEMP TABLE t_seed ON COMMIT DROP AS SELECT node_id FROM t_delta"
            " UNION SELECT node_id FROM t_mt"
        )
        c.execute(
            "CREATE TEMP TABLE t_oldstg ON COMMIT DROP AS SELECT DISTINCT staging_id"
            " FROM staging_member WHERE node_id IN (SELECT node_id FROM t_seed)"
        )
        c.execute(
            "CREATE TEMP TABLE t_a ON COMMIT DROP AS SELECT node_id FROM t_seed UNION"
            " SELECT m.node_id FROM staging_member m JOIN t_oldstg o USING (staging_id)"
        )
        c.execute("ALTER TABLE t_a ADD PRIMARY KEY (node_id)")
        c.execute("ANALYZE t_a")
        nodes = fetch_table(
            c,
            "SELECT r.node_id, r.source_id, r.source_key, r.status, m.staging_id"
            " FROM t_a a JOIN source_record r USING (node_id)"
            " LEFT JOIN staging_member m USING (node_id)",
            pa.schema(
                [
                    ("node_id", pa.int64()),
                    ("source_id", pa.string()),
                    ("source_key", pa.string()),
                    ("status", pa.string()),
                    ("staging_id", pa.int64()),
                ]
            ),
        )
        self.counts["affected_nodes"] = nodes.num_rows
        self.add("record_rows_read_narrow", nodes.num_rows)
        order = pc.sort_indices(
            nodes, sort_keys=[("source_id", "ascending"), ("source_key", "ascending")]
        )
        nodes = nodes.take(order)
        node_id = _np(nodes["node_id"])
        status_active = _np(pc.equal(nodes["status"], "active"))
        old_sid = _np(pc.fill_null(nodes["staging_id"], -1))
        self.counts["member_rows_read"] = int((old_sid >= 0).sum())
        edges = fetch_table(
            c,
            "SELECT p.left_id, p.right_id FROM match_pair p"
            " WHERE p.decision = 'match' AND (p.left_id IN (SELECT node_id FROM t_a)"
            " OR p.right_id IN (SELECT node_id FROM t_a))",
            pa.schema([("left_id", pa.int64()), ("right_id", pa.int64())]),
        )
        self.add("pairs_read", edges.num_rows)
        by_id = np.argsort(node_id)
        sorted_ids = node_id[by_id]

        def position(ids: np.ndarray) -> np.ndarray:
            pos = np.minimum(np.searchsorted(sorted_ids, ids), len(sorted_ids) - 1)
            if not np.array_equal(sorted_ids[pos], ids):
                raise LaneError("invariant broken: a match crosses the border of the affected set")
            return by_id[pos]

        pu, pv = position(_np(edges["left_id"])), position(_np(edges["right_id"]))
        if not (status_active[pu].all() and status_active[pv].all()):
            raise LaneError("invariant broken: a match involves an inactive record")
        labels = connected_components(len(node_id), pu, pv)
        act = np.flatnonzero(status_active)
        roots, comp_of = np.unique(labels[act], return_inverse=True)
        old_ids = np.unique(old_sid[old_sid >= 0])
        self.counts["stagings_read"] = len(old_ids)

        def allocate(k: int) -> list[int]:
            rows = c.execute(
                "SELECT nextval('staging_seq') FROM generate_series(1, %s)", [k]
            ).fetchall()
            return sorted(r[0] for r in rows)

        plan = assign_staging_ids(comp_of, len(roots), old_sid[act], old_ids, allocate)
        new_sid = np.full(len(node_id), -1, dtype=np.int64)
        new_sid[act] = plan.comp_sid[comp_of]
        self.counts["stagings_kept"] = len(roots)
        self.counts["merges"] = sum(1 for e in plan.events if e[0] == "merge")
        self.counts["splits"] = sum(1 for e in plan.events if e[0] == "split")
        self.counts["creates"] = sum(1 for e in plan.events if e[0] == "create")
        self.counts["stagings_retired"] = len(plan.retired)
        self.tick("cluster")
        self.write_stagings(plan, node_id, old_sid, new_sid)
        self.tick("write_staging")
        self.publish(node_id, new_sid, plan.comp_sid)
        self.tick("survive")

    def write_stagings(
        self, plan: Any, node_id: np.ndarray, old_sid: np.ndarray, new_sid: np.ndarray
    ) -> None:
        c, run_id = self.c, self.run_id
        if plan.new_ids:
            copy_in(
                c,
                "staging",
                pa.table(
                    {
                        "staging_id": pa.array(plan.new_ids, pa.int64()),
                        "status": pa.repeat(pa.scalar("active"), len(plan.new_ids)),
                        "created_run": pa.repeat(pa.scalar(run_id), len(plan.new_ids)),
                    }
                ),
            )
        if plan.retired:
            c.execute(
                "UPDATE staging SET status = 'retired' WHERE staging_id = ANY(%s)", [plan.retired]
            )
            c.execute(
                "UPDATE golden SET status = 'retired', stale = false"
                " WHERE golden_id = ANY(%s) AND (status <> 'retired' OR stale)",
                [plan.retired],
            )
        if plan.events:
            kinds, ids, related = zip(*plan.events, strict=True)
            copy_in(
                c,
                "staging_event",
                pa.table(
                    {
                        "run_id": pa.repeat(pa.scalar(run_id), len(kinds)),
                        "kind": pa.array(kinds, pa.string()),
                        "staging_id": pa.array(ids, pa.int64()),
                        "related_id": pa.array(related, pa.int64()),
                    }
                ),
            )
        left = (old_sid >= 0) & (new_sid < 0)
        if left.any():
            c.execute(
                "DELETE FROM staging_member WHERE node_id = ANY(%s)", [node_id[left].tolist()]
            )
        moved = (new_sid >= 0) & (new_sid != old_sid)
        if moved.any():
            c.execute(
                "CREATE TEMP TABLE t_member (node_id bigint, staging_id bigint) ON COMMIT DROP"
            )
            copy_in(
                c,
                "t_member",
                pa.table(
                    {
                        "node_id": pa.array(node_id[moved], pa.int64()),
                        "staging_id": pa.array(new_sid[moved], pa.int64()),
                    }
                ),
            )
            c.execute(
                "INSERT INTO staging_member SELECT node_id, staging_id FROM t_member"
                " ON CONFLICT (node_id) DO UPDATE SET staging_id = EXCLUDED.staging_id"
            )
        self.counts["member_rows_written"] = int(left.sum() + moved.sum())

    # ------------------------------------------------------- survivorship + golden

    def publish(self, node_id: np.ndarray, new_sid: np.ndarray, comp_sid: np.ndarray) -> None:
        cfg, c = self.cfg, self.c
        members = np.flatnonzero(new_sid >= 0)
        sids = np.unique(comp_sid)
        self.counts.update(golden_rows_written=0, golden_changes=0, golden_blocked=0)
        step = max(1000, cfg.runtime.batch_rows // 2)
        names = [a.name for a in cfg.attributes]
        member_schema = pa.schema(
            [
                ("node_id", pa.int64()),
                ("source_id", pa.string()),
                ("source_key", pa.string()),
                ("revision", pa.timestamp("us")),
                *self.attr_fields(names),
            ]
        )
        order = np.argsort(new_sid[members], kind="stable")
        members = members[order]
        sid_sorted = new_sid[members]
        c.execute("CREATE TEMP TABLE t_chunk (node_id bigint PRIMARY KEY) ON COMMIT DROP")
        c.execute("CREATE TEMP TABLE t_chunk_sid (golden_id bigint PRIMARY KEY) ON COMMIT DROP")
        cols = "".join(f", r.{ident(n)}" for n in names)
        for start in range(0, len(sids), step):
            chunk = sids[start : start + step]
            lo = np.searchsorted(sid_sorted, chunk[0], side="left")
            hi = np.searchsorted(sid_sorted, chunk[-1], side="right")
            rows = members[lo:hi]
            c.execute("TRUNCATE t_chunk")
            copy_in(c, "t_chunk", pa.table({"node_id": pa.array(node_id[rows], pa.int64())}))
            table = fetch_table(
                c,
                f"SELECT r.node_id, r.source_id, r.source_key, r.revision{cols}"
                " FROM t_chunk k JOIN source_record r USING (node_id)",
                member_schema,
            )
            self.add("record_rows_read_wide", table.num_rows)
            ids = _np(table["node_id"])
            sort = np.argsort(node_id[rows])
            pos = sort[np.searchsorted(node_id[rows][sort], ids)]
            winners = survive(cfg, table, new_sid[rows][pos])
            self.write_golden(winners)

    def write_golden(self, w: Winners) -> None:
        cfg, c, run_id = self.cfg, self.c, self.run_id
        g = len(w.sids)
        names = [a.name for a in cfg.attributes]
        c.execute("TRUNCATE t_chunk_sid")
        copy_in(c, "t_chunk_sid", pa.table({"golden_id": pa.array(w.sids, pa.int64())}))
        prov = cfg.publication.provenance
        fields = [
            ("golden_id", pa.int64()),
            ("status", pa.string()),
            ("stale", pa.bool_()),
            ("published_once", pa.bool_()),
        ]
        select = "g.golden_id, g.status, g.stale, g.published_once"
        for a in cfg.attributes:
            fields.append((a.name, arrow_type(a.type)))
            select += f", g.{ident(a.name)}"
            if prov:
                fields.append((f"{a.name}__src", pa.string()))
                select += f", g.{ident(a.name + '__src')}"
        current = fetch_table(
            c,
            f"SELECT {select} FROM t_chunk_sid s JOIN golden g ON g.golden_id = s.golden_id",
            pa.schema(fields),
        )
        self.add("golden_rows_read", current.num_rows)
        index = pc.index_in(
            pa.array(w.sids, pa.int64()), value_set=current["golden_id"].combine_chunks()
        )
        exists = _np(pc.is_valid(index))
        status_old = pc.cast(current["status"], pa.string()).combine_chunks().take(index)
        stale_old = _np(pc.fill_null(current["stale"].combine_chunks().take(index), False))
        once_old = _np(pc.fill_null(current["published_once"].combine_chunks().take(index), False))
        blocked = np.zeros(g, dtype=bool)
        blocked_names: list[list[str]] = [[] for _ in range(g)]
        for a in cfg.attributes:
            if not a.nullable:
                nulls = _np(pc.is_null(w.values[a.name]))
                blocked |= nulls
                for i in np.flatnonzero(nulls):
                    blocked_names[i].append(a.name)
        changed_any = np.zeros(g, dtype=bool)
        deltas: list[pa.Table] = []
        olds: dict[str, pa.Array] = {}
        for a in cfg.attributes:
            old = current[a.name].combine_chunks().take(index)
            olds[a.name] = old
            mask = changed_mask(old, w.values[a.name])
            changed_any |= mask
            if prov:
                old_src = current[f"{a.name}__src"].combine_chunks().take(index)
                changed_any |= changed_mask(old_src, w.sources[a.name])
            emit = mask & ~blocked
            if emit.any():
                deltas.append(
                    delta_rows(
                        run_id, a.name, a.type, w.sids, old, w.values[a.name],
                        w.sources[a.name], emit,
                    )
                )  # fmt: skip
        status_ok = _np(pc.fill_null(pc.equal(status_old, "published"), False)) & exists
        write_published = ~blocked & (~status_ok | stale_old | ~once_old | changed_any | ~exists)
        if deltas:
            table = pa.concat_tables(deltas)
            copy_in(c, "golden_delta", table)
            self.add("golden_changes", table.num_rows)
        # blocked: previous values are kept; a never-published golden is created empty
        new_blocked = blocked & ~exists
        if new_blocked.any():
            n = int(new_blocked.sum())
            row = {
                "golden_id": pa.array(w.sids[new_blocked], pa.int64()),
                "status": pa.repeat(pa.scalar("blocked"), n),
                "stale": pa.repeat(pa.scalar(False), n),
                "published_once": pa.repeat(pa.scalar(False), n),
            }
            copy_in(c, "golden", pa.table(row))
        old_blocked = blocked & exists
        is_blocked = _np(pc.fill_null(pc.equal(status_old, "blocked"), False))
        upd = old_blocked & (~is_blocked | (stale_old != once_old))
        if upd.any():
            c.execute(
                "UPDATE golden SET status = 'blocked', stale = published_once"
                " WHERE golden_id = ANY(%s)",
                [w.sids[upd].tolist()],
            )
        if blocked.any():
            n_blocked = int(blocked.sum())
            self.add("golden_blocked", n_blocked)
            subject = [",".join(blocked_names[i]) for i in np.flatnonzero(blocked)]
            detail = [f"golden_id={s}" for s in w.sids[blocked].tolist()]
            copy_in(
                c,
                "anomaly",
                pa.table(
                    {
                        "run_id": pa.repeat(pa.scalar(run_id), n_blocked),
                        "kind": pa.repeat(pa.scalar("golden_blocked"), n_blocked),
                        "subject": pa.array(subject, pa.string()),
                        "detail": pa.array(detail, pa.string()),
                    }
                ),
            )
        written = int(new_blocked.sum() + upd.sum())
        if write_published.any():
            rows = np.flatnonzero(write_published)
            take = pa.array(rows)
            data: dict[str, pa.Array] = {
                "golden_id": pa.array(w.sids[rows], pa.int64()),
                "status": pa.repeat(pa.scalar("published"), len(rows)),
                "stale": pa.repeat(pa.scalar(False), len(rows)),
                "published_once": pa.repeat(pa.scalar(True), len(rows)),
            }
            for name in names:
                data[name] = w.values[name].take(take)
                if prov:
                    data[f"{name}__src"] = w.sources[name].take(take)
            c.execute("CREATE TEMP TABLE IF NOT EXISTS t_gold (LIKE golden) ON COMMIT DROP")
            c.execute("TRUNCATE t_gold")
            copy_in(c, "t_gold", pa.table(data))
            cols = [ident(n) for n in data]
            sets = ", ".join(f"{col} = EXCLUDED.{col}" for col in cols[1:])
            c.execute(
                f"INSERT INTO golden ({', '.join(cols)}) SELECT {', '.join(cols)} FROM t_gold"
                f" ON CONFLICT (golden_id) DO UPDATE SET {sets}"
            )
            written += len(rows)
        self.add("golden_rows_written", written)


def reconcile(cfg: ReferentialConfig, run_id: str, pg: Pg) -> dict[str, Any]:
    run_id = canonical_run_id(run_id)
    require_initialised(pg.conn(), cfg)
    previous = run_begin(pg.conn(), cfg, run_id, "reconcile")
    if previous is not None:
        return {"replayed": True, **previous}

    def once(conn: psycopg.Connection[Any]) -> dict[str, Any]:
        with conn.transaction():
            return Reconciler(conn, cfg, run_id).run()

    try:
        counts = pg.run(once, "reconcile")
    except BaseException as error:
        try:
            run_fail(pg.conn(), run_id, "reconcile", str(error))
        except psycopg.Error:
            pass
        raise
    return counts
