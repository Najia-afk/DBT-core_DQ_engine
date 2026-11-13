"""Ingest + reconcile + survive + export of the reference engine."""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import pyarrow as pa

from engine.core.canonical import (
    canonical_run_id,
    canonical_text,
    ordered_pair,
    row_hash,
    table_from_rows,
)
from engine.core.config import ReferentialConfig

from .matching import (
    EPS,
    BlockValue,
    block_label,
    block_values,
    decide,
    is_eligible,
    score_pair,
)
from .model import Golden, Node, Pair, Record
from .survive import survive_attribute
from .values import CastError, process_attribute, to_timestamp

PairKey = tuple[Node, Node]
Block = tuple[str, BlockValue]
_NO_COUNTS: Counter[str] = Counter()


@dataclass
class IngestResult:
    inserted: int = 0
    updated: int = 0
    deleted: int = 0
    unchanged: int = 0
    rejected: int = 0
    blocked: bool = False


@dataclass
class ReconcileResult:
    delta_nodes: int = 0
    rescored_nodes: int = 0
    pairs: int = 0
    affected_nodes: int = 0
    stagings: int = 0
    merges: int = 0
    splits: int = 0
    creates: int = 0
    golden_changes: int = 0


class ReferenceEngine:
    def __init__(self, config: ReferentialConfig) -> None:
        self.config = config
        self.records: dict[Node, Record] = {}
        self._pending: set[Node] = set()
        self.source_deltas: list[dict[str, Any]] = []
        self.rejects: list[dict[str, Any]] = []
        self.anomalies: list[dict[str, Any]] = []
        self.staging_events: list[dict[str, Any]] = []
        self.golden_deltas: list[dict[str, Any]] = []
        self._index: dict[Block, set[Node]] = {}
        self._node_blocks: dict[Node, dict[str, BlockValue]] = {}
        self._pairs: dict[PairKey, Pair] = {}
        self._adj: dict[Node, set[Node]] = {}
        self._eligible: dict[Node, Counter[str]] = {}
        self._staging_of: dict[Node, int] = {}
        self._members: dict[int, set[Node]] = {}
        self._staging_status: dict[int, str] = {}
        self._staging_created: dict[int, str] = {}
        self._golden: dict[int, Golden] = {}
        self._next_id = 1

    # ------------------------------------------------------------------ ingest

    def ingest(
        self, source_id: str, rows: Iterable[Mapping[str, Any]], run_id: str
    ) -> IngestResult:
        """Merge a batch of rows (source column names) into source_record; emit source_delta."""
        run_id = canonical_run_id(run_id)
        cfg = self.config
        if source_id not in {s.id for s in cfg.sources}:
            raise ValueError(f"unknown source {source_id!r}")
        source = cfg.source(source_id)
        mapping = cfg.mapping(source_id)
        result = IngestResult()
        rejects: list[dict[str, Any]] = []

        def reject(key: str, attribute: str, rule: str, action: str = "reject_record") -> None:
            rejects.append(
                {
                    "run_id": run_id,
                    "source_id": source_id,
                    "source_key": key,
                    "attribute": attribute,
                    "rule": rule,
                    "action": action,
                }
            )

        present: set[str] = set()
        winners: dict[str, tuple[tuple[bool, datetime], int, Mapping[str, Any]]] = {}
        for index, row in enumerate(rows):
            raw_key = row.get(source.key)
            if raw_key is None or str(raw_key) == "":
                reject("", "", "key_null")
                continue
            key = str(raw_key)
            present.add(key)
            revision = None
            if source.revision is not None and row.get(source.revision) is not None:
                try:
                    revision = to_timestamp(row[source.revision])
                except CastError:
                    reject(key, "", "revision")
                    continue
            rank = (revision is not None, revision or datetime.min)
            current = winners.get(key)
            if current is None:
                winners[key] = (rank, index, row)
            elif rank >= current[0]:
                reject(key, "", "duplicate_key")
                winners[key] = (rank, index, row)
            else:
                reject(key, "", "duplicate_key")

        active_keys = {
            r.source_key
            for r in self.records.values()
            if r.source_id == source_id and r.status == "active"
        }
        if source.snapshot == "full" and active_keys:
            ratio = len(present & active_keys) / len(active_keys)
            if ratio < source.full_snapshot_min_ratio - EPS:
                self.anomalies.append(
                    {
                        "run_id": run_id,
                        "kind": "batch_blocked",
                        "subject": source_id,
                        "detail": f"ratio={ratio:.4f} min={source.full_snapshot_min_ratio}",
                    }
                )
                result.blocked = True
                return result

        for key in sorted(winners):
            _, _, row = winners[key]
            values: dict[str, Any] = {a.name: None for a in cfg.attributes}
            rejected = False
            for attr in cfg.attributes:
                column = mapping.get(attr.name)
                if column is None:
                    continue
                value, failures = process_attribute(attr, row.get(column))
                values[attr.name] = value
                for failure in failures:
                    reject(key, attr.name, failure.rule, failure.action)
                    rejected = rejected or failure.action == "reject_record"
            if rejected:
                result.rejected += 1
                continue
            self._merge_record(run_id, source_id, key, values, winners[key][0], result)

        if source.snapshot == "full":
            for key in sorted(active_keys - present):
                record = self.records[(source_id, key)]
                record.status = "retired"
                record.last_change_run = run_id
                self._emit_delta(run_id, record.node, "D", record.row_hash, None)
                result.deleted += 1
        self.rejects.extend(rejects)
        return result

    def _merge_record(
        self,
        run_id: str,
        source_id: str,
        key: str,
        values: dict[str, Any],
        rank: tuple[bool, datetime],
        result: IngestResult,
    ) -> None:
        digest = row_hash(self.config, source_id, values)
        revision = rank[1] if rank[0] else None
        node = (source_id, key)
        record = self.records.get(node)
        if record is None:
            self.records[node] = Record(
                source_id, key, digest, revision, "active", run_id, run_id, values
            )
            self._emit_delta(run_id, node, "I", None, digest)
            result.inserted += 1
            return
        if record.status == "active" and record.row_hash == digest:
            result.unchanged += 1
            return
        reactivated = record.status == "retired"
        old_hash = record.row_hash
        record.row_hash, record.revision, record.values = digest, revision, values
        record.status, record.last_change_run = "active", run_id
        if reactivated:
            self._emit_delta(run_id, node, "I", None, digest)
            result.inserted += 1
        else:
            self._emit_delta(run_id, node, "U", old_hash, digest)
            result.updated += 1

    def _emit_delta(
        self, run_id: str, node: Node, kind: str, old_hash: str | None, new_hash: str | None
    ) -> None:
        self.source_deltas.append(
            {
                "run_id": run_id,
                "source_id": node[0],
                "source_key": node[1],
                "kind": kind,
                "old_hash": old_hash,
                "new_hash": new_hash,
            }
        )
        self._pending.add(node)

    # --------------------------------------------------------------- reconcile

    def reconcile(self, run_id: str) -> ReconcileResult:
        """Blocking, scoring, decisions, clustering and survivorship on what changed."""
        run_id = canonical_run_id(run_id)
        delta = sorted(self._pending)
        self._pending = set()
        result = ReconcileResult(delta_nodes=len(delta))
        if not delta:
            return result
        rescore = self._update_index(run_id, delta)
        removed, changed = self._drop_pairs(rescore)
        new_pairs = self._build_pairs(rescore, changed)
        result.rescored_nodes, result.pairs = len(rescore), len(new_pairs)
        match_touched = self._decide(run_id, removed, new_pairs, changed)
        self._recluster(run_id, delta, match_touched, result)
        return result

    def _update_index(self, run_id: str, delta: list[Node]) -> set[Node]:
        """Update the blocking index; return the nodes whose pairs must be rescored.

        Rescored = delta nodes + every member of a block whose oversized status flipped.
        """
        cfg, limit = self.config, self.config.matching.max_block_size
        old_blocks = {n: self._node_blocks.get(n, {}) for n in delta}
        new_blocks = {
            n: block_values(cfg, self.records[n].values)
            if self.records[n].status == "active"
            else {}
            for n in delta
        }
        touched: set[Block] = set()
        entered: set[Block] = set()
        for n in delta:
            touched.update(old_blocks[n].items())
            entered.update(new_blocks[n].items())
        touched |= entered
        before = {b: len(self._index.get(b, ())) for b in touched}
        for n in delta:
            for block in old_blocks[n].items():
                members = self._index[block]
                members.discard(n)
                if not members:
                    del self._index[block]
            for block in new_blocks[n].items():
                self._index.setdefault(block, set()).add(n)
            if new_blocks[n]:
                self._node_blocks[n] = new_blocks[n]
            else:
                self._node_blocks.pop(n, None)
        after = {b: len(self._index.get(b, ())) for b in touched}
        flipped = {b for b in touched if (before[b] > limit) != (after[b] > limit)}
        rescore = set(delta)
        for block in flipped:
            rescore |= self._index.get(block, set())
        for block in sorted(entered | flipped, key=lambda b: block_label(cfg, *b)):
            if after[block] > limit:
                self.anomalies.append(
                    {
                        "run_id": run_id,
                        "kind": "oversized_block",
                        "subject": block_label(cfg, *block),
                        "detail": f"size={after[block]} max={limit}",
                    }
                )
        return rescore

    def _drop_pairs(self, rescore: set[Node]) -> tuple[dict[PairKey, Pair], set[Node]]:
        removed: dict[PairKey, Pair] = {}
        changed = set(rescore)
        for node in rescore:
            for other in list(self._adj.get(node, ())):
                key = ordered_pair(node, other)
                removed[key] = self._pairs.pop(key)
                self._adj[node].discard(other)
                if other in self._adj:
                    self._adj[other].discard(node)
                changed.add(other)
            self._adj.pop(node, None)
        return removed, changed

    def _build_pairs(self, rescore: set[Node], changed: set[Node]) -> dict[PairKey, Pair]:
        matching, limit = self.config.matching, self.config.matching.max_block_size
        created: dict[PairKey, Pair] = {}
        for node in sorted(rescore):
            if self.records[node].status != "active":
                continue
            partners: set[Node] = set()
            for block in self._node_blocks.get(node, {}).items():
                members = self._index[block]
                if len(members) <= limit:
                    partners |= members
            partners.discard(node)
            for other in partners:
                key = ordered_pair(node, other)
                if key in created:
                    continue
                score, contradiction = score_pair(
                    matching, self.records[key[0]].values, self.records[key[1]].values
                )
                pair = Pair(score, contradiction, is_eligible(matching, score, contradiction))
                created[key] = pair
                self._pairs[key] = pair
                self._adj.setdefault(key[0], set()).add(key[1])
                self._adj.setdefault(key[1], set()).add(key[0])
                changed.update(key)
        return created

    def _decide(
        self,
        run_id: str,
        removed: dict[PairKey, Pair],
        created: dict[PairKey, Pair],
        changed: set[Node],
    ) -> set[Node]:
        """Recompute decisions of every pair touching a node whose neighbourhood changed.

        Returns the nodes whose `match` edges changed (they seed the affected set).
        """
        matching = self.config.matching
        for node in changed:
            neighbours = self._adj.get(node)
            counts = Counter(
                other[0]
                for other in neighbours or ()
                if self._pairs[ordered_pair(node, other)].eligible
            )
            if counts:
                self._eligible[node] = counts
            else:
                self._eligible.pop(node, None)
        redo = {ordered_pair(n, o) for n in changed for o in self._adj.get(n, ())}
        touched: set[Node] = set()
        for key in sorted(redo):
            pair = self._pairs[key]
            previous = removed.get(key) if key in created else pair
            prev_sig = None if previous is None else _signature(previous)
            before_match = previous is not None and previous.decision == "match"
            left, right = key
            pair.decision, pair.reason = decide(
                matching,
                pair.score,
                pair.contradiction,
                self._eligible.get(left, _NO_COUNTS)[right[0]],
                self._eligible.get(right, _NO_COUNTS)[left[0]],
            )
            if prev_sig == _signature(pair):
                pair.run_id = previous.run_id if previous is not None else run_id
            else:
                pair.run_id = run_id
            if before_match != (pair.decision == "match"):
                touched.update(key)
        for key, old in removed.items():
            if key not in self._pairs and old.decision == "match":
                touched.update(key)
        return touched

    def _recluster(
        self, run_id: str, delta: list[Node], match_touched: set[Node], result: ReconcileResult
    ) -> None:
        seeds = set(delta) | match_touched
        old_ids = {self._staging_of[n] for n in seeds if n in self._staging_of}
        affected = set(seeds)
        for sid in old_ids:
            affected |= self._members[sid]
        result.affected_nodes = len(affected)
        active = sorted(n for n in affected if self.records[n].status == "active")
        components = self._components(active, affected)
        overlaps = [
            Counter(self._staging_of[n] for n in comp if n in self._staging_of)
            for comp in components
        ]
        heir: dict[int, int] = {}
        for sid in sorted(old_ids):
            best: int | None = None
            for index, overlap in enumerate(overlaps):
                if overlap[sid] and (best is None or overlap[sid] > overlaps[best][sid]):
                    best = index
            if best is not None:
                heir[sid] = best

        assigned: list[int] = []
        for index, overlap in enumerate(overlaps):
            candidates = sorted(sid for sid, h in heir.items() if h == index)
            if candidates:
                sid = candidates[0]
                for other in candidates[1:]:
                    self._event(run_id, "merge", sid, other)
                    result.merges += 1
            else:
                sid = self._next_id
                self._next_id += 1
                self._staging_status[sid] = "active"
                self._staging_created[sid] = run_id
                if overlap:
                    parent = min(overlap, key=lambda s: (-overlap[s], s))
                    self._event(run_id, "split", parent, sid)
                    result.splits += 1
                else:
                    self._event(run_id, "create", sid, None)
                    result.creates += 1
            assigned.append(sid)

        for sid in old_ids:
            self._members[sid] = set()
        for node in affected:
            self._staging_of.pop(node, None)
        for comp, sid in zip(components, assigned, strict=True):
            self._members.setdefault(sid, set()).update(comp)
            for node in comp:
                self._staging_of[node] = sid
        kept = set(assigned)
        for sid in sorted(old_ids - kept):
            self._staging_status[sid] = "retired"
            golden = self._golden.get(sid)
            if golden is not None:
                golden.status, golden.stale = "retired", False
        result.stagings = len(kept)
        for sid in sorted(kept):
            result.golden_changes += self._survive(run_id, sid)

    def _components(self, active: list[Node], affected: set[Node]) -> list[list[Node]]:
        seen: set[Node] = set()
        components: list[list[Node]] = []
        for start in active:
            if start in seen:
                continue
            seen.add(start)
            stack, comp = [start], []
            while stack:
                node = stack.pop()
                comp.append(node)
                for other in self._adj.get(node, ()):
                    if self._pairs[ordered_pair(node, other)].decision != "match":
                        continue
                    if other not in affected:
                        raise RuntimeError(f"invariant broken: {node} matches {other} outside A")
                    if other not in seen:
                        seen.add(other)
                        stack.append(other)
            components.append(sorted(comp))
        return components

    def _event(self, run_id: str, kind: str, staging_id: int, related: int | None) -> None:
        self.staging_events.append(
            {"run_id": run_id, "kind": kind, "staging_id": staging_id, "related_id": related}
        )

    def _survive(self, run_id: str, sid: int) -> int:
        """Recompute one golden; return the number of attribute changes."""
        cfg = self.config
        members = [self.records[n] for n in sorted(self._members[sid])]
        values: dict[str, Any] = {}
        sources: dict[str, str | None] = {}
        blocked: list[str] = []
        for attr in cfg.attributes:
            values[attr.name], sources[attr.name] = survive_attribute(attr, members)
            if values[attr.name] is None and not attr.nullable:
                blocked.append(attr.name)
        golden = self._golden.get(sid)
        if golden is None:
            empty = {a.name: None for a in cfg.attributes}
            golden = self._golden[sid] = Golden("blocked", False, dict(empty), dict(empty))
        if blocked:
            golden.status, golden.stale = "blocked", golden.published_once
            self.anomalies.append(
                {
                    "run_id": run_id,
                    "kind": "golden_blocked",
                    "subject": ",".join(blocked),
                    "detail": f"golden_id={sid}",
                }
            )
            return 0
        changes = 0
        for attr in cfg.attributes:
            old, new = golden.values[attr.name], values[attr.name]
            if old != new:
                changes += 1
                self.golden_deltas.append(
                    {
                        "run_id": run_id,
                        "golden_id": sid,
                        "attribute": attr.name,
                        "old_value": canonical_text(old, attr.type),
                        "new_value": canonical_text(new, attr.type),
                        "winning_source": sources[attr.name],
                    }
                )
        golden.values, golden.sources = values, sources
        golden.status, golden.stale, golden.published_once = "published", False, True
        return changes

    # ------------------------------------------------------------------ export

    def export(self) -> dict[str, pa.Table]:
        """All canonical projections (schemas of `engine.core.canonical`)."""
        cfg = self.config
        rows = {name: [] for name in _TABLE_NAMES}
        for node in sorted(self.records):
            r = self.records[node]
            rows["source_record"].append(
                {
                    "source_id": r.source_id,
                    "source_key": r.source_key,
                    "row_hash": r.row_hash,
                    "revision": r.revision,
                    "status": r.status,
                    "first_run": r.first_run,
                    "last_change_run": r.last_change_run,
                    **r.values,
                }
            )
        rows["source_delta"] = list(self.source_deltas)
        for (left, right), p in sorted(self._pairs.items()):
            keys = _pair_columns(left, right)
            rows["match_pair"].append(
                {
                    **keys,
                    "score": p.score,
                    "decision": p.decision,
                    "contradiction": p.contradiction,
                    "rule_version": cfg.matching.version,
                }
            )
            if p.decision == "review":
                rows["review"].append(
                    {"run_id": p.run_id, **keys, "score": p.score, "reason": p.reason}
                )
        for sid in sorted(self._staging_status):
            rows["staging"].append(
                {
                    "staging_id": sid,
                    "status": self._staging_status[sid],
                    "created_run": self._staging_created[sid],
                }
            )
            for source_id, source_key in sorted(self._members.get(sid, ())):
                rows["staging_member"].append(
                    {"staging_id": sid, "source_id": source_id, "source_key": source_key}
                )
        rows["staging_event"] = list(self.staging_events)
        for sid in sorted(self._golden):
            g = self._golden[sid]
            row: dict[str, Any] = {"golden_id": sid, "status": g.status, "stale": g.stale}
            for attr in cfg.attributes:
                row[attr.name] = g.values[attr.name]
                if cfg.publication.provenance:
                    row[f"{attr.name}__src"] = g.sources[attr.name]
            rows["golden"].append(row)
        rows["golden_delta"] = list(self.golden_deltas)
        rows["reject"] = list(self.rejects)
        rows["anomaly"] = list(self.anomalies)
        return {name: table_from_rows(cfg, name, rows[name]) for name in _TABLE_NAMES}

    def recompute_from_scratch(self, run_id: str = "scratch") -> dict[str, pa.Table]:
        """Projections of a cold run over the current source state (active records only).

        Event tables (source_delta, reject, golden_delta, ...) then describe this single run,
        not the history; use `compare_exports(..., state_only=True)` against `export()`.
        """
        run_id = canonical_run_id(run_id)
        fresh = ReferenceEngine(self.config)
        for node, r in self.records.items():
            if r.status == "active":
                fresh.records[node] = Record(
                    r.source_id,
                    r.source_key,
                    r.row_hash,
                    r.revision,
                    "active",
                    run_id,
                    run_id,
                    dict(r.values),
                )
                fresh._pending.add(node)
        fresh.reconcile(run_id)
        return fresh.export()


_TABLE_NAMES = (
    "source_record",
    "source_delta",
    "match_pair",
    "staging",
    "staging_member",
    "staging_event",
    "golden",
    "golden_delta",
    "review",
    "reject",
    "anomaly",
)


def _signature(pair: Pair) -> tuple[Any, ...]:
    return (pair.decision, pair.reason, pair.score, pair.contradiction)


def _pair_columns(left: Node, right: Node) -> dict[str, str]:
    return {
        "left_source_id": left[0],
        "left_key": left[1],
        "right_source_id": right[0],
        "right_key": right[1],
    }
