"""Reference implementation of docs/MDM_CONTRACT.md: pure Python, in memory, slow but correct.

It is the oracle the three lanes (ETL/ELT/graph) are certified against with
`engine.core.compare.compare_exports`. Every choice below resolves an ambiguity of the contract;
the lanes must implement the same choices (or the contract must be amended).

Public API: `ReferenceEngine(config)` with `ingest(source_id, rows, run_id)`,
`reconcile(run_id)`, `export()` and `recompute_from_scratch()`.

Ingest
------
* Rows use SOURCE column names (`source.key`, `source.revision`, mapped attribute columns).
  `source_key = str(raw key)`; a null/empty key is rejected (rule `key_null`, attribute "").
  Record-level rejects use attribute "" (empty string).
* `revision` is canonicalised to timestamp('us') (datetime, date or ISO-8601 text; tz-aware
  values are converted to naive UTC). Anything else (e.g. an int) is rejected (rule `revision`).
  No revision column configured => revision is null and the LAST row of a duplicated key wins.
* Duplicate keys in a batch: greatest revision (null = oldest), then last row; each loser is a
  reject (rule `duplicate_key`). Deduplication happens before DQ: a winner rejected by DQ does
  not resurrect a loser.
* A string that is empty after normalisation is null. Checks never run on null values.
  Cast failure (e.g. "abc" as int) = `reject_record` with rule `cast`. Normalizers: `trim` =
  str.strip, `lower`/`upper` = Python str.lower/upper, `strip_accents` = NFD, drop combining
  marks, NFC, `collapse_spaces` = runs of Unicode whitespace -> one space (no trim),
  `digits_only`/`alnum_only` keep ASCII only. Checks: `regex` = re.search (unanchored unless
  the pattern anchors), `in_set` on the canonical text of the normalised value, `min`/`max`
  inclusive, `max_length` in code points.
* All failing checks of all attributes are logged in `reject` (also `null_value` and `warn`
  actions). After a `null_value` failure the remaining checks of that attribute are skipped.
  Any `reject_record` failure rejects the whole record: no state change, no delta, the key counts
  as present in the snapshot (its last valid version is not deleted).
* `row_hash`: see `engine.core.canonical.row_hash_input`; revision is NOT hashed. Same hash on an
  active record = nothing happens (the stored revision is not refreshed either).
* A retired key that reappears (valid) is `I` (old_hash null) even with the same hash.
  `D` carries old_hash=row_hash, new_hash=null.
* Completeness (snapshot `full` only): ratio = |batch keys ∩ active keys| / |active keys|
  (batch keys include rejected rows); the check is skipped when there is no active key. Below
  `full_snapshot_min_ratio` (1e-9 tolerance) the WHOLE batch is blocked: no I/U/D, no rejects
  logged, `anomaly` row `batch_blocked`, `IngestResult.blocked = True`. Snapshots `delta` never
  delete.

Reconcile
---------
* `reconcile` processes every key changed since the previous reconcile (several ingests may be
  pending); a node is "in the delta" if it is in that set, whatever the net effect.
* Blocking: a key with a null part is not generated. A block (key name + value) is oversized when
  its number of ACTIVE records (all sources) is > `max_block_size`; it is not expanded (no
  candidate pairs from it) and an `oversized_block` anomaly is logged for each oversized block
  entered by a delta node or whose status flipped. Candidacy therefore depends only on the
  current state: a pair is a candidate iff it shares a non-oversized block. To keep that true
  incrementally, nodes in a block whose oversized status flipped (either way) are rescored too.
* Rescored set R = delta nodes + members of flipped blocks. Pairs with an endpoint in R are
  deleted and rebuilt; other pairs are untouched. All candidate pairs are stored in match_pair,
  including decision `none` and pairs with insufficient evidence (score null, decision `none`).
* Score = sum(w*s)/sum(w) over comparators whose two sides are non-null, in declared order.
  Active weight < min_evidence => score null. Every threshold comparison uses a 1e-9 tolerance
  in favour of inclusion (score >= accept - 1e-9). Contradiction is computed even when the score
  is null. Levenshtein on code points.
* Decision of a pair: eligible = score >= accept and no contradiction. For an eligible pair
  (a, b), a "candidate" count is taken PER SOURCE: n(a, source(b)) = number of eligible pairs
  of a with nodes of source(b), and n(b, source(a)) likewise. The pair is `match` iff both are
  exactly 1; otherwise `review` (reason `multiple_candidates`). Not eligible with score >=
  review: `review` (reason `contradiction` if contradictory, else `score_band`). Else `none`.
  Rationale: the contract says "un seul candidat éligible" without saying where; counting over
  all sources would make every `match` component at most a pair (a node with two eligible
  pairs would block both), so a golden could never gather 3 sources. Counting per source lets
  crm+erp+web duplicates cluster, while two crm records competing for one erp record stay in
  `review`. Same-source pairs follow the same rule (crm1~crm2 alone => `match`).
* The decisions of a pair depend on those counts, so decisions are recomputed for every pair
  touching a node whose pair neighbourhood changed (not only R).
* Affected set A = delta nodes + endpoints of pairs whose `match` status changed + all members of
  the current stagings of those nodes. Only A is reclustered (invariant: match components never
  cross the border of A; checked at runtime). A pair `match` -> removed/other decision is a change.
* Clustering = connected components of `match` pairs among ACTIVE nodes of A. `staging_member`
  contains active records only; retired records leave their staging.
* Staging ids: ids are int64 from a counter. For each old staging s the "heir" is the new
  component holding most of its members (tie: component with the smallest node). A component
  keeps the smallest id among the stagings whose heir it is (others: `merge` event, kept id in
  `staging_id`, absorbed id in `related_id`, absorbed staging retired). A component that is
  nobody's heir gets a new id: `split` event (staging_id = parent = old id with most members in
  it, related_id = new id) or `create` if it has no old member. Stagings without active member
  are retired (terminal: ids are never reused, a retired staging never revives). New ids are
  allocated in order of the smallest node of the component.

Survivorship / golden
---------------------
* priority: first declared source having an active member with a non-null value; several members
  of that source: greatest revision (null oldest), then smallest (source_id, source_key).
  most_recent: among all members with a non-null value, greatest revision, then smallest
  (source_id, source_key) (literal contract: no source-order step in a tie).
* A non-nullable attribute with no value blocks the golden: status `blocked`, previous values
  kept, `stale` = true iff it had been published before (a never-published blocked golden has
  null values and stale=false), anomaly `golden_blocked`, no golden_delta. Recomputed on every
  touched staging; unblocked => `published`, stale=false, deltas relative to the kept values.
* golden_delta: one row per attribute whose VALUE changes (old/new in canonical text, new
  winning source_id); a first publication yields old null rows for non-null values. A change of
  winning source with the same value updates `<attr>__src` without delta. Retirement and status
  changes produce no delta. A staging absorbed by a merge: its golden becomes `retired`.
* Only stagings produced by the recluster are recomputed.

Export / scratch
----------------
* `review` is state (current review pairs). `recompute_from_scratch()` replays the active
  records in one reconcile; the equivalence property compares STATE tables only
  (`compare_exports(..., state_only=True)`) since event tables are history.
"""

from .engine import IngestResult, ReconcileResult, ReferenceEngine

__all__ = ["IngestResult", "ReconcileResult", "ReferenceEngine"]
