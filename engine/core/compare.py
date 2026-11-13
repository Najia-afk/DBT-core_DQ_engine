"""Equivalence of two canonical exports, modulo staging/golden ids (lane certification).

`compare_exports(a, b)` compares two `dict[str, pyarrow.Table]` (see `engine.core.canonical`)
with DuckDB SQL over the Arrow tables (vectorised, no per-row Python). Identity of a node is
`(source_id, source_key)`; a staging/golden is identified by its partition: the smallest
`source_id:source_key` among its members (`rep`). Ids are never compared.

Checks (category in the report):
* schema: missing tables/columns, differing types (large_string == string).
* invariant: per side, a node in several stagings, a member that is not an active source
  record, an active record without staging, a non-retired golden without member, a staging with
  members but no golden.
* partition: same partition of nodes into stagings.
* source_record: same attribute values and revision (active records only when `state_only`;
  `row_hash` only with `compare_hash`; status/first_run/last_change_run otherwise).
* golden: per partition, same status; for non-`blocked` goldens the same attribute values,
  `<attr>__src` provenance (when both sides have the column) and `stale`. A blocked golden is
  only compared by status (its values/stale depend on history). Retired goldens are not compared
  by content (only their number when not `state_only`).
* match_pair / review: same pairs, scores within `score_tol`, decision, contradiction,
  rule_version / reason. `none` pairs are skipped unless `include_none_pairs` (a pair that is
  `match` on one side and `none` on the other is still reported as missing on the other side).
  `review.run_id` is not compared.
* Event tables, compared as multisets and only when `state_only` is False (they describe the
  history, which a cold recompute cannot reproduce): source_delta, reject, golden_delta (golden
  id replaced by the partition of its final staging, following merge redirects; ids without
  members show as `<unresolved>`), staging_event (run_id, kind), anomaly (when both exports have
  it).
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

import duckdb
import pyarrow as pa

from engine.core.canonical import REQUIRED_TABLES, TABLES

UNRESOLVED = "<unresolved>"
_GOLDEN_FIXED = {"golden_id", "status", "stale"}
_SOURCE_RECORD_FIXED = {
    "source_id",
    "source_key",
    "row_hash",
    "revision",
    "status",
    "first_run",
    "last_change_run",
}
_PAIR_KEYS = ["left_source_id", "left_key", "right_source_id", "right_key"]


@dataclass(frozen=True)
class Divergence:
    category: str
    kind: str
    key: str
    detail: str

    def __str__(self) -> str:
        return f"[{self.category}/{self.kind}] {self.key}: {self.detail}"


@dataclass
class Report:
    """Divergences found (details capped per category/kind; `totals` are exact counts)."""

    divergences: list[Divergence] = field(default_factory=list)
    totals: dict[str, int] = field(default_factory=dict)
    compared: dict[str, tuple[int, int]] = field(default_factory=dict)

    @property
    def ok(self) -> bool:
        return not self.totals

    def by_category(self, category: str) -> list[Divergence]:
        return [d for d in self.divergences if d.category == category]

    def format(self) -> str:
        if self.ok:
            return "exports equivalent"
        lines = [f"{sum(self.totals.values())} divergence(s): {dict(sorted(self.totals.items()))}"]
        lines += [f"  {d}" for d in self.divergences]
        return "\n".join(lines)

    def assert_equal(self) -> None:
        if not self.ok:
            raise AssertionError(self.format())


def compare_exports(
    a: Mapping[str, pa.Table],
    b: Mapping[str, pa.Table],
    *,
    state_only: bool = False,
    include_none_pairs: bool = False,
    compare_hash: bool = False,
    score_tol: float = 1e-9,
    max_details: int = 20,
) -> Report:
    return _Comparer(
        a, b, state_only, include_none_pairs, compare_hash, score_tol, max_details
    ).run()


def _ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _norm_type(typ: pa.DataType) -> str:
    return str(typ).replace("large_string", "string")


class _Comparer:
    def __init__(
        self,
        a: Mapping[str, pa.Table],
        b: Mapping[str, pa.Table],
        state_only: bool,
        include_none_pairs: bool,
        compare_hash: bool,
        score_tol: float,
        max_details: int,
    ) -> None:
        self.tables = {"a": dict(a), "b": dict(b)}
        self.state_only = state_only
        self.include_none = include_none_pairs
        self.compare_hash = compare_hash
        self.tol = score_tol
        self.max = max_details
        self.report = Report()
        self.has_partition = False
        self.con = duckdb.connect()
        self._detail_counts: dict[tuple[str, str], int] = defaultdict(int)
        for side, tables in self.tables.items():
            for name, table in tables.items():
                if name in TABLES:
                    self.con.register(f"{side}_{name}", table)

    # ---------------------------------------------------------------- reporting

    def _total(self, category: str, n: int) -> None:
        if n:
            self.report.totals[category] = self.report.totals.get(category, 0) + n

    def _detail(self, category: str, kind: str, key: str, detail: str) -> None:
        if self._detail_counts[(category, kind)] < self.max:
            self._detail_counts[(category, kind)] += 1
            self.report.divergences.append(Divergence(category, kind, key, detail))

    def _add(self, category: str, kind: str, key: str, detail: str) -> None:
        self._total(category, 1)
        self._detail(category, kind, key, detail)

    def _rows(self, sql: str) -> list[tuple[Any, ...]]:
        return self.con.execute(sql).fetchall()

    def _scalar(self, sql: str) -> int:
        return int(self.con.execute(sql).fetchone()[0])

    # ------------------------------------------------------------------ schema

    def _cols(self, side: str, table: str) -> dict[str, str]:
        return {f.name: _norm_type(f.type) for f in self.tables[side][table].schema}

    def _usable(self, table: str, columns: list[str]) -> bool:
        for side in "ab":
            if table not in self.tables[side]:
                return False
            if not set(columns) <= set(self._cols(side, table)):
                return False
        return True

    def _check_schema(self) -> None:
        for table in TABLES:
            present = {s: table in self.tables[s] for s in "ab"}
            for side in "ab":
                if not present[side] and table in REQUIRED_TABLES:
                    self._add("schema", "missing_table", table, f"missing in {side}")
            if not all(present.values()):
                continue
            ca, cb = self._cols("a", table), self._cols("b", table)
            for col in sorted(set(ca) - set(cb)):
                self._add("schema", "missing_column", f"{table}.{col}", "only in a")
            for col in sorted(set(cb) - set(ca)):
                self._add("schema", "missing_column", f"{table}.{col}", "only in b")
            for col in sorted(set(ca) & set(cb)):
                if ca[col] != cb[col]:
                    self._add("schema", "type", f"{table}.{col}", f"a={ca[col]} b={cb[col]}")
            self.report.compared[table] = (
                self.tables["a"][table].num_rows,
                self.tables["b"][table].num_rows,
            )

    # ------------------------------------------------------------- generic diff

    @staticmethod
    def _expr(alias: str, col: str, cast: set[str]) -> str:
        expr = f"{alias}.{_ident(col)}"
        return f"CAST({expr} AS VARCHAR)" if col in cast else expr

    def _select(self, alias: str, col: str, cast: set[str]) -> str:
        return f"{self._expr(alias, col, cast)} AS {_ident(col)}"

    def _diff(
        self,
        category: str,
        keys: list[str],
        left_sql: str,
        right_sql: str,
        cols: list[str],
        tol_cols: tuple[str, ...] = (),
        key_format: str | None = None,
        gate: str | None = None,
    ) -> None:
        """Full outer join on keys; report missing rows and differing columns.

        With `gate`, the other columns are compared only when the gate column is equal.

        `left_sql`/`right_sql` select the keys and cols (same names on both sides).
        """
        con = self.con
        con.execute(f"CREATE OR REPLACE TEMP TABLE _l AS {left_sql}")
        con.execute(f"CREATE OR REPLACE TEMP TABLE _r AS {right_sql}")
        key_list = ", ".join(_ident(k) for k in keys)
        for side in ("l", "r"):
            dups = self._scalar(
                f"SELECT count(*) FROM (SELECT 1 FROM _{side} GROUP BY {key_list}"
                " HAVING count(*) > 1)"
            )
            if dups:
                self._add(category, "duplicate_key", "a" if side == "l" else "b", f"{dups} keys")
        k_sel = ", ".join(
            f"coalesce(l.{_ident(k)}, r.{_ident(k)}) AS k{i}" for i, k in enumerate(keys)
        )
        c_sel = ", ".join(
            f"l.{_ident(c)} AS a{i}, r.{_ident(c)} AS b{i}" for i, c in enumerate(cols)
        )
        on = " AND ".join(f"l.{_ident(k)} = r.{_ident(k)}" for k in keys)
        con.execute(
            f"CREATE OR REPLACE TEMP TABLE _j AS SELECT {k_sel}, l._p AS _l, r._p AS _r"
            + (f", {c_sel}" if cols else "")
            + f" FROM (SELECT *, 1 AS _p FROM _l) l FULL OUTER JOIN (SELECT *, 1 AS _p FROM _r) r"
            f" ON {on}"
        )
        k_names = ", ".join(f"k{i}" for i in range(len(keys)))

        def fmt(row: tuple[Any, ...]) -> str:
            text = ":".join(str(v).replace("\x1f", ":") for v in row)
            return f"{key_format} {text}" if key_format else text

        def pred(i: int, col: str) -> str:
            if col in tol_cols:
                return f"((a{i} IS NULL) <> (b{i} IS NULL) OR abs(a{i} - b{i}) > {self.tol})"
            return f"a{i} IS DISTINCT FROM b{i}"

        both = "_l IS NOT NULL AND _r IS NOT NULL"
        gate_index = cols.index(gate) if gate else -1

        def guard(i: int) -> str:
            return both if gate_index in (-1, i) else f"{both} AND a{gate_index} = b{gate_index}"

        aggregates = [
            "count(*) FILTER (WHERE _r IS NULL)",
            "count(*) FILTER (WHERE _l IS NULL)",
            *(f"count(*) FILTER (WHERE {guard(i)} AND {pred(i, c)})" for i, c in enumerate(cols)),
        ]
        counts = self._rows("SELECT " + ", ".join(aggregates) + " FROM _j")[0]
        for kind, count, cond in (
            ("only_in_a", counts[0], "_r IS NULL"),
            ("only_in_b", counts[1], "_l IS NULL"),
        ):
            self._total(category, count)
            if count:
                for row in self._rows(
                    f"SELECT {k_names} FROM _j WHERE {cond} ORDER BY {k_names} LIMIT {self.max}"
                ):
                    self._detail(category, kind, fmt(row), f"row only in {kind[-1]}")
        for i, col in enumerate(cols):
            count = counts[2 + i]
            self._total(category, count)
            if not count:
                continue
            for row in self._rows(
                f"SELECT {k_names}, a{i}, b{i} FROM _j WHERE {guard(i)} AND {pred(i, col)}"
                f" ORDER BY {k_names} LIMIT {self.max}"
            ):
                self._detail(
                    category,
                    f"different:{col}",
                    fmt(row[:-2]),
                    f"{col}: a={row[-2]!r} b={row[-1]!r}",
                )

    def _multiset(self, category: str, cols: list[str], left_sql: str, right_sql: str) -> None:
        col_list = ", ".join(_ident(c) for c in cols)
        for kind, one, other in (
            ("only_in_a", left_sql, right_sql),
            ("only_in_b", right_sql, left_sql),
        ):
            rows = self._rows(
                f"SELECT {col_list}, count(*) AS n FROM (SELECT {col_list} FROM ({one}) EXCEPT ALL"
                f" SELECT {col_list} FROM ({other})) GROUP BY ALL ORDER BY ALL"
            )
            for row in rows:
                self._total(category, row[-1])
                self._detail(
                    category,
                    kind,
                    ":".join(str(v) for v in row[:-1]),
                    f"{row[-1]} row(s) only in {kind[-1]}",
                )

    # --------------------------------------------------------------- invariants

    def _prepare_partition(self) -> bool:
        if not self._usable("staging_member", ["staging_id", "source_id", "source_key"]):
            return False
        con = self.con
        for s in "ab":
            con.execute(
                f"CREATE OR REPLACE TEMP TABLE {s}_rep AS SELECT staging_id,"
                f" min(source_id || chr(31) || source_key) AS rep, count(*) AS n"
                f" FROM {s}_staging_member GROUP BY staging_id"
            )
            con.execute(
                f"CREATE OR REPLACE TEMP TABLE {s}_node AS SELECT m.source_id, m.source_key,"
                f" min(m.staging_id) AS staging_id, count(*) AS c FROM {s}_staging_member m"
                f" GROUP BY m.source_id, m.source_key"
            )
            con.execute(
                f"CREATE OR REPLACE TEMP TABLE {s}_part AS SELECT n.source_id, n.source_key,"
                f" r.rep, r.n FROM {s}_node n JOIN {s}_rep r USING (staging_id)"
            )
            dups = self._scalar(f"SELECT count(*) FROM {s}_node WHERE c > 1")
            if dups:
                self._add("invariant", f"{s}:node_in_several_stagings", s, f"{dups} nodes")
        return True

    def _check_invariants(self) -> None:
        for s in "ab":
            if self._usable("source_record", ["source_id", "source_key", "status"]):
                ghosts = self._scalar(
                    f"SELECT count(*) FROM {s}_part p LEFT JOIN {s}_source_record r"
                    f" ON p.source_id = r.source_id AND p.source_key = r.source_key"
                    f" AND r.status = 'active' WHERE r.source_id IS NULL"
                )
                if ghosts:
                    self._add("invariant", f"{s}:member_not_active", s, f"{ghosts} members")
                orphans = self._scalar(
                    f"SELECT count(*) FROM {s}_source_record r LEFT JOIN {s}_part p"
                    f" ON p.source_id = r.source_id AND p.source_key = r.source_key"
                    f" WHERE r.status = 'active' AND p.source_id IS NULL"
                )
                if orphans:
                    self._add(
                        "invariant", f"{s}:active_record_without_staging", s, f"{orphans} records"
                    )
            if self._usable("golden", ["golden_id", "status"]):
                memberless = self._scalar(
                    f"SELECT count(*) FROM {s}_golden g LEFT JOIN {s}_rep r"
                    f" ON r.staging_id = g.golden_id WHERE g.status <> 'retired' AND r.rep IS NULL"
                )
                if memberless:
                    self._add(
                        "invariant", f"{s}:golden_without_members", s, f"{memberless} goldens"
                    )
                nogolden = self._scalar(
                    f"SELECT count(*) FROM {s}_rep r LEFT JOIN {s}_golden g"
                    f" ON r.staging_id = g.golden_id WHERE g.golden_id IS NULL"
                )
                if nogolden:
                    self._add("invariant", f"{s}:staging_without_golden", s, f"{nogolden} stagings")

    # ------------------------------------------------------------------ content

    def _compare_partition(self) -> None:
        con = self.con
        con.execute(
            "CREATE OR REPLACE TEMP TABLE _pd AS SELECT coalesce(x.source_id, y.source_id) AS sid,"
            " coalesce(x.source_key, y.source_key) AS skey, x.rep AS rep_a, y.rep AS rep_b,"
            " x.n AS n_a, y.n AS n_b FROM a_part x FULL OUTER JOIN b_part y"
            " ON x.source_id = y.source_id AND x.source_key = y.source_key"
            " WHERE x.rep IS DISTINCT FROM y.rep"
        )
        show = lambda col: f"replace({col}, chr(31), ':')"  # noqa: E731
        for kind, cond, text in (
            ("node_only_in_a", "rep_b IS NULL", "node has no staging in b"),
            ("node_only_in_b", "rep_a IS NULL", "node has no staging in a"),
        ):
            n = self._scalar(f"SELECT count(*) FROM _pd WHERE {cond}")
            self._total("partition", n)
            for sid, skey in self._rows(
                f"SELECT sid, skey FROM _pd WHERE {cond} ORDER BY sid, skey LIMIT {self.max}"
            ):
                self._detail("partition", kind, f"{sid}:{skey}", text)
        groups = self._rows(
            f"SELECT {show('rep_a')}, {show('rep_b')}, any_value(n_a), any_value(n_b), count(*),"
            f" list(sid || ':' || skey ORDER BY sid, skey)[1:3] FROM _pd"
            f" WHERE rep_a IS NOT NULL AND rep_b IS NOT NULL GROUP BY rep_a, rep_b"
            f" ORDER BY rep_a, rep_b"
        )
        for rep_a, rep_b, n_a, n_b, count, sample in groups:
            self._total("partition", count)
            self._detail(
                "partition",
                "different_grouping",
                f"a:{rep_a} b:{rep_b}",
                f"{count} node(s) grouped differently (staging sizes a={n_a} b={n_b}),"
                f" e.g. {sample}",
            )

    def _compare_source_record(self) -> None:
        if not self._usable("source_record", ["source_id", "source_key", "status", "revision"]):
            return
        attrs = sorted(
            (set(self._cols("a", "source_record")) & set(self._cols("b", "source_record")))
            - _SOURCE_RECORD_FIXED
        )
        cols = ["revision", *attrs]
        if self.compare_hash:
            cols.append("row_hash")
        if not self.state_only:
            cols += ["status", "first_run", "last_change_run"]
        sql = {}
        cast = self._cast_cols("source_record", cols)
        where = " WHERE status = 'active'" if self.state_only else ""
        for s in "ab":
            select = ", ".join(
                ["source_id", "source_key", *(self._select("t", c, cast) for c in cols)]
            )
            sql[s] = f"SELECT {select} FROM {s}_source_record t{where}"
        self._diff("source_record", ["source_id", "source_key"], sql["a"], sql["b"], cols)

    def _cast_cols(self, table: str, cols: list[str]) -> set[str]:
        ca, cb = self._cols("a", table), self._cols("b", table)
        return {c for c in cols if c in ca and c in cb and ca[c] != cb[c]}

    def _compare_golden(self) -> None:
        if not self.has_partition or not self._usable("golden", ["golden_id", "status", "stale"]):
            return
        ca, cb = self._cols("a", "golden"), self._cols("b", "golden")
        common = (set(ca) & set(cb)) - _GOLDEN_FIXED
        cols = ["status", "stale", *sorted(common)]
        cast = self._cast_cols("golden", cols)
        sql = {}
        for s in "ab":
            select = []
            for c in cols:
                expr = self._expr("g", c, cast)
                if c != "status":
                    expr = f"CASE WHEN g.status = 'blocked' THEN NULL ELSE {expr} END"
                select.append(f"{expr} AS {_ident(c)}")
            sql[s] = (
                f"SELECT r.rep AS rep, {', '.join(select)} FROM {s}_golden g"
                f" JOIN {s}_rep r ON r.staging_id = g.golden_id WHERE g.status <> 'retired'"
            )
        self._diff(
            "golden", ["rep"], sql["a"], sql["b"], cols, key_format="partition", gate="status"
        )
        if not self.state_only:
            counts = [
                self._scalar(f"SELECT count(*) FROM {s}_golden WHERE status = 'retired'")
                for s in "ab"
            ]
            if counts[0] != counts[1]:
                self._add("golden", "retired_count", "retired", f"a={counts[0]} b={counts[1]}")

    def _compare_pairs(self) -> None:
        flt = "" if self.include_none else " WHERE decision <> 'none'"
        if self._usable(
            "match_pair", [*_PAIR_KEYS, "score", "decision", "contradiction", "rule_version"]
        ):
            cols = ["score", "decision", "contradiction", "rule_version"]
            cast = self._cast_cols("match_pair", cols)
            sql = {
                s: f"SELECT {', '.join(_PAIR_KEYS)}, "
                + ", ".join(self._select("t", c, cast) for c in cols)
                + f" FROM {s}_match_pair t{flt}"
                for s in "ab"
            }
            self._diff(
                "match_pair",
                _PAIR_KEYS,
                sql["a"],
                sql["b"],
                cols,
                tol_cols=tuple(c for c in ("score",) if c not in cast),
            )
        if self._usable("review", [*_PAIR_KEYS, "score", "reason"]):
            cols = ["score", "reason"]
            cast = self._cast_cols("review", cols)
            sql = {
                s: f"SELECT {', '.join(_PAIR_KEYS)}, "
                + ", ".join(self._select("t", c, cast) for c in cols)
                + f" FROM {s}_review t"
                for s in "ab"
            }
            self._diff(
                "review",
                _PAIR_KEYS,
                sql["a"],
                sql["b"],
                cols,
                tol_cols=tuple(c for c in ("score",) if c not in cast),
            )

    def _compare_events(self) -> None:
        if self._usable("source_delta", ["run_id", "source_id", "source_key", "kind"]):
            cols = ["run_id", "source_id", "source_key", "kind"]
            if self.compare_hash:
                cols += ["old_hash", "new_hash"]
            self._multiset("source_delta", cols, *(f"SELECT * FROM {s}_source_delta" for s in "ab"))
        if self._usable(
            "reject", ["run_id", "source_id", "source_key", "attribute", "rule", "action"]
        ):
            cols = ["run_id", "source_id", "source_key", "attribute", "rule", "action"]
            self._multiset("reject", cols, *(f"SELECT * FROM {s}_reject" for s in "ab"))
        if self._usable("staging_event", ["run_id", "kind"]):
            self._multiset(
                "staging_event",
                ["run_id", "kind"],
                *(f"SELECT * FROM {s}_staging_event" for s in "ab"),
            )
        if self._usable("anomaly", ["run_id", "kind", "subject"]):
            self._multiset(
                "anomaly",
                ["run_id", "kind", "subject"],
                *(f"SELECT * FROM {s}_anomaly" for s in "ab"),
            )
        self._compare_golden_delta()

    def _compare_golden_delta(self) -> None:
        needed = ["run_id", "golden_id", "attribute", "old_value", "new_value", "winning_source"]
        if not (
            self.has_partition
            and self._usable("golden_delta", needed)
            and self._usable("staging_event", ["kind", "staging_id", "related_id"])
        ):
            return
        con = self.con
        for s in "ab":
            con.execute(
                f"CREATE OR REPLACE TEMP TABLE {s}_redirect AS SELECT related_id AS src,"
                f" staging_id AS dst FROM {s}_staging_event WHERE kind = 'merge'"
            )
            con.execute(
                f"CREATE OR REPLACE TEMP TABLE {s}_res AS SELECT DISTINCT golden_id AS id,"
                f" golden_id AS cur FROM {s}_golden_delta"
            )
            for _ in range(10_000):
                updated = con.execute(
                    f"UPDATE {s}_res SET cur = d.dst FROM {s}_redirect d WHERE {s}_res.cur = d.src"
                ).fetchone()[0]
                if not updated:
                    break
        sql = [
            f"SELECT g.run_id, coalesce(replace(r.rep, chr(31), ':'), '{UNRESOLVED}') AS golden,"
            f" g.attribute, g.old_value, g.new_value, g.winning_source FROM {s}_golden_delta g"
            f" JOIN {s}_res x ON x.id = g.golden_id LEFT JOIN {s}_rep r ON r.staging_id = x.cur"
            for s in "ab"
        ]
        self._multiset(
            "golden_delta",
            ["run_id", "golden", "attribute", "old_value", "new_value", "winning_source"],
            *sql,
        )

    def run(self) -> Report:
        self._check_schema()
        self.has_partition = self._guard("prepare_partition", self._prepare_partition) is True
        steps = [self._compare_source_record, self._compare_golden, self._compare_pairs]
        if self.has_partition:
            steps = [self._check_invariants, self._compare_partition, *steps]
        if not self.state_only:
            steps.append(self._compare_events)
        for step in steps:
            self._guard(step.__name__, step)
        self.con.close()
        return self.report

    def _guard(self, name: str, step: Any) -> Any:
        """A SQL failure (incompatible column types...) is a divergence, not a crash."""
        try:
            return step()
        except duckdb.Error as error:
            self._add("error", "sql", name, str(error).splitlines()[0])
            return None
