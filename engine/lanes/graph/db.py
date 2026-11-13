"""Connection, naming and low-level AGE helpers (bulk COPY into label tables, Cypher reads)."""

from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass, field
from typing import Any

import numpy as np
import psycopg

from engine.core.config import ReferentialConfig, load_config

LABEL_BITS = 48
ENTRY_MASK = (1 << LABEL_BITS) - 1
VERTEX_LABELS = ("Record", "Block", "Staging", "Golden")
EDGE_LABELS = ("IN_BLOCK", "MATCH", "REVIEW", "MEMBER_OF", "PUBLISHED_AS")
CYPHER_CHUNK = 8000


class LaneError(RuntimeError):
    """Expected, reportable failure (configuration, environment, drift)."""


def q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def graph_name(cfg: ReferentialConfig) -> str:
    name = f"{cfg.state.schema_name}_g"
    if len(name) > 63:
        raise LaneError(f"state schema {cfg.state.schema_name!r} is too long for an AGE graph name")
    return name


def state_dsn(cfg: ReferentialConfig) -> str:
    dsn = os.environ.get(cfg.state.dsn_env)
    if not dsn:
        raise LaneError(f"environment variable {cfg.state.dsn_env} is not set")
    return dsn


def connect(dsn: str) -> psycopg.Connection:
    """Autocommit connection: transactions are explicit (`with conn.transaction()`)."""
    conn = psycopg.connect(dsn, autocommit=True)
    try:
        conn.execute("LOAD 'age'")
    except psycopg.Error as error:
        conn.close()
        raise LaneError(f"cannot LOAD 'age' (is the AGE extension installed?): {error}") from error
    conn.execute('SET search_path = ag_catalog, "$user", public')
    return conn


def load_cfg(path: str) -> ReferentialConfig:
    try:
        return load_config(path)
    except ValueError as error:
        raise LaneError(str(error)) from error


@dataclass
class Ctx:
    """Everything a phase needs: config, open (transactional) connection, graph metadata."""

    cfg: ReferentialConfig
    conn: psycopg.Connection
    schema: str = ""
    graph: str = ""
    labels: dict[str, int] = field(default_factory=dict)
    counters: dict[str, int] = field(default_factory=dict)
    timings: dict[str, float] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.schema = self.cfg.state.schema_name
        self.graph = graph_name(self.cfg)

    def load_labels(self) -> None:
        rows = self.conn.execute(
            "SELECT l.name, l.id FROM ag_catalog.ag_label l JOIN ag_catalog.ag_graph g"
            " ON g.graphid = l.graph WHERE g.name = %s",
            (self.graph,),
        ).fetchall()
        self.labels = {name: int(lid) for name, lid in rows}
        missing = [n for n in (*VERTEX_LABELS, *EDGE_LABELS) if n not in self.labels]
        if missing:
            raise LaneError(f"graph {self.graph} is not initialised (missing labels {missing})")

    def count(self, key: str, n: int = 1) -> None:
        self.counters[key] = self.counters.get(key, 0) + int(n)

    def t(self, relation: str) -> str:
        """Qualified relational table of the state schema."""
        return f"{q(self.schema)}.{q(relation)}"

    def lt(self, label: str) -> str:
        """Qualified AGE label table."""
        return f"{q(self.graph)}.{q(label)}"

    # ------------------------------------------------------------------ ids

    def alloc(self, label: str, n: int) -> np.ndarray:
        """Reserve `n` graphids of a label (AGE sequence, safe under concurrency)."""
        if n <= 0:
            return np.zeros(0, dtype=np.int64)
        seq = f"{q(self.graph)}.{q(label + '_id_seq')}"
        rows = self.conn.execute(
            f"SELECT nextval('{seq}'::regclass) FROM generate_series(1, %s)", (n,)
        ).fetchall()
        entries = np.fromiter((r[0] for r in rows), dtype=np.int64, count=n)
        return (np.int64(self.labels[label]) << LABEL_BITS) | entries

    # ------------------------------------------------------------- bulk writes

    def copy_vertices(self, label: str, vids: np.ndarray, props: list[str]) -> None:
        with self.conn.cursor() as cur:
            with cur.copy(f"COPY {self.lt(label)} (id, properties) FROM STDIN") as cp:
                for vid, p in zip(vids.tolist(), props, strict=True):
                    cp.write_row((vid, p))
        self.count(f"vertices_{label}", len(props))
        self._maybe_analyze(label, len(props))

    def copy_edges(
        self, label: str, starts: np.ndarray, ends: np.ndarray, props: list[str]
    ) -> np.ndarray:
        eids = self.alloc(label, len(props))
        with self.conn.cursor() as cur:
            with cur.copy(
                f"COPY {self.lt(label)} (id, start_id, end_id, properties) FROM STDIN"
            ) as cp:
                for row in zip(eids.tolist(), starts.tolist(), ends.tolist(), props, strict=True):
                    cp.write_row(row)
        self.count(f"edges_{label}", len(props))
        self._maybe_analyze(label, len(props))
        return eids

    def _maybe_analyze(self, label: str, n: int) -> None:
        # the planner has no statistics for rows written in the current transaction
        if n >= 5000:
            self.conn.execute(f"ANALYZE {self.lt(label)}")

    def temp_ids(self, name: str, values: Any, columns: str = "v graphid") -> None:
        """Temp table filled by COPY (ids as graphid text)."""
        self.conn.execute(f"DROP TABLE IF EXISTS {name}")
        self.conn.execute(f"CREATE TEMP TABLE {name} ({columns}) ON COMMIT DROP")
        with self.conn.cursor() as cur:
            with cur.copy(f"COPY {name} FROM STDIN") as cp:
                for row in values:
                    cp.write_row(row if isinstance(row, tuple) else (row,))

    def delete_edges_by_ids(self, label: str, eids: list[int]) -> int:
        if not eids:
            return 0
        self.temp_ids("_del_e", eids)
        cur = self.conn.execute(f"DELETE FROM {self.lt(label)} e USING _del_e d WHERE e.id = d.v")
        self.count(f"edges_deleted_{label}", cur.rowcount)
        return cur.rowcount

    def delete_edges_by_pair(self, label: str, pairs: list[tuple[int, int]]) -> int:
        if not pairs:
            return 0
        self.temp_ids("_del_p", pairs, "s graphid, e graphid")
        cur = self.conn.execute(
            f"DELETE FROM {self.lt(label)} x USING _del_p d"
            " WHERE x.start_id = d.s AND x.end_id = d.e"
        )
        self.count(f"edges_deleted_{label}", cur.rowcount)
        return cur.rowcount

    def delete_edges_by_start(self, label: str, starts: list[int]) -> int:
        if not starts:
            return 0
        self.temp_ids("_del_s", starts)
        cur = self.conn.execute(
            f"DELETE FROM {self.lt(label)} x USING _del_s d WHERE x.start_id = d.v"
        )
        self.count(f"edges_deleted_{label}", cur.rowcount)
        return cur.rowcount

    def update_edge_props(self, label: str, eids: list[int], props: list[str]) -> None:
        if not eids:
            return
        self.temp_ids("_upd_e", list(zip(eids, props, strict=True)), "v graphid, p text")
        cur = self.conn.execute(
            f"UPDATE {self.lt(label)} e SET properties = u.p::text::agtype FROM _upd_e u"
            " WHERE e.id = u.v"
        )
        self.count(f"edges_updated_{label}", cur.rowcount)

    # ------------------------------------------------------------------ Cypher

    def cypher(self, query: str, kinds: tuple[str, ...]) -> list[tuple[Any, ...]]:
        """Run a read query; `kinds` per returned column: int | float | bool | str | json."""
        exprs = []
        for i, kind in enumerate(kinds):
            col = f"c{i}"
            if kind == "int":
                exprs.append(f"({col}::text)::bigint")
            elif kind == "float":
                exprs.append(f"({col}::text)::float8")
            elif kind == "bool":
                exprs.append(f"({col}::text)::boolean")
            else:
                exprs.append(f"{col}::text")
        cols = ", ".join(f"c{i} ag_catalog.agtype" for i in range(len(kinds)))
        sql = (
            f"SELECT {', '.join(exprs)} FROM ag_catalog.cypher('{self.graph}', $q${query}$q$)"
            f" AS ({cols})"
        )
        self.count("cypher_queries")
        return self.conn.execute(sql).fetchall()

    def cypher_in(
        self, template: str, ids: list[int], kinds: tuple[str, ...], chunk: int = CYPHER_CHUNK
    ) -> list[tuple[Any, ...]]:
        """`template` contains `@IDS@` (a Cypher integer list); runs per chunk of ids."""
        out: list[tuple[Any, ...]] = []
        for start in range(0, len(ids), chunk):
            part = ids[start : start + chunk]
            out.extend(
                self.cypher(template.replace("@IDS@", "[" + ",".join(map(str, part)) + "]"), kinds)
            )
        return out

    def cypher_both(
        self, edge: str, tail: str, ids: list[int], kinds: tuple[str, ...]
    ) -> list[tuple[Any, ...]]:
        """Undirected expansion from `a` (`a.v IN ids`) as two directed queries.

        AGE plans an undirected edge pattern as a cross join filtered by
        `(start=a AND end=b) OR (end=a AND start=b)` (quadratic); directed patterns use the
        start_id/end_id indexes. Edges between two ids appear once per direction.
        """
        out: list[tuple[Any, ...]] = []
        for pattern in (f"(a:Record)-[{edge}]->(b:Record)", f"(a:Record)<-[{edge}]-(b:Record)"):
            out.extend(self.cypher_in(f"MATCH {pattern} WHERE a.v IN @IDS@ {tail}", ids, kinds))
        return out

    # ------------------------------------------------------------------- misc

    def log_step(self, run_id: str, step: str, status: str, started: Any, counts: dict) -> None:
        self.conn.execute(
            f"INSERT INTO {self.t('run_log')} (run_id, referential, step, status, started_at,"
            " ended_at, counts, error) VALUES (%s, %s, %s, %s, %s, now(), %s::jsonb, NULL)",
            (run_id, self.cfg.referential, step, status, started, json.dumps(counts)),
        )

    def lock(self, exclusive: bool) -> None:
        key = int.from_bytes(hashlib.sha256(self.schema.encode()).digest()[:7], "big")
        fn = "pg_advisory_xact_lock" if exclusive else "pg_advisory_xact_lock_shared"
        self.conn.execute(f"SELECT {fn}(%s)", (key,))

    def done_counts(self, run_id: str, step: str) -> dict | None:
        row = self.conn.execute(
            f"SELECT counts FROM {self.t('run_log')} WHERE run_id = %s AND step = %s"
            " AND status IN ('done', 'blocked')",
            (run_id, step),
        ).fetchone()
        return None if row is None else row[0]
