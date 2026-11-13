"""Micro-benchmark: bulk COPY into AGE label tables vs per-row / batched Cypher (CREATE, MERGE).

    python -m engine.lanes.graph.bench --n 20000 --attrs 30

Starts its own `mdm2-graph-bench-*` container (6 GB cap), prints one JSON report, removes it.
All figures are rows/s measured on this machine for the given sizes; no extrapolation here.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from typing import Any

import psycopg

from . import _docker as dk

GRAPH = "bench_g"


def timed(fn) -> float:
    t0 = time.perf_counter()
    fn()
    return time.perf_counter() - t0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--n", type=int, default=20_000)
    p.add_argument("--small", type=int, default=2_000)
    p.add_argument("--attrs", type=int, default=30)
    p.add_argument("--report", default=None)
    args = p.parse_args(argv)

    container = dk.start("bench")
    report: dict[str, Any] = {
        "n": args.n,
        "small_n": args.small,
        "attrs": args.attrs,
        "rows_per_s": {},
    }
    try:
        conn = psycopg.connect(container.dsn(), autocommit=True)
        conn.execute("CREATE EXTENSION IF NOT EXISTS age")
        conn.execute("LOAD 'age'")
        conn.execute('SET search_path = ag_catalog, "$user", public')
        conn.execute("SELECT create_graph(%s)", (GRAPH,))
        labels: dict[str, int] = {}

        def vlabel(name: str) -> None:
            conn.execute("SELECT create_vlabel(%s, %s)", (GRAPH, name))
            labels[name] = conn.execute(
                "SELECT l.id FROM ag_label l JOIN ag_graph g ON g.graphid = l.graph"
                " WHERE g.name = %s AND l.name = %s",
                (GRAPH, name),
            ).fetchone()[0]

        def rate(name: str, rows: int, seconds: float) -> None:
            report["rows_per_s"][name] = {
                "rows": rows,
                "seconds": round(seconds, 3),
                "rows_per_s": round(rows / seconds),
            }

        attr_names = [f"a{i:02d}" for i in range(args.attrs)]

        def ident_props(i: int) -> str:
            return json.dumps({"v": i, "s": "crm", "k": f"key-{i}"})

        def full_props(i: int) -> str:
            d = {"v": i, "s": "crm", "k": f"key-{i}"}
            d.update({a: f"value-{i}-{j}" for j, a in enumerate(attr_names)})
            return json.dumps(d)

        def copy_vertices(label: str, props) -> None:
            seq = f'"{GRAPH}"."{label}_id_seq"'
            ids = [
                r[0]
                for r in conn.execute(
                    f"SELECT nextval('{seq}'::regclass) FROM generate_series(1, {args.n})"
                ).fetchall()
            ]
            with (
                conn.cursor() as cur,
                cur.copy(f'COPY "{GRAPH}"."{label}" (id, properties) FROM STDIN') as cp,
            ):
                for i, entry in enumerate(ids):
                    cp.write_row(((labels[label] << 48) | entry, props(i)))

        vlabel("CopyIdent")
        rate(
            "copy_identity_props_only",
            args.n,
            timed(lambda: copy_vertices("CopyIdent", ident_props)),
        )
        vlabel("CopyFull")
        rate(
            f"copy_{args.attrs}_attrs_in_agtype",
            args.n,
            timed(lambda: copy_vertices("CopyFull", full_props)),
        )

        def cypher(q: str) -> None:
            conn.execute(f"SELECT * FROM cypher('{GRAPH}', $$ {q} $$) AS (x agtype)")

        vlabel("BatchCreate")
        batches = args.n // 1000

        def unwind() -> None:
            conn.execute("BEGIN")
            for b in range(batches):
                items = ",".join(
                    "{v: %d, s: 'crm', k: 'key-%d'}" % (b * 1000 + i, b * 1000 + i)
                    for i in range(1000)
                )
                cypher(f"UNWIND [{items}] AS m CREATE (:BatchCreate {{v: m.v, s: m.s, k: m.k}})")
            conn.execute("COMMIT")

        rate("cypher_unwind_create_batches_of_1000", batches * 1000, timed(unwind))

        vlabel("SingleRow")

        def single_create() -> None:
            conn.execute("BEGIN")
            for i in range(args.small):
                cypher(f"CREATE (:SingleRow {{v: {i}, s: 'crm', k: 'key-{i}'}})")
            conn.execute("COMMIT")

        rate("cypher_create_one_row_per_statement", args.small, timed(single_create))

        # MERGE needs an index to avoid a sequential scan per row
        vlabel("MergeNoIdx")
        vlabel("MergeIdx")
        vlabel("MergeGin")
        seed = args.n

        def merge(label: str, count: int) -> None:
            conn.execute("BEGIN")
            for i in range(count):
                cypher(f"MERGE (:{label} {{k: 'key-{i % seed}'}})")
            conn.execute("COMMIT")

        for label in ("MergeNoIdx", "MergeIdx", "MergeGin"):
            seq = f'"{GRAPH}"."{label}_id_seq"'
            conn.execute(
                f'INSERT INTO "{GRAPH}"."{label}" (id, properties)'
                f" SELECT ag_catalog._graphid({labels[label]}, nextval('{seq}'::regclass)),"
                " ('{\"k\":\"key-' || i || '\"}')::text::agtype"
                f" FROM generate_series(0, {seed - 1}) i"
            )
        conn.execute("ANALYZE")
        expr = "ag_catalog.agtype_access_operator(properties, '\"k\"'::agtype)"
        conn.execute(f'CREATE INDEX ON "{GRAPH}"."MergeIdx" ({expr})')
        conn.execute(f'CREATE INDEX ON "{GRAPH}"."MergeGin" USING gin (properties)')
        rate(
            f"cypher_merge_one_row_per_statement_gin_index_{seed}_existing",
            args.small,
            timed(lambda: merge("MergeGin", args.small)),
        )
        rate(
            f"cypher_merge_one_row_per_statement_no_index_{seed}_existing",
            args.small,
            timed(lambda: merge("MergeNoIdx", args.small)),
        )
        rate(
            f"cypher_merge_one_row_per_statement_expression_index_{seed}_existing",
            args.small,
            timed(lambda: merge("MergeIdx", args.small)),
        )

        def unwind_merge(label: str) -> None:
            conn.execute("BEGIN")
            for b in range(args.small // 1000):
                keys = ",".join("'key-%d'" % (b * 1000 + i) for i in range(1000))
                cypher(f"UNWIND [{keys}] AS k MERGE (:{label} {{k: k}})")
            conn.execute("COMMIT")

        rate(
            "cypher_unwind_merge_batches_of_1000_expression_index",
            (args.small // 1000) * 1000,
            timed(lambda: unwind_merge("MergeIdx")),
        )
        rate(
            "cypher_unwind_merge_batches_of_1000_gin_index",
            (args.small // 1000) * 1000,
            timed(lambda: unwind_merge("MergeGin")),
        )

        # read-back of the payload: agtype properties vs a relational side table
        cols = ", ".join(f"n.{a}" for a in attr_names[: args.attrs])
        agt_cols = ", ".join(f"c{i} agtype" for i in range(args.attrs))
        t0 = time.perf_counter()
        rows = conn.execute(
            f"SELECT * FROM cypher('{GRAPH}', $$ MATCH (n:CopyFull) RETURN {cols} $$)"
            f" AS ({agt_cols})"
        ).fetchall()
        rate(f"read_{args.attrs}_attrs_from_agtype_via_cypher", len(rows), time.perf_counter() - t0)
        sql_cols = ", ".join(f"a{i} text" for i in range(args.attrs))
        conn.execute(f"CREATE TABLE side (id bigint, {sql_cols})")
        with conn.cursor() as cur, cur.copy("COPY side FROM STDIN") as cp:
            for i in range(args.n):
                cp.write_row((i, *[f"value-{i}-{j}" for j in range(args.attrs)]))
        t0 = time.perf_counter()
        rows = conn.execute("SELECT * FROM side").fetchall()
        rate(f"read_{args.attrs}_attrs_from_relational_table", len(rows), time.perf_counter() - t0)
        sizes = conn.execute(
            "SELECT relname, pg_total_relation_size(oid) FROM pg_class WHERE relname IN"
            " ('CopyIdent', 'CopyFull', 'side', 'BatchCreate') AND relkind = 'r'"
        ).fetchall()
        report["table_mb"] = {name: round(size / 2**20, 2) for name, size in sizes}
    finally:
        container.remove()
    text = json.dumps(report, indent=2)
    if args.report:
        open(args.report, "w", encoding="utf-8").write(text)
    print(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())
