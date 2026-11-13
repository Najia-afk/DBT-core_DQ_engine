"""`check-source` : profile une source réelle contre le mapping, en agrégats SQL.

Aucune ligne n'est rapatriée : une requête (plusieurs seulement au-delà de `max_aggs`
agrégats) renvoie une seule ligne de compteurs.
"""

from __future__ import annotations

import difflib
from collections.abc import Callable

from engine.cli.sqlbackends import (
    COMPATIBLE,
    ORDERABLE,
    TEXT,
    Backend,
    DuckBackend,
    PgBackend,
    Unit,
    family,
    literal,
    quote,
)
from engine.core.config import Attribute, Check, ReferentialConfig, Source

BLOCK_RATE = 0.5  # au-delà, la colonne n'est très probablement pas la bonne
ENGINE_NAME = {"duckdb": "DuckDB", "postgres": "PostgreSQL"}

Add = Callable[..., None]


def open_backend(src: Source) -> Backend:
    if src.kind == "parquet":
        return DuckBackend(src.location)
    return PgBackend(src.location, src.dsn_env or "")


def _suggest(name: str, columns: list[str]) -> str:
    close = difflib.get_close_matches(name, columns, n=2, cutoff=0.6)
    close += [c for c in columns if c.lower() == name.lower() and c not in close]
    return f" Did you mean {' or '.join(repr(c) for c in close[:2])}?" if close else ""


def _rate(part: int, whole: int) -> float:
    return round(part / whole, 6) if whole else 0.0


def _pct(part: int, whole: int) -> str:
    return f"{_rate(part, whole):.1%}"


def _condition(backend: Backend, attr: Attribute, chk: Check, v: str) -> str:
    d = backend.dialect
    if chk.rule == "regex":
        return d.regex_match(v, str(chk.value))
    if chk.rule == "in_set":
        text = v if attr.type == "string" else d.text(v)
        return f"{text} IN ({', '.join(literal(str(x)) for x in chk.value)})"
    if chk.rule == "max_length":
        return f"length({v}) <= {int(chk.value)}"
    return f"{v} {'>=' if chk.rule == 'min' else '<='} {chk.value!r}"


def _failing(backend: Backend, attr: Attribute, chk: Check, v: str) -> str:
    return f"{v} IS NOT NULL AND NOT ({_condition(backend, attr, chk, v)})"


def _attr_unit(backend: Backend, i: int, attr: Attribute, col: str, native: str, checks: list):
    d, fam, qcol = backend.dialect, family(native), quote(col)
    if attr.type == "string":
        raw = qcol if fam == TEXT else d.text(qcol)
        value = raw
        for name in attr.normalize:
            value = d.normalize(name, value)
    else:
        raw, value = qcol, d.cast(qcol, fam, attr.type)
    aggs = {f"a{i}_nn": f"count(r{i})"}
    if attr.type != "string":
        aggs[f"a{i}_cf"] = f"count(r{i}) - count(v{i})"
    else:
        aggs[f"a{i}_empty"] = f"count(*) FILTER (WHERE v{i} = '')"
        if attr.normalize:
            aggs[f"a{i}_db"] = d.distinct_count(f"r{i}")
            aggs[f"a{i}_da"] = d.distinct_count(f"v{i}")
    for j, chk in checks:
        aggs[f"a{i}_c{j}"] = f"count(*) FILTER (WHERE {_failing(backend, attr, chk, f'v{i}')})"
    return Unit({f"r{i}": raw, f"v{i}": value}, aggs)


def _size(unit: Unit) -> int:
    return len(unit.aggs) + len(unit.exact)


def _scan(src: str, inner: dict[str, str], cap: str) -> str:
    cols = ", ".join(f"{e} AS {a}" for a, e in inner.items())
    # OFFSET 0 : barrière d'optimisation. PostgreSQL n'aplatit pas la sous-requête (il
    # recalculerait chaque expression à chaque agrégat) ; DuckDB lit les colonnes une fois.
    return f"(SELECT {cols} FROM {src}{cap} OFFSET 0)"


def _run(backend: Backend, units: list[Unit], sample: int | None) -> dict:
    """Une seule instruction SQL, une seule ligne de compteurs (plusieurs au-delà de max_aggs)."""
    limit = backend.dialect.max_aggs
    chunks: list[list[Unit]] = [[]]
    for unit in units:
        if chunks[-1] and sum(_size(u) for u in chunks[-1]) + _size(unit) > limit:
            chunks.append([])
        chunks[-1].append(unit)
    cap = f" LIMIT {int(sample)}" if sample else ""
    out: dict = {}
    for position, chunk in enumerate(chunks):
        inner: dict[str, str] = {"_one": "1"}
        aggs: dict[str, str] = {"n": "count(*)"} if position == 0 else {}
        exact: dict[str, str] = {}
        exact_inner: dict[str, str] = {"_one": "1"}
        for unit in chunk:
            inner.update(unit.inner)
            aggs.update(unit.aggs)
            if unit.exact:
                exact.update(unit.exact)
                exact_inner.update(unit.inner)
        if exact and backend.dialect.inline_exact:
            aggs.update(exact)
            exact = {}
        select = ", ".join(f"{e} AS {a}" for a, e in aggs.items())
        sql = f"SELECT {select} FROM {_scan(backend.src, inner, cap)} t"
        if exact:
            ex_select = ", ".join(f"{e} AS {a}" for a, e in exact.items())
            ex_scan = _scan(backend.src, exact_inner, cap)
            sql = f"SELECT m.*, x.* FROM ({sql}) m, (SELECT {ex_select} FROM {ex_scan} t) x"
        row = backend.row(sql)
        out.update(zip([*aggs, *exact], row, strict=True))
    return out


def check_source(
    cfg: ReferentialConfig,
    source_id: str,
    sample: int | None = None,
    backend: Backend | None = None,
) -> dict:
    src = cfg.source(source_id)
    report: dict = {
        "source": source_id, "kind": src.kind, "location": src.location,
        "snapshot": src.snapshot, "sample": sample, "rows": None, "key": None,
        "revision": None, "attributes": [], "rows_rejected_by_checks": None,
        "distinct_approximate": False, "findings": [], "verdict": "BLOCKING",
    }  # fmt: skip
    findings: list[dict] = report["findings"]

    def add(level: str, message: str, scope: str = "source") -> None:
        findings.append({"level": level, "scope": scope, "message": message})

    owns_backend = backend is None
    try:
        backend = backend or open_backend(src)
        columns = backend.describe()
    except Exception as error:  # connexion, fichier absent, table introuvable...
        add("BLOCKING", f"cannot read source: {' '.join(str(error).split())[:400]}")
        return report
    try:
        _profile(cfg, src, backend, columns, sample, report, add)
    except Exception as error:
        add("BLOCKING", f"profiling query failed: {' '.join(str(error).split())[:400]}")
    finally:
        if owns_backend:
            backend.close()
    levels = {f["level"] for f in findings}
    report["verdict"] = "BLOCKING" if "BLOCKING" in levels else "WARN" if "WARN" in levels else "OK"
    return report


def _profile(
    cfg: ReferentialConfig,
    src: Source,
    backend: Backend,
    columns: dict[str, str],
    sample: int | None,
    report: dict,
    add: Add,
) -> None:
    names = list(columns)
    units: list[Unit] = []

    def need(col: str, what: str) -> bool:
        if col in columns:
            return True
        add("BLOCKING", f"{what}: column '{col}' not found.{_suggest(col, names)}", what)
        return False

    key_ok = need(src.key, "key")
    if key_ok:
        aggs = {"key_nn": "count(k)"}
        units.append(Unit({"k": quote(src.key)}, aggs, {"key_d": "count(DISTINCT k)"}))
    rev_fam = None
    if src.revision and need(src.revision, "revision"):
        rev_fam = family(columns[src.revision])
        aggs = {"rv_nn": "count(rv)"}
        if rev_fam in ORDERABLE | {TEXT}:
            aggs["rv_min"] = backend.dialect.text("min(rv)")
            aggs["rv_max"] = backend.dialect.text("max(rv)")
        units.append(Unit({"rv": quote(src.revision)}, aggs))

    plan: list[tuple[int, Attribute, int, str, str, list[tuple[int, Check]]]] = []
    reject_terms: list[str] = []
    reject_inner: dict[str, str] = {}
    for i, attr in enumerate(cfg.attributes):
        for rank, sc in enumerate(attr.sources, 1):
            if sc.source != src.id or not need(sc.column, f"attribute {attr.name}"):
                continue
            native = columns[sc.column]
            if family(native) not in COMPATIBLE[attr.type]:
                add(
                    "BLOCKING",
                    f"{attr.name}: column '{sc.column}' is {native}, which cannot be read as "
                    f"{attr.type}",
                    f"attribute {attr.name}",
                )
                continue
            checks = []
            for j, chk in enumerate(attr.checks):
                if chk.rule == "regex" and not _regex_supported(backend, str(chk.value)):
                    engine = ENGINE_NAME[backend.dialect.name]
                    add(
                        "WARN",
                        f"{attr.name}: regex {chk.value!r} is not supported by {engine}; "
                        "check skipped (not measured)",
                        f"attribute {attr.name}",
                    )
                    continue
                checks.append((j, chk))
            unit = _attr_unit(backend, i, attr, sc.column, native, checks)
            units.append(unit)
            plan.append((i, attr, rank, sc.column, native, checks))
            for _, chk in checks:
                if chk.on_fail == "reject_record":
                    reject_inner[f"v{i}"] = unit.inner[f"v{i}"]
                    reject_terms.append(f"({_failing(backend, attr, chk, f'v{i}')})")
    if reject_terms:
        agg = f"count(*) FILTER (WHERE {' OR '.join(reject_terms)})"
        units.append(Unit(reject_inner, {"rej": agg}))

    values = _run(backend, units, sample)
    n = int(values["n"])
    report["rows"] = n
    report["rows_rejected_by_checks"] = int(values.get("rej", 0))
    report["distinct_approximate"] = backend.dialect.approx_distinct
    if sample:
        add("INFO", f"profile computed on the first {sample} rows only")
    if n == 0:
        add("BLOCKING", "the source has no rows (nothing to ingest)")
    if key_ok:
        _key(src, values, n, report, add)
    if "rv_nn" in values:
        _revision(cfg, src, columns, rev_fam, values, n, report, add)
    elif not src.revision:
        add("INFO", "no revision column configured for this source", "revision")
    report["attributes"] = [_attribute(cfg, src, values, n, add, *item) for item in plan]


def _regex_supported(backend: Backend, pattern: str) -> bool:
    try:
        backend.probe(f"SELECT {backend.dialect.regex_match(literal(''), pattern)}")
        return True
    except Exception:
        return False


def _key(src: Source, values: dict, n: int, report: dict, add: Add) -> None:
    nn, distinct = int(values["key_nn"]), int(values["key_d"])
    report["key"] = {
        "column": src.key, "null_count": n - nn, "null_rate": _rate(n - nn, n),
        "distinct": distinct, "duplicate_rows": nn - distinct, "unique": nn == distinct,
    }  # fmt: skip
    if n - nn:
        level = "BLOCKING" if (n - nn) / n >= BLOCK_RATE else "WARN"
        add(
            level,
            f"key '{src.key}': {n - nn} rows ({_pct(n - nn, n)}) have a null key and cannot "
            "be ingested",
            "key",
        )
    if nn - distinct:
        msg = (
            f"key '{src.key}': {nn - distinct} rows share their key with another row "
            f"({distinct} distinct keys for {nn} rows)"
        )
        if src.snapshot == "delta":
            add(
                "INFO",
                msg + "; normal in a delta feed (highest revision, else last row, wins)",
                "key",
            )
        elif distinct / nn < BLOCK_RATE:
            add("BLOCKING", msg + ": this column does not look like a key", "key")
        else:
            add(
                "WARN",
                msg + "; only the highest revision (else the last row) is kept, the others "
                "are traced in reject",
                "key",
            )


def _revision(cfg, src, columns, rev_fam, values, n, report, add) -> None:
    nn = int(values["rv_nn"])
    report["revision"] = {
        "column": src.revision, "native_type": columns[src.revision],
        "null_count": n - nn, "null_rate": _rate(n - nn, n),
        "min": values.get("rv_min"), "max": values.get("rv_max"),
    }  # fmt: skip
    if rev_fam not in ORDERABLE | {TEXT}:
        add(
            "BLOCKING",
            f"revision '{src.revision}' is {columns[src.revision]}: it cannot be ordered",
            "revision",
        )
    elif rev_fam == TEXT:
        add(
            "WARN",
            f"revision '{src.revision}' is text: versions are ordered alphabetically, so only "
            "ISO-8601 or zero-padded values are safe",
            "revision",
        )
    if n - nn:
        uses = [
            a.name
            for a in cfg.attributes
            if a.strategy == "most_recent" and any(s.source == src.id for s in a.sources)
        ]
        extra = f" (used by most_recent: {', '.join(uses)})" if uses else ""
        add(
            "WARN" if uses else "INFO",
            f"revision '{src.revision}': {n - nn} rows ({_pct(n - nn, n)}) are null{extra}",
            "revision",
        )


def _attribute(cfg, src, values, n, add, i, attr, rank, col, native, checks) -> dict:
    nn = int(values[f"a{i}_nn"])
    entry: dict = {
        "attribute": attr.name, "column": col, "rank": rank, "of": len(attr.sources),
        "native_type": native, "type": attr.type, "nullable": attr.nullable,
        "null_count": n - nn, "null_rate": _rate(n - nn, n), "cast_failures": None,
        "empty_after_normalisation": None, "distinct_before": None, "distinct_after": None,
        "checks": [],
    }  # fmt: skip
    scope = f"attribute {attr.name}"
    sole = all(s.source == src.id for s in attr.sources)
    if n and nn == 0:
        if not attr.nullable and sole:
            add("BLOCKING", f"{attr.name} ({col}): entirely null and it is the only source of a "
                "non-nullable attribute: every golden would be blocked", scope)  # fmt: skip
        else:
            add("WARN", f"{attr.name} ({col}): entirely null: this source contributes nothing "
                "to it", scope)  # fmt: skip
    elif n - nn and not attr.nullable and sole:
        add("WARN", f"{attr.name} ({col}): {n - nn} null rows; nullable is false and no other "
            "source can fill them, so their golden would be blocked", scope)  # fmt: skip
    if attr.type != "string":
        cf = int(values[f"a{i}_cf"])
        entry["cast_failures"] = cf
        if cf:
            level = "BLOCKING" if cf / nn >= BLOCK_RATE else "WARN"
            add(level, f"{attr.name} ({col}): {cf} of {nn} non-null values ({_pct(cf, nn)}) "
                f"cannot be read as {attr.type}", scope)  # fmt: skip
    else:
        empty = int(values[f"a{i}_empty"])
        entry["empty_after_normalisation"] = empty
        if empty:
            add("INFO", f"{attr.name} ({col}): {empty} values are empty strings after "
                "normalisation (counted as values, not as null)", scope)  # fmt: skip
        if f"a{i}_db" in values:
            entry["distinct_before"] = int(values[f"a{i}_db"])
            entry["distinct_after"] = int(values[f"a{i}_da"])
    for j, chk in checks:
        failing = int(values[f"a{i}_c{j}"])
        entry["checks"].append({
            "rule": chk.rule, "value": chk.value, "on_fail": chk.on_fail,
            "failing_rows": failing, "rate": _rate(failing, n),
        })  # fmt: skip
        if not failing:
            continue
        what = f"{attr.name}: check {chk.rule} fails on {failing} rows ({_pct(failing, n)})"
        if chk.on_fail == "reject_record":
            level = "BLOCKING" if failing / n >= BLOCK_RATE else "WARN"
            add(level, f"{what} → reject_record: these rows produce no delta", scope)
        elif chk.on_fail == "null_value":
            add("WARN", f"{what} → null_value: the value is treated as null", scope)
        else:
            add("INFO", f"{what} → warn: kept, only traced", scope)
    return entry


def render(report: dict) -> str:
    lines = [
        f"Source {report['source']} ({report['kind']} {report['location']}, "
        f"snapshot {report['snapshot']})",
        f"Verdict : {report['verdict']}",
    ]
    if report["rows"] is not None:
        sampled = f" (first {report['sample']} rows only)" if report["sample"] else ""
        lines.append(f"Rows    : {report['rows']:,}{sampled}")
    key = report["key"]
    if key:
        lines.append(
            f"Key     : {key['column']} — {key['null_rate']:.1%} null, {key['distinct']:,} "
            f"distinct, {key['duplicate_rows']:,} duplicate rows"
        )
    rev = report["revision"]
    if rev:
        lines.append(
            f"Revision: {rev['column']} ({rev['native_type']}) — {rev['null_rate']:.1%} null, "
            f"{rev['min']} → {rev['max']}"
        )
    if report["attributes"]:
        lines += ["", "Attributes (rank = priority of this source for the attribute):"]
        approx = " (estimated)" if report["distinct_approximate"] else ""
        rows = [
            ("attribute", "column (rank)", "type/native", "null", "cast fail",
             f"distinct{approx}", "checks")
        ]  # fmt: skip
        for a in report["attributes"]:
            distinct = (
                f"{a['distinct_before']:,}→{a['distinct_after']:,}"
                if a["distinct_before"] is not None else "-"
            )  # fmt: skip
            cast = "-" if a["cast_failures"] is None else f"{a['cast_failures']:,}"
            checks = (
                ", ".join(f"{c['rule']}:{c['failing_rows']:,} {c['on_fail']}" for c in a["checks"])
                or "-"
            )
            rows.append((
                a["attribute"], f"{a['column']} ({a['rank']}/{a['of']})",
                f"{a['type']}/{a['native_type']}", f"{a['null_rate']:.1%}", cast, distinct, checks,
            ))  # fmt: skip
        widths = [max(len(r[c]) for r in rows) for c in range(len(rows[0]))]
        lines += [
            "  " + "  ".join(v.ljust(w) for v, w in zip(r, widths, strict=True)).rstrip()
            for r in rows
        ]
        lines.append(
            f"Rows rejected by reject_record checks: {report['rows_rejected_by_checks']:,}"
        )
    if report["findings"]:
        lines += ["", "Findings:"]
        order = {"BLOCKING": 0, "WARN": 1, "INFO": 2}
        for f in sorted(report["findings"], key=lambda f: order[f["level"]]):
            lines.append(f"  {f['level']:<8} {f['message']}")
    return "\n".join(lines) + "\n"
