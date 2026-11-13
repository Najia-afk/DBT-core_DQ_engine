"""Générateurs de fragments SQL (DuckDB) dérivés de la config : le « moteur » des modèles dbt.

Chaque fonction reproduit la sémantique de `engine.core.reference` (voir sa docstring) ;
les écarts connus (Unicode exotique, regex RE2 vs Python) sont listés dans le README.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from engine.core.config import Attribute, Check, ReferentialConfig

EPS = 1e-9
SQL_TYPES = {
    "string": "VARCHAR",
    "int": "BIGINT",
    "decimal": "DECIMAL(18,4)",
    "date": "DATE",
    "timestamp": "TIMESTAMP",
    "bool": "BOOLEAN",
}
PG_TYPES = {
    "string": "text",
    "int": "bigint",
    "decimal": "numeric(18,4)",
    "date": "date",
    "timestamp": "timestamp",
    "bool": "boolean",
}
SEP = "chr(31)"  # séparateur des composantes d'une clé de blocage


def q(name: str) -> str:
    """Identifiant SQL entre guillemets."""
    return '"' + name.replace('"', '""') + '"'


def lit(text: str) -> str:
    """Littéral texte SQL."""
    return "'" + text.replace("'", "''") + "'"


def num(value: float) -> str:
    """Littéral DOUBLE exact (repr Python) pour que les comparaisons soient identiques."""
    return f"CAST({value!r} AS DOUBLE)"


def _whitespace_ranges() -> list[tuple[int, int]]:
    chars = [c for c in range(0x3001) if chr(c).isspace()]
    ranges: list[tuple[int, int]] = []
    for c in chars:
        if ranges and ranges[-1][1] == c - 1:
            ranges[-1] = (ranges[-1][0], c)
        else:
            ranges.append((c, c))
    return ranges


_WS_RANGES = _whitespace_ranges()
WS_CLASS = "".join(
    f"\\x{{{a:x}}}" if a == b else f"\\x{{{a:x}}}-\\x{{{b:x}}}" for a, b in _WS_RANGES
)
WS_CHARS_SQL = " || ".join(f"chr({c})" for a, b in _WS_RANGES for c in range(a, b + 1))


def normalize_sql(expr: str, normalizers: list[str]) -> str:
    """Normaliseurs dans l'ordre configuré ; la chaîne vide devient NULL."""
    for name in normalizers:
        if name == "trim":
            expr = f"trim({expr}, {WS_CHARS_SQL})"
        elif name == "lower":
            expr = f"lower({expr})"
        elif name == "upper":
            expr = f"upper({expr})"
        elif name == "strip_accents":
            expr = f"strip_accents({expr})"
        elif name == "collapse_spaces":
            expr = f"regexp_replace({expr}, '[{WS_CLASS}]+', ' ', 'g')"
        elif name == "digits_only":
            expr = f"regexp_replace({expr}, '[^0-9]', '', 'g')"
        elif name == "alnum_only":
            expr = f"regexp_replace({expr}, '[^0-9A-Za-z]', '', 'g')"
        else:
            raise ValueError(f"unknown normalizer {name!r}")
    return f"NULLIF({expr}, '')"


def raw_class(duck_type: str) -> str:
    t = duck_type.upper()
    if t.startswith(("VARCHAR", "STRING", "TEXT", "CHAR", "UUID", "ENUM")):
        return "varchar"
    if t == "BOOLEAN":
        return "bool"
    if t.startswith("DECIMAL"):
        return "decimal"
    if t in {"DOUBLE", "FLOAT", "REAL"}:
        return "float"
    if t == "DATE":
        return "date"
    if t.startswith("TIMESTAMP WITH TIME ZONE") or t == "TIMESTAMPTZ":
        return "timestamptz"
    if t.startswith("TIMESTAMP"):
        return "timestamp"
    if t.endswith("INT") or t.endswith("INTEGER") or t.endswith("INT128"):
        return "int"
    return "other"


def cast_sql(col: str, rclass: str, target: str) -> tuple[str, bool]:
    """(expression de conversion, peut échouer). Un échec donne NULL ; l'appelant le détecte avec
    `brut IS NOT NULL AND converti IS NULL`."""
    if target == "string":
        if rclass == "varchar":
            return col, False
        if rclass in {"int", "decimal", "float"}:
            return f"CAST({col} AS VARCHAR)", False
        return "CAST(NULL AS VARCHAR)", True
    if target == "int":
        if rclass == "int":
            return f"TRY_CAST({col} AS BIGINT)", True
        if rclass in {"decimal", "float"}:
            return f"CASE WHEN {col} = floor({col}) THEN TRY_CAST({col} AS BIGINT) END", True
        if rclass == "varchar":
            d = f"TRY_CAST(trim({col}) AS DECIMAL(38,6))"
            return f"CASE WHEN {d} = floor({d}) THEN TRY_CAST({d} AS BIGINT) END", True
        return "CAST(NULL AS BIGINT)", True
    if target == "decimal":
        if rclass in {"int", "decimal", "float"}:
            return f"TRY_CAST({col} AS DECIMAL(18,4))", True
        if rclass == "varchar":
            return f"TRY_CAST(trim({col}) AS DECIMAL(18,4))", True
        return "CAST(NULL AS DECIMAL(18,4))", True
    if target == "date":
        if rclass == "date":
            return col, False
        if rclass in {"timestamp", "timestamptz"}:
            return f"CAST({col} AS DATE)", False
        if rclass == "varchar":
            return f"TRY_CAST(trim({col}) AS DATE)", True
        return "CAST(NULL AS DATE)", True
    if target == "timestamp":
        return timestamp_sql(col, rclass)
    if target == "bool":
        if rclass == "bool":
            return col, False
        text = f"lower(trim(CAST({col} AS VARCHAR)))"
        return (
            f"CASE WHEN {text} IN ('true', '1') THEN TRUE WHEN {text} IN ('false', '0') THEN FALSE"
            " END",
            True,
        )
    raise ValueError(f"unknown attribute type {target!r}")


def timestamp_sql(col: str, rclass: str) -> tuple[str, bool]:
    if rclass == "timestamp":
        return col, False
    if rclass == "timestamptz":
        return f"timezone('UTC', {col})", False
    if rclass == "date":
        return f"CAST({col} AS TIMESTAMP)", False
    if rclass == "varchar":
        return f"timezone('UTC', TRY_CAST(trim({col}) AS TIMESTAMPTZ))", True
    return "CAST(NULL AS TIMESTAMP)", True


def canonical_text_sql(expr: str, attr_type: str) -> str:
    """Texte canonique (cf. `engine.core.canonical.canonical_text`) ; NULL reste NULL."""
    if attr_type == "string":
        return expr
    if attr_type in {"int", "decimal", "date"}:
        return f"CAST({expr} AS VARCHAR)"
    if attr_type == "timestamp":
        return f"strftime({expr}, '%Y-%m-%dT%H:%M:%S.%f')"
    if attr_type == "bool":
        return f"CASE WHEN {expr} THEN 'true' WHEN NOT {expr} THEN 'false' END"
    raise ValueError(f"unknown attribute type {attr_type!r}")


def row_hash_sql(config: ReferentialConfig, source_id: str, value_of: Any) -> str:
    """sha256 de `row_hash_input` ; `value_of(attr)` donne l'expression SQL de la valeur."""
    mapped = config.mapping(source_id)
    parts = []
    for attr in config.attributes:
        if attr.name not in mapped:
            continue
        value = value_of(attr)
        text = canonical_text_sql(value, attr.type)
        token = f"CASE WHEN {value} IS NULL THEN chr(0) ELSE chr(1) || {text} END"
        parts.append(f"{lit(attr.name)} || chr(31) || {token}")
    return "sha256(concat_ws(chr(30), " + ", ".join(parts) + "))"


def check_pass_sql(attr: Attribute, check: Check, expr: str) -> str:
    """Condition vraie quand la valeur (non nulle) respecte le contrôle."""
    if check.rule == "regex":
        return f"regexp_matches({expr}, {lit(str(check.value))})"
    if check.rule == "in_set":
        values = ", ".join(lit(str(v)) for v in check.value)
        return f"{canonical_text_sql(expr, attr.type)} IN ({values})"
    if check.rule == "max_length":
        return f"length({expr}) <= {int(check.value)}"
    bound = format(Decimal(str(check.value)), "f")
    return f"{expr} {'>=' if check.rule == 'min' else '<='} {bound}"


def block_part_sql(attr_type: str, transform: str, n: int | None, expr: str) -> str:
    if transform == "prefix":
        return f"left({expr}, {n})"
    if transform == "year":
        return f"CAST(year({expr}) AS VARCHAR)"
    return canonical_text_sql(expr, attr_type)


def comparator_sim_sql(method: str, a: str, b: str) -> str:
    if method == "exact":
        return f"CASE WHEN {a} = {b} THEN 1.0 ELSE 0.0 END"
    if method == "levenshtein":
        both_ascii = f"strlen({a}) = length({a}) AND strlen({b}) = length({b})"
        dist = (
            f"CAST(CASE WHEN {both_ascii} THEN levenshtein({a}, {b})"
            f" ELSE mdm_lev_cp({a}, {b}) END AS DOUBLE)"
        )
        return f"(1.0 - {dist} / CAST(greatest(length({a}), length({b})) AS DOUBLE))"
    if method == "date_year":
        return f"CASE WHEN year({a}) = year({b}) THEN 1.0 ELSE 0.0 END"
    raise ValueError(f"unknown comparator {method!r}")


@dataclass(frozen=True)
class CheckView:
    idx: int
    rule: str
    action: str
    check: Check

    @property
    def nulls(self) -> bool:
        return self.action == "null_value"


@dataclass(frozen=True)
class AttrView:
    idx: int
    attr: Attribute
    name: str = field(init=False)
    q: str = field(init=False)
    qsrc: str = field(init=False)
    type: str = field(init=False)
    sql_type: str = field(init=False)
    pg_type: str = field(init=False)
    nullable: bool = field(init=False)
    strategy: str = field(init=False)
    checks: tuple[CheckView, ...] = field(init=False)

    def __post_init__(self) -> None:
        a = self.attr
        object.__setattr__(self, "name", a.name)
        object.__setattr__(self, "q", q(a.name))
        object.__setattr__(self, "qsrc", q(a.name + "__src"))
        object.__setattr__(self, "type", a.type)
        object.__setattr__(self, "sql_type", SQL_TYPES[a.type])
        object.__setattr__(self, "pg_type", PG_TYPES[a.type])
        object.__setattr__(self, "nullable", a.nullable)
        object.__setattr__(self, "strategy", a.strategy)
        object.__setattr__(
            self,
            "checks",
            tuple(CheckView(i, c.rule, c.on_fail, c) for i, c in enumerate(a.checks)),
        )


class Spec:
    """Vue de la config pour les gabarits (identifiants quotés, littéraux prêts à l'emploi)."""

    def __init__(self, config: ReferentialConfig, keep_none_pairs: bool = False) -> None:
        self.config = config
        self.keep_none_pairs = keep_none_pairs
        self.attrs = [AttrView(i, a) for i, a in enumerate(config.attributes)]
        self.attr_by_name = {a.name: a for a in self.attrs}
        self.schema = q(config.state.schema_name)
        self.schema_name = config.state.schema_name
        m = config.matching
        self.limit = m.max_block_size
        self.accept = num(m.accept - EPS)
        self.review = num(m.review - EPS)
        self.min_evidence = num(m.min_evidence - EPS)
        self.rule_version = lit(m.version)
        self.provenance = config.publication.provenance
        self.sources = list(config.sources)
        self.source_index = {s.id: i for i, s in enumerate(config.sources)}
        self.source_array = "[" + ", ".join(lit(s.id) for s in config.sources) + "]"

    @property
    def compare_attrs(self) -> list[AttrView]:
        """Attributs lus pour le blocage, le score et les contradictions."""
        names = [c.attr for c in self.config.matching.comparators]
        names += [c.attr for c in self.config.matching.contradictions]
        names += [p.attr for k in self.config.matching.blocking for p in k.parts]
        return [self.attr_by_name[n] for n in dict.fromkeys(names)]

    def score_sql(self, a: str, b: str) -> tuple[str, str, str]:
        """(poids actif, somme pondérée, contradiction) entre les alias `a` et `b`."""
        active, total = ["0.0"], ["0.0"]
        for comp in self.config.matching.comparators:
            qa = q(comp.attr)
            both = f"{a}.{qa} IS NOT NULL AND {b}.{qa} IS NOT NULL"
            weight = repr(comp.weight)
            sim = comparator_sim_sql(comp.method, f"{a}.{qa}", f"{b}.{qa}")
            active.append(f"CASE WHEN {both} THEN {weight} ELSE 0.0 END")
            total.append(f"CASE WHEN {both} THEN {weight} * ({sim}) ELSE 0.0 END")
        contra = [
            f"({a}.{q(c.attr)} IS NOT NULL AND {b}.{q(c.attr)} IS NOT NULL"
            f" AND {a}.{q(c.attr)} <> {b}.{q(c.attr)})"
            for c in self.config.matching.contradictions
        ] or ["FALSE"]
        return " + ".join(active), " + ".join(total), " OR ".join(contra)

    def block_values_sql(self, alias: str) -> list[tuple[str, str]]:
        """[(nom de la clé, expression de valeur ou NULL)] pour les nœuds de l'alias."""
        result = []
        for key in self.config.matching.blocking:
            parts, guards = [], []
            for part in key.parts:
                attr = self.attr_by_name[part.attr]
                col = f"{alias}.{attr.q}"
                parts.append(block_part_sql(attr.type, part.transform, part.n, col))
                guards.append(f"{col} IS NOT NULL")
            value = f"concat_ws({SEP}, {', '.join(parts)})"
            result.append((key.name, f"CASE WHEN {' AND '.join(guards)} THEN {value} END"))
        return result

    def survive_key(self, attr: AttrView, alias: str = "m") -> str:
        """Clé de tri entière (plus petite = gagnante) : priorité de la source pour cet attribut,
        puis rang du membre (revision la plus récente, nulle = la plus ancienne, puis nœud)."""
        if attr.strategy == "most_recent":
            return f"{alias}.rn"
        cases = " ".join(
            f"WHEN {self.source_index[sc.source]} THEN {i}"
            for i, sc in enumerate(attr.attr.sources)
        )
        return f"CAST(CASE {alias}.src_no {cases} ELSE 99 END AS BIGINT) * 4294967296 + {alias}.rn"

    def blocked_sql(self, alias: str) -> str:
        """Vrai si un attribut non nullable n'a aucune valeur survivante (golden `blocked`)."""
        conds = [f"{alias}.{a.q} IS NULL" for a in self.attrs if not a.nullable]
        return "(" + " OR ".join(conds) + ")" if conds else "FALSE"

    def blocked_names_sql(self, alias: str) -> str:
        parts = [
            f"CASE WHEN {alias}.{a.q} IS NULL THEN {lit(a.name)} END"
            for a in self.attrs
            if not a.nullable
        ]
        return f"concat_ws(',', {', '.join(parts)})" if parts else "''"
