"""Validation d'une config : erreurs Pydantic réécrites avec chemin YAML et ligne."""

from __future__ import annotations

import copy
import difflib
import re
from dataclasses import dataclass, field

import yaml
from pydantic import ValidationError

from engine.cli.issues import Issue, fmt_path
from engine.cli.yamlio import compose, line_of
from engine.core.config import (
    COMPARATORS_BY_TYPE,
    IDENT,
    ReferentialConfig,
)

UNION_TAGS = {"str", "int", "float", "list[str]"}
RESERVED = {"golden_id", "staging_id", "status", "stale"}


@dataclass
class Validation:
    config: ReferentialConfig | None
    raw: dict | None
    errors: list[Issue] = field(default_factory=list)


def suggest(word: str, options: list[str]) -> str:
    close = difflib.get_close_matches(str(word), options, n=1, cutoff=0.6)
    if not close:
        lowered = {o.lower(): o for o in options}
        close = [lowered[str(word).lower()]] if str(word).lower() in lowered else []
    return f" Did you mean '{close[0]}'?" if close else ""


def _schema_node(schema: dict, loc: tuple) -> dict | None:
    def resolve(node: dict) -> dict:
        while "$ref" in node:
            node = schema["$defs"][node["$ref"].rsplit("/", 1)[1]]
        for key in ("anyOf", "oneOf"):
            for alt in node.get(key, []):
                alt = resolve(alt)
                if alt.get("type") != "null":
                    return alt
        return node

    node = resolve(schema)
    for part in loc:
        if isinstance(part, int):
            if "items" not in node:
                return None
            node = resolve(node["items"])
        else:
            if part not in node.get("properties", {}):
                return None
            node = resolve(node["properties"][part])
    return node


def _got(value: object) -> str:
    text = repr(value)
    return text if len(text) <= 60 else text[:57] + "..."


def rewrite_error(err: dict, schema: dict) -> tuple[tuple, str, str]:
    """(loc, code, message) lisible pour une erreur Pydantic."""
    kind, loc, msg = err["type"], tuple(err["loc"]), err["msg"]
    parent = _schema_node(schema, loc[:-1]) if loc else None
    if kind == "missing":
        return loc, "missing", "required field is missing"
    if kind == "extra_forbidden":
        allowed = sorted(parent["properties"]) if parent and "properties" in parent else []
        hint = suggest(loc[-1], allowed)
        known = f" (allowed: {', '.join(allowed)})" if allowed else ""
        return loc, "unknown-option", f"unknown option '{loc[-1]}'{known}.{hint}"
    if kind == "literal_error":
        node = _schema_node(schema, loc)
        allowed = [str(v) for v in node.get("enum", [])] if node else []
        if allowed:
            hint = suggest(err["input"], allowed)
            return (
                loc,
                "bad-value",
                f"invalid value {_got(err['input'])} (allowed: {', '.join(allowed)}).{hint}",
            )
    if kind == "string_pattern_mismatch" and err.get("ctx", {}).get("pattern") == IDENT:
        return (
            loc,
            "bad-identifier",
            f"invalid identifier {_got(err['input'])}: use lowercase letters, digits and "
            "underscores, starting with a letter (max 63 characters)",
        )
    if kind == "value_error":
        return loc, "invalid", msg.removeprefix("Value error, ")
    if kind in {"model_type", "dict_type"}:
        return loc, "type", "must be a mapping (key: value)"
    if kind == "list_type":
        return loc, "type", "must be a list"
    if "input" in err and not isinstance(err["input"], (dict, list)):
        return loc, "invalid", f"{msg} (got {_got(err['input'])})"
    return loc, "invalid", msg


def _precheck_regex(raw: dict) -> tuple[dict, list[tuple[tuple, str]]]:
    """re.error n'est pas une ValueError : Pydantic le laisserait remonter en exception brute."""
    data, bad = copy.deepcopy(raw), []
    attrs = data.get("attributes") if isinstance(data, dict) else None
    for i, attr in enumerate(attrs if isinstance(attrs, list) else []):
        checks = attr.get("checks") if isinstance(attr, dict) else None
        for j, chk in enumerate(checks if isinstance(checks, list) else []):
            if isinstance(chk, dict) and chk.get("rule") == "regex":
                value = chk.get("value")
                if isinstance(value, str):
                    try:
                        re.compile(value)
                    except re.error as error:
                        chk["value"] = ""
                        bad.append((("attributes", i, "checks", j, "value"), str(error)))
    return data, bad


def crossref_issues(raw: dict) -> list[tuple[tuple, str, str]]:
    """Contrôles croisés de ReferentialConfig._check, mais exhaustifs (tous les écarts)."""
    out: list[tuple[tuple, str, str]] = []
    sources = raw["sources"]
    src_ids = [s["id"] for s in sources]
    seen: set[str] = set()
    for i, sid in enumerate(src_ids):
        if sid in seen:
            out.append((("sources", i, "id"), "duplicate", f"duplicate source id '{sid}'"))
        seen.add(sid)
    attrs = {a["name"]: a for a in raw["attributes"]}
    seen = set()
    for i, a in enumerate(raw["attributes"]):
        name = a["name"]
        if name in seen:
            out.append((("attributes", i, "name"), "duplicate", f"duplicate attribute '{name}'"))
        seen.add(name)
        if name in RESERVED or name.endswith("__src"):
            out.append(
                (("attributes", i, "name"), "reserved", f"attribute name '{name}' is reserved")
            )
        for j, sc in enumerate(a["sources"]):
            if sc["source"] not in src_ids:
                out.append(
                    (
                        ("attributes", i, "sources", j, "source"),
                        "unknown-source",
                        f"unknown source '{sc['source']}' (known: {', '.join(src_ids)})."
                        f"{suggest(sc['source'], src_ids)}",
                    )
                )
            elif a.get("strategy") == "most_recent":
                src = next(s for s in sources if s["id"] == sc["source"])
                if not src.get("revision"):
                    out.append(
                        (
                            ("attributes", i, "sources", j, "source"),
                            "no-revision",
                            f"strategy most_recent needs a revision column on source "
                            f"'{sc['source']}': set "
                            f"sources[{src_ids.index(sc['source'])}].revision "
                            "or use strategy: priority",
                        )
                    )
    used = {sc["source"] for a in raw["attributes"] for sc in a["sources"]}
    for i, sid in enumerate(src_ids):
        if sid not in used:
            out.append(
                (
                    ("sources", i),
                    "unmapped-source",
                    f"source '{sid}' is not mapped by any attribute",
                )
            )
    names = list(attrs)
    m = raw["matching"]

    def attr_ref(loc: tuple, what: str, name: str) -> dict | None:
        if name in attrs:
            return attrs[name]
        out.append(
            (
                loc,
                "unknown-attribute",
                f"{what} on unknown attribute '{name}' (known: {', '.join(names)})."
                f"{suggest(name, names)}",
            )
        )
        return None

    for i, comp in enumerate(m["comparators"]):
        attr = attr_ref(("matching", "comparators", i, "attr"), "comparator", comp["attr"])
        if attr:
            typ = attr.get("type", "string")
            if comp["method"] not in COMPARATORS_BY_TYPE[typ]:
                allowed = ", ".join(sorted(COMPARATORS_BY_TYPE[typ]))
                out.append(
                    (
                        ("matching", "comparators", i, "method"),
                        "bad-comparator",
                        f"comparator '{comp['method']}' is not valid for {typ} attribute "
                        f"'{comp['attr']}' (allowed for {typ}: {allowed})",
                    )
                )
    for i, c in enumerate(m.get("contradictions", [])):
        attr_ref(("matching", "contradictions", i, "attr"), "contradiction", c["attr"])
    for i, key in enumerate(m["blocking"]):
        for j, part in enumerate(key["parts"]):
            loc = ("matching", "blocking", i, "parts", j, "attr")
            attr = attr_ref(loc, f"blocking key '{key['name']}'", part["attr"])
            if not attr:
                continue
            typ, tr = attr.get("type", "string"), part.get("transform", "exact")
            if tr == "prefix" and typ != "string":
                out.append(
                    (
                        loc[:-1] + ("transform",),
                        "bad-transform",
                        f"prefix needs a string attribute ('{part['attr']}' is {typ})",
                    )
                )
            if tr == "year" and typ not in {"date", "timestamp"}:
                out.append(
                    (
                        loc[:-1] + ("transform",),
                        "bad-transform",
                        f"year needs a date/timestamp attribute ('{part['attr']}' is {typ})",
                    )
                )
    return out


def validate_text(text: str) -> Validation:
    root = compose(text)
    try:
        raw = yaml.safe_load(text)
    except yaml.YAMLError as error:
        mark = getattr(error, "problem_mark", None)
        line = mark.line + 1 if mark else None
        problem = getattr(error, "problem", None) or str(error)
        return Validation(
            None, None, [Issue("error", "yaml-syntax", "", f"invalid YAML: {problem}", line)]
        )
    if not isinstance(raw, dict):
        return Validation(
            None, None, [Issue("error", "yaml-root", "", "config must be a YAML mapping")]
        )

    data, regex_errors = _precheck_regex(raw)
    schema = ReferentialConfig.model_json_schema()
    issues: list[tuple[tuple, str, str]] = [
        (loc, "bad-regex", f"invalid regular expression: {message}")
        for loc, message in regex_errors
    ]
    cfg: ReferentialConfig | None = None
    try:
        cfg = ReferentialConfig.model_validate(data)
    except ValidationError as error:
        errs = error.errors()
        union_parents = {
            tuple(e["loc"][:-1]) for e in errs if e["loc"] and e["loc"][-1] in UNION_TAGS
        }
        seen: set[tuple] = set()
        for err in errs:
            loc = tuple(err["loc"])
            if loc and loc[-1] in UNION_TAGS and loc[:-1] in union_parents:
                if loc[:-1] in seen:
                    continue
                seen.add(loc[:-1])
                issues.append(
                    (
                        loc[:-1],
                        "bad-value",
                        f"invalid value (got {_got(err['input'])}): expected a string, a "
                        "number or a list of strings",
                    )
                )
                continue
            issues.append(rewrite_error(err, schema))
        if issues and all(loc == () for loc, _, _ in issues):
            try:
                detailed = crossref_issues(data)
            except (KeyError, TypeError):
                detailed = []
            if detailed:
                issues = detailed
    out = [
        Issue("error", code, fmt_path(loc), msg, line_of(root, loc)) for loc, code, msg in issues
    ]
    return Validation(cfg, raw, out)
