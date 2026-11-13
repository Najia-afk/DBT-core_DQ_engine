"""`add-source` : ajoute une source et ses mappings en modifiant le TEXTE du YAML.

On n'utilise pas de round-trip (re-sérialisation) : il reformaterait des lignes non concernées.
Les positions des nœuds PyYAML (compose) servent à insérer exactement les lignes ajoutées ; le
reste du fichier (commentaires, ordre, style flow/block, fins de ligne) reste identique à l'octet.
"""

from __future__ import annotations

import copy
import json
import re
from dataclasses import dataclass

import yaml

from engine.cli.yamlio import compose, node_at

SOURCE_KEYS = ("id", "kind", "location", "dsn_env", "key", "revision", "snapshot",
               "full_snapshot_min_ratio")  # fmt: skip


class AddSourceError(ValueError):
    pass


@dataclass(frozen=True)
class Mapping:
    attr: str
    column: str
    position: int | None  # 1-based ; None = en dernier


def parse_mapping(spec: str) -> Mapping:
    attr, sep, rest = spec.partition("=")
    if not sep or not attr or not rest:
        raise AddSourceError(f"invalid --map {spec!r}: expected attr=column[@position]")
    column, at, pos = rest.rpartition("@")
    if not at:
        return Mapping(attr.strip(), rest.strip(), None)
    pos = pos.strip().lower()
    if pos == "first":
        position: int | None = 1
    elif pos == "last":
        position = None
    elif pos.isdigit() and int(pos) >= 1:
        position = int(pos)
    else:
        raise AddSourceError(
            f"invalid position {pos!r} in --map {spec!r}: use a number >= 1 (1 = highest "
            "priority), 'first' or 'last'"
        )
    return Mapping(attr.strip(), column.strip(), position)


def fmt_scalar(value: object) -> str:
    out = yaml.safe_dump(value, default_flow_style=True, width=10**6, allow_unicode=True)
    out = out.removesuffix("\n").removesuffix("\n...").strip()
    if isinstance(value, str) and not out.startswith(("'", '"')) and re.search(r"[,\[\]{}]", out):
        return json.dumps(value, ensure_ascii=False)
    return out


def _flow_map(item: dict) -> str:
    return "{" + ", ".join(f"{k}: {fmt_scalar(v)}" for k, v in item.items()) + "}"


def _last_content_line(lines: list[str], end: yaml.Mark) -> int:
    row = end.line
    head = lines[row][: end.column]
    while head.strip() == "" or head.lstrip().startswith("#"):
        row -= 1
        head = lines[row]
    return row


def _seq_insert(text: str, seq: yaml.SequenceNode, index: int, item: dict) -> tuple[int, str]:
    elems = seq.value
    if seq.flow_style:
        body = _flow_map(item)
        if index < len(elems):
            return elems[index].start_mark.index, body + ", "
        return elems[-1].end_mark.index, ", " + body

    lines = text.split("\n")
    neighbor = elems[min(index, len(elems) - 1)]
    content_col = neighbor.start_mark.column
    prefix = lines[neighbor.start_mark.line][:content_col]
    indent = prefix[: len(prefix) - len(prefix.lstrip())]
    if neighbor.flow_style or not isinstance(neighbor, yaml.MappingNode):
        block = f"{indent}- {_flow_map(item)}"
    else:
        pairs = [f"{k}: {fmt_scalar(v)}" for k, v in item.items()]
        block = f"{indent}- {pairs[0]}" + "".join(f"\n{' ' * content_col}{p}" for p in pairs[1:])

    if index < len(elems):
        row = elems[index].start_mark.line
        while (
            row > 0
            and lines[row - 1].strip().startswith("#")
            and lines[row - 1].startswith(indent + "#")
        ):
            row -= 1
        return sum(len(line) + 1 for line in lines[:row]), block + "\n"
    row = _last_content_line(lines, elems[-1].end_mark)
    return sum(len(line) + 1 for line in lines[:row]) + len(lines[row]), "\n" + block


def add_source(text: str, raw: dict, source: dict, maps: list[Mapping]) -> tuple[str, dict]:
    """(nouveau texte, structure attendue). Lève AddSourceError si la demande est incohérente."""
    if not maps:
        raise AddSourceError(
            "at least one --map attr=column is required (a source must feed an attribute)"
        )
    attrs = {a["name"]: i for i, a in enumerate(raw["attributes"])}
    if source["id"] in {s["id"] for s in raw["sources"]}:
        raise AddSourceError(f"source '{source['id']}' already exists")
    seen: set[str] = set()
    for m in maps:
        if m.attr not in attrs:
            from engine.cli.validation import suggest

            known = ", ".join(attrs)
            raise AddSourceError(
                f"unknown attribute '{m.attr}' (known: {known}).{suggest(m.attr, list(attrs))}"
            )
        if m.attr in seen:
            raise AddSourceError(f"attribute '{m.attr}' is mapped twice in --map")
        seen.add(m.attr)

    root = compose(text)
    ops: list[tuple[int, str]] = []
    expected = copy.deepcopy(raw)
    src_seq, _ = node_at(root, ("sources",))
    ops.append(_seq_insert(text, src_seq, len(src_seq.value), source))
    expected["sources"].append(source)

    for m in maps:
        i = attrs[m.attr]
        current = expected["attributes"][i]["sources"]
        position = len(current) + 1 if m.position is None else m.position
        if position > len(current) + 1:
            raise AddSourceError(
                f"--map {m.attr}={m.column}@{position}: '{m.attr}' has {len(current)} source(s), "
                f"position must be between 1 and {len(current) + 1}"
            )
        item = {"source": source["id"], "column": m.column}
        seq, _ = node_at(root, ("attributes", i, "sources"))
        ops.append(_seq_insert(text, seq, position - 1, item))
        current.insert(position - 1, item)

    new = text
    for index, chunk in sorted(ops, key=lambda op: op[0], reverse=True):
        new = new[:index] + chunk + new[index:]
    return new, expected


def build_source(args: dict) -> dict:
    source = {k: args[k] for k in SOURCE_KEYS if args.get(k) is not None}
    return source
