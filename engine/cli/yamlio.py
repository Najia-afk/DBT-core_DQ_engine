"""Lecture de YAML avec positions (PyYAML compose) ; aucune dépendance supplémentaire."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import yaml


@dataclass(frozen=True)
class TextFile:
    text: str  # normalisé : LF, sans BOM
    bom: bool
    crlf: bool

    def restore(self, text: str) -> bytes:
        if self.crlf:
            text = text.replace("\n", "\r\n")
        return (("\ufeff" if self.bom else "") + text).encode("utf-8")


def read_text_file(path: str | Path) -> TextFile:
    raw = Path(path).read_bytes().decode("utf-8")
    bom = raw.startswith("\ufeff")
    if bom:
        raw = raw[1:]
    crlf = "\r\n" in raw
    return TextFile(raw.replace("\r\n", "\n"), bom, crlf)


def compose(text: str) -> yaml.Node | None:
    try:
        return yaml.compose(text, Loader=yaml.SafeLoader)
    except yaml.YAMLError:
        return None


def node_at(root: yaml.Node | None, loc: tuple) -> tuple[yaml.Node | None, yaml.Node | None]:
    """(nœud valeur, nœud clé) du chemin ; le plus proche ancêtre existant sinon."""
    node, key_node = root, None
    for part in loc:
        if isinstance(node, yaml.MappingNode) and isinstance(part, str):
            found = next(((k, v) for k, v in node.value if k.value == part), None)
            if found is None:
                return node, key_node
            key_node, node = found
        elif isinstance(node, yaml.SequenceNode) and isinstance(part, int):
            if part >= len(node.value):
                return node, key_node
            node, key_node = node.value[part], None
        else:
            return node, key_node
    return node, key_node


def line_of(root: yaml.Node | None, loc: tuple) -> int | None:
    node, key_node = node_at(root, loc)
    target = key_node or node
    return target.start_mark.line + 1 if target is not None else None
