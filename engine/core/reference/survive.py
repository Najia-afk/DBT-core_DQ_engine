"""Survivorship (contract section 2, rule 5)."""

from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime
from typing import Any

from engine.core.config import Attribute

from .model import Record


def _rank(record: Record) -> tuple[bool, datetime]:
    return (record.revision is not None, record.revision or datetime.min)


def _best(candidates: list[Record]) -> Record:
    """Greatest revision (null = oldest), then ascending (source_id, source_key)."""
    best = candidates[0]
    for record in candidates[1:]:
        if _rank(record) > _rank(best) or (
            _rank(record) == _rank(best) and record.node < best.node
        ):
            best = record
    return best


def survive_attribute(attr: Attribute, members: Iterable[Record]) -> tuple[Any, str | None]:
    """(value, winning source_id) of an attribute over the active members of a staging."""
    with_value = [m for m in members if m.values.get(attr.name) is not None]
    if attr.strategy == "most_recent":
        mapped = {sc.source for sc in attr.sources}
        candidates = [m for m in with_value if m.source_id in mapped]
        if candidates:
            winner = _best(candidates)
            return winner.values[attr.name], winner.source_id
        return None, None
    for sc in attr.sources:
        candidates = [m for m in with_value if m.source_id == sc.source]
        if candidates:
            winner = _best(candidates)
            return winner.values[attr.name], winner.source_id
    return None, None
