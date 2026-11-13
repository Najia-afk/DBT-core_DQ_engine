"""State dataclasses of the reference engine."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

Node = tuple[str, str]


@dataclass
class Record:
    source_id: str
    source_key: str
    row_hash: str
    revision: datetime | None
    status: str
    first_run: str
    last_change_run: str
    values: dict[str, Any]

    @property
    def node(self) -> Node:
        return (self.source_id, self.source_key)


@dataclass
class Pair:
    score: float | None
    contradiction: bool
    eligible: bool
    decision: str = "none"
    reason: str = ""
    run_id: str = ""


@dataclass
class Golden:
    status: str
    stale: bool
    values: dict[str, Any]
    sources: dict[str, str | None]
    published_once: bool = False
