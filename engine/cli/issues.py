"""Message de diagnostic commun à validate / add-source / check-source."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path


@dataclass(frozen=True)
class Issue:
    level: str  # error | warning | info
    code: str
    path: str  # chemin YAML, ex. attributes[3].sources[1].source
    message: str
    line: int | None = None  # 1-based

    def as_dict(self) -> dict:
        return asdict(self)

    def render(self, file: str | Path | None = None) -> str:
        label = {"error": "ERROR", "warning": "WARN ", "info": "INFO "}[self.level]
        where = ""
        if file is not None and self.line:
            where = f"{Path(file).name}:{self.line}  "
        elif file is not None:
            where = f"{Path(file).name}  "
        head = f"{self.path}: " if self.path else ""
        return f"{label} {where}{head}{self.message}"


def fmt_path(loc: tuple) -> str:
    out = ""
    for part in loc:
        if isinstance(part, int):
            out += f"[{part}]"
        else:
            out += f".{part}" if out else str(part)
    return out
