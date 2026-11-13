"""Jeux de données du banc : génération (engine.core.synth), mise à disposition `live/`, config."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

import yaml

from .guards import Limits, Monitor
from .procs import PYTHON, StepResult, run_command

DEFAULT_DELTA = {
    "update_pct": 3.0,
    "new_pct": 1.0,
    "delete_pct": 0.5,
    "broken_pct": 0.2,
    "merge_pct": 5.0,
    "split_pct": 5.0,
}


@dataclass(frozen=True)
class DatasetSpec:
    nodes: int
    attributes: int
    sources: int = 3
    seed: int = 42
    kinds: tuple[str, ...] = ("parquet", "parquet", "parquet")
    snapshots: tuple[str, ...] = ("full", "full", "delta")
    deltas: tuple[dict[str, float], ...] = ()  # un dict de DeltaParams par jour >= 1
    referential: str = "bench"
    workers: int = 8
    extra: dict[str, float] = field(default_factory=dict)  # options synth numériques

    @property
    def days(self) -> int:
        return len(self.deltas)

    def uses_postgres_sources(self) -> bool:
        return "postgres" in self.kinds

    def key(self) -> str:
        """Empreinte des paramètres qui déterminent les octets du jeu (hors `workers`)."""
        data = asdict(self)
        data.pop("workers")
        return hashlib.sha256(json.dumps(data, sort_keys=True).encode()).hexdigest()[:12]


def delta_args(delta: dict[str, float]) -> list[str]:
    merged = {**DEFAULT_DELTA, **delta}
    out: list[str] = []
    for name, value in merged.items():
        out += [f"--{name.replace('_', '-')}", str(value)]
    return out


def synth_commands(spec: DatasetSpec, out: Path) -> list[list[str]]:
    base = [PYTHON, "-m", "engine.core.synth"]
    gen = [
        *base, "generate", "--out", str(out), "--days", "0", "--force",
        "--nodes", str(spec.nodes), "--attributes", str(spec.attributes),
        "--sources", str(spec.sources), "--seed", str(spec.seed),
        "--referential", spec.referential, "--kinds", ",".join(spec.kinds),
        "--snapshots", ",".join(spec.snapshots), "--workers", str(spec.workers),
    ]  # fmt: skip
    for name, value in spec.extra.items():
        gen += [f"--{name.replace('_', '-')}", str(value)]
    cmds = [gen]
    for delta in spec.deltas:
        cmds.append(
            [*base, "next-day", "--out", str(out), "--workers", str(spec.workers),
             *delta_args(delta)]
        )  # fmt: skip
    return cmds


def load_manifest(ds: Path) -> dict[str, Any]:
    return json.loads((ds / "manifest.json").read_text(encoding="utf-8"))


def dir_bytes(path: Path) -> int:
    total = 0
    for root, _, files in os.walk(path):
        for name in files:
            try:
                total += os.path.getsize(os.path.join(root, name))
            except OSError:
                continue
    return total


def day_rows(manifest: dict[str, Any], day: int) -> dict[str, int]:
    return {sid: int(v["rows"]) for sid, v in manifest["days"][str(day)]["sources"].items()}


def ensure_dataset(
    spec: DatasetSpec, root: Path, *, limits: Limits, env: dict[str, str]
) -> tuple[Path, dict[str, Any]]:
    """Génère (ou réutilise) le jeu ; retourne (répertoire, infos de génération mesurées)."""
    ds = root / f"ds-{spec.key()}"
    marker = ds / ".bench-complete"
    if marker.exists():
        return ds, json.loads(marker.read_text(encoding="utf-8"))
    if ds.exists():
        shutil.rmtree(ds)
    ds.mkdir(parents=True)
    logs = ds.parent / f"logs-{spec.key()}"
    results: list[StepResult] = []
    with Monitor(limits, disk_path=str(root)) as mon:
        for i, argv in enumerate(synth_commands(spec, ds)):
            res = run_command(
                argv, env=env, cwd=Path.cwd(), log_dir=logs, label=f"synth-{i}", monitor=mon
            )
            results.append(res)
            if not res.ok:
                break
    summary = mon.summary()
    if not all(r.ok for r in results) or len(results) != spec.days + 1:
        reason = mon.abort_reason or (results[-1].stderr_tail[-500:] if results else "no command")
        raise RuntimeError(f"génération du jeu échouée : {reason}")
    manifest = load_manifest(ds)
    info = {
        "key": spec.key(),
        "gen_wall_s": round(sum(r.wall_s for r in results), 1),
        "gen_rss_peak_mb": summary["rss_peak_mb"],
        "bytes": dir_bytes(ds),
        "rows_by_day": {d: sum(day_rows(manifest, int(d)).values()) for d in manifest["days"]},
        "commands": [" ".join(r.argv[2:]) for r in results],
    }
    marker.write_text(json.dumps(info), encoding="utf-8")
    return ds, info


def stage_live(ds: Path, day: int) -> dict[str, int]:
    """`live/<source>/` = exactement les fichiers du jour (full ET delta : une source `delta`
    ne livre que les lignes modifiées du jour, elle n'est pas rejouée en entier)."""
    staged: dict[str, int] = {}
    for src in load_manifest(ds)["sources"]:
        if src["kind"] != "parquet":
            continue
        target = ds / "live" / src["id"]
        shutil.rmtree(target, ignore_errors=True)
        target.mkdir(parents=True)
        n = 0
        for part in sorted((ds / f"day{day}" / src["id"]).glob("part-*.parquet")):
            dest = target / f"day{day}-{part.name}"
            try:
                os.link(part, dest)
            except OSError:
                shutil.copy2(part, dest)
            n += 1
        staged[src["id"]] = n
    return staged


def prepare_config(ds: Path, run_dir: Path, *, batch_rows: int | None) -> Path:
    """Config du jeu ; copie modifiée si `batch_rows` est imposé (paramètre de test documenté)."""
    path = ds / "config.yml"
    if batch_rows is None:
        return path
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    data["runtime"]["batch_rows"] = batch_rows
    out = run_dir / "config.yml"
    out.write_text(yaml.safe_dump(data, sort_keys=False, width=100), encoding="utf-8")
    return out


def load_pg_command(ds: Path, dsn_env: str, day: int, workers: int = 4) -> list[str]:
    return [
        PYTHON, "-m", "engine.core.synth", "load-pg", "--dataset", str(ds), "--dsn-env", dsn_env,
        "--day", str(day), "--workers", str(workers),
    ]  # fmt: skip
