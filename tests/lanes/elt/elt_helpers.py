"""Aides des tests ELT : jeux synthétiques, référence en parallèle, comparaison d'exports."""

from __future__ import annotations

import glob
import shutil
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import yaml

from engine.core.canonical import read_parquet_dir
from engine.core.compare import Report, compare_exports
from engine.core.config import ReferentialConfig, load_config
from engine.core.reference import ReferenceEngine
from engine.core.synth import DeltaParams, SynthParams, generate, stage_live
from engine.core.synth.pg import load_postgres
from engine.lanes.elt.lane import Lane
from engine.lanes.elt.settings import Settings

INGEST_FIELDS = ("inserted", "updated", "deleted", "unchanged", "rejected")
RECONCILE_FIELDS = (
    "delta_nodes",
    "rescored_nodes",
    "pairs",
    "affected_nodes",
    "stagings",
    "merges",
    "splits",
    "creates",
    "golden_changes",
)


def with_schema(cfg: ReferentialConfig, schema: str) -> ReferentialConfig:
    raw = cfg.model_dump(by_alias=True, mode="json", exclude_none=True)
    raw["state"]["schema"] = schema
    return ReferentialConfig.model_validate(raw)


def write_config(cfg: ReferentialConfig, path: Path) -> Path:
    raw = cfg.model_dump(by_alias=True, mode="json", exclude_none=True)
    path.write_text(yaml.safe_dump(raw, sort_keys=False, allow_unicode=True), encoding="utf-8")
    load_config(path)  # la config écrite doit rester valide
    return path


def read_files(files: list[Path]) -> list[dict[str, Any]]:
    if not files:
        return []
    return pa.concat_tables([pq.read_table(f) for f in files]).to_pylist()


def source_rows(cfg: ReferentialConfig, dataset: Path, day: int, source_id: str) -> list[dict]:
    """Lignes que la voie ELT lit pour cette source : table PostgreSQL du jour (full) ou fichiers
    de `location` (parquet) ; la référence reçoit exactement les mêmes lignes."""
    src = cfg.source(source_id)
    if src.kind == "postgres":
        files = sorted((dataset / f"day{day}" / source_id).glob("part-*.parquet"))
        if src.snapshot == "delta" and day > 0:  # la table est alimentée par upsert (cumul)
            files = [f for d in range(day + 1) for f in _day_files(dataset, d, source_id)]
            return _latest_by_key(read_files(files), src.key)
        return read_files(files)
    return read_files([Path(f) for f in sorted(glob.glob(src.location))])


def _day_files(dataset: Path, day: int, source_id: str) -> list[Path]:
    return sorted((dataset / f"day{day}" / source_id).glob("part-*.parquet"))


def _latest_by_key(rows: list[dict], key: str) -> list[dict]:
    latest: dict[Any, dict] = {}
    for row in rows:
        latest[row[key]] = row
    return list(latest.values())


@dataclass
class Harness:
    """Une voie ELT + la référence nourries avec les mêmes lots, jour après jour."""

    cfg: ReferentialConfig
    dataset: Path
    dsn: str
    tmp: Path
    lane: Lane = field(init=False)
    ref: ReferenceEngine = field(init=False)
    last: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.lane = Lane(self.cfg, Settings.from_env(self.cfg))
        self.lane.reset()
        self.lane.init()
        self.ref = ReferenceEngine(self.cfg)

    def stage(self, day: int) -> None:
        load_postgres(self.cfg, self.dsn, self.dataset, day=day)
        stage_live(self.dataset, day)

    async def ingest_day(self, day: int, parallel: bool = True) -> dict[str, Any]:
        self.stage(day)
        run = f"day{day}"
        ids = [s.id for s in self.cfg.sources]
        if parallel:
            out = await self.lane.ingest(ids, run)
            lane_counters = out["counters"]
        else:
            lane_counters = {}
            for sid in ids:
                lane_counters[sid] = (await self.lane.ingest([sid], run))["counters"]
        for sid in ids:
            result = self.ref.ingest(sid, source_rows(self.cfg, self.dataset, day, sid), run)
            got = {k: lane_counters[sid][k] for k in INGEST_FIELDS}
            want = {k: getattr(result, k) for k in INGEST_FIELDS}
            assert got == want, (day, sid, got, want)
        return lane_counters

    async def reconcile_day(self, day: int) -> dict[str, Any]:
        run = f"day{day}"
        result = await self.lane.reconcile(run)
        want = self.ref.reconcile(run)
        got = {k: result["counters"][k] for k in RECONCILE_FIELDS}
        assert got == {k: getattr(want, k) for k in RECONCILE_FIELDS}, (day, got, want)
        self.last = result
        return result

    async def run_day(self, day: int, parallel: bool = True) -> dict[str, Any]:
        await self.ingest_day(day, parallel)
        return await self.reconcile_day(day)

    def export(self, name: str = "export") -> dict[str, pa.Table]:
        out = self.tmp / name
        if out.exists():
            shutil.rmtree(out)
        self.lane.export(out)
        return read_parquet_dir(out)

    def compare(self, **kw: Any) -> Report:
        kw.setdefault("compare_hash", True)
        return compare_exports(self.ref.export(), self.export(), **kw)

    def assert_equivalent(self, **kw: Any) -> None:
        report = self.compare(**kw)
        assert report.ok, report.format()


def make_synth(
    tmp: Path,
    *,
    nodes: int,
    attributes: int,
    sources: int = 3,
    seed: int = 1,
    kinds: tuple[str, ...] = ("postgres", "parquet", "parquet"),
    snapshots: tuple[str, ...] = ("full", "full", "delta"),
    days: int = 2,
    delta: DeltaParams | None = None,
    schema: str = "mdm_elt_test",
) -> tuple[ReferentialConfig, Path]:
    """Jeu synthétique déterministe (<= quelques milliers de nœuds) et sa config."""
    params = SynthParams(
        nodes=nodes,
        attributes=attributes,
        sources=sources,
        seed=seed,
        workers=1,
        kinds=kinds,
        snapshots=snapshots,
    )
    dataset = tmp / "dataset"
    generate(params, dataset, days=days, delta=delta or DeltaParams())
    return with_schema(load_config(dataset / "config.yml"), schema), dataset


DELTA_MIX = DeltaParams(
    update_pct=8, new_pct=4, delete_pct=3, broken_pct=2, merge_pct=15, split_pct=15
)


def write_rows(directory: Path, rows: list[dict[str, Any]], columns: list[str]) -> None:
    """Remplace le contenu de `directory` par un fichier Parquet de ces lignes (texte/entiers)."""
    directory.mkdir(parents=True, exist_ok=True)
    for old in directory.glob("*.parquet"):
        old.unlink()
    arrays = {}
    for col in columns:
        values = [r.get(col) for r in rows]
        arrays[col] = (
            pa.array(values, type=pa.string())
            if all(v is None for v in values)
            else pa.array(values)
        )
    pq.write_table(pa.table(arrays), directory / "batch.parquet")


@dataclass
class RowsHarness:
    """Sources parquet écrites à la main (valeurs brutes texte) et référence en parallèle."""

    cfg: ReferentialConfig
    root: Path
    lane: Lane = field(init=False)
    ref: ReferenceEngine = field(init=False)
    columns: dict[str, list[str]] = field(init=False)

    def __post_init__(self) -> None:
        self.lane = Lane(self.cfg, Settings.from_env(self.cfg))
        self.lane.reset()
        self.lane.init()
        self.ref = ReferenceEngine(self.cfg)
        self.columns = {}
        for s in self.cfg.sources:
            cols = [s.key, *([s.revision] if s.revision else []), *self.cfg.mapping(s.id).values()]
            self.columns[s.id] = list(dict.fromkeys(cols))

    async def step(self, run: str, **batches: list[dict]) -> dict[str, Any]:
        """Ingère les lots donnés (source=lignes) puis réconcilie ; compare à la référence."""
        for sid, rows in batches.items():
            write_rows(self.root / sid, rows, self.columns[sid])
        ids = list(batches)
        out = await self.lane.ingest(ids, run)
        counters = out["counters"] if len(ids) > 1 else {ids[0]: out["counters"]}
        for sid in ids:
            result = self.ref.ingest(sid, batches[sid], run)
            got = {k: counters[sid][k] for k in INGEST_FIELDS} | {
                "blocked": counters[sid]["blocked"]
            }
            want = {k: getattr(result, k) for k in INGEST_FIELDS} | {"blocked": result.blocked}
            assert got == want, (run, sid, got, want)
        rec = await self.lane.reconcile(run)
        want_rec = self.ref.reconcile(run)
        got_rec = {k: rec["counters"][k] for k in RECONCILE_FIELDS}
        assert got_rec == {k: getattr(want_rec, k) for k in RECONCILE_FIELDS}, (run, got_rec)
        return rec

    def export(self, name: str = "export") -> dict[str, pa.Table]:
        out = self.root / name
        if out.exists():
            shutil.rmtree(out)
        self.lane.export(out)
        return read_parquet_dir(out)

    def assert_equivalent(self, **kw: Any) -> None:
        kw.setdefault("compare_hash", True)
        report = compare_exports(self.ref.export(), self.export(), **kw)
        assert report.ok, report.format()
