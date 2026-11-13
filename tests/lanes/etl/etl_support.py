"""Test helpers: dataset, lane runner (in-process CLI), reference engine fed the same data."""

from __future__ import annotations

import contextlib
import io
import json
import shutil
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from engine.core.canonical import read_parquet_dir
from engine.core.compare import Report, compare_exports
from engine.core.config import ReferentialConfig, load_config
from engine.core.reference import ReferenceEngine
from engine.core.synth import DeltaParams, SynthParams, generate
from engine.core.synth.pg import load_postgres
from engine.lanes.etl.cli import main as cli_main

SMALL = {
    "nodes": 400, "attributes": 12, "sources": 3, "seed": 5, "workers": 1,
    "kinds": ("postgres", "postgres", "parquet"), "snapshots": ("full", "full", "delta"),
}  # fmt: skip
HEAVY_DELTA = DeltaParams(
    update_pct=8, new_pct=4, delete_pct=3, broken_pct=2, merge_pct=25, split_pct=25
)


def make_dataset(root: Path, days: int = 2, delta: DeltaParams = HEAVY_DELTA, **over: Any) -> Path:
    params = SynthParams(**{**SMALL, **over})
    out = root / "ds"
    generate(params, out, days=days, delta=delta, force=True)
    return out


def read_day(ds: Path, day: int, source: str) -> list[dict[str, Any]]:
    files = sorted((ds / f"day{day}" / source).glob("part-*.parquet"))
    return pa.concat_tables([pq.read_table(f) for f in files]).to_pylist()


def stage_parquet(ds: Path, cfg: ReferentialConfig, day: int) -> None:
    """Make `live/<source>/` hold exactly the files of the day (source `location` target)."""
    for source in cfg.sources:
        if source.kind != "parquet":
            continue
        live = ds / "live" / source.id
        shutil.rmtree(live, ignore_errors=True)
        live.mkdir(parents=True)
        for part in sorted((ds / f"day{day}" / source.id).glob("part-*.parquet")):
            shutil.copy2(part, live / part.name)


@dataclass
class Lab:
    ds: Path
    dsn: str
    monkeypatch: Any
    out: Path
    config_name: str = "config.yml"
    cfg: ReferentialConfig = field(init=False)
    cfg_path: Path = field(init=False)
    ref: ReferenceEngine = field(init=False)
    last: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.cfg_path = self.ds / self.config_name
        self.cfg = load_config(self.cfg_path)
        self.ref = ReferenceEngine(self.cfg)
        self.monkeypatch.setenv(self.cfg.state.dsn_env, self.dsn)
        for s in self.cfg.sources:
            if s.dsn_env:
                self.monkeypatch.setenv(s.dsn_env, self.dsn)

    # -- lane ---------------------------------------------------------------
    def lane(self, *argv: str, ok: bool = True) -> dict[str, Any]:
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            code = cli_main([argv[0], "--config", str(self.cfg_path), *argv[1:]])
        lines = [ln for ln in buf.getvalue().splitlines() if ln.startswith("{")]
        assert len(lines) == 1, buf.getvalue()
        summary = json.loads(lines[0])
        if ok:
            assert code == 0, summary
        self.last = summary
        return summary

    def load_sources(self, day: int) -> None:
        load_postgres(self.cfg, self.dsn, self.ds, day=day, workers=2)
        stage_parquet(self.ds, self.cfg, day)

    def lane_day(
        self, day: int, run_id: str | None = None, sources: list[str] | None = None
    ) -> dict:
        run_id = run_id or f"d{day}"
        self.load_sources(day)
        out = {}
        for s in self.cfg.sources:
            if sources is None or s.id in sources:
                out[s.id] = self.lane("ingest", "--source", s.id, "--run-id", run_id)
        out["reconcile"] = self.lane("reconcile", "--run-id", run_id)
        return out

    def ref_day(self, day: int, run_id: str | None = None) -> None:
        run_id = run_id or f"d{day}"
        for s in self.cfg.sources:
            self.ref.ingest(s.id, read_day(self.ds, day, s.id), run_id)
        self.ref.reconcile(run_id)

    def export(self, name: str = "export") -> dict[str, pa.Table]:
        target = self.out / name
        shutil.rmtree(target, ignore_errors=True)
        self.lane("export", "--out", str(target))
        return read_parquet_dir(target)

    def compare(self, **kw: Any) -> Report:
        return compare_exports(self.ref.export(), self.export(), **kw)

    def sql(self, query: str, params: Any = None) -> list[tuple]:
        import psycopg

        with psycopg.connect(self.dsn, autocommit=True) as conn:
            conn.execute(f"SET search_path = {self.cfg.state.schema_name}")
            return conn.execute(query, params).fetchall()
