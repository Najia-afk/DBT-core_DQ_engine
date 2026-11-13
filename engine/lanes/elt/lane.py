"""Façade de la voie ELT : configuration, état PostgreSQL, ingest, reconcile, export, reset."""

from __future__ import annotations

import asyncio
import copy
import shutil
import time
from pathlib import Path
from typing import Any

from engine.core.canonical import TABLES, canonical_schemas
from engine.core.config import ReferentialConfig, load_config

from . import statedb
from .extract import connect_duckdb
from .ingest import ingest_source
from .reconcile import reconcile_run
from .settings import Settings, env_secret, state_dsn
from .sqlgen import Spec, lit, q


class Lane:
    def __init__(self, config: ReferentialConfig | str | Path, settings: Settings | None = None):
        self.config = config if isinstance(config, ReferentialConfig) else load_config(config)
        self.settings = settings or Settings.from_env(self.config)
        self.spec = Spec(self.config, keep_none_pairs=self.settings.keep_none_pairs)
        self._dsn: str | None = None

    @property
    def dsn(self) -> str:
        if self._dsn is None:
            self._dsn = state_dsn(self.config)
        return self._dsn

    def run_dir(self, run_id: str) -> Path:
        path = self.settings.referential_dir(self.config) / "runs" / run_id
        path.mkdir(parents=True, exist_ok=True)
        return path

    def summary(
        self,
        command: str,
        run_id: str | None,
        counters: dict[str, Any],
        phases: dict[str, float],
        **extra: Any,
    ) -> dict[str, Any]:
        return {
            "lane": "elt",
            "command": command,
            "referential": self.config.referential,
            "run_id": run_id,
            "status": "ok",
            **extra,
            "counters": counters,
            "seconds": {k: round(v, 3) for k, v in phases.items()},
        }

    def init(self) -> dict[str, Any]:
        t = time.perf_counter()
        info = statedb.init_state(self.dsn, self.spec)
        self.settings.referential_dir(self.config)
        return self.summary("init", None, info, {"total": time.perf_counter() - t})

    def reset(self) -> dict[str, Any]:
        t = time.perf_counter()
        info = statedb.reset_state(self.dsn, self.spec)
        shutil.rmtree(self.settings.referential_dir(self.config), ignore_errors=True)
        return self.summary("reset", None, info, {"total": time.perf_counter() - t})

    async def ingest(self, source_ids: list[str], run_id: str) -> dict[str, Any]:
        """Ingère une ou plusieurs sources ; plusieurs sources tournent en parallèle (processus
        dbt et fichiers DuckDB distincts, ressources partagées)."""
        known = {s.id for s in self.config.sources}
        for sid in source_ids:
            if sid not in known:
                raise statedb.LaneError(f"unknown source {sid!r}")
        if len(source_ids) == 1:
            return await ingest_source(self, source_ids[0], run_id)
        shared = copy.copy(self)
        shared.settings = self.settings.share(len(source_ids))
        t = time.perf_counter()
        results = await asyncio.gather(*(ingest_source(shared, sid, run_id) for sid in source_ids))
        merged = self.summary(
            "ingest",
            run_id,
            {r["source"]: r["counters"] for r in results},
            {"total": time.perf_counter() - t},
            sources=source_ids,
        )
        merged["per_source_seconds"] = {r["source"]: r["seconds"] for r in results}
        return merged

    async def reconcile(self, run_id: str) -> dict[str, Any]:
        return await reconcile_run(self, run_id)

    def export(self, out: str | Path) -> dict[str, Any]:
        """Projections canoniques (un Parquet par table) lues depuis l'état PostgreSQL."""
        t = time.perf_counter()
        target = Path(out)
        target.mkdir(parents=True, exist_ok=True)
        schemas = canonical_schemas(self.config)
        spill = self.settings.referential_dir(self.config) / "spill-export"
        con = connect_duckdb(self.settings, spill)
        counts: dict[str, int] = {}
        try:
            con.execute("INSTALL postgres")
            con.execute("LOAD postgres")
            dsn = env_secret(self.config.state.dsn_env)
            con.execute("ATTACH ? AS pg (TYPE postgres, READ_ONLY)", [dsn])
            for table in TABLES:
                cols = ", ".join(q(n) for n in schemas[table].names)
                path = (target / f"{table}.parquet").as_posix()
                con.execute(
                    f"COPY (SELECT {cols} FROM pg.{self.spec.schema}.{table}) TO {lit(path)}"
                    " (FORMAT parquet, COMPRESSION zstd)"
                )
                row = con.execute(f"SELECT count(*) FROM read_parquet({lit(path)})").fetchone()
                counts[table] = int(row[0]) if row else 0
        finally:
            con.close()
            shutil.rmtree(spill, ignore_errors=True)
        return self.summary(
            "export", None, counts, {"total": time.perf_counter() - t}, out=str(target)
        )
