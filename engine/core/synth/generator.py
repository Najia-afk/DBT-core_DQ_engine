"""Orchestration : état des lignes source, jour 0, deltas, écriture Parquet, vérité, manifeste."""

from __future__ import annotations

import json
import shutil
import time
from concurrent.futures import Executor, ProcessPoolExecutor
from dataclasses import asdict
from datetime import date
from pathlib import Path
from typing import Any

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from .columns import Block, build_column, key_strings
from .params import MAX_DAYS, DeltaParams, SynthParams
from .plan import Plan, build_plan, plan_from_json, write_config

INSERT, UPDATE, DELETE, MERGE, SPLIT, BROKEN = 1, 2, 3, 4, 5, 6
LABELS = ["insert", "update", "delete", "merge", "split", "broken"]
FIELDS = ("key", "ent", "home", "upd", "broken", "dup", "rev", "alive")
DAY_US = 86_400 * 1_000_000
OWNED = ("manifest.json", "config.yml", "truth", "live", "_state")


def start_us(params: SynthParams) -> int:
    return (date.fromisoformat(params.start_date) - date(1970, 1, 1)).days * DAY_US


class SourceState:
    def __init__(self, arrays: dict[str, np.ndarray]):
        self.a = arrays

    def __getitem__(self, name: str) -> np.ndarray:
        return self.a[name]

    def __len__(self) -> int:
        return len(self.a["key"])

    def append(self, new: dict[str, np.ndarray]) -> None:
        for f in FIELDS:
            self.a[f] = np.concatenate([self.a[f], new[f]])


class State:
    """État courant (un jour) : une ligne par clé source ayant existé, vivante ou non."""

    def __init__(self, day: int, next_entity: int, sources: list[SourceState]):
        self.day = day
        self.next_entity = next_entity
        self.sources = sources

    def save(self, out: Path) -> None:
        d = out / "_state"
        d.mkdir(parents=True, exist_ok=True)
        flat: dict[str, np.ndarray] = {
            "day": np.array(self.day),
            "next_entity": np.array(self.next_entity),
            "n_sources": np.array(len(self.sources)),
        }
        for i, st in enumerate(self.sources):
            for f in FIELDS:
                flat[f"s{i}_{f}"] = st[f]
        tmp = d / "current.tmp.npz"
        np.savez(tmp, **flat)
        tmp.replace(d / "current.npz")

    @classmethod
    def load(cls, out: Path) -> State:
        with np.load(out / "_state" / "current.npz") as z:
            n = int(z["n_sources"])
            sources = [SourceState({f: z[f"s{i}_{f}"] for f in FIELDS}) for i in range(n)]
            return cls(int(z["day"]), int(z["next_entity"]), sources)


def initial_state(plan: Plan) -> State:
    p = plan.params
    rng = np.random.default_rng([p.seed, 0xD0])
    n, n_src = p.nodes, p.sources
    present = rng.random((n, n_src)) < p.overlap
    orphan = np.flatnonzero(~present.any(axis=1))
    present[orphan, rng.integers(0, n_src, orphan.size)] = True
    t0 = start_us(p)
    sources = []
    for s in range(n_src):
        ents = np.flatnonzero(present[:, s]).astype(np.int64)
        n_dup = int(round(len(ents) * p.dup_rate))
        dup_from = rng.choice(len(ents), size=n_dup, replace=False)
        ent = np.concatenate([ents, ents[dup_from]])
        dup = np.concatenate([np.zeros(len(ents), bool), np.ones(n_dup, bool)])
        order = rng.permutation(len(ent))
        ent, dup = ent[order], dup[order]
        rows = len(ent)
        sources.append(
            SourceState(
                {
                    "key": plan.sources[s].key_base + np.arange(rows, dtype=np.int64),
                    "ent": ent,
                    "home": ent.copy(),
                    "upd": np.zeros(rows, dtype=np.uint64),
                    "broken": np.full(rows, -1, dtype=np.int16),
                    "dup": dup,
                    "rev": t0 - rng.integers(0, 90 * 86_400, rows) * 1_000_000,
                    "alive": np.ones(rows, dtype=bool),
                }
            )
        )
    return State(0, n, sources)


def _count(total: int, pct: float) -> int:
    return int(round(total * pct / 100))


def evolve(plan: Plan, st: State, dp: DeltaParams, day: int) -> list[dict[str, Any]]:
    """Applique le jour `day` à l'état ; retourne par source les lignes modifiées et compteurs."""
    p = plan.params
    ts_day = start_us(p) + day * DAY_US
    pool_ent = np.concatenate([s["ent"][s["alive"]] for s in st.sources])
    if pool_ent.size == 0:
        raise ValueError("no active row left to evolve")
    mult = np.bincount(pool_ent, minlength=st.next_entity)
    results = []
    for s, src in enumerate(plan.sources):
        a = st.sources[s]
        rng = np.random.default_rng([p.seed, day, s, 0xDE17A])
        alive_idx = np.flatnonzero(a["alive"])
        total = len(alive_idx)
        perm = rng.permutation(alive_idx)
        candidates = plan.broken_candidates(s)
        n_del = _count(total, dp.delete_pct) if src.snapshot == "full" else 0
        n_upd = _count(total, dp.update_pct)
        n_brk = _count(total, dp.broken_pct) if candidates.size else 0
        n_new = _count(total, dp.new_pct)
        del_idx = perm[:n_del]
        upd_idx = perm[n_del : n_del + n_upd]
        rest = perm[n_del + n_upd :]
        brk_idx = rest[a["broken"][rest] < 0][:n_brk]

        a["alive"][del_idx] = False
        a["upd"][upd_idx] |= np.uint64(1 << day)
        a["rev"][upd_idx] = ts_day + rng.integers(0, DAY_US, upd_idx.size)
        n_merge = _count(upd_idx.size, dp.merge_pct)
        merge_idx = upd_idx[:n_merge]
        target = pool_ent[rng.integers(0, len(pool_ent), n_merge)]
        ok = target != a["ent"][merge_idx]
        merge_idx = merge_idx[ok]
        a["ent"][merge_idx] = target[ok]
        split_cand = upd_idx[n_merge:]
        split_cand = split_cand[mult[a["ent"][split_cand]] > 1]
        split_idx = split_cand[: _count(upd_idx.size, dp.split_pct)]
        a["ent"][split_idx] = st.next_entity + np.arange(split_idx.size, dtype=np.int64)
        st.next_entity += split_idx.size

        if brk_idx.size:
            a["broken"][brk_idx] = candidates[rng.integers(0, candidates.size, brk_idx.size)]
            a["rev"][brk_idx] = ts_day + rng.integers(0, DAY_US, brk_idx.size)

        n_exist = _count(n_new, dp.new_existing_share * 100)
        new_ent = np.concatenate(
            [
                pool_ent[rng.integers(0, len(pool_ent), n_exist)],
                st.next_entity + np.arange(n_new - n_exist, dtype=np.int64),
            ]
        )
        st.next_entity += n_new - n_exist
        first = len(a)
        a.append(
            {
                "key": src.key_base + first + np.arange(n_new, dtype=np.int64),
                "ent": new_ent,
                "home": new_ent.copy(),
                "upd": np.zeros(n_new, dtype=np.uint64),
                "broken": np.full(n_new, -1, dtype=np.int16),
                "dup": np.zeros(n_new, dtype=bool),
                "rev": ts_day + rng.integers(0, DAY_US, n_new),
                "alive": np.ones(n_new, dtype=bool),
            }
        )
        labels = {
            INSERT: first + np.arange(n_new, dtype=np.int64),
            UPDATE: np.setdiff1d(upd_idx, np.concatenate([merge_idx, split_idx])),
            DELETE: del_idx,
            MERGE: merge_idx,
            SPLIT: split_idx,
            BROKEN: brk_idx,
        }
        idx = np.concatenate([labels[k] for k in sorted(labels)])
        code = np.concatenate([np.full(len(labels[k]), k, dtype=np.int8) for k in sorted(labels)])
        order = np.argsort(idx, kind="stable")
        counts = {name: int(len(labels[i])) for i, name in enumerate(LABELS, start=1)}
        results.append(
            {
                "idx": idx[order],
                "code": code[order],
                "counts": {
                    "previous_active": total,
                    **counts,
                    "active": int(a["alive"].sum()),
                },
            }
        )
    return results


def _gid_tables(plan: Plan) -> tuple[np.ndarray, pa.Table]:
    """Identifiant global de contrôle = offset de l'attribut + indice du contrôle."""
    offsets = np.zeros(len(plan.attrs) + 1, dtype=np.int64)
    rows: list[tuple[str, str, str]] = []
    for a in plan.attrs:
        offsets[a.idx + 1] = offsets[a.idx] + len(a.checks)
        rows += [(a.name, c["rule"], c["on_fail"]) for c in a.checks]
    table = pa.table(
        {
            "attribute": pa.array([r[0] for r in rows], pa.string()),
            "rule": pa.array([r[1] for r in rows], pa.string()),
            "on_fail": pa.array([r[2] for r in rows], pa.string()),
        }
    )
    return offsets, table


def render_part(job: dict[str, Any]) -> dict[str, Any]:
    plan = plan_from_json(job["params_json"])
    p = plan.params
    src = plan.sources[job["source"]]
    blk = Block(plan, src, job["rows"], job["day"])
    offsets, _ = _gid_tables(plan)
    key = key_strings(src, blk.key) if src.key_is_str else pa.array(blk.key, pa.int64())
    rev = pa.array(job["rows"]["rev"], pa.int64()).cast(pa.timestamp("us"))
    arrays, names = [key, rev], [src.key_col, src.rev_col]
    viol_keys: list[np.ndarray] = []
    viol_gid: list[np.ndarray] = []
    for idx, col in src.columns:
        arr, viol, cidx = build_column(blk, plan.attrs[idx])
        arrays.append(arr)
        names.append(col)
        if viol is not None and viol.any():
            rows = np.flatnonzero(viol)
            viol_keys.append(blk.key[rows])
            viol_gid.append(offsets[idx] + cidx[rows])
    path = Path(job["path"])
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_arrays(arrays, names=names)
    pq.write_table(table, path, compression=p.compression, row_group_size=max(blk.n, 1))
    return {
        "source": job["source"],
        "rows": blk.n,
        "bytes": path.stat().st_size,
        "viol_keys": np.concatenate(viol_keys) if viol_keys else np.empty(0, np.int64),
        "viol_gid": np.concatenate(viol_gid) if viol_gid else np.empty(0, np.int64),
    }


def _run(jobs: list[dict[str, Any]], pool: Executor | None) -> list[dict[str, Any]]:
    if pool is None:
        return [render_part(j) for j in jobs]
    return list(pool.map(render_part, jobs, chunksize=1))


def _truth_entities(plan: Plan, st: State) -> pa.Table:
    parts = []
    for s, src in enumerate(plan.sources):
        a = st.sources[s]
        parts.append(
            pa.table(
                {
                    "source_id": pa.array([src.id] * len(a), pa.string()),
                    "source_key": key_strings(src, a["key"]),
                    "entity_id": pa.array(a["ent"], pa.int64()),
                    "alive": pa.array(a["alive"], pa.bool_()),
                }
            )
        )
    return pa.concat_tables(parts)


def _truth_changes(plan: Plan, st: State, results: list[dict[str, Any]]) -> pa.Table:
    parts = []
    for s, src in enumerate(plan.sources):
        r = results[s]
        label = pa.DictionaryArray.from_arrays(
            pa.array(r["code"].astype(np.int8) - 1, pa.int8()), pa.array(LABELS)
        ).cast(pa.string())
        parts.append(
            pa.table(
                {
                    "source_id": pa.array([src.id] * len(r["idx"]), pa.string()),
                    "source_key": key_strings(src, st.sources[s]["key"][r["idx"]]),
                    "change": label,
                }
            )
        )
    return pa.concat_tables(parts)


def _truth_violations(plan: Plan, per_source: dict[int, tuple[np.ndarray, np.ndarray]]) -> pa.Table:
    _, checks = _gid_tables(plan)
    parts = []
    for s, (keys, gid) in per_source.items():
        src = plan.sources[s]
        order = np.argsort(keys, kind="stable")
        picked = checks.take(pa.array(gid[order]))
        parts.append(
            pa.table(
                {
                    "source_id": pa.array([src.id] * len(keys), pa.string()),
                    "source_key": key_strings(src, keys[order]),
                    "attribute": picked["attribute"],
                    "rule": picked["rule"],
                    "on_fail": picked["on_fail"],
                }
            )
        )
    return pa.concat_tables(parts)


def _render_day(
    plan: Plan,
    out: Path,
    day: int,
    st: State,
    selections: list[np.ndarray],
    pool: Executor | None,
) -> dict[str, Any]:
    p = plan.params
    params_json = p.to_json()
    jobs = []
    for s, src in enumerate(plan.sources):
        sel = selections[s]
        for i in range(max(1, -(-len(sel) // p.part_rows))):
            part = sel[i * p.part_rows : (i + 1) * p.part_rows]
            jobs.append(
                {
                    "params_json": params_json,
                    "source": s,
                    "day": day,
                    "path": str(out / f"day{day}" / src.id / f"part-{i:05d}.parquet"),
                    "rows": {f: st.sources[s][f][part] for f in FIELDS if f != "alive"},
                }
            )
    results = _run(jobs, pool)
    stats: dict[str, Any] = {}
    viol: dict[int, tuple[np.ndarray, np.ndarray]] = {}
    for s, src in enumerate(plan.sources):
        mine = [r for r in results if r["source"] == s]
        stats[src.id] = {
            "rows": sum(r["rows"] for r in mine),
            "bytes": sum(r["bytes"] for r in mine),
            "files": len(mine),
        }
        viol[s] = (
            np.concatenate([r["viol_keys"] for r in mine]),
            np.concatenate([r["viol_gid"] for r in mine]),
        )
    truth = out / "truth"
    truth.mkdir(parents=True, exist_ok=True)
    pq.write_table(_truth_violations(plan, viol), truth / f"day{day}_violations.parquet")
    pq.write_table(_truth_entities(plan, st), truth / f"day{day}_entities.parquet")
    return stats


def load_manifest(out: str | Path) -> dict[str, Any]:
    return json.loads((Path(out) / "manifest.json").read_text(encoding="utf-8"))


def _write_manifest(out: Path, manifest: dict[str, Any]) -> None:
    text = json.dumps(manifest, indent=2, sort_keys=True)
    (out / "manifest.json").write_text(text, encoding="utf-8")


def _source_info(plan: Plan) -> list[dict[str, Any]]:
    return [
        {
            "id": s.id,
            "kind": s.kind,
            "snapshot": s.snapshot,
            "location": s.location,
            "key": s.key_col,
            "key_type": "string" if s.key_is_str else "int64",
            "revision": s.rev_col,
            "columns": len(s.columns) + 2,
        }
        for s in plan.sources
    ]


def _pool(params: SynthParams) -> ProcessPoolExecutor | None:
    workers = params.n_workers()
    return ProcessPoolExecutor(max_workers=workers) if workers > 1 else None


def clean_dataset(out: Path) -> None:
    for child in out.iterdir():
        if child.name in OWNED or (child.name.startswith("day") and child.name[3:].isdigit()):
            shutil.rmtree(child) if child.is_dir() else child.unlink()


def generate_day0(
    params: SynthParams, out: str | Path, *, force: bool = False, pool: Executor | None = None
) -> dict[str, Any]:
    out = Path(out)
    if (out / "manifest.json").exists():
        if not force:
            raise FileExistsError(f"{out} already holds a dataset (use force=True)")
        clean_dataset(out)
    out.mkdir(parents=True, exist_ok=True)
    t0 = time.perf_counter()
    plan = build_plan(params)
    st = initial_state(plan)
    t_state = time.perf_counter()
    own_pool = pool is None
    pool = pool if pool is not None else _pool(params)
    try:
        stats = _render_day(plan, out, 0, st, [np.arange(len(s)) for s in st.sources], pool)
    finally:
        if own_pool and pool is not None:
            pool.shutdown()
    st.save(out)
    write_config(params, out / "config.yml", data_root=out)
    info = {
        "day": 0,
        "kind": "initial",
        "sources": stats,
        "entities": int(params.nodes),
        "rows": sum(v["rows"] for v in stats.values()),
        "bytes": sum(v["bytes"] for v in stats.values()),
        "seconds": round(time.perf_counter() - t0, 3),
        "seconds_state": round(t_state - t0, 3),
    }
    manifest = {
        "version": 1,
        "params": asdict(params),
        "sources": _source_info(plan),
        "attributes": [
            {"name": a.name, "type": a.type, "identity": a.identity} for a in plan.attrs
        ],
        "days": {"0": info},
        "deltas": {},
    }
    _write_manifest(out, manifest)
    return info


def generate_delta(
    out: str | Path,
    day: int | None = None,
    delta: DeltaParams | None = None,
    *,
    pool: Executor | None = None,
) -> dict[str, Any]:
    out = Path(out)
    delta = delta or DeltaParams()
    manifest = load_manifest(out)
    params = SynthParams.from_dict(manifest["params"])
    st = State.load(out)
    day = st.day + 1 if day is None else day
    if day != st.day + 1:
        raise ValueError(f"state is at day {st.day}, cannot generate day {day}")
    if not 1 <= day <= MAX_DAYS:
        raise ValueError(f"day must be in 1..{MAX_DAYS}")
    t0 = time.perf_counter()
    plan = build_plan(params)
    results = evolve(plan, st, delta, day)
    st.day = day
    t_state = time.perf_counter()
    selections = []
    for s, src in enumerate(plan.sources):
        if src.snapshot == "full":
            selections.append(np.flatnonzero(st.sources[s]["alive"]))
        else:
            r = results[s]
            selections.append(r["idx"][r["code"] != DELETE])
    own_pool = pool is None
    pool = pool if pool is not None else _pool(params)
    try:
        stats = _render_day(plan, out, day, st, selections, pool)
    finally:
        if own_pool and pool is not None:
            pool.shutdown()
    pq.write_table(_truth_changes(plan, st, results), out / "truth" / f"day{day}_changes.parquet")
    for s, src in enumerate(plan.sources):
        stats[src.id]["changes"] = results[s]["counts"]
    st.save(out)
    info = {
        "day": day,
        "kind": "delta",
        "sources": stats,
        "rows": sum(v["rows"] for v in stats.values()),
        "bytes": sum(v["bytes"] for v in stats.values()),
        "seconds": round(time.perf_counter() - t0, 3),
        "seconds_state": round(t_state - t0, 3),
    }
    manifest["days"][str(day)] = info
    manifest["deltas"][str(day)] = asdict(delta)
    _write_manifest(out, manifest)
    return info


def generate(
    params: SynthParams,
    out: str | Path,
    *,
    days: int = 0,
    delta: DeltaParams | list[DeltaParams] | None = None,
    force: bool = False,
) -> dict[str, Any]:
    """Jour 0 puis `days` deltas. `delta` : mêmes proportions chaque jour, ou une liste par jour."""
    if not 0 <= days <= MAX_DAYS:
        raise ValueError(f"days must be in 0..{MAX_DAYS}")
    out = Path(out)
    deltas = delta if isinstance(delta, list) else [delta or DeltaParams()] * days
    if len(deltas) < days:
        raise ValueError("one DeltaParams per day is required")
    t0 = time.perf_counter()
    pool = _pool(params)
    try:
        infos = [generate_day0(params, out, force=force, pool=pool)]
        for k in range(1, days + 1):
            infos.append(generate_delta(out, k, deltas[k - 1], pool=pool))
    finally:
        if pool is not None:
            pool.shutdown()
    return {
        "out": str(out),
        "days": infos,
        "rows": sum(i["rows"] for i in infos),
        "bytes": sum(i["bytes"] for i in infos),
        "seconds": round(time.perf_counter() - t0, 3),
    }


def stage_live(out: str | Path, day: int) -> dict[str, Any]:
    """Expose le jour `day` aux sources parquet via `live/<source>/` (liens physiques si possible).

    snapshot=full : le contenu de `live/<source>` est remplacé par les fichiers du jour ;
    snapshot=delta : les parts du jour sont ajoutées (préfixe `dayK-`), les précédentes restent.
    """
    out = Path(out)
    staged = {}
    for src in load_manifest(out)["sources"]:
        if src["kind"] != "parquet":
            continue
        target = out / "live" / src["id"]
        if src["snapshot"] == "full" and target.exists():
            shutil.rmtree(target)
        target.mkdir(parents=True, exist_ok=True)
        count = 0
        for f in sorted((out / f"day{day}" / src["id"]).glob("part-*.parquet")):
            dest = target / f"day{day}-{f.name}"
            dest.unlink(missing_ok=True)
            try:
                dest.hardlink_to(f)
            except OSError:
                shutil.copy2(f, dest)
            count += 1
        staged[src["id"]] = count
    return {"day": day, "staged_files": staged}
