"""Projection vers 1 M nœuds × 200 attributs : mesure si disponible, sinon extrapolation."""

from __future__ import annotations

import math
from typing import Any

TARGET_NODES = 1_000_000
TARGET_ATTRS = 200
TARGET_CELLS = TARGET_NODES * TARGET_ATTRS
WINDOW_H = 6.0
REFERENTIALS_PER_DAY = 50


def cells(run: dict[str, Any]) -> int:
    return int(run["config"]["nodes"]) * int(run["config"]["attributes"])


def phase_wall(run: dict[str, Any], day: int, phase: str) -> float | None:
    for p in run["phases"]:
        if p["day"] == day and p["phase"] == phase and p["status"] == "ok":
            return float(p["wall_s"])
    return None


def day_for_update(run: dict[str, Any], pct: float) -> int | None:
    for i, delta in enumerate(run["config"]["deltas"], start=1):
        if float(delta["update_pct"]) == pct:
            return i
    return None


def scale(points: list[tuple[float, float]], target: float) -> tuple[float, float, float | None]:
    """(borne basse, borne haute, exposant) du temps à `target` cellules.

    Modèle linéaire (exposant 1) depuis le plus grand point ; si deux tailles sont mesurées,
    exposant ajusté k = ln(t2/t1)/ln(c2/c1) depuis le plus grand point. Les deux estimations
    encadrent l'extrapolation (sans prétendre à un intervalle de confiance).
    """
    pts = sorted(points)
    c2, t2 = pts[-1]
    linear = t2 * target / c2
    if len(pts) < 2 or pts[-2][1] <= 0 or t2 <= 0 or pts[-2][0] == c2:
        return linear, linear, None
    c1, t1 = pts[-2]
    k = math.log(t2 / t1) / math.log(c2 / c1)
    fitted = t2 * (target / c2) ** k
    return min(linear, fitted), max(linear, fitted), k


def lane_points(
    runs: list[dict[str, Any]], pct: float | None, phase: str
) -> list[tuple[float, float]]:
    """Points (cellules, secondes) mesurés pour une phase (jour 0 si pct est None)."""
    out = []
    for run in runs:
        day = 0 if pct is None else day_for_update(run, pct)
        if day is None:
            continue
        wall = phase_wall(run, day, phase)
        if wall is not None:
            out.append((float(cells(run)), wall))
    return out


def measured_target(runs: list[dict[str, Any]]) -> dict[str, Any] | None:
    for run in runs:
        if run["config"]["nodes"] >= TARGET_NODES and run["config"]["attributes"] >= TARGET_ATTRS:
            return run
    return None


def describe(low: float, high: float, measured: bool) -> str:
    if measured:
        return f"{low / 60:.1f} min (mesuré)"
    if abs(high - low) < 1e-6:
        return f"≈ {low / 60:.1f} min (extrap. linéaire)"
    return f"≈ {low / 60:.0f}–{high / 60:.0f} min (extrap.)"


def lane_projection(lane: str, runs: list[dict[str, Any]], pct: float = 5.0) -> dict[str, Any]:
    ok_runs = [r for r in runs if r["lane"] == lane]
    target = measured_target(ok_runs)
    rows: dict[str, Any] = {"lane": lane, "measured": target is not None, "metrics": {}}
    for key, day_pct, phase in (
        ("day0_ingest", None, "ingest"),
        ("day0_reconcile", None, "reconcile"),
        ("daily_ingest", pct, "ingest"),
        ("daily_reconcile", pct, "reconcile"),
    ):
        if target is not None:
            day = 0 if day_pct is None else day_for_update(target, day_pct)
            wall = phase_wall(target, day, phase) if day is not None else None
            if wall is not None:
                rows["metrics"][key] = (wall, wall, None, True)
                continue
        pts = lane_points(ok_runs, day_pct, phase)
        if pts:
            low, high, k = scale(pts, TARGET_CELLS)
            rows["metrics"][key] = (low, high, k, False)
    rows["basis"] = sorted({r["name"] for r in ok_runs})
    return rows


def state_size_gb(runs: list[dict[str, Any]]) -> tuple[float, float, bool] | None:
    """Taille de la base d'état après le jour 0 : (basse, haute, mesuré)."""
    target = measured_target(runs)
    if target is not None:
        for p in target["phases"]:
            if p["day"] == 0 and p["phase"] == "reconcile" and p["status"] == "ok":
                gb = p["pg_db_mb"] / 1024
                return gb, gb, True
    pts = []
    for run in runs:
        for p in run["phases"]:
            if p["day"] == 0 and p["phase"] == "reconcile" and p["status"] == "ok":
                pts.append((float(cells(run)), p["pg_db_mb"] / 1024))
    if not pts:
        return None
    low, high, _ = scale(pts, TARGET_CELLS)
    return low, high, False


def capacity(seconds: float, parallel: int = 1, window_h: float = WINDOW_H) -> float:
    return parallel * window_h * 3600.0 / seconds if seconds > 0 else 0.0


def render_projection(runs: list[dict[str, Any]], pct: float = 5.0) -> str:
    lines = [
        "## Projection 1 M nœuds × 200 attributs (généré)",
        "",
        "Le **mesuré** est étiqueté « mesuré ». Le reste est une **extrapolation** par cellules "
        "(nœuds × attributs) : borne linéaire (exposant 1) et borne par exposant ajusté sur les "
        "deux plus grandes tailles mesurées (un coût fixe par invocation fausse l'ajustement "
        "aux petites tailles). Aucune incertitude statistique n'est calculable avec 2 points.",
        "",
        "| voie | base | jour 0 : ingest + reconcile | quotidien "
        f"({pct:g} % de mises à jour) : ingest + reconcile | état PG après jour 0 |",
        "|---|---|---|---|---|",
    ]
    capacity_rows: list[str] = []
    for lane in ("etl", "elt", "graph"):
        lane_runs = [
            r
            for r in runs
            if r["lane"] == lane and r["status"] == "ok" and not r["name"].startswith("equiv-")
        ]
        if not lane_runs:
            continue
        largest_ok = max(cells(r) for r in lane_runs)
        blocked = [
            r for r in runs if r["lane"] == lane and r["status"] != "ok" and cells(r) > largest_ok
        ]
        if blocked:
            lines.append(
                f"| {lane} | {', '.join(r['name'] for r in blocked)} | "
                "non validé : échec à une taille supérieure | "
                "capacité non extrapolée | non validé |"
            )
            continue
        proj = lane_projection(lane, lane_runs, pct)
        m = proj["metrics"]

        def pair(a: str, b: str, m: dict = m) -> str:
            if a not in m or b not in m:
                return "–"
            low = m[a][0] + m[b][0]
            high = m[a][1] + m[b][1]
            return describe(low, high, m[a][3] and m[b][3])

        size = state_size_gb(lane_runs)
        size_txt = "–"
        if size:
            low, high, measured = size
            size_txt = (
                f"{low:.1f} Go (mesuré)" if measured else f"≈ {low:.0f}–{high:.0f} Go (extrap.)"
            )
        lines.append(
            f"| {lane} | {', '.join(proj['basis'])} | {pair('day0_ingest', 'day0_reconcile')} | "
            f"{pair('daily_ingest', 'daily_reconcile')} | {size_txt} |"
        )
        if {"daily_ingest", "daily_reconcile"} <= set(m):
            low = m["daily_ingest"][0] + m["daily_reconcile"][0]
            high = m["daily_ingest"][1] + m["daily_reconcile"][1]
            measured = m["daily_ingest"][3] and m["daily_reconcile"][3]
            label = "mesuré" if measured else "extrap."
            capacity_rows.append(
                f"| {lane} | {label} | {low / 60:.1f}–{high / 60:.1f} | "
                f"{capacity(high):.0f}–{capacity(low):.0f} | "
                f"{capacity(high, 3):.0f}–{capacity(low, 3):.0f} | "
                f"{REFERENTIALS_PER_DAY * low / 3600 / WINDOW_H:.1f}–"
                f"{REFERENTIALS_PER_DAY * high / 3600 / WINDOW_H:.1f} | "
                f"{(size[0] * REFERENTIALS_PER_DAY if size else 0):.0f}–"
                f"{(size[1] * REFERENTIALS_PER_DAY if size else 0):.0f} |"
            )
    if capacity_rows:
        lines += [
            "",
            f"Capacité quotidienne sur cette machine, fenêtre de {WINDOW_H:g} h, exécutions "
            "séquentielles (1 run) ou 3 runs parallèles **en supposant un passage à l'échelle "
            "parfait ×3 (borne haute, non mesurée à cette taille)** :",
            "",
            "| voie | statut | min / référentiel (quotidien) | réf. en 6 h, 1 run | "
            "réf. en 6 h, 3 parallèles (borne haute) | runs parallèles requis pour 50 réf. | "
            "disque d'état pour 50 réf. (Go) |",
            "|---|---|---|---|---|---|---|",
            *capacity_rows,
        ]
    return "\n".join(lines)
