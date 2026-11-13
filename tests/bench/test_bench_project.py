"""Projection 1 M × 200 : modèle d'extrapolation et étiquetage mesuré / extrapolé."""

from __future__ import annotations

import math

from engine.bench import project
from engine.bench.project import capacity, lane_projection, render_projection, scale


def make_run(lane: str, nodes: int, attrs: int, day0_ingest: float, day0_rec: float,
             d5_ingest: float, d5_rec: float, db_mb: float = 1000.0) -> dict:  # fmt: skip
    def phase(day: int, name: str, wall: float, **extra: float) -> dict:
        return {"day": day, "phase": name, "wall_s": wall, "status": "ok", "pg_db_mb": db_mb,
                **extra}  # fmt: skip

    return {
        "name": f"{lane}-{nodes}", "lane": lane, "status": "ok",
        "config": {"nodes": nodes, "attributes": attrs,
                   "deltas": [{"update_pct": 1.0}, {"update_pct": 5.0}]},
        "phases": [phase(0, "ingest", day0_ingest), phase(0, "reconcile", day0_rec),
                   phase(1, "ingest", d5_ingest / 2), phase(1, "reconcile", d5_rec / 2),
                   phase(2, "ingest", d5_ingest), phase(2, "reconcile", d5_rec)],
    }  # fmt: skip


def test_scale_linear_from_one_point() -> None:
    low, high, k = scale([(100.0, 10.0)], 1000.0)
    assert (low, high, k) == (100.0, 100.0, None)


def test_scale_fits_exponent_from_two_points() -> None:
    low, high, k = scale([(100.0, 10.0), (1000.0, 100.0)], 10_000.0)
    assert k is not None and math.isclose(k, 1.0)
    assert math.isclose(low, 1000.0) and math.isclose(high, 1000.0)
    low, high, k = scale([(100.0, 10.0), (1000.0, 1000.0)], 10_000.0)  # exposant 2
    assert k is not None and math.isclose(k, 2.0)
    assert math.isclose(low, 10_000.0) and math.isclose(high, 100_000.0)  # linéaire vs k = 2


def test_capacity() -> None:
    assert capacity(3600.0, parallel=1, window_h=6.0) == 6.0
    assert capacity(3600.0, parallel=3, window_h=6.0) == 18.0
    assert capacity(0.0) == 0.0


def test_projection_is_labelled_extrapolated_without_a_1m_run() -> None:
    runs = [make_run("etl", 100_000, 50, 10, 30, 5, 10, 500),
            make_run("etl", 300_000, 200, 50, 190, 20, 20, 3600)]  # fmt: skip
    proj = lane_projection("etl", runs)
    assert proj["measured"] is False
    low, high, k, measured = proj["metrics"]["day0_ingest"]
    assert measured is False and k is not None and low > 0 and high >= low
    text = render_projection(runs)
    assert "extrap." in text and "(mesuré)" not in text.split("capacité")[0]


def test_projection_uses_the_measured_1m_run_when_present() -> None:
    runs = [make_run("etl", 300_000, 200, 50, 190, 20, 20),
            make_run("etl", 1_000_000, 200, 160, 700, 60, 80, 12_000)]  # fmt: skip
    proj = lane_projection("etl", runs)
    assert proj["measured"] is True
    assert proj["metrics"]["day0_reconcile"] == (700.0, 700.0, None, True)
    assert project.state_size_gb(runs) == (12_000 / 1024, 12_000 / 1024, True)
    assert "mesuré" in render_projection(runs)


def test_failure_at_larger_scale_prevents_capacity_claim() -> None:
    small = make_run("elt", 100_000, 50, 10, 30, 5, 10)
    failed = make_run("elt", 300_000, 200, 50, 190, 20, 20)
    failed["status"] = "aborted"
    text = render_projection([small, failed])
    assert "capacité non extrapolée" in text
    assert "réf. en 6 h" not in text
