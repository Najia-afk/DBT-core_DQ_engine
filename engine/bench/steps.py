"""Étapes nommées du banc (échelle, équivalence) : paramètres figés pour la reproductibilité."""

from __future__ import annotations

from dataclasses import dataclass

from .dataset import DatasetSpec

SMALL_DELTA = {"new_pct": 0.5, "delete_pct": 0.2, "broken_pct": 0.1}
HEAVY_DELTA = {
    "update_pct": 8.0, "new_pct": 4.0, "delete_pct": 3.0, "broken_pct": 2.0,
    "merge_pct": 25.0, "split_pct": 25.0,
}  # fmt: skip


@dataclass(frozen=True)
class Step:
    name: str
    nodes: int
    attributes: int
    update_pcts: tuple[float, ...]
    seed: int = 42

    def dataset(self, *, kinds: tuple[str, ...] | None = None, workers: int = 8) -> DatasetSpec:
        deltas = tuple({**SMALL_DELTA, "update_pct": u} for u in self.update_pcts)
        return DatasetSpec(
            nodes=self.nodes,
            attributes=self.attributes,
            seed=self.seed,
            kinds=kinds or ("parquet", "parquet", "parquet"),
            snapshots=("full", "full", "delta"),
            deltas=deltas,
            workers=workers,
        )


LADDER: dict[str, Step] = {
    "L1": Step("L1", 100_000, 50, (1.0, 5.0, 20.0)),
    "L2": Step("L2", 300_000, 200, (1.0, 5.0)),
    "L3": Step("L3", 1_000_000, 200, (1.0, 5.0)),
    "G1": Step("G1", 100_000, 50, (1.0, 5.0, 20.0)),
    "G2": Step("G2", 200_000, 50, (1.0, 5.0)),
    "G3": Step("G3", 100_000, 200, (1.0, 5.0)),
    "C50": Step("C50", 50_000, 30, (1.0, 5.0, 20.0)),  # comparaison voie à voie (graphe)
    "S2K": Step("S2K", 2_000, 20, (5.0,)),  # test d'intégration
}


def equivalence_dataset(nodes: int = 10_000, attributes: int = 30, days: int = 3) -> DatasetSpec:
    return DatasetSpec(
        nodes=nodes,
        attributes=attributes,
        seed=7,
        kinds=("parquet", "parquet", "parquet"),
        snapshots=("full", "full", "delta"),
        deltas=tuple(HEAVY_DELTA for _ in range(days)),
        workers=4,
    )
