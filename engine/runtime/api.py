"""API publique synchrone : soumission, consultation, annulation (sans exécuter de step)."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from engine.runtime.settings import RuntimeSettings
from engine.runtime.store import RuntimeStore, SubmitResult


def connect(dsn: str | None = None, settings: RuntimeSettings | None = None) -> RuntimeStore:
    """Ouvre pg-meta (DSN ou variable MDM_META_DSN) et applique la migration idempotente."""
    dsn = dsn or os.environ.get("MDM_META_DSN")
    if not dsn:
        raise ValueError("pg-meta DSN required (argument or MDM_META_DSN)")
    store = RuntimeStore(dsn, settings)
    store.open()
    store.migrate()
    return store


def submit_run(
    config_path: str | Path,
    *,
    store: RuntimeStore,
    lane: str = "etl",
    idempotency_key: str | None = None,
    params: dict[str, Any] | None = None,
) -> SubmitResult:
    """Persiste un run. Lève AdmissionError (429) ou IdempotencyConflict (409)."""
    return store.submit_run(config_path, lane=lane, idempotency_key=idempotency_key, params=params)


def get_run(run_id: str, *, store: RuntimeStore) -> dict[str, Any] | None:
    return store.get_run(run_id)


def cancel_run(run_id: str, *, store: RuntimeStore) -> str:
    return store.cancel_run(run_id)
