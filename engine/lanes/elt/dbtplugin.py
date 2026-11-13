"""Plugin dbt-duckdb : fonctions utilisateur exigées par la sémantique du contrat.

`levenshtein` de DuckDB compte des **octets** UTF-8 (`levenshtein('é', 'e') = 2`) alors que le
contrat impose des points de code. Le SQL généré n'appelle `mdm_lev_cp` (rapidfuzz, C++, exact)
que si l'une des deux chaînes n'est pas ASCII : le cas courant reste en SQL pur.
"""

from __future__ import annotations

from typing import Any

import duckdb
import pyarrow as pa
from dbt.adapters.duckdb.plugins import BasePlugin

LEVENSHTEIN_UDF = "mdm_lev_cp"


def _levenshtein_codepoints(left: pa.Array, right: pa.Array) -> pa.Array:
    from rapidfuzz.distance import Levenshtein

    distance = Levenshtein.distance
    values = [
        None if a is None or b is None else distance(a, b)
        for a, b in zip(left.to_pylist(), right.to_pylist(), strict=True)
    ]
    return pa.array(values, type=pa.int64())


def register_udfs(conn: duckdb.DuckDBPyConnection) -> None:
    try:
        conn.create_function(
            LEVENSHTEIN_UDF,
            _levenshtein_codepoints,
            ["VARCHAR", "VARCHAR"],
            "BIGINT",
            type="arrow",
        )
    except duckdb.Error as error:  # déjà enregistrée sur cette connexion
        if "already" not in str(error).lower():
            raise


class Plugin(BasePlugin):
    def configure_connection(self, conn: Any) -> None:
        register_udfs(conn)

    def configure_cursor(self, cursor: Any) -> None:
        register_udfs(cursor)
