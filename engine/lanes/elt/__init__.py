"""Voie B « ELT » : brut chargé tel quel, transformations en SQL (DuckDB) pilotées par dbt Core.

Le projet dbt est généré depuis la config YAML (voir README.md) ; PostgreSQL porte l'état et la
publication. CLI : `python -m engine.lanes.elt {init,ingest,reconcile,export,reset}`.
"""
