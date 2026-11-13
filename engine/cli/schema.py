"""Export du JSON Schema de ReferentialConfig, enrichi de descriptions pour l'éditeur."""

from __future__ import annotations

import json

from engine.cli.normalizers import DESCRIPTIONS_FR
from engine.core.config import ReferentialConfig

DESCRIPTIONS: dict[str, dict[str, str]] = {
    "ReferentialConfig": {
        "referential": "Identifiant du référentiel (minuscules, chiffres, _).",
        "version": "Version de la config (entier ≥ 1).",
        "state": "Base d'état/publication du référentiel.",
        "sources": "Sources de données. L'ordre ici n'a pas d'effet sur la survie.",
        "attributes": "Attributs du référentiel, avec la liste ORDONNÉE des colonnes sources.",
        "matching": "Règles de rapprochement (blocage, comparateurs, seuils).",
        "publication": "Options de publication du golden.",
        "runtime": "Paramètres d'exécution.",
    },
    "State": {
        "dsn_env": "Nom de la variable d'environnement contenant le DSN PostgreSQL d'état.",
        "schema": "Schéma PostgreSQL qui reçoit les tables du référentiel.",
    },
    "Source": {
        "id": "Identifiant de la source (référencé par attributes[].sources[].source).",
        "kind": "postgres (table/vue) ou parquet (fichier ou glob).",
        "location": "postgres : schema.table ; parquet : chemin ou glob (/data/x/*.parquet).",
        "dsn_env": "postgres uniquement : variable d'environnement contenant le DSN.",
        "key": "Colonne source qui identifie un enregistrement de façon unique.",
        "revision": "Colonne ordonnant les versions d'une clé (date, horodatage, entier).",
        "snapshot": "full : le lot contient tout (clés absentes = retirées) ; delta : changements.",
        "full_snapshot_min_ratio": "snapshot full : part minimale de clés attendues, sinon bloqué.",
    },
    "SourceColumn": {
        "source": "Identifiant d'une source déclarée dans sources.",
        "column": "Nom exact de la colonne dans cette source.",
    },
    "Attribute": {
        "name": "Nom de l'attribut cible (minuscules, chiffres, _ ; noms réservés interdits).",
        "type": "Type cible : string, int, decimal, date, timestamp, bool.",
        "nullable": "false : si toutes les sources sont nulles, le golden est bloqué.",
        "normalize": "Normaliseurs appliqués dans l'ordre (string uniquement). "
        + "; ".join(f"{k} = {v}" for k, v in DESCRIPTIONS_FR.items()),
        "sources": "Colonnes sources PAR ORDRE DE PRIORITÉ : la première non nulle et valide "
        "gagne.",
        "strategy": "priority : ordre de la liste ; most_recent : révision la plus récente.",
        "checks": "Contrôles de qualité appliqués à l'ingestion.",
    },
    "Check": {
        "rule": "regex | in_set | min | max | max_length.",
        "value": "regex : motif ; in_set : liste ; min/max/max_length : nombre.",
        "on_fail": "reject_record : nœud rejeté ; null_value : valeur nulle ; warn : tracé.",
    },
    "BlockPart": {
        "attr": "Attribut utilisé.",
        "transform": "exact | prefix (avec n) | year (date).",
        "n": "prefix uniquement : nombre de caractères conservés.",
    },
    "BlockingKey": {
        "name": "Nom de la clé de blocage.",
        "parts": "Composantes concaténées de la clé.",
    },
    "ComparatorRule": {
        "attr": "Attribut comparé.",
        "method": "exact | levenshtein (string) | date_year (date, timestamp).",
        "weight": "Poids dans le score (la somme des poids doit valoir 1).",
    },
    "Contradiction": {"attr": "Deux valeurs non nulles différentes : jamais de fusion auto."},
    "Matching": {
        "version": "Étiquette de version des règles.",
        "blocking": "Clés de blocage (union des candidats).",
        "max_block_size": "Un bloc plus grand n'est pas expansé (anomalie oversized_block).",
        "comparators": "Comparateurs pondérés.",
        "contradictions": "Attributs dont une divergence empêche la fusion automatique.",
        "accept": "Score ≥ accept : fusion automatique (un seul candidat, sans contradiction).",
        "review": "Score entre review et accept : revue manuelle.",
        "min_evidence": "Poids actif minimal pour qu'une décision soit prise.",
    },
    "Publication": {"provenance": "Ajoute une colonne <attr>__src (source gagnante) par attribut."},
    "Runtime": {
        "profile": "small | medium | large.",
        "batch_rows": "Taille de lot (≥ 1000).",
        "max_attempts": "Tentatives par étape (1-10).",
        "step_timeout_minutes": "Délai maximal par étape.",
    },
}


def build_schema() -> dict:
    schema = ReferentialConfig.model_json_schema()
    for name, props in DESCRIPTIONS.items():
        node = schema if name == "ReferentialConfig" else schema["$defs"].get(name)
        if not node:
            continue
        for prop, text in props.items():
            if prop in node.get("properties", {}):
                node["properties"][prop].setdefault("description", text)
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "title": "MDM referential config",
        **{k: v for k, v in schema.items() if k != "title"},
    }


def schema_text() -> str:
    return json.dumps(build_schema(), indent=2, ensure_ascii=False) + "\n"
