# DQ Engine : MDM maison piloté par configuration

Moteur de réconciliation de référentiels (clients, produits, etc.) en mode **delta** :
chaque source est fusionnée par `row_hash`, seuls les deltas valides déclenchent la
réconciliation des stagings touchés, puis la survie et la publication du golden.
Tout est décrit dans un fichier YAML par référentiel.

Trois voies d'exécution du même contrat, pour comparer ETL, ELT et graphe :

| Voie | Code | Calcul | État |
|---|---|---|---|
| ETL | `engine/lanes/etl/` | Python (Arrow, rapidfuzz), PostgreSQL en stockage | [README](engine/lanes/etl/README.md) |
| ELT | `engine/lanes/elt/` | DuckDB piloté par dbt (modèles générés depuis la config) | [README](engine/lanes/elt/README.md) |
| Graphe | `engine/lanes/graph/` | PostgreSQL + Apache AGE, charge utile relationnelle | [README](engine/lanes/graph/README.md) |

## Documents

- [Note finale HTML](docs/MDM_NOTE_FINALE.html) : architectures, résultats, gaps et recommandation,
  autonome et imprimable en PDF.
- [Contrat du moteur](docs/MDM_CONTRACT.md) : référence unique (delta, règles, tables, CLI).
- [Guide de l'ingénieur data](docs/ENGINEER_GUIDE.md) : brancher une nouvelle source en YAML.
- [Résultats de mesure](docs/bench/RESULTS.md) et [étude](docs/MDM_STUDY.md) : débits mesurés, écarts.
- [Spécification du moteur de pipelines](docs/SPECIFICATIONS.md) : exigences d'exploitation (file, workers,
  Airflow, Airbyte) dont le runtime ne couvre qu'un sous-ensemble ([README](engine/runtime/README.md)).
- Anciennes recherches du laboratoire : `docs/archive/`.

## Résultat de la recette

ETL a exécuté le jeu **1 M de nœuds synthétiques × 200 attributs cibles**
(environ 1,9 M de lignes source) : 19 min pour l'initial, puis 4 min 16 s
pour le cycle à 5 % de changements. Pic client + PostgreSQL : 8,1 Go.
ELT a échoué à 300 k × 200 : les résultats négatifs sont conservés dans
l'[étude](docs/MDM_STUDY.md), sans extrapolation de capacité.

La [pile Docker](deploy/README.md) est plafonnée à **17 Go** au total.
Elle a passé un run complet via le worker jusqu'aux exports canoniques.
Les tests du package installé passent dans un conteneur de 4 Go
(741 tests, 111 tests de bases externes ignorés) ; le runtime a aussi
passé 75 tests sur PostgreSQL réel. Les essais lourds ont été séquentiels.

**Cela ne certifie pas encore 50 référentiels de production par jour.**
Le stockage durable, les migrations, les connecteurs réels et les autres
gaps sont explicitement recensés dans l'étude.

## Organisation

```text
configs/        exemples de référentiels (YAML)
engine/core/    config Pydantic, schémas canoniques, comparateur, oracle de référence, générateur synthétique
engine/cli/     validate, explain, schema, add-source, check-source, new-referential
engine/lanes/   etl | elt | graph : init, ingest, reconcile, export, reset
engine/runtime/ file persistante PostgreSQL, workers asyncio, admission, reprise
engine/bench/   banc de mesure (Docker, mémoire bornée)
deploy/lanes/   compose par voie
tests/          core, cli, lanes, runtime, bench
```

## Installation (Windows / PowerShell, Python 3.12)

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -e ".[dev,elt]"
.\.venv\Scripts\python.exe -m engine.cli validate configs\example\customer.yml
.\.venv\Scripts\python.exe -m pytest tests -q
$env:MDM_TEST_DOCKER = "1"   # active les tests PostgreSQL/AGE (conteneurs bornés en mémoire)
```

Les tests qui lancent Docker démarrent des conteneurs `mdm2-*` limités en mémoire et les
suppriment à la fin.
