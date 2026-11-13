# Déploiement de la voie ETL

`compose.yml` : PostgreSQL d'état (`mdm2-etl-pg`, 6 Go, pas de swap, `shm_size 512m`) et un
conteneur d'exécution à la demande (`mdm2-etl-run`, profil `run`). Les images ne sont pas
publiées : `docker compose --profile run build etl` construit `mdm2-etl-runner` (Python 3.12 +
`engine/`, utilisateur non root). Rien n'est lancé automatiquement.

```powershell
$env:MDM_ETL_PG_PASSWORD = "..."            # obligatoire, jamais dans le dépôt
docker compose -f deploy/lanes/etl/compose.yml up -d pg
docker compose -f deploy/lanes/etl/compose.yml --profile run run --rm etl init   --config /config/customer.yml
docker compose -f deploy/lanes/etl/compose.yml --profile run run --rm etl ingest --config /config/customer.yml --source crm --run-id 2026-11-13
docker compose -f deploy/lanes/etl/compose.yml --profile run run --rm etl reconcile --config /config/customer.yml --run-id 2026-11-13
docker compose -f deploy/lanes/etl/compose.yml --profile run run --rm etl export --config /config/customer.yml --out /out
docker compose -f deploy/lanes/etl/compose.yml down            # le volume mdm2-etl-pgdata est conservé
```

Notes d'exploitation

* Le YAML référence les DSN par nom de variable (`MDM_STATE_DSN`, `CRM_DSN`, `ERP_DSN` fournis par
  le compose pour l'exemple `configs/example/customer.yml`) ; adapter `environment:` aux
  `dsn_env` de la config. Les sources Parquet sont lues sous `/data` (monter le même chemin que
  `sources[].location`).
* Un référentiel = un schéma d'état (`state.schema`) : plusieurs référentiels peuvent partager
  une base ; un `ingest` par source peut tourner en parallèle, un seul `reconcile` à la fois par
  référentiel (verrou advisory), les fusions d'ingest attendent `MDM_ETL_LOCK_TIMEOUT_S`.
* Réglages PostgreSQL du compose : `shared_buffers=1GB`, `max_wal_size=8GB`,
  `synchronous_commit=off` (acceptable : l'état est reconstructible par rejeu des sources ; à
  revoir pour une production). Le chargement initial d'un gros référentiel fait grossir le WAL
  (reconcile en une transaction).
* Sauvegarde : `pg_dump -n <schéma>` ; remise à zéro : commande `reset`.
* Tests : `MDM_TEST_DOCKER=1 pytest tests/lanes/etl` démarre son propre conteneur
  `mdm2-etl-pg-<hex>` (mêmes limites) et le supprime.
