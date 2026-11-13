# Déploiement de la voie ELT

* `compose.yml` : PostgreSQL d'état/publication (`mdm2-elt-pg`) et, avec le profil `run`, un
  conteneur d'exécution de la CLI (`mdm2-elt-run`) ; chaque conteneur est plafonné à 6 Go
  (mémoire et swap) et 512 Mo de `shm`.
* `Dockerfile` / `requirements.txt` : image Python 3.12 avec dbt-core, dbt-duckdb, DuckDB et
  l'extension `postgres` préinstallée (**non construite dans cette session** : seule la syntaxe du
  compose a été validée avec `docker compose config`).

```powershell
$env:MDM_ELT_DB_PASSWORD = '…'
docker compose -f deploy/lanes/elt/compose.yml up -d postgres
$env:MDM_STATE_DSN = "postgresql://mdm:$env:MDM_ELT_DB_PASSWORD@127.0.0.1:55460/mdm"
python -m engine.lanes.elt init --config configs/example/customer.yml
```

Ressources : DuckDB `memory_limit` ≤ 6 Go (`MDM_ELT_MEMORY_LIMIT`) et `threads` ≤ 8
(`MDM_ELT_THREADS`) sont explicites ; voir `engine/lanes/elt/README.md` pour le détail, les
mesures et les écarts.
