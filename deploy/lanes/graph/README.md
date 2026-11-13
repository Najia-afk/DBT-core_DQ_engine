# Voie graphe : déploiement local (PostgreSQL 16 + Apache AGE 1.6.0)

Image : `apache/age:release_PG16_1.6.0` (déjà présente localement, jamais tirée par les outils).
Le moteur (`python -m engine.lanes.graph`) est un client Python ; seul le serveur tourne en conteneur.

## Serveur de dev

```powershell
$env:MDM_GRAPH_PASSWORD = '<mot de passe local>'
docker compose -f deploy\lanes\graph\compose.yml up -d
# DSN d'état (secret : variable d'environnement, jamais dans la config)
$env:MDM_STATE_DSN = "host=127.0.0.1 port=55432 dbname=mdm_state user=mdm password=$env:MDM_GRAPH_PASSWORD"
python -m engine.lanes.graph init --config configs\example\customer.yml
```

Plafonds de ressources : `--memory 6g --memory-swap 6g --shm-size 512m` (compose : `mem_limit`,
`memswap_limit`, `shm_size`). Les conteneurs de test/smoke/bench sont nommés `mdm2-graph-*`, publiés
sur `127.0.0.1` avec un port aléatoire, et supprimés (`docker rm -f -v`) en fin d'exécution.

## Ce que l'image impose

* L'extension `age` doit être chargée **par session** (`LOAD 'age'`) et `ag_catalog` doit être dans le
  `search_path` ; le moteur le fait à chaque connexion. `CREATE EXTENSION age` exige un super-utilisateur
  (l'utilisateur `POSTGRES_USER` de l'image l'est) : `init` le fait avec `IF NOT EXISTS`.
* Le rôle utilisé par `MDM_STATE_DSN` doit pouvoir créer un schéma, des tables, un graphe AGE
  (`ag_catalog.create_graph`) et des index.
* Un graphe AGE = un schéma PostgreSQL `<state.schema>_g` ; les tables relationnelles du moteur sont dans
  `<state.schema>`. `reset` supprime les deux.

## Variables d'environnement

| Variable | Rôle |
|---|---|
| `state.dsn_env` de la config (ex. `MDM_STATE_DSN`) | DSN libpq de la base d'état (AGE) |
| `source.dsn_env` de chaque source postgres | DSN de lecture de la source |
| `MDM_GRAPH_FAIL_AT` | injection de panne (tests) : `reconcile_after_index`, `..._after_pairs`, `..._after_decisions`, `..._after_recluster`, `..._after_survive`, `reconcile_before_commit`, `ingest_before_commit` ; le processus s'arrête net (code 137) |
| `MDM_TEST_DOCKER=1` | active les tests Docker (`tests/lanes/graph`) |

## Tests, smoke, bench

```powershell
$env:MDM_TEST_DOCKER = '1'
python -m pytest tests\lanes\graph -q                 # démarre un conteneur mdm2-graph-<hex>
python -m engine.lanes.graph.smoke --nodes 27500 --attributes 30     # ~50 000 nœuds source
python -m engine.lanes.graph.bench --n 20000 --attrs 30              # COPY vs Cypher CREATE/MERGE
```

Sans `MDM_TEST_DOCKER`, les tests Docker sont ignorés (les tests purs `test_graph_values.py` et
`test_graph_units.py` tournent toujours).
