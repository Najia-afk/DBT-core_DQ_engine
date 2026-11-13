# engine.bench : banc de performance et d'équivalence des trois voies

Ticket c1-bench. Mesure ETL / ELT / graphe sur la même machine, avec les mêmes jeux
(`engine.core.synth`) et les mêmes garde-fous. Résultats : `docs/bench/RESULTS.md`
(tableaux générés + projection), `docs/bench/FINDINGS.md` (bugs et limites des voies),
JSON bruts dans `docs/bench/runs/`, `equivalence/`, `concurrency/`.

## Commandes

```text
python -m engine.bench ladder --lane etl --step L1 [--export final]   # étape nommée (steps.py)
python -m engine.bench run --lane elt --nodes 300000 --attributes 200 --update-pcts 1,5
python -m engine.bench equiv --mode reference --lanes etl,elt,graph   # <= 10 k nœuds, 3 jours
python -m engine.bench equiv --mode lanes --lanes etl,elt --step L1   # voie contre voie
python -m engine.bench concurrency --lane etl                         # 3 référentiels, engine.runtime
python -m engine.bench report                                         # régénère RESULTS.md
python -m engine.bench compare --a DIR --b DIR [--hash]               # deux exports Parquet
```

Étapes : `L1` 100 k × 50 (3 jours delta à 1/5/20 %), `L2` 300 k × 200 (1/5 %), `L3` 1 M × 200
(1/5 %), `G1..G3` échelle du graphe, `C50` 50 k × 30 (comparaison voie à voie du graphe),
`S2K` 2 k × 20 (intégration). Chaque jour delta ajoute 0,5 % de nouveaux, 0,2 % de retraits
(sources `full`), 0,1 % de lignes cassées ; 5 % des mises à jour fusionnent et 5 % scindent
(valeurs par défaut du générateur).

## Ce qui est mesuré (par phase : init, ingest, reconcile, export)

* mur (s), lignes (lignes source du jour pour ingest ; nœuds du delta pour reconcile), débit ;
* pic RSS de **l'arbre de processus** lancé (psutil, échantillon 0,5 s ; somme des working
  sets Windows, donc borne haute) et décomposition par processus au pic ;
* pic mémoire et CPU du conteneur PostgreSQL (`docker stats`, cadence ≈ 2-3 s) ;
* taille de la base (`pg_database_size`), du `PGDATA` (WAL inclus, `du` dans le conteneur,
  pic échantillonné toutes les 5 s), WAL écrit pendant la phase (différence de LSN),
  répertoire de travail (ELT) ;
* mémoire utilisée de l'hôte (`psutil.virtual_memory().used`, base avant run publiée) : elle
  inclut la VM Docker et son cache, donc elle dépasse la somme « RSS client + conteneur ».

## Garde-fous (arrêt du run, raison enregistrée)

| règle | seuil par défaut |
|---|---|
| disque libre du volume de travail | < 25 Go |
| durée d'une phase (« étape ») | > 90 min |
| RAM totale (arbre client + conteneur PG) | > 27 Go (contrainte 28 Go, 1 Go de marge pour le retard d'échantillonnage) |

Le conteneur est `mdm2-bench-<voie>[-tag]` (jamais un autre nom : refus explicite), mémoire et
mémoire+swap 12 Go, `shm` 1 Go, `shared_buffers=3GB`, `work_mem=64MB`,
`max_wal_size=16GB`, **`synchronous_commit=off`** (`fsync` actif). Les données du run (volume,
répertoire de travail, jeu de données) sont supprimées en fin de run, y compris en cas d'échec.
Sources : **Parquet** pour les échelles (jeu rejoué par jour dans `live/`, une source `delta`
ne reçoit que les lignes du jour) ; `--kinds postgres,...` charge les tables sources avec
`engine.core.synth load-pg` (non utilisé dans les mesures publiées).

## Modules

`sampler.py` (échantillonneurs, séries) · `guards.py` (règles et moniteur de phase) ·
`pg.py` (conteneur) · `dataset.py` (génération, staging `live/`) · `procs.py` (commandes) ·
`scenario.py` (run d'une voie → JSON) · `equiv.py` (référence et voie à voie ; `compare`
borne la mémoire DuckDB de `compare_exports`) · `concurrency.py` (runtime) · `project.py`
(projection) · `report.py` (RESULTS.md entre marqueurs `BENCH:GENERATED`).

Tests : `pytest tests/bench` (sans Docker) ; `MDM_TEST_DOCKER=1` ajoute un run ETL de 2 000
nœuds de bout en bout.
