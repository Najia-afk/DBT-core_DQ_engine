# Voie A « ETL classique » (`engine.lanes.etl`)

La transformation est faite **hors base** par le moteur Python (extraction → transformation
vectorisée Arrow/numpy → chargement) ; PostgreSQL ne sert qu'à l'état et à la publication.
Référence : `docs/MDM_CONTRACT.md` (§4 tables, §6 CLI). Oracle de sémantique :
`engine/core/reference` (comparaison : `engine.core.compare`).

## Utilisation

```text
python -m engine.lanes.etl init      --config C
python -m engine.lanes.etl ingest    --config C --source S --run-id R
python -m engine.lanes.etl reconcile --config C --run-id R
python -m engine.lanes.etl export    --config C --out DIR      # <table>.parquet, un snapshot cohérent
python -m engine.lanes.etl reset     --config C                # DROP SCHEMA ... CASCADE
```

* Une ligne JSON sur stdout : `lane, command, referential, status, run_id, source, counts, seconds
  (durées par phase), total_seconds, peak_rss_mb` (+ `error` si échec). Code de sortie 0 = succès,
  1 = échec (message sur stderr), 2 = config illisible. Un lot `full` bloqué par le contrôle de
  complétude n'est pas une erreur : exit 0, `counts.blocked=true`, anomalie `batch_blocked`,
  aucune écriture d'état.
* Secrets : DSN lus dans les variables d'environnement nommées par la config
  (`state.dsn_env`, `sources[].dsn_env`). Jamais dans le YAML.
* Idempotence : `(run_id, étape)` est tracé dans `run_log`. Rejouer une étape déjà `ok` ne fait
  rien (`counts.replayed=true`). Rejouer les mêmes données avec un autre `run_id` ne produit
  aucun delta (comparaison de hash). Une étape morte (kill) reste `running` et se rejoue.
* Réglages d'environnement : `MDM_ETL_LOCK_TIMEOUT_S` (60), `MDM_ETL_RETRY_BASE_S` (1.0),
  `MDM_ETL_WORKERS` (threads rapidfuzz, défaut min(8, CPU)). `MDM_ETL_FAULT=<point>` est un
  crochet de **test** qui tue le process (`ingest:after_batch`, `ingest:before_apply`,
  `reconcile:after_blocks|after_scoring|after_pairs|before_commit`).
* Délais explicites sur chaque connexion : `lock_timeout`, `statement_timeout` et
  `idle_in_transaction_session_timeout` (= `runtime.step_timeout_minutes`), `TimeZone=UTC`.
  Erreurs transitoires (connexion perdue, deadlock, sérialisation, lock timeout) : nouvelle
  tentative avec attente exponentielle, `runtime.max_attempts` essais par lot / par transaction ;
  au-delà, échec explicite. Un `statement_timeout` n'est jamais rejoué.

## Conception

| Module | Rôle |
|---|---|
| `transform.py` | normalisation, cast, contrôles DQ, hash de ligne : **vectorisé** (pyarrow.compute/numpy) |
| `sources.py` | lecture en flux : PostgreSQL via `COPY … TO STDOUT` (CSV) lu par Arrow, Parquet via `pyarrow.dataset` ; lots de `runtime.batch_rows` |
| `arrowio.py` | passerelle Arrow ↔ PostgreSQL par COPY CSV (bulk, sans boucle par ligne) |
| `ingest.py` | stage UNLOGGED par lot, puis MERGE SQL par hash en une transaction |
| `matching.py` | clés de blocage, scoring (`rapidfuzz.process.cpdist` + Arrow) |
| `cluster.py` | composantes connexes (numpy, hooking + pointer jumping) et stabilité des ids de staging |
| `survive.py` | survie par attribut, vectorisée (une clé entière par candidat, `minimum.reduceat` par staging) |
| `reconcile.py` | orchestration de l'étape 2+3 dans une transaction |
| `export.py`, `schema.py`, `db.py`, `cli.py` | projections canoniques, migrations, connexions/retries/verrous, CLI |

### Ingest (par source, parallélisable entre sources)

1. Flux source → lots. Pour chaque lot : clé `str()`, révision → `timestamp(us)`, cast typé,
   normalisation, contrôles (`reject_record` / `null_value` / `warn`), hash `sha256` de
   `row_hash_input` (voir `engine.core.canonical`). Mémoire bornée par la taille d'un lot, pas par
   celle de la source.
2. Par lot, **une transaction** : la métadonnée étroite de *toutes* les lignes (clé, révision, type
   de ligne, hash) va dans `stg_<source>_meta` (UNLOGGED) ; seules les lignes dont le hash diffère de
   `source_record` chargent leurs valeurs dans la table large `stg_<source>_val` (une colonne par
   attribut mappé) — un snapshot inchangé n'écrit presque rien. Les rejets vont dans
   `stg_<source>_rej`. Échec transitoire ⇒ le lot est rejoué (la transaction du lot a été annulée).
3. Après le dernier lot, **une transaction** (verrou advisory partagé) : dédoublonnage
   (révision max puis dernière ligne ; le doublon perdant est un rejet `duplicate_key`),
   contrôle de complétude (`full_snapshot_min_ratio`) puis `INSERT`/`UPDATE` de `source_record`
   par comparaison de hash, `source_delta` (I/U/D, une clé retirée qui revient = `I`),
   retrait logique des clés absentes d'un snapshot `full`, `reject`, `pending_node`, `run_log`.
   Tout est annulé en bloc en cas d'erreur.

### Reconcile (un seul à la fois par référentiel, uniquement sur le delta)

1. `pending_node` = nœuds des ingests depuis le dernier reconcile (plusieurs ingests possibles).
2. Index de blocage `block_index(node_id, bname, bvalue)` mis à jour pour le delta ; blocs
   `oversized` détectés par comptage avant/après (anomalie `oversized_block`, jamais tronqués).
3. Paires touchant le jeu à re-scorer supprimées ; candidats générés **en SQL** par jointure sur
   l'index `(bname, bvalue, node_id)` (blocs ≤ `max_block_size`) ; valeurs des attributs de
   matching lues par COPY et scorées **dans le moteur** (Levenshtein rapidfuzz, exact, année).
4. Décisions (match/review, comptage d'éligibles par source voisine) en numpy sur les paires
   adjacentes aux nœuds modifiés ; seules les paires nouvelles ou dont la décision change sont
   écrites. Les paires de décision `none` ne sont pas stockées (voir écarts).
5. Ensemble affecté A = delta + extrémités des paires dont le statut `match` a changé + membres de
   leurs stagings. Composantes connexes de A uniquement, ids stables (le plus ancien est conservé,
   fusion = `merge`, scission = `split`, sinon `create`), écriture de la différence seulement
   (`staging_member`, `staging`, `staging_event`).
6. Survie vectorisée par lots de stagings, comparaison avec le golden publié, `golden_delta`,
   upsert des seuls goldens modifiés, `blocked`/`stale`/`retired`, anomalies, `run_log`.

Compteurs de preuve « delta seulement » (dans `counts`) : `delta_nodes`, `affected_nodes`,
`stagings_total` / `stagings_read` / `stagings_kept`, `member_rows_read|written`,
`record_rows_read_narrow|wide|match_attrs`, `golden_rows_read|written`, `pairs_read|inserted|
updated|removed`, `candidate_pairs`, `merges|splits|creates`, `golden_changes`, `golden_blocked`.
Les tests vérifient en plus, par `xmin`, qu'aucune ligne non concernée n'a été réécrite.

### Publication atomique

Tout reconcile s'exécute dans **une seule transaction PostgreSQL** (`pg_advisory_xact_lock`
exclusif) qui écrit `match_pair`, `block_index`, `staging*`, `golden`, `golden_delta`, `anomaly`,
vide `pending_node` et passe `run_log` à `ok`. Les lecteurs voient l'ancien état jusqu'au `COMMIT`
puis le nouveau (MVCC) ; un crash ou une erreur annule tout et `pending_node` reste intact : le
rejeu converge. Pour lire plusieurs tables de façon cohérente, utiliser une transaction
`REPEATABLE READ` (c'est ce que fait `export`). Les ingests, eux, committent `source_record`
avant le reconcile : `source_record`/`source_delta` peuvent donc être « en avance » sur `golden`
(c'est l'état « ingéré, non publié »). Les lecteurs de la publication lisent `golden`,
`golden_delta` (par `run_id`) et `staging_member`.

## Tables d'état (schéma `state.schema`)

`schema_version` · `source_record` (identité `node_id`, `source_id`, `source_key`, `row_hash`,
`revision`, `status`, `first_run`, `last_change_run` + **une colonne typée par attribut**) ·
`source_delta` · `pending_node` · `match_pair` (par `node_id`, `eligible`, `decision`, `reason`) ·
`block_index` · `staging`, `staging_member`, `staging_event` · `golden` (une colonne par attribut,
`<attr>__src` si `publication.provenance`, `published_once`) · `golden_delta` · `reject` · `anomaly` ·
`run_log` · vues `match_pair_keyed` et `review` (ordre canonique des paires). Tables de stage
UNLOGGED `stg_<source>_*` : créées au début d'un ingest, supprimées à la fin.
`init` est idempotent et versionné (`schema_version`), ajoute les colonnes des attributs
introduits plus tard ; changer le type d'un attribut impose un `reset`.

## Tests

`tests/lanes/etl` (33 tests). Les tests PostgreSQL exigent `MDM_TEST_DOCKER=1` (conteneur
`mdm2-etl-pg-<hex>`, `--memory 6g --memory-swap 6g --shm-size 512m`, supprimé en fin de session) ;
sinon ils sont ignorés (7 tests sans base restent actifs). `MDM_ETL_TEST_ADMIN_DSN` réutilise un
serveur existant. Données ≤ 4 000 nœuds.

* `test_etl_transform` : transformation vectorisée = règles du moteur de référence, valeur par
  valeur (aléatoire, accents, contrôles, casts sales).
* `test_etl_equivalence` : lane = `ReferenceEngine` (`compare_exports`, événements inclus) sur
  J0 → J1 → J2 : 6 configs générées (3 sources/12 attributs, 50 attributs, blocs minuscules,
  tout-Parquet sans provenance, barrière de complétude, 4 000 nœuds en plusieurs lots).
* `test_etl_example` : `configs/example/customer.yml` adaptée (PG + timestamptz + numeric + Parquet,
  accents, doublons de clé inter-lots, clés nulles, `most_recent`).
* `test_etl_scenarios` : scénario écrit à la main (scission, fusion, réactivation, golden
  `blocked`/`stale`, révision illisible, revue, rejets).
* `test_etl_lane` : incrémental = recalcul à froid de la lane, rejeu idempotent, aucun delta sur
  rejeu, travail borné au delta (compteurs + `xmin`), kill pendant reconcile/ingest puis rejeu,
  ingests parallèles, échecs signalés, retry de lot sur perte de connexion, reset.
* `test_etl_retry` : classification et bornage des retries, timeouts.

## Mesures : smoke 100 000 nœuds × 50 attributs

Données `engine.core.synth` (seed 11, 3 sources : crm/erp PostgreSQL `full`, web Parquet `delta` ;
J0 = 189 891 lignes source, J1 = delta par défaut). PostgreSQL 16.4 dans Docker (6 Go, `fsync=off`),
moteur sur l'hôte Windows (32 threads), **machine partagée avec d'autres jobs** : valeurs
indicatives, une exécution, un process par commande (`peak_rss_mb` = pic du process).

| Étape | Volume | Durée | Débit | Pic RSS | Phases (s) |
|---|---|---|---|---|---|
| ingest J0 crm | 63 566 lignes | 4,49 s | 14,2 k lignes/s | 256 Mo | extract 0,51 · transform 1,21 · stage 0,91 · merge 1,65 |
| ingest J0 erp | 63 169 lignes | 4,89 s | 12,9 k/s | 274 Mo | 0,55 · 1,27 · 0,92 · 1,95 |
| ingest J0 web | 63 156 lignes | 3,94 s | 16,0 k/s | 300 Mo | 0,03 · 1,18 · 0,88 · 1,64 |
| reconcile J0 | 189 407 nœuds → 112 152 stagings | 31,8 s | 6,0 k nœuds/s | 401 Mo | blocs 5,9 · score 6,1 · décision 1,2 · cluster 1,7 · écriture staging 1,3 · survie 14,3 |
| ingest J1 crm | 63 884 lignes lues, 2 850 changées | 3,11 s | 20,5 k/s | 250 Mo | 0,55 · 1,25 · 0,42 · 0,69 |
| ingest J1 web | 2 653 lignes | 0,49 s | 5,4 k/s | 103 Mo | |
| reconcile J1 | 8 204 nœuds du delta (affectés 15 774) | 3,86 s | 2,1 k nœuds/s | 147 Mo | blocs 1,0 · score 0,9 · survie 0,8 |
| export | 4,72 M lignes (golden_delta 3,63 M) | 16,5 s | | 253 Mo | golden_delta 10,0 |

Delta J1 : `stagings_read` = 6 982 sur 112 152 stagings (6,2 %), 7 605 goldens écrits. Espace
disque du schéma d'état après J1 : 582 Mo (`golden_delta` 266, `source_record` 111, `block_index`
66, `golden` 59). 36 336 goldens `blocked` à J0 : attributs non nullables génériques (`code_025`,
`qty_020`, `qty_023`, `ts_045`) sans valeur dans les sources du staging, conforme au contrat.

Ces chiffres ne sont **pas** extrapolés : le benchmark 1 M × 200 reste à mesurer. Ordres de
grandeur à surveiller : la survie et l'écriture de `golden_delta` croissent avec
nœuds × attributs ; l'ingest est borné par la transformation Python/Arrow (≈ 50 k lignes/s pour 50
attributs, plus lent à 200) et par le hash sha256 (boucle Python sur buffers Arrow).

## Écarts et limites connus (honnêtes)

* `match_pair` ne stocke que les paires de décision `match` ou `review` (les `none` ne sont pas
  persistées) : `compare_exports` par défaut ignore les `none` ; avec `include_none_pairs=True` la
  lane diffère de la référence.
* Reconcile = **une** transaction : sur un chargement initial 1 M × 200 elle sera longue (WAL,
  verrou exclusif ⇒ les fusions d'ingest attendent `lock_timeout` puis réessaient). Mémoire du
  reconcile : O(|affecté| × attributs de matching) + survie par lots ; non mesurée au-delà de
  100 k nœuds.
* Une seule exécution Python par commande : pas de parallélisme intra-source (seuls Arrow et
  rapidfuzz sont multi-threads), pas de lecture PostgreSQL partitionnée, pas de multi-process.
* Reprise : un ingest interrompu repart de zéro (relecture de la source) ; pas de reprise
  intermédiaire ni multi-hôtes ; pas de HA. Le verrou de session d'un ingest se perd si la
  connexion est recréée après une erreur transitoire.
* Sémantique Unicode : exacte, car toute chaîne non ASCII imprimable (accents, contrôles) passe
  par les méthodes Python de la référence ; seuls les chemins ASCII utilisent les noyaux Arrow/RE2.
  Un motif `regex` non compris par RE2 (retour arrière, assertions) bascule en Python.
  Colonnes source `float` : chemin lent par valeur (`str(float)`). Un `numeric` PostgreSQL lu vers
  un attribut `string` s'écrit en décimal positionnel (la référence, sur un `Decimal` de scale > 6,
  produirait `1E-7`).
* Ordre de lecture d'une source PostgreSQL non spécifié : en cas de doublon de clé de même
  révision, « dernière ligne » dépend de l'ordre de scan.
* Caractère NUL dans une chaîne : refusé par PostgreSQL, l'ingest échoue (pas de rejet DQ).
* Types d'attributs et noms : les noms d'attributs ne peuvent pas égaler une colonne physique
  (`node_id`, `source_id`, `source_key`, `row_hash`, `revision`, `first_run`,
  `last_change_run`, `published_once`) ni dépasser 58 caractères avec provenance : `init` refuse
  avec un message clair.
* Pas de `ANALYZE` automatique des tables d'état au-delà de l'autovacuum ; les tables temporaires du
  reconcile sont analysées. Le crochet `MDM_ETL_FAULT` reste dans le code de production
  (inactif sans la variable).
