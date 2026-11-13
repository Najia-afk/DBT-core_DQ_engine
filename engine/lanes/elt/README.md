# Voie B « ELT » : brut tel quel, transformations SQL (DuckDB) pilotées par dbt Core

Voie `engine/lanes/elt` du contrat `docs/MDM_CONTRACT.md` (§6). Le brut des sources est copié tel
quel dans des fichiers Parquet, **toute** la logique MDM (normalisation, contrôles DQ, delta,
blocage, scoring, clusters, survie) s'exécute en SQL dans DuckDB, via des modèles dbt **générés
depuis la config YAML** (l'ingénieur n'écrit ni SQL ni Python), et l'état/la publication sont dans
PostgreSQL.

## Utilisation

```text
python -m engine.lanes.elt init      --config C
python -m engine.lanes.elt ingest    --config C --source S [--source S2 | S,S2] --run-id R
python -m engine.lanes.elt reconcile --config C --run-id R
python -m engine.lanes.elt export    --config C --out DIR
python -m engine.lanes.elt reset     --config C
```

* Une seule ligne JSON sur stdout (`status`, `counters`, `seconds` par phase) ; code de sortie 1 et
  `{"status": "error", "error": ...}` en cas d'échec. Les journaux dbt/DuckDB ne sortent jamais sur
  stdout (fichiers du répertoire de travail).
* Idempotence : un run-id déjà publié n'est pas rejoué (`"replayed": true`, mêmes compteurs) ; un
  lot identique sous un nouveau run-id ne produit aucun delta (hash) et un reconcile sans delta ne
  lance même pas dbt.
* `ingest` accepte plusieurs sources : elles tournent **en parallèle** (un processus dbt, un fichier
  DuckDB et une transaction de fusion PostgreSQL par source ; les ressources sont partagées).
* Secrets : uniquement par variables d'environnement nommées dans la config (`state.dsn_env`,
  `sources[].dsn_env`). Le DSN est scindé : dbt reçoit une chaîne libpq **sans mot de passe** et
  `PGPASSWORD` (testé : aucun secret dans les journaux conservés).

| Variable | Défaut | Rôle |
|---|---|---|
| `MDM_ELT_WORKDIR` | `./.mdm_elt_work` | brut Parquet, projets dbt générés, fichiers DuckDB, journaux (un `.gitignore` y est créé) |
| `MDM_ELT_MEMORY_LIMIT` | `4GB` | `memory_limit` DuckDB **par processus** (refus au-delà de 6 Go) ; divisé entre ingests parallèles |
| `MDM_ELT_THREADS` | `4` | `threads` DuckDB (plafonné à 8) |
| `MDM_ELT_DBT_TIMEOUT_S` | `runtime.step_timeout_minutes` | délai de chaque dbt ; dépassement = arbre de processus tué |
| `MDM_ELT_KEEP_NONE_PAIRS` | `0` | persiste aussi les paires `none` (égalité stricte avec la référence) |
| `MDM_ELT_KEEP_WORK` | `0` | garde `runs/<run-id>/…` (SQL compilé, fichier DuckDB, journaux) pour le diagnostic |
| `MDM_ELT_FAULT` | — | injection de panne (tests) : `ingest_after_dbt`, `ingest_before_commit`, `reconcile_after_dbt`, `reconcile_mid_apply`, `reconcile_before_commit` = mort brutale du processus |

## Architecture

```text
sources (PostgreSQL | Parquet)
   │ EL : copie des seules colonnes utiles + ordinal  ->  <workdir>/…/raw/part-*.parquet (immuable, rejouable)
   ▼
INGEST  (1 projet dbt par source et par run, 15 modèles, 1 fichier DuckDB par source)
   i_keys → i_ranked/i_winners (doublons) → i_norm (cast + normalisation + contrôles, 1 passage)
   → i_valid (row_hash) ; i_current (lecture étroite de l'état PG) → i_status (complétude) → i_delta (I/U/D)
   → tables de travail PG `_wi_*`  ──►  UNE transaction : source_record, source_delta, reject, anomaly,
                                          pending_delta, run_log
RECONCILE (1 projet dbt, 74 modèles, un seul reconcile à la fois par référentiel)
   delta en attente → blocs (avant/après, bascule « trop gros ») → paires candidates par jointure à
   hachage → score/décision en SQL → composantes connexes → ids de staging (fusion/scission) →
   survie par attribut → golden + golden_delta → tables de travail PG `_wr_*`
   ──►  UNE transaction (publication atomique) : match_pair, block_index, staging*, golden*, anomaly,
                                                 pending_delta, run_log
EXPORT : projections canoniques Parquet (schémas de engine.core.canonical) lues depuis PostgreSQL
```

Le Python ne fait que de l'orchestration (asyncio) : extraction (thread), rendu du projet, sous-
processus dbt (`asyncio.create_subprocess_exec`, délai, arbre de processus tué avec `psutil`, journal
capturé dans `dbt.out.log`, statuts/durées par modèle lus dans `run_results.json`), transaction
PostgreSQL (thread).

### Brut : Parquet plutôt que schéma `raw` PostgreSQL

Écriture colonnaire séquentielle sans amplification de WAL pour des lignes de 200 colonnes,
relecture parallèle native par DuckDB, lot immuable (même run-id = mêmes octets), aucune charge
d'écriture supplémentaire sur la base d'état. Le prix : un volume disque temporaire (supprimé en fin
de run réussi).

### Pourquoi générer des modèles dbt depuis la config (et non `vars` + macros)

Un moteur Python (`sqlgen.py`, gabarits `dbt_project/models/**/*.sql`, rendu `render.py` avec Jinja
aux délimiteurs `<< >>`/`<% %>`) déroule la config en SQL concret **avant** dbt ; ce que dbt reçoit
contient encore son propre Jinja (`ref`, `config`, macros).

* le SQL exécuté est explicite (200 attributs = 200 expressions écrites), inspectable dans
  `target/compiled` (`MDM_ELT_KEEP_WORK=1`) et validé statiquement par `dbt parse` (testé) ;
* les expressions de conversion dépendent du **type réel** de chaque colonne brute (lu dans le
  Parquet au moment du run) : seule une génération par run peut les spécialiser (`TRY_CAST` depuis
  VARCHAR, aucune conversion inutile pour un DATE déjà typé…) ;
* les fragments SQL sont des fonctions Python testables seules contre la référence (casts, normaliseurs,
  contrôles, hash, scores : 296 tests sans dbt ni PostgreSQL) ;
* des boucles Jinja dbt sur 200 attributs dans chaque modèle ralentissent le parsing et rendent les
  erreurs illisibles. Les macros statiques ne gardent que ce qui ne dépend pas de la config :
  `pg_q` (lecture PostgreSQL) et `components` (composantes connexes).

**Ce que dbt apporte réellement ici, honnêtement** : DAG et ordre d'exécution déduits des `ref()`,
durée/statut par modèle (c'est la source des mesures par phase), isolation de processus (un dbt par
étape : tuable, mémoire rendue), configuration DuckDB (extensions, `ATTACH`, limites) par profil,
`dbt parse` comme contrôle statique, SQL compilé consultable. **Ce qu'il coûte** : ≈ 5 s de démarrage
par invocation (import, parsing, `ATTACH`) et ≈ 60 ms par modèle même sur un delta minuscule ; deux
niveaux de Jinja ; l'algorithme itératif (composantes) passe par `run_query` dans une macro ; les
écritures PostgreSQL sont des `post_hook` non transactionnels. Un script DuckDB généré par les mêmes
gabarits donnerait les mêmes résultats avec ~10 s de moins par run ; dbt n'a de sens que si la
plateforme le standardise (lignage, exploitation commune).

## Tables (schéma `state.schema`)

`source_record`, `source_delta`, `match_pair`, `staging`, `staging_member`, `staging_event`,
`golden`, `golden_delta`, `reject`, `anomaly`, `run_log` (contrat §4) + internes : `pending_delta`
(delta en attente de reconcile, avec un numéro de séquence), `block_index` (blocs des nœuds actifs
au dernier reconcile, index (bloc) et (nœud)), `meta` (compteur d'ids de staging), vue `review`
(= `match_pair` en décision `review`). `match_pair` porte en plus `eligible`, `reason`, `run_id`.
Colonnes d'attributs typées (`text`, `bigint`, `numeric(18,4)`, `date`, `timestamp`, `boolean`).

Choix de stockage :
* les paires `none` ne sont **pas** persistées par défaut (elles n'influencent aucune décision : la
  règle « un candidat éligible par source » ne compte que des paires éligibles, donc stockées) ;
  `MDM_ELT_KEEP_NONE_PAIRS=1` les persiste (testé : égalité stricte avec la référence) ;
* `golden.stale` « publié au moins une fois » se déduit de l'ancienne ligne (pas de colonne en plus) ;
* un changement de config qui modifie les colonnes (`source_record`/`golden`) est refusé par `init`
  (pas de migration : `reset` puis réingestion).

## Delta seulement (reconcile)

Rien n'est relu hors du delta : le projet lit dans PostgreSQL, par jointure côté serveur sur des
tables de travail de clés (poussées par dbt), uniquement : les enregistrements en attente, les
blocs de ces nœuds (taille avant/après), les membres des blocs à expanser (≤ `max_block_size`),
les paires touchant les nœuds rescorés ou dont le compteur éligible change, les stagings des nœuds
graines et leurs membres, les golden de ces stagings. Les paires candidates sont produites par jointure
à hachage dans les blocs, scorées en SQL (levenshtein, exact, date_year ; somme pondérée des
comparateurs actifs, `min_evidence`, contradictions, tolérance 1e-9 comme la référence), décidées
avec la règle par source, puis seul l'ensemble affecté est reclusterisé. Les compteurs du résumé
prouvent le travail : `read_*` (lignes lues dans l'état) et `written_*` ; les tests vérifient que
les `xmin` PostgreSQL de toutes les lignes non touchées sont inchangés et que le nombre de lignes
golden réécrites égale `written_golden`.

* **Composantes connexes** : macro `components` (FastSV : accrochage des racines au plus petit voisin
  + raccourcis de pointeur), des jointures à hachage O(N+E) par tour et ~log(diamètre) tours ; pas de
  CTE récursive qui énumère des chemins. Testé sur une chaîne de 60 000 nœuds (ordre aléatoire,
  diamètre maximal) + 3 000 cliques + 5 000 isolés : 8 s dbt compris. L'invariant « une composante
  ne franchit pas la frontière de l'ensemble affecté » est vérifié à chaque run (erreur sinon).
* **Ids de staging** : héritier = composante contenant le plus de membres (égalité : plus petit
  nœud) ; fusion = plus ancien id conservé + événement `merge` ; scission = nouvel id + `split` ;
  nouveaux ids alloués dans l'ordre du plus petit nœud (compteur dans `meta`, transactionnel).
* **Survie sans auto-jointure** : un `GROUP BY staging` avec, par attribut, deux `arg_min` entiers
  (valeur et indice de la source gagnante) filtrés sur les valeurs non nulles ; la clé de tri est
  `priorité_source × 2³² + rang` (rang du membre : revision récente d'abord, puis nœud). 200
  attributs = 400 agrégats dans **un** passage, 0 jointure (vérifié sur le SQL généré). Une première
  version avec `arg_min(struct, HUGEINT)` coûtait 290-326 s à 100 k nœuds (états `Value` DuckDB) contre
  9,9 s : les agrégats spécialisés sur entiers sont indispensables.
* `golden_delta` : un `UNION ALL` par attribut sur les seuls stagings recalculés.

## Publication atomique et reprise

Chaque run écrit ses résultats dans des tables de travail PostgreSQL (`_wi_<nom>_<source>`,
`_wr_<nom>`) pendant dbt, puis une **unique transaction** applique upserts/suppressions, l'événement
`run_log` et la purge du delta en attente, et supprime les tables de travail : un lecteur voit l'état
précédent ou le suivant, jamais un mélange. Si le processus meurt avant le `COMMIT` (kill -9 pendant
dbt, pendant l'application, juste avant le commit), l'état publié est strictement inchangé (empreinte
de toutes les tables vérifiée), le delta reste en attente et une relance converge vers le résultat de
la référence ; les tables de travail orphelines sont nettoyées au lancement suivant.
Verrous consultatifs PostgreSQL : un seul reconcile par référentiel (attente bornée), un seul ingest
par source, et la fusion d'un ingest attend la fin d'un reconcile en cours (verrou partagé/exclusif),
si bien qu'un reconcile lit un état stable. Le delta en attente est purgé par numéro de séquence :
une ligne ré-ingérée entre-temps reste en attente.

## Sémantique : le levenshtein natif de DuckDB est faux pour le contrat

levenshtein de DuckDB compte des **octets** UTF-8 (levenshtein('éc', 'd') = 3, contre 2 en points de
code, comme 1 - distance / max(len) du contrat et de la référence). Le SQL généré n'appelle donc la
fonction native que si les deux chaînes sont ASCII (strlen = length) et sinon une fonction utilisateur
mdm_lev_cp (rapidfuzz, exact, vectorisée Arrow) enregistrée par le plugin dbt-duckdb
engine.lanes.elt.dbtplugin (module_paths + plugins du profil généré). Le cas courant reste en
SQL pur ; seules les paires non ASCII passent par Python. Testé : parité sur des alphabets accentués/CJK,
scénario d'équivalence avec irst_name sans strip_accents, et appel de la fonction depuis un vrai
run dbt (mdm_lev_cp('éc','d') = 2, natif = 3).

## Écarts de sémantique connus

* Normaliseurs `lower`/`upper` : DuckDB applique la casse « simple », Python la casse complète
  (`ß`→`SS`, `İ`, `ﬁ`, sigma final). `strip_accents` : marques Unicode retirées (identique sur nos
  corpus, non garanti pour toute l'Unicode) ; `trim`/`collapse_spaces` utilisent exactement
  l'ensemble des espaces Unicode de Python.
* Expressions régulières : RE2 (DuckDB) au lieu de `re` : pas d'assertions avant/arrière ni de
  références arrière (la config les accepte, `re.compile` seul les valide), `$` ne tolère pas le saut
  de ligne final.
* Conversions texte → nombre/date : `TRY_CAST` DuckDB ; formats exotiques (ex. `2020-1-5`) et
  `double` → texte peuvent différer de Python. Les colonnes absentes de la source valent NULL.
* « Dernière ligne gagnante » à révision égale : l'ordinal d'une source PostgreSQL est l'ordre de
  scan (non garanti) ; pour Parquet c'est l'ordre fichiers triés puis lignes.

## Tests (`tests/lanes/elt`, PostgreSQL via Docker : `MDM_TEST_DOCKER=1`, sinon skip propre)

| Fichier | Objet |
|---|---|
| `test_elt_sqlgen.py` | parité SQL/Python : normaliseurs (toutes permutations ≤ 3), casts, contrôles, row_hash, levenshtein, score pondéré (sans Docker) |
| `test_elt_equivalence.py` | recette n°1 : jeux synthétiques jour 0→1→2 (≈ 50 attributs ; 4 sources mixtes postgres/parquet, full/delta, fusions/scissions/retraits/rejets), `compare_exports` avec `compare_hash`, compteurs = `ReconcileResult`, propriété incrémental == à froid, mode `KEEP_NONE_PAIRS` strict |
| `test_elt_scenarios.py` | données sales (clés nulles/vides, doublons, révisions invalides, échecs de cast, `null_value`/`warn`, lots tronqués bloqués, blocs trop gros, golden bloqués, réapparitions) sur 3 graines/configs, 5 étapes chacune |
| `test_elt_example_config.py` | `configs/example/customer.yml` adaptée (sources PostgreSQL à colonnes MAJUSCULES + Parquet), 3 jours |
| `test_elt_operations.py` | idempotence (rejeu, lot identique), preuve « delta seulement » (compteurs + `xmin`), ingest parallèle, contrat CLI (JSON, codes, secrets, reset) |
| `test_elt_failure.py` | kill pendant reconcile (3 points) / arbre dbt tué en vol / ingest : état intact puis convergence |
| `test_elt_runtime.py` | dbt asynchrone (délai + arbre de processus), composantes sur gros clusters, génération 200 attributs sans auto-jointure + `dbt parse`, limites de ressources, extraction brut |

Résultat : **321 tests passent** avec `MDM_TEST_DOCKER=1` (≈ 17 min ; conteneur `mdm2-elt-test-*`
supprimé en fin de session) ; sans Docker 305 passent et 16 sont ignorés proprement. Les jeux de
test restent sous 5 000 nœuds source (≤ 2 000 entités, ≈ 3 800 lignes au plus).

`python tests/lanes/elt/scale_smoke.py --nodes 100000` rejoue la mesure ci-dessous (hors pytest).

## Mesures : smoke 100 000 nœuds × 50 attributs

Machine de mesure : 32 threads logiques, PostgreSQL 16.4 en conteneur (6 Go mémoire+swap, shm 512 Mo,
`shared_buffers=1GB`, `synchronous_commit=off`), DuckDB `memory_limit=5GB`, `threads=8` (partagés
entre les 3 ingests parallèles). Jeu `engine.core.synth`, graine 42 : 3 sources (crm et erp
PostgreSQL complètes, web Parquet delta), 190 009 lignes au jour 0 (63 k par source), delta du jour 1
≈ 1 % (0,7 % maj., 0,2 % nouveaux, 0,1 % retraits, 0,1 % rejets). Une seule exécution ; le pic RSS
est la somme des RSS de Python + dbt + DuckDB échantillonnée toutes les 0,5 s (hors conteneur
PostgreSQL : ≈ 0,93 Gio).

| Phase | Jour 0 (chargement initial) | Jour 1 (delta ≈ 1 %) |
|---|---|---|
| Ingest, 3 sources en parallèle (mur) | 18,9 s ; 190 009 lignes = **10 070 lignes/s** ; pic RSS 1,15 Go | 13,5 s ; relit 190 770 lignes (14 100 lignes/s) ; pic 1,17 Go |
| dont extraction brut (par source) | 0,6-1,0 s (≈ 65-105 k lignes/s) | 0,6-1,0 s |
| dont dbt (par source) | 13,4-14,2 s (≈ 5 s de démarrage ; `i_norm` 2,9 s, `i_valid` 1,3 s, `i_upsert` 1,7 s) | 11,6-12,3 s (`i_norm` 3,0 s) |
| dont fusion PostgreSQL (par source) | 3,3-3,5 s (63 k lignes × ≈ 42 colonnes) | 0,06-0,09 s |
| Reconcile, total | **56,5 s** ; 189 570 nœuds du delta = **3 360 nœuds/s** ; pic RSS 2,33 Go | **12,0 s** pour 1 831 nœuds ; pic 0,58 Go |
| dont dbt | 40,5 s (survie `r_gold_new` 9,9 s, lecture `r_d_rec` 4,3 s, `r_gold_up` 3,3 s, `r_gold_delta` 3,1 s) | 11,3 s (dont ≈ 10 s fixes : démarrage + 74 modèles) |
| dont application PostgreSQL (1 transaction) | 15,6 s (111 830 paires, 189 570 `staging_member`, 111 861 golden, 2,16 M `golden_delta`…) | 0,34 s |
| Lu / écrit dans l'état (jour 1) | — | lu : 6 381 `source_record` (3,4 %), 1 617 golden (1,4 %), 4 803 paires, 5 369 `staging_member` ; écrit : 1 438 golden, 883 paires, 733 `staging_member` |

État final : 189 826 enregistrements actifs, 112 230 stagings actifs, 111 895 paires stockées, 0
membre sans enregistrement actif, 0 actif sans staging, base d'état 505 Mo. Aucune extrapolation vers
1 M × 200 n'est présentée comme mesure (job réservé à l'orchestrateur).

## Écarts (gap) honnêtes

* **Coût fixe par invocation dbt** : ≈ 5 s de démarrage + ≈ 60 ms/modèle (≈ 10 s pour un reconcile
  de 74 modèles même sur 1 831 nœuds). Sur 50 référentiels/jour × (N sources + reconcile) cela
  représente plusieurs minutes de CPU fixe ; un delta minuscule ne coûte pas moins de ~12 s.
* **Le chargement initial est un « delta = tout »** : le reconcile jour 0 charge tous les nœuds dans
  DuckDB (fichier de travail 254 Mo à 190 k nœuds × 50 attributs) et pousse/relit des tables de
  travail PostgreSQL ; à 1 M × 200 il faudra surveiller disque temporaire, mémoire (≤ 6 Go, repli
  disque DuckDB) et durée de la transaction d'application (non mesuré ; découpage du delta en lots
  = piste, non implémenté : `runtime.batch_rows` n'est pas exploité).
* Une source `snapshot: delta` dont `location` accumule les fichiers (cas `stage_live` du
  générateur) est relue en entier à chaque run : coût croissant et un rejet `duplicate_key` par
  ancienne version. Pas de filigrane sur `revision` ni de liste de fichiers par run ; une source
  PostgreSQL est lue en entier (pas de `WHERE revision > …`).
* `init` ne migre pas le schéma ; pas de reprise multi-hôtes ni HA (verrous PostgreSQL + répertoire
  de travail local) ; pas d'Airflow/Airbyte/Keycloak ; la lecture de l'état par DuckDB se fait par
  plusieurs connexions (cohérente grâce au verrou, pas sous un instantané unique) ;
  l'extension DuckDB `postgres` est téléchargée au premier usage (le `Dockerfile` la préinstalle ;
  image non construite ici).
* Les gabarits (`dbt_project/`) ne sont pas déclarés comme `package-data` dans `pyproject.toml` (non
  modifié ici) : la voie fonctionne depuis un checkout ou une installation éditable.
* Résumé JSON : `dbt_models` (durées par modèle) est dans la sortie mais pas dans `run_log`.
* Aucune mesure sur des volumes ≥ 1 M, ni sous N référentiels concurrents.

## Points à signaler (contrat / config / référence, non modifiés)

1. « Incrémental == à froid » ne tient pas **sur les sources brutes** dès qu'une ligne DQ-invalide
   masque une version valide (la dernière version valide reste active) : la propriété n'est vraie que
   sur l'état `source_record` (c'est ce que fait `recompute_from_scratch`) ; le contrat §2 mérite de le dire.
2. Contrat/config muets sur la **consommation d'une source `delta`** (filigrane, fichiers déjà lus) :
   voir l'écart ci-dessus.
3. §3 « sémantique identique dans SQL, Python et PostgreSQL » ne couvre pas les normaliseurs
   (casse complète vs simple) ni les regex (`re` vs RE2) : à restreindre dans `config.py` (valider
   les motifs avec un sous-ensemble RE2) ou à normer.
4. `row_hash_input` n'est pas injectif si une valeur contient U+001E/U+001F/U+0000 (collision
   théorique ; sans effet sur nos jeux).
5. La référence persiste les paires `none` (non borné) : à confirmer comme exigence ou à rendre optionnel.
