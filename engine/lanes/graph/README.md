# Voie C « graphe » : PostgreSQL 16 + Apache AGE 1.6.0 (openCypher)

Implémentation du contrat `docs/MDM_CONTRACT.md` (delta-driven, config-driven) sur un graphe de
propriétés AGE. Certifiée contre `ReferenceEngine` avec `engine.core.compare` (voir « Tests »).
Tout chiffre ci-dessous est **mesuré** sur cette machine, sauf la section « Extrapolations »,
qui est explicitement **non mesurée**.

## Utilisation

```text
python -m engine.lanes.graph init       --config C
python -m engine.lanes.graph ingest     --config C --source S --run-id R
python -m engine.lanes.graph reconcile  --config C --run-id R [--components hop|varlen]
python -m engine.lanes.graph export     --config C --out DIR
python -m engine.lanes.graph reset      --config C
```

* DSN d'état : variable d'environnement `state.dsn_env` ; DSN des sources postgres : `source.dsn_env`.
  Aucun secret dans la config, ni dans les sorties JSON.
* stdout : **une** ligne JSON (`lane, command, run_id, status, replayed, counts, seconds,
  wall_seconds`). Code de sortie `0` = ok, `2` = erreur attendue (config, variable absente, source
  illisible, dérive de config), `1` = autre exception ; la ligne JSON porte alors `status: "error"`.
* Un lot `full` sous `full_snapshot_min_ratio` est **bloqué** : sortie `0`, `status: "blocked"`, aucune
  écriture sauf l'anomalie `batch_blocked` (le contrat ne précise pas le code de sortie).
* Idempotence : `(run_id, étape)` terminé est rejoué sans effet (`replayed: true`). Rejouer les mêmes
  données avec un nouveau run-id ne produit aucun delta.
* `init` crée l'extension (`CREATE EXTENSION IF NOT EXISTS age`, super-utilisateur requis), le schéma
  relationnel `<state.schema>`, le graphe `<state.schema>_g`, les labels et les index ; changer la liste
  d'attributs exige `reset` (pas de migration en v1). `reset` supprime graphe + schéma.
* Outils : `python -m engine.lanes.graph.smoke` (50 k nœuds), `...bench` (COPY vs Cypher). Déploiement et
  limites de ressources : `deploy/lanes/graph/`.

## Modèle physique (graphe vs relationnel : décision et mesures)

```text
(Record {v,s,k}) -[:IN_BLOCK]-> (Block {v,bk,o})            blocage : Block = (clé de blocage, valeur)
(Record) -[:MATCH {s,c,e,d,r,run}]-> (Record)               TOUTE paire candidate (score, décision), gauche -> droite
(Record) -[:REVIEW {s,r,run}]-> (Record)                    paires en revue (état « review »)
(Record) -[:MEMBER_OF]-> (Staging {v}) -[:PUBLISHED_AS]-> (Golden {v})
```

`v` = copie de l'identifiant graphid de l'élément (voir gotcha n°2). Les ids `Staging`/`Golden` sont
dérivés du graphid : `staging_id = graphid & (2^48-1)` ; `golden_id = staging_id`.

Tables relationnelles du schéma d'état (**charge utile et journaux, hors graphe**) :
`source_record` (une colonne typée par attribut + `_vid`, hash, révision, statut), `golden` (valeurs +
`<attr>__src`), `staging`, `pending` (delta en attente), journaux `source_delta`, `reject`, `anomaly`,
`staging_event`, `golden_delta`, `run_log`, `meta`.

**Décision hybride.** Les 200 attributs ne vivent **pas** dans l'agtype. Mesure (20 000 sommets ×
30 attributs texte, `bench`) :

| | COPY 20 k sommets | lecture 30 attributs | taille |
|---|---|---|---|
| propriétés d'identité seules (`v,s,k`) | 204 k lignes/s | — | 2,3 Mo |
| 30 attributs dans l'agtype | 38,5 k lignes/s | 8,4 k lignes/s (Cypher) | 16,7 Mo |
| table relationnelle typée (30 colonnes) | — | 188 k lignes/s (SQL) | 9,2 Mo |

Les attributs en agtype coûtent ~5× à l'écriture, ~22× à la lecture et ~1,8× en place ; le graphe ne
garde donc que l'identité, la topologie de blocage et les décisions.

### Ce qui est calculé où

| Étape | Dans le graphe (openCypher) | SQL sur tables AGE / relationnel | **Non graphe** (Python / Arrow) |
|---|---|---|---|
| Ingest : lecture, normalisation, cast, DQ, `row_hash`, delta I/U/D, rejets, complétude | — | MERGE relationnel (`source_record`, `source_delta`, `reject`) ; création des sommets `Record` par COPY | **tout le calcul** (pyarrow / numpy), v. « Équivalence » |
| Index de blocage | tailles de blocs (`count`), blocs actuels d'un nœud, membres des blocs qui basculent (`MATCH ... IN_BLOCK`) | COPY / DELETE des arêtes `IN_BLOCK` ; création des `Block` ; drapeau `o` (oversize) | identités de blocs (préfixe, année, texte canonique) ; détection de bascule |
| Paires candidates | **traversée 2 sauts** `Record-IN_BLOCK->Block<-IN_BLOCK-Record` (blocs non oversize) | COPY / DELETE des arêtes `MATCH` | **score** (levenshtein rapidfuzz `cpdist`, sommes pondérées dans l'ordre déclaré), contradiction, éligibilité |
| Décision match/review/none | **comptes d'éligibles par nœud et par source** (`count(m)` sur `MATCH {e:true}`) ; lecture des paires voisines | UPDATE des propriétés `MATCH`, COPY / DELETE des `REVIEW` | règle de décision (`reference.matching.decide`, fonction pure partagée avec l'oracle) |
| Clusters | **adjacence des arêtes `MATCH {d:'match'}`** des nœuds affectés (1 saut) ; ensemble affecté (`MEMBER_OF`) | COPY / DELETE des `MEMBER_OF` | **étiquetage en composantes (union-find)**, héritiers, `merge`/`split`/`create`, ids de staging |
| Survie / golden | — | upsert `golden`, `golden_delta`, `anomaly` | survie par attribut (numpy `lexsort`), diff golden |
| Export | `match_pair`, `review`, `staging_member` relus **par Cypher** | autres tables en SQL | construction des tables Arrow / Parquet |

Mode `--components varlen` : composantes par chemins de longueur variable (`[:MATCH*1..8 {d:'match'}]`),
fermeture **dans** le graphe. Exact aux petites tailles (testé), inutilisable à l'échelle : voir gotcha n°6.

Chargement en masse : `COPY` direct dans les tables de labels AGE (`id` = `(label_id << 48) | nextval`,
exactement ce que fait `CREATE`), puis `ANALYZE` si ≥ 5 000 lignes (le planificateur n'a pas de stats pour
les lignes de la transaction courante). Aucune boucle `MERGE` par ligne.

| Méthode (mesurée, `bench`, 20 000 lignes sauf mention) | lignes/s |
|---|---|
| COPY dans la table de label (identité seule) | 204 000 |
| `UNWIND [...] CREATE` par lots de 1 000 | 51 000 |
| `CREATE` par ligne (1 instruction/ligne, 2 000 lignes) | 1 075 |
| `MERGE` par ligne, 20 000 existants, sans index / index d'expression (2 000 lignes) | 731 / 738 |
| `MERGE` par ligne, index GIN sur `properties` | 949 |
| `UNWIND ... MERGE` lots de 1 000, index d'expression (inutilisé par MERGE) | **51** |
| `UNWIND ... MERGE` lots de 1 000, index GIN | 35 430 |

COPY est > 200× plus rapide que `MERGE` par ligne et ~4× plus rapide que `UNWIND CREATE`. Un `UNWIND MERGE` sans GIN est
quadratique (balayage séquentiel par ligne).

## Atomicité, reprise, concurrence

* **Publication atomique** : un `reconcile` = **une transaction PostgreSQL** (arêtes `MATCH`/`REVIEW`/
  `MEMBER_OF`/`PUBLISHED_AS`, `golden`, `golden_delta`, `staging*`, consommation de `pending`, `run_log`).
  Un lecteur voit l'état avant ou après ; `export` lit en `REPEATABLE READ READ ONLY`.
* Un `ingest` = une transaction par (run, source). Verrous consultatifs par référentiel : `ingest`
  partagé (plusieurs sources en parallèle, testé), `reconcile` exclusif.
* Panne en cours (`kill -9` simulé par `MDM_GRAPH_FAIL_AT=<point>` → `os._exit(137)`) : transaction annulée par
  le serveur, `pending` intact, rien de publié ; la relance converge vers l'état de la référence (6 points
  de `reconcile` + `ingest`, testés). Les séquences AGE ne sont pas annulées : les ids ont des trous mais
  restent croissants.
* Rejouer un run-id `reconcile` déjà terminé ne retraite pas les `pending` apparus depuis : ils partent au
  run suivant.

## Équivalence avec la référence

* Normalisation / cast / checks / `row_hash` vectorisés (`values.py`) : chaque étape est exacte par
  construction (noyaux Arrow à jeu de caractères ASCII explicite pour les valeurs ASCII ; reprise des
  fonctions de référence sur les valeurs **distinctes** pour le reste ; regex toujours via `re`). Testé contre
  `process_attribute` / `row_hash` sur valeurs aléatoires (Unicode, espaces exotiques, types mixtes).
* Scores, identités de blocs, survie : implémentations indépendantes de la référence, comparées à
  `score_pair`, `block_values`, `survive_attribute` (égalité flottante exacte).
* Réutilisés de la référence (fonctions pures, donc non indépendants) : `decide`, `cast_value`,
  `normalize_string` (repli), `to_timestamp`.
* Écart connu : un décimal `-0` est haché `0.0000` (Arrow n'a pas de zéro négatif) ; la référence hacherait
  `-0.0000`.
* Écart de politique : une colonne source absente fait échouer l'`ingest` (code 2) ; la référence la traite
  comme nulle.

## Gotchas AGE 1.6.0 / PG16 rencontrés (mesurés)

1. **Motif d'arête non orienté = produit cartésien.** `(a)-[m]-(b)` est compilé en jointure filtrée par
   `(start=a AND end=b) OR (end=a AND start=b)` (226 M lignes écartées pour 700 nœuds / 386 arêtes ; `reconcile`
   de 763 nœuds : 16,9 s → 0,27 s). Le moteur émet deux requêtes orientées (`cypher_both`).
2. **`id(n) IN [...]` n'est pas servi par un index** (`age_id(_agtype_build_vertex(...)) = ANY(...)`,
   balayage séquentiel). Une propriété `v` recopiant l'id + index btree d'expression
   (`agtype_access_operator(properties,'"v"')`) rend `r.v IN [...]` indexé : 500 ids 0,075 s vs 0,46 s,
   5 000 ids 0,37 s vs 0,80 s (100 k sommets). `MATCH (n {v: x})` (égalité de map) n'utilise pas cet index.
3. Aucun index n'est créé sur `id`, `start_id`, `end_id` des tables de labels : à créer (`init` le fait).
4. Paramètres Cypher : le 3ᵉ argument de `cypher()` doit être un `$n` typé `agtype` ; psycopg 3 ne sait pas
   l'inférer (`IndeterminateDatatype`). Les listes d'entiers sont donc écrites dans la requête (entiers
   Python uniquement, jamais de texte utilisateur dans le Cypher).
5. `agtype::text` renvoie le texte **sans guillemets** pour une chaîne ; `(c::text)::bigint/float8/boolean`
   convertit directement côté SQL. Les flottants font l'aller-retour sans perte.
6. **Chemins de longueur variable** : `[:MATCH*1..8 {d:'match'}]` énumère des chemins ; sur 52 k nœuds, le
   premier lot de 8 000 nœuds tournait encore après **plus de 8 min 18 s** (interrompu) alors que le saut unique
   complet prend ~2,5 s (phase `recluster` du jour 0).
7. `MERGE` n'utilise pas les index d'expression (motif `@>`) ; GIN requis (tableau ci-dessus).
8. Un label portant un mot-clé (`Unwind`) est une erreur de syntaxe ; `MATCH` fonctionne comme label d'arête.
9. Une connexion psycopg non autocommit ouvre une transaction implicite : `with conn.transaction()` n'est
   alors qu'un SAVEPOINT jamais validé (bug rencontré). Le moteur utilise l'autocommit + transactions
   explicites.
10. `LOAD 'age'` et `search_path = ag_catalog` par session ; `create_graph` exige un nom d'au moins 3 caractères ;
    un graphe est un schéma PostgreSQL.

## Mesures : smoke 50 k nœuds × 30 attributs

Jeu synthétique (`engine.core.synth`, graine 11) : 27 500 entités → **52 170 lignes source** (3 sources :
crm/erp PostgreSQL `full`, web Parquet `delta`), 30 attributs. Conteneur `mdm2-graph-*` : 6 Go RAM+swap,
shm 512 Mo, `shared_buffers=1GB`, **durabilité par défaut** (fsync actif), disque Docker. Chaque phase est
un processus CLI séparé (temps de mur incluant ~0,9 s de démarrage/connexion) ; « interne » = somme des
phases mesurées dans le processus. Machine : 32 threads, un seul conteneur, une seule exécution (pas de
médiane).

| Phase | Jour 0 (initial) | Jour 1 (delta ≈ 0,9 %) | Jour 2 (delta ≈ 4,3 %) |
|---|---|---|---|
| ingest crm (17,4 k lignes) mur / interne | 2,87 s / 1,88 s | 2,33 s / 1,42 s | 2,25 s / 1,31 s |
| ingest erp | 2,87 s | 2,25 s | 2,16 s |
| ingest web (17,3 k lignes) | 2,69 s | 2,12 s | 2,07 s |
| ingest, lignes/s (mur, 1 source) | ≈ 6 100 | ≈ 7 500 | ≈ 7 800 |
| reconcile mur / interne | 15,1 s / 14,1 s | 1,64 s / 0,65 s | 2,80 s / 1,85 s |
| reconcile, nœuds du delta | 52 012 | 446 | 2 249 |
| reconcile, nœuds du delta/s (interne) | 3 700 | 690 | 1 215 |
| reconcile, nœuds affectés / goldens réécrits | 52 012 / 30 959 | 863 / 407 | 4 243 / 2 034 |
| export complet (jour 2 : 52 638 records, 617 439 `golden_delta`) | 5,1 s | 5,2 s | 5,2 s |

Phases internes du `reconcile` jour 0 : index 3,41 s, paires 2,59 s, décisions 1,63 s, clusters 2,52 s,
survie/publication 3,79 s. Jour 0 de bout en bout (3 ingest séquentiels + reconcile) : 23,5 s soit
≈ 2 200 nœuds/s, **sous** la cible de 2 300 nœuds/s, sans concurrence entre référentiels et avec 30 attributs.
Les jours 1/2 : l'ingest relit et re-hache **tout** le snapshot `full` de chaque source (≈ 2,2 s par source,
indépendant de la taille du delta : c'est inhérent à la détection des retraits) ; le débit `reconcile` y est
dominé par les coûts fixes (démarrage, connexion) et n'est pas représentatif d'un régime asymptotique.

Preuve « delta seulement » : le delta de 446 nœuds réécrit 407 goldens, 162+91 arêtes `MEMBER_OF`, 459+430
arêtes `MATCH` et laisse le reste de l'état intact. Test `xmin` : après la modification d'**une** ligne source,
le nombre de lignes PostgreSQL réécrites (xmin changé ou supprimées) est ≤ max(8, 2 % de la table) pour
`golden`, `MEMBER_OF`, `MATCH`, `IN_BLOCK`, `Record` et `source_record`, et un `reconcile` sans delta n'écrit rien.

Mémoire : RSS crête du client Python (processus CLI) : ingest 147–175 Mo, reconcile jour 0 396 Mo (jour 1/2
91/107 Mo), export 389–396 Mo, interpréteur seul 73 Mo. Conteneur AGE : crête **291 Mo** (échantillonnée
toutes les ~1 s par `docker stats`, plafond 6 Go). Disque : base d'état **166 Mo** après les 3 jours
(`golden_delta` 41,9 Mo, `source_record` 23,6 Mo, `Block` 13,3, `MATCH` 13,1, `IN_BLOCK` 12,8, `golden` 10,9,
`Record` 8,6, `MEMBER_OF` 7,0, `source_delta` 6,4, `REVIEW` 1,2). Les 4 labels de sommets et les 5 labels d'arêtes (avec leurs index) pèsent ≈ 68 Mo, soit ~41 % de la base.

## Extrapolations (NON mesurées)

Linéaires en nombre de cellules (lignes × attributs), à partir du smoke ci-dessus ; aucune n'a été exécutée
(pas de job 1 M × 200).

* Mémoire client : ~190 octets/cellule à l'ingest (≈ 80 Mo / 0,42 M cellules mappées) et ~207 octets/cellule au
  reconcile jour 0 (≈ 323 Mo / 1,56 M cellules). 1 M × 200 (≈ 160–200 M cellules) donnerait de l'ordre de
  **30–40 Go** de RSS : **hors budget** (6 Go/conteneur, 30 Go au total) tant que l'ingest et la survie ne sont pas
  découpés en lots.
* Temps (si la mémoire n'était pas un obstacle). Jour 0 du smoke, durées internes : phases liées aux nœuds
  (index, paires, décisions, clusters) 10,1 s, phases liées aux cellules (ingest 5,7 s + survie 3,8 s) 9,5 s.
  Pour 1 M nœuds × 200 attributs : nœuds ×19, cellules ×128 → 10,1 × 19 + 9,5 × 128 ≈ 1 400 s, soit **≈ 25 min et
  ≈ 700 nœuds/s** pour un référentiel seul, ~3× sous la cible de 2 300 nœuds/s (hypothèse de linéarité, sans la
  dégradation des index et du planificateur à cette taille). 50 référentiels/jour = ~21 h séquentielles : il en
  faudrait ≥ 4 en parallèle sur 6 h, ce que la mémoire ci-dessus interdit aujourd'hui ; non mesuré.
* `golden_delta` : la première publication écrit une ligne par attribut non nul (606 134 lignes pour 30 959
  goldens ici, ~68 octets/ligne) ; à 1 M × 200 : de l'ordre de 10⁸ lignes (~7–10 Go) en **une** transaction.

## Écarts (ce qui ne passe pas à 1 M × 200) et ce qu'il faudrait

1. **Lecture de la source en une fois** (tuples Python → Arrow) : pas de découpage par `batch_rows`. Il faut
   un flux par morceaux (COPY BINARY / curseur → Arrow) avec déduplication et complétude en deux passes.
2. **Un reconcile = une transaction non découpée** : WAL, verrous et mémoire proportionnels au delta. Il faudrait
   un traitement par lots d'ensembles affectés (les composantes sont indépendantes) avec publication atomique
   par jonction de métadonnées plutôt qu'une longue transaction.
3. **La survie lit toutes les colonnes de tous les membres affectés** via des tuples Python (190 o/cellule) ;
   il faudrait un moteur colonnaire (DuckDB/Arrow natif, ou calcul côté serveur par colonne).
4. **AGE** : pas d'index de propriétés automatiques, pas de parallélisme intra-requête dans `cypher()`,
   planificateur sensible aux statistiques, motifs non orientés et chemins variables inutilisables, listes d'ids
   écrites dans le texte des requêtes (un Cypher par lot de 8 000 ids). Le graphe n'apporte ici que la topologie
   (blocs, paires, appartenance) ; la charge utile (200 attributs) est de toute façon relationnelle.
5. **Neo4j / hybride** : si le graphe doit rester le support des composantes sur des dizaines de millions
   d'arêtes, Neo4j (index natifs de propriétés, `MERGE` indexé, algorithmes de composantes connexes GDS)
   éliminerait les gotchas 1–3, 6–7, mais ne résout pas la charge utile : l'architecture réaliste est
   **hybride** — payload en colonnaire (Parquet/DuckDB), graphe (ou simples tables d'arêtes) limité aux
   décisions et au clustering.
6. Non couverts : plusieurs référentiels en parallèle (non mesuré), haute disponibilité, reprise multi-hôtes,
   ramasse-miettes des `Block` vides, migration de schéma (changement d'attributs = `reset`), noms d'attributs
   > 58 caractères (`<attr>__src` dépasse 63 caractères PostgreSQL), `ingest` concurrent d'une même source.

## Tests

`MDM_TEST_DOCKER=1 python -m pytest tests/lanes/graph -q` (démarre un conteneur `mdm2-graph-<hex>`,
6 Go, supprimé en fin de session ; sans la variable les tests Docker sont ignorés).

* `test_graph_values.py`, `test_graph_units.py` (sans Docker) : pipeline vectorisé, scores, blocs, survie
  == référence.
* `test_graph_equivalence.py` : jeu synthétique 30 attributs (450 entités) et config adaptée de
  `configs/example/customer.yml`, jour 0 → 1 → 2 (mises à jour, fusions, scissions, retraits, rejets DQ,
  doublons, nuls) ; équivalence complète à chaque jour (`compare_exports`, événements inclus), incrémental ==
  recalcul à froid (voie graphe et référence), compteurs « delta seulement », preuve `xmin`, rejeu, cohérence
  structurelle du graphe (sommets, arêtes, tailles de blocs, orientation, 1 `MEMBER_OF` par actif).
* `test_graph_edge_cases.py` : DQ, doublons de clé et révisions, clé nulle, réactivation, lot bloqué, chaînes
  étranges (guillemets, `\`, `$$`, retours à la ligne, Unicode, emoji) via COPY et Cypher, blocs
  surdimensionnés (anomalie, bascule), golden bloqué, `most_recent`, types natifs ; modes `hop` et `varlen`.
* `test_graph_failure.py` : kill à 6 points du reconcile et avant commit de l'ingest ; rejeu convergent.
* `test_graph_cli.py` : contrat CLI (une ligne JSON, codes de sortie, idempotence, `reset`, dérive de config,
  secrets), ingest parallèle des sources.
