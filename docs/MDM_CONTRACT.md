# MDM maison : contrat du moteur (v2)

Ce document est la **référence unique** pour les trois voies (ETL, ELT, graphe). Il remplace
les anciens documents MDM (archivés dans `docs/archive/`). `docs/SPECIFICATIONS.md` reste la
spec du moteur de pipelines (file, workers, admission) ; ce contrat en est un sous-ensemble
vertical dédié au MDM.

## 1. Cible de dimensionnement (hypothèse de travail)

- 50 référentiels par jour, ~1 M de nœuds source par référentiel, ~200 attributs par nœud.
- Un run = un référentiel. Cible : 50 M nœuds/jour, ~10 G valeurs d'attributs/jour.
- Fenêtre de batch hypothétique : 6 h ⇒ ≥ 2 300 nœuds/s en moyenne, avec N référentiels en
  parallèle sur la machine.
- Machine de mesure : 32 threads, 63 Go de RAM, disque local. Aucune extrapolation présentée
  comme mesure.

## 2. Principe : tout est piloté par le delta

Un référentiel = une config YAML (`engine/core/config.py`). L'ingénieur data n'écrit pas de code :
il déclare sources, attributs (avec l'ordre des sources), nullabilité, contrôles, règles de
rapprochement. Le moteur déroule trois couches, **uniquement sur ce qui a changé**.

```text
source (snapshot/feed)
   │ 1. INGEST (par source, parallélisable)
   │    normalise + contrôle DQ + row_hash ; MERGE dans source_record ; émet source_delta
   ▼
source_delta (I / U / D) ── seulement les deltas DQ-valides continuent
   │ 2. RECONCILE (un seul à la fois par référentiel)
   │    candidats via index de blocking → scores → décisions → clusters touchés
   ▼
staging (cluster de source_record) ── seuls les stagings touchés sont recalculés
   │ 3. SURVIVE + PUBLISH
   │    survie par attribut sur les stagings touchés → golden ; delta golden
   ▼
golden + golden_delta (+ xref source→golden)
```

Règles du delta :

1. **Ingest** : clé `(source_id, source_key)`. `row_hash` = hash des valeurs normalisées des
   attributs mappés de la source. Nouvelle clé ⇒ `I` ; hash différent ⇒ `U` ; même hash ⇒
   aucun delta (rejouer le même lot ne change rien : idempotence). Clé absente d'un snapshot
   `full` ⇒ `D` (retrait logique, jamais physique) **seulement si** le lot passe le contrôle de
   complétude (`full_snapshot_min_ratio`) ; sinon le lot est bloqué et aucune suppression n'a lieu.
   Doublon de clé dans un même lot : garder la plus grande `revision`, puis la dernière ligne ;
   les autres sont tracés en `reject`.
2. **DQ à l'ingestion** : chaque contrôle a une action `reject_record` (le nœud ne produit
   pas de delta, la dernière version valide reste active, l'erreur est visible),
   `null_value` (la valeur est traitée comme nulle) ou `warn`. Un nœud rejeté ne déclenche
   **aucune** réconciliation.
3. **Reconcile** : l'ensemble affecté = nœuds du delta + membres de leurs stagings actuels.
   Les paires ne sont rescorées que si l'un des deux nœuds est dans le delta. Les stagings
   non touchés ne sont ni relus ni réécrits.
4. **Staging** : composantes connexes des paires acceptées. Un staging garde l'identifiant
   existant le plus ancien ; fusion ⇒ l'autre identifiant est redirigé (événement `merge`) ;
   scission due à un changement de source ⇒ la plus grande partie garde l'id, l'autre reçoit
   un nouvel id (événement `split`). Paires en revue (score entre `review` et `accept`, ou
   plusieurs candidats éligibles) ne fusionnent pas ; elles vont dans `review`.
5. **Survie** (par attribut, sur les membres actifs du staging) : parcourir `sources` dans
   l'ordre déclaré, première valeur non nulle valide gagne (`strategy: priority`), ou la plus
   récente par `revision` (`strategy: most_recent`) ; égalité : `revision` la plus grande,
   puis `(source_id, source_key)` croissant. Si toutes les sources sont nulles : attribut nul
   si `nullable: true` ; sinon le golden est `blocked` (non publié, ancienne version conservée
   et marquée périmée, anomalie visible).
6. **Golden** : identifiant stable (celui du staging). Staging sans membre actif ⇒ `retired`.
   Toute modification d'attribut produit une ligne `golden_delta` (ancien, nouveau, source
   gagnante, run). Un golden dont rien n'a changé n'est pas réécrit.

**Propriété de recette n°1 (équivalence incrémentale)** : appliquer J1, puis le delta J2 donne
le même résultat (partition en stagings, valeurs golden, statuts) que recalculer J2 à froid,
modulo les identifiants. Elle est testée sur des deltas générés (insertions, mises à jour qui
fusionnent/scindent, retraits, rejets DQ).

## 3. Sémantique de rapprochement (identique pour les trois voies)

Pour que les voies soient comparables, seuls des comparateurs de sémantique identique dans
SQL, Python et PostgreSQL sont autorisés en v1 :

| Comparateur | Score |
|---|---|
| `exact` | 1 si égal (après normalisation), sinon 0 |
| `levenshtein` | `1 - distance / max(len(a), len(b))` |
| `date_year` | 1 si même année, sinon 0 |

Un comparateur dont l'un des deux côtés est nul est ignoré ; le score du couple est la somme
pondérée des comparateurs actifs divisée par la somme de leurs poids. Si le poids actif
< `min_evidence`, le couple n'a pas de décision. Contradiction (`contradictions`) : deux valeurs
non nulles différentes sur cet attribut ⇒ jamais de fusion automatique (revue si score ≥ `review`).
Décision : un seul candidat éligible avec score ≥ `accept` et sans contradiction ⇒ `match` ;
`[review, accept[` ou plusieurs candidats ⇒ `review` ; sinon aucune décision.
Blocage : une ou plusieurs clés de blocage (union des candidats) ; un bloc > `max_block_size`
n'est pas expansé : il est tracé comme anomalie `oversized_block` (jamais tronqué silencieusement).

## 4. Tables canoniques (schéma d'état, identique en sens logique pour les trois voies)

| Table | Contenu clé |
|---|---|
| `source_record` | `source_id, source_key, row_hash, revision, status(active/retired), first_run, last_change_run`, une colonne par attribut normalisé |
| `source_delta` | `run_id, source_id, source_key, kind(I/U/D), old_hash, new_hash` |
| `match_pair` | `left_key, right_key, score, decision(match/review/none), contradiction, rule_version` (clé canonique ordonnée) |
| `staging_member` | `staging_id, source_id, source_key` ; `staging(staging_id, status, created_run)` ; `staging_event(run_id, kind merge/split/create, ids)` |
| `golden` | `golden_id(=staging_id), status(published/blocked/retired), stale bool`, une colonne par attribut, `attr__src` optionnel (provenance) |
| `golden_delta` | `run_id, golden_id, attribute, old_value, new_value, winning_source` |
| `review` | `run_id, left_key, right_key, score, reason` |
| `reject` | `run_id, source_id, source_key, attribute, rule, action` (pas de valeur personnelle journalisée en clair par défaut) |
| `run_log` | `run_id, referential, step, status, started_at, ended_at, counts, error` |

Ce sont des **contrats logiques** ; le stockage physique propre à chaque voie est libre
(tables PostgreSQL, DuckDB, graphe AGE), mais chaque voie doit savoir exporter ces projections
via la commande `export` (§6) pour la comparaison.

## 5. Config : une seule source de vérité

Voir `engine/core/config.py` (Pydantic strict, `extra=forbid`, références croisées validées) et
l'exemple `configs/example/customer.yml`. Cas d'usage « brancher une nouvelle source » :
1. déclarer la source (`sources:`) ;
2. pour chaque attribut concerné, ajouter `{source, column}` **à la position voulue** dans la
   liste `sources` de l'attribut (la position = la priorité de survie) ;
3. `mdm validate`, puis `mdm check-source` (profil + mapping), puis un run.

Aucun SQL ni Python à écrire pour ce parcours. Les types de contrôle et de normalisation
sont une liste fermée ; en ajouter un demande du code moteur (écart assumé, cf. SPECIFICATIONS §1.1).

## 6. Contrat d'exécution des voies

Chaque voie est un package `engine/lanes/<etl|elt|graph>/` exposant :

```text
python -m engine.lanes.<lane> init       --config C                 # crée le schéma d'état (idempotent)
python -m engine.lanes.<lane> ingest     --config C --source S --run-id R
python -m engine.lanes.<lane> reconcile  --config C --run-id R      # étapes 2 et 3
python -m engine.lanes.<lane> export     --config C --out DIR       # projections canoniques en Parquet
python -m engine.lanes.<lane> reset      --config C                 # supprime l'état du référentiel
```

Chaque commande : code de sortie ≠ 0 en cas d'échec, une ligne JSON de synthèse sur stdout
(compteurs, durées par phase), idempotente pour un même `run-id`. Les secrets viennent de
variables d'environnement, jamais de la config.

`init`, `export` et `reset` acceptent aussi `--run-id` facultatif pour que le
runtime identifie et supervise leurs processus. Cela ne change pas leur état
métier ni ne rend `reset` idempotent vis-à-vis des données supprimées.

## 7. Ce que l'étude doit conclure

Pour chaque voie : débit mesuré (chargement initial 1 M × 200 ; delta quotidien à 1 %, 5 %,
20 %), mémoire, disque, temps par phase, comportement sous N référentiels concurrents, résultat
d'équivalence avec la référence, et **écarts** (gap) entre ce qui est livré et les exigences ci-dessus
et celles de `SPECIFICATIONS.md` (Airflow, Airbyte, Keycloak, HA, reprise multi-hôtes, etc.).

## 8. Précisions issues de l'implémentation (normatives)

Constatées en construisant les trois voies ; elles complètent les §2 et §3.

1. **Propriété n°1** : elle porte sur l'état `source_record` (dernière version *valide* de chaque
   clé), pas sur les sources brutes : une ligne DQ-invalide qui masque une version valide
   n'efface pas la version valide.
2. **Lot `full` bloqué** (complétude insuffisante) : aucune suppression, code de sortie 0,
   `status: blocked`, anomalie `batch_blocked`.
3. **Levenshtein** compte des points de code Unicode, pas des octets (le `levenshtein` natif de
   DuckDB compte des octets : il est corrigé par UDF pour les chaînes non ASCII).
4. **Normaliseurs** `lower`/`upper` : casse simple. **Regex** : sous-ensemble commun à `re` et
   RE2 ; la config est validée avec `re` seul (écart connu).
5. **Paires de décision `none`** : non persistées par défaut (elles n'influencent aucune
   décision) ; option de persistance pour les tests stricts.
6. **Colonne source absente** : erreur de configuration (code 2), jamais une valeur nulle
   silencieuse.
7. **Vocabulaire des rejets** : `key_null`, `revision`, `duplicate_key`, `cast`, plus les règles
   de `checks`.
8. **Doublon de clé à révision égale** dans un même lot : « dernière ligne » n'est déterministe
   que pour une source ordonnée ; pour plusieurs fichiers Parquet ou une table sans ordre, le
   résultat dépend de l'ordre de lecture (**écart connu**).
9. **Source `snapshot: delta`** : le contrat ne définit pas comment ne lire que les nouveaux
   fichiers (filigrane sur `revision`, liste de fichiers par run) : les voies relisent la source
   entière (**écart connu**).
10. **Chargement initial** : `golden_delta` écrit une ligne par attribut et par golden, soit ~10⁸
    lignes à 1 M × 200 (**point ouvert** : événement `create` unique à la place ?).
11. **Noms** : un attribut ne peut pas porter un nom de colonne canonique (`revision`,
    `source_id`, `source_key`, `row_hash`, `first_run`, `last_change_run`, `run_id`, `golden_id`,
    `staging_id`, `status`, `stale`, `node_id`, `published_once`) ni dépasser
    58 caractères (`<nom>__src` ≤ 63).
