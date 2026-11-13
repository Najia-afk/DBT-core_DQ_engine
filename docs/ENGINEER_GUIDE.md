# Guide de l'ingénieur data : brancher une source sans écrire de code

Ce guide décrit l'outillage en ligne de commande du MDM maison (`engine/cli/`). Il complète le
contrat (`docs/MDM_CONTRACT.md`, qui fait foi pour la sémantique du moteur) et la config
(`engine/core/config.py`, Pydantic strict). Un référentiel = **un fichier YAML** ; brancher une
source = **éditer ce YAML**, pas écrire du SQL ni du Python.

Toutes les commandes se lancent depuis la racine du dépôt :

```powershell
.\.venv\Scripts\python.exe -m engine.cli <commande> [options]
```

Les exemples ci-dessous abrègent en `mdm <commande>`. Code de sortie : `0` (OK ou WARN),
`1` (erreur / BLOCKING). `--json` produit une sortie structurée pour les scripts et la CI.

## 1. Parcours nominal : brancher une nouvelle source

Cas : le référentiel `customer` existe (`configs/example/customer.yml`) ; on veut ajouter la
source `billing` (table PostgreSQL `public.billing_accounts`, flux incrémental) qui fournit
`last_name`, `first_name` et `email`, ce dernier avec la **plus haute priorité**.

### Étape 0 : partir d'une config valide

```text
mdm validate configs/example/customer.yml
WARN  customer.yml:43  attributes[4].sources: 'country' is nullable: false but not fed by web. ...
OK: 0 error(s), 4 warning(s), 0 note(s)
```

Les warnings existants sont connus ; on ne regardera que ceux que l'ajout **introduit**
(`add-source` les isole pour vous).

### Étape 1 : prévisualiser l'ajout (`--dry-run`)

```text
mdm add-source configs/example/customer.yml --source-id billing --kind postgres ^
    --location public.billing_accounts --dsn-env BILLING_DSN --key account_id ^
    --revision updated_at --snapshot delta ^
    --map last_name=NOM_FACT first_name=PRENOM_FACT email=MAIL@1 --dry-run
```

(`^` = continuation de ligne `cmd` ; en PowerShell, utiliser le caractère `` ` ``.)

Sortie réelle (extrait, avant/après) :

```diff
@@ -13,18 +13,19 @@
      key: client_no, revision: modified_at, snapshot: full}
   - {id: web, kind: parquet, location: /data/web/customers/*.parquet,
      key: account_id, revision: event_ts, snapshot: delta}
+  - {id: billing, kind: postgres, location: public.billing_accounts, dsn_env: BILLING_DSN, key: account_id, revision: updated_at, snapshot: delta}

 attributes:
   - name: last_name
     ...
-    sources: [{source: crm, column: LASTNAME}, {source: erp, column: nom}, {source: web, column: surname}]
+    sources: [{source: crm, column: LASTNAME}, {source: erp, column: nom}, {source: web, column: surname}, {source: billing, column: NOM_FACT}]
...
   - name: email
     normalize: [trim, lower]
-    sources: [{source: web, column: email}, {source: crm, column: EMAIL}]
+    sources: [{source: billing, column: MAIL}, {source: web, column: email}, {source: crm, column: EMAIL}]
     strategy: most_recent

WARN  customer.yml:44  attributes[4].sources: 'country' is nullable: false but not fed by billing, web. ...
WARN  customer.yml:58  matching.blocking[0].parts[1].attr: blocking key 'ln_year' uses nullable 'birth_date' which is not fed by billing, web: ...
dry-run: nothing written
```

Points à retenir :

- `@1` place la colonne en **première** position de la liste `sources` de l'attribut
  (= première priorité). Sans `@`, la colonne est ajoutée **en dernier** (`@first`, `@last` et
  tout entier ≥ 1 sont acceptés).
- Les commentaires, l'ordre des clés, les sauts de ligne et l'encodage (BOM, CRLF) du fichier
  sont conservés : seules des lignes sont insérées. Le diff est donc relisible en revue de code.
- Le résultat est **validé avant écriture** ; s'il est invalide, rien n'est écrit
  (`NOT WRITTEN`). L'écriture est atomique (fichier temporaire puis remplacement).
- Les lignes `WARN` affichées sont uniquement celles **nouvelles** par rapport à la config
  d'origine : ici, `billing` ne fournit ni `country` ni `birth_date`, c'est attendu.

### Étape 2 : écrire pour de bon

Relancer la même commande sans `--dry-run`. Elle refuse si l'identifiant de source existe déjà,
si un attribut est inconnu (avec suggestion « Did you mean ... ? »), si un attribut est mappé
deux fois, ou si la position dépasse le nombre de sources de l'attribut + 1.

### Étape 3 : valider et relire en français

```text
mdm validate configs/example/customer.yml --strict   # --strict : les warnings donnent le code 1
mdm explain  configs/example/customer.yml --attribute email
```

`explain` décrit en clair ce que fait la config pour un attribut (normalisation, ordre des
sources, stratégie, contrôles, rôle dans le rapprochement) ; `--lang en` pour l'anglais.

### Étape 4 : profiler la source réelle contre le mapping (`check-source`)

```text
mdm check-source configs/example/customer.yml --source billing
```

Exemple de rapport (jeu de test de 100 lignes) :

```text
Source crm (parquet data/crm.parquet, snapshot full)
Verdict : WARN
Rows    : 100
Key     : customer_id — 0.0% null, 98 distinct, 2 duplicate rows
Revision: updated_at (TIMESTAMP) — 0.0% null, 2024-01-01 00:00:00 → 2024-04-09 00:00:00

Attributes (rank = priority of this source for the attribute):
  attribute   column (rank)    type/native     null   cast fail  distinct (estimated)  checks
  last_name   LASTNAME (1/2)   string/VARCHAR  10.0%  -          3→2                   max_length:5 reject_record
  email       EMAIL (1/2)      string/VARCHAR  0.0%   -          97→91                 regex:6 null_value
  birth_date  BIRTHDATE (1/2)  date/VARCHAR    8.0%   4          -                     -
Rows rejected by reject_record checks: 5

Findings:
  WARN     key 'customer_id': 2 rows share their key with another row (98 distinct keys for 100 rows); ...
  WARN     last_name: check max_length fails on 5 rows (5.0%) → reject_record: these rows produce no delta
  WARN     email: check regex fails on 6 rows (6.0%) → null_value: the value is treated as null
  WARN     birth_date (BIRTHDATE): 4 of 92 non-null values (4.3%) cannot be read as date
```

Comment le lire :

| Élément | Signification | Que faire |
|---|---|---|
| **Verdict** | `OK`, `WARN` ou `BLOCKING` (code 1) | BLOCKING : ne pas lancer de run |
| Colonne manquante | `column 'LASTNAM' not found. Did you mean 'LASTNAME'?` | corriger le YAML (colonne, clé ou révision) |
| Clé | taux de nuls, nombre de clés distinctes, lignes en doublon | une clé nulle ne peut pas être ingérée ; des doublons en `full` : seule la plus grande `revision` (sinon la dernière ligne) est gardée, les autres vont en `reject` |
| Révision | type, taux de nuls, min → max | texte : ordre alphabétique (sûr seulement en ISO-8601 ou zéros à gauche) ; type non ordonnable : BLOCKING |
| `rank` | position de cette source dans la liste de l'attribut (1/3 = prioritaire) | vérifier que c'est voulu |
| `null` | part de valeurs nulles de la colonne | comparer à `nullable` (§4.2) |
| `cast fail` | valeurs non nulles illisibles dans le type déclaré (`int`, `decimal`, `date`, `timestamp`, `bool`) | ≥ 50 % : BLOCKING (mauvaise colonne probable) ; sinon WARN |
| `distinct a→b` | valeurs distinctes avant → après normalisation | mesure l'effet de `normalize` (3→2 : deux variantes fusionnent) |
| `checks` | par contrôle : nombre de lignes en échec et `on_fail` | voir §4.3 ; `reject_record` ≥ 50 % : BLOCKING |
| Rows rejected | lignes rejetées par au moins un contrôle `reject_record` | volume qui ne produira aucun delta |

Les colonnes de la source qui ne sont pas dans le mapping ne sont pas lues. `--sample N` limite
le profil aux N premières lignes (utile pour un premier contact avec une grosse table ; le
rapport l'indique).

### Étape 5 : lancer un run

Une fois `validate` sans erreur et `check-source` à `OK` (ou à un `WARN` compris et accepté), la
config est prête pour une voie (`python -m engine.lanes.<voie> ingest ...`, cf. contrat §6).

## 2. Créer un nouveau référentiel

```text
mdm new-referential customer_b --out configs/customer_b      # --force pour écraser
```

Écrit `customer_b.yml` (valide, commenté, zones `CHANGE_ME` à compléter) et `mdm.schema.json`.
La première ligne du YAML référence ce schéma : les éditeurs compatibles (VS Code + extension
YAML) proposent alors l'auto-complétion et signalent les erreurs en direct. Pour régénérer le
schéma seul : `mdm schema --out mdm.schema.json` (sans `--out`, sur stdout).

## 3. Référence des commandes

| Commande | Rôle | Options notables |
|---|---|---|
| `validate CONFIG` | erreurs Pydantic réécrites avec **chemin YAML et ligne**, puis lint | `--strict`, `--json` |
| `explain CONFIG` | la config en langage clair | `--attribute NOM`, `--lang fr\|en` |
| `schema` | JSON Schema de la config | `--out FICHIER` |
| `add-source CONFIG` | ajoute source + mappings sans toucher au reste du fichier | `--source-id --kind --location --key` (requis), `--revision`, `--dsn-env`, `--snapshot`, `--full-snapshot-min-ratio`, `--map ATTR=COL[@POS]` (≥ 1), `--dry-run` |
| `check-source CONFIG` | profil d'une source réelle contre le mapping | `--source` (requis), `--sample N`, `--json` |
| `new-referential NOM` | squelette de référentiel | `--out` (requis), `--force` |

Exemple d'erreur de `validate` (avec suggestion) :

```text
ERROR bad.yml:20  attributes[0].normalize[1]: invalid value 'lowr' (allowed: trim, lower, upper, strip_accents, collapse_spaces, digits_only, alnum_only). Did you mean 'lower'?
INVALID: 1 error(s)
```

La validation est faite en passes : corriger les erreurs affichées puis relancer, d'autres
(références croisées) peuvent apparaître.

### Lint (avertissements de `validate`)

Ce que Pydantic ne peut pas dire : la config est valide mais probablement mal pensée.

| Code | Niveau | Signification |
|---|---|---|
| `nonnull-partial-coverage` | warning | `nullable: false` mais une source ne fournit pas l'attribut : ses enregistrements n'ont aucune valeur ; si toutes les valeurs sont nulles, golden **bloqué** |
| `maybe-most-recent` | warning | attribut « qui bouge » (email, adresse, téléphone...) en `priority` : une vieille valeur de la source prioritaire bat toujours les récentes |
| `null-on-nonnullable` | warning | `on_fail: null_value` sur un attribut non nullable : peut bloquer le golden |
| `in-set-not-normalized` | warning | valeurs de `in_set` qui ne survivent pas à la normalisation (ex. `FR` avec `lower`) : jamais satisfaites |
| `source-few-attributes` | warning | source qui alimente moins de 2 attributs |
| `source-no-revision` | warning | source sans colonne de révision (doublons départagés par l'ordre des lignes ; `most_recent` impossible) |
| `comparator-single-source` | warning | comparateur sur un attribut fourni par une seule source : ignoré pour les autres |
| `all-comparators-nullable` | warning | tous les attributs comparés sont nullables : certains couples n'auront aucune décision |
| `blocking-sparse` / `blocking-all-nullable` | warning | clé de blocage sur un attribut absent de certaines sources / toutes les clés dépendent d'un attribut nullable |
| `blocking-nullable`, `ratio-ignored`, `many-attributes` | info | à connaître (mesurer le taux de nuls avec `check-source`, etc.) |

## 4. Listes fermées et sémantique

Tout ce qui suit est une **liste fermée** : une valeur hors liste est refusée par `validate`.
En ajouter une demande du code moteur (§6).

### 4.1 Listes

| Domaine | Valeurs |
|---|---|
| Types d'attribut (`type`) | `string` (défaut), `int`, `decimal`, `date`, `timestamp`, `bool` |
| Sources (`kind`) | `postgres` (`location: schema.table`, `dsn_env` obligatoire), `parquet` (chemin ou glob, `dsn_env` interdit) |
| Snapshot | `full` (défaut ; une clé absente est retirée **seulement si** le lot passe `full_snapshot_min_ratio`, défaut 0,9), `delta` |
| Normaliseurs (`normalize`, **string seulement**, appliqués dans l'ordre, sans doublon) | `trim` (espace, tab, LF, CR en début/fin), `lower`, `upper`, `strip_accents` (`é`→`e`), `collapse_spaces` (toute suite d'espaces → un espace), `digits_only` (garde 0-9), `alnum_only` (garde lettres et chiffres Unicode) |
| Contrôles (`rule`) | `regex` (string), `in_set` (liste non vide), `min` / `max` (int, decimal), `max_length` (string) |
| Action en échec (`on_fail`, défaut `reject_record`) | `reject_record`, `null_value`, `warn` |
| Stratégies de survie | `priority` (défaut), `most_recent` (exige `revision` sur chaque source de l'attribut) |
| Comparateurs | `exact` (tous types), `levenshtein` (string), `date_year` (date, timestamp) |
| Transformations de clé de blocage | `exact`, `prefix` (+ `n` de 1 à 32, string), `year` (date, timestamp) |

Noms (`referential`, `attribute`, `source`, clé de blocage) : `^[a-z][a-z0-9_]{0,62}$`. Noms
réservés pour un attribut : `golden_id`, `staging_id`, `status`, `stale`, et tout nom finissant
par `__src`.

### 4.2 Survie et `nullable`

- Pour chaque attribut d'un staging (cluster de nœuds sources), on parcourt les sources **dans
  l'ordre de la liste `sources` de l'attribut** ; la première valeur non nulle valide gagne
  (`priority`).
- `most_recent` : la valeur du nœud à la `revision` la plus récente gagne ; l'ordre de la
  liste n'a alors pas d'effet. Égalité : plus grande `revision`, puis `(source_id, source_key)`
  croissant.
- Toutes les sources nulles : l'attribut est publié **nul** si `nullable: true` ; sinon le golden
  est **`blocked`** (non publié, ancienne version conservée et marquée périmée, anomalie visible).
  `nullable: false` n'est donc pas un simple « type » : c'est une exigence de publication. À
  réserver aux attributs que **toutes** les sources fournissent (lint `nonnull-partial-coverage`).
- Une valeur vide après normalisation (ex. `digits_only` sur `abc` → chaîne vide) n'est **pas**
  nulle : elle compte comme valeur. `check-source` la signale en `INFO` (« empty strings »).

### 4.3 Contrôles de qualité

- Ils s'appliquent à l'ingestion, **après normalisation**, et uniquement aux valeurs non nulles.
  `in_set` compare donc les valeurs normalisées ; `max_length` mesure la longueur normalisée.
- `reject_record` : le nœud n'émet aucun delta, la dernière version valide reste active,
  l'erreur est tracée dans `reject`. Il ne déclenche aucune réconciliation.
- `null_value` : la valeur est traitée comme nulle (une autre source peut alors gagner).
- `warn` : valeur conservée, anomalie seulement tracée.

### 4.4 Delta et idempotence (rappel)

Clé = `(source_id, source_key)`. Nouvelle clé : `I` ; `row_hash` différent : `U` ; identique :
rien (rejouer un lot ne change rien). Doublon de clé dans un lot : plus grande `revision`, puis
dernière ligne ; les autres en `reject`. Détail : contrat §2.

## 5. Ce que `check-source` mesure (et ne mesure pas)

- **Une seule instruction SQL** de type agrégat renvoie **une ligne de compteurs** : aucune
  donnée source n'est rapatriée ni écrite. PostgreSQL : session en lecture seule. Au-delà de
  1 500 agrégats (DuckDB) ou 1 600 (PostgreSQL, plafond de 1 664 colonnes de sortie), plusieurs
  instructions sont nécessaires.
- **Parquet via DuckDB**, **PostgreSQL** via `psycopg` (DSN dans la variable d'environnement
  `dsn_env`, jamais dans la config).
- **`distinct a→b` est exact sur PostgreSQL et estimé (HLL, ordre de 2 %) sur DuckDB** (indiqué
  « estimated » dans le rapport, `distinct_approximate` en JSON). Un `count(DISTINCT)` exact par
  attribut sur DuckDB dépasse 2,7 Go dès 400 agrégats, même sur 500 lignes. L'unicité de la clé,
  elle, est **exacte** sur les deux moteurs.
- Mémoire DuckDB plafonnée à 3 Go (variable `MDM_CHECK_SOURCE_MEMORY`, ex. `6GB`) ; au-delà,
  débordement sur disque dans un dossier temporaire supprimé à la fin.
- Le profil reproduit les normaliseurs, la lecture typée et les contrôles **en SQL** ; ce n'est
  pas le moteur d'ingestion. Écarts possibles :
  - `regex` : DuckDB utilise RE2 (recherche partielle, pas de lookahead), PostgreSQL le POSIX
    `~`. Une expression non supportée par le moteur de la source est **ignorée avec un WARN**
    (« check skipped (not measured) »), jamais fatale. Préférer des expressions simples
    (classes, ancrages, quantificateurs).
  - `strip_accents` : table de translittération identique sur PostgreSQL (`translate`), natif
    `strip_accents` sur DuckDB ; `alnum_only` : Unicode sur DuckDB, `[:alnum:]` de la locale
    sur PostgreSQL.
  - Lecture typée : `TRY_CAST` (DuckDB) ; PostgreSQL ≥ 16 `pg_input_is_valid`, sinon une
    expression régulière approximative.
- Comportement du moteur sur une valeur **illisible** dans son type (`cast fail`) : non précisé
  par le contrat ; `check-source` la compte et la signale, c'est tout.
- Un attribut `bool` accepte une colonne booléenne, entière ou texte lisible comme booléen.

### Performance mesurée (une exécution, machine partagée)

| Source | Lignes × attributs | Durée | RSS max |
|---|---|---|---|
| Parquet synthétique (`engine.core.synth`), 4 fichiers | 190 k × 172 | 15 s | 1,0 Go |
| Parquet synthétique, 21 fichiers (0,94 Go) | 1,02 M × 200 | 86 s | 1,8 Go |

Avant correction, le même profil à 190 k × 172 prenait 586 s et 15 Go : un `count(DISTINCT clé)`
exact mélangé aux autres agrégats fait basculer DuckDB sur un plan « distinct » pour toute la
requête. L'unicité de la clé est désormais calculée dans une sous-requête de la **même
instruction**. Chiffres indicatifs : machine de 32 threads, d'autres traitements tournaient en
parallèle ; PostgreSQL n'a été mesuré qu'à 2 000 lignes × 200 attributs (test d'intégration).

## 6. Ce qui demande du code (honnête)

Aucun de ces points ne se règle par la config ; tous demandent un développement dans le moteur,
puis la mise à jour des listes fermées et du schéma :

- **Un nouveau normaliseur** (ex. formatage téléphone, soundex, suppression de ponctuation
  ciblée, transcodage), **un nouveau type de contrôle** (ex. longueur minimale, format IBAN,
  contrôle de clé Luhn), **un nouveau comparateur** (ex. Jaro-Winkler, phonétique, distance
  numérique) ou **une nouvelle transformation de blocage**. Les trois voies doivent garder une
  sémantique identique (contrat §3).
- **Un type de source autre que `postgres` et `parquet`** : CSV, JSON, API, fichiers déposés
  sur SFTP, S3/Azure, autre SGBD. Contournement : exporter vers Parquet ou exposer une vue
  PostgreSQL.
- **Attribut calculé** : concaténer, découper, extraire une partie d'une colonne, conversion de
  code (lookup, table de correspondance), jointure entre tables sources. Contournement :
  une vue SQL en amont dans la source.
- **Normalisation différente selon la source** : `normalize` est défini par attribut, pas par
  couple (source, attribut).
- **Contrôle inter-attributs** (ex. date de fin ≥ date de début) ou **inter-lignes**.
- **Stratégie de survie autre** que `priority` / `most_recent` (ex. « la plus fréquente », « la
  plus complète », règle par attribut selon un autre attribut).
- **Attribut multivalué** ou imbriqué : un attribut = une valeur scalaire.
- **Comparateurs ou blocage inter-attributs** non couverts (ex. comparer prénom/nom croisés).
- **Dépôt de secrets** : les DSN sont lus dans des variables d'environnement ; la gestion des
  secrets (coffre, rotation) n'est pas dans cet outillage.
- **Ce que `check-source` ne fait pas** : il ne simule ni la réconciliation, ni le taux de
  fusion, ni l'impact sur les golden existants ; il ne détecte pas qu'une colonne bien typée est
  « la mauvaise » sauf indices statistiques (clé quasi unique, taux de nuls ou d'échecs ≥ 50 %).

## 7. Dépannage

| Message | Cause probable | Action |
|---|---|---|
| `INVALID: n error(s)` avec `fichier:ligne chemin: ...` | config invalide | aller à la ligne indiquée ; suivre « allowed » et « Did you mean » |
| `source 'x' is not mapped by any attribute` | source déclarée sans mapping | ajouter `{source, column}` à au moins un attribut (ou utiliser `add-source`) |
| `unknown source 'crn' (known: crm, erp)` | faute de frappe dans `--source` | utiliser la suggestion |
| `most_recent needs a revision column on <source>` | stratégie `most_recent` sur une source sans `revision` | renseigner `revision` de la source |
| `comparator weights must sum to 1` | poids des comparateurs | ajuster pour que la somme fasse 1 |
| `dsn_env is required for postgres` / `is not allowed for parquet` | mauvais couple kind / dsn_env | corriger la source |
| `cannot read source: ...` (BLOCKING) | fichier/glob sans correspondance, table introuvable ou non lisible, connexion impossible | vérifier `location` ; pour Parquet, un glob doit correspondre à au moins un fichier |
| `environment variable X is not set` | `dsn_env` pointe vers une variable absente | l'exporter avant de lancer (`$env:X = "..."`) |
| `column 'c' not found. Did you mean ...?` | colonne mal orthographiée ou casse différente (PostgreSQL et Parquet distinguent la casse) | corriger `column`, `key` ou `revision` |
| `column 'c' is DATE, which cannot be read as int` | type déclaré incompatible avec le type natif | corriger `type` ou la colonne |
| `... does not look like a key` | moins de 50 % de clés distinctes en `full` | mauvaise colonne `key` |
| `regex ... is not supported by DuckDB/PostgreSQL; check skipped` | syntaxe non portable | simplifier l'expression ; le contrôle n'est alors pas mesuré |
| `profiling query failed: ... Out of Memory` | agrégats trop volumineux | relancer avec `MDM_CHECK_SOURCE_MEMORY=6GB`, ou `--sample` |
| `add-source` : `NOT WRITTEN: the result would be invalid` | l'ajout produirait une config invalide | lire les erreurs affichées ; rien n'a été modifié |
| Accents illisibles dans la console | page de code Windows | la sortie redirigée est en UTF-8 ; en console : `chcp 65001` |

## 8. Tests de l'outillage

```powershell
.\.venv\Scripts\python.exe -m pytest tests/cli -q                       # sans Docker
$env:MDM_TEST_DOCKER = "1"                                               # + PostgreSQL éphémère
.\.venv\Scripts\python.exe -m pytest tests/cli/test_cli_check_source_pg.py -q
```

Le test PostgreSQL démarre un conteneur `mdm2-cli-pg-<id>` limité à 3 Go de mémoire et le
supprime en fin de module ; il compare le rapport PostgreSQL au rapport Parquet des mêmes
données et vérifie qu'un profil à 200 attributs tient dans une seule requête.
