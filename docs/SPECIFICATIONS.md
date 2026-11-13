# Moteur de pipelines data - Specifications v0.4

Statut : proposition a valider, pas une certification de capacite.
Reference unique issue de la v0.3 : moteur configurable, Airflow obligatoire, dbt pour le SQL,
concurrence et protection des ressources des le premier livrable.

## 1. Besoin et perimetre

### 1.1 Objectif

Executer des pipelines BI, reprises de donnees, transferts backend et controles qualite sans
reecrire un ETL en Python ni dans des TaskGroups Airflow pour chaque demande.

Un pipeline assemble des briques reutilisables : extraire, transformer, controler, charger ou copier.
Il peut faire de l'ETL, de l'ELT, un simple transfert ou plusieurs passages entre zones. Selon le
traitement, les volumes et les garanties demandees, les donnees peuvent aller directement vers leur
destination, passer par des fichiers ou transiter par une zone de travail PostgreSQL avant
transformation et publication. Le passage par une zone intermediaire n'est pas obligatoire.

Le pipeline declare ses sources, sorties, controles, transformations, contraintes et parcours
autorises. Le moteur peut choisir entre ces parcours si le mode automatique est demande et si
le choix preserve le resultat et les garanties du pipeline. Un nouveau pipeline utilise un dossier
de configuration et, si necessaire, des modeles SQL dbt.
Une nouvelle famille de connecteurs ou de regles peut demander du code dans le moteur ou son
package dbt ; un nouveau client ne doit pas en demander.

### 1.2 Contraintes

- Deploiement on-prem ; serveur de depart de 16 Gio de RAM.
- Airflow obligatoire en production pour les calendriers et les dependances entre pipelines.
- Pas d'equipe Python specialisee necessaire a la creation des pipelines courants.
- PostgreSQL et DuckDB pour le calcul ; PostgreSQL, Parquet et CSV pour les sorties.
- Airbyte pour les extractions compatibles avec ses connecteurs.
- Qualite facultative, standard ou specifique ; possibilite de bloquer, avertir ou rejeter des lignes.
- Donnees, droits, fichiers et connexions separes par tenant.
- API utilisable sans interface ; site HTMX ensuite pour configurer et suivre.
- Base de dimensionnement : 15 a 25 utilisateurs ; objectif de plusieurs centaines de jobs par jour.

### 1.3 Enveloppe a mesurer

Cas maximal annonce : un flux de 200 millions de lignes, des projets de modelisation jusqu'a
300 tables, certaines jusqu'a 200 colonnes. Cela ne signifie pas 200 millions de lignes dans
chacune des 300 tables ni 50 traitements lourds simultanes sur 16 Gio.

La capacite depend aussi des types, largeurs, cardinalites, jointures, tris, copies, index, disque
et delais attendus. A titre illustratif, 200 millions de lignes de 1 Ko font environ 200 Go avant
compression, hors intermediaires et index. Un objectif de duree doit etre accompagne de ces mesures.

Le moteur doit absorber des demandes en attente sans tout demarrer. Il peut refuser temporairement
une admission et ne garantit pas qu'un traitement arbitraire tiendra dans la machine.

### 1.4 Hors perimetre initial

- Streaming, calcul distribue et autoscaling d'infrastructure.
- Editeur graphique d'ETL, moteur de scripts Python utilisateurs ou DAG libre de code utilisateur.
- Oracle, SQL Server et connecteur natif de virtualisation.
- Remplacement exhaustif d'un produit de qualite : reprise des fonctions effectivement utilisees.
- Haute disponibilite multi-hotes sans stockage et exploitation adaptes.

## 2. Architecture et repartition des roles

```text
Airflow : calendrier, dependances entre pipelines, soumission et suivi des runs
    |
    | DAGs minces ; operateur generique qui soumet/attend un pipeline
    v
API -------------------- pg-meta : file, runs, tentatives, quotas, audit
    |                                ^
    v                                |
Moteur/workers : validation du pipeline, choix du parcours, execution et reprise des briques
    |
    +-- Airbyte : synchro vers destination declaree
    +-- DuckDB : calcul local, Parquet, CSV, acces PostgreSQL
    +-- pg-zone : raw / staging / gold / quarantaine facultatifs
    +-- PostgreSQL cible ou livraison de fichiers

HTMX -> API ; catalogue externe <- artefacts et lignage
```

| Composant | Responsabilite |
|---|---|
| API FastAPI | Validation, versions, preparation du plan d'execution, soumission, annulation, suivi et autorisation |
| Workers Python | Reservation des ressources, execution bornee, supervision et reprise |
| Package dbt `dq_core` | Regles SQL reutilisables, conventions et resultats DQ |
| `pg-meta` | Etat durable et coordination, jamais transport des donnees metier |
| `pg-zone` | Zone data facultative, schemas et roles distincts de `pg-meta` |
| Airflow | Planification, backfills, dependances entre pipelines, soumission/suivi des runs, alertes |
| Airbyte | Extraction et chargement brut, lorsque le connecteur convient |
| Keycloak | Identite OIDC on-prem et comptes de service |

Une image applicative avec points d'entree API et worker ; pas de broker supplementaire au depart.
`pg-meta` et `pg-zone` sont des bases et roles distincts ; leur partage eventuel d'une instance
PostgreSQL doit etre pris en compte dans les budgets et l'isolation de charge.

### 2.1 Airflow orchestre les pipelines, le moteur orchestre leurs briques

La definition de reference d'un pipeline reside dans la configuration versionnee du tenant, avec
ses contrats et ses modeles dbt eventuels. Elle est independante du mode de lancement : la meme
definition peut etre executee directement par le moteur (CLI/API) ou soumise par Airflow. Il n'y a
pas de logique ETL/ELT dupliquee dans les DAGs.

Les DAGs Airflow declarent le calendrier, les parametres d'execution et les dependances entre
pipelines. Un operateur generique soumet un run identifie par tenant, pipeline et version, puis
attend son resultat de facon deferrable. Une fabrique de DAG commune ou des DAGs fins peuvent
instancier cet operateur ; ils ne contiennent pas les etapes metier, les requetes SQL, les regles
DQ, les copies de donnees ni la logique de reprise d'une etape.

Le moteur valide la definition et execute le graphe interne de briques typees derive des datasets.
Il choisit, si demande, un parcours parmi les parcours autorises, puis gere dependances internes,
branches, parallelisme, admission des ressources, tentatives et reprise. Airflow ne materialise
pas une tache par brique et n'est pas informe des details internes du parcours ; il observe le
statut global du run. Le graphe reste limite aux types et contrats supportes par le moteur, pas
un framework de taches arbitraires.

Airflow gere les erreurs de transport de soumission ; le moteur gere les tentatives des briques
admises. La soumission est idempotente et utilise une cle stable ; les retries Airflow ne relancent
pas un effet de bord sous une nouvelle identite. Les pools Airflow peuvent limiter les soumissions,
sans remplacer les budgets et verrous du moteur.

## 3. Contrat d'execution

### 3.1 Concepts

| Concept | Definition |
|---|---|
| Tenant | Espace de donnees, droits, connexions et quotas |
| Pipeline | Dossier versionne contenant definition, contrats et SQL dbt |
| Dataset | Donnees logiques avec contrat et emplacement resolu pour une execution |
| Step (brique) | Operation typee avec entrees, sorties, ressources et politique d'echec |
| Run | Execution d'une version immuable avec parametres et intervalle de donnees |
| Tentative | Execution d'un step avec identifiant, proprietaire et numero de generation |
| Livraison | Ensemble de sorties declarees comme disponibles pour les consommateurs |

### 3.2 Fonctions de base

| Type | Fonction | Implementation attendue |
|---|---|---|
| `extract` | Acquerir une source ou attendre une synchro | Airbyte, lecture PostgreSQL ou fichiers |
| `transform` | Produire des datasets par SQL | CLI dbt en sous-processus : `run` ou `build` explicite |
| `quality` | Evaluer les controles et decider de la suite | Tests dbt et collecte de resultats |
| `load` | Publier vers une destination | COPY PostgreSQL, fichiers Parquet/CSV ; SFTP au lot 2 |
| `passthrough` | Copier sans logique metier | Reutilise les lecteurs/chargeurs, pas un second moteur de copie |

Les entrees et sorties sont explicites et determinent les dependances entre briques. Les briques
independantes peuvent s'executer en parallele sous reserve des budgets et verrous. Changer de moteur
ne transfere pas automatiquement les donnees. Une sortie DuckDB devient une entree PostgreSQL par
un `load` declare, pas par convention. Une sortie non disponible empeche les briques dependantes
de demarrer.

Chaque sortie porte son emplacement, schema, version ou intervalle, statut de publication et
producteur. Les glob patterns de fichiers sont resolus en manifeste stable avant traitement.
Un fichier incomplet ou une table encore en chargement n'est pas une entree disponible.

### 3.3 Airbyte : destination configurable

La connexion Airbyte referencee peut charger :
- un PostgreSQL cible directement, pour EL ou ELT ;
- `pg-zone` avant transformation ;
- une destination fichiers compatible avec le connecteur retenu.

La destination doit exister dans la configuration Airbyte ; le moteur ne pretend pas rediriger
arbitrairement une connexion a chaque run. Le step enregistre l'identifiant du job distant,
attend son resultat et decrit les datasets effectivement produits.

Une synchro deja lancee est suivie plutot que relancee. Si la soumission distante a une issue
inconnue, le moteur reconcilie avec Airbyte ; a defaut de preuve, intervention requise.
Une connexion sans isolation des executions est serialisee. Les formats bruts Airbyte sont
normalises par des modeles explicites lorsque necessaire.

Airbyte ecrit dans une zone brute non publiee si la DQ doit conditionner la visibilite finale.
Une extraction directe dans une table deja exposee n'offre pas de barriere DQ retroactive.

### 3.4 Choix du parcours d'execution

Une definition peut imposer un parcours (`direct`, `fichiers`, `pg_staging`) ou declarer plusieurs
parcours autorises et demander `auto`. Ce choix mixte est borne par la configuration publiee :
le moteur ne cree pas de destination Airbyte, ne change pas une connexion distante et ne reordonne
pas les dependances metier. Il choisit uniquement parmi les strategies effectivement configurees
et compatibles avec les contrats, les regles DQ, les connecteurs et les garanties de publication.

En mode `auto`, les criteres declares (volume estime, moteurs disponibles, ressources et couts
de transfert) servent a choisir une strategie valide. Une DQ bloquante impose une sortie non
publiee jusqu'a validation ; elle interdit donc un parcours qui exposerait la cible avant le
controle. Le moteur enregistre pour chaque run le parcours retenu, les criteres et les versions
utilisees. Le plan est fixe avant que les briques du run deviennent executables. Le meme choix
s'applique que le run soit lance par CLI/API ou par Airflow ; Airflow ne choisit pas les briques
internes et n'a pas a reconstruire le graphe du parcours. Une indisponibilite ne provoque pas de
bascule silencieuse vers une autre strategie. Le moteur attend, echoue explicitement ou demande
une decision selon la politique du pipeline.

### 3.5 Snapshot et reproductibilite

Un run fixe version de config, SQL, versions dbt/adaptateurs/packages et parametres.
Il fixe aussi le parcours retenu et, selon la source, un manifeste de fichiers, un snapshot ou un watermark.
Un watermark n'avance qu'apres la publication correspondante.
La configuration seule ne fige pas une source mutable : la garantie de relecture est declaree
par pipeline, avec les limites du connecteur.

## 4. Configuration

### 4.1 Organisation

```text
pipelines-config/
  platform.json
  tenants/
    client_alpha/
      tenant.json
      pipelines/
        sales_repair/
          pipeline.json
          contracts/
          dbt/
            dbt_project.yml
            models/
            tests/
```

JSON pour la configuration du moteur, valide par un schema commun expose par l'API.
Les fichiers natifs dbt restent en YAML et SQL. Les secrets sont references, jamais stockes dans Git.
Le moteur fournit les fonctions et conventions communes ; les pipelines fournissent les choix metier.
La configuration est organisee par tenant puis par pipeline et constitue la definition unique
utilisee par le moteur, la CLI et les DAGs Airflow. La CLI lance directement cette configuration ;
en production, un DAG Airflow reference le tenant/pipeline/version et appelle l'operateur generique.

Chaque publication cree une version immuable comprenant les contrats et le projet dbt.
Les dependances sont epinglees et preparees a la publication, pas telechargees a chaque execution.
Les modifications Git directes passent par la meme validation avant activation.
Une tentative de publication sur une version depassee renvoie `409`.

### 4.2 Exemple de pipeline : corrections tardives de ventes

Exemple synthetique : lecture de Parquet, rejet des identifiants absents, transfert des lignes
valides vers PostgreSQL, recalcul SQL puis publication d'une partition.
Les chemins `run://` designent des emplacements resolus par le moteur, pas un protocole externe.

```json
{
  "pipeline": "sales_repair",
  "datasets": {
    "incoming": {
      "location": "connection://landing/sales/batch_042/manifest.json",
      "contract": "contracts/sales.json"
    },
    "raw": { "location": "run://raw/sales.parquet" },
    "valid": { "location": "run://valid/sales.parquet" },
    "rejects": { "location": "run://rejects/sales.parquet" },
    "staged": { "location": "connection://pg_work/run_schema/sales" },
    "corrected": { "location": "connection://pg_work/run_schema/fct_sales" },
    "published": { "location": "connection://warehouse/gold/fct_sales" }
  },
  "steps": [
    {
      "id": "read",
      "type": "extract",
      "inputs": ["incoming"],
      "outputs": ["raw"],
      "resources": "small"
    },
    {
      "id": "check",
      "type": "quality",
      "engine": "duckdb",
      "inputs": ["raw"],
      "outputs": ["valid", "rejects"],
      "resources": "medium",
      "rules": [
        { "id": "sale_id_required", "test": "not_null", "column": "sale_id", "scope": "row" },
        { "id": "volume_guard", "test": "row_count_min", "value": 1, "scope": "dataset" }
      ],
      "gate": {
        "row_errors": "filter",
        "dataset_errors": "stop",
        "max_rejected_pct": 1
      }
    },
    {
      "id": "stage",
      "type": "load",
      "inputs": ["valid"],
      "outputs": ["staged"],
      "resources": "small",
      "mode": "replace_work"
    },
    {
      "id": "rebuild",
      "type": "transform",
      "engine": "postgres",
      "inputs": ["staged"],
      "outputs": ["corrected"],
      "resources": "small",
      "dbt": { "command": "run", "select": "tag:sales_repair", "threads": 1 }
    },
    {
      "id": "publish",
      "type": "load",
      "inputs": ["corrected"],
      "outputs": ["published"],
      "resources": "small",
      "mode": "replace_partition",
      "partition": { "column": "business_date", "parameter": "business_date" },
      "write_group": "warehouse_sales"
    }
  ],
  "parameters": { "business_date": { "type": "date", "required": true } },
  "retry": { "max_attempts": 2, "only_if": "safe" },
  "timeout_minutes": 180
}
```

Les mappings dbt lient `staged` a une source generee et `corrected` au modele selectionne.
Ils font partie de la validation ; un nom de dataset ne suffit pas a recabler un modele SQL.
Le contrat precise types, cles, nullabilite, donnees sensibles et politique d'evolution.

### 4.3 Validation

A la publication : schema JSON, identifiants, references, types des parametres, graphe sans cycle,
dependances derivees des datasets, compatibilite des emplacements, sorties DQ, parcours autorises,
regles de choix automatique, strategie d'ecriture et ressources.
Les mappings SQL et groupes d'ecriture doivent couvrir les relations cibles des modeles dbt.
Une cible dynamique non declaree est refusee.

L'ordre des declarations JSON n'est pas un ordre d'execution : les dependances resultent des
datasets produits et consommes. Les entrees sans dependance commune peuvent s'executer en parallele.
La validation refuse les cycles, sorties ambigues et parcours qui ne respectent pas les portes DQ
ou les contraintes de publication.

`dbt parse` valide le projet et ses references, pas l'execution SQL. Compilation et tests
d'integration sur la cible completent cette validation.
Pas de garantie de validite d'une source distante entre publication et execution.

Les contrats generent des sources/tests dbt lorsque pertinent. La macro `generate_schema_name`
calcule les noms ; le provisionnement cree les schemas et attribue les droits.
`on_schema_change` concerne les modeles incrementaux dbt compatibles, pas toute l'ingestion :
les changements de source ont leur politique propre.

Ajout de colonne permis seulement si le contrat le prevoit ; changement de type, suppression ou
renommage : blocage par defaut et migration explicite. Pas de deduction automatique de cles
metier ou de transformation en etoile depuis le seul schema technique.

## 5. Concurrence et protection du serveur

### 5.1 Profil de depart 16 Gio

Le profil concerne la machine d'execution complete. Airflow, Airbyte, Keycloak et le catalogue
sont supposes heberges ailleurs ; s'ils partagent la machine, leurs consommations sont deduites
avant de fixer le budget des jobs.

Exemple de repartition initiale a mesurer : 4 Gio pour OS et services de controle, 4 Gio pour
PostgreSQL local, 6 Gio au maximum pour les sous-processus jobs et 2 Gio de marge.
Ce n'est pas une garantie d'absence d'OOM : les limites effectives et mesures de RSS font foi.

```json
{
  "host": {
    "ram_gib": 16,
    "job_memory_budget_mib": 6144,
    "max_running_steps": 2,
    "max_queued_runs": 2000,
    "min_free_disk_gib": 30,
    "max_reserved_work_gib": 300
  },
  "profiles": {
    "small": { "process_memory_mib": 2048, "duckdb_memory_mib": 1024, "threads": 1 },
    "medium": { "process_memory_mib": 4096, "duckdb_memory_mib": 2048, "threads": 1 },
    "large": { "process_memory_mib": 6144, "duckdb_memory_mib": 4096, "threads": 2 }
  },
  "tenant_defaults": { "max_running_steps": 1, "max_queued_runs": 200 },
  "timeouts": { "db_lock_seconds": 30, "step_minutes": 120 },
  "retention_days": { "logs": 30, "artifacts": 90, "rejects": 14, "audit": 365 }
}
```

Exemple : un `medium` et un `small` peuvent coexister ; un `large` reserve seul le budget jobs.
Les 300 Gio de reservation disque ne sont valables que si le volume existe et garde la marge
libre requise. Chaque step declare aussi une estimation de travail disque et ses connexions.
Un besoin superieur a la capacite configuree est refuse explicitement, pas mis en attente infinie.

`memory_limit` DuckDB n'est pas une limite du RSS total. Les processus sont bornes par des
limites OS/conteneurs effectives ; leurs descendants sont inclus. Les threads dbt et DuckDB,
le CPU et le disque temporaire sont bornes. Une limite depassee devient une erreur explicite.
Les reservations ne remplacent ni les limites ni la surveillance.

Pour PostgreSQL, un petit client peut declencher une grosse requete serveur : limites de connexions,
parallelisme, `work_mem`, fichiers temporaires et timeout sont aussi definis cote base.
Les budgets locaux ne protegent pas a eux seuls une base distante.

### 5.2 File et ordonnancement

`pg-meta` porte une file persistante. Les workers reclament un travail par transaction courte
avec `FOR UPDATE SKIP LOCKED` ; aucune transaction de file ne reste ouverte pendant le calcul.
Claim, reservation des ressources et propriete de tentative sont enregistres atomiquement.
Ordre d'acquisition des ressources fixe ; tout ou rien, sans conserver un jeton en attendant les autres.

Un step demarre seulement si sont disponibles :
- budget memoire et slots sur son hote ;
- estimation disque et marge libre ;
- quota du tenant ;
- capacite des connexions et cibles concernees ;
- droit d'ecriture exclusif sur le groupe cible si necessaire.

Les budgets sont par hote et par cible, pas multiplies a chaque ajout de worker.
L'ordonnancement utilise une rotation entre tenants, avec vieillissement des demandes.
Un gros job eligible reserve progressivement sa priorite d'acces, pas des jetons partiels :
le scheduler cesse temporairement de lancer les petits jobs qui empechent son admission.
L'equite et l'absence de famine sont testees, pas deduites d'un simple tri par priorite.

Les profils et plafonds sont configures par l'operateur ; un tenant ne peut pas relever ses limites.
Les modifications de capacite affectent les nouveaux departs ; on ne tue pas un job pour appliquer
une baisse de quota. Si le budget devient depasse, aucun nouveau depart.
Ajouter des replicas sur le meme hote augmente les consommateurs, pas les ressources disponibles.
L'extension multi-hotes demande un budget par hote et des donnees de reprise accessibles.

### 5.3 Admission API

`POST /runs` persiste la demande et renvoie `202` sans executer le traitement dans l'API.
La cle d'idempotence est unique par tenant ; elle est liee au contenu de la demande.
Meme cle et meme contenu : run existant (`200`). Meme cle et contenu different : `409`.
La recherche d'une demande existante precede le controle de plafond de file.

`429` avec `Retry-After` si plafond atteint ; `503` si service indisponible. Une erreur reseau
peut laisser l'admission incertaine : le client re-soumet la meme cle pour retrouver le resultat.
L'API ne peut pas garantir une reponse lorsque la base ou le reseau est indisponible.

L'operateur Airflow utilise tenant, DAG, task et run Airflow pour construire une cle stable ;
pas seulement `logical_date`. L'intervalle de donnees reste un champ distinct.
La soumission est retentee avec delai, jitter et echeance explicite, sans occuper un slot de
calcul Airflow en attente deferrable. Une echeance expiree signale une admission non confirmee,
pas une preuve que rien n'a ete cree. Une nouvelle verification utilise la meme cle.

## 6. Ecritures, reprise et etats

### 6.1 Etat observable

Runs : `queued`, `running`, `waiting_resource`, `succeeded`, `succeeded_with_warnings`,
`failed`, `cancel_requested`, `cancelled`, `needs_attention`.
Steps/tentatives exposent aussi `retry_wait`, proprietaire, heartbeat, echeance et motif d'attente.
Une erreur, un rejet ou une reprise manuelle n'est jamais converti en succes silencieux.

### 6.2 Verrous et collisions

Un verrou de file ne protege pas les donnees. Un verrou limite au triplet tenant/pipeline/date
ne suffit pas non plus : deux dates ou pipelines peuvent modifier les memes tables.

Les sorties mutables declarent un groupe d'ecriture canonique, partage entre pipelines visant
les memes relations physiques. Par defaut, une seule tentative ecrit dans ce groupe, toutes dates
confondues. Un groupe peut couvrir une etoile entiere. Les sources partagees en cours de synchro
sont aussi protegees si leur lecture ne fournit pas un snapshot stable.

Le moteur empeche ses propres collisions ; les transactions PostgreSQL peuvent encore attendre
des lecteurs ou ecrivains externes. `lock_timeout` et `statement_timeout` bornent cette attente.
Un timeout est trace et ne declenche une relance que si la politique de l'operation l'autorise.
On promet une attente bornee et diagnostiquable, pas l'absence universelle de locks.

### 6.3 Heartbeat et tentatives perdues

Une expiration de heartbeat rend une tentative suspecte, pas automatiquement terminee.
Chaque tentative a une generation ; un ancien proprietaire ne peut plus valider ses transitions
d'etat dans `pg-meta`. Cela ne suffit pas a bloquer ses effets dans une cible externe.

Sur l'hote initial, le superviseur confirme l'arret de l'arbre de processus et la fin des
sessions/transactions associees avant de liberer une ecriture pour reprise.
Les sessions PostgreSQL sont identifiees par tentative ; les operations externes par job distant.
Si l'arret ou l'issue d'une publication ne peut pas etre confirme, le groupe reste bloque
et le run passe en `needs_attention`.

Pas de liberation aveugle d'un verrou d'ecriture a expiration du heartbeat.
Une future execution multi-hotes devra conserver cette garantie par un mecanisme de fencing
effectif cote publication ou une reconciliation, pas seulement par un lease dans `pg-meta`.

### 6.4 Publication et idempotence

Livraison au moins une fois au niveau des tentatives ; effets idempotents seulement pour les
strategies qui l'assurent. La cle HTTP deduplique une demande, pas les donnees ecrites.

| Strategie | Condition de reprise |
|---|---|
| Remplacement d'une table de travail | Sortie privee de tentative, recreable |
| Remplacement de partition | Perimetre explicite, transaction delete+insert et serialisation |
| Merge | Cles declarees, source deterministe et dedupliquee, transaction |
| Append | Identifiant de lot et deduplication durable cote cible, sinon reprise automatique interdite |
| Export fichiers | Repertoire de livraison versionne, manifeste final et verification des checksums |
| Airbyte / SFTP | Reconciliation des jobs/fichiers et capacites du connecteur ; issue inconnue bloquante |

Une publication PostgreSQL peut produire les donnees de travail puis publier dans une transaction
adaptee a la cible. Le journal de livraison dans cette meme base permet de reconnaitre un commit
reussi dont l'accuse a ete perdu. Il n'y a pas de transaction distribuee avec `pg-meta`.

Pour plusieurs tables, le contrat choisit :
- publication par table, avec coherence partielle acceptee explicitement ;
- publication groupee par une procedure transactionnelle validee ;
- version de livraison et bascule de lecture par les consommateurs.

Pas de swap generique suppose sans impact sur vues, contraintes et privileges.
Les modeles dbt qui ecrivent directement dans une cible exposee n'acquierent pas automatiquement
une atomicite a l'echelle du projet.

Un renommage fichier n'est atomique que si le stockage le garantit. Une livraison multifichiers
devient visible par manifeste final ; les consommateurs doivent respecter ce protocole.
PostgreSQL et SFTP peuvent avoir des resultats differents : les sorties reussies sont conservees
et la livraison incomplete est signalee, sans pretendu rollback global.

### 6.5 Arret, annulation et reexecution

Arret propre : plus de nouveaux departs, delai de grace, puis terminaison controlee et reconciliation.
Une annulation n'est confirmee qu'apres arret des operations ; les sorties deja publiees restent
declarees comme telles. Pas de remise en file avant verification des effets precedents.

Reprise depuis un step si ses entrees et sorties amont sont encore valides.
Sinon, reexecution depuis un point reproductible ou refus motive.
`dbt retry` est possible seulement avec les artefacts et conditions requis par la version retenue ;
ce n'est pas le mecanisme general de reprise du moteur.

## 7. Qualite des donnees

### 7.1 Profils et catalogue

- `none` : aucun controle DQ, sans desactiver les validations techniques d'entrees.
- `standard` : regles selectionnees depuis un contrat explicite, pas unicite de chaque colonne.
- `custom` : regles catalogue parametrees et tests SQL metier dbt.

Priorite aux tests dbt existants : non-null, unicite, valeurs acceptees, relations.
Fraicheur via la commande dbt adaptee, pas supposee incluse dans un `dbt test`.
Packages tiers seulement apres verification de maintenance et compatibilite avec les versions
DuckDB/PostgreSQL retenues. Le package commun ne duplique pas les fonctionnalites disponibles.

Une brique `quality` peut controler des fichiers Parquet/CSV avec DuckDB ou des tables PostgreSQL
(raw, staging ou modele) avec SQL/dbt. Le pipeline declare le point de controle et les effets
attendus. Pour une DQ bloquante avant transformation/publication, les donnees restent non publiees ;
les lignes valides et rejetees sont des sorties explicites, et seules les sorties valides alimentent
les briques suivantes. Le mode automatique ne change de moteur DQ que si les regles gardent la meme
semantique ; sinon le moteur impose le choix explicite ou refuse la publication du pipeline.

`transform` utilise `dbt run` si les controles sont dans un step dedie.
`dbt build` est possible si le pipeline lui confie aussi les tests ; ceux-ci ne sont pas rejoues
systematiquement dans un step `quality` identique.

### 7.2 Decisions

| Portee de regle | Exemples | Decision possible |
|---|---|---|
| Ligne | Champ absent, format invalide, plage interdite | Rejet, arret ou avertissement |
| Dataset | Livraison vide, fraicheur, ecart de volume | Arret ou avertissement |
| Entre datasets | Integrite referentielle, rapprochement de totaux | Arret ou avertissement ; rejet seulement avec mapping explicite |

Une regle `unique` ne choisit pas quelle occurrence conserver. La deduplication est un modele SQL
avec une politique deterministe declaree, par exemple date d'effet puis identifiant source.

Le mode `filter` exige une identification fiable des lignes et un mapping de rejets pour chaque
regle filtrante. Les seules sorties generiques de `store_failures` ne le garantissent pas.
Si cela manque, publication du pipeline refusee ; pas de filtrage approximatif.

Le pourcentage rejete porte sur les lignes distinctes rejetees / lignes controlees, pas le nombre
total de violations. Une entree vide suit une regle explicite. Les seuils bloquants priment
sur `continue` ; le resultat final et les compteurs restent visibles.
Le downstream consomme la sortie valide declaree, jamais implicitement la source brute.

### 7.3 Resultats et cout

Resultats par run, dataset et regle : statut, severite, lignes controlees, violations, rejets,
horodatage et perimetre controle. Les erreurs SQL sont distinctes des echecs de regles.
Les resultats dbt natifs et les metriques supplementaires du package sont collectes explicitement.

`store_failures` sert au diagnostic ; les rejets historiques sont materialises avec run et tentative.
Le stockage integral, les echantillons et la retention sont configures ; un quota depasse ne
transforme pas un controle incomplet en controle reussi.

Optimisation : regrouper certains controles simples par agregation. Pas de promesse de scan
unique pour unicite, relations et comparaisons historiques. Benchmark par famille de regles.
L'incremental est autorise avec perimetre explicite ; une unicite globale ou une relation globale
ne se reduit pas sans preuve au controle de la partition du jour.
Profilage a la demande, echantillonne ou complet, avec cout et perimetre affiches.

## 8. Isolation, stockage et exploitation

### 8.1 Donnees et identites

Schemas par tenant et couche : `<tenant>__raw`, `__stg`, `__gold`, `__quarantine`.
Schemas prives de run/tentative pour les travaux ; pas de collision entre deux pipelines du tenant.
References partagees seulement en lecture et declarees.

Roles PostgreSQL au moindre privilege ; identites d'execution limitees au tenant.
Les privileges de provisionnement sont separes de ceux d'execution. SQL publie par des auteurs
autorises ; ce n'est pas une sandbox de code hostile.
Fichiers et secrets montes au perimetre du job, chemins contraints aux connexions autorisees.
Un meme worker disposant de tous les secrets n'est pas une frontiere d'isolation forte :
les exigences plus fortes imposent pools d'execution ou conteneurs dedies.

Keycloak OIDC on-prem, puis federation possible vers une identite interne.
Validation signature, issuer, audience et expiration ; comptes de service via OAuth2.
Les roles et autorisations par tenant sont controles par l'API. Pas de mots de passe locaux moteur.
Secrets hors Git, TLS, chiffrement des volumes selon politique d'exploitation.
Les logs ne contiennent pas volontairement de lignes metier ; les erreurs SQL et artefacts sont
aussi controles car ils peuvent contenir des valeurs ou du SQL sensible.

### 8.2 Stockage

Premier deploiement mono-hote : volumes persistants partages entre API et workers, repertoires
par tenant/run/tentative. Les fichiers DuckDB actifs ne sont jamais partages entre processus
ecrivains. Les donnees d'echange durables sont des tables ou fichiers avec manifeste.

Le worker n'est pas suppose sans etat : son calcul temporaire est local. La reprise utilise des
checkpoints durables ou recommence depuis une entree reproductible.
Les donnees nécessaires a une reprise ne sont pas purgees avant la fin de leur fenetre de reprise.
La purge quotidienne exclut les tentatives actives, references retenues et investigations en cours.
Sur manque disque : suspension des nouveaux departs concernes, alerte et erreur explicite si
un travail en cours ne peut plus continuer ; jamais suppression d'entrees actives pour faire place.

Sauvegarde de `pg-meta`, configs et donnees non reproductibles. `pg-zone` n'est reconstructible
que si les sources et leur historique le permettent. RPO/RTO et restauration testes avant prod.
La perte de l'hote reste une panne de service dans le profil initial.

### 8.3 Audit et lignage

Audit des publications, lancements, annulations, reprises et acces sensibles, sans revendication
de conformite automatique a une reglementation.
Retention configurable avec valeurs d'exemple, a valider selon les contraintes reelles.

Conserver les artefacts dbt effectivement generes, logs JSON et informations de version.
`catalog.json` demande une generation dediee lorsqu'il est requis.
Les etapes hors dbt exposent leurs datasets d'entree/sortie et leur execution.
OpenLineage/OpenMetadata restent des integrations a verifier sur versions epinglees :
un endpoint maison ne garantit pas leur ingestion native.

## 9. API et interface HTMX

API minimale presente des le socle :
- validation et publication de config, controle optimiste `base_version` ;
- preparation/consultation du plan immuable, soumission avec `Idempotency-Key` et consultation par cle ;
- execution idempotente du graphe interne d'un pipeline et consultation de son statut/resultat ;
- liste paginee des runs, details et motifs d'attente ;
- annulation et reprise validee avec `from_step` ;
- logs bornes/pagines puis SSE, resultats DQ et artefacts autorises ;
- capacite, file, workers, `/health` et `/metrics`.

Base `/api/v1`. Roles : admin plateforme, engineer tenant, steward qualite, viewer, service.
L'etat attendu, les effets deja publies et les actions possibles doivent etre visibles.
L'API de publication n'effectue pas une compilation longue dans une transaction de base :
preparation, validation puis activation coherente ; une version incomplete n'est jamais executable.

Interface ulterieure : FastAPI/Jinja2/HTMX, formulaires guides et edition JSON validee.
Ecrans pipelines, versions, runs, logs, qualite et capacite.
Session OIDC securisee et protection CSRF. Aucun acces direct aux bases ou au depot depuis l'UI.
Meme services applicatifs et API ; pas de deuxieme logique metier.
Le module UI peut etre deploye separement sans imposer une nouvelle pile technique.

Git reste la reference versionnee ; publication UI cree un commit et une version activee.
L'UI ou la CLI peut lancer un run immediat ; calendriers et dependances entre pipelines restent dans Airflow.
La premiere UI n'est pas un concepteur graphique de workflow.

## 10. Structure d'implementation

```text
engine/
  api/
  config/
  worker/
  steps/
  dbt/
  store/
  lineage/
  ui/
dbt_packages/
  dq_core/
airflow/
deploy/
examples/
tests/
docs/
```

Fonctions et modules simples, adaptateurs types pour les connecteurs, pas de framework de plugins
generaliste. Les lecteurs/chargeurs sont reutilises par extraction, chargement et copie.
Python orchestre des sous-processus et appels I/O ; les millions de lignes restent dans SQL,
COPY et fichiers, pas dans des listes ou boucles Python.
Asyncio aide le controle concurrent, pas la vitesse du calcul SQL.

Tables principales : tenants/grants, versions, runs, steps/tentatives, reservations de ressources,
groupes d'ecriture, sorties/livraisons, artefacts, resultats DQ, audit.
Les transitions autorisees et contraintes d'unicite sont testees en integration PostgreSQL.

## 11. Cas synthetiques difficiles et recette

Les chiffres suivants sont des profils de test, pas des performances deja obtenues.
Chaque resultat publie precise CPU, RAM, SSD, reseau, versions, taille des entrees, duree,
RSS maximal, disque temporaire et impact sur la base.

| Cas | Charge ou incident | Resultat attendu |
|---|---|---|
| Rafale | 500 demandes, 12 tenants, plafond de file abaisse pour provoquer `429` | Clients re-soumettent ; un seul run par cle, aucune demande admise perdue |
| Serveur 16 Gio | Jobs petits et gros, budget jobs 6 Gio | Chevauchement small/medium observe ; large seul ; reservations jamais depassees |
| Famine | Un large attend pendant l'arrivee continue de small | Le large finit par obtenir le budget sous une charge et des durees de jobs bornees |
| Cible commune | Deux pipelines et deux dates modifient la meme table | Aucun chevauchement d'ecritures moteur dans le meme groupe |
| Lock externe | Transaction externe bloque une publication | Attente bornee, motif visible, aucune attente sans echeance |
| Heartbeat perdu | Worker encore vivant ou connexion de controle coupee | Pas de seconde publication tant que l'ancien ecrivain n'est pas neutralise |
| Accuse perdu | Commit cible reussi puis API/worker coupe | Reconciliation du journal cible, pas de double append |
| Rejets multiples | 1 million de lignes ; 12 000 lignes ont deux violations chacune | 12 000 lignes rejetees, 24 000 violations, taux rejete 1,2 % |
| Corrections tardives | 40 millions de ventes, 24 mois, corrections de trois dates anciennes | Recalcul du perimetre declare ; pas de simple filtre sur la date du run |
| Doublons contradictoires | Meme cle, dates d'effet identiques, montants differents | Blocage ou arbitrage deterministe explicite, jamais choix aleatoire |
| Etoile | Projet dbt de 300 tables, jusqu'a 200 colonnes, faits jusqu'a 200 millions de lignes | Schema, coherence et mode de publication verifies ; duree et espace mesures |
| Disque court | Temporaire presque plein pendant jointure/extraction | Pas de nouveau depart dangereux ; echec explicite sans purge de donnees actives |
| Livraison partielle | PostgreSQL publie, SFTP indisponible | PostgreSQL non rejoue inutilement ; sortie SFTP incomplete visible et reprenable |
| Airbyte incertain | Reponse perdue apres lancement de synchro | Suivi/reconciliation, pas de lancement distant aveugle en double |
| Isolation | Deux tenants utilisent les memes noms de tables et fichiers | Donnees et artefacts distincts, acces croise refuse |
| Restauration | Restauration metadata/config et sorties critiques | Runs reconcilies avant reprise, preuves conservees |

Les tests a 200 millions de lignes sont conditionnes a un disque suffisant et a une fenetre
de mesure reservee. Leur preparation fait partie du premier lot ; ils ne sont pas remplaces
par un test de quelques lignes. Aucun SLA de production avant leurs mesures.

## 12. Livraison

### Lot 1 : socle representatif, concurrent des le depart

Configuration versionnee des graphes internes de briques et parcours autorises, CLI/API de lancement,
coordination PostgreSQL, workers, budgets reels 16 Gio, verrous de cible, timeouts, reprise prudente,
isolation, audit minimal et metriques. Fonctions de base PostgreSQL/Parquet/CSV, dbt et DQ ; operateur
Airflow generique qui soumet et suit un pipeline, avec DAGs minces pour calendriers et dependances
entre pipelines.
Integration Airbyte minimale sur une version/connexion pilote, avec suivi du job distant.

Recette : execution d'une meme configuration par CLI/API et Airflow ; plusieurs parcours ETL/ELT
(dont Airbyte direct, fichiers, et staging PostgreSQL avec DQ/dbt/publication), plusieurs pipelines
de tenants distincts en concurrence, branches et briques independantes gerees par le moteur,
ecritures concurrentes serialisees, rafale de demandes, worker perdu, lock externe, limite
memoire/disque et reexecution.
Le jeu de benchmark gros volume est livre avec le socle ; une mesure complete sur le serveur
cible est requise pour annoncer sa capacite. Un seul parcours sequentiel ne constitue pas une recette.

### Lot 2 : durcissement des transferts et des regles

Connecteurs Airbyte supplementaires, SFTP et livraison multifichiers, qualite metier,
profilage et politiques de publication groupee. Reprise progressive de pipelines reels.
Benchmarks par famille DQ et tests de restauration. Pas de bascule generale sans comparaison
des sorties avec les traitements de reference.

### Lot 3 : interface et integrations

HTMX pour creer, publier, lancer et suivre sans shell ; integrations de catalogue verifiees.
Multi-hotes uniquement si les mesures le justifient, avec stockage, budgets et reprise adaptes.
Pas de promesse d'autoscaling sur une machine fixe.

## 13. Revue severe : points de vigilance restants

| Point | Position retenue | Preuve ou decision encore necessaire |
|---|---|---|
| Deux orchestrateurs | Airflow pour calendrier et dependances inter-pipelines ; moteur pour graphe interne, choix de parcours, admission, execution et reprise ; dbt pour SQL | Meme run lance par CLI/API ou operateur Airflow ; aucune logique ETL/ELT dans les DAG |
| Complexite moteur | Graphe declaratif contraint de briques typees, pas de code de tache arbitraire ni de DAG libre | Tests de branches, parallelisme, panne et transitions avant enrichissement fonctionnel |
| 16 Gio | Admission bornee, spill et concurrence selective | Mesures sur machine cible, liste des services coheberges |
| Reprise | Jamais de double ecrivain volontaire ; issue inconnue bloquante | Tests de neutralisation des processus et reconciliation cible |
| DQ filtrante | Seulement avec identification/mapping de lignes | Contrats et tests par regle ; pas de filtrage des erreurs dataset |
| Etoile coherente | Unite de publication declaree | Choix des consommateurs : par table ou version complete |
| SQL personnalisable | Auteurs de confiance, droits limites | Niveau d'isolation attendu entre tenants |
| Stockage | Pas de worker miraculeusement sans etat | SSD disponible, volumes persistants, retention et sauvegarde |
| Configuration | Parametres de fonctionnement, pas substitut aux decisions | Valeurs autorisees, ressources physiques et echeances metier |

Avant mise en production : versions de composants epinglees, volumetrie en octets et intervalle
de chaque pipeline pilote, fenetres de traitement, SSD disponible, sources reproductibles,
RPO/RTO et responsables de reconciliation. Ces informations n'empechent pas le developpement
du socle ; elles conditionnent les garanties que l'on peut annoncer.
