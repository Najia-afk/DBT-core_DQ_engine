# Étude MDM maison : ETL, ELT, graphe

Date : 2026-11-13. Conclusion de laboratoire, **pas une certification de production**.
Références : [contrat](MDM_CONTRACT.md), [guide ingénieur](ENGINEER_GUIDE.md),
[mesures et commandes](bench/RESULTS.md), [gaps détaillés](bench/FINDINGS.md).

## 1. Conclusion

La voie **ETL vectorisée + PostgreSQL** est la meilleure candidate dans les
essais disponibles. L'ELT fournit un projet dbt inspectable, mais son chargement
initial large n'a pas passé les essais à 300 k × 200. Le graphe est une
implémentation hybride utile pour matérialiser les liens, pas une preuve
qu'AGE remplace efficacement un moteur colonnaire pour 200 attributs.

L'étude établit une réconciliation incrémentale commune et des limites de
capacité. Elle **ne valide pas encore 50 référentiels de production à 1 M
de nœuds et 200 attributs par jour**. Il manque notamment une recette
répétée sur cette population, son disque d'état durable, les connecteurs
réels, les migrations de configuration et le durcissement opérationnel.

## 2. Le moteur doit faire un delta, pas refaire le MDM chaque jour

```text
sources -> mapping / normalisation / DQ
        -> MERGE par (source_id, source_key), comparaison de row_hash
        -> source_delta : I / U / D admissibles
        -> candidats et décisions affectés
        -> stagings affectés : fusion / scission / memberships
        -> survie des attributs de ces stagings
        -> golden modifiés + provenance + historique
        -> COMMIT de publication
```

- Le hash identique ne déclenche pas de réconciliation.
- Un rejet DQ conserve la dernière version valide et une anomalie visible.
- Les sources `full` restent lues pour détecter les changements et les
  absences ; la transformation aval est incrémentale. Ce n'est pas du CDC.
- Une disparition n'est un retrait que dans un snapshot complet admissible.
- Les priorités et contrôles sont configurés par attribut. La nullabilité
  golden ne se confond pas avec la validité d'un enregistrement source.
- Le golden publié change seulement si la valeur, la provenance ou l'état
  change. Le reconcile est transactionnel ; les lecteurs voient l'ancien
  ou le nouvel état, pas une publication partielle.

Cette logique est testée sur les insertions, mises à jour, retraits, merges,
splits, ambiguïtés, rejets, replay et interruptions. L'équivalence à froid
porte sur le dernier état **source admissible**, pas sur une source brute
qui a perdu ses anciennes versions valides.

## 3. Trois approches, pas trois noms pour le même calcul

| Dimension | ETL | ELT | Graphe |
|---|---|---|---|
| Ingest | Lots Arrow, DQ et normalisation dans Python | Brut puis SQL DuckDB généré, exécuté par dbt | Python/Arrow, payload relationnel |
| Candidats | Index et jointures PostgreSQL | Jointures DuckDB | Sommets de blocking et traversées AGE |
| Scoring | rapidfuzz / Arrow | SQL et UDF Unicode ciblé | Python sur candidats du graphe |
| Composantes | numpy, ensemble affecté | Calcul SQL et propagation des labels | Adjacence AGE, étiquetage Python |
| Survie | Vectorisée par lots de stagings | SQL généré | Python/Arrow |
| Publication | Transaction PostgreSQL | COPY puis transaction PostgreSQL | Transaction PostgreSQL/AGE |
| Coût principal observé | COPY, historique et transaction initiale | Démarrage dbt, calcul large et mémoire hors budget DuckDB | Structure des liens, traversées, payload large |

Le détail du calcul partagé est dans les README des voies. Les seuils sont
des règles de démonstration, pas une calibration métier. Pas de modèle
d'IA, d'embedding ni de service externe dans le moteur de rapprochement.

## 4. Méthode et preuves

- Machine : Windows, 32 threads, environ 64 Go de RAM physique.
- Bases réelles : PostgreSQL 16.4 et AGE 1.6.0/PG16 en Docker.
- Essais de performance séquentiels ; conteneurs et arbres clients mesurés.
- Les essais du banc lancent Python/dbt sur l'hôte. Le déploiement Docker
  intégral est distinct et doit être mesuré séparément.
- Sources synthétiques Parquet : le nombre de nœuds passé au générateur
  représente sa population latente ; le recouvrement entre sources produit
  environ 1,9 ligne source par nœud à trois sources. Le schéma comporte
  200 attributs cibles, mais toutes les sources ne renseignent pas tous les
  attributs. Ce n'est pas un essai de 200 valeurs non nulles par ligne.
- Une seule mesure par taille/configuration : pas de p95 ni d'intervalle de
  confiance. Les caches, le matériel partagé, les largeurs, le nombre de
  candidats et l'historique influencent les résultats.
- Les benchmarks initiaux utilisent `synchronous_commit=off`, `fsync` actif.
  Cela réduit la garantie de durabilité à l'acquittement ; la pile Docker
  de déploiement ne désactive pas ce réglage.

### Équivalence

| Recette | Résultat | Preuve |
|---|---|---|
| Trois voies, 2 k × 30, J0/J1/J2, états et événements | Égaux à la référence, hashes inclus | [JSON](bench/equivalence/reference-2k.json) |
| ETL contre ELT, 100 k × 50, cycles à 1/5/20 % | Projections égales, hashes inclus | [JSON](bench/equivalence/lanes-L1.json) |

Une comparaison commune n'est pas une preuve indépendante lorsque l'oracle
et une voie partagent une fonction. Sans vérité terrain métier annotée,
la précision/rappel et le taux de fausses fusions restent à calibrer.

### Mesures de départ

Temps ingest + reconcile, hors génération des sources et export ; sources
déjà accessibles en Parquet. Valeurs issues des JSON, pas d'un SLA :

| Voie / population | Initial | Quotidien à 5 % | Observation |
|---|---|---|---|
| ETL, 100 k × 50 | 35,8 s | 8,7 s | Environ 190 k lignes source initiales |
| ELT, 100 k × 50 | 74,3 s | 30,8 s | Coût fixe dbt visible sur petit delta |
| ETL, 300 k × 200 | 241,6 s | 36,7 s | Succès, pic client + PG 4,8 Go |
| ETL, 1 M × 200 | 1 136,9 s (19 min) | 255,8 s (4 min 16 s) | Environ 1,9 M lignes source ; pic client + PG 8,1 Go |
| ELT, 300 k × 200 | Non terminé | Non mesuré | Arrêt RAM, puis arrêt à 90 min |

Les résultats du dernier essai au volume cible et de la concurrence figurent
dans la [table détaillée](bench/RESULTS.md). Un essai interrompu n'est ni un
débit exploitable ni un succès partiel du MDM.

Le [run ETL au volume cible](bench/runs/etl-L3-final.json) a été exécuté
avec PostgreSQL plafonné à 8 Go, ingest des sources en séquentiel et lots
de 25 000 lignes. À 1 % de changements, ingest + reconcile prennent
176,4 s. L'état final est de 12,2 Go ; le pic PGDATA (WAL inclus) de
21,2 Go. Le pic Windows entier de 36,3 Go inclut les autres usages et
la VM/cache Docker ; il n'est pas le RSS du travail testé.

### Runtime réel et package Docker

| Voie, trois référentiels de 20 k × 30 | Un slot | Trois slots | Gain mesuré | Sérialisation du même référentiel |
|---|---|---|---|---|
| ETL | 36,08 s | 14,84 s | ×2,43 | Vérifiée |
| ELT | 149,06 s | 55,18 s | ×2,70 | Vérifiée |

Les runs réussissent, les compteurs finaux sont identiques et trois steps
concurrents sont observés. Preuves : [ETL](bench/concurrency/etl-3x20k.json)
et [ELT](bench/concurrency/elt-3x20k.json). Cela ne certifie pas les mêmes
gains sur trois gros référentiels.

La [pile Docker](../deploy/compose.yml), plafonnée à **17 Go au total**
(worker 8 + état 8 + metadata 1), a aussi exécuté un run ETL complet :
génération de 2 k nœuds synthétiques, soumission, ingestion, réconciliation
et 11 exports canoniques. Le package installé contient les modèles dbt ;
l'utilisateur applicatif est non root. Le [smoke reproductible](../deploy/smoke.ps1)
supprime uniquement les volumes de son projet jetable.

Validation du package final dans un conteneur de 4 Go : 741 tests passent,
111 tests nécessitant des bases externes sont ignorés. Recette séparée
du runtime sur PostgreSQL réel : 75 tests passent. Ruff est propre.
Deux avertissements Python sur `fork` depuis un processus multithread
ont été observés dans les tests du générateur : ils ne sont pas supprimés.

## 5. Dimensionnement des 50 référentiels

L'objectif représente **50 M de nœuds et jusqu'à 10 milliards de valeurs
d'attributs par jour**, sans compter les relations, candidats, alternatives,
index et événements historiques.

Une fenêtre de six heures, utilisée ici comme hypothèse de dimensionnement,
exige au minimum 2 315 nœuds/s agrégés. Ce seuil ne suffit pas à certifier
la capacité : il faut distinguer première intégration et petits deltas,
source full et source CDC, nombre de candidats et coût de l'historique.

Le disque peut être plus contraignant que le CPU. Extrapoler les tailles
d'état ETL mesurées conduit à plusieurs centaines de Go pour 50 référentiels,
hors sauvegardes, WAL, rétention et espace temporaire. Le disque disponible
de cette machine ne permet donc pas de conserver et exploiter 50 états
complets pour une recette durable.

Le parallélisme ne donne pas gratuitement trois fois le débit :
il partage le disque, PostgreSQL, le cache et les budgets mémoire. Les
essais de concurrence à petite taille prouvent le fonctionnement de
l'ordonnanceur, pas ce multiplicateur à 1 M × 200.

## 6. Ce que l'ingénieur peut configurer

Le [guide pratique](ENGINEER_GUIDE.md) décrit le parcours :

1. Déclarer la source et sa clé ; fournir le connecteur supporté et le DSN
   hors configuration.
2. Associer ses colonnes aux attributs cibles.
3. Choisir la position dans la liste de sources par attribut : priorité
   de survie ; ou `most_recent` avec une révision.
4. Déclarer types, nullabilité, normaliseurs, checks et leurs effets.
5. Valider, expliquer puis profiler la source avant de soumettre un run.

`add-source --dry-run` montre le diff avant écriture. `check-source`
signale les colonnes absentes, conversions invalides, rejets et doublons.
Le nombre de distincts par attribut est estimé sous DuckDB et explicitement
étiqueté ; l'unicité de la clé est vérifiée exactement.

**Limite importante :** « tout en config » vaut pour les connecteurs et
primitives supportés. Une nouvelle famille de connecteur, une nouvelle
fonction métier ou une migration de schéma peut encore demander du code.
Le validateur ne transforme pas un ajout d'attribut en migration déployable.

## 7. Matrice exigences / livraison / gap

| Exigence | Livraison constatée | Gap avant exploitation |
|---|---|---|
| Merge puis réconciliation seulement sur delta admissible | Trois voies et tests des écritures inchangées | Encodage hash injectif/versionné à durcir |
| Config commune, mapping et survie par attribut | Pydantic, CLI, exemples, SQL dbt généré | Évolution/migration de schéma et versionnement du déploiement |
| Lots et mémoire maîtrisée | ETL ingest en flux, worker surveillé, limites Docker | RSS ELT non borné par DuckDB ; graphe non entièrement en lots |
| Async et admission | File pg-meta, claims, workers asyncio, budgets, quotas par référentiel | Quotas tenant, bornes de connexions/cibles et disque par step |
| Reprise et replay | Idempotence de soumission, génération de tentative, rollback/replay, tests kill | Fencing effectif côté cible, sessions externes, reprise multi-hôtes |
| Publication cohérente | Transaction PostgreSQL et export repeatable-read | Transaction initiale longue ; watermark d'extraction généralisé |
| Sources full/delta | PG et Parquet, barrière de complétude, retraits logiques | Airbyte/CDC et manifeste des fichiers consommés |
| dbt Core | Projet généré et artefacts SQL, DAG et subprocess supervisé | Budget de mémoire global et optimisation du plan large |
| MDM de relations métier | Memberships et liens staging/golden | Épisodes et réconciliation générique de relations métier non livrés |
| Stewardship | Revue persistée, provenance et anomalies | Arbitrages autorisés, UI et application versionnée des décisions |
| Sécurité/exploitation | DSN hors YAML, utilisateur Docker non root, ports DB non publiés | OIDC, isolation tenant, RBAC, purge et audit d'exploitation |
| Airflow obligatoire en production | Runtime utilisable par CLI | Opérateur deferrable et DAGs de soumission non livrés |
| 50 × 1 M × 200 quotidien | Mesures partielles, échecs enregistrés | Recette réelle répétée, stockage durable et contrat de fraîcheur |

## 8. Décision technique et prochaines barrières

**Décision conditionnelle :** continuer avec ETL/Arrow + PostgreSQL comme
socle opérationnel candidat ; garder l'ELT comme alternative instrumentée
tant que ses plans larges ne passent pas le budget ; réserver le graphe
aux liens et parcours dont l'utilité métier et le coût sont démontrés.

Ordre avant une mise en production :

1. Corriger et versionner la sérialisation des hashes, les doublons non
   ordonnés et la parité Unicode/décimale ; migration/replay contrôlé.
2. Fixer le vrai profil : lignes par source, largeur, changement quotidien,
   candidats, seuils, relations, rétention et fraîcheur.
3. Mettre en place les migrations, le manifeste des sources et les contrôles
   de publication/fencing.
4. Recette du volume cible répétée au moins trois fois, avec durabilité
   requise, mesures froid/chaud explicitement définies et concurrence réelle.
5. Déployer le stockage dimensionné, sauvegarde/restauration, permissions,
   supervision puis l'intégration Airflow et les connecteurs réels.

Ce dépôt fournit un laboratoire exécutable et une étude des gaps.
Il n'est pas présenté comme un MDM de production achevé.
