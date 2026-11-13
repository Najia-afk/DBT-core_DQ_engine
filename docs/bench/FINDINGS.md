# Constats du banc et limites de recette

Date : 2026-11-13. Les preuves sont les [JSON des runs](runs/) et
les [comparaisons](equivalence/), pas les projections.

## Résultats négatifs à conserver

- **ELT, 300 000 nœuds synthétiques × 200 attributs** :
  [premier essai](runs/elt-L2.json), arrêt au budget RAM de 28 Go.
  Le pic échantillonné de l'arbre client est de 26 447 Mo ; client + PostgreSQL,
  28 703 Mo. `memory_limit` DuckDB ne borne pas le RSS de dbt.
- [Second essai ELT](runs/elt-L2-retry1.json) : arrêt après 90 minutes
  de `reconcile`, malgré une consommation plus basse (22 949 Mo client + PG).
  La publication du nouveau cycle n'est pas réalisée. Ce résultat interdit de
  déduire une capacité à 1 M × 200 depuis le succès à 100 k × 50.
- Ces essais ont utilisé PostgreSQL en conteneur, mais Python/dbt sur l'hôte.
  Les limites de mémoire des conteneurs ne couvraient donc pas les clients.
  Le banc applique un garde RSS échantillonné et tue l'arbre en dépassement ;
  ce n'est pas une limite OS instantanée.
- La mémoire totale de Windows incluant les autres usages et le cache de la VM
  Docker a atteint 56,74 Go lors du premier essai ELT. Le plafond de travail
  du banc ne doit pas être présenté comme un plafond de consommation de
  l'hôte entier.

## Ce qui est comparable

- [ETL contre ELT, 100 k × 50](equivalence/lanes-L1.json) :
  mêmes projections, y compris les hashes, après les cycles communs.
- [ETL, ELT et graphe contre l'oracle, 2 k × 30](equivalence/reference-2k.json) :
  trois cycles, comparaison des états et des événements. Une égalité avec
  l'oracle n'est pas une mesure de précision/rappel sur une population réelle.
- Les composantes communes à l'oracle et à une voie sont décrites dans leurs
  README. En particulier, le graphe réutilise des fonctions de décision de la
  référence : ce n'est pas une validation totalement indépendante.

## Risques du contrat et du code restant ouverts

| Sujet | Conséquence | Suite requise avant production |
|---|---|---|
| Sérialisation de `row_hash_input` avec séparateurs et jeton nul | Certaines valeurs contenant les caractères de contrôle peuvent rendre l'encodage non injectif ; un changement peut être invisible au delta | Encodage injectif et versionné, tests adversariaux dans les trois voies, migration des hashes et recalcul explicite |
| Égalité de révision sur doublons | La « dernière ligne » dépend de l'ordre de lecture d'une table ou de plusieurs fichiers | Critère stable déclaré, ou rejet de l'ambiguïté |
| Rejets DQ et version valide conservée | Le recalcul à froid sur les sources brutes ne reproduit pas nécessairement le dernier état valide | Comparer l'état `source_record` admissible ; afficher l'anomalie et la fraîcheur |
| Regex et casse Unicode | `re`, RE2 et casse simple ne sont pas universellement équivalents | Sous-ensemble réellement validé à la configuration ou implémentation commune |
| Décimal zéro signé | La voie graphe et l'oracle peuvent produire des hashes différents | Canonicalisation commune et cas de parité explicite |
| Source incrémentale | Pas de catalogue durable des fichiers consommés ni watermark d'extraction généralisé | Manifeste par run et progression atomique après publication |
| Historique initial par attribut | Des dizaines à centaines de millions d'événements à la première publication | Mesurer la rétention, partitionner et envisager un événement de création compact |
| Configuration évolutive | Ajouter un attribut après `init` ne constitue pas une migration de tables | Plan de migration versionné et reprise contrôlée |
| Publication et reprise | Une transaction complète peut devenir longue ; fencing limité à pg-meta | Publication clôturée côté cible et récupération des transactions/processus |
| Graphe | Charge utile hybride, survie et composantes partiellement calculées hors AGE ; lecture non entièrement bornée | Ne pas le présenter comme une voie graphe pure ni comme validé à 1 M × 200 |

## Corrections du banc pendant la recette finale

- Le premier essai du runtime avec les vraies voies a révélé que `init`,
  `export` et `reset` refusaient son argument de supervision `--run-id`.
  Les trois parsers l'acceptent désormais ; 15 tests croisés couvrent le
  contrat CLI et la concurrence ETL/ELT réelle est passée après correction.
- Une voie manquante ou en échec fait échouer le comparateur voie-à-voie ;
  une comparaison vide ne peut plus être annoncée réussie.
- La concurrence n'est pas validée sur une file vide. Succès des runs,
  parallélisme observé, sérialisation, compteurs cohérents et budget mémoire
  doivent tous être vérifiés.
- Le runtime classe désormais un lot d'ingestion bloqué comme avertissement,
  même si la voie sort en code 0 ; il n'annonce plus un succès sans réserve.
- Les projections de capacité ne sont plus affichées lorsqu'une voie échoue
  à une taille supérieure à son dernier succès.

Les gaps ci-dessus restent des gaps : les enregistrer ne les corrige pas.
