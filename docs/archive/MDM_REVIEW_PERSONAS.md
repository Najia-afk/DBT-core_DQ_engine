# Relecture par personas et decisions du PO

Date : 2026-11-12. Relectures de la specification et du backlog, sans execution des moteurs.
Les avis ci-dessous sont des risques de conception, pas des bogues observes en production.

## Persona expert MDM / data steward

| Priorite | Constat | Suite |
|---|---|---|
| Bloquant | Les champs de matching, normalisations, seuils et contradictions ne sont pas fixes pour chaque type. | La normalisation fait partie d'une chaine DQ configurable ; definir les regles de demo et leur oracle avec le PO avant le matching automatique. |
| Bloquant | Une fusion ou scission ne specifie pas encore toutes les regles de conservation d'ID et de redirection des liens. | Ne pas faire de fusion/scission silencieuse ; definir et tester le parcours d'arbitrage avant implementation. |
| Bloquant | La preuve de completude d'un snapshot autoritatif et la conduite en cas de panne source manquent. | Definir le manifeste et tester incremental, incomplet, indisponible, complet vide et retour avant retraite par absence. |
| Bloquant | La cle du lien courant ne represente pas distinctement des episodes d'emploi ou de residence. | Conserver l'historique et les incoherences en console ; faire trancher la granularite metier des episodes avant de figer une cle. |
| Eleve | L'ordre des revisions, dates tardives et contenus contradictoires n'est pas precis. | Versionner les donnees et refuser/mettre en revue les conflits de revision ; definir et tester le traitement exact. |
| Eleve | Le retrait d'un doublon intra-source est ambigu : noeud source ou appartenance ? | Le PO demande un arbitrage humain quand un vrai doublon ne peut etre corrige a la source. Ne pas retirer automatiquement la source. |
| Eleve | Les cas de conflits de survie et l'usage d'une ancienne valeur sans support actif ne sont pas specifies. | Definir par contrat les regles automatiques, les cas en revue et le devenir de l'ancienne valeur. |
| Eleve | L'autorite d'arbitrage sur les relations n'est pas attribuee. | Preciser les droits du steward source et du/des masters concernes ; tester les refus. |
| Moyen | Decisions, rejets et audit ne sont pas enumeres explicitement dans l'oracle inter-voies. | Les inclure et tester la detection d'une mutation pour chaque dimension. |

## Persona architecte d'entreprise / benchmark

| Priorite | Constat | Suite |
|---|---|---|
| Eleve | Publier entites avant les relations peut rendre visible un cycle incoherent. | Construire les etapes dans une version de travail et n'activer qu'une livraison coherente ; preciser la garantie par voie, sans transaction distribuee supposee. |
| Eleve | Le trajet PostgreSQL → DuckDB/dbt → PostgreSQL n'est pas encore prouve. | Ajouter un test reel de faisabilite, versions et transferts, avant de considerer S3 realisable. |
| Eleve | Le raw precharge de B et l'ODBC de A biaisent le temps total si les frontieres ne sont pas alignees. | Mesurer separement acquisition, calcul et total depuis le meme etat ; declarer caches et ressources. |
| Eleve | L'environnement Docker cible, les quotas et l'application des limites ne sont pas encore mesures. | Faire une matrice de versions et relever limites effectives, capacite machine et espace avant comparaison. |
| Moyen | Reponses HTTP de soumission et garde-fou de l'identite demo restent a preciser. | Aligner les codes de soumission sur la spec moteur ; demo locale exclusivement, refusee hors configuration explicite. |

## Clarifications du PO recues

- DQ de bout en bout, dans l'esprit d'un MDM « fabric-like » : profiling, normalisation,
  controles, regles de survie, exhaustivite et coherence. La DQ n'est pas un pretraitement
  unique ; ses decisions sont gouvernees par les contrats de donnees.
- Le POC doit aussi proposer des regles floues et des scores configurables avec une zone de
  revue humaine. Le PO accepte provisoirement : candidat unique >= 0,95 sans contradiction,
  rapprochement automatique ; 0,80 a < 0,95 ou candidats multiples, revue. Les poids,
  contradictions et seuils doivent etre calibres avant production.
- Suivre la distribution des scores par cycle et son evolution (drift), avec denominateurs,
  historique et avertissement si les populations ou versions ne sont pas comparables.
- Un objet de bonne qualite au cycle precedent doit encore remonter si sa source devient
  invalide. Le blocage porte sur la datasource ou le noeud, pas systematiquement tout le cycle ;
  les champs obligatoires golden conditionnent la publication. Le blocage partiel isole l'objet
  ou la relation invalide ; une anomalie mineure remonte sans forcement tout bloquer. Le moteur
  doit se degrader proprement avec fallback selon contrat, sans masquer les valeurs vieillies.
- Un doublon reel non corrigeable a la source doit etre soumis au steward de cette source
  dans la console, pas resolu par retraite automatique.
- Les personnes, lieux et liens evoluent ; une personne peut rester liee dans la source a un
  lieu devenu invalide. L'incoherence doit etre visible et traitable en console, sans suppression.
- Le steward visualise et arbitre ; il corrige les donnees dans le referentiel source, jamais
  via une surcouche ou une edition de valeur source dans le MDM.
- Conserver les episodes des relations et l'historique PostgreSQL des deltas d'attributs et
  relations, y compris anciennes et nouvelles valeurs. Le detail de la cle d'episode reste ouvert.
- L'IHM de stewardship est **commune aux trois approches**. Elle utilise des services et une
  API communs, avec indication de la voie/run, sans partager les decisions de calcul entre voies.
- L'aide par agent/skill/MCP est souhaitee ; distinguer la conception de l'integration, la
  preparation de l'API et les fonctionnalites effectivement livrees.

Ces relectures ne certifient pas l'implementation. Les decisions encore ouvertes doivent etre
tranchees avant la recette semantique et le benchmark, pas remplacees par des valeurs inventees.
