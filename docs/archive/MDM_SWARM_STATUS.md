# Etat du swarm MDM

Date : 2026-11-12. Point de reprise factuel, a mettre a jour au fil des sprints.
Backlog : [MDM_SPRINTS.md](MDM_SPRINTS.md). Relecture :
[MDM_REVIEW_PERSONAS.md](MDM_REVIEW_PERSONAS.md).

## Resultats du demarrage

| Zone | Etat | Preuve / limite |
|---|---|---|
| Relectures | Effectuees par personas MDM et architecte/benchmark | Risques et decisions PO dans la revue ; pas une certification du produit. |
| Donnees de demonstration | Socle livre | Jeu local synthetique a trois sources et six cycles ; verite terrain separee. Pas encore un jeu volumineux ni Febrl. |
| Donnees de volume | Generateur partiel | [Generateur JSONL](../examples/scale/README.md), distinct du format des voies ; aucune execution MDM a 10 millions et aucun adaptateur livre. |
| Docker | Socle de faisabilite livre | PostgreSQL, ODBC et AGE/openCypher testes ; trajet PostgreSQL → DuckDB/dbt-duckdb → PostgreSQL teste. Aucun benchmark des trois approches. |
| API | Tranche partielle | `GET /health` et endpoints de consultation ETL en mode demonstration ; aucune authentification ni autorisation. |
| Contrats et DQ | Partiels | Modeles Pydantic et primitives de profiling, normalisation et validation ; aucune politique de publication et aucun fallback executé. |
| Ingestion | Partielle | Lecture de snapshots et observations DQ ; les voies de reconciliation ont leurs propres ingestions. Pas d'Airbyte. |
| Historique | Partiel | Une tranche PostgreSQL independante n'est pas branchee aux cycles ; les voies ont leurs propres historiques, de couverture variable et incomplets. |
| Scoring flou | Reference experimentale | Formule, seuils et metriques dans [examples/matching](../examples/matching/README.md) ; les voies disposent de leurs propres regles et decisions, non alignees. |
| Drift des scores | Tranche partielle | Classes, denominateurs et comparaison des versions en memoire ; pas de dashboard, d'alertes ni de suivi durable. |
| Oracle de cycles | Partiel | [Jeu et comparateur](../examples/oracle/README.md) : attendus multicycle independants ; accepte les projections ETL et ELT, non conforme pour le graphe ; couverture incomplete des regles. |
| Banc de comparaison | Socle partiel | [Runner et analyse d'integration](../engine/benchmark/README.md) : temps mural et empreintes, sans mesure CPU, memoire, reseau ni integration des voies. Une sonde de candidats montre un ecart ETL/ELT ; leurs adaptateurs de replay et l'isolation des attendus ne sont pas encore comparables. |
| Voie ETL | Tranche partielle | [ETL / ODBC](../deploy/etl/README.md), 10 tests sur PostgreSQL isole ; l'oracle partiel accepte ses sept projections. Les regles completes, la charge et l'equivalence restent a prouver. |
| Voie ELT | Tranche partielle | [PostgreSQL / DuckDB / dbt](../engine/elt/README.md), 7 tests sur PostgreSQL isole ; l'oracle partiel accepte ses sept projections. Extraction en memoire, regles completes et charge non validees. |
| Moteur Python/dbt de la specification | Non conforme | La voie ELT execute dbt par subprocessus bloquant et traite les cycles sequentiellement ; pas de file persistante, workers, admission concurrente ou reprise conformes a [la specification](SPECIFICATIONS.md). |
| Voie graphe | Tranche partielle | [AGE/openCypher](../engine/graph/README.md), 62 tests sur AGE isole ; l'oracle echoue en C0 sur une contradiction de dates et C1 du jeu commun bloque la publication apres conservation des anomalies. Aucune equivalence revendiquee. |
| Trois moteurs complets | Non livres | Aucun resultat d'equivalence de bout en bout n'est annonce. |
| Note de cadrage et maquette | Illustrations livrees | [Note pour le comite](MDM_COMITE_ARCHITECTURE.html) et [maquette interactive](MDM_CONSOLE_DEMO.html), fictive et non connectee ; aucune mesure de performance MDM. |
| Consultation ETL locale | Tranche partielle | [API et pages en lecture](../src/web/README.md), 10 tests sur PostgreSQL alimente par l'ETL ; mode demonstration explicite, sans authentification ni autorisation, pas d'arbitrage ni autres voies. |
| IHM commune et MCP | Non livres | Aucune interface de stewardship complete, aucun chatbot ni integration MCP. |

S0 est avance, mais la recette integrale et les statuts de chaque ticket restent a confirmer.
Les trois voies ont ete developpees en parallele, mais les composants partiels ne constituent
pas une recette complete. S2 a S7 ne sont pas termines ; l'oracle commun reste partiel et
les regles metier et effets DQ doivent encore etre alignes et testes.

## Validations observees

- Image applicative construite et `GET /health` teste localement, puis conteneur arrete.
- 174 tests du generateur, de l'API, des contrats, de la qualite, du drift, de l'ingestion et
  de la reference de matching ont reussi dans le conteneur Python 3.12 ; Ruff est passe.
- Historique : 10 tests sur PostgreSQL 16.4 jetable, avec replay, conflit et rollback ; pas
  de preuve de branchement a une publication MDM.
- Suites transverses : 196 tests Python 3.12. Tests isoles supplementaires : ETL 10, ELT 7,
  graphe 62, consultation ETL 10. Ces comptes couvrent des perimetres differents et ne sont
  ni un score de conformite ni une mesure comparative de performance.
- Publication : aucun secret reel identifie par un premier balayage limite ; les identifiants
  de laboratoire sont publics et non utilisables en production. Ce balayage n'est pas une
  garantie d'absence de donnees sensibles. Les fichiers non suivis restent a examiner avant commit.
- Docker : commandes et versions des sondes dans [deploy/README.md](../deploy/README.md).
  Les deux volumes du laboratoire ont ete conserves ; ils ne sont pas des donnees de production.

## Avant la suite

1. Stabiliser la formule commune de scoring flou, les contradictions, l'oracle des cycles et
   les cas de doublons, retirement, source invalide et episodes. Les seuils de demonstration
   ont ete indiques par le PO ; les hypotheses restantes ne valent pas calibration de production.
2. Relier DQ et historique aux etats et publications, dont les cas de panne et de fallback.
3. Completer les trois voies separees, aligner leurs regles et prouver leurs resultats et leurs couts.
4. Construire la console unique et ses droits, ensuite produire le papier avec mesures reelles.
