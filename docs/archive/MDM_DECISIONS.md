# Decisions et recherches du laboratoire MDM

Statut : recherche initiale, choix de production non valides. Reference :
[specification MDM](MDM_RECONCILIATION_SPEC.md).

## Articles Microsoft identifies

- [GraphRAG and PostgreSQL Integration in Docker with Cypher Query and AI Agents](https://techcommunity.microsoft.com/blog/azure-ai-foundry-blog/graphrag-and-postgresql-integration-in-docker-with-cypher-query-and-ai-agents/4420623),
  Helen Zeng, 4 juin 2025 : exemple GraphRAG en Docker utilisant PostgreSQL et Apache AGE pour
  les requetes Cypher, et une recherche vectorielle avec ou sans pgvector. L'exemple traite aussi
  d'agents et de services Azure ; il n'est pas une implementation MDM ni une mesure comparative
  de reconciliation. C'est une piste possible pour l'article demande, a confirmer par le PO.
- [Combining pgvector and Apache AGE - knowledge graph & semantic intelligence in a single engine](https://techcommunity.microsoft.com/blog/adforpostgresql/combining-pgvector-and-apache-age---knowledge-graph--semantic-intelligence-in-a-/4508781)
  : combinaison de recherche de similarite et de parcours graphe dans PostgreSQL ; l'article
  explique notamment la construction de liens de similarite. Il ne valide pas les seuils
  ni les resultats d'un rapprochement d'identites MDM.

Ces articles ne constituent pas une garantie qu'AGE et pgvector fonctionnent ensemble dans
l'image retenue pour le laboratoire. Tester les versions et les requetes reelles avant de
documenter une telle capacite.

## Etat des choix

| Sujet | Etat | Preuve / suite |
|---|---|---|
| Graphe du laboratoire | AGE + PostgreSQL retenu pour la demonstration | S0-02 a execute openCypher avec AGE 1.6.0/PostgreSQL 16.10 ; transactions, reprise et modele complet restent a prouver en S4. Neo4j uniquement sur decision explicite si bloque. |
| Recherche vectorielle | Optionnelle, non necessaire aux premieres decisions | Ne pas presenter une similarite comme identite ; mesure de qualite et modele local a definir avant activation. |
| Jeu de personnes | Febrl 4 candidat, generateur synthetique local en solution de repli | Verifier les droits du jeu lui-meme, pas seulement la licence du logiciel qui le distribue. |
| Doublon source | Arbitrage steward demande par le PO | Pas de retraite automatique du doublon ni d'edition de la source dans l'IHM. |
| DQ et degradation | Source ou noeud affecte, isolation partielle, fallback selon contrat | Granularite et effets par champ, source et publication restent a preciser et tester. |
| Historique | Episodes de relations et changements d'attributs et liens conserves | Preciser identite d'episode, temps metier et politique de conservation. |
| Scoring de demonstration | >= 0,95 unique et non contradictoire : automatique ; 0,80 a < 0,95 ou candidats multiples : revue | Formule et contradictions restent a definir et calibrer. Suivre la distribution et sa derive par run, avec denominateur. |
| OIDC, Airbyte, MCP et chatbot | Hors implementation initiale | Interfaces et limites documentees, pas de promesse d'integration. |

Le pilote PostgreSQL ODBC a aussi ete teste sur PostgreSQL 16.4. Un test de faisabilite a
effectivement execute PostgreSQL → DuckDB/dbt-duckdb → PostgreSQL et observe un `HASH_JOIN`.
Versions, commandes et limites figurent dans [le README Docker](../deploy/README.md).
Ce microtest ne demontre ni la reconciliation MDM ni la capacite de production ; les garanties
des trois voies et leurs benchmarks restent a tester.
