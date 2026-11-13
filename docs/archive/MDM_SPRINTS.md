# Plan directeur : MDM batch et reconciliation

Statut : **plan propose, aucun ticket execute**. PO : Najia. Reference metier :
[MDM_RECONCILIATION_SPEC.md](MDM_RECONCILIATION_SPEC.md). Contrat du moteur existant :
[SPECIFICATIONS.md](SPECIFICATIONS.md). Prompt de lancement :
[MDM_AGENT_SWARM_PROMPT.md](MDM_AGENT_SWARM_PROMPT.md).

Ce plan reprend le principe Aria : vue d'ensemble, tickets identifies, dependances, verification
et prompt autonome. Il adapte le format AA+ aux creations : il serait trompeur d'inventer une
cause racine, des numeros de lignes ou des diffs pour du code qui n'existe pas. Chaque ticket
ci-dessous est une mission autonome dans son contexte commun ; avant execution, l'agent releve
les fichiers reellement presents et precise son diff. Les points sont relatifs, pas des jours.

## Regles communes a tous les tickets

| Contrainte | Application |
|---|---|
| C1 : contrat | Lire les deux specifications ; pas de changement silencieux des regles ou de promesse de moteur complet. |
| C2 : isolation | Voies et donnees separees ; chaque voie calcule elle-meme ses decisions. API/UI utilisent les services, pas d'acces direct UI/agent a la base. |
| C3 : donnees | Fixtures synthetiques ; sources, droits et seed documentes ; verite terrain invisible du matching. Pas de secrets dans Git. |
| C4 : Docker | Compose local reproductible, versions epinglees et ressources bornees ; ne pas modifier ou supprimer les volumes preexistants d'autres projets. |
| C5 : fiabilite | Reprise et transactions explicites ; aucun succes silencieux ; idempotence, audit et tests de non-regression. |
| C6 : securite | Autorisation cote serveur ; mode demo non expose comme production ; donnees et logs minimises ; aucune integration distante non approuvee. |
| C7 : preuve | Les tests declares par un ticket sont a creer si absents. Aucune mesure, licence ou fonctionnalite non verifiee n'est presentee comme acquise. |

Structure inspiree des projets Aria et Bubble examines : `engine/` pour contrats et voies,
`src/api/` pour routes et services API, `src/web/` pour l'interface, `pipelines-config/` pour
JSON et dbt, `deploy/` pour Compose, `tests/` pour tests, `examples/` pour fixtures et `docs/`
pour decisions et article. Certains repertoires sont encore a creer. Les chemins ci-dessous
sont des zones de propriete ; ne pas modifier celles attribuees en parallele a un autre agent.

## Dependances et jalons

```text
S0 (contrat, donnees, outillage)
  -> S1 (modele + oracle + gouvernance du cycle)
      -> S2 (ETL ODBC) ---\
      -> S3 (ELT dbt) -----+-> S5 (equivalence et benchmark) -> S7 (papier)
      -> S4 (graphe AGE) --/
      -> S6 (API / stewardship / IHM / vis.js) -----------------> S7
```

Le socle et l'oracle doivent etre figes avant l'implementation parallele des voies. S6 peut
demarrer sur les contrats de S1, mais son integration reelle attend une voie finie ; l'evaluation
des trois voies attend S2, S3 et S4. Ne pas traiter les estimations comme un calendrier.

| Sprint | But et jalon de sortie | Tickets | Points |
|---|---|---|---:|
| S0 | Faisabilite Docker, donnees, decision graphe et squelette | S0-01 a S0-04 | 13 |
| S1 | Contrat canonique, DQ, historique et suite d'acceptation independante | S1-01 a S1-06 | 31 |
| S2 | Voie A ETL/ODBC complete sur cycles | S2-01 a S2-03 | 16 |
| S3 | Voie B moteur/ELT complete sur cycles | S3-01 a S3-04 | 21 |
| S4 | Voie C graphe complete sur cycles | S4-01 a S4-03 | 18 |
| S5 | Equivalence, qualite des matchs, drift et mesures | S5-01 a S5-04 | 21 |
| S6 | API, stewardship, dashboards et graphe | S6-01 a S6-04 | 21 |
| S7 | Article, reproduction et recette finale | S7-01 a S7-03 | 13 |

**Definition of Done commune :** criteres propres au ticket satisfaits, tests automatises crees
et executes, preuves et commandes documentees, erreurs visibles, aucune regression du contrat,
documentation et statut mis a jour. S'il manque Docker, un pilote, une licence ou une decision
bloquante, marquer BLOCKED avec preuve et consequence ; ne pas marquer DONE. Aucun commit,
push, installation globale, connexion Aria ou operation destructive sans demande/autorisation.

## S0 — Cadrage et base reproductible

### S0-01 — Donnees, droits et scenarios | P0 | 5 pts

- **Probleme / resultat :** trois sources comparables de personnes, lieux et liens n'existent pas.
- **Propriete :** `examples/datasets/`, generateur de fixtures et documentation du jeu.
- **Dependances :** aucune.
- **Mission :** verifier droits du jeu Febrl et de ses fichiers, epingler source/version ; sinon
  retenir l'alternative synthetique de la spec. Produire A/B/C, lieux, relations et cycles
  deterministes, manifestes et verite terrain reservee aux tests. Prevoir situations match exact,
  solo, ambigu, doublon source, changement d'attribut, source HS, retraite et reactivation.
- **Acceptation / verification :** deux generations meme seed ont checksums identiques ; seed
  differente change le jeu ; verite absente des inputs du moteur ; chaque scenario a un attendu
  explicite et droits/provenance sont cites. Executer `pytest tests/datasets -q` : tous les tests
  de generation et d'isolation passent.

### S0-02 — Compose et matrice technique | P0 | 3 pts

- **Probleme / resultat :** aucun environnement local ni pilote ODBC/AGE verifie.
- **Propriete :** `deploy/`, `.env.example`, documentation de demarrage.
- **Dependances :** aucune.
- **Mission :** creer Compose a noms/projet et volumes propres, PostgreSQL, ODBC et profil AGE ;
  limiter les ressources et epingler images/versions compatibles avec les plateformes retenues.
  Garder la pile minimale ; noter RAM/disque requis et commandes sans destruction de volume.
- **Acceptation / verification :** `docker compose -f deploy/compose.yml config` valide ; test
  de sante PostgreSQL, requete ODBC et creation/requete AGE passent dans les conteneurs. Si AGE
  ou le pilote echoue, conserver logs versions et marquer S4 bloque ; ne pas simuler un succes.

### S0-03 — Recherche et decisions | P1 | 2 pts

- **Probleme / resultat :** article Microsoft exact et compatibilites inconnus.
- **Propriete :** `docs/MDM_DECISIONS.md`.
- **Dependances :** S0-02 pour la matrice Docker.
- **Mission :** rechercher et sourcer l'article Microsoft evoque ; distinguer article trouve,
  documentation generale et incertitude. Documenter versions AGE/Postgres, licence des images,
  possibilite Neo4j et pgvector optionnel ; enregistrer les hypotheses PO de la spec avec statut.
- **Acceptation / verification :** chaque affirmation technique a une source ou une preuve
  locale ; si article exact introuvable, le dire et demander URL/annee au PO ; documenter le
  choix graphe et ses blocages sans substitution automatique.

### S0-04 — Projet Python et tests | P0 | 3 pts

- **Probleme / resultat :** depot sans application ni runner de tests.
- **Propriete :** `pyproject.toml`, `engine/`, `src/api/`, `tests/` configuration, commandes projet.
- **Dependances :** aucune ; coordonner avec S0-01/S0-02 pour eviter les fichiers partages.
- **Mission :** configurer packaging, Pydantic, pytest, lint/type-check existants si disponibles,
  et points d'entree futurs ; documenter installation dans un environnement isole.
- **Acceptation / verification :** installation depuis manifeste, `pytest tests -q` avec un vrai
  test de fumee, commande de lint et type-check documentees et reussies. Aucun faux test vide.

## S1 — Contrat commun et oracle

### S1-01 — Schema et configuration | P0 | 5 pts

- **Probleme / resultat :** aucun contrat executable des sources, golden, liens et regles.
- **Propriete :** `engine/contracts/`, `pipelines-config/`.
- **Dependances :** S0-01, S0-04.
- **Mission :** schemas Pydantic et JSON versionnes pour source, revisions, staging, golden,
  supports, relations, conflits, provenance, run et policies. Valider cles, types, statut, dates,
  priorites, thresholds et references ; refuser champs/regles inconnus ou contradictoires.
- **Acceptation / verification :** `pytest tests/contracts -q` : config valide chargee, schema
  exporte, identifiants absents, priorites ambigues et types invalides rejetes avec erreur explicite.
  Une config de demo est lisible sans modifier du code.

### S1-05 — Data contracts et DQ transversale | P0 | 5 pts

- **Probleme / resultat :** profiling, normalisation et anomalies a tous les niveaux n'ont pas
  de contrat commun ; les effets total/partiel/mineur demandent une decision PO.
- **Propriete :** `engine/` contrats DQ, `tests/` dedies ; coordonner les interfaces avec S1-01.
- **Dependances :** S1-01 et clarification PO des effets, granularite et reprise.
- **Mission :** versionner controles, normalisations, profiling, severites, perimetre et decisions ;
  tracer source, version et preuve de controle sans effacer la raw. Produire resultats
  d'exhaustivite/coherence sur entites et relations, et garder visibles les revisions devenues
  invalides et relations vers lieux invalides. Faire consommer ces contrats par A/B/C.
- **Acceptation / verification :** tests unitaires et d'acceptation verifient les trois decisions
  effectivement tranchees, golden precedemment valide puis source invalide, exhaustivite,
  coherence et journal d'anomalies ; aucun effet bloque n'est transforme en succes.

### S1-06 — Historique des deltas | P0 | 5 pts

- **Probleme / resultat :** pas de journal PostgreSQL des changements entre cycles.
- **Propriete :** `engine/history/`, `sql/history/`, `tests/integration/test_history.py`.
- **Dependances :** S0-02 ; integrer au contrat S1-01 avant utilisation par une voie.
- **Mission :** conserver les changements d'attributs et de relations, anciennes et nouvelles
  valeurs, identites, run, date d'observation/d'effet et provenance. Gerer episodes de relations,
  permissions et retention ; parametrer les requetes et ne pas inscrire de valeurs dans les logs.
- **Acceptation / verification :** tests PostgreSQL reels couvrent insertion, rollback, replay
  identique sans double evenement, cle identique/contenu divergent explicitement refuse, valeurs
  nulles/absentes distinguees et recuperation ordonnee. Les voies devront ensuite l'integrer ;
  la presence du journal seul ne prouve pas l'historisation de bout en bout.

### S1-02 — Oracle d'acceptation par cycles | P0 | 8 pts

- **Probleme / resultat :** les trois voies pourraient produire des resultats incompatibles.
- **Propriete :** `tests/acceptance/`, `examples/scenarios/`.
- **Dependances :** S0-01, S1-01, S1-05 pour les effets DQ.
- **Mission :** definir fixtures et sorties attendues lisibles pour C0 initial, C1 changement et
  ajout, C2 doublon et ambiguite, C3 source HS, C4 retrait confirme, C5 reactivation et replay.
  L'oracle compare groupes modulo IDs, survie/provenance, supports, lifecycle et erreurs ; pas
  de dependance aux decisions internes d'une voie. Prevoir multi-employeurs, reference absente,
  source ID stable, conflit d'attribut et arbitrage, noms bruites, scores aux bornes des seuils,
  dates contradictoires, candidat manquant a cause du blocking et deux candidats eligibles.
  Inclure nouvelle revision corrigee dans la
  source et decision humaine approuvee versionnee, cycle qui les applique, DQ a tous les niveaux, donnees hier valides puis
  invalides et personne liee a un lieu invalide. Comparer decisions, rejets et audit.
- **Acceptation / verification :** `pytest tests/acceptance -q` valide l'oracle sur sorties
  attendues et rejette des sorties volontairement mutees sur identite, dates, source et relation ;
  les tests d'adaptateur de chaque voie sont branches dans leurs sprints.

### S1-03 — Contrat de publication et reprise | P0 | 5 pts

- **Probleme / resultat :** un run partiel pourrait laisser golden et relations incoherents.
- **Propriete :** `engine/contracts/` pour interfaces de run ; `docs/MDM_PUBLICATION.md`.
- **Dependances :** S1-01.
- **Mission :** definir etats, cle d'idempotence, manifeste/config immuable, serialisation des
  ecritures, livraison versionnee atomique par voie, journal et reprise apres issue incertaine.
  Prevoir une panne entre personnes, lieux et relations ; watermark uniquement apres publication.
- **Acceptation / verification :** `pytest tests/contracts/test_publication.py -q` teste transitions,
  re-soumission meme cle/contenu et conflit de contenu ; documenter la preuve transactionnelle
  ou l'operation manuelle necessaire pour chaque futur adaptateur.

### S1-04 — Contrat de stewardship et permissions | P0 | 3 pts

- **Probleme / resultat :** arbitrage humain et isolation des sources non specifies au niveau API.
- **Propriete :** `engine/contracts/` pour identites, conflits et decisions.
- **Dependances :** S1-01.
- **Mission :** modeliser autorisations source/type, versions optimistes, statuts de decision,
  motif, audit, droits sur vues multi-sources et cycle d'application ; definir le format des
  decisions communes a toutes les voies. Les corrections sont effectuees dans les sources.
  L'auth reelle reste hors lot.
- **Acceptation / verification :** `pytest tests/contracts/test_stewardship.py -q` couvre actions
  autorisees, refus inter-source/type, conflit de version et masquage de donnees.

## S2 — Reference ETL relationnelle / ODBC

### S2-01 — Extraction et traitement ETL | P0 | 5 pts

- **Probleme / resultat :** aucune baseline ODBC comparable.
- **Propriete :** `engine/legacy_etl/` extraction, chargement, mesure.
- **Dependances :** S0-02, S1-01, S1-03.
- **Mission :** lecture source PostgreSQL via ODBC et chargement en lots dans zone de travail
  propre, normalisation et journaux de transfert. Gerer erreurs, bornes et snapshots fixes ;
  pas de simulation de PowerCenter ni chargement de toutes les lignes en memoire Python.
- **Acceptation / verification :** `pytest tests/legacy_etl/test_odbc.py -q` en Compose couvre
  transfert reel, echec de connexion, volumes, checksum et rejouabilite ; le temps ODBC est
  expose dans les metriques.

### S2-02 — Identites et survie relationnelles | P0 | 8 pts

- **Probleme / resultat :** la baseline ne reconcilie pas encore personnes et lieux.
- **Propriete :** `engine/legacy_etl/` matching et SQL de reconciliation.
- **Dependances :** S2-01, S1-02, S1-04.
- **Mission :** implementer independamment les regles de la spec : cle source, candidats bornes,
  matching flou et scores configures/expliques, revue ambigue, staging/solo, revue du doublon
  intra-source, survie par attribut, lineage et application
  des nouvelles revisions source et decisions humaines approuvees au cycle suivant.
- **Acceptation / verification :** `pytest tests/legacy_etl/test_entities.py -q` et tests
  d'acceptation de S1 sur les entites passent ; replay conserve IDs et dates semantiques ;
  aucune verite terrain n'est utilisee comme cle.

### S2-03 — Relations, publication et pannes | P0 | 3 pts

- **Probleme / resultat :** la baseline doit reconcilier le modele et publier en securite.
- **Propriete :** `engine/legacy_etl/` relations/publication, tests d'integration dedies.
- **Dependances :** S2-02, S1-03.
- **Mission :** supports multi-sources, changements de statut, extremites ambiguës, publication
  coherente et reprise ; conserver toutes les relations retirees.
- **Acceptation / verification :** `pytest tests/legacy_etl -q` et oracle complet passent sur
  tous les cycles ; injecter panne avant commit et apres commit/accuse perdu, verifier qu'aucune
  publication partielle ou double n'est presente.

## S3 — Voie moteur Python / DuckDB / dbt

### S3-01 — Sous-ensemble moteur configurable | P0 | 8 pts

- **Probleme / resultat :** la spec moteur existe, son implementation n'existe pas.
- **Propriete :** `engine/` orchestration, CLI/API technique et tests moteur.
- **Dependances :** S1-01, S1-03.
- **Mission :** realiser le chemin vertical raw -> transform -> quality -> load, config JSON
  validee Pydantic, plan immuable, version de config, run/etat, logs, serialisation des ecritures
  et reprise prudente. Documenter clairement les fonctions non implementees de la spec complete.
- **Acceptation / verification :** `pytest tests/engine -q` teste validation, dependencies,
  soumission idempotente, conflit, erreur explicite, publication et reexecution ; CLI et API
  soumettent la meme config. Aucun Airflow/Airbyte reel affirme dans le POC.

### S3-02 — Raw et transfert DuckDB | P0 | 5 pts

- **Probleme / resultat :** le parcours ELT et ses transferts ne sont pas effectifs.
- **Propriete :** `engine/elt/` connecteurs et ressources ; `pipelines-config/` definition B.
- **Dependances :** S3-01, S0-02.
- **Mission :** charger les sources simulees en raw PostgreSQL, figer le snapshot, transferer
  vers calcul DuckDB, fixer limites threads/memoire et declarer les sorties vers PostgreSQL.
  Enregistrer couts/volumes de transfert et plan de jointure, sans supposer un hash join.
- **Acceptation / verification :** `pytest tests/elt/test_transfers.py -q` verifie contenu,
  manifeste, limites configurees, echec explicite et sortie cible ; preuve du plan DuckDB.

### S3-03 — Modeles dbt et reconciliation | P0 | 5 pts

- **Probleme / resultat :** matching, survie et relations ne sont pas implementes dans B.
- **Propriete :** `pipelines-config/` projet dbt et `engine/elt/` adaptation au moteur.
- **Dependances :** S3-02, S1-02, S1-04.
- **Mission :** modeles dbt/adaptateurs epingles, SQL de candidats, staging/golden et relations,
  matching flou et scores expliques, lineage, DQ, conflits et application des revisions source et decisions humaines approuvees au cycle
  suivant ; expliciter frontiere DuckDB/PostgreSQL.
- **Acceptation / verification :** `dbt parse` puis execution/test du projet documente ;
  `pytest tests/elt/test_reconciliation.py -q` et oracle par cycles passent avec sorties reelles.

### S3-04 — Publication et incidents ELT | P0 | 3 pts

- **Probleme / resultat :** coherence et reprise de B ne sont pas demontrees.
- **Propriete :** `engine/elt/` publication et tests incidents.
- **Dependances :** S3-03, S1-03.
- **Mission :** versionner puis publier ensemble entites et relations, conserver retraites,
  gerer rollback et commit dont l'accuse est perdu.
- **Acceptation / verification :** `pytest tests/elt -q` et oracle complet passent ; tests de
  panne, double demande et replay ne produisent ni incoherence visible ni doublon.

## S4 — Reconciliation graphe

### S4-01 — Modele et ingestion graphe | P0 | 5 pts

- **Probleme / resultat :** pas de graphe source -> staging -> golden exploitable.
- **Propriete :** `engine/graph/` schema, ingestion, requetes AGE.
- **Dependances :** S0-02, S0-03, S1-01.
- **Mission :** creer noeuds/aretes et contraintes possibles, indexes et acces openCypher ;
  assurer identites stables, lifecycle et support transactionnel. Identifier explicitement ce
  qui est garanti par l'application plutot que par AGE.
- **Acceptation / verification :** `pytest tests/graph/test_ingestion.py -q` execute de vraies
  requetes AGE en Compose, controle unicite, creation, dates et replay ; echec technique bloque S4.

### S4-02 — Matching et survie dans le graphe | P0 | 8 pts

- **Probleme / resultat :** les parcours de candidats ne prennent pas encore de decisions.
- **Propriete :** `engine/graph/` matching, clusters, survie.
- **Dependances :** S4-01, S1-02, S1-04.
- **Mission :** calculer independamment rattachements, solo, revue du doublon source, ambiguite et
  attributs, matching flou et scores expliques selon contrat, appliquer revisions source et decisions humaines approuvees au cycle
  suivant ; capturer requetes, provenance et limites de parcours.
- **Acceptation / verification :** `pytest tests/graph/test_entities.py -q` et oracle entites
  passent, y compris cas transitif contradictoire et revue ; aucune affectation d'une autre voie.

### S4-03 — Relations et publication graphe | P0 | 5 pts

- **Probleme / resultat :** les relations golden et leur histoire ne sont pas garanties.
- **Propriete :** `engine/graph/` relations/publication, tests incidents dedies.
- **Dependances :** S4-02, S1-03.
- **Mission :** reconcilier aretes avec supports sources et noeuds valides ; retirer/reactiver
  sans suppression ; assurer publication coherente et journal de reprise de la voie.
- **Acceptation / verification :** `pytest tests/graph -q` et oracle complet passent ; tests
  support multiple, extremite retiree, erreur avant/apres commit et replay passent.

## S5 — Comparaison et mesures

### S5-01 — Comparateur inter-voies | P0 | 5 pts

- **Probleme / resultat :** des sorties divergentes pourraient etre comparees comme equivalentes.
- **Propriete :** `engine/benchmark/compare.py`, `tests/benchmark/`.
- **Dependances :** S2-03, S3-04, S4-03.
- **Mission :** normaliser IDs par membership source et comparer par cycle toutes les dimensions
  de S1-02 et la verite terrain. Sortir ecarts structures, precision/rappel/F1, FP/FN et revues,
  avec denominateurs explicites.
- **Acceptation / verification :** `pytest tests/benchmark/test_compare.py -q` detecte valeurs,
  associations et relations mutees ; execution des trois voies reelles termine sans ecart
  inexpliqué. Aucun classement si ecarts ou echec.

### S5-02 — Harness de benchmark | P0 | 8 pts

- **Probleme / resultat :** aucune mesure reproductible bout en bout.
- **Propriete :** `engine/benchmark/` runner, rapports bruts et protocole.
- **Dependances :** S5-01.
- **Mission :** automatiser jeux, phases, 3 repetitions froid/chaud, capture machine/versions,
  CPU, RSS services/processus, disque temporaire, volume, transfert, latences et compteurs.
  Stocker resultats par run et publier mediane/min/max sans masquer echecs.
- **Acceptation / verification :** `pytest tests/benchmark/test_runner.py -q` teste collecte et
  traitement des echecs ; executer un jeu de fumee en Docker sur les 3 voies et produire un
  rapport attribuable et reproductible avec les memes ressources.

### S5-03 — Essais parametrables et option vectorielle | P1 | 3 pts

- **Probleme / resultat :** le petit jeu seul ne montre pas les limites de charge.
- **Propriete :** configurations benchmark et rapport de limites.
- **Dependances :** S5-02.
- **Mission :** preparer jeux de tailles et densites de doublons croissantes, full et incremental,
  concurrence distincte ; tester pgvector seulement apres decision et preuve de disponibilite.
- **Acceptation / verification :** au moins deux tailles effectivement mesurees si ressources
  disponibles, avec tailles, repetitions et ecarts ; sinon etat BLOCKED et limites documentees.
  Option vectorielle non realisee clairement marquee, sans conclusion inventee.

### S5-04 — Distribution et drift des scores | P1 | 5 pts

- **Probleme / resultat :** le PO veut observer la proportion de scores eleves et son evolution.
- **Propriete :** `engine/benchmark/` agregation des scores ; `tests/benchmark/` tests associes.
- **Dependances :** S5-01 ; formules, seuils et jeu de reference figes par run.
- **Mission :** calculer par voie/source/type et version les effectifs et proportions par classe,
  dont `>= 0,95`, nombres sans score, decisions et revues ; comparer au cycle precedent et a une
  reference explicite en points de pourcentage. Signaler changements de population/regle,
  echantillon insuffisant et comparaisons impossibles ; ne pas ajuster de seuil automatiquement.
- **Acceptation / verification :** tests avec 100 scores dont 5 >= 0,95 donnent 5 %, puis un
  second jeu a 9 % donne +4 points ; zero score, scores manquants, versions differentes et
  faible effectif sont traites explicitement. Les metriques de paires et d'entites ne sont
  jamais confondues ; les valeurs ne sont pas declarees comme precision sans verite terrain.

## S6 — API et interface de stewardship

### S6-01 — Services et REST | P0 | 8 pts

- **Probleme / resultat :** pas d'API de consultation ou de decision.
- **Propriete :** `src/api/`, `engine/` services, `tests/api/`.
- **Dependances :** S1-04, S2-03, S3-04 et S4-03 pour integration des trois voies reelles.
- **Mission :** fournir operations spec §6, OpenAPI, pagination, erreurs, injection d'identite demo
  serveur, permissions par source/type, audit, idempotence et verrous optimistes ; integrer les
  trois voies via un contrat commun, distinguer voie, run et version. Decisions et nouvelles
  revisions corrigees dans les sources sont prises en compte au cycle suivant, sans edition MDM.
- **Acceptation / verification :** `pytest tests/api -q` couvre tous endpoints, refus inter-source,
  masquage, conflit `409`, idempotence et application d'arbitrage et nouvelle revision dans un cycle
  reel pour chacune des trois voies ; verifier que raw reste inchange.

### S6-02 — UI dossiers et decisions | P0 | 5 pts

- **Probleme / resultat :** les stewards n'ont pas d'espace de travail.
- **Propriete :** `src/web/` templates HTMX, `tests/ui/`.
- **Dependances :** S6-01.
- **Mission :** IHM commune aux trois voies ; vues sources, erreurs DQ, conflits, dossiers,
  lignage, formulaire de decision et historique ; aucune correction des valeurs sources dans MDM ;
  droits appliques cote serveur
  via services communs, pagination et erreurs visibles.
- **Acceptation / verification :** `pytest tests/ui/test_stewardship.py -q` et parcours navigateur
  documente : un steward ne voit que sa source et arbitre les dossiers autorises, un master arbitre son type, version
  obsolete est refusee, erreur visible et action auditee. Echappement et CSRF selon mode de session.

### S6-03 — Dashboards et vis-network | P1 | 5 pts

- **Probleme / resultat :** etat du MDM et provenance graphique illisibles.
- **Propriete :** `src/web/` dashboard, graph ; `tests/ui/` associes.
- **Dependances :** S6-01, S5-04 ; integration de S6-02 pour navigation partagee.
- **Mission :** dashboard runs, qualite, conflits, statuts, distribution et drift des scores avec
  denominateurs et messages « non comparable » ; visualiser source/staging/golden et
  relations avec legende, filtres, dates, statut, origine et limite explicite du sous-graphe.
- **Acceptation / verification :** `pytest tests/ui/test_graph.py -q` verifie droits et bornes ;
  test navigateur verifie legende, filtrage, detail et rendu d'un sous-graphe avec relation retiree.
  Capturer une image synthetique pour l'article sans pretendre a un benchmark visuel.

### S6-04 — Preparation MCP | P1 | 3 pts

- **Probleme / resultat :** un futur assistant ne doit pas contourner les permissions.
- **Propriete :** documentation du contrat d'integration et tests d'API ; pas de serveur MCP.
- **Dependances :** S6-01.
- **Mission :** cataloguer operations de lecture bornees, actions a confirmer, identite propagee,
  pagination, erreurs et audit ; fournir exemples REST neutres et mapping futur MCP.
- **Acceptation / verification :** tests API existants prouvent permissions et limites pour les
  operations exposees ; documentation precise que ni OAuth ni chatbot ni transport MCP ne sont
  livres. Pas de cle ou acces externe.

## S7 — Article et recette

### S7-01 — Papier architecture d'entreprise | P0 | 5 pts

- **Probleme / resultat :** pas de comparaison neutre et lisible pour decideurs.
- **Propriete :** `docs/MDM_ARCHITECTURE_PAPER.md`, illustrations generees autorisees.
- **Dependances :** S5-02, S6-03.
- **Mission :** rediger exigences, principes, familles d'approches avant les outils, schemas,
  illustrations vis-network, protocole, mesures liees aux artefacts, pro/cons et recommandations
  conditionnelles ; distinguer cout observe et aspects non mesures, limites et lacunes du POC.
- **Acceptation / verification :** chaque chiffre reference un run et chaque image a legende,
  source et droits ; resultats divergents ou indisponibles affiches comme tels, aucune affirmation
  de performance inventee, lecture compréhensible sans connaissance des produits.

### S7-02 — Runbook et reproductibilite | P0 | 5 pts

- **Probleme / resultat :** un lecteur ne peut pas reproduire de bout en bout.
- **Propriete :** `README.md`, `docs/MDM_RUNBOOK.md`.
- **Dependances :** S2-03, S3-04, S4-03, S6-03, S5-02.
- **Mission :** documenter prerequisites Windows/Docker, versions, licences, commandes, ports,
  ressources, lancement cycles/benchmark/UI, diagnostic, sauvegarde et arret non destructif.
- **Acceptation / verification :** depuis environnement propre et isole, suivre commandes
  documentees jusqu'a run, verification des 3 voies, API et UI ; noter les sorties et les
  prerequis impossibles. Aucune commande ne supprime des volumes sans accord explicite.

### S7-03 — Recette finale et revue PO | P0 | 3 pts

- **Probleme / resultat :** l'ensemble peut rester incoherent malgre les tests unitaires.
- **Propriete :** rapport de recette dans `docs/`, statuts du present backlog.
- **Dependances :** S5-03, S5-04, S6-04, S7-01, S7-02.
- **Mission :** executer tests pertinents, Compose et demonstration sur les cycles ; recenser
  preuves, manques, ecarts a la spec moteur, risques, decisions PO et recommandations.
- **Acceptation / verification :** reprise et replay prouves pour les 3 voies, resultats
  comparables, roles testables, article et runbook reproductibles. Chaque manquement bloque
  la recette et figure explicitement au rapport, sans note DONE de complaisance.

## Ordre et coordination des agents

- Un agent = un ticket, contexte et perimetre explicites ; au maximum paralleliser des tickets
  dont dependances et fichiers proprietaires ne se chevauchent pas.
- S0-01/S0-02/S0-04 sont paralleles apres partage des interfaces et fichiers communs ; S0-03
  suit S0-02. S1-02/S1-03/S1-04 suivent S1-01, avec coordination des contrats.
- Une fois S1 fige, A, B et C sont paralleles, un proprietaire par voie ; ne pas lancer deux
  agents sur le meme fichier de config, migration, Compose ou suite d'acceptation.
- Integrateur seul responsable des interfaces communes, puis tests transverses. Les agents
  rendent fichiers modifies, commandes/resultats, decisions, risques et blocages ; ne pas
  changer le contrat commun en secret.
- Statuts : TODO / IN PROGRESS / BLOCKED / DONE avec preuves. Reviser les points apres
  calibration ; ne pas assimiler points et duree ni repartir les tickets de force.
