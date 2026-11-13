# MDM batch maison : contrat de reconciliation

Statut : proposition de laboratoire, a valider avec le product owner (PO). Aucun moteur,
aucune performance et aucune conformite ne sont deja livres. Ce document complete
[la specification du moteur data](SPECIFICATIONS.md) ; il ne remplace ni ses garanties ni
son perimetre. Les [sprints](MDM_SPRINTS.md) sont le backlog d'execution.

## 1. Intention

Construire, sur la meme histoire de donnees et les memes regles, trois implementations
comparables d'un MDM batch : une reference ETL relationnelle « a l'ancienne », une approche
ELT pilotee par le moteur data, et une approche graphe. On cherche la reconciliation des
identites, des attributs et des relations, pas seulement la detection de doublons. Livrer
aussi une interface de stewardship et un article d'architecture fonde sur des mesures.

Perimetre de demonstration : personnes, lieux et relations `WORKS_AT` et `LIVES_AT`. Une
personne peut travailler dans plusieurs lieux ; plusieurs sources peuvent decrire la meme
personne, le meme lieu ou la meme relation. Trois referentiels source simules alimentent le
laboratoire. L'acquisition Airbyte est simulee par le chargement raw : aucune performance
Airbyte n'est mesuree.

Hors perimetre de cette phase : donnees de personnes reelles, streaming, production, editeur
graphique ETL, implementer OAuth/OIDC, integration Airbyte reelle, chatbot autonome et serveur
MCP de production. Une API exploitable ulterieurement par MCP est requise, pas une promesse
que REST est automatiquement MCP. GraphQL est facultatif et ne remplace pas REST.

## 2. Jeux de donnees et sources

Base candidate : [Febrl 4 du Python Record Linkage Toolkit](https://recordlinkage.readthedocs.io/en/latest/ref-datasets.html),
10 000 enregistrements synthetiques repartis en deux ensembles de 5 000, avec liens de verite
terrain. La [licence BSD-3-Clause du code du toolkit](https://raw.githubusercontent.com/J535D165/recordlinkage/master/LICENSE)
ne suffit pas a prouver les droits propres a chaque fichier : verifier et consigner provenance,
version, droits de redistribution et attribution du jeu avant de le redistribuer. Sinon le
telecharger a l'execution uniquement si les conditions le permettent, ou substituer un jeu
synthetique genere localement dont les droits sont documentes. Pas de scraping de personnes,
ni de source a provenance incertaine.

Construire trois flux stables A/B/C par transformation documentee des deux ensembles ; ne pas
pretendre que Febrl fournit lui-meme trois referentiels ou des relations d'emploi. Generer
lieux, relations, evenements et troisieme source de facon deterministe avec seed et verite
terrain separee. Masquer les identifiants de verite terrain aux moteurs de matching. Garder
le jeu de verite d'origine et identifier ce qui est synthetise ou exclu. Produire manifeste,
empreintes, version, seed, volumes, ordre des cycles et licences ; le meme snapshot fige est
utilise par toutes les voies. Tests : petits fixtures lisibles, Febrl, et jeux synthetiques
plus grands ; aucune taille de production garantie.

## 3. Trois voies et frontieres

| Voie | Mise en oeuvre attendue | Nature et limite de comparaison |
|---|---|---|
| A : ETL historique | PostgreSQL source/cible, pilote ODBC PostgreSQL, etapes et chargements explicites en lots, SQL relationnel | Baseline reproductible inspirée d'un ETL classique, **pas** PowerCenter ni benchmark d'un produit commercial. L'ODBC, le transfert et la transaction sont mesures. |
| B : ELT | Raw PostgreSQL precharge, DuckDB pour calcul local/hash joins, dbt Core et adaptateurs compatibles epingles pour transformations, moteur Python de configuration et orchestration | `hash join`/table de hachage est une strategie de calcul, pas une garantie automatique : verifier le plan. Expliciter extraction PostgreSQL, materiel intermediaire et publication vers PostgreSQL. |
| C : graphe | PostgreSQL + Apache AGE/openCypher si la matrice Docker fonctionne, moteur graphe pour candidats et parcours, etat des noeuds et aretes ; metadata relationnelle si necessaire | Valider compatibilite versions, transactions, contraintes et plan d'execution. Si impossible, exposer le blocage et proposer Neo4j au PO ; ne pas remplacer silencieusement le graphe par du SQL. |

`pgvector` est une experience **optionnelle** de generation de candidats, jamais un prerequis
de l'identite ni une preuve de correspondance. Aucun modele externe ni envoi de donnees ; si
active, epingler modele, version, dimension, index et seuils ; mesurer aussi precision et rappel.
Arrow est un format de transfert memoire, pas une base ni un langage de requete. AGE est une
[extension graphe PostgreSQL avec openCypher](https://age.apache.org/age-manual/master/intro/overview.html) ;
[Microsoft documente AGE et ses contraintes sur Azure](https://learn.microsoft.com/en-us/azure/postgresql/extensions/concepts-extensions-considerations).
Une source potentiellement recherchee est l'article Microsoft du 4 juin 2025
[GraphRAG and PostgreSQL Integration in Docker with Cypher Query and AI Agents](https://techcommunity.microsoft.com/blog/azure-ai-foundry-blog/graphrag-and-postgresql-integration-in-docker-with-cypher-query-and-ai-agents/4420623) :
il traite d'AGE, Cypher et de recherche vectorielle avec ou sans pgvector, pas de reconciliation
MDM ni de preuve de performance pour ce projet. Autre piste :
[Combining pgvector and Apache AGE](https://techcommunity.microsoft.com/blog/adforpostgresql/combining-pgvector-and-apache-age---knowledge-graph--semantic-intelligence-in-a-/4508781).
Le PO doit confirmer s'il s'agit de l'article vise. Le laboratoire est local Docker, distinct
d'une garantie de disponibilite d'extensions sur Azure.

Les trois voies ont des donnees et espaces de travail separes, le meme contrat, les memes regles
versionnees, inputs, calendrier, sorties et oracle ; elles ne reutilisent pas les affectations ou
decisions d'une autre voie. Un eventuel code partage est documente par couche : les decisions de
matching, survie et cycle de vie doivent etre effectivement calculees dans chaque voie. La
normalisation et les tests peuvent partager un contrat ; declarer tout traitement partage pour
eviter de presenter trois executions d'un meme moteur comme trois approches independantes.

La specification existante decrit un moteur data qui n'est pas encore implemente. B doit
construire un **sous-ensemble vertical explicite**, compatible avec ses contrats (config JSON,
CLI/API, raw, transform, quality, load, versions, idempotence et protection des ecritures).
Ce POC ne revendique pas satisfaire les lots complets, la concurrence, les integrations Airflow
et Airbyte ou le dimensionnement de [SPECIFICATIONS.md](SPECIFICATIONS.md). Les exigences de
production non livrees et les ecarts sont indiques dans le papier.

## 4. Modele canonique et invariants

```text
source_record (source_id, source_key, type, revision, etat, dates, attributs)
       --MEMBER_OF--> staging_entity --PUBLISHED_AS--> golden_entity
golden_person --WORKS_AT / LIVES_AT--> golden_place
source_relationship --SUPPORTS--> golden_relationship
```

Il s'agit d'un contrat logique ; representation physique propre a chaque voie. `source_id`
identifie un referentiel et non un type de noeud. La cle `(source_id, type, source_key)` est
stable et unique ; `revision` ordonne les versions d'une cle, avec un departage deterministe
declare. Chaque noeud source appartient a au plus un staging courant ; un staging publie au
plus un golden courant ; un golden peut avoir plusieurs sources. Un staging solo est conserve
et peut construire un golden si ses champs minimaux sont valides et si aucune ambiguite ne
l'interdit. Les identifiants golden sont stables d'un cycle a l'autre, y compris si un autre
attribut gagne ; les fusions/scissions et redirections sont explicites, historisees et ne sont
jamais deduites silencieusement.

Chaque source, staging, golden et relation dispose d'un identifiant, `created_at`,
`updated_at`, `status` (`operational` ou `retired`) et provenance. `created_at` est immuable ;
`updated_at` change seulement en cas de changement semantique, pas a chaque replay. Distinguer
horodatage technique d'observation et date d'effet metier si disponible. Les liens source-vers-
golden et les changements de statut sont historises. La creation de la relation conserve aussi
sa date propre ; les relations retirees ne sont pas physiquement supprimees.

Pour le POC, `LIVES_AT` admet plusieurs relations si les sources l'affirment : ne pas inventer
une regle d'unicite de residence. Les relations ont une identite d'episode stable permettant
plusieurs periodes entre les memes extremites ; les dates d'effet et la cle d'episode doivent
etre definies par contrat, sans deduire une identite a partir de dates manquantes.
Un support source porte `(source_id, source_relationship_key)` et ses dates/statut. Plusieurs
supports peuvent maintenir une relation active ; retirer un support ne retire la relation
qu'en l'absence de tout support actif confirme. Relation avec extremite absente ou ambigue :
quarantaine, raison et trace, jamais lien fabrique. Si une extremite est retiree, la relation
golden courante est retiree pour coherence, avec motif ; le support source reste visible.
Sa reactivation exige des supports et extremites actifs. Si un lieu devient invalide mais
reste reference par une source, conserver cette affirmation source et ouvrir une incoherence
visible en console ; ne pas effacer l'histoire.

Conserver en PostgreSQL l'historique des deltas d'attributs et de relations : identite,
run et revision, changement, ancien et nouveau contenu, dates d'observation/d'effet si
connues, provenance et acteur/regle. Un replay identique ne cree pas de delta. Les valeurs
historisees sont des donnees potentiellement sensibles : droits, retention et purge sont
configurables, distincts des logs techniques qui ne doivent pas les exposer.

## 5. Cycle et regles de reference du POC

Une config versionnee exprime normalisation, cles, regles de correspondance, priorites de
survie, seuils et politiques de source ; Pydantic valide les structures, types, references et
contradictions. Ne pas evaluer de code arbitraire depuis la config. Le meme contrat de regles
est traduit pour chaque implementation ; la traduction et sa version sont auditees.

Le PO demande des regles floues et des scores des le POC, et non uniquement des comparaisons
exactes. Prevoir criteres et poids par type, blocking, seuil d'acceptation et plage de revue
configurables, explication du score et version de la regle. Un score ou une similarite
vectorielle n'est jamais une preuve d'identite. En cas de plusieurs candidats eligibles, de
contradiction ou de doute, creer une revue humaine. Pour le POC le PO accepte une politique
provisoire : un seul candidat eligible avec score >= 0,95 et aucune contradiction peut etre
apparie automatiquement ; un score de 0,80 a moins de 0,95 ou plusieurs candidats eligibles
produit une revue ; en dessous de 0,80, pas de match propose par ce score. Les regles de
contradiction priment sur les seuils et doivent etre definies explicitement : un score eleve
ne rend pas automatiquement compatibles des attributs contradictoires. Les poids, formules,
conditions de contradiction et blocking restent a formaliser et calibrer. Les seuils de demo
ne sont pas des seuils de production et sont propres a la formule retenue. Figer pour chaque
run regles et versions ; les trois voies appliquent les memes semantiques et les memes seuils,
meme si leurs plans de calcul different.

Pour chaque cycle :

1. Figer les sources et leur manifeste ; ingester revisions et controles. Identifier par cle
   source avant toute recherche : une revision met a jour le meme noeud, pas un nouveau golden.
2. Normaliser sans detruire l'original, traiter personnes puis lieux. Chercher des candidats
   via cles explicites, rapprochement flou et blocking borne ; apparier automatiquement seulement
   si les regles et seuils versionnes donnent **un seul** candidat eligible. Ambiguite, contradiction ou liens transitifs
   contradictoires : dossier de revue ; ne pas fusionner automatiquement. Aucun candidat :
   solo eligible ou rejet motive. Ne pas utiliser la verite terrain pour decider.
3. Garantir au plus un membre `operational` par `(staging, source_id, type)` : en cas de
   doublons d'une meme source, ne pas retirer automatiquement le noeud source ni son
   appartenance existante. Mettre la nouvelle affectation en attente pour ne pas violer
   l'invariant. Un doublon reel non corrigeable a la source est presente au steward de cette
   source pour arbitrage ; l'ambiguite d'identite est aussi mise en revue. Les effets sur le
   golden dependent du contrat DQ ; ne pas utiliser la date comme preuve d'identite. Une source
   invalide/HS n'est pas preuve d'une retraite ; bloquer ou mettre en attente suivant la politique.
4. Calculer chaque attribut golden depuis les membres actifs eligibles et valeurs non nulles :
   priorite de source configuree par attribut, puis date source la plus recente, puis
   `(source_id, source_key)` croissant. Conserver valeur gagnante, source, regle/version et
   alternatives ; en cas de conflit declare non resoluble par regle, creer une revue et conserver
   la derniere valeur publiee si elle existe ; sinon ne pas publier l'attribut requis.
5. Construire les golden personnes et lieux valides **dans la version de travail**, puis
   recalculer les relations. Resoudre les extremites par leurs liens source/golden, agregation
   des supports et mise a jour d'etat. Ne pas exposer les entites du nouveau cycle avant les liens.
6. Produire rejets, conflits, lignage et livraison coherente, puis activer une version de lecture
   par bascule atomique dans chaque voie ; avancer les watermarks admissibles apres publication.
   Documenter les limites et traitements partiels explicites du contrat DQ ; ne pas supposer de
   transaction distribuee entre PostgreSQL et un autre stockage.
   Rejouer le meme manifeste et la meme config ne cree ni doublon ni evenement semantique.

La disparition d'une ligne n'est pas une suppression par defaut : retraite seulement sur
evenement explicite ou snapshot complet declare autoritatif, avec validation de completude.
Un flux incremental, incomplet, en echec ou source HS ne provoque aucune retraite par absence.
Golden sans source active confirmee : `retired` ; reactiver si une source valide revient.
Retraites, arbitrages et activations sont audites, jamais des suppressions physiques.

### DQ tout au long du cycle

La DQ est une capacite transversale du MDM : profiling, normalisation tracable, validations de
contrats a l'ingestion, controles de matching et de survie, puis exhaustivite et coherence des
golden et relations. Elle produit des anomalies, leur severite, perimetre, provenance et statut
pour la console, sans effacer la donnee d'origine. Une source auparavant valide qui devient
invalide est ingeree et conservee comme nouvelle revision ; son erreur est visible meme si la
reconciliation ou la publication est bloquee. Ne pas traiter la derniere valeur publiee comme
une preuve que la source est encore valide.

Le PO demande trois niveaux d'effet gouvernes par data contract et une degradation progressive :
une erreur bloquante affecte la datasource ou le noeud concerne, pas systematiquement le cycle
entier ; l'absence de champs obligatoires golden peut empecher la publication de l'objet.
Un blocage partiel isole l'objet ou la relation invalide et laisse traiter les autres lorsque
le contrat l'autorise. Une anomalie mineure reste visible ; le traitement peut continuer selon
le contrat. La survie peut recourir a une autre source valide selon les regles configurees ;
une ancienne valeur ou livraison conservee doit etre marquee comme telle, jamais presentee
silencieusement comme fraiche. Determiner explicitement les perimetres, etats, regles de
fallback et d'avancement de watermark dans le contrat avant implementation. Ne pas convertir
une erreur bloquante en succes, supprimer une source fautive ou avancer un watermark non publie.

Ces priorites, les effets precis de l'arbitrage source et le traitement des retraites sont des
**hypotheses de demonstration**, non une decision metier acquise. Le PO doit les valider avant
de les traiter comme regles de production. Separations de clusters et correction d'un faux
match : revue et action explicite historisee ; pas de scission automatique dans le POC.

## 6. Stewardship, API et securite

L'IHM est commune aux trois voies et indique explicitement voie, run et version de livraison ;
les adaptateurs exposent le meme contrat de services sans reutiliser les affectations ou les
decisions calculees par une autre voie.

REST `/api/v1` est le contrat de reference ; operations : soumettre/consulter runs, lister sources,
noeuds et relations, consulter lignage et conflits, proposer decision de rapprochement ou de
survie, accepter/refuser avec motif, consulter les erreurs de sa source, audit et metriques.
Le steward corrige les donnees dans le referentiel source ; l'IHM MDM ne corrige ni la raw
ni une surcouche de valeurs source. La nouvelle revision source est ingeree au cycle suivant.
Listes paginees et filtrees ;
enveloppes d'erreur et OpenAPI versionnees. Identifiant, version attendue/ETag et cle d'idempotence
pour decisions ; conflit concurrent `409`, droits refuses `403`, inconnu `404`. Arbitrage
applique lors du prochain cycle ou via reexecution explicite, jamais mutation cachee de golden.

Roles : steward source limite a ses sources et erreurs ; master steward par type `person` ou
`place` pour arbitrages affectant le golden ; administrateur de plateforme ; viewer en lecture
autorisee. Les vues de conflit masquent les valeurs des sources non autorisees. Les liens et
metriques ne doivent pas permettre de contourner cette isolation. Tests positifs et negatifs
sur chaque endpoint et service, y compris relations et decision concurrente.

Les decisions humaines approuvees sont des entrees versionnees communes, avec auteur, motif,
portee et date. Chaque voie les applique elle-meme, avec les memes priorites et limites ; une
decision de rapprochement qui contredit une contrainte du contrat est rejetee et expliquee.
Les revisions source, decisions et leurs versions sont incluses dans le manifeste du cycle et la
comparaison. Les refus et conflits de version restent audites sans changer le golden.

OAuth/OIDC n'est pas implemente ici : interface d'identite injectable, mode demo local explicite
sur donnees synthetiques seulement, permissions appliquees cote serveur, aucune confiance
accordee a un role fourni librement par un client. Ce mode n'est pas securise pour exposition
publique ou production ; l'authentification reelle est un prealable. Les audits ne journalisent
pas de valeurs personnelles ; secrets hors Git ; CSRF sur formulaires a session, echappement
HTML, controles de pagination et de cout.

FastAPI/Pydantic, templates Jinja/HTMX, tableaux d'erreurs, files d'arbitrage, historique,
dashboard et exploration [vis-network](https://visjs.github.io/vis-network/docs/network/) :
filtrer/borner les sous-graphes, distinguer source/staging/golden et relations, afficher statut,
provenance et dates. Les actions passent par les memes services et autorisations que REST.
Une future passerelle MCP doit utiliser des operations explicites et autorisees de l'API, listes
bornees, controles de droits et confirmations pour toute ecriture ; aucun acces DB direct,
aucun LLM externe ni chatbot reel dans ce lot.

## 7. Evaluation et article

### Suivi des scores et de leur derive

Le PO demande un indicateur de repartition et de drift. Pour chaque cycle, voie, source,
type de noeud et version de regle, compter les paires candidates scorees dans des classes
explicites, notamment `< 0,80`, `[0,80 ; 0,95[` et `>= 0,95`. Afficher effectifs, nombre
de candidats sans score et denominateur : par exemple « 5 % des paires scorees sont >= 0,95 »
n'est pas « 5 % des personnes sont correctement rapprochees ». Distinguer paires, personnes,
correspondances acceptees, revues et arbitrages ; eviter de compter une personne ayant plusieurs
candidats comme plusieurs personnes. Les seuils de classe sont configures et versionnes.

Comparer la distribution au cycle precedent et a une periode de reference explicite :
ecarts en points de pourcentage par classe, effectifs et evolution du taux de donnees
manquantes, rejets, revues et decision humaine. La comparaison n'est pas interpretable si la
formule, la population, le blocking ou la version changent sans segmentation ; afficher
« non comparable » ou « donnees insuffisantes », pas zero. Le minimum d'echantillon et les
seuils d'alerte sont configures ; une alerte ne change pas automatiquement les regles.
Avec de la verite terrain ou des arbitrages representatifs, suivre precision et rappel ; sans
ces etiquettes, une derive des scores n'est ni une mesure de justesse ni une preuve de biais.
Conserver l'historique des metriques agregées et afficher leurs limites en console.

Avant performance : valider equivalence semantique par cycle, repartition des clusters modulo
identifiants, attributs et provenance, etats, relations/supports, decisions, rejets et audit ;
comparer aussi a la verite terrain independante. Signaler chaque ecart, ne pas annoncer un
gagnant si les sorties divergent. Mesurer precision/rappel/F1 des paires et taux de faux positifs
et negatifs, ainsi que volume de revues ; detailler denominateurs et cas ambigus. Verifier que la
generation de candidats ne masque pas les faux negatifs.

Mesurer separement chargement initial, cycles incrementaux, matching, survie, relations,
publication et temps total de bout en bout, avec transferts ODBC et PostgreSQL/DuckDB inclus.
Le total part du meme etat des sources pour chaque voie et inclut le prechargement raw de B ;
les mesures avec raw deja disponible sont des sous-mesures distinctes. Definir et rapporter l'etat
reel des caches froids/chauds, sans supposer qu'un redemarrage purge les caches du systeme hote.
Consigner demarrages a froid/chaud, echantillons repetes (au moins 3, mediane et min/max),
CPU, RAM/RSS processus et services, disque pic/temporaire, nombre de lignes et octets, taux
de doublons, taille des clusters, versions/images, config, seed, machine et limites Docker.
Journaliser echecs et impossibilites ; comparer en mono-concurrence a ressources equivalentes,
puis documenter une experience de concurrence distincte. Ne pas extrapoler au 200 M de lignes
de la spec moteur ; tout grand volume exige une mesure reelle sur ressources declarees.

L'article d'architecture s'adresse aux decideurs : exigences generiques et neutres vis-a-vis des
outils, schemas des trois familles, hypotheses, methode et resultats sources, tableaux pro/cons,
limites, couts operationnels, gouvernance et recommandations conditionnelles. Il contient des
images ou diagrammes produits a partir des graphes de demonstration vis.js avec legende et
donnees synthetiques ; les captures sont des illustrations, pas des preuves de performance.
Le papier ne declare ni SLA ni victoire avant resultats et reproductibilite.

## 8. Decisions a valider

1. Regle exacte de doublons d'un meme referentiel : est-ce bien une seule occurrence active
   par cluster et par source, et quel departage si les dates sont douteuses ?
2. Retirement, retour d'une source HS, absence en snapshot complet et effets sur golden/relations.
3. Regles metier de matching et seuils de revue, valeurs obligatoires, priorites par attribut,
   droits d'arbitrage, cardinalite des residences, cle d'episode et effets precis des trois
   niveaux de DQ sur les champs, les fallback, les watermarks et les publications.
4. Si AGE est incompatible avec Docker cible : accepter Neo4j ou adapter versions ?
5. Quel article Microsoft (URL/annee) ? Cible materielle, taille des jeux, temps et budgets de test ?
6. Priorite entre POC reproductible et moteur data complet ; OAuth reel et GraphQL futur.

Les hypotheses ci-dessus permettent de preparer les tickets sans les presenter comme arbitrees.
