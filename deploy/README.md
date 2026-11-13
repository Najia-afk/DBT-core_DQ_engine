# Exécution entièrement conteneurisée : ETL / ELT et runtime

Le [Compose](compose.yml) lance pg-meta (1 Go), PostgreSQL d'état (8 Go) et
le worker avec tous ses descendants (8 Go). Total de plafonds : **17 Go**,
swap désactivé pour ces conteneurs. Le [runtime](../configs/runtime.yml)
admet au maximum deux steps et 6 Go réservés aux clients. Ne pas lancer
simultanément les autres Compose de voies ou le banc lourd.

Le [Dockerfile commun](../Dockerfile) inclut les modèles dbt dans le package
installé. L'utilisateur applicatif n'est pas root et `/work` est son espace
d'écriture ; `/data` appartient aussi à cet utilisateur lors de la création
d'un volume neuf. Les bases ne publient aucun port sur l'hôte.

## Démarrage local (PowerShell)

Les données d'exemple ne sont pas des sources déjà disponibles : adapter
le [référentiel](../configs/example/customer.yml), connecter/charger les
sources avant de soumettre. Les globs Parquet du conteneur doivent pointer
dans `/data`. Les tables CRM/ERP de l'exemple doivent réellement exister.

```powershell
# Mot de passe local, hors Git, composé de caractères sûrs pour une URI.
$env:MDM_DB_PASSWORD = "votre_mot_de_passe_local"
docker compose -f deploy\compose.yml up -d --build
docker compose -f deploy\compose.yml exec worker python -m engine.cli validate /cfg/example/customer.yml
docker compose -f deploy\compose.yml exec worker python -m engine.runtime submit --config /cfg/example/customer.yml --lane etl --key customer-jour1 --include-init
docker compose -f deploy\compose.yml exec worker python -m engine.runtime status
docker compose -f deploy\compose.yml logs --tail 50 worker
docker compose -f deploy\compose.yml down
```

`down` conserve les volumes. Ne pas ajouter `--volumes` sans intention
explicite de perdre les données. Des mots de passe contenant des caractères
réservés d'URI nécessitent un DSN correctement encodé.

Pour une recette autonome sur sources synthétiques :

```powershell
.\deploy\smoke.ps1
```

Le script crée un projet Docker au nom unique, exécute une soumission
jusqu'aux 11 exports Parquet puis supprime ses conteneurs et volumes.

## Graphe et banc de mesure

Le [Compose AGE](lanes/graph/compose.yml) est séparé : ce PostgreSQL standard
n'installe pas AGE. Chaque voie a sa [documentation](../README.md).
Le [banc](../engine/bench/README.md) démarre ses propres bases jetables et
mesure les clients lancés sur l'hôte : ne pas confondre ces mesures avec
un benchmark de la pile entièrement conteneurisée.

## Limites

Un OOM du conteneur borne réellement l'arbre applicatif ; il ne garantit
pas une reprise automatique sans vérification des écritures. Pas de HA,
OIDC, API HTTP ni intégration Airflow/Airbyte dans cette pile.
L'ajout d'un champ après création de l'état nécessite encore une migration
de schéma : le validateur de configuration ne la réalise pas.
