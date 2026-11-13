# Résultats du banc MDM

Les chemins de profil personnel des preuves publiées sont masqués par
`<USER_HOME>` ; les mesures sont inchangées. Pour reproduire les commandes,
remplacer ce marqueur par le répertoire local approprié. Avant toute nouvelle
publication ou régénération du rapport, masquer à nouveau les chemins personnels
dans les JSON et le Markdown généré.

<!-- BENCH:GENERATED:START -->
## Runs mesurés (généré par `python -m engine.bench report`)

| run | voie | date | statut | nœuds | attr. | jours delta | Σ phases (s) | pic RAM client+PG (Go) | base PG finale (Go) | pic PGDATA (Go) | hash cfg | graine |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| elt-L1 | elt | 2026-11-13 04:12 | ok | 100 000 | 50 | 3 | 184.2 | 2.6 | 0.5 | 1.8 | 3d68f2479499 | 42 |
| elt-L2-retry1 | elt | 2026-11-13 06:03 | aborted | 300 000 | 200 | 2 | 5 502.5 | 22.4 | 1.5 | 3.8 | 9698835ff5da | 42 |
| elt-L2 | elt | 2026-11-13 04:25 | aborted | 300 000 | 200 | 2 | 255.7 | 28.0 | 1.5 | 3.8 | f1645cb7564d | 42 |
| equiv-elt-2k | elt | 2026-11-13 15:47 | ok | 2 000 | 30 | 2 | 68.6 | 0.8 | 0.0 | 0.1 | de6112bbadee | 7 |
| equiv-etl-2k | etl | 2026-11-13 15:46 | ok | 2 000 | 30 | 2 | 12.4 | 0.3 | 0.0 | 0.1 | 61fd77ef3f7a | 7 |
| equiv-graph-2k | graph | 2026-11-13 15:48 | ok | 2 000 | 30 | 2 | 20.5 | 0.3 | 0.0 | 0.1 | 331425cb73aa | 7 |
| etl-L1 | etl | 2026-11-13 04:08 | ok | 100 000 | 50 | 3 | 81.1 | 1.3 | 0.5 | 1.4 | 072b5146f987 | 42 |
| etl-L2 | etl | 2026-11-13 04:20 | ok | 300 000 | 200 | 2 | 305.5 | 4.8 | 3.7 | 8.8 | c0076c42fbd0 | 42 |
| etl-L3-final | etl | 2026-11-13 16:15 | ok | 1 000 000 | 200 | 2 | 1 570.1 | 8.1 | 12.2 | 21.2 | df11c4092ba0 | 42 |

## Projection 1 M nœuds × 200 attributs (généré)

Le **mesuré** est étiqueté « mesuré ». Le reste est une **extrapolation** par cellules (nœuds × attributs) : borne linéaire (exposant 1) et borne par exposant ajusté sur les deux plus grandes tailles mesurées (un coût fixe par invocation fausse l'ajustement aux petites tailles). Aucune incertitude statistique n'est calculable avec 2 points.

| voie | base | jour 0 : ingest + reconcile | quotidien (5 % de mises à jour) : ingest + reconcile | état PG après jour 0 |
|---|---|---|---|---|
| etl | etl-L1, etl-L2, etl-L3-final | 18.9 min (mesuré) | 4.3 min (mesuré) | 11.7 Go (mesuré) |
| elt | elt-L2-retry1, elt-L2 | non validé : échec à une taille supérieure | capacité non extrapolée | non validé |

Capacité quotidienne sur cette machine, fenêtre de 6 h, exécutions séquentielles (1 run) ou 3 runs parallèles **en supposant un passage à l'échelle parfait ×3 (borne haute, non mesurée à cette taille)** :

| voie | statut | min / référentiel (quotidien) | réf. en 6 h, 1 run | réf. en 6 h, 3 parallèles (borne haute) | runs parallèles requis pour 50 réf. | disque d'état pour 50 réf. (Go) |
|---|---|---|---|---|---|---|
| etl | mesuré | 4.3–4.3 | 84–84 | 253–253 | 0.6–0.6 | 583–583 |

### elt-L1 — elt 100 000 nœuds × 50 attributs  3 jour(s) delta

Statut : **ok**

| clé | valeur |
|---|---|
| date | 2026-11-13T04:12:25+01:00 |
| machine | 32 threads, 63.7 Go RAM, Windows-11-10.0.26300-SP0 |
| versions | python 3.12.10, duckdb 2.0.0, pyarrow 26.0.1, psycopg 3.3.6, dbt-core 1.12.7, dbt-duckdb 1.11.1, docker 29.1.3 |
| PostgreSQL | 16.4 (Debian 16.4-1.pgdg120+2) (postgres:16.4-bookworm) |
| config | hash 3d68f2479499, graine 42, sources ['parquet', 'parquet', 'parquet'] / ['full', 'full', 'delta'], ingest native, batch_rows config (50000) |
| deltas (jour 1..N) | upd 1.0% new 0.5% del 0.2% brk 0.1%; upd 5.0% new 0.5% del 0.2% brk 0.1%; upd 20.0% new 0.5% del 0.2% brk 0.1% |
| réglages PG | {"memory": "12g", "shm_size": "1g", "shared_buffers": "3GB", "work_mem": "64MB", "maintenance_work_mem": "1GB", "effective_cache_size": "8GB", "max_wal_size": "16GB", "min_wal_size": "2GB", "checkpoint_timeout": "30min", "synchronous_commit": "off", "max_connections": 100} |
| client | ELT mem 6GB threads 8 ; ETL workers None |
| jeu de données | 0.12 Go, génération 13.4 s, RSS pic 677.2 Mo |
| pic hôte (psutil used) | 29.61 Go (base avant run 25.64 Go) |

| jour | phase | mur (s) | lignes | lignes/s | RSS client pic (Mo) | RAM PG pic (Mo) | CPU PG pic (%) | base PG (Mo) | PGDATA pic (Mo) | WAL (Mo) | workdir pic (Mo) | statut |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | init | 1.0 | – | – | 58.9 | 101.7 | 8.2 | 7.7 | 45.6 | 0.5 | 0.0 | ok |
| 0 | ingest | 19.6 | 190 009 | 9 709.9 | 994.7 | 362.7 | 166.3 | 147.8 | 294.3 | 300.0 | 173.7 | ok |
| 0 | reconcile | 54.7 | 189 570 | 3 464.9 | 2 226.1 | 942.5 | 130.1 | 456.9 | 1 659.5 | 745.2 | 252.5 | ok |
| 1 | ingest | 14.3 | 127 976 | 8 952.8 | 745.8 | 945.4 | 94.8 | 459.4 | 1 552.1 | 8.4 | 50.8 | ok |
| 1 | reconcile | 13.3 | 3 096 | 232.4 | 520.9 | 932.0 | 36.8 | 462.5 | 1 559.5 | 11.2 | 16.3 | ok |
| 2 | ingest | 14.2 | 130 913 | 9 207.6 | 844.2 | 933.7 | 19.7 | 469.5 | 1 575.3 | 18.9 | 61.4 | ok |
| 2 | reconcile | 16.6 | 10 692 | 642.8 | 590.7 | 938.1 | 40.2 | 478.8 | 1 638.0 | 34.2 | 47.8 | ok |
| 3 | ingest | 14.9 | 140 922 | 9 449.0 | 884.8 | 940.4 | 89.6 | 492.6 | 1 660.7 | 68.8 | 58.4 | ok |
| 3 | reconcile | 29.6 | 39 310 | 1 327.5 | 1 510.6 | 959.3 | 96.3 | 521.0 | 1 834.3 | 112.8 | 118.7 | ok |
| 3 | export | 5.9 | – | – | 407.2 | 947.8 | 96.6 | 521.0 | 1 855.6 | 0.5 | 0.0 | ok |

<details><summary>commandes exactes</summary>

```text
python -m engine.lanes.elt init --config <USER_HOME>\AppData\Local\Temp\mdm2-bench\ds-44f125e514ea\config.yml
python -m engine.lanes.elt ingest --config <USER_HOME>\AppData\Local\Temp\mdm2-bench\ds-44f125e514ea\config.yml --source crm --source erp --source web --run-id d0
python -m engine.lanes.elt reconcile --config <USER_HOME>\AppData\Local\Temp\mdm2-bench\ds-44f125e514ea\config.yml --run-id d0
python -m engine.lanes.elt ingest --config <USER_HOME>\AppData\Local\Temp\mdm2-bench\ds-44f125e514ea\config.yml --source crm --source erp --source web --run-id d1
python -m engine.lanes.elt reconcile --config <USER_HOME>\AppData\Local\Temp\mdm2-bench\ds-44f125e514ea\config.yml --run-id d1
python -m engine.lanes.elt ingest --config <USER_HOME>\AppData\Local\Temp\mdm2-bench\ds-44f125e514ea\config.yml --source crm --source erp --source web --run-id d2
python -m engine.lanes.elt reconcile --config <USER_HOME>\AppData\Local\Temp\mdm2-bench\ds-44f125e514ea\config.yml --run-id d2
python -m engine.lanes.elt ingest --config <USER_HOME>\AppData\Local\Temp\mdm2-bench\ds-44f125e514ea\config.yml --source crm --source erp --source web --run-id d3
python -m engine.lanes.elt reconcile --config <USER_HOME>\AppData\Local\Temp\mdm2-bench\ds-44f125e514ea\config.yml --run-id d3
python -m engine.lanes.elt export --config <USER_HOME>\AppData\Local\Temp\mdm2-bench\ds-44f125e514ea\config.yml --out <USER_HOME>\AppData\Local\Temp\mdm2-bench\exports\elt-L1\day3
```
</details>

### elt-L2-retry1 — elt 300 000 nœuds × 200 attributs  2 jour(s) delta

Statut : **aborted** — step_timeout: 90.0 min > 90 min

| clé | valeur |
|---|---|
| date | 2026-11-13T06:03:33+01:00 |
| machine | 32 threads, 63.7 Go RAM, Windows-11-10.0.26300-SP0 |
| versions | python 3.12.10, duckdb 2.0.0, pyarrow 26.0.1, psycopg 3.3.6, dbt-core 1.12.7, dbt-duckdb 1.11.1, docker 29.1.3 |
| PostgreSQL | 16.4 (Debian 16.4-1.pgdg120+2) (postgres:16.4-bookworm) |
| config | hash 9698835ff5da, graine 42, sources ['parquet', 'parquet', 'parquet'] / ['full', 'full', 'delta'], ingest native, batch_rows config (50000) |
| deltas (jour 1..N) | upd 1.0% new 0.5% del 0.2% brk 0.1%; upd 5.0% new 0.5% del 0.2% brk 0.1% |
| réglages PG | {"memory": "12g", "shm_size": "1g", "shared_buffers": "3GB", "work_mem": "64MB", "maintenance_work_mem": "1GB", "effective_cache_size": "8GB", "max_wal_size": "16GB", "min_wal_size": "2GB", "checkpoint_timeout": "30min", "synchronous_commit": "off", "max_connections": 100} |
| client | ELT mem 3GB threads 4 ; ETL workers None |
| jeu de données | 0.99 Go, génération 19.2 s, RSS pic 1610.4 Mo |
| pic hôte (psutil used) | 51.37 Go (base avant run 25.42 Go) |

| jour | phase | mur (s) | lignes | lignes/s | RSS client pic (Mo) | RAM PG pic (Mo) | CPU PG pic (%) | base PG (Mo) | PGDATA pic (Mo) | WAL (Mo) | workdir pic (Mo) | statut |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | init | 1.1 | – | – | 78.6 | 102.1 | 9.3 | 7.8 | 45.6 | 0.6 | 0.0 | ok |
| 0 | ingest | 101.0 | 570 748 | 5 652.2 | 2 583.0 | 2 071.6 | 234.7 | 1 038.7 | 3 700.7 | 2 015.0 | 1 548.5 | ok |
| 0 | reconcile | 5 400.4 | – | – | 20 804.3 | 2 225.2 | 193.4 | 1 529.3 | 3 920.1 | 339.1 | 4 656.3 | aborted |

<details><summary>commandes exactes</summary>

```text
python -m engine.lanes.elt init --config <USER_HOME>\AppData\Local\Temp\mdm2-bench\ds-5724e112c91d\config.yml
python -m engine.lanes.elt ingest --config <USER_HOME>\AppData\Local\Temp\mdm2-bench\ds-5724e112c91d\config.yml --source crm --source erp --source web --run-id d0
python -m engine.lanes.elt reconcile --config <USER_HOME>\AppData\Local\Temp\mdm2-bench\ds-5724e112c91d\config.yml --run-id d0
```
</details>

### elt-L2 — elt 300 000 nœuds × 200 attributs  2 jour(s) delta

Statut : **aborted** — ram_budget: 28.0 GB > 28.0 GB

| clé | valeur |
|---|---|
| date | 2026-11-13T04:25:33+01:00 |
| machine | 32 threads, 63.7 Go RAM, Windows-11-10.0.26300-SP0 |
| versions | python 3.12.10, duckdb 2.0.0, pyarrow 26.0.1, psycopg 3.3.6, dbt-core 1.12.7, dbt-duckdb 1.11.1, docker 29.1.3 |
| PostgreSQL | 16.4 (Debian 16.4-1.pgdg120+2) (postgres:16.4-bookworm) |
| config | hash f1645cb7564d, graine 42, sources ['parquet', 'parquet', 'parquet'] / ['full', 'full', 'delta'], ingest native, batch_rows config (50000) |
| deltas (jour 1..N) | upd 1.0% new 0.5% del 0.2% brk 0.1%; upd 5.0% new 0.5% del 0.2% brk 0.1% |
| réglages PG | {"memory": "12g", "shm_size": "1g", "shared_buffers": "3GB", "work_mem": "64MB", "maintenance_work_mem": "1GB", "effective_cache_size": "8GB", "max_wal_size": "16GB", "min_wal_size": "2GB", "checkpoint_timeout": "30min", "synchronous_commit": "off", "max_connections": 100} |
| client | ELT mem 6GB threads 8 ; ETL workers None |
| jeu de données | 0.99 Go, génération 19.1 s, RSS pic 1616.8 Mo |
| pic hôte (psutil used) | 56.74 Go (base avant run 27.36 Go) |

| jour | phase | mur (s) | lignes | lignes/s | RSS client pic (Mo) | RAM PG pic (Mo) | CPU PG pic (%) | base PG (Mo) | PGDATA pic (Mo) | WAL (Mo) | workdir pic (Mo) | statut |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | init | 1.1 | – | – | 78.6 | 101.9 | 9.2 | 7.8 | 45.6 | 0.6 | 0.0 | ok |
| 0 | ingest | 124.0 | 570 748 | 4 604.8 | 2 921.3 | 2 136.1 | 189.8 | 1 036.8 | 3 677.9 | 2 013.5 | 1 706.7 | ok |
| 0 | reconcile | 130.6 | – | – | 26 447.0 | 2 255.9 | 243.1 | 1 527.2 | 3 917.7 | 338.8 | 1 422.6 | aborted |

<details><summary>commandes exactes</summary>

```text
python -m engine.lanes.elt init --config <USER_HOME>\AppData\Local\Temp\mdm2-bench\ds-5724e112c91d\config.yml
python -m engine.lanes.elt ingest --config <USER_HOME>\AppData\Local\Temp\mdm2-bench\ds-5724e112c91d\config.yml --source crm --source erp --source web --run-id d0
python -m engine.lanes.elt reconcile --config <USER_HOME>\AppData\Local\Temp\mdm2-bench\ds-5724e112c91d\config.yml --run-id d0
```
</details>

### equiv-elt-2k — elt 2 000 nœuds × 30 attributs  2 jour(s) delta

Statut : **ok**

| clé | valeur |
|---|---|
| date | 2026-11-13T15:47:50+01:00 |
| machine | 32 threads, 63.7 Go RAM, Windows-11-10.0.26300-SP0 |
| versions | python 3.12.10, duckdb 2.0.0, pyarrow 26.0.1, psycopg 3.3.6, dbt-core 1.12.7, dbt-duckdb 1.11.1, docker 29.1.3 |
| PostgreSQL | 16.4 (Debian 16.4-1.pgdg120+2) (postgres:16.4-bookworm) |
| config | hash de6112bbadee, graine 7, sources ['parquet', 'parquet', 'parquet'] / ['full', 'full', 'delta'], ingest native, batch_rows config (50000) |
| deltas (jour 1..N) | upd 8.0% new 4.0% del 3.0% brk 2.0%; upd 8.0% new 4.0% del 3.0% brk 2.0% |
| réglages PG | {"memory": "4g", "shm_size": "1g", "shared_buffers": "512MB", "work_mem": "64MB", "maintenance_work_mem": "1GB", "effective_cache_size": "8GB", "max_wal_size": "16GB", "min_wal_size": "2GB", "checkpoint_timeout": "30min", "synchronous_commit": "off", "max_connections": 100} |
| client | ELT mem 2GB threads 2 ; ETL workers None |
| jeu de données | 0.00 Go, génération 7.6 s, RSS pic 295.5 Mo |
| pic hôte (psutil used) | 27.41 Go (base avant run 26.88 Go) |

| jour | phase | mur (s) | lignes | lignes/s | RSS client pic (Mo) | RAM PG pic (Mo) | CPU PG pic (%) | base PG (Mo) | PGDATA pic (Mo) | WAL (Mo) | workdir pic (Mo) | statut |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | init | 1.0 | – | – | 61.6 | 52.5 | 7.9 | 7.7 | 45.6 | 0.4 | 0.0 | ok |
| 0 | ingest | 11.0 | 3 761 | 342.4 | 710.5 | 60.5 | 24.1 | 10.4 | 49.2 | 5.4 | 13.1 | ok |
| 0 | reconcile | 12.2 | 3 754 | 308.3 | 343.0 | 79.7 | 14.9 | 18.4 | 69.3 | 18.1 | 11.1 | ok |
| 0 | export | 1.3 | – | – | 113.3 | 76.0 | 11.1 | 18.4 | 88.7 | 0.0 | 0.0 | ok |
| 1 | ingest | 8.3 | 2 730 | 329.4 | 713.3 | 83.2 | 11.4 | 18.7 | 88.7 | 1.6 | 0.4 | ok |
| 1 | reconcile | 11.9 | 526 | 44.1 | 304.6 | 78.3 | 7.7 | 20.2 | 90.5 | 3.8 | 4.7 | ok |
| 1 | export | 1.4 | – | – | 112.6 | 67.9 | 12.7 | 20.2 | 90.5 | 0.0 | 0.0 | ok |
| 2 | ingest | 8.4 | 2 762 | 327.7 | 709.3 | 84.1 | 8.6 | 20.6 | 90.5 | 1.0 | 0.4 | ok |
| 2 | reconcile | 11.8 | 528 | 44.8 | 362.5 | 80.4 | 9.5 | 21.8 | 92.5 | 3.4 | 4.6 | ok |
| 2 | export | 1.4 | – | – | 111.2 | 73.4 | 8.2 | 21.8 | 92.3 | 0.0 | 0.0 | ok |

<details><summary>commandes exactes</summary>

```text
python -m engine.lanes.elt init --config <USER_HOME>\AppData\Local\Temp\mdm2-bench\ds-bbc33848591b\config.yml
python -m engine.lanes.elt ingest --config <USER_HOME>\AppData\Local\Temp\mdm2-bench\ds-bbc33848591b\config.yml --source crm --source erp --source web --run-id d0
python -m engine.lanes.elt reconcile --config <USER_HOME>\AppData\Local\Temp\mdm2-bench\ds-bbc33848591b\config.yml --run-id d0
python -m engine.lanes.elt export --config <USER_HOME>\AppData\Local\Temp\mdm2-bench\ds-bbc33848591b\config.yml --out <USER_HOME>\AppData\Local\Temp\mdm2-bench\exports\equiv-elt-2k\day0
python -m engine.lanes.elt ingest --config <USER_HOME>\AppData\Local\Temp\mdm2-bench\ds-bbc33848591b\config.yml --source crm --source erp --source web --run-id d1
python -m engine.lanes.elt reconcile --config <USER_HOME>\AppData\Local\Temp\mdm2-bench\ds-bbc33848591b\config.yml --run-id d1
python -m engine.lanes.elt export --config <USER_HOME>\AppData\Local\Temp\mdm2-bench\ds-bbc33848591b\config.yml --out <USER_HOME>\AppData\Local\Temp\mdm2-bench\exports\equiv-elt-2k\day1
python -m engine.lanes.elt ingest --config <USER_HOME>\AppData\Local\Temp\mdm2-bench\ds-bbc33848591b\config.yml --source crm --source erp --source web --run-id d2
python -m engine.lanes.elt reconcile --config <USER_HOME>\AppData\Local\Temp\mdm2-bench\ds-bbc33848591b\config.yml --run-id d2
python -m engine.lanes.elt export --config <USER_HOME>\AppData\Local\Temp\mdm2-bench\ds-bbc33848591b\config.yml --out <USER_HOME>\AppData\Local\Temp\mdm2-bench\exports\equiv-elt-2k\day2
```
</details>

### equiv-etl-2k — etl 2 000 nœuds × 30 attributs  2 jour(s) delta

Statut : **ok**

| clé | valeur |
|---|---|
| date | 2026-11-13T15:46:24+01:00 |
| machine | 32 threads, 63.7 Go RAM, Windows-11-10.0.26300-SP0 |
| versions | python 3.12.10, duckdb 2.0.0, pyarrow 26.0.1, psycopg 3.3.6, dbt-core 1.12.7, dbt-duckdb 1.11.1, docker 29.1.3 |
| PostgreSQL | 16.4 (Debian 16.4-1.pgdg120+2) (postgres:16.4-bookworm) |
| config | hash 61fd77ef3f7a, graine 7, sources ['parquet', 'parquet', 'parquet'] / ['full', 'full', 'delta'], ingest parallel, batch_rows config (50000) |
| deltas (jour 1..N) | upd 8.0% new 4.0% del 3.0% brk 2.0%; upd 8.0% new 4.0% del 3.0% brk 2.0% |
| réglages PG | {"memory": "4g", "shm_size": "1g", "shared_buffers": "512MB", "work_mem": "64MB", "maintenance_work_mem": "1GB", "effective_cache_size": "8GB", "max_wal_size": "16GB", "min_wal_size": "2GB", "checkpoint_timeout": "30min", "synchronous_commit": "off", "max_connections": 100} |
| client | ELT mem None threads None ; ETL workers 8 |
| jeu de données | 0.00 Go, génération 7.6 s, RSS pic 295.5 Mo |
| pic hôte (psutil used) | 27.09 Go (base avant run 26.63 Go) |

| jour | phase | mur (s) | lignes | lignes/s | RSS client pic (Mo) | RAM PG pic (Mo) | CPU PG pic (%) | base PG (Mo) | PGDATA pic (Mo) | WAL (Mo) | workdir pic (Mo) | statut |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | init | 0.8 | – | – | 60.0 | 59.6 | 7.6 | 7.7 | 45.6 | 0.5 | 0.0 | ok |
| 0 | ingest | 1.1 | 3 761 | 3 322.1 | 284.5 | 66.1 | 25.0 | 10.5 | 46.0 | 4.0 | 0.0 | ok |
| 0 | reconcile | 2.2 | 3 754 | 1 682.1 | 95.3 | 101.1 | 64.4 | 19.3 | 48.8 | 12.1 | 0.0 | ok |
| 0 | export | 1.2 | – | – | 98.2 | 72.4 | 9.7 | 19.3 | 73.6 | 0.0 | 0.0 | ok |
| 1 | ingest | 1.0 | 2 730 | 2 644.0 | 181.2 | 75.1 | 14.9 | 19.9 | 73.6 | 0.9 | 0.0 | ok |
| 1 | reconcile | 1.2 | 526 | 440.4 | 84.8 | 75.9 | 18.0 | 21.1 | 74.2 | 2.3 | 0.0 | ok |
| 1 | export | 1.3 | – | – | 93.6 | 75.3 | 8.3 | 21.1 | 75.3 | 0.0 | 0.0 | ok |
| 2 | ingest | 1.0 | 2 762 | 2 701.1 | 182.0 | 77.3 | 14.6 | 21.5 | 75.3 | 0.9 | 0.0 | ok |
| 2 | reconcile | 1.2 | 528 | 441.6 | 84.1 | 76.4 | 17.1 | 22.7 | 75.8 | 2.1 | 0.0 | ok |
| 2 | export | 1.3 | – | – | 96.5 | 78.0 | 8.7 | 22.7 | 76.9 | 0.0 | 0.0 | ok |

<details><summary>commandes exactes</summary>

```text
python -m engine.lanes.etl init --config <USER_HOME>\AppData\Local\Temp\mdm2-bench\ds-bbc33848591b\config.yml
python -m engine.lanes.etl ingest --config <USER_HOME>\AppData\Local\Temp\mdm2-bench\ds-bbc33848591b\config.yml --source crm --run-id d0
python -m engine.lanes.etl ingest --config <USER_HOME>\AppData\Local\Temp\mdm2-bench\ds-bbc33848591b\config.yml --source erp --run-id d0
python -m engine.lanes.etl ingest --config <USER_HOME>\AppData\Local\Temp\mdm2-bench\ds-bbc33848591b\config.yml --source web --run-id d0
python -m engine.lanes.etl reconcile --config <USER_HOME>\AppData\Local\Temp\mdm2-bench\ds-bbc33848591b\config.yml --run-id d0
python -m engine.lanes.etl export --config <USER_HOME>\AppData\Local\Temp\mdm2-bench\ds-bbc33848591b\config.yml --out <USER_HOME>\AppData\Local\Temp\mdm2-bench\exports\equiv-etl-2k\day0
python -m engine.lanes.etl ingest --config <USER_HOME>\AppData\Local\Temp\mdm2-bench\ds-bbc33848591b\config.yml --source crm --run-id d1
python -m engine.lanes.etl ingest --config <USER_HOME>\AppData\Local\Temp\mdm2-bench\ds-bbc33848591b\config.yml --source erp --run-id d1
python -m engine.lanes.etl ingest --config <USER_HOME>\AppData\Local\Temp\mdm2-bench\ds-bbc33848591b\config.yml --source web --run-id d1
python -m engine.lanes.etl reconcile --config <USER_HOME>\AppData\Local\Temp\mdm2-bench\ds-bbc33848591b\config.yml --run-id d1
python -m engine.lanes.etl export --config <USER_HOME>\AppData\Local\Temp\mdm2-bench\ds-bbc33848591b\config.yml --out <USER_HOME>\AppData\Local\Temp\mdm2-bench\exports\equiv-etl-2k\day1
python -m engine.lanes.etl ingest --config <USER_HOME>\AppData\Local\Temp\mdm2-bench\ds-bbc33848591b\config.yml --source crm --run-id d2
python -m engine.lanes.etl ingest --config <USER_HOME>\AppData\Local\Temp\mdm2-bench\ds-bbc33848591b\config.yml --source erp --run-id d2
python -m engine.lanes.etl ingest --config <USER_HOME>\AppData\Local\Temp\mdm2-bench\ds-bbc33848591b\config.yml --source web --run-id d2
python -m engine.lanes.etl reconcile --config <USER_HOME>\AppData\Local\Temp\mdm2-bench\ds-bbc33848591b\config.yml --run-id d2
python -m engine.lanes.etl export --config <USER_HOME>\AppData\Local\Temp\mdm2-bench\ds-bbc33848591b\config.yml --out <USER_HOME>\AppData\Local\Temp\mdm2-bench\exports\equiv-etl-2k\day2
```
</details>

### equiv-graph-2k — graph 2 000 nœuds × 30 attributs  2 jour(s) delta

Statut : **ok**

| clé | valeur |
|---|---|
| date | 2026-11-13T15:48:26+01:00 |
| machine | 32 threads, 63.7 Go RAM, Windows-11-10.0.26300-SP0 |
| versions | python 3.12.10, duckdb 2.0.0, pyarrow 26.0.1, psycopg 3.3.6, dbt-core 1.12.7, dbt-duckdb 1.11.1, docker 29.1.3 |
| PostgreSQL | 16.10 (Debian 16.10-1.pgdg13+1) (apache/age:release_PG16_1.6.0) |
| config | hash 331425cb73aa, graine 7, sources ['parquet', 'parquet', 'parquet'] / ['full', 'full', 'delta'], ingest sequential, batch_rows config (50000) |
| deltas (jour 1..N) | upd 8.0% new 4.0% del 3.0% brk 2.0%; upd 8.0% new 4.0% del 3.0% brk 2.0% |
| réglages PG | {"memory": "4g", "shm_size": "1g", "shared_buffers": "512MB", "work_mem": "64MB", "maintenance_work_mem": "1GB", "effective_cache_size": "8GB", "max_wal_size": "16GB", "min_wal_size": "2GB", "checkpoint_timeout": "30min", "synchronous_commit": "off", "max_connections": 100} |
| client | ELT mem None threads None ; ETL workers None |
| jeu de données | 0.00 Go, génération 7.6 s, RSS pic 295.5 Mo |
| pic hôte (psutil used) | 26.79 Go (base avant run 26.63 Go) |

| jour | phase | mur (s) | lignes | lignes/s | RSS client pic (Mo) | RAM PG pic (Mo) | CPU PG pic (%) | base PG (Mo) | PGDATA pic (Mo) | WAL (Mo) | workdir pic (Mo) | statut |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | init | 1.0 | – | – | 52.5 | 81.2 | 11.1 | 8.8 | 46.2 | 0.7 | 0.0 | ok |
| 0 | ingest | 3.0 | 3 761 | 1 240.8 | 55.3 | 85.2 | 7.0 | 12.0 | 47.2 | 4.2 | 0.0 | ok |
| 0 | reconcile | 2.1 | 3 754 | 1 759.9 | 99.9 | 104.8 | 42.6 | 23.4 | 50.4 | 11.6 | 0.0 | ok |
| 0 | export | 1.7 | – | – | 87.1 | 123.6 | 37.8 | 23.4 | 77.7 | 0.0 | 0.0 | ok |
| 1 | ingest | 2.9 | 2 730 | 948.9 | 67.2 | 98.8 | 4.6 | 23.8 | 77.7 | 0.8 | 0.0 | ok |
| 1 | reconcile | 2.1 | 526 | 254.2 | 84.5 | 132.5 | 87.6 | 25.0 | 78.1 | 2.0 | 0.0 | ok |
| 1 | export | 1.4 | – | – | 82.9 | 154.4 | 38.1 | 25.0 | 79.4 | 0.0 | 0.0 | ok |
| 2 | ingest | 2.9 | 2 762 | 963.4 | 70.6 | 159.1 | 4.7 | 25.4 | 79.4 | 1.0 | 0.0 | ok |
| 2 | reconcile | 2.0 | 528 | 257.2 | 84.7 | 189.8 | 88.8 | 26.7 | 79.8 | 1.8 | 0.0 | ok |
| 2 | export | 1.4 | – | – | 84.1 | 162.1 | 39.3 | 26.7 | 97.1 | 0.0 | 0.0 | ok |

<details><summary>commandes exactes</summary>

```text
python -m engine.lanes.graph init --config <USER_HOME>\AppData\Local\Temp\mdm2-bench\ds-bbc33848591b\config.yml
python -m engine.lanes.graph ingest --config <USER_HOME>\AppData\Local\Temp\mdm2-bench\ds-bbc33848591b\config.yml --source crm --run-id d0
python -m engine.lanes.graph ingest --config <USER_HOME>\AppData\Local\Temp\mdm2-bench\ds-bbc33848591b\config.yml --source erp --run-id d0
python -m engine.lanes.graph ingest --config <USER_HOME>\AppData\Local\Temp\mdm2-bench\ds-bbc33848591b\config.yml --source web --run-id d0
python -m engine.lanes.graph reconcile --config <USER_HOME>\AppData\Local\Temp\mdm2-bench\ds-bbc33848591b\config.yml --run-id d0
python -m engine.lanes.graph export --config <USER_HOME>\AppData\Local\Temp\mdm2-bench\ds-bbc33848591b\config.yml --out <USER_HOME>\AppData\Local\Temp\mdm2-bench\exports\equiv-graph-2k\day0
python -m engine.lanes.graph ingest --config <USER_HOME>\AppData\Local\Temp\mdm2-bench\ds-bbc33848591b\config.yml --source crm --run-id d1
python -m engine.lanes.graph ingest --config <USER_HOME>\AppData\Local\Temp\mdm2-bench\ds-bbc33848591b\config.yml --source erp --run-id d1
python -m engine.lanes.graph ingest --config <USER_HOME>\AppData\Local\Temp\mdm2-bench\ds-bbc33848591b\config.yml --source web --run-id d1
python -m engine.lanes.graph reconcile --config <USER_HOME>\AppData\Local\Temp\mdm2-bench\ds-bbc33848591b\config.yml --run-id d1
python -m engine.lanes.graph export --config <USER_HOME>\AppData\Local\Temp\mdm2-bench\ds-bbc33848591b\config.yml --out <USER_HOME>\AppData\Local\Temp\mdm2-bench\exports\equiv-graph-2k\day1
python -m engine.lanes.graph ingest --config <USER_HOME>\AppData\Local\Temp\mdm2-bench\ds-bbc33848591b\config.yml --source crm --run-id d2
python -m engine.lanes.graph ingest --config <USER_HOME>\AppData\Local\Temp\mdm2-bench\ds-bbc33848591b\config.yml --source erp --run-id d2
python -m engine.lanes.graph ingest --config <USER_HOME>\AppData\Local\Temp\mdm2-bench\ds-bbc33848591b\config.yml --source web --run-id d2
python -m engine.lanes.graph reconcile --config <USER_HOME>\AppData\Local\Temp\mdm2-bench\ds-bbc33848591b\config.yml --run-id d2
python -m engine.lanes.graph export --config <USER_HOME>\AppData\Local\Temp\mdm2-bench\ds-bbc33848591b\config.yml --out <USER_HOME>\AppData\Local\Temp\mdm2-bench\exports\equiv-graph-2k\day2
```
</details>

### etl-L1 — etl 100 000 nœuds × 50 attributs  3 jour(s) delta

Statut : **ok**

| clé | valeur |
|---|---|
| date | 2026-11-13T04:08:17+01:00 |
| machine | 32 threads, 63.7 Go RAM, Windows-11-10.0.26300-SP0 |
| versions | python 3.12.10, duckdb 2.0.0, pyarrow 26.0.1, psycopg 3.3.6, dbt-core 1.12.7, dbt-duckdb 1.11.1, docker 29.1.3 |
| PostgreSQL | 16.4 (Debian 16.4-1.pgdg120+2) (postgres:16.4-bookworm) |
| config | hash 072b5146f987, graine 42, sources ['parquet', 'parquet', 'parquet'] / ['full', 'full', 'delta'], ingest parallel, batch_rows config (50000) |
| deltas (jour 1..N) | upd 1.0% new 0.5% del 0.2% brk 0.1%; upd 5.0% new 0.5% del 0.2% brk 0.1%; upd 20.0% new 0.5% del 0.2% brk 0.1% |
| réglages PG | {"memory": "12g", "shm_size": "1g", "shared_buffers": "3GB", "work_mem": "64MB", "maintenance_work_mem": "1GB", "effective_cache_size": "8GB", "max_wal_size": "16GB", "min_wal_size": "2GB", "checkpoint_timeout": "30min", "synchronous_commit": "off", "max_connections": 100} |
| client | ELT mem None threads None ; ETL workers 8 |
| jeu de données | 0.12 Go, génération 13.2 s, RSS pic 690.4 Mo |
| pic hôte (psutil used) | 27.6 Go (base avant run 25.15 Go) |

| jour | phase | mur (s) | lignes | lignes/s | RSS client pic (Mo) | RAM PG pic (Mo) | CPU PG pic (%) | base PG (Mo) | PGDATA pic (Mo) | WAL (Mo) | workdir pic (Mo) | statut |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | init | 0.8 | – | – | 60.5 | 129.6 | 7.5 | 7.7 | 45.6 | 0.5 | 0.0 | ok |
| 0 | ingest | 7.2 | 190 009 | 26 528.7 | 811.3 | 503.1 | 222.0 | 148.4 | 436.3 | 197.6 | 0.0 | ok |
| 0 | reconcile | 28.6 | 189 570 | 6 620.4 | 352.9 | 692.4 | 152.2 | 457.4 | 1 229.8 | 479.9 | 0.0 | ok |
| 1 | ingest | 3.3 | 127 976 | 38 819.6 | 564.8 | 600.1 | 31.2 | 459.6 | 1 167.6 | 4.5 | 0.0 | ok |
| 1 | reconcile | 3.1 | 3 096 | 1 008.5 | 87.5 | 641.4 | 92.6 | 462.8 | 1 185.9 | 6.4 | 0.0 | ok |
| 2 | ingest | 3.6 | 130 913 | 36 292.8 | 574.1 | 618.6 | 85.0 | 470.0 | 1 189.1 | 12.8 | 0.0 | ok |
| 2 | reconcile | 5.1 | 10 692 | 2 081.3 | 155.5 | 681.4 | 94.5 | 482.4 | 1 240.8 | 20.5 | 0.0 | ok |
| 3 | ingest | 4.2 | 140 922 | 33 403.1 | 702.8 | 710.4 | 174.6 | 510.5 | 1 240.8 | 46.4 | 0.0 | ok |
| 3 | reconcile | 11.8 | 39 310 | 3 316.5 | 245.7 | 735.3 | 99.5 | 544.2 | 1 440.3 | 67.1 | 0.0 | ok |
| 3 | export | 13.3 | – | – | 256.3 | 820.7 | 119.9 | 535.9 | 1 414.7 | 7.4 | 0.0 | ok |

<details><summary>commandes exactes</summary>

```text
python -m engine.lanes.etl init --config <USER_HOME>\AppData\Local\Temp\mdm2-bench\ds-44f125e514ea\config.yml
python -m engine.lanes.etl ingest --config <USER_HOME>\AppData\Local\Temp\mdm2-bench\ds-44f125e514ea\config.yml --source crm --run-id d0
python -m engine.lanes.etl ingest --config <USER_HOME>\AppData\Local\Temp\mdm2-bench\ds-44f125e514ea\config.yml --source erp --run-id d0
python -m engine.lanes.etl ingest --config <USER_HOME>\AppData\Local\Temp\mdm2-bench\ds-44f125e514ea\config.yml --source web --run-id d0
python -m engine.lanes.etl reconcile --config <USER_HOME>\AppData\Local\Temp\mdm2-bench\ds-44f125e514ea\config.yml --run-id d0
python -m engine.lanes.etl ingest --config <USER_HOME>\AppData\Local\Temp\mdm2-bench\ds-44f125e514ea\config.yml --source crm --run-id d1
python -m engine.lanes.etl ingest --config <USER_HOME>\AppData\Local\Temp\mdm2-bench\ds-44f125e514ea\config.yml --source erp --run-id d1
python -m engine.lanes.etl ingest --config <USER_HOME>\AppData\Local\Temp\mdm2-bench\ds-44f125e514ea\config.yml --source web --run-id d1
python -m engine.lanes.etl reconcile --config <USER_HOME>\AppData\Local\Temp\mdm2-bench\ds-44f125e514ea\config.yml --run-id d1
python -m engine.lanes.etl ingest --config <USER_HOME>\AppData\Local\Temp\mdm2-bench\ds-44f125e514ea\config.yml --source crm --run-id d2
python -m engine.lanes.etl ingest --config <USER_HOME>\AppData\Local\Temp\mdm2-bench\ds-44f125e514ea\config.yml --source erp --run-id d2
python -m engine.lanes.etl ingest --config <USER_HOME>\AppData\Local\Temp\mdm2-bench\ds-44f125e514ea\config.yml --source web --run-id d2
python -m engine.lanes.etl reconcile --config <USER_HOME>\AppData\Local\Temp\mdm2-bench\ds-44f125e514ea\config.yml --run-id d2
python -m engine.lanes.etl ingest --config <USER_HOME>\AppData\Local\Temp\mdm2-bench\ds-44f125e514ea\config.yml --source crm --run-id d3
python -m engine.lanes.etl ingest --config <USER_HOME>\AppData\Local\Temp\mdm2-bench\ds-44f125e514ea\config.yml --source erp --run-id d3
python -m engine.lanes.etl ingest --config <USER_HOME>\AppData\Local\Temp\mdm2-bench\ds-44f125e514ea\config.yml --source web --run-id d3
python -m engine.lanes.etl reconcile --config <USER_HOME>\AppData\Local\Temp\mdm2-bench\ds-44f125e514ea\config.yml --run-id d3
python -m engine.lanes.etl export --config <USER_HOME>\AppData\Local\Temp\mdm2-bench\ds-44f125e514ea\config.yml --out <USER_HOME>\AppData\Local\Temp\mdm2-bench\exports\etl-L1\day3
```
</details>

### etl-L2 — etl 300 000 nœuds × 200 attributs  2 jour(s) delta

Statut : **ok**

| clé | valeur |
|---|---|
| date | 2026-11-13T04:20:17+01:00 |
| machine | 32 threads, 63.7 Go RAM, Windows-11-10.0.26300-SP0 |
| versions | python 3.12.10, duckdb 2.0.0, pyarrow 26.0.1, psycopg 3.3.6, dbt-core 1.12.7, dbt-duckdb 1.11.1, docker 29.1.3 |
| PostgreSQL | 16.4 (Debian 16.4-1.pgdg120+2) (postgres:16.4-bookworm) |
| config | hash c0076c42fbd0, graine 42, sources ['parquet', 'parquet', 'parquet'] / ['full', 'full', 'delta'], ingest parallel, batch_rows config (50000) |
| deltas (jour 1..N) | upd 1.0% new 0.5% del 0.2% brk 0.1%; upd 5.0% new 0.5% del 0.2% brk 0.1% |
| réglages PG | {"memory": "12g", "shm_size": "1g", "shared_buffers": "3GB", "work_mem": "64MB", "maintenance_work_mem": "1GB", "effective_cache_size": "8GB", "max_wal_size": "16GB", "min_wal_size": "2GB", "checkpoint_timeout": "30min", "synchronous_commit": "off", "max_connections": 100} |
| client | ELT mem None threads None ; ETL workers 8 |
| jeu de données | 0.99 Go, génération 18.9 s, RSS pic 1606.7 Mo |
| pic hôte (psutil used) | 38.25 Go (base avant run 25.24 Go) |

| jour | phase | mur (s) | lignes | lignes/s | RSS client pic (Mo) | RAM PG pic (Mo) | CPU PG pic (%) | base PG (Mo) | PGDATA pic (Mo) | WAL (Mo) | workdir pic (Mo) | statut |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | init | 1.0 | – | – | 59.8 | 101.1 | 8.3 | 7.8 | 45.6 | 0.6 | 0.0 | ok |
| 0 | ingest | 50.1 | 570 748 | 11 385.5 | 3 653.4 | 2 829.3 | 320.1 | 1 036.6 | 3 241.5 | 1 147.3 | 0.0 | ok |
| 0 | reconcile | 191.5 | 564 902 | 2 950.3 | 999.9 | 2 766.8 | 199.9 | 3 606.0 | 8 965.6 | 3 722.6 | 0.0 | ok |
| 1 | ingest | 17.6 | 384 622 | 21 887.9 | 2 175.3 | 2 602.0 | 198.8 | 3 623.4 | 8 649.5 | 22.7 | 0.0 | ok |
| 1 | reconcile | 8.7 | 9 235 | 1 067.8 | 253.0 | 2 539.5 | 99.3 | 3 647.3 | 8 576.6 | 38.6 | 0.0 | ok |
| 2 | ingest | 18.6 | 393 432 | 21 150.3 | 2 285.3 | 2 673.7 | 162.3 | 3 702.2 | 8 760.8 | 73.2 | 0.0 | ok |
| 2 | reconcile | 18.1 | 31 891 | 1 758.7 | 604.0 | 2 667.5 | 111.3 | 3 782.9 | 8 938.8 | 120.2 | 0.0 | ok |

<details><summary>commandes exactes</summary>

```text
python -m engine.lanes.etl init --config <USER_HOME>\AppData\Local\Temp\mdm2-bench\ds-5724e112c91d\config.yml
python -m engine.lanes.etl ingest --config <USER_HOME>\AppData\Local\Temp\mdm2-bench\ds-5724e112c91d\config.yml --source crm --run-id d0
python -m engine.lanes.etl ingest --config <USER_HOME>\AppData\Local\Temp\mdm2-bench\ds-5724e112c91d\config.yml --source erp --run-id d0
python -m engine.lanes.etl ingest --config <USER_HOME>\AppData\Local\Temp\mdm2-bench\ds-5724e112c91d\config.yml --source web --run-id d0
python -m engine.lanes.etl reconcile --config <USER_HOME>\AppData\Local\Temp\mdm2-bench\ds-5724e112c91d\config.yml --run-id d0
python -m engine.lanes.etl ingest --config <USER_HOME>\AppData\Local\Temp\mdm2-bench\ds-5724e112c91d\config.yml --source crm --run-id d1
python -m engine.lanes.etl ingest --config <USER_HOME>\AppData\Local\Temp\mdm2-bench\ds-5724e112c91d\config.yml --source erp --run-id d1
python -m engine.lanes.etl ingest --config <USER_HOME>\AppData\Local\Temp\mdm2-bench\ds-5724e112c91d\config.yml --source web --run-id d1
python -m engine.lanes.etl reconcile --config <USER_HOME>\AppData\Local\Temp\mdm2-bench\ds-5724e112c91d\config.yml --run-id d1
python -m engine.lanes.etl ingest --config <USER_HOME>\AppData\Local\Temp\mdm2-bench\ds-5724e112c91d\config.yml --source crm --run-id d2
python -m engine.lanes.etl ingest --config <USER_HOME>\AppData\Local\Temp\mdm2-bench\ds-5724e112c91d\config.yml --source erp --run-id d2
python -m engine.lanes.etl ingest --config <USER_HOME>\AppData\Local\Temp\mdm2-bench\ds-5724e112c91d\config.yml --source web --run-id d2
python -m engine.lanes.etl reconcile --config <USER_HOME>\AppData\Local\Temp\mdm2-bench\ds-5724e112c91d\config.yml --run-id d2
```
</details>

### etl-L3-final — etl 1 000 000 nœuds × 200 attributs  2 jour(s) delta

Statut : **ok**

| clé | valeur |
|---|---|
| date | 2026-11-13T16:15:54+01:00 |
| machine | 32 threads, 63.7 Go RAM, Windows-11-10.0.26300-SP0 |
| versions | python 3.12.10, duckdb 2.0.0, pyarrow 26.0.1, psycopg 3.3.6, dbt-core 1.12.7, dbt-duckdb 1.11.1, docker 29.1.3 |
| PostgreSQL | 16.4 (Debian 16.4-1.pgdg120+2) (postgres:16.4-bookworm) |
| config | hash df11c4092ba0, graine 42, sources ['parquet', 'parquet', 'parquet'] / ['full', 'full', 'delta'], ingest sequential, batch_rows 25000 |
| deltas (jour 1..N) | upd 1.0% new 0.5% del 0.2% brk 0.1%; upd 5.0% new 0.5% del 0.2% brk 0.1% |
| réglages PG | {"memory": "8g", "shm_size": "1g", "shared_buffers": "2GB", "work_mem": "64MB", "maintenance_work_mem": "1GB", "effective_cache_size": "8GB", "max_wal_size": "8GB", "min_wal_size": "2GB", "checkpoint_timeout": "30min", "synchronous_commit": "off", "max_connections": 100} |
| client | ELT mem None threads None ; ETL workers 8 |
| jeu de données | 3.31 Go, génération 42.2 s, RSS pic 1845.4 Mo |
| pic hôte (psutil used) | 36.31 Go (base avant run 26.66 Go) |

| jour | phase | mur (s) | lignes | lignes/s | RSS client pic (Mo) | RAM PG pic (Mo) | CPU PG pic (%) | base PG (Mo) | PGDATA pic (Mo) | WAL (Mo) | workdir pic (Mo) | statut |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | init | 1.0 | – | – | 59.7 | 111.5 | 8.4 | 7.8 | 45.6 | 0.6 | 0.0 | ok |
| 0 | ingest | 348.7 | 1 900 732 | 5 450.3 | 2 039.1 | 3 461.1 | 200.9 | 3 398.1 | 8 485.4 | 3 804.3 | 0.0 | ok |
| 0 | reconcile | 788.2 | 1 881 296 | 2 386.9 | 1 346.2 | 5 399.6 | 198.4 | 11 947.1 | 21 723.0 | 12 503.9 | 0.0 | ok |
| 1 | ingest | 124.9 | 1 281 098 | 10 255.2 | 2 012.6 | 5 927.9 | 138.8 | 11 939.5 | 20 377.8 | 381.9 | 0.0 | ok |
| 1 | reconcile | 51.4 | 30 741 | 597.6 | 389.8 | 6 043.6 | 113.9 | 12 026.7 | 20 295.9 | 915.4 | 0.0 | ok |
| 2 | ingest | 145.8 | 1 310 431 | 8 989.7 | 2 080.4 | 6 507.5 | 132.1 | 12 212.2 | 20 667.5 | 992.6 | 0.0 | ok |
| 2 | reconcile | 110.0 | 106 257 | 965.9 | 471.1 | 6 857.7 | 112.5 | 12 490.0 | 20 868.7 | 1 422.6 | 0.0 | ok |

<details><summary>commandes exactes</summary>

```text
python -m engine.lanes.etl init --config <USER_HOME>\AppData\Local\Temp\mdm2-bench\run-etl-L3-final\config.yml
python -m engine.lanes.etl ingest --config <USER_HOME>\AppData\Local\Temp\mdm2-bench\run-etl-L3-final\config.yml --source crm --run-id d0
python -m engine.lanes.etl ingest --config <USER_HOME>\AppData\Local\Temp\mdm2-bench\run-etl-L3-final\config.yml --source erp --run-id d0
python -m engine.lanes.etl ingest --config <USER_HOME>\AppData\Local\Temp\mdm2-bench\run-etl-L3-final\config.yml --source web --run-id d0
python -m engine.lanes.etl reconcile --config <USER_HOME>\AppData\Local\Temp\mdm2-bench\run-etl-L3-final\config.yml --run-id d0
python -m engine.lanes.etl ingest --config <USER_HOME>\AppData\Local\Temp\mdm2-bench\run-etl-L3-final\config.yml --source crm --run-id d1
python -m engine.lanes.etl ingest --config <USER_HOME>\AppData\Local\Temp\mdm2-bench\run-etl-L3-final\config.yml --source erp --run-id d1
python -m engine.lanes.etl ingest --config <USER_HOME>\AppData\Local\Temp\mdm2-bench\run-etl-L3-final\config.yml --source web --run-id d1
python -m engine.lanes.etl reconcile --config <USER_HOME>\AppData\Local\Temp\mdm2-bench\run-etl-L3-final\config.yml --run-id d1
python -m engine.lanes.etl ingest --config <USER_HOME>\AppData\Local\Temp\mdm2-bench\run-etl-L3-final\config.yml --source crm --run-id d2
python -m engine.lanes.etl ingest --config <USER_HOME>\AppData\Local\Temp\mdm2-bench\run-etl-L3-final\config.yml --source erp --run-id d2
python -m engine.lanes.etl ingest --config <USER_HOME>\AppData\Local\Temp\mdm2-bench\run-etl-L3-final\config.yml --source web --run-id d2
python -m engine.lanes.etl reconcile --config <USER_HOME>\AppData\Local\Temp\mdm2-bench\run-etl-L3-final\config.yml --run-id d2
```
</details>

## Équivalence

### lanes-L1

| paire | résultat | écarts | s | RSS Mo |
|---|---|---|---|---|
| etl-vs-elt | égal | {} | 10.4 | 1 914.5 |
| etl-vs-elt+row_hash | égal | {} | 11.1 | 1 923.4 |

## Concurrence via engine.runtime

### elt-3x20k

```json
{
 "lane": "elt",
 "referentials": 3,
 "nodes": 20000,
 "attributes": 30,
 "sequential_wall_s": 149.06,
 "concurrent_wall_s": 55.18,
 "speedup": 2.7,
 "concurrent_step_concurrency_max": 3,
 "concurrent_run_concurrency_max": 3,
 "all_succeeded": true,
 "same_ref_serialization": [
  {
   "referential": "r1",
   "gap_s": 0.02,
   "ok": true
  }
 ],
 "state_equal_seq_vs_conc": true,
 "budget_mib": 9216,
 "profile_process_mib": 3072,
 "worker_tree_rss_peak_mib_concurrent": 1374.3,
 "sum_of_concurrent_step_peaks_mib": 1213.0,
 "step_peak_rss_mib_max": 460,
 "state_container_mem_peak_mib": 596.5,
 "total_peak_mib_concurrent": 1858.3,
 "within_budget": true,
 "validated": true
}
```

### etl-3x20k

```json
{
 "lane": "etl",
 "referentials": 3,
 "nodes": 20000,
 "attributes": 30,
 "sequential_wall_s": 36.08,
 "concurrent_wall_s": 14.84,
 "speedup": 2.43,
 "concurrent_step_concurrency_max": 3,
 "concurrent_run_concurrency_max": 3,
 "all_succeeded": true,
 "same_ref_serialization": [
  {
   "referential": "r1",
   "gap_s": 0.01,
   "ok": true
  }
 ],
 "state_equal_seq_vs_conc": true,
 "budget_mib": 9216,
 "profile_process_mib": 3072,
 "worker_tree_rss_peak_mib_concurrent": 632.0,
 "sum_of_concurrent_step_peaks_mib": 573.0,
 "step_peak_rss_mib_max": 194,
 "state_container_mem_peak_mib": 461.8,
 "total_peak_mib_concurrent": 1093.8,
 "within_budget": true,
 "validated": true
}
```

<!-- BENCH:GENERATED:END -->
