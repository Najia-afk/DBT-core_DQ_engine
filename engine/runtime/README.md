# engine.runtime : file, admission et workers asyncio

Sous-ensemble vertical de SPECIFICATIONS §5-6 : un run de référentiel (config `engine.core.config`)
est planifié en steps, persisté dans `pg-meta` (PostgreSQL), puis exécuté par un ou plusieurs
workers asyncio qui lancent la lane (`python -m engine.lanes.<lane> ...`, contrat MDM §6) en
sous-processus supervisés.

```text
submit (CLI/API sync) ──► pg-meta : runs / steps / attempts / write_groups / workers
                                   ▲  claim FOR UPDATE SKIP LOCKED (transaction courte)
worker (asyncio) ── admission ─────┘
   ├─ step = sous-processus lane (timeout, RSS arbre de processus, heartbeat, annulation)
   └─ reaper : propriétaires morts, heartbeats expirés
```

## Modules

| Fichier | Rôle |
|---|---|
| `schema.sql` | migration idempotente (`mdm_runtime`), verrou advisory pour les migrations concurrentes |
| `planner.py` | `build_plan` : `init?` → `ingest[source]` → `reconcile` → `export?` |
| `admission.py` | logique pure : plafonds de file, budgets hôte, rotation, vieillissement |
| `store.py` | accès pg-meta (psycopg sync, pool) : soumission, claim, fin de tentative, reaper |
| `executor.py` | sous-processus, surveillance RSS/timeout, arbre de processus (psutil) |
| `engine.py` | `RuntimeEngine` : boucle de claim, steps concurrents, reprise sur incident |
| `cli.py` | `python -m engine.runtime submit|worker|status|cancel|resolve` |
| `api.py` | fonctions de soumission/consultation/annulation (sync) |

Les accès base passent par `asyncio.to_thread` : sous Windows psycopg async exige la boucle
Selector, alors que les sous-processus asyncio exigent la boucle Proactor.

## Planification

`init` (si `include_init`) → un `ingest:<source>` par source (parallèles, plafonnés par
`referential.max_running_steps`) → `reconcile` (dépend de **tous** les ingests) → `export` (si
`include_export`, `export_out` requis). Un seul run actif par référentiel : le **groupe d'écriture**
est le référentiel (`write_groups`, clé primaire), acquis au premier claim du run et libéré
seulement quand le run est terminal. Les autres référentiels avancent en parallèle.
Paramètres d'un run : `include_init`, `include_export`, `export_out`, `sources`, `profile`,
`profile_by_kind`, `timeout_seconds`, `max_attempts` (défauts issus de `runtime:` de la config).

## Machine d'états

Run : `queued → running ⇄ waiting_resource → succeeded | succeeded_with_warnings | failed`;
`cancel_requested → cancelled`; `needs_attention` (jamais résolu automatiquement).
Step : `pending → running → succeeded | failed | cancelled | retry_wait | suspect`;
`retry_wait → running` après `not_before`; `skipped` pour les steps en aval d'un échec.
`succeeded_with_warnings` : un summary de lane avec `warnings` non nul ou un
lot bloqué (`status: blocked`, `blocked: true`, y compris dans les compteurs
par source). Un code de sortie 0 ne signifie donc pas que toutes les sources
sont fraîches et admissibles.

- **Claim** : une transaction courte sous verrou advisory d'admission (budgets par hôte, pas par
  worker), puis `SELECT ... FOR UPDATE SKIP LOCKED` sur run et step ; claim, génération,
  attempt et groupe d'écriture sont écrits atomiquement. Aucune transaction ne reste ouverte
  pendant le calcul.
- **Fencing** : chaque claim incrémente `steps.generation`; heartbeat, pid et fin de tentative
  sont conditionnés par `(owner, generation)`. Une perte confirmée ou un suspect incrémente aussi
  la génération : l'ancien propriétaire ne valide plus rien dans pg-meta.
- **Échec** : code ≠ 0, timeout, RSS dépassé, perte. Réessai borné (`max_attempts`) avec backoff
  exponentiel si le motif est dans `retry.retry_on`, le code n'est pas dans
  `non_retryable_exit_codes` et la lane n'a pas répondu `"retryable": false`.
- **Annulation** : `queued` → `cancelled` immédiat; sinon `cancel_requested`, le heartbeat le
  voit, l'arbre est arrêté (terminate puis kill), `cancelled` une fois plus rien ne tourne.
- **Reprise sur incident** (au démarrage et périodiquement) : propriétaire mort sur le même hôte
  (pid + date de création) → arbre restant tué ; arrêt confirmé ⇒ tentative `lost` (réessai borné),
  sinon `suspect`. Heartbeat expiré sans preuve de mort (autre hôte, worker vivant) ⇒ `suspect` et
  run `needs_attention` ; **le groupe d'écriture reste bloqué** jusqu'à
  `python -m engine.runtime resolve --run-id R --confirm-stopped` (décision opérateur).
- **Admission** : à la soumission, la demande existante (clé + hash) est cherchée avant le plafond
  (`max_queued_runs` hôte/référentiel → `AdmissionError` avec `retry_after`, CLI code 3);
  même clé + contenu différent → `IdempotencyConflict` (code 4). Au claim, un besoin supérieur à
  la capacité configurée échoue explicitement (`capacity_exceeded`). Rotation entre référentiels
  (le moins récemment servi d'abord) et vieillissement : un step prêt depuis `aging_seconds` passe
  devant et, s'il ne tient pas, bloque le départ de petits steps (priorité réservée, pas de
  jetons partiels).

Idempotence de soumission : `UNIQUE (referential, idempotency_key)`; le hash couvre référentiel,
lane, chemin et SHA-256 de la config, params.

## Configuration du runtime (YAML, `--runtime-config` ou `MDM_RUNTIME_CONFIG`)

DSN pg-meta : `--dsn` ou `MDM_META_DSN`. Tous les blocs sont optionnels et stricts (clé inconnue = erreur).

| Clé | Défaut | Sens |
|---|---|---|
| `host.job_memory_budget_mib` | 6144 | budget mémoire des sous-processus jobs |
| `host.max_running_steps` | 2 | slots d'exécution sur l'hôte |
| `host.max_threads` | 32 | budget de threads |
| `host.max_queued_runs` | 2000 | plafond de file (429) |
| `host.min_free_disk_gib` / `host.work_dir` | 30 / `.` | marge disque libre contrôlée au claim |
| `profiles.<nom>.process_memory_mib / duckdb_memory_mib / threads` | small/medium/large | budget RSS du processus + variables `MDM_*` transmises à la lane |
| `referential.max_running_steps` | 3 | ingests parallèles par référentiel |
| `referential.max_queued_runs` | 200 | plafond de file par référentiel |
| `scheduler.poll_interval_seconds / aging_seconds / reaper_interval_seconds / retry_after_seconds` | 0.5 / 300 / 5 / 30 | boucle, vieillissement, reaper, `Retry-After` |
| `heartbeat.interval_seconds / timeout_seconds` | 5 / 30 | heartbeat de tentative, expiration |
| `monitor.rss_interval_seconds / kill_grace_seconds / *_tail_bytes` | 1 / 5 / 4096, 65536 | mesure RSS (arbre inclus), délai terminate→kill, queues conservées |
| `retry.backoff_base_seconds / backoff_max_seconds / jitter` | 5 / 300 / 0.1 | backoff exponentiel |
| `retry.retry_on` | `exit_nonzero, lost` | motifs réessayables (`timeout`, `rss_exceeded` possibles) |
| `retry.non_retryable_exit_codes` | `[2]` | codes de sortie jamais réessayés |
| `lane.command` / `lane.python` / `lane.env` / `lane.cwd` | `{python} -m engine.lanes.{lane}` | gabarit du préfixe de commande ; le moteur ajoute `<cmd> --config C [--source S] [--out D] --run-id R` |

La lane reçoit aussi `MDM_RUN_ID`, `MDM_STEP`, `MDM_ATTEMPT`, `MDM_PROFILE`, `MDM_THREADS`,
`MDM_PROCESS_MEMORY_MIB`, `MDM_DUCKDB_MEMORY_MIB`. La synthèse est la dernière ligne JSON de
stdout ; code 0 sans synthèse valide = échec `invalid_summary` (non réessayé).

## Tests

`pytest tests/runtime` : planner, admission, réglages (sans Docker). Avec `MDM_TEST_DOCKER=1`,
la fixture lance `postgres:16.4-bookworm` (`--memory 3g --memory-swap 3g`, conteneur
`mdm2-runtime-pg-*`, port aléatoire, supprimé en fin de session) et exécute les scénarios avec
`tests/runtime/fake_lane.py` (ordre, parallélisme borné, concurrence inter-référentiels,
retry, épuisement, timeout, RSS, annulation, crash du worker, fencing, idempotence, admission,
équité, CLI).

## Écarts connus par rapport à SPECIFICATIONS (honnête)

- Pas d'API HTTP FastAPI (donc pas de 202/200/409/429/503 réels) : fonctions Python et CLI ;
  pas d'authentification ni de quotas par tenant (le « tenant » = référentiel).
- Groupe d'écriture = référentiel entier ; pas de groupes déclarés par sortie/étoile ni de
  protection des sources partagées.
- Admission : mémoire par profil, slots, threads, disque libre, plafonds de file. Absents :
  estimation de travail disque par step, quota de connexions/cibles, limites `work_mem`,
  `statement_timeout`/`lock_timeout` côté base, CPU, `max_reserved_work_gib`.
- Le RSS est surveillé (arbre inclus) par échantillonnage, pas une limite OS/cgroup/Job Object :
  un pic plus rapide que `rss_interval_seconds` peut passer.
- Hôte unique : la reprise confirme l'arrêt de l'arbre par pid + argv. Les sessions PostgreSQL
  par tentative et les opérations externes (Airbyte/SFTP) ne sont pas identifiées ni
  réconciliées ; le fencing ne protège que pg-meta, pas les effets dans la cible. Multi-hôtes non
  supporté (budgets et reprise par hôte).
- Pas de stratégies de publication (§6.4), pas d'audit, de rétention, de lignage ni d'exposition
  des métriques ; pas de dérivation du plan depuis des datasets/dbt (planner fixe MDM).
- `needs_attention` ne se résout que par l'opérateur (`resolve`) ; pas de réconciliation automatique.
- Admission serialisée par un verrou advisory global : suffisant pour une file de quelques
  milliers de runs, pas dimensionné pour un très fort débit de claims.
- Tests Postgres sous Windows/Docker Desktop seulement validés ; durées temporisées courtes
  (heartbeat 0,2 s) sensibles à une machine très chargée.
