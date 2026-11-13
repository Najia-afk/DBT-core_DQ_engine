-- pg-meta : file persistante du runtime MDM (sous-ensemble vertical de SPECIFICATIONS §5-6).
-- Migration idempotente : peut être rejouée et lancée par plusieurs workers en parallèle.

CREATE SCHEMA IF NOT EXISTS mdm_runtime;
SET LOCAL search_path = mdm_runtime;

CREATE TABLE IF NOT EXISTS schema_version (
    version    integer PRIMARY KEY,
    applied_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS runs (
    run_id              text PRIMARY KEY,
    seq                 bigint GENERATED ALWAYS AS IDENTITY,
    referential         text NOT NULL,
    lane                text NOT NULL,
    config_path         text NOT NULL,
    idempotency_key     text NOT NULL,
    request_hash        text NOT NULL,
    status              text NOT NULL DEFAULT 'queued'
        CHECK (status IN ('queued', 'running', 'waiting_resource', 'succeeded',
                          'succeeded_with_warnings', 'failed', 'cancel_requested',
                          'cancelled', 'needs_attention')),
    status_reason       text,
    params              jsonb NOT NULL DEFAULT '{}'::jsonb,
    queued_at           timestamptz NOT NULL DEFAULT now(),
    started_at          timestamptz,
    ended_at            timestamptz,
    cancel_requested_at timestamptz,
    UNIQUE (referential, idempotency_key)
);
CREATE INDEX IF NOT EXISTS runs_active_idx ON runs (referential, queued_at, seq)
    WHERE status IN ('queued', 'running', 'waiting_resource', 'cancel_requested');

CREATE TABLE IF NOT EXISTS steps (
    run_id          text NOT NULL REFERENCES runs (run_id) ON DELETE CASCADE,
    step            text NOT NULL,
    ordinal         integer NOT NULL DEFAULT 0,
    kind            text NOT NULL,
    source_id       text,
    depends_on      text[] NOT NULL DEFAULT '{}',
    status          text NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'retry_wait', 'running', 'succeeded', 'failed',
                          'cancelled', 'skipped', 'suspect')),
    profile         text NOT NULL,
    attempt         integer NOT NULL DEFAULT 0,
    max_attempts    integer NOT NULL,
    generation      bigint NOT NULL DEFAULT 0,
    timeout_seconds integer NOT NULL,
    owner           text,
    host            text,
    pid             integer,
    pid_started     double precision,
    heartbeat_at    timestamptz,
    ready_at        timestamptz,
    not_before      timestamptz,
    started_at      timestamptz,
    ended_at        timestamptz,
    summary         jsonb,
    error           text,
    error_kind      text,
    wait_reason     text,
    peak_rss_mib    integer,
    PRIMARY KEY (run_id, step)
);
CREATE INDEX IF NOT EXISTS steps_claim_idx ON steps (status, ready_at)
    WHERE status IN ('pending', 'retry_wait', 'running');

CREATE TABLE IF NOT EXISTS attempts (
    run_id       text NOT NULL,
    step         text NOT NULL,
    attempt      integer NOT NULL,
    generation   bigint NOT NULL,
    owner        text NOT NULL,
    host         text NOT NULL,
    pid          integer,
    started_at   timestamptz NOT NULL DEFAULT now(),
    ended_at     timestamptz,
    outcome      text,
    exit_code    integer,
    error        text,
    summary      jsonb,
    peak_rss_mib integer,
    PRIMARY KEY (run_id, step, attempt),
    FOREIGN KEY (run_id, step) REFERENCES steps (run_id, step) ON DELETE CASCADE
);

-- Groupe d'écriture = référentiel : une seule ligne => un seul run propriétaire.
CREATE TABLE IF NOT EXISTS write_groups (
    group_name  text PRIMARY KEY,
    run_id      text NOT NULL REFERENCES runs (run_id),
    acquired_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS workers (
    worker_id    text PRIMARY KEY,
    host         text NOT NULL,
    pid          integer NOT NULL,
    pid_started  double precision NOT NULL,
    started_at   timestamptz NOT NULL DEFAULT now(),
    heartbeat_at timestamptz NOT NULL DEFAULT now(),
    stopped_at   timestamptz
);

CREATE TABLE IF NOT EXISTS referential_rotation (
    referential   text PRIMARY KEY,
    last_claim_at timestamptz NOT NULL
);

INSERT INTO schema_version (version) VALUES (1) ON CONFLICT DO NOTHING;
