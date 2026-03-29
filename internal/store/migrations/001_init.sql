CREATE TABLE IF NOT EXISTS market_runtime (
    market TEXT PRIMARY KEY,
    latest_state JSONB NOT NULL DEFAULT '{}'::jsonb,
    latest_publishability JSONB NOT NULL DEFAULT '{}'::jsonb,
    last_stable_hash TEXT NOT NULL DEFAULT '',
    safe_sequence BIGINT NOT NULL DEFAULT 0,
    pending_sequence BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS cycle_records (
    market TEXT NOT NULL,
    record_id TEXT PRIMARY KEY,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS cycle_records_market_record_idx
    ON cycle_records (market, record_id DESC);

CREATE TABLE IF NOT EXISTS publications (
    market TEXT NOT NULL,
    sequence BIGINT NOT NULL,
    state TEXT NOT NULL,
    prepared_at TIMESTAMPTZ NOT NULL,
    sent_at TIMESTAMPTZ NULL,
    acked_at TIMESTAMPTZ NULL,
    idempotency_key TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    envelope_version TEXT NOT NULL,
    envelope BYTEA NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (market, sequence)
);

CREATE UNIQUE INDEX IF NOT EXISTS publications_unresolved_market_idx
    ON publications (market)
    WHERE state <> 'acked';

CREATE TABLE IF NOT EXISTS market_leases (
    market TEXT PRIMARY KEY,
    owner TEXT NOT NULL,
    fencing_token BIGINT NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
