ALTER TABLE cycle_records
    ADD COLUMN IF NOT EXISTS computed_at TIMESTAMPTZ;

UPDATE cycle_records
SET computed_at = COALESCE(
    (payload->'computed'->>'ComputedAt')::timestamptz,
    created_at
)
WHERE computed_at IS NULL;

ALTER TABLE cycle_records
    ALTER COLUMN computed_at SET NOT NULL;

DROP INDEX IF EXISTS cycle_records_market_record_idx;

ALTER TABLE cycle_records
    DROP CONSTRAINT IF EXISTS cycle_records_pkey;

ALTER TABLE cycle_records
    ADD CONSTRAINT cycle_records_pkey PRIMARY KEY (market, record_id);

CREATE INDEX IF NOT EXISTS cycle_records_market_computed_idx
    ON cycle_records (market, computed_at DESC, record_id DESC);
