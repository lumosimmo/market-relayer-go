package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/lumosimmo/market-relayer-go/internal/pricing"
)

func (db *DB) VerifyExact(ctx context.Context, query ExactQuery) (CycleRecord, error) {
	record, err := db.loadCycleRecord(ctx, query.Market, query.RecordID)
	if err != nil {
		return CycleRecord{}, err
	}
	mismatches := metadataMismatches(record.Metadata, query.Metadata)
	if len(mismatches) > 0 {
		return CycleRecord{}, &MetadataMismatchError{Fields: mismatches}
	}
	return record, nil
}

func (db *DB) Audit(ctx context.Context, query ExactQuery) (AuditResult, error) {
	record, err := db.loadCycleRecord(ctx, query.Market, query.RecordID)
	if err != nil {
		return AuditResult{}, err
	}
	mismatches := metadataMismatches(record.Metadata, query.Metadata)
	return AuditResult{
		Record:        record,
		MetadataMatch: len(mismatches) == 0,
		Mismatches:    mismatches,
	}, nil
}

func (db *DB) SafeSequence(ctx context.Context, market string) (uint64, error) {
	runtime, err := db.loadRuntime(ctx, strings.TrimSpace(market))
	if err != nil {
		return 0, err
	}
	return runtime.SafeSequence, nil
}

func (db *DB) LatestState(ctx context.Context, market string) (pricing.State, error) {
	runtime, err := db.loadRuntime(ctx, strings.TrimSpace(market))
	if err != nil {
		return pricing.State{}, err
	}
	return runtime.State, nil
}

func (db *DB) LatestPublishability(ctx context.Context, market string) (PublishabilityState, error) {
	runtime, err := db.loadRuntime(ctx, strings.TrimSpace(market))
	if err != nil {
		return PublishabilityState{}, err
	}
	return runtime.Publishability, nil
}

func (db *DB) SavePublishability(ctx context.Context, state PublishabilityState) error {
	return db.savePublishability(ctx, nil, state)
}

func (db *DB) SavePublishabilityWithLease(ctx context.Context, lease Lease, state PublishabilityState) error {
	return db.savePublishability(ctx, func(ctx context.Context, tx pgx.Tx) error {
		return assertLease(ctx, tx, lease)
	}, state)
}

func (db *DB) LatestSnapshot(ctx context.Context, market string) (Snapshot, error) {
	record, err := db.latestCycle(ctx, strings.TrimSpace(market))
	if err != nil {
		return Snapshot{}, err
	}
	runtime, err := db.loadRuntime(ctx, strings.TrimSpace(market))
	if err != nil {
		return Snapshot{}, err
	}
	recent, err := db.RecentCycles(ctx, strings.TrimSpace(market), 5)
	if err != nil {
		return Snapshot{}, err
	}
	lastSuccessful, err := db.lastSuccessfulPublication(ctx, strings.TrimSpace(market))
	if err != nil {
		return Snapshot{}, err
	}
	return Snapshot{
		SchemaVersion:             SchemaVersion,
		Record:                    record,
		Publishability:            runtime.Publishability,
		LastSuccessfulPublication: lastSuccessful,
		RecentCycles:              recent,
	}, nil
}

func (db *DB) RecentCycles(ctx context.Context, market string, limit int) ([]CycleRecord, error) {
	rows, err := db.pool.Query(ctx, `
		SELECT payload
		FROM cycle_records
		WHERE market = $1
		ORDER BY computed_at DESC, record_id DESC
		LIMIT $2
	`, strings.TrimSpace(market), limit)
	if err != nil {
		return nil, fmt.Errorf("store: load recent cycles: %w", err)
	}
	defer rows.Close()

	records := make([]CycleRecord, 0, limit)
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return nil, fmt.Errorf("store: load recent cycles: %w", err)
		}
		var record CycleRecord
		if err := json.Unmarshal(raw, &record); err != nil {
			return nil, fmt.Errorf("store: decode recent cycle: %w", err)
		}
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: load recent cycles: %w", err)
	}
	return records, nil
}

func (db *DB) latestCycle(ctx context.Context, market string) (CycleRecord, error) {
	var raw []byte
	if err := db.pool.QueryRow(ctx, `
		SELECT payload
		FROM cycle_records
		WHERE market = $1
		ORDER BY computed_at DESC, record_id DESC
		LIMIT 1
	`, market).Scan(&raw); errors.Is(err, pgx.ErrNoRows) {
		return CycleRecord{}, fmt.Errorf("%w: %s", ErrSnapshotNotFound, market)
	} else if err != nil {
		return CycleRecord{}, fmt.Errorf("store: load latest cycle: %w", err)
	}
	var record CycleRecord
	if err := json.Unmarshal(raw, &record); err != nil {
		return CycleRecord{}, fmt.Errorf("store: decode latest cycle: %w", err)
	}
	return record, nil
}

func (db *DB) lastSuccessfulPublication(ctx context.Context, market string) (*PreparedPublication, error) {
	rows, err := db.pool.Query(ctx, `
		SELECT market, sequence, state, prepared_at, sent_at, acked_at, idempotency_key, payload_hash, envelope_version, envelope
		FROM publications
		WHERE market = $1 AND state = 'acked'
		ORDER BY sequence DESC
		LIMIT 1
	`, market)
	if err != nil {
		return nil, fmt.Errorf("store: load last successful publication: %w", err)
	}
	defer rows.Close()
	if !rows.Next() {
		return nil, nil
	}
	publication, err := scanPublication(rows)
	if err != nil {
		return nil, err
	}
	return &publication, nil
}

func (db *DB) savePublishability(ctx context.Context, guard leaseGuard, state PublishabilityState) error {
	return db.withTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		if guard != nil {
			if err := guard(ctx, tx); err != nil {
				return err
			}
		}
		payload, err := marshalJSON(state)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO market_runtime (market, latest_publishability, last_stable_hash, pending_sequence, updated_at)
			VALUES ($1, $2, $3, $4, $5)
			ON CONFLICT (market) DO UPDATE SET
			    latest_publishability = EXCLUDED.latest_publishability,
			    last_stable_hash = EXCLUDED.last_stable_hash,
			    pending_sequence = EXCLUDED.pending_sequence,
			    updated_at = EXCLUDED.updated_at
		`, state.Market, payload, state.LastStableHash, int64(state.PendingSequence), state.UpdatedAt.UTC()); err != nil {
			return fmt.Errorf("store: save publishability: %w", err)
		}
		return nil
	})
}

func metadataMismatches(actual Metadata, expected Metadata) []string {
	mismatches := make([]string, 0, 4)
	if actual.ConfigDigest != expected.ConfigDigest {
		mismatches = append(mismatches, "config_digest")
	}
	if actual.PricingAlgorithmVersion != expected.PricingAlgorithmVersion {
		mismatches = append(mismatches, "pricing_algorithm_version")
	}
	if actual.SchemaVersion != expected.SchemaVersion {
		mismatches = append(mismatches, "schema_version")
	}
	if actual.EnvelopeVersion != expected.EnvelopeVersion {
		mismatches = append(mismatches, "envelope_version")
	}
	return mismatches
}

func marshalJSON(value any) ([]byte, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("store: marshal json: %w", err)
	}
	return raw, nil
}
