package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

func (db *DB) CommitCycle(ctx context.Context, record CycleRecord) error {
	return db.commitCycle(ctx, nil, record)
}

func (db *DB) CommitCycleWithLease(ctx context.Context, lease Lease, record CycleRecord) error {
	return db.commitCycle(ctx, func(ctx context.Context, tx pgx.Tx) error {
		return assertLease(ctx, tx, lease)
	}, record)
}

func (db *DB) PreviousCycle(ctx context.Context, market string, recordID string) (CycleRecord, bool, error) {
	market = strings.TrimSpace(market)
	recordID = strings.TrimSpace(recordID)

	computedAt, ok, err := db.loadCycleComputedAt(ctx, market, recordID)
	if err != nil {
		return CycleRecord{}, false, err
	}
	if !ok {
		return CycleRecord{}, false, nil
	}

	var raw []byte
	err = db.pool.QueryRow(ctx, `
		SELECT payload
		FROM cycle_records
		WHERE market = $1 AND (computed_at, record_id) < ($2, $3)
		ORDER BY computed_at DESC, record_id DESC
		LIMIT 1
	`, market, computedAt, recordID).Scan(&raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return CycleRecord{}, false, nil
	}
	if err != nil {
		return CycleRecord{}, false, fmt.Errorf("store: load previous cycle: %w", err)
	}
	var record CycleRecord
	if err := json.Unmarshal(raw, &record); err != nil {
		return CycleRecord{}, false, fmt.Errorf("store: decode previous cycle: %w", err)
	}
	return record, true, nil
}

func (db *DB) loadCycleComputedAt(ctx context.Context, market string, recordID string) (time.Time, bool, error) {
	var computedAt time.Time
	err := db.pool.QueryRow(ctx, `
		SELECT computed_at
		FROM cycle_records
		WHERE market = $1 AND record_id = $2
	`, market, recordID).Scan(&computedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return time.Time{}, false, nil
	}
	if err != nil {
		return time.Time{}, false, fmt.Errorf("store: load cycle timestamp: %w", err)
	}
	return computedAt.UTC(), true, nil
}

func (db *DB) loadCycleRecord(ctx context.Context, market string, recordID string) (CycleRecord, error) {
	var raw []byte
	if err := db.pool.QueryRow(ctx, `
		SELECT payload
		FROM cycle_records
		WHERE market = $1 AND record_id = $2
	`, strings.TrimSpace(market), strings.TrimSpace(recordID)).Scan(&raw); errors.Is(err, pgx.ErrNoRows) {
		return CycleRecord{}, fmt.Errorf("store: cycle record %s/%s not found", market, recordID)
	} else if err != nil {
		return CycleRecord{}, fmt.Errorf("store: load cycle record: %w", err)
	}
	var record CycleRecord
	if err := json.Unmarshal(raw, &record); err != nil {
		return CycleRecord{}, fmt.Errorf("store: decode cycle record: %w", err)
	}
	return record, nil
}

func (db *DB) commitCycle(ctx context.Context, guard leaseGuard, record CycleRecord) error {
	computedAt := record.Computed.ComputedAt.UTC()
	if computedAt.IsZero() {
		return errors.New("store: cycle record computed time is required")
	}

	return db.withTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		if guard != nil {
			if err := guard(ctx, tx); err != nil {
				return err
			}
		}

		runtime, err := ensureRuntimeRow(ctx, tx, record.Market)
		if err != nil {
			return err
		}

		if record.PreparedPublication != nil {
			if runtime.PendingSequence != 0 {
				return fmt.Errorf("store: market %s already has unresolved publication %d", record.Market, runtime.PendingSequence)
			}
			expected := runtime.SafeSequence + 1
			if record.PreparedPublication.Sequence != expected {
				return fmt.Errorf("store: prepared publication sequence %d does not match next safe sequence %d", record.PreparedPublication.Sequence, expected)
			}
			if err := insertPublication(ctx, tx, *record.PreparedPublication); err != nil {
				return err
			}
			runtime.PendingSequence = record.PreparedPublication.Sequence
		}

		payload, err := marshalJSON(record)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO cycle_records (market, record_id, computed_at, payload)
			VALUES ($1, $2, $3, $4)
			ON CONFLICT (market, record_id) DO UPDATE SET
			    computed_at = EXCLUDED.computed_at,
			    payload = EXCLUDED.payload
		`, record.Market, record.RecordID, computedAt, payload); err != nil {
			return fmt.Errorf("store: write cycle record: %w", err)
		}

		statePayload, err := marshalJSON(record.Computed.State)
		if err != nil {
			return err
		}
		publishabilityPayload, err := marshalJSON(record.Publishability)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			UPDATE market_runtime
			SET latest_state = $2,
			    latest_publishability = $3,
			    last_stable_hash = $4,
			    pending_sequence = $5,
			    updated_at = $6
			WHERE market = $1
		`, record.Market, statePayload, publishabilityPayload, record.Publishability.LastStableHash, runtime.PendingSequence, record.Publishability.UpdatedAt.UTC()); err != nil {
			return fmt.Errorf("store: write runtime state: %w", err)
		}
		return nil
	})
}
