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

type runtimeRow struct {
	State           pricing.State
	Publishability  PublishabilityState
	LastStableHash  string
	SafeSequence    uint64
	PendingSequence uint64
}

func (db *DB) loadRuntime(ctx context.Context, market string) (runtimeRow, error) {
	row := db.pool.QueryRow(ctx, `
		SELECT latest_state, latest_publishability, last_stable_hash, safe_sequence, pending_sequence
		FROM market_runtime
		WHERE market = $1
	`, market)

	var stateRaw []byte
	var publishabilityRaw []byte
	var runtime runtimeRow
	var safeSequence int64
	var pendingSequence int64
	if err := row.Scan(&stateRaw, &publishabilityRaw, &runtime.LastStableHash, &safeSequence, &pendingSequence); errors.Is(err, pgx.ErrNoRows) {
		return runtimeRow{}, nil
	} else if err != nil {
		return runtimeRow{}, fmt.Errorf("store: load runtime: %w", err)
	}
	if len(stateRaw) != 0 {
		if err := json.Unmarshal(stateRaw, &runtime.State); err != nil {
			return runtimeRow{}, fmt.Errorf("store: decode latest state: %w", err)
		}
	}
	if len(publishabilityRaw) != 0 {
		if err := json.Unmarshal(publishabilityRaw, &runtime.Publishability); err != nil {
			return runtimeRow{}, fmt.Errorf("store: decode latest publishability: %w", err)
		}
	}
	runtime.SafeSequence = uint64(safeSequence)
	runtime.PendingSequence = uint64(pendingSequence)
	return runtime, nil
}

func ensureRuntimeRow(ctx context.Context, tx pgx.Tx, market string) (runtimeRow, error) {
	if _, err := tx.Exec(ctx, `
		INSERT INTO market_runtime (market)
		VALUES ($1)
		ON CONFLICT (market) DO NOTHING
	`, strings.TrimSpace(market)); err != nil {
		return runtimeRow{}, fmt.Errorf("store: ensure runtime row: %w", err)
	}

	var stateRaw []byte
	var publishabilityRaw []byte
	var row runtimeRow
	var safeSequence int64
	var pendingSequence int64
	if err := tx.QueryRow(ctx, `
		SELECT latest_state, latest_publishability, last_stable_hash, safe_sequence, pending_sequence
		FROM market_runtime
		WHERE market = $1
		FOR UPDATE
	`, strings.TrimSpace(market)).Scan(&stateRaw, &publishabilityRaw, &row.LastStableHash, &safeSequence, &pendingSequence); err != nil {
		return runtimeRow{}, fmt.Errorf("store: lock runtime row: %w", err)
	}
	if len(stateRaw) != 0 {
		if err := json.Unmarshal(stateRaw, &row.State); err != nil {
			return runtimeRow{}, fmt.Errorf("store: decode runtime state: %w", err)
		}
	}
	if len(publishabilityRaw) != 0 {
		if err := json.Unmarshal(publishabilityRaw, &row.Publishability); err != nil {
			return runtimeRow{}, fmt.Errorf("store: decode runtime publishability: %w", err)
		}
	}
	row.SafeSequence = uint64(safeSequence)
	row.PendingSequence = uint64(pendingSequence)
	return row, nil
}
