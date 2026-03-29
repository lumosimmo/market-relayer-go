package store

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

var errLeaseLost = errors.New("store: lease is not held")

type leaseGuard func(context.Context, pgx.Tx) error

func (db *DB) TryAcquireLease(ctx context.Context, market string, owner string, ttl time.Duration) (Lease, bool, error) {
	now := time.Now().UTC()
	expiresAt := now.Add(ttl)
	var lease Lease
	err := db.pool.QueryRow(ctx, `
		INSERT INTO market_leases (market, owner, fencing_token, expires_at, updated_at)
		VALUES ($1, $2, 1, $3, $4)
		ON CONFLICT (market) DO UPDATE SET
		    owner = EXCLUDED.owner,
		    fencing_token = CASE
		        WHEN market_leases.owner = EXCLUDED.owner AND market_leases.expires_at > $4 THEN market_leases.fencing_token
		        ELSE market_leases.fencing_token + 1
		    END,
		    expires_at = EXCLUDED.expires_at,
		    updated_at = EXCLUDED.updated_at
		WHERE market_leases.expires_at <= $4 OR market_leases.owner = EXCLUDED.owner
		RETURNING market, owner, fencing_token, expires_at
	`, strings.TrimSpace(market), strings.TrimSpace(owner), expiresAt, now).Scan(
		&lease.Market,
		&lease.Owner,
		&lease.FencingToken,
		&lease.ExpiresAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		current, _, loadErr := db.CurrentLease(ctx, market)
		return current, false, loadErr
	}
	if err != nil {
		return Lease{}, false, fmt.Errorf("store: acquire lease: %w", err)
	}
	return lease, true, nil
}

func (db *DB) RenewLease(ctx context.Context, lease Lease, ttl time.Duration) (Lease, bool, error) {
	refreshed := Lease{}
	var renewed bool
	err := db.withTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		current, ok, err := loadLeaseForUpdate(ctx, tx, lease.Market)
		if err != nil {
			return err
		}
		now := time.Now().UTC()
		if !ok || current.Owner != lease.Owner || current.FencingToken != lease.FencingToken || !current.ExpiresAt.After(now) {
			return nil
		}
		refreshed = current
		refreshed.ExpiresAt = now.Add(ttl)
		renewed = true
		_, err = tx.Exec(ctx, `
			UPDATE market_leases
			SET expires_at = $2, updated_at = $3
			WHERE market = $1
		`, refreshed.Market, refreshed.ExpiresAt, now)
		return err
	})
	return refreshed, renewed, err
}

func (db *DB) ReleaseLease(ctx context.Context, lease Lease) error {
	return db.withTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `
			UPDATE market_leases
			SET expires_at = $4, updated_at = $4
			WHERE market = $1 AND owner = $2 AND fencing_token = $3
		`, lease.Market, lease.Owner, lease.FencingToken, time.Now().UTC().Add(-time.Millisecond))
		return err
	})
}

func (db *DB) CurrentLease(ctx context.Context, market string) (Lease, bool, error) {
	row := db.pool.QueryRow(ctx, `
		SELECT market, owner, fencing_token, expires_at
		FROM market_leases
		WHERE market = $1
	`, strings.TrimSpace(market))
	var lease Lease
	if err := row.Scan(&lease.Market, &lease.Owner, &lease.FencingToken, &lease.ExpiresAt); errors.Is(err, pgx.ErrNoRows) {
		return Lease{}, false, nil
	} else if err != nil {
		return Lease{}, false, fmt.Errorf("store: load current lease: %w", err)
	}
	if !lease.ExpiresAt.After(time.Now().UTC()) {
		return Lease{}, false, nil
	}
	return lease, true, nil
}

func loadLeaseForUpdate(ctx context.Context, tx pgx.Tx, market string) (Lease, bool, error) {
	var lease Lease
	if err := tx.QueryRow(ctx, `
		SELECT market, owner, fencing_token, expires_at
		FROM market_leases
		WHERE market = $1
		FOR UPDATE
	`, strings.TrimSpace(market)).Scan(&lease.Market, &lease.Owner, &lease.FencingToken, &lease.ExpiresAt); errors.Is(err, pgx.ErrNoRows) {
		return Lease{}, false, nil
	} else if err != nil {
		return Lease{}, false, fmt.Errorf("store: load lease: %w", err)
	}
	return lease, true, nil
}

func assertLease(ctx context.Context, tx pgx.Tx, lease Lease) error {
	current, ok, err := loadLeaseForUpdate(ctx, tx, lease.Market)
	if err != nil {
		return err
	}
	if !ok || current.Owner != lease.Owner || current.FencingToken != lease.FencingToken || !current.ExpiresAt.After(time.Now().UTC()) {
		return errLeaseLost
	}
	return nil
}
