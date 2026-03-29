package store

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

func (db *DB) withTx(ctx context.Context, fn func(context.Context, pgx.Tx) error) error {
	if err := pgx.BeginFunc(ctx, db.pool, func(tx pgx.Tx) error {
		return fn(ctx, tx)
	}); err != nil {
		return fmt.Errorf("store: transaction: %w", err)
	}
	return nil
}
