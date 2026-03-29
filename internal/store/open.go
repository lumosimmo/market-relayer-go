package store

import (
	"context"
	"fmt"
	"strconv"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/lumosimmo/market-relayer-go/internal/config"
)

type DB struct {
	pool *pgxpool.Pool
}

func Open(dsn string) (*DB, error) {
	return OpenContext(context.Background(), dsn)
}

func OpenContext(ctx context.Context, dsn string) (*DB, error) {
	return OpenConfigContext(ctx, config.DefaultStoreConfig(dsn))
}

func OpenConfig(cfg config.StoreConfig) (*DB, error) {
	return OpenConfigContext(context.Background(), cfg)
}

func OpenConfigContext(ctx context.Context, cfg config.StoreConfig) (*DB, error) {
	pool, err := openPool(ctx, cfg)
	if err != nil {
		return nil, err
	}

	db := &DB{pool: pool}
	current, err := currentMigrationVersionPool(ctx, pool)
	if err != nil {
		pool.Close()
		return nil, err
	}
	if current != latestMigrationVersion() {
		pool.Close()
		return nil, fmt.Errorf("store: schema version %d is not current; run migrations", current)
	}
	return db, nil
}

func CurrentVersion(ctx context.Context, cfg config.StoreConfig) (int64, error) {
	return currentMigrationVersion(ctx, cfg)
}

func (db *DB) Close() error {
	if db.pool != nil {
		db.pool.Close()
	}
	return nil
}

func (db *DB) Ping(ctx context.Context) error {
	if err := db.pool.Ping(ctx); err != nil {
		return fmt.Errorf("store: ping: %w", err)
	}
	return nil
}

func openPool(parent context.Context, cfg config.StoreConfig) (*pgxpool.Pool, error) {
	poolConfig, err := pgxpool.ParseConfig(cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("store: parse dsn: %w", err)
	}
	poolConfig.MaxConns = int32(cfg.MaxOpenConns)
	poolConfig.MinConns = cfg.MinOpenConns
	poolConfig.MaxConnLifetime = cfg.ConnMaxLifetime
	poolConfig.ConnConfig.RuntimeParams["lock_timeout"] = strconv.FormatInt(cfg.LockTimeout.Milliseconds(), 10)
	ctx, cancel := context.WithTimeout(parent, cfg.ConnectTimeout)
	defer cancel()
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, fmt.Errorf("store: open pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("store: ping: %w", err)
	}
	return pool, nil
}
