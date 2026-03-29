package app

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/lumosimmo/market-relayer-go/internal/config"
	"github.com/lumosimmo/market-relayer-go/internal/store"
)

func openTestStore(ctx context.Context, cfg config.StoreConfig) (runtimeStore, error) {
	if _, err := store.Migrate(ctx, cfg); err != nil {
		return nil, err
	}
	return store.OpenConfigContext(ctx, cfg)
}

func openMigratedStoreForTest(tb testing.TB, dsn string) *store.DB {
	tb.Helper()

	cfg := config.StoreConfig{
		DSN:                strings.TrimSpace(dsn),
		MaxOpenConns:       4,
		MinOpenConns:       0,
		ConnMaxLifetime:    30 * time.Minute,
		ConnectTimeout:     5 * time.Second,
		LockTimeout:        5 * time.Second,
		LeaseTTL:           15 * time.Second,
		LeaseRenewInterval: 5 * time.Second,
	}
	if _, err := store.Migrate(context.Background(), cfg); err != nil {
		tb.Fatalf("store.Migrate() error = %v", err)
	}

	db, err := store.OpenConfigContext(context.Background(), cfg)
	if err != nil {
		tb.Fatalf("store.OpenConfigContext() error = %v", err)
	}
	return db
}
