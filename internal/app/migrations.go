package app

import (
	"context"

	"github.com/lumosimmo/market-relayer-go/internal/config"
	"github.com/lumosimmo/market-relayer-go/internal/store"
)

func MigrateStore(ctx context.Context, options Options) (int64, error) {
	cfg, err := loadStoreConfig(options)
	if err != nil {
		return 0, err
	}
	return store.Migrate(ctx, cfg.Store)
}

func CurrentStoreVersion(ctx context.Context, options Options) (int64, error) {
	cfg, err := loadStoreConfig(options)
	if err != nil {
		return 0, err
	}
	return store.CurrentVersion(ctx, cfg.Store)
}

func loadStoreConfig(options Options) (*config.Config, error) {
	cfg, _, err := loadRuntimeMetadata(options)
	if err != nil {
		return nil, err
	}
	return cfg, nil
}
