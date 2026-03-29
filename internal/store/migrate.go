package store

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	ternmigrate "github.com/jackc/tern/v2/migrate"

	"github.com/lumosimmo/market-relayer-go/internal/config"
)

//go:embed migrations/*.sql
var migrationFiles embed.FS

const versionTable = "schema_version"

func Migrate(ctx context.Context, cfg config.StoreConfig) (int64, error) {
	conn, err := openMigrationConn(ctx, cfg)
	if err != nil {
		return 0, err
	}
	defer conn.Close(ctx)

	return migrateConn(ctx, conn)
}

func currentMigrationVersion(ctx context.Context, cfg config.StoreConfig) (int64, error) {
	conn, err := openMigrationConn(ctx, cfg)
	if err != nil {
		return 0, err
	}
	defer conn.Close(ctx)

	return withMigrator(ctx, conn, func(migrator *ternmigrate.Migrator) (int64, error) {
		version, err := migrator.GetCurrentVersion(ctx)
		if err != nil {
			return 0, fmt.Errorf("store: load migration version: %w", err)
		}
		return int64(version), nil
	})
}

func currentMigrationVersionPool(ctx context.Context, pool *pgxpool.Pool) (int64, error) {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return 0, fmt.Errorf("store: acquire connection: %w", err)
	}
	defer conn.Release()

	return withMigrator(ctx, conn.Conn(), func(migrator *ternmigrate.Migrator) (int64, error) {
		version, err := migrator.GetCurrentVersion(ctx)
		if err != nil {
			return 0, fmt.Errorf("store: load migration version: %w", err)
		}
		return int64(version), nil
	})
}

func migrateConn(ctx context.Context, conn *pgx.Conn) (int64, error) {
	return withMigrator(ctx, conn, func(migrator *ternmigrate.Migrator) (int64, error) {
		if err := loadMigrations(migrator); err != nil {
			return 0, err
		}
		if err := migrator.Migrate(ctx); err != nil {
			return 0, fmt.Errorf("store: apply migrations: %w", err)
		}

		version, err := migrator.GetCurrentVersion(ctx)
		if err != nil {
			return 0, fmt.Errorf("store: load migration version: %w", err)
		}
		return int64(version), nil
	})
}

func loadMigrations(migrator *ternmigrate.Migrator) error {
	migrationFS, err := fs.Sub(migrationFiles, "migrations")
	if err != nil {
		return fmt.Errorf("store: access migrations: %w", err)
	}
	if err := migrator.LoadMigrations(migrationFS); err != nil {
		return fmt.Errorf("store: load migrations: %w", err)
	}
	return nil
}

func withMigrator(
	ctx context.Context,
	conn *pgx.Conn,
	fn func(*ternmigrate.Migrator) (int64, error),
) (int64, error) {
	migrator, err := ternmigrate.NewMigrator(ctx, conn, versionTable)
	if err != nil {
		return 0, fmt.Errorf("store: init migrator: %w", err)
	}

	return fn(migrator)
}

func migrationNames() ([]string, error) {
	migrationFS, err := fs.Sub(migrationFiles, "migrations")
	if err != nil {
		return nil, fmt.Errorf("store: access migrations: %w", err)
	}

	names, err := ternmigrate.FindMigrations(migrationFS)
	if err != nil {
		return nil, fmt.Errorf("store: read migrations: %w", err)
	}
	return names, nil
}

func latestMigrationVersion() int64 {
	names, err := migrationNames()
	if err != nil || len(names) == 0 {
		return 0
	}

	version, err := migrationVersion(names[len(names)-1])
	if err != nil {
		return 0
	}
	return version
}

func migrationVersion(name string) (int64, error) {
	prefix, _, ok := strings.Cut(name, "_")
	if !ok {
		return 0, fmt.Errorf("store: parse migration version %s: missing separator", name)
	}

	version, err := strconv.ParseInt(prefix, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("store: parse migration version %s: %w", name, err)
	}
	return version, nil
}

func openMigrationConn(parent context.Context, cfg config.StoreConfig) (*pgx.Conn, error) {
	connConfig, err := pgx.ParseConfig(cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("store: parse dsn: %w", err)
	}
	connConfig.ConnectTimeout = cfg.ConnectTimeout
	connConfig.RuntimeParams["lock_timeout"] = strconv.FormatInt(cfg.LockTimeout.Milliseconds(), 10)

	ctx, cancel := context.WithTimeout(parent, cfg.ConnectTimeout)
	defer cancel()

	conn, err := pgx.ConnectConfig(ctx, connConfig)
	if err != nil {
		return nil, fmt.Errorf("store: open connection: %w", err)
	}
	if err := conn.Ping(ctx); err != nil {
		conn.Close(ctx)
		return nil, fmt.Errorf("store: ping: %w", err)
	}
	return conn, nil
}
