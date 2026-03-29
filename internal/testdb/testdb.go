package testdb

import (
	"context"
	"fmt"
	"math/rand/v2"
	"net/url"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/lumosimmo/market-relayer-go/internal/config"
)

const defaultBaseDSN = "postgres://postgres:postgres@127.0.0.1:55432/market_relayer_test?sslmode=disable"
const SkipPostgresTestsEnv = "MARKET_RELAYER_SKIP_POSTGRES_TESTS"

var schemaCleaner = regexp.MustCompile(`[^a-z0-9_]+`)

func StoreConfig(tb testing.TB) config.StoreConfig {
	tb.Helper()

	baseDSN := baseDSN()
	adminCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := pgx.Connect(adminCtx, baseDSN)
	if err != nil {
		if baseDSN == defaultBaseDSN {
			handleUnavailablePostgres(tb, err)
		}
		tb.Fatalf("pgx.Connect() error = %v", err)
	}
	defer func() {
		if closeErr := conn.Close(adminCtx); closeErr != nil {
			tb.Fatalf("conn.Close() error = %v", closeErr)
		}
	}()

	schema := uniqueSchema(tb.Name())
	if _, err := conn.Exec(adminCtx, `CREATE SCHEMA `+schemaIdentifier(schema)); err != nil {
		tb.Fatalf("CREATE SCHEMA %q error = %v", schema, err)
	}
	tb.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		cleanupConn, err := pgx.Connect(cleanupCtx, baseDSN)
		if err != nil {
			tb.Fatalf("pgx.Connect(cleanup) error = %v", err)
		}
		defer func() {
			if closeErr := cleanupConn.Close(cleanupCtx); closeErr != nil {
				tb.Fatalf("cleanupConn.Close() error = %v", closeErr)
			}
		}()
		if _, err := cleanupConn.Exec(cleanupCtx, `DROP SCHEMA IF EXISTS `+schemaIdentifier(schema)+` CASCADE`); err != nil {
			tb.Fatalf("DROP SCHEMA %q error = %v", schema, err)
		}
	})

	return config.DefaultStoreConfig(schemaDSN(baseDSN, schema))
}

func DSN(tb testing.TB) string {
	tb.Helper()
	return StoreConfig(tb).DSN
}

func baseDSN() string {
	if dsn := strings.TrimSpace(os.Getenv("MARKET_RELAYER_TEST_POSTGRES_DSN")); dsn != "" {
		return dsn
	}
	return defaultBaseDSN
}

func SkipPostgresTestsRequested() bool {
	value := strings.TrimSpace(os.Getenv(SkipPostgresTestsEnv))
	switch strings.ToLower(value) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

func handleUnavailablePostgres(tb testing.TB, err error) {
	tb.Helper()
	if SkipPostgresTestsRequested() {
		tb.Skipf("postgres unavailable for test store: %v", err)
	}
	tb.Fatalf(
		"postgres unavailable for test store: %v; start postgres or set %s=1 to skip postgres-backed tests",
		err,
		SkipPostgresTestsEnv,
	)
}

func schemaDSN(baseDSN string, schema string) string {
	parsed, err := url.Parse(baseDSN)
	if err != nil {
		panic(fmt.Sprintf("testdb: parse base dsn: %v", err))
	}
	query := parsed.Query()
	query.Set("search_path", schema)
	parsed.RawQuery = query.Encode()
	return parsed.String()
}

func uniqueSchema(name string) string {
	normalized := strings.ToLower(name)
	normalized = schemaCleaner.ReplaceAllString(normalized, "_")
	normalized = strings.Trim(normalized, "_")
	if normalized == "" {
		normalized = "test"
	}
	if len(normalized) > 40 {
		normalized = normalized[:40]
	}
	return fmt.Sprintf("t_%s_%x", normalized, rand.Uint64())
}

func schemaIdentifier(input string) string {
	return pgx.Identifier{input}.Sanitize()
}
