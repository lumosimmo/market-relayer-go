package app

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lumosimmo/market-relayer-go/internal/clock"
	"github.com/lumosimmo/market-relayer-go/internal/config"
	"github.com/lumosimmo/market-relayer-go/internal/feeds"
	"github.com/lumosimmo/market-relayer-go/internal/fixedpoint"
	"github.com/lumosimmo/market-relayer-go/internal/metrics"
	"github.com/lumosimmo/market-relayer-go/internal/operator"
	"github.com/lumosimmo/market-relayer-go/internal/pricing"
	"github.com/lumosimmo/market-relayer-go/internal/relayer"
	"github.com/lumosimmo/market-relayer-go/internal/store"
	"github.com/lumosimmo/market-relayer-go/internal/testdb"
	"github.com/lumosimmo/market-relayer-go/internal/testsupport"
)

func TestAppLastMileBootstrapRecoverAndLifecycleBranches(t *testing.T) {
	t.Parallel()

	t.Run("run service shuts down immediately when channels are nil", func(t *testing.T) {
		t.Parallel()

		closed := 0
		if err := runService(context.Background(), nil, nil, func() error {
			closed++
			return nil
		}); err != nil {
			t.Fatalf("runService() error = %v, want nil", err)
		}
		if closed != 1 {
			t.Fatalf("close calls = %d, want 1", closed)
		}
	})

	t.Run("bootstrap surfaces store sink and build-loop failures", func(t *testing.T) {
		t.Parallel()

		configText := testsupport.ValidBootstrapConfigWithStoreDSN("://bad-dsn")
		if _, err := Bootstrap(context.Background(), Options{
			ConfigPath: testsupport.WriteConfig(t, configText),
			StoreOpen:  openTestStore,
		}); err == nil {
			t.Fatal("Bootstrap(invalid dsn) error = nil, want open failure")
		}

		sinkDir := t.TempDir()
		configText = testsupport.ValidBootstrapConfigWithStoreDSN(testdb.DSN(t))
		configText = strings.Replace(configText, "kind: http\n  url: http://127.0.0.1:18080/publish", "kind: file\n  path: "+sinkDir, 1)
		if _, err := Bootstrap(context.Background(), Options{
			ConfigPath: testsupport.WriteConfig(t, configText),
			StoreOpen:  openTestStore,
		}); err == nil {
			t.Fatal("Bootstrap(file sink directory) error = nil, want sink init failure")
		}

		configText = testsupport.ValidBootstrapConfigWithStoreDSN(testdb.DSN(t))
		if _, err := Bootstrap(context.Background(), Options{
			ConfigPath: testsupport.WriteConfig(t, configText),
			StoreOpen:  openTestStore,
			FeedBuild: feeds.BuildOptions{
				EndpointOverrides: map[string]string{
					"coinbase-primary": "://bad",
				},
			},
		}); err == nil {
			t.Fatal("Bootstrap(bad feed override) error = nil, want build-loop failure")
		}
	})

	t.Run("validate config and recover surface metadata and store failures", func(t *testing.T) {
		t.Parallel()

		if _, err := ValidateConfig(Options{
			ConfigPath: filepath.Join(t.TempDir(), "missing-config.yaml"),
			StoreOpen:  openTestStore,
		}); err == nil {
			t.Fatal("ValidateConfig(missing file) error = nil, want metadata load failure")
		}

		if _, err := Recover(context.Background(), Options{
			ConfigPath: filepath.Join(t.TempDir(), "missing-config.yaml"),
			StoreOpen:  openTestStore,
		}, RecoveryOptions{}); err == nil {
			t.Fatal("Recover(missing file) error = nil, want metadata load failure")
		}

		configText := testsupport.ValidBootstrapConfigWithStoreDSN("://bad-dsn")
		if _, err := Recover(context.Background(), Options{
			ConfigPath: testsupport.WriteConfig(t, configText),
			StoreOpen:  openTestStore,
		}, RecoveryOptions{
			Mode: operator.ModeExact,
		}); err == nil {
			t.Fatal("Recover(invalid dsn) error = nil, want store open failure")
		}
	})

	t.Run("build loops surfaces relayer construction failures", func(t *testing.T) {
		t.Parallel()

		cfg, err := config.LoadBytes([]byte(testsupport.ValidBootstrapConfig(t)), config.LoadOptions{})
		if err != nil {
			t.Fatalf("config.LoadBytes() error = %v", err)
		}
		cfg.Markets[0].SessionMode = config.SessionModeScheduled

		storeDB := openMigratedStoreForTest(t, testdb.DSN(t))
		t.Cleanup(func() {
			if closeErr := storeDB.Close(); closeErr != nil {
				t.Fatalf("store.Close() error = %v", closeErr)
			}
		})

		_, _, err = buildLoops(cfg, RuntimeMetadata{
			ConfigDigest:            "config-digest",
			PricingAlgorithmVersion: pricing.AlgorithmVersion,
		}, storeDB, appTestSink{}, feeds.BuildOptions{}, clock.Fixed{Time: time.Date(2026, 3, 27, 19, 0, 0, 0, time.UTC)}, metrics.NewRegistry(), nil)
		if err == nil {
			t.Fatal("buildLoops(scheduled session mode) error = nil, want relayer loop failure")
		}
	})

	t.Run("runtime state and start loops cover nil store empty market and loop error forwarding", func(t *testing.T) {
		t.Parallel()

		if (&runtimeState{}).storeLive(context.Background()) {
			t.Fatal("storeLive(nil store) = true, want false")
		}
		if got := coalesceMarket("", " ", "\t"); got != "" {
			t.Fatalf("coalesceMarket(blank) = %q, want empty string", got)
		}

		storeDB := openMigratedStoreForTest(t, testdb.DSN(t))
		t.Cleanup(func() {
			if closeErr := storeDB.Close(); closeErr != nil {
				t.Fatalf("store.Close() error = %v", closeErr)
			}
		})

		now := time.Date(2026, 3, 27, 19, 15, 0, 0, time.UTC)
		quote := appQuote(t, "coinbase", "BTCUSD", "65941.00", now)
		loop, err := relayer.NewLoop(relayer.LoopOptions{
			Market: config.MarketConfig{
				Symbol:                 "ETHUSD",
				SessionMode:            config.SessionModeAlwaysOpen,
				PublishInterval:        3 * time.Second,
				StalenessThreshold:     10 * time.Second,
				MaxFallbackAge:         30 * time.Second,
				ClampBPS:               50,
				DivergenceThresholdBPS: 250,
				PublishScale:           2,
				Sources:                []config.MarketSourceRef{{Name: "coinbase"}},
			},
			Metadata: store.Metadata{
				ConfigDigest:            "config-digest",
				PricingAlgorithmVersion: pricing.AlgorithmVersion,
				SchemaVersion:           store.SchemaVersion,
				EnvelopeVersion:         relayer.EnvelopeVersion,
			},
			Store: storeDB,
			Sink:  appNoopSink{},
			Sources: []feeds.Source{
				appStaticSource{name: "coinbase", quote: quote},
			},
			Clock: clock.Fixed{Time: now},
		})
		if err != nil {
			t.Fatalf("relayer.NewLoop() error = %v", err)
		}

		runtime := &Runtime{
			Loops: []*relayer.Loop{loop},
			state: &runtimeState{loopLive: map[string]bool{"ETHUSD": false}},
		}

		errCh := runtime.StartLoops(context.Background())
		select {
		case err := <-errCh:
			if err == nil || !strings.Contains(err.Error(), "does not match market") {
				t.Fatalf("StartLoops() error = %v, want forwarded loop error", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("StartLoops() did not forward loop error")
		}
	})

	t.Run("market window uses schedule-aware source for scheduled markets", func(t *testing.T) {
		t.Parallel()

		window, err := marketWindow(config.MarketConfig{
			Symbol:        "WTIUSD",
			SessionMode:   config.SessionModeScheduled,
			SessionSource: "pyth-wti",
		}, []feeds.Source{
			appScheduledSource{
				name: "pyth-wti",
				openAt: func(at time.Time) bool {
					return at.Minute()%2 == 0
				},
			},
		})
		if err != nil {
			t.Fatalf("marketWindow() error = %v", err)
		}
		if got := window(time.Date(2026, 3, 27, 12, 2, 0, 0, time.UTC)); got.State != "open" {
			t.Fatalf("marketWindow(open).State = %q, want open", got.State)
		}
		if got := window(time.Date(2026, 3, 27, 12, 3, 0, 0, time.UTC)); got.State != "external_stale" {
			t.Fatalf("marketWindow(closed).State = %q, want external_stale", got.State)
		}
	})
}

type appStaticSource struct {
	name  string
	quote feeds.Quote
	err   error
}

func (source appStaticSource) Name() string {
	return source.name
}

func (source appStaticSource) Fetch(context.Context, feeds.FetchBudget) (feeds.Quote, error) {
	return source.quote, source.err
}

type appScheduledSource struct {
	name   string
	openAt func(time.Time) bool
}

func (source appScheduledSource) Name() string {
	return source.name
}

func (source appScheduledSource) Fetch(context.Context, feeds.FetchBudget) (feeds.Quote, error) {
	return feeds.Quote{}, nil
}

func (source appScheduledSource) SessionOpenAt(at time.Time) bool {
	return source.openAt(at)
}

type appNoopSink struct{}

func (appNoopSink) Kind() string {
	return "noop"
}

func (appNoopSink) Publish(context.Context, store.PreparedPublication) (relayer.Ack, error) {
	return relayer.Ack{}, nil
}

func (appNoopSink) Close() error {
	return nil
}

func appQuote(t *testing.T, source string, symbol string, value string, at time.Time) feeds.Quote {
	t.Helper()

	parsed, err := fixedpoint.ParseDecimal(value)
	if err != nil {
		t.Fatalf("fixedpoint.ParseDecimal(%q) error = %v", value, err)
	}

	return feeds.Quote{
		Source:             source,
		Symbol:             symbol,
		Bid:                parsed,
		Ask:                parsed,
		Last:               parsed,
		Scale:              parsed.Scale,
		ReceivedAt:         at.UTC(),
		FreshnessBasisKind: feeds.FreshnessBasisReceivedAt,
		FreshnessBasisAt:   at.UTC(),
	}
}
