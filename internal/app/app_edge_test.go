package app

import (
	"context"
	"errors"
	"io"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lumosimmo/market-relayer-go/internal/api"
	"github.com/lumosimmo/market-relayer-go/internal/clock"
	"github.com/lumosimmo/market-relayer-go/internal/config"
	"github.com/lumosimmo/market-relayer-go/internal/feeds"
	"github.com/lumosimmo/market-relayer-go/internal/metrics"
	"github.com/lumosimmo/market-relayer-go/internal/operator"
	"github.com/lumosimmo/market-relayer-go/internal/relayer"
	"github.com/lumosimmo/market-relayer-go/internal/status"
	"github.com/lumosimmo/market-relayer-go/internal/store"
	"github.com/lumosimmo/market-relayer-go/internal/testdb"
	"github.com/lumosimmo/market-relayer-go/internal/testsupport"
)

func TestValidateConfigRunBootstrapAndBuildLoopsEdgePaths(t *testing.T) {
	t.Parallel()

	t.Run("validate config returns runtime metadata", func(t *testing.T) {
		t.Parallel()

		metadata, err := ValidateConfig(Options{
			ConfigPath: testsupport.WriteConfig(t, testsupport.ValidBootstrapConfig(t)),
			StoreOpen:  openTestStore,
		})
		if err != nil {
			t.Fatalf("ValidateConfig() error = %v", err)
		}
		if metadata.ConfigDigest == "" {
			t.Fatal("ValidateConfig().ConfigDigest is empty")
		}
		if metadata.PricingAlgorithmVersion == "" {
			t.Fatal("ValidateConfig().PricingAlgorithmVersion is empty")
		}
	})

	t.Run("run returns bootstrap error", func(t *testing.T) {
		t.Parallel()

		err := Run(context.Background(), Options{
			ConfigPath: filepath.Join(t.TempDir(), "missing-config.yaml"),
			StoreOpen:  openTestStore,
		})
		if err == nil {
			t.Fatal("Run() error = nil, want bootstrap failure")
		}
	})

	t.Run("bootstrap surfaces api listen failure", func(t *testing.T) {
		t.Parallel()

		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("net.Listen() error = %v", err)
		}
		defer listener.Close()

		configText := testsupport.ValidBootstrapConfigWithStoreDSN(testdb.DSN(t))
		configText = strings.Replace(configText, "listen_addr: 127.0.0.1:0", "listen_addr: "+listener.Addr().String(), 1)

		_, err = Bootstrap(context.Background(), Options{
			ConfigPath: testsupport.WriteConfig(t, configText),
			StoreOpen:  openTestStore,
		})
		if err == nil {
			t.Fatal("Bootstrap() error = nil, want listen failure")
		}
	})

	t.Run("build loops rejects empty source set", func(t *testing.T) {
		t.Parallel()

		storeDB := openMigratedStoreForTest(t, testdb.DSN(t))
		t.Cleanup(func() {
			if closeErr := storeDB.Close(); closeErr != nil {
				t.Fatalf("store.Close() error = %v", closeErr)
			}
		})

		_, _, err := buildLoops(&config.Config{
			Markets: []config.MarketConfig{{
				Symbol:          "ETHUSD",
				PublishInterval: 3 * time.Second,
			}},
		}, RuntimeMetadata{
			ConfigDigest:            "config-digest",
			PricingAlgorithmVersion: "1",
		}, storeDB, appTestSink{}, feeds.BuildOptions{}, clock.Fixed{Time: time.Date(2026, 3, 27, 12, 0, 0, 0, time.UTC)}, metrics.NewRegistry(), nil)
		if err == nil {
			t.Fatal("buildLoops() error = nil, want source validation failure")
		}
	})
}

func TestRecoverSelectionAndSinkInitializationFailures(t *testing.T) {
	t.Parallel()

	t.Run("multiple markets require an explicit symbol", func(t *testing.T) {
		t.Parallel()

		configText := strings.Replace(testsupport.ValidBootstrapConfig(t), `markets:
`, `  - name: coinbase-btc
    kind: coinbase
    product_id: BTC-USD
    request_timeout: 1s
    timestamp_mode: source_timestamp
  - name: kraken-btc
    kind: kraken
    pair: BTC/USD
    request_timeout: 1s
    timestamp_mode: received_at
    max_unchanged_age: 10s
  - name: hyperliquid-btc
    kind: hyperliquid
    market: BTC
    request_timeout: 1s
    timestamp_mode: source_timestamp
markets:
`, 1) + `
  - symbol: BTCUSD
    session_mode: always_open
    perp_book_source: hyperliquid-btc
    impact_notional_usd: "10000"
    publish_interval: 3s
    staleness_threshold: 10s
    max_fallback_age: 30s
    clamp_bps: 50
    divergence_threshold_bps: 250
    publish_scale: 2
    sources:
      - name: coinbase-btc
      - name: kraken-btc
`
		_, err := Recover(context.Background(), Options{
			ConfigPath: testsupport.WriteConfig(t, configText),
			StoreOpen:  openTestStore,
		}, RecoveryOptions{
			Mode: operator.ModeExact,
		})
		if err == nil {
			t.Fatal("Recover() error = nil, want missing market symbol failure")
		}
		if got := err.Error(); !strings.Contains(got, "market symbol is required") {
			t.Fatalf("Recover() error = %q, want missing market symbol failure", got)
		}
	})

	t.Run("unknown market is rejected", func(t *testing.T) {
		t.Parallel()

		_, err := Recover(context.Background(), Options{
			ConfigPath: testsupport.WriteConfig(t, testsupport.ValidBootstrapConfig(t)),
			StoreOpen:  openTestStore,
		}, RecoveryOptions{
			Mode:   operator.ModeExact,
			Market: "BTCUSD",
		})
		if err == nil {
			t.Fatal("Recover() error = nil, want missing market failure")
		}
		if got := err.Error(); !strings.Contains(got, "not found in config") {
			t.Fatalf("Recover() error = %q, want market-not-found failure", got)
		}
	})

	t.Run("resume pending surfaces sink initialization error", func(t *testing.T) {
		t.Parallel()

		sinkDir := t.TempDir()
		configText := testsupport.ValidBootstrapConfigWithStoreDSN(testdb.DSN(t))
		configText = strings.Replace(configText, "kind: http\n  url: http://127.0.0.1:18080/publish", "kind: file\n  path: "+sinkDir, 1)

		_, err := Recover(context.Background(), Options{
			ConfigPath: testsupport.WriteConfig(t, configText),
			StoreOpen:  openTestStore,
		}, RecoveryOptions{
			Mode: operator.ModeResumePending,
		})
		if err == nil {
			t.Fatal("Recover(resume-pending) error = nil, want sink initialization failure")
		}
	})
}

func TestRuntimeStateSnapshotHelpersAndLoopLifecycle(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 3, 27, 13, 0, 0, 0, time.UTC)
	storeDB := openMigratedStoreForTest(t, testdb.DSN(t))
	t.Cleanup(func() {
		if closeErr := storeDB.Close(); closeErr != nil && !strings.Contains(closeErr.Error(), "database not open") {
			t.Fatalf("store.Close() error = %v", closeErr)
		}
	})

	oneMarket := config.MarketConfig{Symbol: "ETHUSD"}
	multiMarkets := []config.MarketConfig{{Symbol: "ETHUSD"}, {Symbol: "BTCUSD"}}

	state := &runtimeState{
		metadata: api.Metadata{ServiceName: "market-relayer-go"},
		clock:    clock.Fixed{Time: now},
		store:    storeDB,
		markets:  []config.MarketConfig{oneMarket},
		loopLive: map[string]bool{"ETHUSD": false},
	}

	if got := state.Ready(context.Background()); got.Ready {
		t.Fatalf("Ready() = %+v, want not ready", got)
	}

	if got := state.Health(context.Background()); got.Healthy || !got.Checks.StoreLive || got.Checks.LoopLive {
		t.Fatalf("Health() = %+v, want live store and stopped loops", got)
	}

	state.markLoopsStarted()
	if started, live := state.loopHealth(); !started || !live {
		t.Fatalf("loopHealth() after start = (%t, %t), want (true, true)", started, live)
	}
	state.markLoopStopped("ETHUSD")
	if started, live := state.loopHealth(); !started || live {
		t.Fatalf("loopHealth() after stop = (%t, %t), want (true, false)", started, live)
	}

	snapshot, err := state.Snapshot(context.Background(), "")
	if err == nil {
		t.Fatalf("Snapshot(single market) = %+v, want latest-snapshot error", snapshot)
	}
	if got := err.Error(); !strings.Contains(got, "latest snapshot") {
		t.Fatalf("Snapshot(single market) error = %q, want latest snapshot failure", got)
	}

	if err := storeDB.Close(); err != nil {
		t.Fatalf("store.Close() error = %v", err)
	}
	if got := state.Health(context.Background()); got.Checks.StoreLive {
		t.Fatalf("Health() after store close = %+v, want store down", got)
	}

	if got := (&runtimeState{}).now(); got.IsZero() || got.Location() != time.UTC {
		t.Fatalf("runtimeState.now() = %s, want non-zero UTC time", got)
	}

	multiState := &runtimeState{
		store:   storeDB,
		markets: multiMarkets,
	}
	if _, err := multiState.Snapshot(context.Background(), ""); err == nil || !strings.Contains(err.Error(), "market symbol is required") {
		t.Fatalf("Snapshot(multiple markets) error = %v, want missing market symbol failure", err)
	}
	if _, err := multiState.Snapshot(context.Background(), "DOGEUSD"); err == nil || !strings.Contains(err.Error(), "not found in config") {
		t.Fatalf("Snapshot(unknown market) error = %v, want market-not-found failure", err)
	}

	if market, err := selectMarketFromList(nil, ""); err == nil || market.Symbol != "" {
		t.Fatalf("selectMarketFromList(nil) error = %v, want empty-config failure", err)
	}

	if got := coalesceMarket("", " ETHUSD ", "BTCUSD"); got != " ETHUSD " {
		t.Fatalf("coalesceMarket() = %q, want first non-empty value", got)
	}
	if got := coalesceReason("", status.ReasonSourceStale); got != status.ReasonSourceStale {
		t.Fatalf("coalesceReason() = %q, want %q", got, status.ReasonSourceStale)
	}
	if got := coalesceReason(""); got != status.ReasonNone {
		t.Fatalf("coalesceReason(empty) = %q, want %q", got, status.ReasonNone)
	}
	if got := coalesceSessionState("", status.SessionStateFallbackOnly); got != status.SessionStateFallbackOnly {
		t.Fatalf("coalesceSessionState() = %q, want %q", got, status.SessionStateFallbackOnly)
	}
	if got := coalesceSessionState(""); got != status.SessionStateOpen {
		t.Fatalf("coalesceSessionState(empty) = %q, want %q", got, status.SessionStateOpen)
	}
	if got := effectivePublishability(status.Condition{}, store.PublishabilityState{
		Status: status.StatusAvailable,
		Reason: status.ReasonNone,
	}); got.Status != status.StatusAvailable || got.Reason != status.ReasonNone {
		t.Fatalf("effectivePublishability(stored) = %+v, want stored condition", got)
	}
	if got := effectivePublishability(status.Condition{}, store.PublishabilityState{}); got.Status != status.StatusUnknown || got.Reason != status.ReasonNone {
		t.Fatalf("effectivePublishability(empty) = %+v, want unknown/none", got)
	}
	if age := lastGoodAgeSeconds(now, time.Time{}); age != nil {
		t.Fatalf("lastGoodAgeSeconds(zero) = %v, want nil", age)
	}
	if age := lastGoodAgeSeconds(now, now.Add(time.Second)); age == nil || *age != 0 {
		t.Fatalf("lastGoodAgeSeconds(future) = %v, want 0", age)
	}
}

func TestStartLoopsAndRunServiceAPIErrorBranches(t *testing.T) {
	t.Parallel()

	runtime := &Runtime{
		Loops: []*relayer.Loop{},
		state: &runtimeState{},
	}
	errCh := runtime.StartLoops(context.Background())
	select {
	case _, ok := <-errCh:
		if ok {
			t.Fatal("StartLoops() channel remained open for empty loop set")
		}
	case <-time.After(time.Second):
		t.Fatal("StartLoops() did not close empty-loop channel")
	}

	apiErrors := make(chan error, 1)
	apiErrors <- errors.New("api failed")
	closeErr := errors.New("close failed")
	err := runService(context.Background(), nil, apiErrors, func() error {
		return closeErr
	})
	if err == nil {
		t.Fatal("runService() error = nil, want joined api/close error")
	}
	if got := err.Error(); !strings.Contains(got, "api failed") || !strings.Contains(got, "close failed") {
		t.Fatalf("runService() error = %q, want api and close errors", got)
	}
}

type appTestSink struct{}

func (appTestSink) Kind() string {
	return "test"
}

func (appTestSink) Publish(context.Context, store.PreparedPublication) (relayer.Ack, error) {
	return relayer.Ack{}, errors.New("unexpected publish")
}

func (appTestSink) Close() error {
	return nil
}

var _ io.Closer = appTestSink{}
