package app

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lumosimmo/market-relayer-go/internal/testdb"
	"github.com/lumosimmo/market-relayer-go/internal/testsupport"
)

func TestBootstrapValidationFailures(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		config  func(*testing.T) string
		wantErr string
	}{
		{
			name: "missing required source config",
			config: func(t *testing.T) string {
				return strings.Replace(testsupport.ValidBootstrapConfig(t), "    product_id: ETH-USD\n", "", 1)
			},
			wantErr: "coinbase source requires product_id",
		},
		{
			name: "duplicate market symbol",
			config: func(t *testing.T) string {
				return testsupport.ValidBootstrapConfig(t) + `
  - symbol: ETHUSD
    session_mode: always_open
    perp_book_source: hyperliquid-eth
    impact_notional_usd: "10000"
    publish_interval: 3s
    staleness_threshold: 10s
    max_fallback_age: 30s
    clamp_bps: 50
    divergence_threshold_bps: 250
    publish_scale: 2
    sources:
      - name: coinbase-primary
`
			},
			wantErr: "duplicate market symbol",
		},
		{
			name: "unknown source reference",
			config: func(t *testing.T) string {
				return strings.Replace(testsupport.ValidBootstrapConfig(t), "      - name: kraken-secondary", "      - name: missing-source", 1)
			},
			wantErr: "unknown source reference",
		},
		{
			name: "missing store setting",
			config: func(t *testing.T) string {
				return strings.Replace(
					testsupport.ValidBootstrapConfigWithStoreDSN(testsupport.PlaceholderStoreDSN),
					"  dsn: \""+testsupport.PlaceholderStoreDSN+"\"\n",
					"  dsn: \"\"\n",
					1,
				)
			},
			wantErr: "store.dsn is required",
		},
		{
			name: "missing sink setting",
			config: func(t *testing.T) string {
				return strings.Replace(testsupport.ValidBootstrapConfig(t), "url: http://127.0.0.1:18080/publish", "url: \"\"", 1)
			},
			wantErr: "sink.url is required",
		},
		{
			name: "timestampless primary missing guardrails",
			config: func(t *testing.T) string {
				return strings.Replace(testsupport.ValidBootstrapConfig(t), "timestamp_mode: source_timestamp", "timestamp_mode: received_at", 1)
			},
			wantErr: "timestamp-less primary source",
		},
		{
			name: "deprecated alias",
			config: func(t *testing.T) string {
				return strings.Replace(testsupport.ValidBootstrapConfig(t), "session_mode: always_open", "session: always_open", 1)
			},
			wantErr: "deprecated alias",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			configPath := testsupport.WriteConfig(t, tt.config(t))
			_, err := Bootstrap(context.Background(), Options{
				ConfigPath: configPath,
				StoreOpen:  openTestStore,
			})
			if err == nil {
				t.Fatalf("Bootstrap() error = nil, want %q", tt.wantErr)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("Bootstrap() error = %q, want substring %q", err.Error(), tt.wantErr)
			}
		})
	}
}

func TestBootstrapSuccess(t *testing.T) {
	t.Parallel()

	configPath := testsupport.WriteConfig(t, testsupport.ValidBootstrapConfigWithStoreDSN(testdb.DSN(t)))

	runtime, err := Bootstrap(context.Background(), Options{
		ConfigPath: configPath,
		StoreOpen:  openTestStore,
	})
	if err != nil {
		t.Fatalf("Bootstrap() error = %v", err)
	}
	t.Cleanup(func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if closeErr := runtime.Close(shutdownCtx); closeErr != nil {
			t.Fatalf("runtime.Close() error = %v", closeErr)
		}
	})

	if runtime.Metadata.ConfigDigest == "" {
		t.Fatal("runtime.Metadata.ConfigDigest is empty")
	}
	if runtime.Metadata.PricingAlgorithmVersion == "" {
		t.Fatal("runtime.Metadata.PricingAlgorithmVersion is empty")
	}
	if len(runtime.Loops) != 1 {
		t.Fatalf("len(runtime.Loops) = %d, want 1", len(runtime.Loops))
	}

	client := http.Client{Timeout: time.Second}

	response, err := client.Get("http://" + runtime.API.Addr() + "/readyz")
	if err != nil {
		t.Fatalf("GET /readyz error = %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("GET /readyz status before StartLoops = %d, want %d", response.StatusCode, http.StatusServiceUnavailable)
	}

	runtime.state.markLoopsStarted()

	response, err = client.Get("http://" + runtime.API.Addr() + "/healthz")
	if err != nil {
		t.Fatalf("GET /healthz error = %v", err)
	}
	response.Body.Close()

	if response.StatusCode != http.StatusOK {
		t.Fatalf("GET /healthz status = %d, want %d", response.StatusCode, http.StatusOK)
	}

	response, err = client.Get("http://" + runtime.API.Addr() + "/readyz")
	if err != nil {
		t.Fatalf("GET /readyz error after StartLoops = %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("GET /readyz status after StartLoops = %d, want %d", response.StatusCode, http.StatusOK)
	}
}

func TestRunServiceClosesRuntimeWhenLoopChannelCloses(t *testing.T) {
	t.Parallel()

	loopErrors := make(chan error)
	close(loopErrors)

	var closed atomic.Int32
	err := runService(context.Background(), loopErrors, nil, func() error {
		closed.Add(1)
		return nil
	})
	if err != nil {
		t.Fatalf("runService() error = %v, want nil", err)
	}
	if got := closed.Load(); got != 1 {
		t.Fatalf("close count = %d, want 1", got)
	}
}

func TestRunServiceClosesRuntimeWhenAPIChannelCloses(t *testing.T) {
	t.Parallel()

	apiErrors := make(chan error)
	close(apiErrors)

	var closed atomic.Int32
	err := runService(context.Background(), nil, apiErrors, func() error {
		closed.Add(1)
		return nil
	})
	if err != nil {
		t.Fatalf("runService() error = %v, want nil", err)
	}
	if got := closed.Load(); got != 1 {
		t.Fatalf("close count = %d, want 1", got)
	}
}

func TestRunServiceClosesRuntimeWhenCancellationRacesClosedChannels(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	loopErrors := make(chan error)
	close(loopErrors)
	apiErrors := make(chan error)
	close(apiErrors)

	var closed atomic.Int32
	err := runService(ctx, loopErrors, apiErrors, func() error {
		closed.Add(1)
		return nil
	})
	if err != nil {
		t.Fatalf("runService() error = %v, want nil", err)
	}
	if got := closed.Load(); got != 1 {
		t.Fatalf("close count = %d, want 1", got)
	}
}

func TestRunServiceJoinsLoopErrorWithCloseError(t *testing.T) {
	t.Parallel()

	loopErrors := make(chan error, 1)
	loopErrors <- errors.New("loop failed")

	closeErr := errors.New("close failed")
	err := runService(context.Background(), loopErrors, nil, func() error {
		return closeErr
	})
	if err == nil {
		t.Fatal("runService() error = nil, want joined error")
	}
	if !strings.Contains(err.Error(), "loop failed") {
		t.Fatalf("runService() error = %q, want loop error", err.Error())
	}
	if !strings.Contains(err.Error(), "close failed") {
		t.Fatalf("runService() error = %q, want close error", err.Error())
	}
}

func TestCoordinatedCloseCancelsAndWaitsForLoopShutdown(t *testing.T) {
	t.Parallel()

	loopErrors := make(chan error)
	cancelCalled := make(chan struct{})
	loopsStopped := make(chan struct{})
	go func() {
		<-cancelCalled
		close(loopErrors)
		close(loopsStopped)
	}()

	var closed atomic.Int32
	shutdown := coordinatedClose(func() {
		close(cancelCalled)
	}, loopErrors, time.Second, func() error {
		select {
		case <-loopsStopped:
		default:
			t.Fatal("closeFn ran before loops stopped")
		}
		closed.Add(1)
		return nil
	})

	if err := shutdown(); err != nil {
		t.Fatalf("coordinatedClose() error = %v, want nil", err)
	}
	if got := closed.Load(); got != 1 {
		t.Fatalf("close count = %d, want 1", got)
	}
}
