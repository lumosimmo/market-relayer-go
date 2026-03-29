package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/lumosimmo/market-relayer-go/internal/metrics"
	"github.com/lumosimmo/market-relayer-go/internal/pricing"
	"github.com/lumosimmo/market-relayer-go/internal/relayer"
	"github.com/lumosimmo/market-relayer-go/internal/status"
	"github.com/lumosimmo/market-relayer-go/internal/store"
)

func TestServerExposesMetricsAndOperatorEndpoints(t *testing.T) {
	t.Parallel()

	metricsRegistry := metrics.NewRegistry()
	metricsRegistry.IncStoreFailure("ETHUSD")

	reporter := stubReporter{
		health: HealthResponse{
			Healthy: true,
			Checks: HealthChecks{
				StoreLive: true,
				LoopLive:  true,
			},
		},
		ready: ReadyResponse{
			Ready:           true,
			ConfigLoaded:    true,
			StoreOpen:       true,
			SinkInitialized: true,
			LoopStarted:     true,
		},
		status: StatusResponse{
			Markets: []MarketStatus{
				{
					Market:              "ETHUSD",
					State:               "fresh",
					Publishability:      status.NewCondition(status.StatusAvailable, status.ReasonNone),
					SessionState:        status.SessionStateOpen,
					SessionReason:       status.ReasonNone,
					FallbackPublishable: true,
					Prices: PriceSet{
						External:        PriceAvailability{Status: status.StatusAvailable, Reason: status.ReasonNone},
						Oracle:          PriceAvailability{Status: status.StatusAvailable, Reason: status.ReasonNone},
						OraclePlusBasis: PriceAvailability{Status: status.StatusAvailable, Reason: status.ReasonNone},
					},
				},
			},
		},
		snapshot: store.Snapshot{
			SchemaVersion: store.SchemaVersion,
			Record: store.CycleRecord{
				RecordID: "cycle-0001",
				Market:   "ETHUSD",
				Metadata: store.Metadata{
					ConfigDigest:            "config-digest",
					PricingAlgorithmVersion: pricing.AlgorithmVersion,
					SchemaVersion:           store.SchemaVersion,
					EnvelopeVersion:         relayer.EnvelopeVersion,
				},
			},
		},
	}

	server, err := NewServer(Options{
		ListenAddr:     "127.0.0.1:0",
		BaseContext:    context.Background(),
		Metadata:       Metadata{ServiceName: "market-relayer-go", InstanceID: "api-test", BootedAt: time.Date(2026, 3, 27, 12, 0, 0, 0, time.UTC)},
		Reporter:       reporter,
		MetricsHandler: metricsRegistry.Handler(),
	})
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}
	t.Cleanup(func() {
		if closeErr := server.Close(context.Background()); closeErr != nil {
			t.Fatalf("server.Close() error = %v", closeErr)
		}
	})
	server.Start()

	client := http.Client{Timeout: time.Second}

	t.Run("healthz", func(t *testing.T) {
		response, err := client.Get("http://" + server.Addr() + "/healthz")
		if err != nil {
			t.Fatalf("GET /healthz error = %v", err)
		}
		defer response.Body.Close()
		if response.StatusCode != http.StatusOK {
			t.Fatalf("GET /healthz status = %d, want %d", response.StatusCode, http.StatusOK)
		}

		var got HealthResponse
		if err := json.NewDecoder(response.Body).Decode(&got); err != nil {
			t.Fatalf("json.Decode(/healthz) error = %v", err)
		}
		if !got.Healthy {
			t.Fatal("/healthz healthy = false, want true")
		}
		if !got.Checks.StoreLive || !got.Checks.LoopLive {
			t.Fatalf("/healthz checks = %+v, want live store and loop", got.Checks)
		}
	})

	t.Run("readyz", func(t *testing.T) {
		response, err := client.Get("http://" + server.Addr() + "/readyz")
		if err != nil {
			t.Fatalf("GET /readyz error = %v", err)
		}
		defer response.Body.Close()
		if response.StatusCode != http.StatusOK {
			t.Fatalf("GET /readyz status = %d, want %d", response.StatusCode, http.StatusOK)
		}

		var got ReadyResponse
		if err := json.NewDecoder(response.Body).Decode(&got); err != nil {
			t.Fatalf("json.Decode(/readyz) error = %v", err)
		}
		if !got.Ready {
			t.Fatal("/readyz ready = false, want true")
		}
	})

	t.Run("statusz", func(t *testing.T) {
		response, err := client.Get("http://" + server.Addr() + "/statusz")
		if err != nil {
			t.Fatalf("GET /statusz error = %v", err)
		}
		defer response.Body.Close()
		if response.StatusCode != http.StatusOK {
			t.Fatalf("GET /statusz status = %d, want %d", response.StatusCode, http.StatusOK)
		}

		var got StatusResponse
		if err := json.NewDecoder(response.Body).Decode(&got); err != nil {
			t.Fatalf("json.Decode(/statusz) error = %v", err)
		}
		if len(got.Markets) != 1 || got.Markets[0].State != "fresh" {
			t.Fatalf("/statusz markets = %+v, want one fresh market", got.Markets)
		}
	})

	t.Run("snapshotz", func(t *testing.T) {
		response, err := client.Get("http://" + server.Addr() + "/snapshotz?market=ETHUSD")
		if err != nil {
			t.Fatalf("GET /snapshotz error = %v", err)
		}
		defer response.Body.Close()
		if response.StatusCode != http.StatusOK {
			t.Fatalf("GET /snapshotz status = %d, want %d", response.StatusCode, http.StatusOK)
		}

		var got store.Snapshot
		if err := json.NewDecoder(response.Body).Decode(&got); err != nil {
			t.Fatalf("json.Decode(/snapshotz) error = %v", err)
		}
		if got.SchemaVersion != store.SchemaVersion {
			t.Fatalf("/snapshotz schema_version = %q, want %q", got.SchemaVersion, store.SchemaVersion)
		}
		if got.Record.Metadata.EnvelopeVersion != relayer.EnvelopeVersion {
			t.Fatalf("/snapshotz envelope_version = %q, want %q", got.Record.Metadata.EnvelopeVersion, relayer.EnvelopeVersion)
		}
	})

	t.Run("metrics", func(t *testing.T) {
		response, err := client.Get("http://" + server.Addr() + "/metrics")
		if err != nil {
			t.Fatalf("GET /metrics error = %v", err)
		}
		defer response.Body.Close()
		if response.StatusCode != http.StatusOK {
			t.Fatalf("GET /metrics status = %d, want %d", response.StatusCode, http.StatusOK)
		}
	})
}

func TestServerReadyzReturnsUnavailableForBootIncompleteRuntime(t *testing.T) {
	t.Parallel()

	server, err := NewServer(Options{
		ListenAddr:  "127.0.0.1:0",
		BaseContext: context.Background(),
		Reporter: stubReporter{
			ready: ReadyResponse{
				Ready:           false,
				ConfigLoaded:    true,
				StoreOpen:       true,
				SinkInitialized: true,
				LoopStarted:     false,
			},
		},
		MetricsHandler: metrics.NewRegistry().Handler(),
	})
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}
	t.Cleanup(func() {
		if closeErr := server.Close(context.Background()); closeErr != nil {
			t.Fatalf("server.Close() error = %v", closeErr)
		}
	})
	server.Start()

	client := http.Client{Timeout: time.Second}
	response, err := client.Get("http://" + server.Addr() + "/readyz")
	if err != nil {
		t.Fatalf("GET /readyz error = %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("GET /readyz status = %d, want %d", response.StatusCode, http.StatusServiceUnavailable)
	}
}

func TestServerAppliesDefaultTimeoutsAndPropagatesRequestIDs(t *testing.T) {
	t.Parallel()

	reporter := &requestIDReporter{}
	server, err := NewServer(Options{
		ListenAddr:  "127.0.0.1:0",
		BaseContext: context.Background(),
		Reporter:    reporter,
	})
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}
	t.Cleanup(func() {
		if closeErr := server.Close(context.Background()); closeErr != nil {
			t.Fatalf("server.Close() error = %v", closeErr)
		}
	})
	server.Start()

	if got := server.server.ReadHeaderTimeout; got != defaultReadHeaderTimeout {
		t.Fatalf("ReadHeaderTimeout = %s, want %s", got, defaultReadHeaderTimeout)
	}
	if got := server.server.ReadTimeout; got != defaultReadTimeout {
		t.Fatalf("ReadTimeout = %s, want %s", got, defaultReadTimeout)
	}
	if got := server.server.WriteTimeout; got != defaultWriteTimeout {
		t.Fatalf("WriteTimeout = %s, want %s", got, defaultWriteTimeout)
	}
	if got := server.server.IdleTimeout; got != defaultIdleTimeout {
		t.Fatalf("IdleTimeout = %s, want %s", got, defaultIdleTimeout)
	}

	client := http.Client{Timeout: time.Second}

	request, err := http.NewRequest(http.MethodGet, "http://"+server.Addr()+"/healthz", nil)
	if err != nil {
		t.Fatalf("http.NewRequest() error = %v", err)
	}
	request.Header.Set(requestIDHeader, "req-123")

	response, err := client.Do(request)
	if err != nil {
		t.Fatalf("client.Do() error = %v", err)
	}
	response.Body.Close()

	if got := response.Header.Get(requestIDHeader); got != "req-123" {
		t.Fatalf("response %s = %q, want %q", requestIDHeader, got, "req-123")
	}
	if got := reporter.lastRequestID(); got != "req-123" {
		t.Fatalf("reporter request id = %q, want %q", got, "req-123")
	}

	response, err = client.Get("http://" + server.Addr() + "/readyz")
	if err != nil {
		t.Fatalf("GET /readyz error = %v", err)
	}
	response.Body.Close()

	generated := response.Header.Get(requestIDHeader)
	if generated == "" {
		t.Fatalf("response %s is empty, want generated request id", requestIDHeader)
	}
	if got := reporter.lastRequestID(); got != generated {
		t.Fatalf("reporter request id = %q, want generated id %q", got, generated)
	}
}

func TestServerUsesConfiguredTimeouts(t *testing.T) {
	t.Parallel()

	server, err := NewServer(Options{
		ListenAddr:        "127.0.0.1:0",
		BaseContext:       context.Background(),
		ReadHeaderTimeout: 2 * time.Second,
		ReadTimeout:       3 * time.Second,
		WriteTimeout:      4 * time.Second,
		IdleTimeout:       5 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}
	t.Cleanup(func() {
		if closeErr := server.Close(context.Background()); closeErr != nil {
			t.Fatalf("server.Close() error = %v", closeErr)
		}
	})

	if got := server.server.ReadHeaderTimeout; got != 2*time.Second {
		t.Fatalf("ReadHeaderTimeout = %s, want %s", got, 2*time.Second)
	}
	if got := server.server.ReadTimeout; got != 3*time.Second {
		t.Fatalf("ReadTimeout = %s, want %s", got, 3*time.Second)
	}
	if got := server.server.WriteTimeout; got != 4*time.Second {
		t.Fatalf("WriteTimeout = %s, want %s", got, 4*time.Second)
	}
	if got := server.server.IdleTimeout; got != 5*time.Second {
		t.Fatalf("IdleTimeout = %s, want %s", got, 5*time.Second)
	}
}

func TestWriteJSONEncodesStructuredResponses(t *testing.T) {
	t.Parallel()

	recorder := httptest.NewRecorder()
	writeJSON(recorder, http.StatusAccepted, struct {
		Ready bool `json:"ready"`
	}{Ready: true})

	response := recorder.Result()
	defer response.Body.Close()

	if response.StatusCode != http.StatusAccepted {
		t.Fatalf("StatusCode = %d, want %d", response.StatusCode, http.StatusAccepted)
	}
	if got := response.Header.Get("Content-Type"); got != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", got)
	}
	if got := recorder.Body.String(); got != "{\"ready\":true}\n" {
		t.Fatalf("Body = %q, want %q", got, "{\"ready\":true}\n")
	}
}

func TestWriteJSONReturnsInternalServerErrorOnEncodeFailure(t *testing.T) {
	t.Parallel()

	recorder := httptest.NewRecorder()
	writeJSON(recorder, http.StatusOK, struct {
		Invalid chan int `json:"invalid"`
	}{Invalid: make(chan int)})

	response := recorder.Result()
	defer response.Body.Close()

	if response.StatusCode != http.StatusInternalServerError {
		t.Fatalf("StatusCode = %d, want %d", response.StatusCode, http.StatusInternalServerError)
	}
}

type stubReporter struct {
	health   HealthResponse
	ready    ReadyResponse
	status   StatusResponse
	snapshot store.Snapshot
}

func (reporter stubReporter) Health(context.Context) HealthResponse {
	return reporter.health
}

func (reporter stubReporter) Ready(context.Context) ReadyResponse {
	return reporter.ready
}

func (reporter stubReporter) Status(context.Context) (StatusResponse, error) {
	return reporter.status, nil
}

func (reporter stubReporter) Snapshot(context.Context, string) (store.Snapshot, error) {
	return reporter.snapshot, nil
}

type requestIDReporter struct {
	mu        sync.Mutex
	requestID string
}

func (reporter *requestIDReporter) Health(ctx context.Context) HealthResponse {
	reporter.record(ctx)
	return HealthResponse{Healthy: true}
}

func (reporter *requestIDReporter) Ready(ctx context.Context) ReadyResponse {
	reporter.record(ctx)
	return ReadyResponse{Ready: true}
}

func (reporter *requestIDReporter) Status(ctx context.Context) (StatusResponse, error) {
	reporter.record(ctx)
	return StatusResponse{}, nil
}

func (reporter *requestIDReporter) Snapshot(ctx context.Context, _ string) (store.Snapshot, error) {
	reporter.record(ctx)
	return store.Snapshot{}, nil
}

func (reporter *requestIDReporter) record(ctx context.Context) {
	reporter.mu.Lock()
	defer reporter.mu.Unlock()
	reporter.requestID = RequestIDFromContext(ctx)
}

func (reporter *requestIDReporter) lastRequestID() string {
	reporter.mu.Lock()
	defer reporter.mu.Unlock()
	return reporter.requestID
}
