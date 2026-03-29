package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/lumosimmo/market-relayer-go/internal/store"
)

func TestServerStaticReporterEndpointsAndGracefulErrorChannel(t *testing.T) {
	t.Parallel()

	metadata := Metadata{
		ServiceName:             "market-relayer-go",
		InstanceID:              "static-reporter-test",
		ConfigDigest:            "config-digest",
		PricingAlgorithmVersion: "1",
		BootedAt:                time.Date(2026, 3, 27, 10, 0, 0, 0, time.UTC),
	}
	server, err := NewServer(Options{
		ListenAddr:  "127.0.0.1:0",
		BaseContext: context.Background(),
		Metadata:    metadata,
	})
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}
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
		if err := decodeJSON(response.Body, &got); err != nil {
			t.Fatalf("decode /healthz error = %v", err)
		}
		if got.Metadata != metadata {
			t.Fatalf("/healthz metadata = %+v, want %+v", got.Metadata, metadata)
		}
		if !got.Healthy || !got.Checks.StoreLive || !got.Checks.LoopLive {
			t.Fatalf("/healthz response = %+v, want healthy live checks", got)
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
		if err := decodeJSON(response.Body, &got); err != nil {
			t.Fatalf("decode /readyz error = %v", err)
		}
		if !got.Ready || !got.ConfigLoaded || !got.StoreOpen || !got.SinkInitialized || !got.LoopStarted {
			t.Fatalf("/readyz response = %+v, want fully ready", got)
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
		if err := decodeJSON(response.Body, &got); err != nil {
			t.Fatalf("decode /statusz error = %v", err)
		}
		if got.Metadata != metadata {
			t.Fatalf("/statusz metadata = %+v, want %+v", got.Metadata, metadata)
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
		if err := decodeJSON(response.Body, &got); err != nil {
			t.Fatalf("decode /snapshotz error = %v", err)
		}
		if got.SchemaVersion != "" || got.Record.RecordID != "" || got.LastSuccessfulPublication != nil || len(got.RecentCycles) != 0 {
			t.Fatalf("/snapshotz = %+v, want zero snapshot", got)
		}
	})

	if err := server.Close(context.Background()); err != nil {
		t.Fatalf("server.Close() error = %v", err)
	}
	select {
	case _, ok := <-server.Errors():
		if ok {
			t.Fatal("server.Errors() remained open after graceful shutdown")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server.Errors() did not close after graceful shutdown")
	}
}

func TestServerRoutesSurfaceReporterFailuresAndServeErrors(t *testing.T) {
	t.Parallel()

	server, err := NewServer(Options{
		ListenAddr:  "127.0.0.1:0",
		BaseContext: context.Background(),
		Metadata: Metadata{
			ServiceName: "market-relayer-go",
			InstanceID:  "error-reporter-test",
		},
		Reporter: errorReporter{},
	})
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}
	t.Cleanup(func() {
		_ = server.Close(context.Background())
	})
	server.Start()

	client := http.Client{Timeout: time.Second}

	response, err := client.Get("http://" + server.Addr() + "/healthz")
	if err != nil {
		t.Fatalf("GET /healthz error = %v", err)
	}
	body, readErr := io.ReadAll(response.Body)
	response.Body.Close()
	if readErr != nil {
		t.Fatalf("io.ReadAll(/healthz) error = %v", readErr)
	}
	if response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("GET /healthz status = %d, want %d", response.StatusCode, http.StatusServiceUnavailable)
	}
	if string(body) == "" {
		t.Fatal("/healthz body is empty, want JSON payload")
	}

	response, err = client.Get("http://" + server.Addr() + "/statusz")
	if err != nil {
		t.Fatalf("GET /statusz error = %v", err)
	}
	body, readErr = io.ReadAll(response.Body)
	response.Body.Close()
	if readErr != nil {
		t.Fatalf("io.ReadAll(/statusz) error = %v", readErr)
	}
	if response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("GET /statusz status = %d, want %d", response.StatusCode, http.StatusServiceUnavailable)
	}
	if string(body) == "" {
		t.Fatal("/statusz body is empty, want error message")
	}

	response, err = client.Get("http://" + server.Addr() + "/snapshotz?market=ETHUSD")
	if err != nil {
		t.Fatalf("GET /snapshotz error = %v", err)
	}
	body, readErr = io.ReadAll(response.Body)
	response.Body.Close()
	if readErr != nil {
		t.Fatalf("io.ReadAll(/snapshotz) error = %v", readErr)
	}
	if response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("GET /snapshotz status = %d, want %d", response.StatusCode, http.StatusServiceUnavailable)
	}
	if string(body) == "" {
		t.Fatal("/snapshotz body is empty, want error message")
	}

	broken, err := NewServer(Options{ListenAddr: "127.0.0.1:0"})
	if err != nil {
		t.Fatalf("NewServer(broken) error = %v", err)
	}
	if err := broken.listener.Close(); err != nil {
		t.Fatalf("listener.Close() error = %v", err)
	}
	broken.Start()

	select {
	case serveErr, ok := <-broken.Errors():
		if !ok {
			t.Fatal("broken.Errors() closed without surfacing Serve error")
		}
		if serveErr == nil {
			t.Fatal("broken.Errors() returned nil error")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("broken.Errors() did not report Serve error")
	}
}

func TestServerMapsReporterHTTPStatusErrors(t *testing.T) {
	t.Parallel()

	server, err := NewServer(Options{
		ListenAddr:  "127.0.0.1:0",
		BaseContext: context.Background(),
		Reporter: statusErrorReporter{
			statusErr:   BadRequest(errors.New("market symbol is required")),
			snapshotErr: NotFound(errors.New("market not found")),
		},
	})
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}
	t.Cleanup(func() {
		_ = server.Close(context.Background())
	})
	server.Start()

	client := http.Client{Timeout: time.Second}

	response, err := client.Get("http://" + server.Addr() + "/statusz")
	if err != nil {
		t.Fatalf("GET /statusz error = %v", err)
	}
	body, readErr := io.ReadAll(response.Body)
	response.Body.Close()
	if readErr != nil {
		t.Fatalf("io.ReadAll(/statusz) error = %v", readErr)
	}
	if response.StatusCode != http.StatusBadRequest {
		t.Fatalf("GET /statusz status = %d, want %d", response.StatusCode, http.StatusBadRequest)
	}
	if string(body) == "" {
		t.Fatal("/statusz body is empty, want error message")
	}

	response, err = client.Get("http://" + server.Addr() + "/snapshotz?market=BTCUSD")
	if err != nil {
		t.Fatalf("GET /snapshotz error = %v", err)
	}
	body, readErr = io.ReadAll(response.Body)
	response.Body.Close()
	if readErr != nil {
		t.Fatalf("io.ReadAll(/snapshotz) error = %v", readErr)
	}
	if response.StatusCode != http.StatusNotFound {
		t.Fatalf("GET /snapshotz status = %d, want %d", response.StatusCode, http.StatusNotFound)
	}
	if string(body) == "" {
		t.Fatal("/snapshotz body is empty, want error message")
	}
}

func TestWriteJSONAndResponseMetadataErrorPaths(t *testing.T) {
	t.Parallel()

	recorder := httptest.NewRecorder()
	writeJSON(recorder, http.StatusAccepted, map[string]string{"status": "ok"})
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("writeJSON(success) status = %d, want %d", recorder.Code, http.StatusAccepted)
	}
	if got := recorder.Header().Get("Content-Type"); got != "application/json" {
		t.Fatalf("writeJSON(success) content type = %q, want %q", got, "application/json")
	}
	if got := recorder.Body.String(); got != "{\"status\":\"ok\"}\n" {
		t.Fatalf("writeJSON(success) body = %q, want compact JSON with trailing newline", got)
	}

	recorder = httptest.NewRecorder()
	writeJSON(recorder, http.StatusAccepted, map[string]any{"bad": make(chan int)})
	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("writeJSON() status = %d, want %d", recorder.Code, http.StatusInternalServerError)
	}
	if body := recorder.Body.String(); body == "" {
		t.Fatal("writeJSON() error body is empty, want encode failure text")
	}

	fallback := Metadata{ServiceName: "market-relayer-go", InstanceID: "fallback"}
	if got := responseMetadata(Metadata{}, fallback); got != fallback {
		t.Fatalf("responseMetadata(empty) = %+v, want %+v", got, fallback)
	}
	explicit := Metadata{ServiceName: "explicit", InstanceID: "instance"}
	if got := responseMetadata(explicit, fallback); got != explicit {
		t.Fatalf("responseMetadata(explicit) = %+v, want %+v", got, explicit)
	}
}

type errorReporter struct{}

type statusErrorReporter struct {
	statusErr   error
	snapshotErr error
}

func (errorReporter) Health(context.Context) HealthResponse {
	return HealthResponse{}
}

func (errorReporter) Ready(context.Context) ReadyResponse {
	return ReadyResponse{}
}

func (errorReporter) Status(context.Context) (StatusResponse, error) {
	return StatusResponse{}, errors.New("status unavailable")
}

func (errorReporter) Snapshot(context.Context, string) (store.Snapshot, error) {
	return store.Snapshot{}, errors.New("snapshot unavailable")
}

func (reporter statusErrorReporter) Health(context.Context) HealthResponse {
	return HealthResponse{}
}

func (reporter statusErrorReporter) Ready(context.Context) ReadyResponse {
	return ReadyResponse{}
}

func (reporter statusErrorReporter) Status(context.Context) (StatusResponse, error) {
	return StatusResponse{}, reporter.statusErr
}

func (reporter statusErrorReporter) Snapshot(context.Context, string) (store.Snapshot, error) {
	return store.Snapshot{}, reporter.snapshotErr
}

func decodeJSON[T any](reader io.Reader, target *T) error {
	return json.NewDecoder(reader).Decode(target)
}
