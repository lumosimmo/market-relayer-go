package api

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/lumosimmo/market-relayer-go/internal/status"
	"github.com/lumosimmo/market-relayer-go/internal/store"
)

type Metadata struct {
	ServiceName             string    `json:"service_name"`
	InstanceID              string    `json:"instance_id"`
	ConfigDigest            string    `json:"config_digest,omitempty"`
	PricingAlgorithmVersion string    `json:"pricing_algorithm_version,omitempty"`
	BootedAt                time.Time `json:"booted_at,omitempty"`
}

const (
	defaultReadHeaderTimeout = 5 * time.Second
	defaultReadTimeout       = 10 * time.Second
	defaultWriteTimeout      = 10 * time.Second
	defaultIdleTimeout       = 60 * time.Second
	requestIDHeader          = "X-Request-ID"
)

type HealthChecks struct {
	StoreLive           bool `json:"store_live"`
	LoopLive            bool `json:"loop_live"`
	LeaseControllerLive bool `json:"lease_controller_live"`
}

type HealthResponse struct {
	Healthy  bool         `json:"healthy"`
	Metadata Metadata     `json:"metadata"`
	Checks   HealthChecks `json:"checks"`
}

type ReadyResponse struct {
	Ready           bool `json:"ready"`
	ConfigLoaded    bool `json:"config_loaded"`
	StoreOpen       bool `json:"store_open"`
	SinkInitialized bool `json:"sink_initialized"`
	LoopStarted     bool `json:"loop_started"`
}

type PriceAvailability struct {
	Status status.Status `json:"status"`
	Reason status.Reason `json:"reason"`
}

type PriceSet struct {
	External        PriceAvailability `json:"external"`
	Oracle          PriceAvailability `json:"oracle"`
	OraclePlusBasis PriceAvailability `json:"oracle_plus_basis"`
}

type MarketStatus struct {
	Market              string              `json:"market"`
	State               string              `json:"state"`
	OwnedByThisInstance bool                `json:"owned_by_this_instance"`
	LeaseOwner          string              `json:"lease_owner,omitempty"`
	LeaseExpiresAt      *time.Time          `json:"lease_expires_at,omitempty"`
	Publishability      status.Condition    `json:"publishability"`
	SessionState        status.SessionState `json:"session_state"`
	SessionReason       status.Reason       `json:"session_reason"`
	LastGoodAgeSeconds  *float64            `json:"last_good_age_seconds,omitempty"`
	FallbackPublishable bool                `json:"fallback_publishable"`
	Prices              PriceSet            `json:"prices"`
}

type StatusResponse struct {
	Metadata Metadata       `json:"metadata"`
	Markets  []MarketStatus `json:"markets"`
}

type Reporter interface {
	Health(context.Context) HealthResponse
	Ready(context.Context) ReadyResponse
	Status(context.Context) (StatusResponse, error)
	Snapshot(context.Context, string) (store.Snapshot, error)
}

type Options struct {
	ListenAddr        string
	BaseContext       context.Context
	Metadata          Metadata
	Reporter          Reporter
	MetricsHandler    http.Handler
	ReadHeaderTimeout time.Duration
	ReadTimeout       time.Duration
	WriteTimeout      time.Duration
	IdleTimeout       time.Duration
}

type Server struct {
	server   *http.Server
	listener net.Listener
	errCh    chan error
	once     sync.Once
}

type requestIDContextKey struct{}

func NewServer(options Options) (*Server, error) {
	listener, err := net.Listen("tcp", options.ListenAddr)
	if err != nil {
		return nil, err
	}

	reporter := options.Reporter
	if reporter == nil {
		reporter = staticReporter{metadata: options.Metadata}
	}
	metricsHandler := options.MetricsHandler
	if metricsHandler == nil {
		metricsHandler = http.NotFoundHandler()
	}

	mux := http.NewServeMux()
	mux.Handle("/metrics", metricsHandler)
	mux.HandleFunc("/healthz", func(writer http.ResponseWriter, request *http.Request) {
		response := reporter.Health(request.Context())
		response.Metadata = responseMetadata(response.Metadata, options.Metadata)
		writeJSON(writer, statusCode(response.Healthy), response)
	})
	mux.HandleFunc("/readyz", func(writer http.ResponseWriter, request *http.Request) {
		response := reporter.Ready(request.Context())
		writeJSON(writer, statusCode(response.Ready), response)
	})
	mux.HandleFunc("/statusz", func(writer http.ResponseWriter, request *http.Request) {
		response, err := reporter.Status(request.Context())
		if err != nil {
			http.Error(writer, err.Error(), ErrorStatus(err))
			return
		}
		response.Metadata = responseMetadata(response.Metadata, options.Metadata)
		writeJSON(writer, http.StatusOK, response)
	})
	mux.HandleFunc("/snapshotz", func(writer http.ResponseWriter, request *http.Request) {
		snapshot, err := reporter.Snapshot(request.Context(), request.URL.Query().Get("market"))
		if err != nil {
			http.Error(writer, err.Error(), ErrorStatus(err))
			return
		}
		writeJSON(writer, http.StatusOK, snapshot)
	})

	server := &http.Server{
		Handler:           withRequestID(mux),
		ReadHeaderTimeout: normalizeTimeout(options.ReadHeaderTimeout, defaultReadHeaderTimeout),
		ReadTimeout:       normalizeTimeout(options.ReadTimeout, defaultReadTimeout),
		WriteTimeout:      normalizeTimeout(options.WriteTimeout, defaultWriteTimeout),
		IdleTimeout:       normalizeTimeout(options.IdleTimeout, defaultIdleTimeout),
	}
	if options.BaseContext != nil {
		server.BaseContext = func(net.Listener) context.Context {
			return options.BaseContext
		}
	}

	return &Server{
		server:   server,
		listener: listener,
		errCh:    make(chan error, 1),
	}, nil
}

func (server *Server) Start() {
	go func() {
		err := server.server.Serve(server.listener)
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			server.errCh <- err
			return
		}
		close(server.errCh)
	}()
}

func (server *Server) Addr() string {
	return server.listener.Addr().String()
}

func (server *Server) Errors() <-chan error {
	return server.errCh
}

func (server *Server) Close(ctx context.Context) error {
	var shutdownErr error
	server.once.Do(func() {
		shutdownErr = server.server.Shutdown(ctx)
	})
	return shutdownErr
}

func writeJSON(writer http.ResponseWriter, statusCode int, value any) {
	var body bytes.Buffer
	encoder := json.NewEncoder(&body)
	if err := encoder.Encode(value); err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		return
	}

	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(statusCode)
	_, _ = writer.Write(body.Bytes())
}

func RequestIDFromContext(ctx context.Context) string {
	requestID, _ := ctx.Value(requestIDContextKey{}).(string)
	return requestID
}

func statusCode(ok bool) int {
	if ok {
		return http.StatusOK
	}
	return http.StatusServiceUnavailable
}

func responseMetadata(value Metadata, fallback Metadata) Metadata {
	if value == (Metadata{}) {
		return fallback
	}
	return value
}

func withRequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requestID := strings.TrimSpace(request.Header.Get(requestIDHeader))
		if requestID == "" {
			requestID = generateRequestID()
		}

		writer.Header().Set(requestIDHeader, requestID)
		ctx := context.WithValue(request.Context(), requestIDContextKey{}, requestID)
		next.ServeHTTP(writer, request.WithContext(ctx))
	})
}

func normalizeTimeout(value time.Duration, fallback time.Duration) time.Duration {
	if value > 0 {
		return value
	}
	return fallback
}

func generateRequestID() string {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err == nil {
		return hex.EncodeToString(raw[:])
	}
	return hex.EncodeToString([]byte(time.Now().UTC().Format(time.RFC3339Nano)))
}

type staticReporter struct {
	metadata Metadata
}

func (reporter staticReporter) Health(context.Context) HealthResponse {
	return HealthResponse{
		Healthy:  true,
		Metadata: reporter.metadata,
		Checks: HealthChecks{
			StoreLive:           true,
			LoopLive:            true,
			LeaseControllerLive: true,
		},
	}
}

func (reporter staticReporter) Ready(context.Context) ReadyResponse {
	return ReadyResponse{
		Ready:           true,
		ConfigLoaded:    true,
		StoreOpen:       true,
		SinkInitialized: true,
		LoopStarted:     true,
	}
}

func (reporter staticReporter) Status(context.Context) (StatusResponse, error) {
	return StatusResponse{Metadata: reporter.metadata}, nil
}

func (reporter staticReporter) Snapshot(context.Context, string) (store.Snapshot, error) {
	return store.Snapshot{}, nil
}
