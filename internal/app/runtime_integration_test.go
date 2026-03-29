package app

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lumosimmo/market-relayer-go/internal/api"
	"github.com/lumosimmo/market-relayer-go/internal/feeds"
	"github.com/lumosimmo/market-relayer-go/internal/status"
	"github.com/lumosimmo/market-relayer-go/internal/store"
	"github.com/lumosimmo/market-relayer-go/internal/testdb"
	"github.com/lumosimmo/market-relayer-go/internal/testsupport"
)

func TestServePublishesAndExposesRuntimeEndpoints(t *testing.T) {
	t.Parallel()

	var feedRequests atomic.Int32
	feedServer := testsupport.NewCountingMarketFeedServer(t, &feedRequests)
	defer feedServer.Close()

	sinkCalls := make(chan []byte, 4)
	sinkServer := testsupport.NewRecordingSinkServer(t, func(body []byte, writer http.ResponseWriter, request *http.Request) {
		sinkCalls <- body
		writer.WriteHeader(http.StatusCreated)
	})
	defer sinkServer.Close()

	configPath := testsupport.WriteConfig(t, strings.Replace(testsupport.ValidBootstrapConfigWithStoreDSN(testdb.DSN(t)), "url: http://127.0.0.1:18080/publish", "url: "+sinkServer.URL, 1))

	runtime, err := Bootstrap(context.Background(), Options{
		ConfigPath: configPath,
		StoreOpen:  openTestStore,
		FeedBuild: feeds.BuildOptions{
			EndpointOverrides: map[string]string{
				"coinbase-primary": feedServer.URL,
				"kraken-secondary": feedServer.URL,
				"hyperliquid-eth":  feedServer.URL,
			},
		},
	})
	if err != nil {
		t.Fatalf("Bootstrap() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	serveDone := make(chan error, 1)
	go func() {
		serveDone <- Serve(ctx, runtime)
	}()

	testsupport.WaitForBody(t, sinkCalls)

	client := http.Client{Timeout: time.Second}
	snapshot := waitForSnapshot(t, client, runtime.API.Addr(), "ETHUSD")

	healthResponse, healthStatus := getJSON[api.HealthResponse](t, client, "http://"+runtime.API.Addr()+"/healthz")
	if healthStatus != http.StatusOK {
		t.Fatalf("GET /healthz status = %d, want %d", healthStatus, http.StatusOK)
	}
	if !healthResponse.Healthy || !healthResponse.Checks.StoreLive || !healthResponse.Checks.LoopLive {
		t.Fatalf("GET /healthz = %+v, want healthy live runtime", healthResponse)
	}

	readyResponse, readyStatus := getJSON[api.ReadyResponse](t, client, "http://"+runtime.API.Addr()+"/readyz")
	if readyStatus != http.StatusOK {
		t.Fatalf("GET /readyz status = %d, want %d", readyStatus, http.StatusOK)
	}
	if !readyResponse.Ready || !readyResponse.ConfigLoaded || !readyResponse.StoreOpen || !readyResponse.SinkInitialized || !readyResponse.LoopStarted {
		t.Fatalf("GET /readyz = %+v, want fully ready runtime", readyResponse)
	}

	statusResponse, statusCode := getJSON[api.StatusResponse](t, client, "http://"+runtime.API.Addr()+"/statusz")
	if statusCode != http.StatusOK {
		t.Fatalf("GET /statusz status = %d, want %d", statusCode, http.StatusOK)
	}
	if len(statusResponse.Markets) != 1 {
		t.Fatalf("len(GET /statusz markets) = %d, want 1", len(statusResponse.Markets))
	}
	market := statusResponse.Markets[0]
	if market.Market != "ETHUSD" || market.State != "fresh" {
		t.Fatalf("GET /statusz market = %+v, want ETHUSD fresh", market)
	}
	if market.Publishability.Status != status.StatusAvailable || market.Publishability.Reason != status.ReasonNone {
		t.Fatalf("GET /statusz publishability = %+v, want available/none", market.Publishability)
	}
	if market.Prices.External.Status != status.StatusAvailable || market.Prices.Oracle.Status != status.StatusAvailable || market.Prices.OraclePlusBasis.Status != status.StatusAvailable {
		t.Fatalf("GET /statusz prices = %+v, want all available", market.Prices)
	}

	if snapshot.SchemaVersion != store.SchemaVersion {
		t.Fatalf("GET /snapshotz schema version = %q, want %q", snapshot.SchemaVersion, store.SchemaVersion)
	}
	if snapshot.Record.Market != "ETHUSD" {
		t.Fatalf("GET /snapshotz market = %q, want ETHUSD", snapshot.Record.Market)
	}
	if snapshot.LastSuccessfulPublication == nil || snapshot.LastSuccessfulPublication.Sequence != 1 {
		t.Fatalf("GET /snapshotz last successful publication = %+v, want sequence 1", snapshot.LastSuccessfulPublication)
	}
	if len(snapshot.RecentCycles) == 0 {
		t.Fatal("GET /snapshotz recent cycles = 0, want at least one persisted cycle")
	}

	safeSequence, err := runtime.Store.SafeSequence(context.Background(), "ETHUSD")
	if err != nil {
		t.Fatalf("Store.SafeSequence() error = %v", err)
	}
	if safeSequence != 1 {
		t.Fatalf("Store.SafeSequence() = %d, want 1", safeSequence)
	}
	if feedRequests.Load() < 2 {
		t.Fatalf("feed requests = %d, want at least 2", feedRequests.Load())
	}

	cancel()

	select {
	case err := <-serveDone:
		if err != nil {
			t.Fatalf("Serve() error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Serve() did not return after cancellation")
	}
}

func TestRunServesUntilContextCancellation(t *testing.T) {
	t.Parallel()

	feedServer := testsupport.NewMarketFeedServer(t)
	defer feedServer.Close()

	sinkCalls := make(chan []byte, 4)
	sinkServer := testsupport.NewRecordingSinkServer(t, func(body []byte, writer http.ResponseWriter, request *http.Request) {
		sinkCalls <- body
		writer.WriteHeader(http.StatusCreated)
	})
	defer sinkServer.Close()

	listenAddr := reserveListenAddr(t)
	config := testsupport.ValidBootstrapConfigWithStoreDSN(testdb.DSN(t))
	config = strings.Replace(config, "listen_addr: 127.0.0.1:0", "listen_addr: "+listenAddr, 1)
	config = strings.Replace(config, "url: http://127.0.0.1:18080/publish", "url: "+sinkServer.URL, 1)
	configPath := testsupport.WriteConfig(t, config)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runDone := make(chan error, 1)
	go func() {
		runDone <- Run(ctx, Options{
			ConfigPath: configPath,
			StoreOpen:  openTestStore,
			FeedBuild: feeds.BuildOptions{
				EndpointOverrides: map[string]string{
					"coinbase-primary": feedServer.URL,
					"kraken-secondary": feedServer.URL,
					"hyperliquid-eth":  feedServer.URL,
				},
			},
		})
	}()

	testsupport.WaitForBody(t, sinkCalls)

	client := http.Client{Timeout: time.Second}
	snapshot := waitForSnapshot(t, client, listenAddr, "ETHUSD")
	if snapshot.LastSuccessfulPublication == nil || snapshot.LastSuccessfulPublication.Sequence != 1 {
		t.Fatalf("GET /snapshotz last successful publication = %+v, want sequence 1", snapshot.LastSuccessfulPublication)
	}

	cancel()

	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("Run() error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run() did not return after cancellation")
	}
}

func waitForSnapshot(t *testing.T, client http.Client, addr string, market string) store.Snapshot {
	t.Helper()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		snapshot, statusCode := getJSON[store.Snapshot](t, client, "http://"+addr+"/snapshotz?market="+market)
		if statusCode == http.StatusOK && snapshot.LastSuccessfulPublication != nil {
			return snapshot
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("snapshot did not become available before timeout")
	return store.Snapshot{}
}

func getJSON[T any](t *testing.T, client http.Client, url string) (T, int) {
	t.Helper()

	var zero T
	response, err := client.Get(url)
	if err != nil {
		t.Fatalf("GET %s error = %v", url, err)
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return zero, response.StatusCode
	}

	var value T
	if err := json.NewDecoder(response.Body).Decode(&value); err != nil {
		t.Fatalf("json.Decode(%s) error = %v", url, err)
	}
	return value, response.StatusCode
}

func reserveListenAddr(t *testing.T) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen() error = %v", err)
	}
	addr := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatalf("listener.Close() error = %v", err)
	}
	return addr
}
