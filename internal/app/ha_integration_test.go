package app

import (
	"context"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lumosimmo/market-relayer-go/internal/api"
	"github.com/lumosimmo/market-relayer-go/internal/feeds"
	"github.com/lumosimmo/market-relayer-go/internal/testdb"
	"github.com/lumosimmo/market-relayer-go/internal/testsupport"
)

func TestPostgresHAUsesSingleOwnerPerMarket(t *testing.T) {
	dsn := testdb.DSN(t)

	feedServer := testsupport.NewMarketFeedServer(t)
	defer feedServer.Close()

	var sinkCalls atomic.Int32
	sinkServer := testsupport.NewRecordingSinkServer(t, func(_ []byte, writer http.ResponseWriter, request *http.Request) {
		sinkCalls.Add(1)
		writer.WriteHeader(http.StatusCreated)
	})
	defer sinkServer.Close()

	configA := testsupport.WriteConfig(t, postgresBootstrapConfig(t, dsn, "ha-a", sinkServer.URL))
	if _, err := MigrateStore(context.Background(), Options{ConfigPath: configA}); err != nil {
		t.Fatalf("MigrateStore() error = %v", err)
	}
	configB := testsupport.WriteConfig(t, postgresBootstrapConfig(t, dsn, "ha-b", sinkServer.URL))

	buildOptions := feeds.BuildOptions{
		EndpointOverrides: map[string]string{
			"coinbase-primary": feedServer.URL,
			"kraken-secondary": feedServer.URL,
			"hyperliquid-eth":  feedServer.URL,
		},
	}
	runtimeA, err := Bootstrap(context.Background(), Options{
		ConfigPath: configA,
		FeedBuild:  buildOptions,
	})
	if err != nil {
		t.Fatalf("Bootstrap(runtimeA) error = %v", err)
	}
	runtimeB, err := Bootstrap(context.Background(), Options{
		ConfigPath: configB,
		FeedBuild:  buildOptions,
	})
	if err != nil {
		t.Fatalf("Bootstrap(runtimeB) error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	serveA := make(chan error, 1)
	go func() {
		serveA <- Serve(ctx, runtimeA)
	}()
	serveB := make(chan error, 1)
	go func() {
		serveB <- Serve(ctx, runtimeB)
	}()

	deadline := time.Now().Add(5 * time.Second)
	client := http.Client{Timeout: time.Second}
	observedOwner := false
	for time.Now().Before(deadline) {
		statusA, codeA := getJSON[api.StatusResponse](t, client, "http://"+runtimeA.API.Addr()+"/statusz")
		statusB, codeB := getJSON[api.StatusResponse](t, client, "http://"+runtimeB.API.Addr()+"/statusz")
		if codeA != http.StatusOK || codeB != http.StatusOK || len(statusA.Markets) != 1 || len(statusB.Markets) != 1 {
			time.Sleep(50 * time.Millisecond)
			continue
		}

		ownerA := statusA.Markets[0].OwnedByThisInstance
		ownerB := statusB.Markets[0].OwnedByThisInstance
		if ownerA && ownerB {
			t.Fatalf("both runtimes own ETHUSD: runtimeA=%+v runtimeB=%+v", statusA.Markets[0], statusB.Markets[0])
		}
		if ownerA || ownerB {
			observedOwner = true
		}
		if sinkCalls.Load() > 0 && observedOwner {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	if !observedOwner {
		t.Fatal("no runtime reported market ownership before timeout")
	}
	if sinkCalls.Load() == 0 {
		t.Fatal("sink did not receive a publication before timeout")
	}

	cancel()

	select {
	case err := <-serveA:
		if err != nil {
			t.Fatalf("Serve(runtimeA) error = %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Serve(runtimeA) did not return after cancellation")
	}
	select {
	case err := <-serveB:
		if err != nil {
			t.Fatalf("Serve(runtimeB) error = %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Serve(runtimeB) did not return after cancellation")
	}
}

func postgresBootstrapConfig(t *testing.T, dsn string, instanceID string, sinkURL string) string {
	t.Helper()

	configText := testsupport.ValidBootstrapConfigWithStoreDSN(dsn)
	configText = strings.Replace(configText, "instance_id: bootstrap-test", "instance_id: "+instanceID, 1)
	configText = strings.Replace(configText, "url: http://127.0.0.1:18080/publish", "url: "+sinkURL, 1)
	return configText
}
