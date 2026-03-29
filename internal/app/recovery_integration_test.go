package app

import (
	"context"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lumosimmo/market-relayer-go/internal/feeds"
	"github.com/lumosimmo/market-relayer-go/internal/operator"
	"github.com/lumosimmo/market-relayer-go/internal/status"
	"github.com/lumosimmo/market-relayer-go/internal/testdb"
	"github.com/lumosimmo/market-relayer-go/internal/testsupport"
)

func TestRecoverResumePendingReplaysPublicationFromStore(t *testing.T) {
	t.Parallel()

	feedServer := testsupport.NewMarketFeedServer(t)
	defer feedServer.Close()

	var sinkHealthy atomic.Bool
	replayed := make(chan []byte, 2)
	sinkServer := testsupport.NewRecordingSinkServer(t, func(body []byte, writer http.ResponseWriter, request *http.Request) {
		if !sinkHealthy.Load() {
			http.Error(writer, "temporary outage", http.StatusServiceUnavailable)
			return
		}
		replayed <- body
		writer.WriteHeader(http.StatusCreated)
	})
	defer sinkServer.Close()

	storeDSN := testdb.DSN(t)
	configPath := testsupport.WriteConfig(t, strings.Replace(testsupport.ValidBootstrapConfigWithStoreDSN(storeDSN), "url: http://127.0.0.1:18080/publish", "url: "+sinkServer.URL, 1))
	seedCycle(t, configPath, feedServer.URL)

	storeDB := openMigratedStoreForTest(t, storeDSN)
	pending, err := storeDB.PendingPublications(context.Background(), "ETHUSD")
	if err != nil {
		t.Fatalf("PendingPublications(before recover) error = %v", err)
	}
	if err := storeDB.Close(); err != nil {
		t.Fatalf("store.Close(before recover) error = %v", err)
	}
	if len(pending) != 1 || pending[0].Sequence != 1 {
		t.Fatalf("PendingPublications(before recover) = %+v, want one sequence 1", pending)
	}

	sinkHealthy.Store(true)

	report, err := Recover(context.Background(), Options{
		ConfigPath: configPath,
		StoreOpen:  openTestStore,
	}, RecoveryOptions{
		Mode:   operator.ModeResumePending,
		Market: "ETHUSD",
	})
	if err != nil {
		t.Fatalf("Recover(resume-pending) error = %v", err)
	}
	if len(report.ReplayedSequences) != 1 || report.ReplayedSequences[0] != 1 {
		t.Fatalf("Recover(resume-pending).ReplayedSequences = %v, want [1]", report.ReplayedSequences)
	}
	if report.NextSequence != 2 {
		t.Fatalf("Recover(resume-pending).NextSequence = %d, want 2", report.NextSequence)
	}

	select {
	case <-replayed:
	case <-time.After(2 * time.Second):
		t.Fatal("working sink did not receive replayed publication")
	}

	recoveredStore := openMigratedStoreForTest(t, storeDSN)
	defer func() {
		if closeErr := recoveredStore.Close(); closeErr != nil {
			t.Fatalf("store.Close(after recover) error = %v", closeErr)
		}
	}()

	pending, err = recoveredStore.PendingPublications(context.Background(), "ETHUSD")
	if err != nil {
		t.Fatalf("PendingPublications(after recover) error = %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("PendingPublications(after recover) = %+v, want empty", pending)
	}

	safeSequence, err := recoveredStore.SafeSequence(context.Background(), "ETHUSD")
	if err != nil {
		t.Fatalf("SafeSequence(after recover) error = %v", err)
	}
	if safeSequence != 1 {
		t.Fatalf("SafeSequence(after recover) = %d, want 1", safeSequence)
	}

	publishability, err := recoveredStore.LatestPublishability(context.Background(), "ETHUSD")
	if err != nil {
		t.Fatalf("LatestPublishability(after recover) error = %v", err)
	}
	if publishability.Status != status.StatusAvailable || publishability.Reason != status.ReasonNone {
		t.Fatalf("LatestPublishability(after recover) = {%q %q}, want {available none}", publishability.Status, publishability.Reason)
	}
	if publishability.LastSequence != 1 || publishability.PendingSequence != 0 {
		t.Fatalf("LatestPublishability(after recover) sequences = {%d %d}, want {1 0}", publishability.LastSequence, publishability.PendingSequence)
	}
}

func TestRecoverExactDefaultsToSingleConfiguredMarket(t *testing.T) {
	t.Parallel()

	feedServer := testsupport.NewMarketFeedServer(t)
	defer feedServer.Close()

	sinkServer := testsupport.NewRecordingSinkServer(t, func(_ []byte, writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusCreated)
	})
	defer sinkServer.Close()

	configPath := testsupport.WriteConfig(t, strings.Replace(testsupport.ValidBootstrapConfigWithStoreDSN(testdb.DSN(t)), "url: http://127.0.0.1:18080/publish", "url: "+sinkServer.URL, 1))
	recordID := seedCycle(t, configPath, feedServer.URL)

	report, err := Recover(context.Background(), Options{
		ConfigPath: configPath,
		StoreOpen:  openTestStore,
	}, RecoveryOptions{
		Mode:     operator.ModeExact,
		RecordID: recordID,
	})
	if err != nil {
		t.Fatalf("Recover(exact) error = %v", err)
	}
	if report.Mode != operator.ModeExact {
		t.Fatalf("Recover(exact).Mode = %q, want %q", report.Mode, operator.ModeExact)
	}
	if report.Record.RecordID != recordID {
		t.Fatalf("Recover(exact).Record.RecordID = %q, want %q", report.Record.RecordID, recordID)
	}
	if !report.MetadataMatch || !report.PricingMatch {
		t.Fatalf("Recover(exact) flags = {metadata:%t pricing:%t}, want true/true", report.MetadataMatch, report.PricingMatch)
	}
}

func seedCycle(t *testing.T, configPath string, feedBaseURL string) string {
	t.Helper()

	runtime, err := Bootstrap(context.Background(), Options{
		ConfigPath: configPath,
		StoreOpen:  openTestStore,
		FeedBuild: feeds.BuildOptions{
			EndpointOverrides: map[string]string{
				"coinbase-primary": feedBaseURL,
				"kraken-secondary": feedBaseURL,
				"hyperliquid-eth":  feedBaseURL,
			},
		},
	})
	if err != nil {
		t.Fatalf("Bootstrap(seed) error = %v", err)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if closeErr := runtime.Close(shutdownCtx); closeErr != nil {
			t.Fatalf("runtime.Close(seed) error = %v", closeErr)
		}
	}()

	if len(runtime.workers) == 0 {
		t.Fatal("seed runtime has no market workers")
	}
	lease, acquired, err := runtime.Store.TryAcquireLease(context.Background(), "ETHUSD", runtime.Config.Service.InstanceID, runtime.Config.Store.LeaseTTL)
	if err != nil {
		t.Fatalf("TryAcquireLease(seed) error = %v", err)
	}
	if !acquired {
		t.Fatal("TryAcquireLease(seed) = false, want true")
	}
	runtime.workers[0].store.UpdateLease(lease)
	defer func() {
		runtime.workers[0].store.ClearLease()
		if releaseErr := runtime.Store.ReleaseLease(context.Background(), lease); releaseErr != nil {
			t.Fatalf("ReleaseLease(seed) error = %v", releaseErr)
		}
	}()

	if err := runtime.Loops[0].RunCycle(context.Background()); err != nil {
		t.Fatalf("RunCycle(seed) error = %v", err)
	}

	recent, err := runtime.Store.RecentCycles(context.Background(), "ETHUSD", 1)
	if err != nil {
		t.Fatalf("RecentCycles(seed) error = %v", err)
	}
	if len(recent) != 1 {
		t.Fatalf("RecentCycles(seed) len = %d, want 1", len(recent))
	}
	return recent[0].RecordID
}
