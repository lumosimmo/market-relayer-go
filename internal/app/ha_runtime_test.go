package app

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lumosimmo/market-relayer-go/internal/api"
	"github.com/lumosimmo/market-relayer-go/internal/clock"
	"github.com/lumosimmo/market-relayer-go/internal/config"
	"github.com/lumosimmo/market-relayer-go/internal/feeds"
	"github.com/lumosimmo/market-relayer-go/internal/operator"
	"github.com/lumosimmo/market-relayer-go/internal/pricing"
	"github.com/lumosimmo/market-relayer-go/internal/relayer"
	"github.com/lumosimmo/market-relayer-go/internal/status"
	"github.com/lumosimmo/market-relayer-go/internal/store"
	"github.com/lumosimmo/market-relayer-go/internal/testdb"
	"github.com/lumosimmo/market-relayer-go/internal/testsupport"
)

func TestBootstrapAndBuildLoopsWithRuntimeStoreStub(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 3, 27, 20, 0, 0, 0, time.UTC)
	storeDB := &appRuntimeDBStub{schemaVersion: store.SchemaVersion}
	configPath := testsupport.WriteConfig(t, testsupport.ValidBootstrapConfigWithStoreDSN("postgres://example.invalid/runtime"))

	runtime, err := Bootstrap(context.Background(), Options{
		ConfigPath: configPath,
		Clock:      clock.Fixed{Time: now},
		StoreOpen: func(context.Context, config.StoreConfig) (runtimeStore, error) {
			return storeDB, nil
		},
	})
	if err != nil {
		t.Fatalf("Bootstrap() error = %v", err)
	}
	t.Cleanup(func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if closeErr := runtime.Close(shutdownCtx); closeErr != nil {
			t.Fatalf("runtime.Close() error = %v", closeErr)
		}
	})

	if runtime.Store != runtimeStore(storeDB) {
		t.Fatal("Bootstrap() did not retain runtime store stub")
	}
	if len(runtime.Loops) != 1 {
		t.Fatalf("len(runtime.Loops) = %d, want 1", len(runtime.Loops))
	}
	if len(runtime.workers) != 1 {
		t.Fatalf("len(runtime.workers) = %d, want 1", len(runtime.workers))
	}
	if runtime.workers[0].owner != runtime.Config.Service.InstanceID {
		t.Fatalf("worker owner = %q, want %q", runtime.workers[0].owner, runtime.Config.Service.InstanceID)
	}
	if runtime.workers[0].leaseReleaseTimeout != runtime.Config.Store.LockTimeout {
		t.Fatalf("worker leaseReleaseTimeout = %s, want %s", runtime.workers[0].leaseReleaseTimeout, runtime.Config.Store.LockTimeout)
	}

	cfg, err := config.LoadBytes([]byte(testsupport.ValidBootstrapConfigWithStoreDSN("postgres://example.invalid/runtime")), config.LoadOptions{})
	if err != nil {
		t.Fatalf("config.LoadBytes() error = %v", err)
	}

	loops, workers, err := buildLoops(
		cfg,
		RuntimeMetadata{
			ConfigDigest:            runtime.Metadata.ConfigDigest,
			PricingAlgorithmVersion: runtime.Metadata.PricingAlgorithmVersion,
		},
		storeDB,
		appNoopSink{},
		feeds.BuildOptions{Clock: clock.Fixed{Time: now}},
		clock.Fixed{Time: now},
		nil,
		nil,
	)
	if err != nil {
		t.Fatalf("buildLoops() error = %v", err)
	}
	if len(loops) != 1 || len(workers) != 1 {
		t.Fatalf("buildLoops() counts = (%d loops, %d workers), want (1, 1)", len(loops), len(workers))
	}
	if workers[0].store == nil {
		t.Fatal("buildLoops() did not wire leased store")
	}
	if workers[0].leaseReleaseTimeout != cfg.Store.LockTimeout {
		t.Fatalf("buildLoops() leaseReleaseTimeout = %s, want %s", workers[0].leaseReleaseTimeout, cfg.Store.LockTimeout)
	}
}

func TestRecoverUsesRuntimeStoreStubForExactAndResumePending(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 3, 27, 20, 5, 0, 0, time.UTC)
	record := store.CycleRecord{
		RecordID: "cycle-0001",
		Market:   "ETHUSD",
		Metadata: store.Metadata{
			ConfigDigest:            "cfg-digest",
			PricingAlgorithmVersion: pricing.AlgorithmVersion,
			SchemaVersion:           store.SchemaVersion,
			EnvelopeVersion:         relayer.EnvelopeVersion,
		},
	}

	t.Run("exact mode verifies record through leased recovery store", func(t *testing.T) {
		t.Parallel()

		storeDB := &appRuntimeDBStub{
			schemaVersion: store.SchemaVersion,
			verifyRecord:  record,
		}

		report, err := Recover(context.Background(), Options{
			ConfigPath: testsupport.WriteConfig(t, testsupport.ValidBootstrapConfigWithStoreDSN("postgres://example.invalid/runtime")),
			StoreOpen: func(context.Context, config.StoreConfig) (runtimeStore, error) {
				return storeDB, nil
			},
		}, RecoveryOptions{
			Mode:     "exact",
			RecordID: record.RecordID,
		})
		if err != nil {
			t.Fatalf("Recover(exact) error = %v", err)
		}
		if report.Record.RecordID != record.RecordID {
			t.Fatalf("Recover(exact).Record.RecordID = %q, want %q", report.Record.RecordID, record.RecordID)
		}
		if storeDB.verifyQuery.RecordID != record.RecordID {
			t.Fatalf("VerifyExact() record id = %q, want %q", storeDB.verifyQuery.RecordID, record.RecordID)
		}
		if storeDB.verifyQuery.Metadata.SchemaVersion != store.SchemaVersion {
			t.Fatalf("VerifyExact() schema version = %q, want %q", storeDB.verifyQuery.Metadata.SchemaVersion, store.SchemaVersion)
		}
		if storeDB.closeCalls != 1 {
			t.Fatalf("store.Close() calls = %d, want 1", storeDB.closeCalls)
		}
	})

	t.Run("resume pending acquires and releases a market lease", func(t *testing.T) {
		t.Parallel()

		lease := store.Lease{
			Market:       "ETHUSD",
			Owner:        "bootstrap-test-recovery",
			FencingToken: 4,
			ExpiresAt:    now.Add(time.Minute),
		}
		storeDB := &appRuntimeDBStub{
			schemaVersion: store.SchemaVersion,
			safeSequence:  9,
			tryAcquireResponses: []appAcquireResult{{
				lease:    lease,
				acquired: true,
			}},
		}

		report, err := Recover(context.Background(), Options{
			ConfigPath: testsupport.WriteConfig(t, testsupport.ValidBootstrapConfigWithStoreDSN("postgres://example.invalid/runtime")),
			StoreOpen: func(context.Context, config.StoreConfig) (runtimeStore, error) {
				return storeDB, nil
			},
		}, RecoveryOptions{
			Mode: "resume-pending",
		})
		if err != nil {
			t.Fatalf("Recover(resume-pending) error = %v", err)
		}
		if report.NextSequence != 10 {
			t.Fatalf("Recover(resume-pending).NextSequence = %d, want 10", report.NextSequence)
		}
		if len(storeDB.tryAcquireCalls) != 1 {
			t.Fatalf("TryAcquireLease() calls = %d, want 1", len(storeDB.tryAcquireCalls))
		}
		if len(storeDB.releaseCalls) != 1 || storeDB.releaseCalls[0] != lease {
			t.Fatalf("ReleaseLease() calls = %+v, want [%+v]", storeDB.releaseCalls, lease)
		}
		if storeDB.closeCalls != 1 {
			t.Fatalf("store.Close() calls = %d, want 1", storeDB.closeCalls)
		}
	})

	t.Run("resume pending renews the lease while recovery is running", func(t *testing.T) {
		t.Parallel()

		now := time.Date(2026, 3, 27, 20, 8, 0, 0, time.UTC)
		lease := store.Lease{
			Market:       "ETHUSD",
			Owner:        "bootstrap-test-recovery",
			FencingToken: 4,
			ExpiresAt:    now.Add(200 * time.Millisecond),
		}
		renewedLease := lease
		renewedLease.ExpiresAt = now.Add(400 * time.Millisecond)

		pendingStartedCh := make(chan struct{}, 1)
		pendingBlockCh := make(chan struct{})
		renewCh := make(chan store.Lease, 3)
		storeDB := &appRuntimeDBStub{
			schemaVersion: store.SchemaVersion,
			tryAcquireResponses: []appAcquireResult{{
				lease:    lease,
				acquired: true,
			}},
			renewResponses: []appRenewResult{
				{lease: renewedLease, renewed: true},
				{lease: renewedLease, renewed: true},
				{lease: renewedLease, renewed: true},
			},
			pendingStartedCh: pendingStartedCh,
			pendingBlockCh:   pendingBlockCh,
			renewCh:          renewCh,
		}

		configText := testsupport.ValidBootstrapConfigWithStoreDSN("postgres://example.invalid/runtime")
		configText = strings.Replace(
			configText,
			`store:
  dsn: "postgres://example.invalid/runtime"
`,
			`store:
  dsn: "postgres://example.invalid/runtime"
  lease_ttl: 200ms
  lease_renew_interval: 20ms
`,
			1,
		)

		resultCh := make(chan operator.RecoveryReport, 1)
		errCh := make(chan error, 1)
		go func() {
			report, err := Recover(context.Background(), Options{
				ConfigPath: testsupport.WriteConfig(t, configText),
				StoreOpen: func(context.Context, config.StoreConfig) (runtimeStore, error) {
					return storeDB, nil
				},
			}, RecoveryOptions{
				Mode: "resume-pending",
			})
			resultCh <- report
			errCh <- err
		}()

		select {
		case <-pendingStartedCh:
		case <-time.After(2 * time.Second):
			t.Fatal("PendingPublications() was not called")
		}
		select {
		case renewed := <-renewCh:
			if renewed != lease {
				t.Fatalf("RenewLease() received lease %+v, want %+v", renewed, lease)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("RenewLease() was not called while recovery was blocked")
		}
		close(pendingBlockCh)

		report := <-resultCh
		if err := <-errCh; err != nil {
			t.Fatalf("Recover(resume-pending with renewal) error = %v", err)
		}
		if report.NextSequence != 1 {
			t.Fatalf("Recover(resume-pending with renewal).NextSequence = %d, want 1", report.NextSequence)
		}
		if len(storeDB.renewCalls) == 0 {
			t.Fatal("RenewLease() calls = 0, want at least one renewal")
		}
		if len(storeDB.releaseCalls) != 1 || storeDB.releaseCalls[0] != renewedLease {
			t.Fatalf("ReleaseLease() calls = %+v, want latest renewed lease %+v", storeDB.releaseCalls, renewedLease)
		}
	})
}

func TestLeasedStoreEnforcesWriteLeaseAndDelegatesReads(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 3, 27, 20, 10, 0, 0, time.UTC)
	lease := store.Lease{
		Market:       "ETHUSD",
		Owner:        "instance-a",
		FencingToken: 7,
		ExpiresAt:    now.Add(time.Minute),
	}
	record := store.CycleRecord{RecordID: "cycle-0001", Market: "ETHUSD"}
	publication := store.PreparedPublication{Market: "ETHUSD", Sequence: 3}
	publishability := store.PublishabilityState{
		Market:          "ETHUSD",
		Status:          status.StatusAvailable,
		Reason:          status.ReasonNone,
		LastStableHash:  "stable-hash",
		PendingSequence: 3,
		UpdatedAt:       now,
	}
	query := store.ExactQuery{
		Market:   "ETHUSD",
		RecordID: record.RecordID,
		Metadata: record.Metadata,
	}
	expectedState := pricing.State{LastOracleAt: now}
	storeDB := &appRuntimeDBStub{
		pendingPublications:  []store.PreparedPublication{publication},
		verifyRecord:         record,
		auditResult:          store.AuditResult{Record: record, MetadataMatch: true},
		previousCycle:        record,
		previousFound:        true,
		safeSequence:         12,
		latestState:          expectedState,
		latestPublishability: publishability,
	}
	leased := store.NewLeaseBoundRuntimeStore(storeDB)

	if _, err := leased.CurrentLease(); err == nil {
		t.Fatal("CurrentLease() error = nil, want lease-missing failure")
	}
	if err := leased.CommitCycle(context.Background(), record); err == nil {
		t.Fatal("CommitCycle() error = nil, want lease-missing failure")
	}
	if err := leased.MarkPublicationSent(context.Background(), "ETHUSD", 3, now); err == nil {
		t.Fatal("MarkPublicationSent() error = nil, want lease-missing failure")
	}
	if err := leased.AcknowledgePublication(context.Background(), "ETHUSD", 3, now); err == nil {
		t.Fatal("AcknowledgePublication() error = nil, want lease-missing failure")
	}
	if err := leased.SavePublishability(context.Background(), publishability); err == nil {
		t.Fatal("SavePublishability() error = nil, want lease-missing failure")
	}

	leased.UpdateLease(lease)
	if got, err := leased.CurrentLease(); err != nil || got != lease {
		t.Fatalf("CurrentLease() = (%+v, %v), want (%+v, nil)", got, err, lease)
	}
	if err := leased.CommitCycle(context.Background(), record); err != nil {
		t.Fatalf("CommitCycle() error = %v", err)
	}
	if err := leased.MarkPublicationSent(context.Background(), "ETHUSD", 3, now); err != nil {
		t.Fatalf("MarkPublicationSent() error = %v", err)
	}
	if err := leased.AcknowledgePublication(context.Background(), "ETHUSD", 3, now); err != nil {
		t.Fatalf("AcknowledgePublication() error = %v", err)
	}
	if err := leased.SavePublishability(context.Background(), publishability); err != nil {
		t.Fatalf("SavePublishability() error = %v", err)
	}
	if got, err := leased.PendingPublications(context.Background(), "ETHUSD"); err != nil || len(got) != 1 || got[0].Sequence != publication.Sequence {
		t.Fatalf("PendingPublications() = (%+v, %v), want sequence %d", got, err, publication.Sequence)
	}
	if got, err := leased.VerifyExact(context.Background(), query); err != nil || got.RecordID != record.RecordID {
		t.Fatalf("VerifyExact() = (%+v, %v), want record %q", got, err, record.RecordID)
	}
	if got, err := leased.Audit(context.Background(), query); err != nil || got.Record.RecordID != record.RecordID {
		t.Fatalf("Audit() = (%+v, %v), want record %q", got, err, record.RecordID)
	}
	if got, found, err := leased.PreviousCycle(context.Background(), "ETHUSD", record.RecordID); err != nil || !found || got.RecordID != record.RecordID {
		t.Fatalf("PreviousCycle() = (%+v, %t, %v), want record %q", got, found, err, record.RecordID)
	}
	if got, err := leased.SafeSequence(context.Background(), "ETHUSD"); err != nil || got != 12 {
		t.Fatalf("SafeSequence() = (%d, %v), want (12, nil)", got, err)
	}
	if got, err := leased.LatestState(context.Background(), "ETHUSD"); err != nil || !got.LastOracleAt.Equal(expectedState.LastOracleAt) {
		t.Fatalf("LatestState() = (%+v, %v), want %+v", got, err, expectedState)
	}
	if got, err := leased.LatestPublishability(context.Background(), "ETHUSD"); err != nil || got.LastStableHash != publishability.LastStableHash {
		t.Fatalf("LatestPublishability() = (%+v, %v), want %+v", got, err, publishability)
	}

	if len(storeDB.commitCalls) != 1 || storeDB.commitCalls[0].lease != lease {
		t.Fatalf("CommitCycleWithLease() calls = %+v, want lease %+v", storeDB.commitCalls, lease)
	}
	if len(storeDB.markSentCalls) != 1 || storeDB.markSentCalls[0].lease != lease {
		t.Fatalf("MarkPublicationSentWithLease() calls = %+v, want lease %+v", storeDB.markSentCalls, lease)
	}
	if len(storeDB.ackCalls) != 1 || storeDB.ackCalls[0].lease != lease {
		t.Fatalf("AcknowledgePublicationWithLease() calls = %+v, want lease %+v", storeDB.ackCalls, lease)
	}
	if len(storeDB.savePublishabilityCalls) != 1 || storeDB.savePublishabilityCalls[0].lease != lease {
		t.Fatalf("SavePublishabilityWithLease() calls = %+v, want lease %+v", storeDB.savePublishabilityCalls, lease)
	}

	leased.ClearLease()
	if _, err := leased.CurrentLease(); err == nil {
		t.Fatal("CurrentLease() after ClearLease = nil, want lease-missing failure")
	}
}

func TestMarketWorkerRunCancelsLoopOnLeaseLossAndReleasesState(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 3, 27, 20, 15, 0, 0, time.UTC)
	initialLease := store.Lease{
		Market:       "ETHUSD",
		Owner:        "instance-a",
		FencingToken: 1,
		ExpiresAt:    now.Add(20 * time.Millisecond),
	}
	renewedLease := initialLease
	renewedLease.FencingToken = 2
	renewedLease.ExpiresAt = now.Add(40 * time.Millisecond)

	storeDB := &appRuntimeDBStub{
		tryAcquireResponses: []appAcquireResult{{
			lease:    initialLease,
			acquired: true,
		}},
		renewResponses: []appRenewResult{
			{lease: renewedLease, renewed: true},
			{renewed: false},
		},
		releaseCh: make(chan store.Lease, 1),
	}
	leased := store.NewLeaseBoundRuntimeStore(storeDB)
	loop := newAppTestLoop(t, appTestMarket("ETHUSD"), leased, appBlockingSource{name: "coinbase"}, clock.Fixed{Time: now})
	state := &runtimeState{
		leases:   map[string]leaseStatus{"ETHUSD": {}},
		loopLive: map[string]bool{"ETHUSD": true},
	}
	worker := &marketWorker{
		market:             appTestMarket("ETHUSD"),
		owner:              "instance-a",
		db:                 storeDB,
		loop:               loop,
		store:              leased,
		leaseTTL:           40 * time.Millisecond,
		leaseRenewInterval: 10 * time.Millisecond,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- worker.Run(ctx, state)
	}()

	var released store.Lease
	select {
	case released = <-storeDB.releaseCh:
	case <-time.After(2 * time.Second):
		t.Fatal("ReleaseLease() was not called")
	}
	cancel()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Run() error = %v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run() did not exit after cancellation")
	}

	if released.FencingToken != renewedLease.FencingToken {
		t.Fatalf("released fencing token = %d, want %d", released.FencingToken, renewedLease.FencingToken)
	}
	if _, err := leased.CurrentLease(); err == nil {
		t.Fatal("leased store still holds a lease after reset")
	}
	if got := state.leaseSnapshot("ETHUSD"); got != (leaseStatus{}) {
		t.Fatalf("leaseSnapshot() = %+v, want cleared lease status", got)
	}
	if len(storeDB.renewCalls) != 2 {
		t.Fatalf("RenewLease() calls = %d, want 2", len(storeDB.renewCalls))
	}
}

func TestMarketWorkerRunReturnsLoopErrorAfterLeaseAcquisition(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 3, 27, 20, 20, 0, 0, time.UTC)
	lease := store.Lease{
		Market:       "ETHUSD",
		Owner:        "instance-a",
		FencingToken: 3,
		ExpiresAt:    now.Add(time.Minute),
	}
	storeDB := &appRuntimeDBStub{
		tryAcquireResponses: []appAcquireResult{{
			lease:    lease,
			acquired: true,
		}},
	}
	leased := store.NewLeaseBoundRuntimeStore(storeDB)
	loop := newAppTestLoop(t, appTestMarket("ETHUSD"), leased, appStaticSource{
		name:  "coinbase",
		quote: appQuote(t, "coinbase", "BTCUSD", "65941.00", now),
	}, clock.Fixed{Time: now})
	state := &runtimeState{leases: map[string]leaseStatus{"ETHUSD": {}}}
	worker := &marketWorker{
		market:             appTestMarket("ETHUSD"),
		owner:              "instance-a",
		db:                 storeDB,
		loop:               loop,
		store:              leased,
		leaseTTL:           time.Minute,
		leaseRenewInterval: 10 * time.Millisecond,
	}

	err := worker.Run(context.Background(), state)
	if err == nil {
		t.Fatal("Run() error = nil, want loop failure")
	}
	if !strings.Contains(err.Error(), "does not match market") {
		t.Fatalf("Run() error = %q, want market mismatch", err.Error())
	}
	if len(storeDB.releaseCalls) != 1 || storeDB.releaseCalls[0] != lease {
		t.Fatalf("ReleaseLease() calls = %+v, want [%+v]", storeDB.releaseCalls, lease)
	}
	if _, currentErr := leased.CurrentLease(); currentErr == nil {
		t.Fatal("leased store still holds a lease after loop error")
	}
}

func TestMarketWorkerResetLeaseStateUsesReleaseTimeoutBudget(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 3, 27, 20, 22, 0, 0, time.UTC)
	lease := store.Lease{
		Market:       "ETHUSD",
		Owner:        "instance-a",
		FencingToken: 3,
		ExpiresAt:    now.Add(time.Minute),
	}
	storeDB := &appRuntimeDBStub{}
	leased := store.NewLeaseBoundRuntimeStore(storeDB)
	leased.UpdateLease(lease)
	loop := newAppTestLoop(t, appTestMarket("ETHUSD"), leased, appStaticSource{
		name:  "coinbase",
		quote: appQuote(t, "coinbase", "ETHUSD", "1981.75", now),
	}, clock.Fixed{Time: now})
	worker := &marketWorker{
		market:              appTestMarket("ETHUSD"),
		owner:               "instance-a",
		db:                  storeDB,
		loop:                loop,
		store:               leased,
		leaseTTL:            time.Minute,
		leaseRenewInterval:  10 * time.Millisecond,
		leaseReleaseTimeout: 250 * time.Millisecond,
	}

	worker.resetLeaseState(nil)

	if len(storeDB.releaseCalls) != 1 || storeDB.releaseCalls[0] != lease {
		t.Fatalf("ReleaseLease() calls = %+v, want [%+v]", storeDB.releaseCalls, lease)
	}
	if !storeDB.releaseDeadlineFound {
		t.Fatal("ReleaseLease() context did not carry a deadline")
	}
	if got := storeDB.releaseDeadline.Sub(storeDB.releaseStartedAt); got < 150*time.Millisecond || got > time.Second {
		t.Fatalf("ReleaseLease() deadline budget = %s, want approximately 250ms", got)
	}
	if _, err := leased.CurrentLease(); err == nil {
		t.Fatal("leased store still holds a lease after reset")
	}
}

func TestRuntimeStateReportsOwnershipThroughHealthStatusAndSnapshot(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 3, 27, 20, 25, 0, 0, time.UTC)
	leaseExpiry := now.Add(time.Minute)
	storeDB := &appRuntimeDBStub{
		schemaVersion: store.SchemaVersion,
		latestSnapshots: map[string]appSnapshotResult{
			"ETHUSD": {
				snapshot: statusSnapshot(now, pricing.Result{
					SessionState:    status.SessionStateOpen,
					SessionReason:   status.ReasonNone,
					External:        price(status.StatusAvailable, status.ReasonNone, "1981.75"),
					Oracle:          price(status.StatusAvailable, status.ReasonNone, "1981.75"),
					OraclePlusBasis: price(status.StatusAvailable, status.ReasonNone, "1981.75"),
					State: pricing.State{
						LastOracleAt:            now.Add(-5 * time.Second),
						OracleFallbackExpiresAt: now.Add(20 * time.Second),
					},
				}, status.NewCondition(status.StatusAvailable, status.ReasonNone)),
			},
			"BTCUSD": {
				err: errors.New("latest snapshot unavailable"),
			},
		},
	}

	cfg := &config.Config{
		Markets: []config.MarketConfig{
			appTestMarket("ETHUSD"),
			appTestMarket("BTCUSD"),
		},
	}
	loops := []*relayer.Loop{
		newAppTestLoop(t, appTestMarket("ETHUSD"), store.NewLeaseBoundRuntimeStore(storeDB), appStaticSource{
			name:  "coinbase",
			quote: appQuote(t, "coinbase", "ETHUSD", "1981.75", now),
		}, clock.Fixed{Time: now}),
		newAppTestLoop(t, appTestMarket("BTCUSD"), store.NewLeaseBoundRuntimeStore(storeDB), appStaticSource{
			name:  "coinbase",
			quote: appQuote(t, "coinbase", "BTCUSD", "65941.00", now),
		}, clock.Fixed{Time: now}),
	}

	state := newRuntimeState(api.Metadata{ServiceName: "market-relayer-go"}, clock.Fixed{Time: now}, cfg, storeDB, loops)
	if got := state.Ready(context.Background()); got.Ready {
		t.Fatalf("Ready() before markLoopsStarted = %+v, want not ready", got)
	}
	if got := state.Health(context.Background()); got.Healthy {
		t.Fatalf("Health() before markLoopsStarted = %+v, want unhealthy", got)
	}

	state.markLoopsStarted()
	state.setLeaseStatus("ETHUSD", store.Lease{
		Market:    "ETHUSD",
		Owner:     "instance-a",
		ExpiresAt: leaseExpiry,
	}, true)
	state.setLeaseStatus("BTCUSD", store.Lease{
		Market: "BTCUSD",
		Owner:  "instance-b",
	}, false)

	if got := state.Ready(context.Background()); !got.Ready {
		t.Fatalf("Ready() after markLoopsStarted = %+v, want ready", got)
	}
	if got := state.Health(context.Background()); !got.Healthy || !got.Checks.LeaseControllerLive {
		t.Fatalf("Health() after markLoopsStarted = %+v, want healthy lease controller", got)
	}

	statusResponse, err := state.Status(context.Background())
	if err != nil {
		t.Fatalf("Status() error = %v", err)
	}
	ethStatus := findMarketStatus(t, statusResponse.Markets, "ETHUSD")
	if !ethStatus.OwnedByThisInstance || ethStatus.LeaseOwner != "instance-a" || ethStatus.LeaseExpiresAt == nil {
		t.Fatalf("ETH status = %+v, want owned market lease metadata", ethStatus)
	}
	btcStatus := findMarketStatus(t, statusResponse.Markets, "BTCUSD")
	if btcStatus.State != "unpublishable" || btcStatus.LeaseOwner != "instance-b" {
		t.Fatalf("BTC status = %+v, want unpublishable store-backed standby market", btcStatus)
	}

	snapshot, err := state.Snapshot(context.Background(), "ETHUSD")
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}
	if !snapshot.OwnedByThisInstance || snapshot.LeaseOwner != "instance-a" || snapshot.LeaseExpiresAt == nil {
		t.Fatalf("Snapshot() = %+v, want lease ownership metadata", snapshot)
	}

	state.markLoopStopped("ETHUSD")
	if got := state.Health(context.Background()); got.Healthy || got.Checks.LoopLive {
		t.Fatalf("Health() after markLoopStopped = %+v, want unhealthy stopped loop", got)
	}
	if got := state.leaseSnapshot("ETHUSD"); got != (leaseStatus{}) {
		t.Fatalf("leaseSnapshot() after stop = %+v, want cleared lease", got)
	}
}

func TestMigrationHelpersLoadConfigAndDriveStoreVersionChecks(t *testing.T) {
	t.Parallel()

	t.Run("loadStoreConfig returns parsed runtime config", func(t *testing.T) {
		t.Parallel()

		configText := testsupport.ValidBootstrapConfigWithStoreDSN("postgres://example.invalid/runtime")
		cfg, err := loadStoreConfig(Options{ConfigPath: testsupport.WriteConfig(t, configText)})
		if err != nil {
			t.Fatalf("loadStoreConfig() error = %v", err)
		}
		if cfg.Store.DSN != "postgres://example.invalid/runtime" {
			t.Fatalf("loadStoreConfig().Store.DSN = %q, want postgres://example.invalid/runtime", cfg.Store.DSN)
		}
	})

	t.Run("migration helpers surface config and store errors", func(t *testing.T) {
		t.Parallel()

		if _, err := loadStoreConfig(Options{ConfigPath: "missing-config.yaml"}); err == nil {
			t.Fatal("loadStoreConfig() error = nil, want config load failure")
		}
		invalid := Options{ConfigPath: testsupport.WriteConfig(t, testsupport.ValidBootstrapConfigWithStoreDSN("://bad-dsn"))}
		if _, err := MigrateStore(context.Background(), invalid); err == nil {
			t.Fatal("MigrateStore() error = nil, want invalid dsn failure")
		}
		if _, err := CurrentStoreVersion(context.Background(), invalid); err == nil {
			t.Fatal("CurrentStoreVersion() error = nil, want invalid dsn failure")
		}
	})

	t.Run("migration helpers return current store version", func(t *testing.T) {
		t.Parallel()

		options := Options{ConfigPath: testsupport.WriteConfig(t, testsupport.ValidBootstrapConfigWithStoreDSN(testdb.DSN(t)))}
		version, err := MigrateStore(context.Background(), options)
		if err != nil {
			t.Fatalf("MigrateStore() error = %v", err)
		}
		current, err := CurrentStoreVersion(context.Background(), options)
		if err != nil {
			t.Fatalf("CurrentStoreVersion() error = %v", err)
		}
		if current != version {
			t.Fatalf("CurrentStoreVersion() = %d, want %d", current, version)
		}
	})
}

func TestServeAndRuntimeCloseManageLifecycle(t *testing.T) {
	t.Parallel()

	t.Run("serve closes runtime dependencies after empty loop set exits", func(t *testing.T) {
		t.Parallel()

		server := newAppTestServer(t)
		server.Start()

		storeDB := &appRuntimeDBStub{schemaVersion: store.SchemaVersion}
		sink := &appCloseSink{}
		runtime := &Runtime{
			Config: &config.Config{API: config.APIConfig{ShutdownTimeout: time.Second}},
			Store:  storeDB,
			Sink:   sink,
			API:    server,
			state:  &runtimeState{},
		}

		if err := Serve(context.Background(), runtime); err != nil {
			t.Fatalf("Serve() error = %v", err)
		}
		if storeDB.closeCalls != 1 {
			t.Fatalf("store.Close() calls = %d, want 1", storeDB.closeCalls)
		}
		if sink.closeCalls != 1 {
			t.Fatalf("sink.Close() calls = %d, want 1", sink.closeCalls)
		}
	})

	t.Run("runtime close joins sink and store shutdown errors", func(t *testing.T) {
		t.Parallel()

		server := newAppTestServer(t)
		server.Start()

		storeDB := &appRuntimeDBStub{
			schemaVersion: store.SchemaVersion,
			closeErr:      errors.New("store close failed"),
		}
		sink := &appCloseSink{closeErr: errors.New("sink close failed")}
		runtime := &Runtime{
			Store: storeDB,
			Sink:  sink,
			API:   server,
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		err := runtime.Close(ctx)
		if err == nil {
			t.Fatal("Close() error = nil, want joined close failures")
		}
		if !strings.Contains(err.Error(), "store close failed") || !strings.Contains(err.Error(), "sink close failed") {
			t.Fatalf("Close() error = %q, want sink and store close failures", err.Error())
		}
	})
}

type appRuntimeDBStub struct {
	mu sync.Mutex

	schemaVersion           string
	schemaErr               error
	verifyRecord            store.CycleRecord
	verifyErr               error
	verifyQuery             store.ExactQuery
	auditResult             store.AuditResult
	auditErr                error
	auditQuery              store.ExactQuery
	previousCycle           store.CycleRecord
	previousFound           bool
	previousErr             error
	pendingPublications     []store.PreparedPublication
	pendingErr              error
	pendingStartedCh        chan struct{}
	pendingBlockCh          chan struct{}
	safeSequence            uint64
	safeSequenceErr         error
	latestState             pricing.State
	latestStateErr          error
	latestPublishability    store.PublishabilityState
	latestPublishErr        error
	latestSnapshots         map[string]appSnapshotResult
	recentCycles            []store.CycleRecord
	recentCyclesErr         error
	tryAcquireResponses     []appAcquireResult
	tryAcquireCalls         []appLeaseAcquireCall
	renewResponses          []appRenewResult
	renewCalls              []store.Lease
	renewCh                 chan store.Lease
	releaseCalls            []store.Lease
	releaseCh               chan store.Lease
	releaseStartedAt        time.Time
	releaseDeadline         time.Time
	releaseDeadlineFound    bool
	currentLease            store.Lease
	currentLeaseFound       bool
	currentLeaseErr         error
	commitCalls             []appCommitCall
	markSentCalls           []appMarkSentCall
	ackCalls                []appAckCall
	savePublishabilityCalls []appSavePublishabilityCall
	closeCalls              int
	closeErr                error
}

type appSnapshotResult struct {
	snapshot store.Snapshot
	err      error
}

type appAcquireResult struct {
	lease    store.Lease
	acquired bool
	err      error
}

type appRenewResult struct {
	lease   store.Lease
	renewed bool
	err     error
}

type appLeaseAcquireCall struct {
	market string
	owner  string
	ttl    time.Duration
}

type appCommitCall struct {
	lease  store.Lease
	record store.CycleRecord
}

type appMarkSentCall struct {
	lease    store.Lease
	market   string
	sequence uint64
	sentAt   time.Time
}

type appAckCall struct {
	lease    store.Lease
	market   string
	sequence uint64
	ackedAt  time.Time
}

type appSavePublishabilityCall struct {
	lease store.Lease
	state store.PublishabilityState
}

func (storeDB *appRuntimeDBStub) Close() error {
	storeDB.mu.Lock()
	defer storeDB.mu.Unlock()
	storeDB.closeCalls++
	return storeDB.closeErr
}

func (storeDB *appRuntimeDBStub) Ping(context.Context) error {
	storeDB.mu.Lock()
	defer storeDB.mu.Unlock()
	if storeDB.schemaErr != nil {
		return storeDB.schemaErr
	}
	return nil
}

func (storeDB *appRuntimeDBStub) CommitCycleWithLease(_ context.Context, lease store.Lease, record store.CycleRecord) error {
	storeDB.mu.Lock()
	defer storeDB.mu.Unlock()
	storeDB.commitCalls = append(storeDB.commitCalls, appCommitCall{lease: lease, record: record})
	return nil
}

func (storeDB *appRuntimeDBStub) VerifyExact(_ context.Context, query store.ExactQuery) (store.CycleRecord, error) {
	storeDB.mu.Lock()
	defer storeDB.mu.Unlock()
	storeDB.verifyQuery = query
	return storeDB.verifyRecord, storeDB.verifyErr
}

func (storeDB *appRuntimeDBStub) Audit(_ context.Context, query store.ExactQuery) (store.AuditResult, error) {
	storeDB.mu.Lock()
	defer storeDB.mu.Unlock()
	storeDB.auditQuery = query
	return storeDB.auditResult, storeDB.auditErr
}

func (storeDB *appRuntimeDBStub) PreviousCycle(context.Context, string, string) (store.CycleRecord, bool, error) {
	storeDB.mu.Lock()
	defer storeDB.mu.Unlock()
	return storeDB.previousCycle, storeDB.previousFound, storeDB.previousErr
}

func (storeDB *appRuntimeDBStub) PendingPublications(ctx context.Context, _ string) ([]store.PreparedPublication, error) {
	storeDB.mu.Lock()
	publications := append([]store.PreparedPublication(nil), storeDB.pendingPublications...)
	err := storeDB.pendingErr
	startedCh := storeDB.pendingStartedCh
	blockCh := storeDB.pendingBlockCh
	storeDB.mu.Unlock()

	if startedCh != nil {
		select {
		case startedCh <- struct{}{}:
		default:
		}
	}
	if blockCh != nil {
		select {
		case <-blockCh:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	return publications, err
}

func (storeDB *appRuntimeDBStub) MarkPublicationSentWithLease(_ context.Context, lease store.Lease, market string, sequence uint64, sentAt time.Time) error {
	storeDB.mu.Lock()
	defer storeDB.mu.Unlock()
	storeDB.markSentCalls = append(storeDB.markSentCalls, appMarkSentCall{
		lease:    lease,
		market:   market,
		sequence: sequence,
		sentAt:   sentAt,
	})
	return nil
}

func (storeDB *appRuntimeDBStub) AcknowledgePublicationWithLease(_ context.Context, lease store.Lease, market string, sequence uint64, ackedAt time.Time) error {
	storeDB.mu.Lock()
	defer storeDB.mu.Unlock()
	storeDB.ackCalls = append(storeDB.ackCalls, appAckCall{
		lease:    lease,
		market:   market,
		sequence: sequence,
		ackedAt:  ackedAt,
	})
	return nil
}

func (storeDB *appRuntimeDBStub) SafeSequence(context.Context, string) (uint64, error) {
	storeDB.mu.Lock()
	defer storeDB.mu.Unlock()
	return storeDB.safeSequence, storeDB.safeSequenceErr
}

func (storeDB *appRuntimeDBStub) LatestState(context.Context, string) (pricing.State, error) {
	storeDB.mu.Lock()
	defer storeDB.mu.Unlock()
	return storeDB.latestState, storeDB.latestStateErr
}

func (storeDB *appRuntimeDBStub) LatestPublishability(context.Context, string) (store.PublishabilityState, error) {
	storeDB.mu.Lock()
	defer storeDB.mu.Unlock()
	return storeDB.latestPublishability, storeDB.latestPublishErr
}

func (storeDB *appRuntimeDBStub) SavePublishabilityWithLease(_ context.Context, lease store.Lease, state store.PublishabilityState) error {
	storeDB.mu.Lock()
	defer storeDB.mu.Unlock()
	storeDB.savePublishabilityCalls = append(storeDB.savePublishabilityCalls, appSavePublishabilityCall{
		lease: lease,
		state: state,
	})
	return nil
}

func (storeDB *appRuntimeDBStub) LatestSnapshot(_ context.Context, market string) (store.Snapshot, error) {
	storeDB.mu.Lock()
	defer storeDB.mu.Unlock()
	result, ok := storeDB.latestSnapshots[market]
	if !ok {
		return store.Snapshot{}, errors.New("latest snapshot unavailable")
	}
	return result.snapshot, result.err
}

func (storeDB *appRuntimeDBStub) RecentCycles(context.Context, string, int) ([]store.CycleRecord, error) {
	storeDB.mu.Lock()
	defer storeDB.mu.Unlock()
	return append([]store.CycleRecord(nil), storeDB.recentCycles...), storeDB.recentCyclesErr
}

func (storeDB *appRuntimeDBStub) TryAcquireLease(_ context.Context, market string, owner string, ttl time.Duration) (store.Lease, bool, error) {
	storeDB.mu.Lock()
	defer storeDB.mu.Unlock()
	storeDB.tryAcquireCalls = append(storeDB.tryAcquireCalls, appLeaseAcquireCall{market: market, owner: owner, ttl: ttl})
	if len(storeDB.tryAcquireResponses) == 0 {
		return store.Lease{}, false, nil
	}
	result := storeDB.tryAcquireResponses[0]
	storeDB.tryAcquireResponses = storeDB.tryAcquireResponses[1:]
	return result.lease, result.acquired, result.err
}

func (storeDB *appRuntimeDBStub) RenewLease(_ context.Context, lease store.Lease, _ time.Duration) (store.Lease, bool, error) {
	storeDB.mu.Lock()
	defer storeDB.mu.Unlock()
	storeDB.renewCalls = append(storeDB.renewCalls, lease)
	renewCh := storeDB.renewCh
	if len(storeDB.renewResponses) == 0 {
		sendRenewSignal(renewCh, lease)
		return store.Lease{}, false, nil
	}
	result := storeDB.renewResponses[0]
	storeDB.renewResponses = storeDB.renewResponses[1:]
	sendRenewSignal(renewCh, lease)
	return result.lease, result.renewed, result.err
}

func sendRenewSignal(ch chan store.Lease, lease store.Lease) {
	if ch == nil {
		return
	}
	select {
	case ch <- lease:
	default:
	}
}

func (storeDB *appRuntimeDBStub) ReleaseLease(ctx context.Context, lease store.Lease) error {
	storeDB.mu.Lock()
	storeDB.releaseCalls = append(storeDB.releaseCalls, lease)
	storeDB.releaseStartedAt = time.Now()
	storeDB.releaseDeadline, storeDB.releaseDeadlineFound = ctx.Deadline()
	releaseCh := storeDB.releaseCh
	storeDB.mu.Unlock()

	if releaseCh != nil {
		releaseCh <- lease
	}
	return nil
}

func (storeDB *appRuntimeDBStub) CurrentLease(context.Context, string) (store.Lease, bool, error) {
	storeDB.mu.Lock()
	defer storeDB.mu.Unlock()
	return storeDB.currentLease, storeDB.currentLeaseFound, storeDB.currentLeaseErr
}

type appCloseSink struct {
	mu         sync.Mutex
	closeCalls int
	closeErr   error
}

func (sink *appCloseSink) Kind() string {
	return "close-test"
}

func (sink *appCloseSink) Publish(context.Context, store.PreparedPublication) (relayer.Ack, error) {
	return relayer.Ack{}, nil
}

func (sink *appCloseSink) Close() error {
	sink.mu.Lock()
	defer sink.mu.Unlock()
	sink.closeCalls++
	return sink.closeErr
}

type appBlockingSource struct {
	name string
}

func (source appBlockingSource) Name() string {
	return source.name
}

func (source appBlockingSource) Fetch(ctx context.Context, _ feeds.FetchBudget) (feeds.Quote, error) {
	<-ctx.Done()
	return feeds.Quote{}, ctx.Err()
}

func appTestMarket(symbol string) config.MarketConfig {
	return config.MarketConfig{
		Symbol:                 symbol,
		SessionMode:            config.SessionModeAlwaysOpen,
		PublishInterval:        3 * time.Second,
		StalenessThreshold:     10 * time.Second,
		MaxFallbackAge:         30 * time.Second,
		ClampBPS:               50,
		DivergenceThresholdBPS: 250,
		PublishScale:           2,
	}
}

func newAppTestLoop(t *testing.T, market config.MarketConfig, storeDB relayer.Store, source feeds.Source, serviceClock clock.Clock) *relayer.Loop {
	t.Helper()

	loop, err := relayer.NewLoop(relayer.LoopOptions{
		Market: market,
		Metadata: store.Metadata{
			ConfigDigest:            "config-digest",
			PricingAlgorithmVersion: pricing.AlgorithmVersion,
			SchemaVersion:           store.SchemaVersion,
			EnvelopeVersion:         relayer.EnvelopeVersion,
		},
		Store:   storeDB,
		Sink:    appNoopSink{},
		Sources: []feeds.Source{source},
		Clock:   serviceClock,
		Budgets: relayer.PhaseBudgets{
			Fetch:   100 * time.Millisecond,
			Compute: 100 * time.Millisecond,
			Persist: 100 * time.Millisecond,
			Publish: 100 * time.Millisecond,
		},
		Attempts: relayer.AttemptBudgets{PublishAttemptTimeout: 50 * time.Millisecond},
	})
	if err != nil {
		t.Fatalf("relayer.NewLoop() error = %v", err)
	}
	return loop
}

func newAppTestServer(t *testing.T) *api.Server {
	t.Helper()

	server, err := api.NewServer(api.Options{
		ListenAddr: "127.0.0.1:0",
	})
	if err != nil {
		t.Fatalf("api.NewServer() error = %v", err)
	}
	return server
}

func findMarketStatus(t *testing.T, markets []api.MarketStatus, symbol string) api.MarketStatus {
	t.Helper()

	for _, market := range markets {
		if market.Market == symbol {
			return market
		}
	}
	t.Fatalf("market %q not found in status response: %+v", symbol, markets)
	return api.MarketStatus{}
}
