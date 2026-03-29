package store

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/lumosimmo/market-relayer-go/internal/config"
	"github.com/lumosimmo/market-relayer-go/internal/feeds"
	"github.com/lumosimmo/market-relayer-go/internal/fixedpoint"
	"github.com/lumosimmo/market-relayer-go/internal/pricing"
	"github.com/lumosimmo/market-relayer-go/internal/status"
	"github.com/lumosimmo/market-relayer-go/internal/testdb"
)

func TestMigrateOpenAndPing(t *testing.T) {
	cfg := testStoreConfig(t)
	resetDatabase(t, cfg)

	version, err := CurrentVersion(context.Background(), cfg)
	if err != nil {
		t.Fatalf("CurrentVersion(before migrate) error = %v", err)
	}
	if version != 0 {
		t.Fatalf("CurrentVersion(before migrate) = %d, want 0", version)
	}

	if _, err := OpenConfig(cfg); err == nil || !strings.Contains(err.Error(), "not current") {
		t.Fatalf("Open(before migrate) error = %v, want schema-not-current failure", err)
	}

	version, err = Migrate(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Migrate() error = %v", err)
	}
	if want := latestMigrationVersion(); version != want {
		t.Fatalf("Migrate() version = %d, want %d", version, want)
	}

	version, err = CurrentVersion(context.Background(), cfg)
	if err != nil {
		t.Fatalf("CurrentVersion(after migrate) error = %v", err)
	}
	if want := latestMigrationVersion(); version != want {
		t.Fatalf("CurrentVersion(after migrate) = %d, want %d", version, want)
	}

	db, err := OpenConfig(cfg)
	if err != nil {
		t.Fatalf("Open(after migrate) error = %v", err)
	}
	if err := db.Ping(context.Background()); err != nil {
		t.Fatalf("Ping() error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := db.Ping(context.Background()); err == nil {
		t.Fatal("Ping(after Close) error = nil, want ping failure")
	}
}

func TestLeaseLifecycleAndFencing(t *testing.T) {
	cfg := testStoreConfig(t)
	db := openMigratedStore(t, cfg)
	defer closeStore(t, db)

	leaseA, acquired, err := db.TryAcquireLease(context.Background(), "ETHUSD", "owner-a", 250*time.Millisecond)
	if err != nil {
		t.Fatalf("TryAcquireLease(owner-a) error = %v", err)
	}
	if !acquired || leaseA.FencingToken != 1 {
		t.Fatalf("TryAcquireLease(owner-a) = (%+v, %t), want token 1 acquired", leaseA, acquired)
	}

	current, ok, err := db.CurrentLease(context.Background(), "ETHUSD")
	if err != nil {
		t.Fatalf("CurrentLease() error = %v", err)
	}
	if !ok || current.Owner != "owner-a" || current.FencingToken != 1 {
		t.Fatalf("CurrentLease() = (%+v, %t), want owner-a token 1", current, ok)
	}

	leaseB, acquired, err := db.TryAcquireLease(context.Background(), "ETHUSD", "owner-b", 250*time.Millisecond)
	if err != nil {
		t.Fatalf("TryAcquireLease(owner-b) error = %v", err)
	}
	if acquired || leaseB.Owner != "owner-a" {
		t.Fatalf("TryAcquireLease(owner-b) = (%+v, %t), want current lease and false", leaseB, acquired)
	}

	leaseA, renewed, err := db.RenewLease(context.Background(), leaseA, 250*time.Millisecond)
	if err != nil {
		t.Fatalf("RenewLease(owner-a) error = %v", err)
	}
	if !renewed {
		t.Fatal("RenewLease(owner-a) = false, want true")
	}

	if err := db.ReleaseLease(context.Background(), Lease{Market: "ETHUSD", Owner: "owner-a", FencingToken: 999}); err != nil {
		t.Fatalf("ReleaseLease(wrong token) error = %v", err)
	}
	current, ok, err = db.CurrentLease(context.Background(), "ETHUSD")
	if err != nil {
		t.Fatalf("CurrentLease(after wrong release) error = %v", err)
	}
	if !ok || current.FencingToken != leaseA.FencingToken {
		t.Fatalf("CurrentLease(after wrong release) = (%+v, %t), want unchanged live lease", current, ok)
	}

	if err := db.ReleaseLease(context.Background(), leaseA); err != nil {
		t.Fatalf("ReleaseLease(owner-a) error = %v", err)
	}
	if _, ok, err := db.CurrentLease(context.Background(), "ETHUSD"); err != nil || ok {
		t.Fatalf("CurrentLease(after release) = (%t, %v), want false nil", ok, err)
	}

	leaseB, acquired, err = db.TryAcquireLease(context.Background(), "ETHUSD", "owner-b", time.Second)
	if err != nil {
		t.Fatalf("TryAcquireLease(owner-b second) error = %v", err)
	}
	if !acquired || leaseB.FencingToken != 2 {
		t.Fatalf("TryAcquireLease(owner-b second) = (%+v, %t), want token 2 acquired", leaseB, acquired)
	}

	leaseBReacquired, acquired, err := db.TryAcquireLease(context.Background(), "ETHUSD", "owner-b", time.Second)
	if err != nil {
		t.Fatalf("TryAcquireLease(owner-b live reacquire) error = %v", err)
	}
	if !acquired || leaseBReacquired.FencingToken != leaseB.FencingToken {
		t.Fatalf("TryAcquireLease(owner-b live reacquire) = (%+v, %t), want same token %d acquired", leaseBReacquired, acquired, leaseB.FencingToken)
	}
}

func TestConcurrentLeaseAcquireAllowsSingleWinner(t *testing.T) {
	cfg := testStoreConfig(t)
	db := openMigratedStore(t, cfg)
	defer closeStore(t, db)

	type result struct {
		lease    Lease
		acquired bool
		err      error
	}

	start := make(chan struct{})
	results := make(chan result, 2)
	var wait sync.WaitGroup
	for _, owner := range []string{"owner-a", "owner-b"} {
		owner := owner
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			lease, acquired, err := db.TryAcquireLease(context.Background(), "ETHUSD", owner, time.Second)
			results <- result{lease: lease, acquired: acquired, err: err}
		}()
	}
	close(start)
	wait.Wait()
	close(results)

	acquired := make([]result, 0, 2)
	for result := range results {
		if result.err != nil {
			t.Fatalf("TryAcquireLease(concurrent) error = %v", result.err)
		}
		if result.acquired {
			acquired = append(acquired, result)
		}
	}
	if len(acquired) != 1 {
		t.Fatalf("concurrent acquired count = %d, want 1", len(acquired))
	}

	current, ok, err := db.CurrentLease(context.Background(), "ETHUSD")
	if err != nil {
		t.Fatalf("CurrentLease(concurrent) error = %v", err)
	}
	if !ok || current.Owner != acquired[0].lease.Owner {
		t.Fatalf("CurrentLease(concurrent) = (%+v, %t), want owner %q", current, ok, acquired[0].lease.Owner)
	}
}

func TestPublicationLifecycleAndRecoveryViews(t *testing.T) {
	cfg := testStoreConfig(t)
	db := openMigratedStore(t, cfg)
	defer closeStore(t, db)

	lease, acquired, err := db.TryAcquireLease(context.Background(), "ETHUSD", "owner-a", time.Second)
	if err != nil {
		t.Fatalf("TryAcquireLease() error = %v", err)
	}
	if !acquired {
		t.Fatal("TryAcquireLease() = false, want true")
	}

	first := testCycleRecord(t, "cycle-1", 1, true)
	if err := db.CommitCycleWithLease(context.Background(), lease, first); err != nil {
		t.Fatalf("CommitCycleWithLease(first) error = %v", err)
	}

	pending, err := db.PendingPublications(context.Background(), "ETHUSD")
	if err != nil {
		t.Fatalf("PendingPublications(first) error = %v", err)
	}
	if len(pending) != 1 || pending[0].Sequence != 1 {
		t.Fatalf("PendingPublications(first) = %+v, want one sequence 1", pending)
	}

	state, err := db.LatestState(context.Background(), "ETHUSD")
	if err != nil {
		t.Fatalf("LatestState() error = %v", err)
	}
	if state.LastOracle == nil || state.LastOraclePlusBasis == nil {
		t.Fatalf("LatestState() = %+v, want persisted values", state)
	}

	publishability, err := db.LatestPublishability(context.Background(), "ETHUSD")
	if err != nil {
		t.Fatalf("LatestPublishability() error = %v", err)
	}
	if publishability.PendingSequence != 1 || publishability.LastStableHash != "stable-1" {
		t.Fatalf("LatestPublishability() = %+v, want pending sequence 1 stable-1", publishability)
	}

	if _, err := db.VerifyExact(context.Background(), ExactQuery{
		Market:   "ETHUSD",
		RecordID: "cycle-1",
		Metadata: first.Metadata,
	}); err != nil {
		t.Fatalf("VerifyExact() error = %v", err)
	}

	if _, err := db.VerifyExact(context.Background(), ExactQuery{
		Market:   "ETHUSD",
		RecordID: "cycle-1",
		Metadata: Metadata{ConfigDigest: "other"},
	}); !errors.Is(err, ErrMetadataMismatch) {
		t.Fatalf("VerifyExact(metadata mismatch) error = %v, want metadata mismatch", err)
	}

	audit, err := db.Audit(context.Background(), ExactQuery{
		Market:   "ETHUSD",
		RecordID: "cycle-1",
		Metadata: Metadata{ConfigDigest: "other"},
	})
	if err != nil {
		t.Fatalf("Audit() error = %v", err)
	}
	if audit.MetadataMatch || len(audit.Mismatches) == 0 {
		t.Fatalf("Audit() = %+v, want mismatches", audit)
	}

	sentAt := time.Date(2026, 3, 27, 18, 0, 5, 0, time.UTC)
	if err := db.MarkPublicationSentWithLease(context.Background(), lease, "ETHUSD", 1, sentAt); err != nil {
		t.Fatalf("MarkPublicationSentWithLease() error = %v", err)
	}
	ackedAt := sentAt.Add(2 * time.Second)
	if err := db.AcknowledgePublicationWithLease(context.Background(), lease, "ETHUSD", 1, ackedAt); err != nil {
		t.Fatalf("AcknowledgePublicationWithLease() error = %v", err)
	}

	pending, err = db.PendingPublications(context.Background(), "ETHUSD")
	if err != nil {
		t.Fatalf("PendingPublications(after ack) error = %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("PendingPublications(after ack) = %+v, want empty", pending)
	}

	safeSequence, err := db.SafeSequence(context.Background(), "ETHUSD")
	if err != nil {
		t.Fatalf("SafeSequence() error = %v", err)
	}
	if safeSequence != 1 {
		t.Fatalf("SafeSequence() = %d, want 1", safeSequence)
	}

	second := testCycleRecord(t, "cycle-2", 2, true)
	if err := db.CommitCycleWithLease(context.Background(), lease, second); err != nil {
		t.Fatalf("CommitCycleWithLease(second) error = %v", err)
	}

	previous, ok, err := db.PreviousCycle(context.Background(), "ETHUSD", "cycle-2")
	if err != nil {
		t.Fatalf("PreviousCycle() error = %v", err)
	}
	if !ok || previous.RecordID != "cycle-1" {
		t.Fatalf("PreviousCycle() = (%+v, %t), want cycle-1 true", previous, ok)
	}

	recent, err := db.RecentCycles(context.Background(), "ETHUSD", 2)
	if err != nil {
		t.Fatalf("RecentCycles() error = %v", err)
	}
	if len(recent) != 2 || recent[0].RecordID != "cycle-2" || recent[1].RecordID != "cycle-1" {
		t.Fatalf("RecentCycles() = %+v, want [cycle-2 cycle-1]", recent)
	}

	snapshot, err := db.LatestSnapshot(context.Background(), "ETHUSD")
	if err != nil {
		t.Fatalf("LatestSnapshot() error = %v", err)
	}
	if snapshot.Record.RecordID != "cycle-2" {
		t.Fatalf("LatestSnapshot().RecordID = %q, want cycle-2", snapshot.Record.RecordID)
	}
	if snapshot.LastSuccessfulPublication == nil || snapshot.LastSuccessfulPublication.Sequence != 1 {
		t.Fatalf("LatestSnapshot().LastSuccessfulPublication = %+v, want sequence 1", snapshot.LastSuccessfulPublication)
	}
}

func TestCycleOrderingUsesComputedTimeAndMarketScopedIdentity(t *testing.T) {
	cfg := testStoreConfig(t)
	db := openMigratedStore(t, cfg)
	defer closeStore(t, db)

	older := testCycleRecord(t, "shared-record", 1, false)
	older = rebindCycleRecord(older, "ETHUSD", time.Date(2026, 3, 27, 18, 0, 0, 0, time.UTC))
	if err := db.CommitCycle(context.Background(), older); err != nil {
		t.Fatalf("CommitCycle(older) error = %v", err)
	}

	otherMarket := testCycleRecord(t, "shared-record", 1, false)
	otherMarket = rebindCycleRecord(otherMarket, "BTCUSD", time.Date(2026, 3, 27, 18, 1, 0, 0, time.UTC))
	if err := db.CommitCycle(context.Background(), otherMarket); err != nil {
		t.Fatalf("CommitCycle(other market) error = %v", err)
	}

	latest := testCycleRecord(t, "cycle-a", 1, false)
	latest = rebindCycleRecord(latest, "ETHUSD", time.Date(2026, 3, 27, 18, 5, 0, 0, time.UTC))
	if err := db.CommitCycle(context.Background(), latest); err != nil {
		t.Fatalf("CommitCycle(latest) error = %v", err)
	}

	previous, ok, err := db.PreviousCycle(context.Background(), "ETHUSD", latest.RecordID)
	if err != nil {
		t.Fatalf("PreviousCycle() error = %v", err)
	}
	if !ok || previous.RecordID != older.RecordID {
		t.Fatalf("PreviousCycle() = (%+v, %t), want %q true", previous, ok, older.RecordID)
	}

	recent, err := db.RecentCycles(context.Background(), "ETHUSD", 2)
	if err != nil {
		t.Fatalf("RecentCycles() error = %v", err)
	}
	if len(recent) != 2 || recent[0].RecordID != latest.RecordID || recent[1].RecordID != older.RecordID {
		t.Fatalf("RecentCycles() = %+v, want [%q %q]", recent, latest.RecordID, older.RecordID)
	}

	snapshot, err := db.LatestSnapshot(context.Background(), "ETHUSD")
	if err != nil {
		t.Fatalf("LatestSnapshot() error = %v", err)
	}
	if snapshot.Record.RecordID != latest.RecordID {
		t.Fatalf("LatestSnapshot().RecordID = %q, want %q", snapshot.Record.RecordID, latest.RecordID)
	}

	btcRecent, err := db.RecentCycles(context.Background(), "BTCUSD", 1)
	if err != nil {
		t.Fatalf("RecentCycles(BTCUSD) error = %v", err)
	}
	if len(btcRecent) != 1 || btcRecent[0].RecordID != otherMarket.RecordID {
		t.Fatalf("RecentCycles(BTCUSD) = %+v, want [%q]", btcRecent, otherMarket.RecordID)
	}
}

func TestLeaseBoundWritesRejectStaleOwners(t *testing.T) {
	cfg := testStoreConfig(t)
	db := openMigratedStore(t, cfg)
	defer closeStore(t, db)

	leaseA, acquired, err := db.TryAcquireLease(context.Background(), "ETHUSD", "owner-a", time.Second)
	if err != nil {
		t.Fatalf("TryAcquireLease(owner-a) error = %v", err)
	}
	if !acquired {
		t.Fatal("TryAcquireLease(owner-a) = false, want true")
	}
	if err := db.ReleaseLease(context.Background(), leaseA); err != nil {
		t.Fatalf("ReleaseLease(owner-a) error = %v", err)
	}
	if _, acquired, err := db.TryAcquireLease(context.Background(), "ETHUSD", "owner-b", time.Second); err != nil || !acquired {
		t.Fatalf("TryAcquireLease(owner-b) = (%t, %v), want true nil", acquired, err)
	}

	record := testCycleRecord(t, "cycle-1", 1, true)
	if err := db.CommitCycleWithLease(context.Background(), leaseA, record); !errors.Is(err, errLeaseLost) {
		t.Fatalf("CommitCycleWithLease(stale) error = %v, want errLeaseLost", err)
	}
	if err := db.SavePublishabilityWithLease(context.Background(), leaseA, record.Publishability); !errors.Is(err, errLeaseLost) {
		t.Fatalf("SavePublishabilityWithLease(stale) error = %v, want errLeaseLost", err)
	}
	if err := db.MarkPublicationSentWithLease(context.Background(), leaseA, "ETHUSD", 1, time.Now().UTC()); !errors.Is(err, errLeaseLost) {
		t.Fatalf("MarkPublicationSentWithLease(stale) error = %v, want errLeaseLost", err)
	}
	if err := db.AcknowledgePublicationWithLease(context.Background(), leaseA, "ETHUSD", 1, time.Now().UTC()); !errors.Is(err, errLeaseLost) {
		t.Fatalf("AcknowledgePublicationWithLease(stale) error = %v, want errLeaseLost", err)
	}
}

func TestHelpers(t *testing.T) {
	names, err := migrationNames()
	if err != nil {
		t.Fatalf("migrationNames() error = %v", err)
	}
	if len(names) == 0 {
		t.Fatal("migrationNames() returned no migrations")
	}
	lastVersion, err := migrationVersion(names[len(names)-1])
	if err != nil {
		t.Fatalf("migrationVersion(last) error = %v", err)
	}
	if got := latestMigrationVersion(); got != lastVersion {
		t.Fatalf("latestMigrationVersion() = %d, want %d", got, lastVersion)
	}

	actual := Metadata{
		ConfigDigest:            "config-a",
		PricingAlgorithmVersion: "1",
		SchemaVersion:           "1",
		EnvelopeVersion:         "1",
	}
	expected := Metadata{
		ConfigDigest:            "config-b",
		PricingAlgorithmVersion: "2",
		SchemaVersion:           "2",
		EnvelopeVersion:         "2",
	}
	if got := metadataMismatches(actual, expected); len(got) != 4 {
		t.Fatalf("metadataMismatches() = %v, want all fields", got)
	}
	if raw, err := marshalJSON(map[string]string{"ok": "true"}); err != nil || !strings.Contains(string(raw), `"ok":"true"`) {
		t.Fatalf("marshalJSON(success) = (%q, %v), want encoded map", raw, err)
	}
	if _, err := marshalJSON(make(chan int)); err == nil {
		t.Fatal("marshalJSON(channel) error = nil, want failure")
	}
}

func TestOpenPoolUsesConfiguredConnectionBounds(t *testing.T) {
	cfg := testStoreConfig(t)
	cfg.MaxOpenConns = 3
	cfg.MinOpenConns = 1

	pool, err := openPool(context.Background(), cfg)
	if err != nil {
		t.Fatalf("openPool() error = %v", err)
	}
	defer pool.Close()

	if got, want := pool.Config().MaxConns, int32(3); got != want {
		t.Fatalf("pool.Config().MaxConns = %d, want %d", got, want)
	}
	if got, want := pool.Config().MinConns, int32(1); got != want {
		t.Fatalf("pool.Config().MinConns = %d, want %d", got, want)
	}
}

func TestOpenRequiresExplicitMigrationForUnfencedLifecycle(t *testing.T) {
	cfg := testStoreConfig(t)
	resetDatabase(t, cfg)

	if _, err := Open(cfg.DSN); err == nil || !strings.Contains(err.Error(), "not current") {
		t.Fatalf("Open(before migrate) error = %v, want schema-not-current failure", err)
	}
	if _, err := Migrate(context.Background(), cfg); err != nil {
		t.Fatalf("Migrate() error = %v", err)
	}

	db, err := Open(cfg.DSN)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer closeStore(t, db)

	if version, err := CurrentVersion(context.Background(), cfg); err != nil {
		t.Fatalf("CurrentVersion(after Open) error = %v", err)
	} else if want := latestMigrationVersion(); version != want {
		t.Fatalf("CurrentVersion(after Open) = %d, want %d", version, want)
	}

	record := testCycleRecord(t, "cycle-open-1", 1, true)
	if err := db.CommitCycle(context.Background(), record); err != nil {
		t.Fatalf("CommitCycle() error = %v", err)
	}

	sentAt := record.PreparedPublication.PreparedAt.Add(time.Second)
	if err := db.MarkPublicationSent(context.Background(), record.Market, record.PreparedPublication.Sequence, sentAt); err != nil {
		t.Fatalf("MarkPublicationSent() error = %v", err)
	}
	ackedAt := sentAt.Add(time.Second)
	if err := db.AcknowledgePublication(context.Background(), record.Market, record.PreparedPublication.Sequence, ackedAt); err != nil {
		t.Fatalf("AcknowledgePublication() error = %v", err)
	}

	publishability := record.Publishability
	publishability.Status = status.StatusUnavailable
	publishability.Reason = status.ReasonSinkUnavailable
	publishability.PendingSequence = 0
	publishability.LastSequence = record.PreparedPublication.Sequence
	publishability.LastStableHash = "stable-after-ack"
	publishability.UpdatedAt = ackedAt
	if err := db.SavePublishability(context.Background(), publishability); err != nil {
		t.Fatalf("SavePublishability() error = %v", err)
	}

	if got, err := db.SafeSequence(context.Background(), record.Market); err != nil {
		t.Fatalf("SafeSequence() error = %v", err)
	} else if got != record.PreparedPublication.Sequence {
		t.Fatalf("SafeSequence() = %d, want %d", got, record.PreparedPublication.Sequence)
	}

	if got, err := db.LatestState(context.Background(), record.Market); err != nil {
		t.Fatalf("LatestState() error = %v", err)
	} else if got.LastOracle == nil || got.LastOraclePlusBasis == nil {
		t.Fatalf("LatestState() = %+v, want persisted state", got)
	}

	if got, err := db.LatestPublishability(context.Background(), record.Market); err != nil {
		t.Fatalf("LatestPublishability() error = %v", err)
	} else if got.Status != status.StatusUnavailable || got.Reason != status.ReasonSinkUnavailable || got.LastStableHash != "stable-after-ack" {
		t.Fatalf("LatestPublishability() = %+v, want saved publishability", got)
	}

	snapshot, err := db.LatestSnapshot(context.Background(), record.Market)
	if err != nil {
		t.Fatalf("LatestSnapshot() error = %v", err)
	}
	if snapshot.LastSuccessfulPublication == nil || snapshot.LastSuccessfulPublication.Sequence != record.PreparedPublication.Sequence {
		t.Fatalf("LatestSnapshot().LastSuccessfulPublication = %+v, want sequence %d", snapshot.LastSuccessfulPublication, record.PreparedPublication.Sequence)
	}
}

func TestMigrateIsIdempotentAndHelpersRejectBadConfig(t *testing.T) {
	cfg := testStoreConfig(t)
	resetDatabase(t, cfg)

	if version, err := Migrate(context.Background(), cfg); err != nil {
		t.Fatalf("Migrate(first) error = %v", err)
	} else if want := latestMigrationVersion(); version != want {
		t.Fatalf("Migrate(first) = %d, want %d", version, want)
	}

	if version, err := Migrate(context.Background(), cfg); err != nil {
		t.Fatalf("Migrate(second) error = %v", err)
	} else if want := latestMigrationVersion(); version != want {
		t.Fatalf("Migrate(second) = %d, want %d", version, want)
	}

	badCfg := cfg
	badCfg.DSN = "://bad-dsn"
	if _, err := Migrate(context.Background(), badCfg); err == nil || !strings.Contains(err.Error(), "parse dsn") {
		t.Fatalf("Migrate(bad dsn) error = %v, want parse dsn failure", err)
	}
	if _, err := CurrentVersion(context.Background(), badCfg); err == nil || !strings.Contains(err.Error(), "parse dsn") {
		t.Fatalf("CurrentVersion(bad dsn) error = %v, want parse dsn failure", err)
	}
	if _, err := OpenConfig(badCfg); err == nil || !strings.Contains(err.Error(), "parse dsn") {
		t.Fatalf("OpenConfig(bad dsn) error = %v, want parse dsn failure", err)
	}
}

func TestCommitCycleAndCheckpointsRejectInvalidState(t *testing.T) {
	cfg := testStoreConfig(t)
	db := openMigratedStore(t, cfg)
	defer closeStore(t, db)

	first := testCycleRecord(t, "cycle-invalid-1", 1, true)
	if err := db.CommitCycle(context.Background(), first); err != nil {
		t.Fatalf("CommitCycle(first) error = %v", err)
	}

	second := testCycleRecord(t, "cycle-invalid-2", 2, true)
	if err := db.CommitCycle(context.Background(), second); err == nil || !strings.Contains(err.Error(), "already has unresolved publication") {
		t.Fatalf("CommitCycle(second pending) error = %v, want unresolved publication failure", err)
	}

	sentAt := first.PreparedPublication.PreparedAt.Add(time.Second)
	if err := db.MarkPublicationSent(context.Background(), first.Market, first.PreparedPublication.Sequence, sentAt); err != nil {
		t.Fatalf("MarkPublicationSent() error = %v", err)
	}
	if err := db.AcknowledgePublication(context.Background(), first.Market, first.PreparedPublication.Sequence, sentAt.Add(time.Second)); err != nil {
		t.Fatalf("AcknowledgePublication() error = %v", err)
	}

	outOfSequence := testCycleRecord(t, "cycle-invalid-3", 5, true)
	if err := db.CommitCycle(context.Background(), outOfSequence); err == nil || !strings.Contains(err.Error(), "does not match next safe sequence") {
		t.Fatalf("CommitCycle(out of sequence) error = %v, want sequence failure", err)
	}

	withoutPublication := testCycleRecord(t, "cycle-invalid-4", 2, false)
	if err := db.CommitCycle(context.Background(), withoutPublication); err != nil {
		t.Fatalf("CommitCycle(without publication) error = %v", err)
	}

	if err := db.MarkPublicationSent(context.Background(), "ETHUSD", 999, time.Now().UTC()); err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("MarkPublicationSent(missing) error = %v, want not found", err)
	}
	if err := db.AcknowledgePublication(context.Background(), "ETHUSD", 999, time.Now().UTC()); err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("AcknowledgePublication(missing) error = %v, want not found", err)
	}
}

func TestSnapshotAndLookupHandleMissingAndCorruptData(t *testing.T) {
	cfg := testStoreConfig(t)
	db := openMigratedStore(t, cfg)
	defer closeStore(t, db)

	if _, err := db.LatestSnapshot(context.Background(), "ETHUSD"); err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("LatestSnapshot(empty) error = %v, want not found", err)
	}
	if _, err := db.VerifyExact(context.Background(), ExactQuery{Market: "ETHUSD", RecordID: "missing"}); err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("VerifyExact(missing) error = %v, want not found", err)
	}

	if _, err := db.pool.Exec(context.Background(), `
		INSERT INTO cycle_records (market, record_id, computed_at, payload)
		VALUES ($1, $2, $3, $4)
	`, "ETHUSD", "broken-cycle", time.Date(2026, 3, 27, 18, 0, 0, 0, time.UTC), []byte(`{"record_id":1}`)); err != nil {
		t.Fatalf("insert broken cycle record error = %v", err)
	}
	if _, err := db.loadCycleRecord(context.Background(), "ETHUSD", "broken-cycle"); err == nil || !strings.Contains(err.Error(), "decode cycle record") {
		t.Fatalf("loadCycleRecord(broken) error = %v, want decode failure", err)
	}
	if _, err := db.latestCycle(context.Background(), "ETHUSD"); err == nil || !strings.Contains(err.Error(), "decode latest cycle") {
		t.Fatalf("latestCycle(broken) error = %v, want decode failure", err)
	}

	if _, err := db.pool.Exec(context.Background(), `
		INSERT INTO market_runtime (market, latest_state, latest_publishability, updated_at)
		VALUES ($1, $2, $3, $4)
	`, "BTCUSD", []byte(`{"LastOracle":"bad"}`), []byte(`{"market":"BTCUSD"}`), time.Now().UTC()); err != nil {
		t.Fatalf("insert broken runtime row error = %v", err)
	}
	if _, err := db.LatestState(context.Background(), "BTCUSD"); err == nil || !strings.Contains(err.Error(), "decode latest state") {
		t.Fatalf("LatestState(broken) error = %v, want decode failure", err)
	}
	if _, err := db.LatestPublishability(context.Background(), "BTCUSD"); err == nil || !strings.Contains(err.Error(), "decode latest state") {
		t.Fatalf("LatestPublishability(broken state) error = %v, want decode failure", err)
	}

	if _, err := db.pool.Exec(context.Background(), `
		UPDATE market_runtime
		SET latest_state = $2, latest_publishability = $3
		WHERE market = $1
	`, "BTCUSD", []byte(`{}`), []byte(`{"pending_sequence":"bad"}`)); err != nil {
		t.Fatalf("update broken runtime publishability error = %v", err)
	}
	if _, err := db.LatestPublishability(context.Background(), "BTCUSD"); err == nil || !strings.Contains(err.Error(), "decode latest publishability") {
		t.Fatalf("LatestPublishability(broken publishability) error = %v, want decode failure", err)
	}
}

func TestOpenPoolAndOpenFailWhenDatabaseIsUnavailable(t *testing.T) {
	cfg := testStoreConfig(t)
	cfg.DSN = "postgres://postgres:postgres@127.0.0.1:1/market_relayer_test?sslmode=disable"
	cfg.ConnectTimeout = 200 * time.Millisecond

	if _, err := OpenConfig(cfg); err == nil || !strings.Contains(err.Error(), "ping") {
		t.Fatalf("OpenConfig(unavailable db) error = %v, want ping failure", err)
	}
	if _, err := CurrentVersion(context.Background(), cfg); err == nil || !strings.Contains(err.Error(), "open connection") {
		t.Fatalf("CurrentVersion(unavailable db) error = %v, want connect failure", err)
	}
	if _, err := Migrate(context.Background(), cfg); err == nil || !strings.Contains(err.Error(), "open connection") {
		t.Fatalf("Migrate(unavailable db) error = %v, want connect failure", err)
	}
	if _, err := Open(cfg.DSN); err == nil || !strings.Contains(err.Error(), "ping") {
		t.Fatalf("Open(unavailable db) error = %v, want ping failure", err)
	}
}

func TestMigrationHelpersRejectBrokenSchemaMetadata(t *testing.T) {
	cfg := testStoreConfig(t)
	resetDatabase(t, cfg)

	pool, err := openPool(context.Background(), cfg)
	if err != nil {
		t.Fatalf("openPool() error = %v", err)
	}
	defer pool.Close()

	ctx := context.Background()
	if _, err := pool.Exec(ctx, `
		CREATE TABLE schema_version (
			version TEXT NOT NULL
		)
	`); err != nil {
		t.Fatalf("CREATE TABLE schema_version error = %v", err)
	}

	if _, err := currentMigrationVersion(ctx, cfg); err == nil || !strings.Contains(err.Error(), "load migration version") {
		t.Fatalf("currentMigrationVersion(broken schema) error = %v, want load migration version failure", err)
	}

	if _, err := pool.Exec(ctx, `DROP TABLE schema_version`); err != nil {
		t.Fatalf("DROP TABLE schema_version error = %v", err)
	}

	if version, err := currentMigrationVersion(ctx, cfg); err != nil {
		t.Fatalf("currentMigrationVersion(recreated) error = %v", err)
	} else if version != 0 {
		t.Fatalf("currentMigrationVersion(recreated) = %d, want 0", version)
	}

	if _, err := migrationVersion("bad.sql"); err == nil || !strings.Contains(err.Error(), "missing separator") {
		t.Fatalf("migrationVersion(bad.sql) error = %v, want parse failure", err)
	}
}

func TestLeaseHelpersHandleExpiryAndMissingLease(t *testing.T) {
	cfg := testStoreConfig(t)
	db := openMigratedStore(t, cfg)
	defer closeStore(t, db)

	lease, acquired, err := db.TryAcquireLease(context.Background(), "ETHUSD", "owner-a", time.Hour)
	if err != nil {
		t.Fatalf("TryAcquireLease() error = %v", err)
	}
	if !acquired {
		t.Fatal("TryAcquireLease() = false, want true")
	}

	if _, err := db.pool.Exec(context.Background(), `
		UPDATE market_leases
		SET expires_at = NOW() - INTERVAL '1 second'
		WHERE market = $1
	`, "ETHUSD"); err != nil {
		t.Fatalf("expire lease error = %v", err)
	}

	if current, ok, err := db.CurrentLease(context.Background(), "ETHUSD"); err != nil {
		t.Fatalf("CurrentLease(expired) error = %v", err)
	} else if ok || current != (Lease{}) {
		t.Fatalf("CurrentLease(expired) = (%+v, %t), want zero false", current, ok)
	}

	if renewedLease, renewed, err := db.RenewLease(context.Background(), lease, time.Hour); err != nil {
		t.Fatalf("RenewLease(expired) error = %v", err)
	} else if renewed || renewedLease != (Lease{}) {
		t.Fatalf("RenewLease(expired) = (%+v, %t), want zero false", renewedLease, renewed)
	}

	reacquiredLease, reacquired, err := db.TryAcquireLease(context.Background(), "ETHUSD", "owner-a", time.Hour)
	if err != nil {
		t.Fatalf("TryAcquireLease(expired same owner) error = %v", err)
	}
	if !reacquired || reacquiredLease.FencingToken != lease.FencingToken+1 {
		t.Fatalf("TryAcquireLease(expired same owner) = (%+v, %t), want token %d acquired", reacquiredLease, reacquired, lease.FencingToken+1)
	}

	if err := db.ReleaseLease(context.Background(), reacquiredLease); err != nil {
		t.Fatalf("ReleaseLease(reacquired) error = %v", err)
	}
	releasedLease, reacquired, err := db.TryAcquireLease(context.Background(), "ETHUSD", "owner-a", time.Hour)
	if err != nil {
		t.Fatalf("TryAcquireLease(released same owner) error = %v", err)
	}
	if !reacquired || releasedLease.FencingToken != reacquiredLease.FencingToken+1 {
		t.Fatalf("TryAcquireLease(released same owner) = (%+v, %t), want token %d acquired", releasedLease, reacquired, reacquiredLease.FencingToken+1)
	}

	record := testCycleRecord(t, "cycle-expired-1", 1, true)
	if err := db.CommitCycleWithLease(context.Background(), lease, record); !errors.Is(err, errLeaseLost) {
		t.Fatalf("CommitCycleWithLease(expired) error = %v, want errLeaseLost", err)
	}

	missingLease := Lease{Market: "BTCUSD", Owner: "nobody", FencingToken: 7}
	publishability := PublishabilityState{
		Market:         "BTCUSD",
		Status:         status.StatusAvailable,
		Reason:         status.ReasonNone,
		UpdatedAt:      time.Now().UTC(),
		LastStableHash: "missing-lease",
	}
	if err := db.SavePublishabilityWithLease(context.Background(), missingLease, publishability); !errors.Is(err, errLeaseLost) {
		t.Fatalf("SavePublishabilityWithLease(missing) error = %v, want errLeaseLost", err)
	}
}

func TestStoreOperationsRespectCanceledContext(t *testing.T) {
	cfg := testStoreConfig(t)
	db := openMigratedStore(t, cfg)
	defer closeStore(t, db)

	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := OpenConfigContext(canceledCtx, cfg); !errors.Is(err, context.Canceled) {
		t.Fatalf("OpenConfigContext(canceled) error = %v, want context canceled", err)
	}
	if _, err := db.PendingPublications(canceledCtx, "ETHUSD"); !errors.Is(err, context.Canceled) {
		t.Fatalf("PendingPublications(canceled) error = %v, want context canceled", err)
	}
	if _, _, err := db.TryAcquireLease(canceledCtx, "ETHUSD", "owner-a", time.Second); !errors.Is(err, context.Canceled) {
		t.Fatalf("TryAcquireLease(canceled) error = %v, want context canceled", err)
	}
	if err := db.CommitCycle(canceledCtx, testCycleRecord(t, "cycle-canceled", 1, true)); !errors.Is(err, context.Canceled) {
		t.Fatalf("CommitCycle(canceled) error = %v, want context canceled", err)
	}
}

func TestHistoryAndSnapshotBranchesWithoutSuccessfulPublication(t *testing.T) {
	cfg := testStoreConfig(t)
	db := openMigratedStore(t, cfg)
	defer closeStore(t, db)

	if previous, ok, err := db.PreviousCycle(context.Background(), "ETHUSD", "cycle-1"); err != nil {
		t.Fatalf("PreviousCycle(empty) error = %v", err)
	} else if ok || previous.RecordID != "" || previous.Market != "" {
		t.Fatalf("PreviousCycle(empty) = (%+v, %t), want zero-like false", previous, ok)
	}

	if recent, err := db.RecentCycles(context.Background(), "ETHUSD", 5); err != nil {
		t.Fatalf("RecentCycles(empty) error = %v", err)
	} else if len(recent) != 0 {
		t.Fatalf("RecentCycles(empty) = %+v, want empty", recent)
	}

	record := testCycleRecord(t, "cycle-no-ack", 1, false)
	if err := db.CommitCycle(context.Background(), record); err != nil {
		t.Fatalf("CommitCycle(no publication) error = %v", err)
	}

	snapshot, err := db.LatestSnapshot(context.Background(), "ETHUSD")
	if err != nil {
		t.Fatalf("LatestSnapshot(no ack) error = %v", err)
	}
	if snapshot.LastSuccessfulPublication != nil {
		t.Fatalf("LatestSnapshot(no ack).LastSuccessfulPublication = %+v, want nil", snapshot.LastSuccessfulPublication)
	}
	if len(snapshot.RecentCycles) != 1 || snapshot.RecentCycles[0].RecordID != record.RecordID {
		t.Fatalf("LatestSnapshot(no ack).RecentCycles = %+v, want [%s]", snapshot.RecentCycles, record.RecordID)
	}
	if got, err := db.SafeSequence(context.Background(), "UNSEEN"); err != nil {
		t.Fatalf("SafeSequence(unseen) error = %v", err)
	} else if got != 0 {
		t.Fatalf("SafeSequence(unseen) = %d, want 0", got)
	}

	if _, err := db.Audit(context.Background(), ExactQuery{Market: "ETHUSD", RecordID: "missing"}); err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("Audit(missing) error = %v, want not found", err)
	}
}

func TestCommitCycleFailsWhenRuntimeRowIsCorrupt(t *testing.T) {
	cfg := testStoreConfig(t)
	db := openMigratedStore(t, cfg)
	defer closeStore(t, db)

	record := testCycleRecord(t, "cycle-corrupt-runtime", 1, false)
	if _, err := db.pool.Exec(context.Background(), `
		INSERT INTO market_runtime (market, latest_state, latest_publishability, updated_at)
		VALUES ($1, $2, $3, $4)
	`, "ETHUSD", []byte(`{"LastOracle":"bad"}`), []byte(`{"market":"ETHUSD"}`), time.Now().UTC()); err != nil {
		t.Fatalf("insert corrupt runtime state error = %v", err)
	}
	if err := db.CommitCycle(context.Background(), record); err == nil || !strings.Contains(err.Error(), "decode runtime state") {
		t.Fatalf("CommitCycle(corrupt state) error = %v, want decode runtime state failure", err)
	}

	if _, err := db.pool.Exec(context.Background(), `
		UPDATE market_runtime
		SET latest_state = $2, latest_publishability = $3
		WHERE market = $1
	`, "ETHUSD", []byte(`{}`), []byte(`{"pending_sequence":"bad"}`)); err != nil {
		t.Fatalf("update corrupt runtime publishability error = %v", err)
	}
	if err := db.CommitCycle(context.Background(), record); err == nil || !strings.Contains(err.Error(), "decode runtime publishability") {
		t.Fatalf("CommitCycle(corrupt publishability) error = %v, want decode runtime publishability failure", err)
	}
}

func TestPublicationHelpersRejectDuplicateInsertAndCorruptHistoryRows(t *testing.T) {
	cfg := testStoreConfig(t)
	db := openMigratedStore(t, cfg)
	defer closeStore(t, db)

	record := testCycleRecord(t, "cycle-dup-insert", 1, true)
	if err := db.withTx(context.Background(), func(ctx context.Context, tx pgx.Tx) error {
		return insertPublication(ctx, tx, *record.PreparedPublication)
	}); err != nil {
		t.Fatalf("insertPublication(first) error = %v", err)
	}
	if err := db.withTx(context.Background(), func(ctx context.Context, tx pgx.Tx) error {
		return insertPublication(ctx, tx, *record.PreparedPublication)
	}); err == nil || !strings.Contains(err.Error(), "insert publication") {
		t.Fatalf("insertPublication(duplicate) error = %v, want insert failure", err)
	}

	if _, err := db.pool.Exec(context.Background(), `
		INSERT INTO cycle_records (market, record_id, computed_at, payload)
		VALUES ($1, $2, $3, $4)
	`, "BROKEN", "cycle-broken-prev", time.Date(2026, 3, 27, 18, 0, 0, 0, time.UTC), []byte(`{"record_id":1}`)); err != nil {
		t.Fatalf("insert broken previous-cycle row error = %v", err)
	}
	current := testCycleRecord(t, "cycle-current", 1, false)
	current = rebindCycleRecord(current, "BROKEN", time.Date(2026, 3, 27, 18, 1, 0, 0, time.UTC))
	if err := db.CommitCycle(context.Background(), current); err != nil {
		t.Fatalf("CommitCycle(current broken test) error = %v", err)
	}
	if _, ok, err := db.PreviousCycle(context.Background(), "BROKEN", current.RecordID); err == nil || !strings.Contains(err.Error(), "decode previous cycle") {
		t.Fatalf("PreviousCycle(corrupt row) = (%t, %v), want decode failure", ok, err)
	}
	if _, err := db.RecentCycles(context.Background(), "BROKEN", 5); err == nil || !strings.Contains(err.Error(), "decode recent cycle") {
		t.Fatalf("RecentCycles(corrupt row) error = %v, want decode failure", err)
	}
}

func openMigratedStore(t *testing.T, cfg config.StoreConfig) *DB {
	t.Helper()
	resetDatabase(t, cfg)
	if _, err := Migrate(context.Background(), cfg); err != nil {
		t.Fatalf("Migrate() error = %v", err)
	}
	db, err := OpenConfig(cfg)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	return db
}

func closeStore(t *testing.T, db *DB) {
	t.Helper()
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func testStoreConfig(t *testing.T) config.StoreConfig {
	t.Helper()

	cfg := config.StoreConfig{
		DSN:                testStoreDSN(),
		MaxOpenConns:       4,
		MinOpenConns:       0,
		ConnMaxLifetime:    time.Minute,
		ConnectTimeout:     5 * time.Second,
		LockTimeout:        5 * time.Second,
		LeaseTTL:           time.Second,
		LeaseRenewInterval: 200 * time.Millisecond,
	}

	ctx, cancel := context.WithTimeout(context.Background(), cfg.ConnectTimeout)
	defer cancel()
	conn, err := pgx.Connect(ctx, cfg.DSN)
	if err != nil {
		if os.Getenv("MARKET_RELAYER_TEST_POSTGRES_DSN") == "" {
			if testdb.SkipPostgresTestsRequested() {
				t.Skipf("postgres integration unavailable: %v", err)
			}
			t.Fatalf(
				"postgres integration unavailable: %v; start postgres or set %s=1 to skip postgres-backed tests",
				err,
				testdb.SkipPostgresTestsEnv,
			)
		}
		t.Fatalf("pgx.Connect() error = %v", err)
	}
	if err := conn.Close(ctx); err != nil {
		t.Fatalf("conn.Close() error = %v", err)
	}
	return cfg
}

func resetDatabase(t *testing.T, cfg config.StoreConfig) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, cfg.DSN)
	if err != nil {
		t.Fatalf("pgx.Connect(reset) error = %v", err)
	}
	defer func() {
		if err := conn.Close(ctx); err != nil {
			t.Fatalf("conn.Close(reset) error = %v", err)
		}
	}()

	if _, err := conn.Exec(ctx, `DROP SCHEMA IF EXISTS public CASCADE`); err != nil {
		t.Fatalf("DROP SCHEMA public CASCADE error = %v", err)
	}
	if _, err := conn.Exec(ctx, `CREATE SCHEMA public`); err != nil {
		t.Fatalf("CREATE SCHEMA public error = %v", err)
	}
}

func testStoreDSN() string {
	if dsn := os.Getenv("MARKET_RELAYER_TEST_POSTGRES_DSN"); dsn != "" {
		return dsn
	}
	return "postgres://postgres:postgres@127.0.0.1:55432/market_relayer_test?sslmode=disable"
}

func TestCycleRecordRawBookRoundTrips(t *testing.T) {
	cfg := testStoreConfig(t)
	resetDatabase(t, cfg)

	if _, err := Migrate(context.Background(), cfg); err != nil {
		t.Fatalf("Migrate() error = %v", err)
	}

	db, err := OpenConfig(cfg)
	if err != nil {
		t.Fatalf("OpenConfig() error = %v", err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Fatalf("db.Close() error = %v", err)
		}
	})

	record := testCycleRecord(t, "cycle-book-1", 1, false)
	ts := record.Computed.ComputedAt.Add(-time.Second)
	record.RawBook = &feeds.BookSnapshot{
		Source:             "hyperliquid-eth",
		Symbol:             record.Market,
		Market:             "ETH",
		Bids:               []feeds.BookLevel{{Price: mustDecimal(t, "1981.85"), Size: mustDecimal(t, "1.5")}},
		Asks:               []feeds.BookLevel{{Price: mustDecimal(t, "1981.95"), Size: mustDecimal(t, "2.0")}},
		ReceivedAt:         record.Computed.ComputedAt,
		SourceTS:           &ts,
		FreshnessBasisKind: feeds.FreshnessBasisSourceTimestamp,
		FreshnessBasisAt:   ts,
	}

	if err := db.CommitCycle(context.Background(), record); err != nil {
		t.Fatalf("CommitCycle() error = %v", err)
	}

	got, err := db.VerifyExact(context.Background(), ExactQuery{
		Market:   record.Market,
		RecordID: record.RecordID,
		Metadata: record.Metadata,
	})
	if err != nil {
		t.Fatalf("VerifyExact() error = %v", err)
	}
	if got.RawBook == nil || got.RawBook.Market != "ETH" {
		t.Fatalf("VerifyExact().RawBook = %+v, want market ETH", got.RawBook)
	}

	snapshot, err := db.LatestSnapshot(context.Background(), record.Market)
	if err != nil {
		t.Fatalf("LatestSnapshot() error = %v", err)
	}
	if snapshot.Record.RawBook == nil || len(snapshot.Record.RawBook.Bids) != 1 || len(snapshot.Record.RawBook.Asks) != 1 {
		t.Fatalf("LatestSnapshot().Record.RawBook = %+v, want persisted top levels", snapshot.Record.RawBook)
	}
}

func testCycleRecord(t *testing.T, recordID string, sequence uint64, includePublication bool) CycleRecord {
	t.Helper()

	value := mustDecimal(t, "1981.90")
	computedAt := time.Date(2026, 3, 27, 18, 0, 0, 0, time.UTC)
	result := pricing.Result{
		Market:        "ETHUSD",
		PublishScale:  2,
		ComputedAt:    computedAt,
		SessionState:  status.SessionStateOpen,
		SessionReason: status.ReasonNone,
		External: pricing.Price{
			Status: status.StatusAvailable,
			Reason: status.ReasonNone,
			Value:  &value,
			Scale:  2,
		},
		Oracle: pricing.Price{
			Status: status.StatusAvailable,
			Reason: status.ReasonNone,
			Value:  &value,
			Scale:  2,
		},
		OraclePlusBasis: pricing.Price{
			Status: status.StatusAvailable,
			Reason: status.ReasonNone,
			Value:  &value,
			Scale:  2,
		},
		State: pricing.State{
			LastOracle:                       &value,
			LastOraclePlusBasis:              &value,
			LastOracleAt:                     computedAt,
			LastOraclePlusBasisAt:            computedAt,
			OracleFallbackExpiresAt:          computedAt.Add(time.Minute),
			OraclePlusBasisFallbackExpiresAt: computedAt.Add(time.Minute),
		},
	}

	record := CycleRecord{
		RecordID: recordID,
		Market:   "ETHUSD",
		Computed: result,
		Metadata: Metadata{
			ConfigDigest:            "config-digest",
			PricingAlgorithmVersion: "1",
			SchemaVersion:           SchemaVersion,
			EnvelopeVersion:         "1",
		},
		Publishability: PublishabilityState{
			Market:             "ETHUSD",
			Status:             status.StatusAvailable,
			Reason:             status.ReasonNone,
			UpdatedAt:          computedAt,
			LastStableHash:     "stable-1",
			LastSequence:       sequence - 1,
			LastIdempotencyKey: "idempotency-0",
			LastPayloadHash:    "payload-0",
			PendingSequence:    0,
		},
	}

	if includePublication {
		record.Publishability.PendingSequence = sequence
		record.Publishability.LastSequence = sequence - 1
		record.PreparedPublication = &PreparedPublication{
			Market:          "ETHUSD",
			Sequence:        sequence,
			State:           PublicationStatePrepared,
			PreparedAt:      computedAt,
			IdempotencyKey:  "idempotency-" + recordID,
			PayloadHash:     "payload-" + recordID,
			EnvelopeVersion: "1",
			Envelope:        []byte(`{"market":"ETHUSD","sequence":1}`),
		}
	}

	return record
}

func mustDecimal(t *testing.T, input string) fixedpoint.Value {
	t.Helper()
	value, err := fixedpoint.ParseDecimal(input)
	if err != nil {
		t.Fatalf("ParseDecimal(%q) error = %v", input, err)
	}
	return value
}

func rebindCycleRecord(record CycleRecord, market string, computedAt time.Time) CycleRecord {
	record.Market = market
	record.Computed.Market = market
	record.Computed.ComputedAt = computedAt.UTC()
	record.Computed.State.LastOracleAt = record.Computed.ComputedAt
	record.Computed.State.LastOraclePlusBasisAt = record.Computed.ComputedAt
	record.Computed.State.OracleFallbackExpiresAt = record.Computed.ComputedAt.Add(time.Minute)
	record.Computed.State.OraclePlusBasisFallbackExpiresAt = record.Computed.ComputedAt.Add(time.Minute)
	record.Publishability.Market = market
	record.Publishability.UpdatedAt = record.Computed.ComputedAt
	if record.PreparedPublication != nil {
		record.PreparedPublication.Market = market
		record.PreparedPublication.PreparedAt = record.Computed.ComputedAt
	}
	return record
}
