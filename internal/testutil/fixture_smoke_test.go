package testutil

import (
	"context"
	"testing"

	"github.com/lumosimmo/market-relayer-go/internal/operator"
	"github.com/lumosimmo/market-relayer-go/internal/status"
	"github.com/lumosimmo/market-relayer-go/internal/store"
	"github.com/lumosimmo/market-relayer-go/internal/testdb"
)

func TestFixtureSmokeCoversRecoveryAndCrashBoundaries(t *testing.T) {
	t.Parallel()

	fixtures := loadTestFixtures(t)
	storeDSN := testdb.DSN(t)

	harness, err := NewHarness(storeDSN, fixtures.Market, fixtures.Metadata)
	if err != nil {
		t.Fatalf("NewHarness() error = %v", err)
	}

	happy := mustFixtureScenario(t, fixtures, "happy-path-open")
	happyRecord := mustRunPublishedScenario(t, harness, happy, "happy")
	mustAckFixturePublication(t, harness, happyRecord, happy.SinkOutcomes[0], "happy")

	fallback := mustFixtureScenario(t, fixtures, "fallback-transition")
	fallbackRecord := mustRunPublishedScenario(t, harness, fallback, "fallback")
	if fallbackRecord.Computed.SessionState != status.SessionStateFallbackOnly {
		t.Fatalf("fallback session state = %q, want %q", fallbackRecord.Computed.SessionState, status.SessionStateFallbackOnly)
	}
	if fallbackRecord.Computed.Oracle.Reason != status.ReasonFallbackActive {
		t.Fatalf("fallback oracle reason = %q, want %q", fallbackRecord.Computed.Oracle.Reason, status.ReasonFallbackActive)
	}
	mustAckFixturePublication(t, harness, fallbackRecord, fallback.SinkOutcomes[0], "fallback")

	if err := harness.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	restarted, err := NewHarness(storeDSN, fixtures.Market, fixtures.Metadata)
	if err != nil {
		t.Fatalf("NewHarness(restarted) error = %v", err)
	}

	exactReport, err := restarted.Recover(context.Background(), operator.ModeExact, fallbackRecord.RecordID, nil)
	if err != nil {
		t.Fatalf("Recover(exact) error = %v", err)
	}
	if exactReport.Record.RecordID != fallbackRecord.RecordID {
		t.Fatalf("Recover(exact).Record.RecordID = %q, want %q", exactReport.Record.RecordID, fallbackRecord.RecordID)
	}
	if !exactReport.MetadataMatch || !exactReport.PricingMatch {
		t.Fatalf("Recover(exact) = %+v, want metadata and pricing match", exactReport)
	}
	safeSequence, err := restarted.SafeSequence()
	if err != nil {
		t.Fatalf("SafeSequence() error = %v", err)
	}
	if safeSequence != fallbackRecord.PreparedPublication.Sequence {
		t.Fatalf("SafeSequence() after exact = %d, want %d", safeSequence, fallbackRecord.PreparedPublication.Sequence)
	}

	pendingRecovery := mustFixtureScenario(t, fixtures, "pending-recovery")
	pendingRecord := mustRunPublishedScenario(t, restarted, pendingRecovery, "pending")
	pendingSink := newFixtureSink("fixture-recovery", pendingRecovery.SinkOutcomes)
	pendingReport, err := restarted.Recover(context.Background(), operator.ModeResumePending, "", pendingSink)
	if err != nil {
		t.Fatalf("Recover(resume-pending pending path) error = %v", err)
	}
	assertReplayedSequence(t, pendingReport, pendingRecord.PreparedPublication.Sequence, "pending recovery")
	if got := len(pendingSink.publications()); got != 1 {
		t.Fatalf("pending recovery sink calls = %d, want 1", got)
	}

	crashRecovery := mustFixtureScenario(t, fixtures, "sent-boundary-recovery")
	crashRecord := mustRunPublishedScenario(t, restarted, crashRecovery, "crash")
	if err := restarted.MarkPublicationSent(crashRecord.PreparedPublication.Sequence, crashRecovery.SinkOutcomes[0].AckedAt); err != nil {
		t.Fatalf("MarkPublicationSent(crash) error = %v", err)
	}
	pending, err := restarted.PendingPublications()
	if err != nil {
		t.Fatalf("PendingPublications() error = %v", err)
	}
	if len(pending) != 1 || pending[0].State != store.PublicationStateSent {
		t.Fatalf("PendingPublications() after sent boundary = %+v, want one sent publication", pending)
	}
	if err := restarted.Close(); err != nil {
		t.Fatalf("restarted.Close() error = %v", err)
	}

	resumed, err := NewHarness(storeDSN, fixtures.Market, fixtures.Metadata)
	if err != nil {
		t.Fatalf("NewHarness(resumed) error = %v", err)
	}
	t.Cleanup(func() {
		if closeErr := resumed.Close(); closeErr != nil {
			t.Fatalf("resumed.Close() error = %v", closeErr)
		}
	})

	duplicateSink := newFixtureSink("fixture-recovery", crashRecovery.SinkOutcomes)
	crashReport, err := resumed.Recover(context.Background(), operator.ModeResumePending, "", duplicateSink)
	if err != nil {
		t.Fatalf("Recover(resume-pending crash path) error = %v", err)
	}
	assertReplayedSequence(t, crashReport, crashRecord.PreparedPublication.Sequence, "crash recovery")
	if got := len(duplicateSink.publications()); got != 1 {
		t.Fatalf("duplicate recovery sink calls = %d, want 1", got)
	}
	if !duplicateSink.outcomes[0].Duplicate {
		t.Fatal("duplicate recovery fixture did not preserve duplicate ack outcome")
	}
	snapshot, err := resumed.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}
	if snapshot.LastSuccessfulPublication == nil || snapshot.LastSuccessfulPublication.Sequence != crashRecord.PreparedPublication.Sequence {
		t.Fatalf("Snapshot().LastSuccessfulPublication = %+v, want sequence %d", snapshot.LastSuccessfulPublication, crashRecord.PreparedPublication.Sequence)
	}
	safeSequence, err = resumed.SafeSequence()
	if err != nil {
		t.Fatalf("SafeSequence() after crash recovery error = %v", err)
	}
	if safeSequence != crashRecord.PreparedPublication.Sequence {
		t.Fatalf("SafeSequence() after crash recovery = %d, want %d", safeSequence, crashRecord.PreparedPublication.Sequence)
	}
}
