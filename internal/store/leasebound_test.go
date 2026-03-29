package store

import (
	"context"
	"testing"
	"time"

	"github.com/lumosimmo/market-relayer-go/internal/pricing"
)

func TestLeaseBoundRuntimeStoreRequiresLeaseForWritesAndDelegatesReads(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 3, 26, 12, 0, 0, 0, time.UTC)
	lease := Lease{
		Market:       "ETHUSD",
		Owner:        "instance-a",
		FencingToken: 7,
		ExpiresAt:    now.Add(time.Minute),
	}
	record := CycleRecord{
		Market:   "ETHUSD",
		RecordID: "cycle-1",
	}
	publication := PreparedPublication{
		Market:   "ETHUSD",
		Sequence: 3,
	}
	query := ExactQuery{
		Market:   "ETHUSD",
		RecordID: "cycle-1",
	}
	publishability := PublishabilityState{
		Market:       "ETHUSD",
		UpdatedAt:    now,
		LastSequence: 3,
	}
	expectedState := pricing.State{LastOracleAt: now}
	storeDB := &leaseBoundStoreStub{
		pending:              []PreparedPublication{publication},
		verifyRecord:         record,
		auditResult:          AuditResult{Record: record, MetadataMatch: true},
		previousRecord:       record,
		previousFound:        true,
		safeSequence:         12,
		latestState:          expectedState,
		latestPublishability: publishability,
	}
	leased := NewLeaseBoundRuntimeStore(storeDB)
	ctx := context.Background()

	if _, err := leased.CurrentLease(); err == nil {
		t.Fatal("CurrentLease() error = nil, want lease-missing failure")
	} else if got := err.Error(); got != "store: lease is not held" {
		t.Fatalf("CurrentLease() error = %q, want %q", got, "store: lease is not held")
	}
	if err := leased.CommitCycle(ctx, record); err == nil {
		t.Fatal("CommitCycle() error = nil, want lease-missing failure")
	}
	if err := leased.MarkPublicationSent(ctx, "ETHUSD", 3, now); err == nil {
		t.Fatal("MarkPublicationSent() error = nil, want lease-missing failure")
	}
	if err := leased.AcknowledgePublication(ctx, "ETHUSD", 3, now); err == nil {
		t.Fatal("AcknowledgePublication() error = nil, want lease-missing failure")
	}
	if err := leased.SavePublishability(ctx, publishability); err == nil {
		t.Fatal("SavePublishability() error = nil, want lease-missing failure")
	}

	leased.UpdateLease(lease)
	if got, err := leased.CurrentLease(); err != nil || got != lease {
		t.Fatalf("CurrentLease() = (%+v, %v), want (%+v, nil)", got, err, lease)
	}
	if err := leased.CommitCycle(ctx, record); err != nil {
		t.Fatalf("CommitCycle() error = %v", err)
	}
	if err := leased.MarkPublicationSent(ctx, "ETHUSD", 3, now); err != nil {
		t.Fatalf("MarkPublicationSent() error = %v", err)
	}
	if err := leased.AcknowledgePublication(ctx, "ETHUSD", 3, now); err != nil {
		t.Fatalf("AcknowledgePublication() error = %v", err)
	}
	if err := leased.SavePublishability(ctx, publishability); err != nil {
		t.Fatalf("SavePublishability() error = %v", err)
	}

	if got, err := leased.PendingPublications(ctx, "ETHUSD"); err != nil || len(got) != 1 || got[0].Sequence != publication.Sequence {
		t.Fatalf("PendingPublications() = (%+v, %v), want sequence %d", got, err, publication.Sequence)
	}
	if got, err := leased.VerifyExact(ctx, query); err != nil || got.RecordID != record.RecordID {
		t.Fatalf("VerifyExact() = (%+v, %v), want record %q", got, err, record.RecordID)
	}
	if got, err := leased.Audit(ctx, query); err != nil || got.Record.RecordID != record.RecordID {
		t.Fatalf("Audit() = (%+v, %v), want record %q", got, err, record.RecordID)
	}
	if got, found, err := leased.PreviousCycle(ctx, "ETHUSD", record.RecordID); err != nil || !found || got.RecordID != record.RecordID {
		t.Fatalf("PreviousCycle() = (%+v, %t, %v), want record %q", got, found, err, record.RecordID)
	}
	if got, err := leased.SafeSequence(ctx, "ETHUSD"); err != nil || got != 12 {
		t.Fatalf("SafeSequence() = (%d, %v), want (12, nil)", got, err)
	}
	if got, err := leased.LatestState(ctx, "ETHUSD"); err != nil || !got.LastOracleAt.Equal(expectedState.LastOracleAt) {
		t.Fatalf("LatestState() = (%+v, %v), want %+v", got, err, expectedState)
	}
	if got, err := leased.LatestPublishability(ctx, "ETHUSD"); err != nil || got.LastSequence != publishability.LastSequence {
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

type leaseBoundStoreStub struct {
	pending              []PreparedPublication
	verifyRecord         CycleRecord
	auditResult          AuditResult
	previousRecord       CycleRecord
	previousFound        bool
	safeSequence         uint64
	latestState          pricing.State
	latestPublishability PublishabilityState

	commitCalls             []leaseBoundCommitCall
	markSentCalls           []leaseBoundMarkSentCall
	ackCalls                []leaseBoundAckCall
	savePublishabilityCalls []leaseBoundSavePublishabilityCall
}

type leaseBoundCommitCall struct {
	lease  Lease
	record CycleRecord
}

type leaseBoundMarkSentCall struct {
	lease    Lease
	market   string
	sequence uint64
	sentAt   time.Time
}

type leaseBoundAckCall struct {
	lease    Lease
	market   string
	sequence uint64
	ackedAt  time.Time
}

type leaseBoundSavePublishabilityCall struct {
	lease Lease
	state PublishabilityState
}

func (storeDB *leaseBoundStoreStub) CommitCycleWithLease(_ context.Context, lease Lease, record CycleRecord) error {
	storeDB.commitCalls = append(storeDB.commitCalls, leaseBoundCommitCall{lease: lease, record: record})
	return nil
}

func (storeDB *leaseBoundStoreStub) VerifyExact(context.Context, ExactQuery) (CycleRecord, error) {
	return storeDB.verifyRecord, nil
}

func (storeDB *leaseBoundStoreStub) Audit(context.Context, ExactQuery) (AuditResult, error) {
	return storeDB.auditResult, nil
}

func (storeDB *leaseBoundStoreStub) PreviousCycle(context.Context, string, string) (CycleRecord, bool, error) {
	return storeDB.previousRecord, storeDB.previousFound, nil
}

func (storeDB *leaseBoundStoreStub) PendingPublications(context.Context, string) ([]PreparedPublication, error) {
	return storeDB.pending, nil
}

func (storeDB *leaseBoundStoreStub) MarkPublicationSentWithLease(_ context.Context, lease Lease, market string, sequence uint64, sentAt time.Time) error {
	storeDB.markSentCalls = append(storeDB.markSentCalls, leaseBoundMarkSentCall{lease: lease, market: market, sequence: sequence, sentAt: sentAt})
	return nil
}

func (storeDB *leaseBoundStoreStub) AcknowledgePublicationWithLease(_ context.Context, lease Lease, market string, sequence uint64, ackedAt time.Time) error {
	storeDB.ackCalls = append(storeDB.ackCalls, leaseBoundAckCall{lease: lease, market: market, sequence: sequence, ackedAt: ackedAt})
	return nil
}

func (storeDB *leaseBoundStoreStub) SafeSequence(context.Context, string) (uint64, error) {
	return storeDB.safeSequence, nil
}

func (storeDB *leaseBoundStoreStub) LatestState(context.Context, string) (pricing.State, error) {
	return storeDB.latestState, nil
}

func (storeDB *leaseBoundStoreStub) LatestPublishability(context.Context, string) (PublishabilityState, error) {
	return storeDB.latestPublishability, nil
}

func (storeDB *leaseBoundStoreStub) SavePublishabilityWithLease(_ context.Context, lease Lease, state PublishabilityState) error {
	storeDB.savePublishabilityCalls = append(storeDB.savePublishabilityCalls, leaseBoundSavePublishabilityCall{lease: lease, state: state})
	return nil
}
