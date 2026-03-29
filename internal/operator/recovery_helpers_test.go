package operator

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/lumosimmo/market-relayer-go/internal/pricing"
	"github.com/lumosimmo/market-relayer-go/internal/relayer"
	"github.com/lumosimmo/market-relayer-go/internal/status"
	"github.com/lumosimmo/market-relayer-go/internal/store"
)

func TestRecoveryHelperBranches(t *testing.T) {
	t.Parallel()

	t.Run("validation and exact query reject incomplete options and unsupported modes", func(t *testing.T) {
		t.Parallel()

		if err := validateRecoveryOptions(RecoveryOptions{}); err == nil {
			t.Fatal("validateRecoveryOptions(nil store) error = nil, want store validation")
		}
		if err := validateRecoveryOptions(RecoveryOptions{Store: &stubRecoveryStore{}}); err == nil {
			t.Fatal("validateRecoveryOptions(blank market) error = nil, want market validation")
		}
		if _, err := exactQuery(RecoveryOptions{
			Market:   recoveryMarket(),
			Metadata: recoveryMetadata(),
		}, ModeExact); err == nil {
			t.Fatal("exactQuery(blank record id) error = nil, want record id validation")
		}
		if _, err := RunRecovery(context.Background(), RecoveryOptions{
			Mode:   RecoveryMode("bad"),
			Market: recoveryMarket(),
			Store:  &stubRecoveryStore{},
		}); err == nil {
			t.Fatal("RunRecovery(unsupported mode) error = nil, want unsupported mode")
		}
		if _, err := RunRecovery(context.Background(), RecoveryOptions{
			Mode:     ModeResumePending,
			Market:   recoveryMarket(),
			Metadata: recoveryMetadata(),
			Store:    &stubRecoveryStore{},
		}); err == nil {
			t.Fatal("RunRecovery(resume-pending without sink) error = nil, want sink validation")
		}
		if _, err := RunRecovery(context.Background(), RecoveryOptions{
			Mode:     ModeExact,
			Market:   recoveryMarket(),
			Metadata: recoveryMetadata(),
			Store:    &stubRecoveryStore{verifyExactErr: errors.New("boom")},
			RecordID: "record-1",
		}); err == nil {
			t.Fatal("RunRecovery(exact store error) error = nil, want propagated verify error")
		}
	})

	t.Run("load prior state and recompute record use previous cycle state", func(t *testing.T) {
		t.Parallel()

		db := openRecoveryStore(t)
		first := recoveryCycleRecord(t, "cycle-prior-1", 1, recoveryMarket(), recoveryMetadata())
		second := recoveryCycleRecord(t, "cycle-prior-2", 2, recoveryMarket(), recoveryMetadata())
		if err := db.CommitCycle(context.Background(), first); err != nil {
			t.Fatalf("CommitCycle(first) error = %v", err)
		}
		acknowledgeRecoveryPublication(t, db, first)
		if err := db.CommitCycle(context.Background(), second); err != nil {
			t.Fatalf("CommitCycle(second) error = %v", err)
		}

		none, err := loadPriorState(context.Background(), RecoveryOptions{
			Market: recoveryMarket(),
			Store:  db,
		}, first)
		if err != nil {
			t.Fatalf("loadPriorState(first) error = %v", err)
		}
		if none != (pricing.State{}) {
			t.Fatalf("loadPriorState(first) = %+v, want zero state", none)
		}

		prior, err := loadPriorState(context.Background(), RecoveryOptions{
			Market: recoveryMarket(),
			Store:  db,
		}, second)
		if err != nil {
			t.Fatalf("loadPriorState(second) error = %v", err)
		}
		if prior.LastExternal == nil || prior.LastOracle == nil {
			t.Fatalf("loadPriorState(second) = %+v, want populated prior state", prior)
		}
		if !sameFixedpointValue(prior.LastExternal, first.Computed.State.LastExternal) ||
			!sameFixedpointValue(prior.LastOracle, first.Computed.State.LastOracle) ||
			!prior.OracleFallbackExpiresAt.Equal(first.Computed.State.OracleFallbackExpiresAt) {
			t.Fatalf("loadPriorState(second) = %+v, want prior state from first cycle %+v", prior, first.Computed.State)
		}

		recomputed, err := recomputeRecord(RecoveryOptions{Market: recoveryMarket()}, second, prior)
		if err != nil {
			t.Fatalf("recomputeRecord() error = %v", err)
		}
		if recomputed.SessionState != status.SessionStateOpen || recomputed.SessionReason != status.ReasonNone {
			t.Fatalf("recomputeRecord() session = {%q %q}, want {open none}", recomputed.SessionState, recomputed.SessionReason)
		}
		if !sameResult(recomputed, second.Computed) {
			t.Fatalf("recomputeRecord() core pricing = %+v, want %+v", recomputed, second.Computed)
		}
	})

	t.Run("audit and helper error branches report envelope mismatches and prior-state failures", func(t *testing.T) {
		t.Parallel()

		db := openRecoveryStore(t)
		record := recoveryCycleRecord(t, "cycle-audit-envelope", 1, recoveryMarket(), recoveryMetadata())
		mutated := *record.PreparedPublication
		mutated.Envelope = rewriteRecoveryEnvelopeMetadata(t, mutated.Envelope, "different-digest", pricing.AlgorithmVersion)
		record.PreparedPublication = &mutated
		record.Publishability.PendingSequence = mutated.Sequence
		record.Publishability.LastIdempotencyKey = mutated.IdempotencyKey
		record.Publishability.LastPayloadHash = mutated.PayloadHash
		if err := db.CommitCycle(context.Background(), record); err != nil {
			t.Fatalf("CommitCycle(audit envelope) error = %v", err)
		}

		report, err := RunRecovery(context.Background(), RecoveryOptions{
			Mode:     ModeAudit,
			Market:   recoveryMarket(),
			Metadata: recoveryMetadata(),
			Store:    db,
			RecordID: record.RecordID,
		})
		if err != nil {
			t.Fatalf("RunRecovery(audit envelope mismatch) error = %v", err)
		}
		if report.PricingMatch {
			t.Fatal("RunRecovery(audit envelope mismatch).PricingMatch = true, want false")
		}
		if !containsString(report.Mismatches, "prepared_envelope") {
			t.Fatalf("RunRecovery(audit envelope mismatch).Mismatches = %v, want prepared_envelope", report.Mismatches)
		}

		if _, err := loadPriorState(context.Background(), RecoveryOptions{
			Market: recoveryMarket(),
			Store:  &stubRecoveryStore{previousErr: errors.New("previous failed")},
		}, record); err == nil {
			t.Fatal("loadPriorState(previous error) error = nil, want propagated error")
		}
	})

	t.Run("scheduled market audit replays closed-session semantics from stored record", func(t *testing.T) {
		t.Parallel()

		scheduled := recoveryMarket()
		scheduled.SessionMode = "scheduled"
		previous := recoveryCycleRecord(t, "cycle-scheduled-prior", 1, recoveryMarket(), recoveryMetadata())
		record := recoveryScheduledClosedRecord(t, "cycle-scheduled-closed", 2, recoveryMetadata())

		prior, err := loadPriorState(context.Background(), RecoveryOptions{
			Market: scheduled,
			Store: &stubRecoveryStore{
				previous:      previous,
				previousFound: true,
			},
		}, record)
		if err != nil {
			t.Fatalf("loadPriorState(scheduled) error = %v", err)
		}
		if prior.LastOracle == nil {
			t.Fatalf("loadPriorState(scheduled) = %+v, want restored prior state", prior)
		}

		recomputed, err := recomputeRecord(RecoveryOptions{Market: scheduled}, record, pricing.State{})
		if err != nil {
			t.Fatalf("recomputeRecord(scheduled) error = %v", err)
		}
		if !sameResult(recomputed, record.Computed) {
			t.Fatalf("recomputeRecord(scheduled) = %+v, want %+v", recomputed, record.Computed)
		}
	})

	t.Run("resume pending defaults publishability and replays checkpoints", func(t *testing.T) {
		t.Parallel()

		publication := *recoveryCycleRecord(t, "cycle-replay", 3, recoveryMarket(), recoveryMetadata()).PreparedPublication
		ackTime := publication.PreparedAt.Add(time.Second)
		recording := &stubRecoveryStore{
			pending: []store.PreparedPublication{publication},
			latest:  store.PublishabilityState{},
		}
		sink := &recoverySink{
			acks: []relayer.Ack{{
				Sequence:       publication.Sequence,
				IdempotencyKey: publication.IdempotencyKey,
				PayloadHash:    publication.PayloadHash,
				AckedAt:        ackTime,
			}},
		}

		report, err := RunRecovery(context.Background(), RecoveryOptions{
			Mode:     ModeResumePending,
			Market:   recoveryMarket(),
			Metadata: recoveryMetadata(),
			Store:    recording,
			Sink:     sink,
		})
		if err != nil {
			t.Fatalf("RunRecovery(resume pending helper) error = %v", err)
		}
		if len(report.ReplayedSequences) != 1 || report.ReplayedSequences[0] != publication.Sequence {
			t.Fatalf("ReplayedSequences = %v, want [%d]", report.ReplayedSequences, publication.Sequence)
		}
		if report.NextSequence != publication.Sequence+1 {
			t.Fatalf("NextSequence = %d, want %d", report.NextSequence, publication.Sequence+1)
		}
		if len(recording.markCalls) != 1 || len(recording.ackCalls) != 1 {
			t.Fatalf("checkpoint calls = mark:%d ack:%d, want 1 each", len(recording.markCalls), len(recording.ackCalls))
		}
		if len(recording.savedPublishability) != 1 {
			t.Fatalf("saved publishability states = %d, want 1", len(recording.savedPublishability))
		}
		saved := recording.savedPublishability[0]
		if saved.Status != status.StatusAvailable || saved.Reason != status.ReasonNone {
			t.Fatalf("saved publishability = {%q %q}, want {available none}", saved.Status, saved.Reason)
		}
		if saved.UpdatedAt != ackTime.UTC() {
			t.Fatalf("saved publishability updated_at = %s, want %s", saved.UpdatedAt, ackTime.UTC())
		}
	})

	t.Run("replay pending publication propagates checkpoint failures", func(t *testing.T) {
		t.Parallel()

		publication := *recoveryCycleRecord(t, "cycle-replay-errors", 5, recoveryMarket(), recoveryMetadata()).PreparedPublication
		ack := relayer.Ack{
			Sequence:       publication.Sequence,
			IdempotencyKey: publication.IdempotencyKey,
			PayloadHash:    publication.PayloadHash,
			AckedAt:        publication.PreparedAt.Add(time.Second),
		}

		if _, err := replayPendingPublication(context.Background(), RecoveryOptions{
			Store: &stubRecoveryStore{markErr: errors.New("mark failed")},
			Sink:  &recoverySink{acks: []relayer.Ack{ack}},
		}, publication); err == nil {
			t.Fatal("replayPendingPublication(mark error) error = nil, want mark failure")
		}

		if _, err := replayPendingPublication(context.Background(), RecoveryOptions{
			Store: &stubRecoveryStore{ackErr: errors.New("ack failed")},
			Sink:  &recoverySink{acks: []relayer.Ack{ack}},
		}, publication); err == nil {
			t.Fatal("replayPendingPublication(ack error) error = nil, want ack failure")
		}
	})

	t.Run("replay pending publication rejects mismatched ack identity", func(t *testing.T) {
		t.Parallel()

		publication := *recoveryCycleRecord(t, "cycle-replay-mismatch", 6, recoveryMarket(), recoveryMetadata()).PreparedPublication
		storeDB := &stubRecoveryStore{}

		_, err := replayPendingPublication(context.Background(), RecoveryOptions{
			Store: storeDB,
			Sink: &recoverySink{acks: []relayer.Ack{{
				Sequence:       publication.Sequence,
				IdempotencyKey: publication.IdempotencyKey,
				PayloadHash:    "wrong-payload-hash",
				AckedAt:        publication.PreparedAt.Add(time.Second),
			}}},
		}, publication)
		if err == nil || !strings.Contains(err.Error(), "ack mismatch") {
			t.Fatalf("replayPendingPublication(ack mismatch) error = %v, want ack mismatch", err)
		}
		if len(storeDB.markCalls) != 0 || len(storeDB.ackCalls) != 0 {
			t.Fatalf("checkpoint calls = mark:%d ack:%d, want 0 each", len(storeDB.markCalls), len(storeDB.ackCalls))
		}
	})

	t.Run("default publishability and ack normalization choose available utc state", func(t *testing.T) {
		t.Parallel()

		at := time.Date(2026, 3, 27, 16, 0, 0, 0, time.FixedZone("CET", 3600))
		publishability := defaultPublishability("ETHUSD", at)
		if publishability.Status != status.StatusAvailable || publishability.Reason != status.ReasonNone {
			t.Fatalf("defaultPublishability() = {%q %q}, want {available none}", publishability.Status, publishability.Reason)
		}
		if !publishability.UpdatedAt.Equal(at) {
			t.Fatalf("defaultPublishability().UpdatedAt = %s, want %s", publishability.UpdatedAt, at)
		}

		if got := normalizedAckTime(relayer.Ack{AckedAt: at}); !got.Equal(at.UTC()) {
			t.Fatalf("normalizedAckTime(explicit) = %s, want %s", got, at.UTC())
		}
		if got := normalizedAckTime(relayer.Ack{}); got.IsZero() {
			t.Fatal("normalizedAckTime(zero) = zero, want current UTC time")
		}
	})
}

type stubRecoveryStore struct {
	verifyExactResult   store.CycleRecord
	verifyExactErr      error
	auditResult         store.AuditResult
	auditErr            error
	previous            store.CycleRecord
	previousFound       bool
	previousErr         error
	pending             []store.PreparedPublication
	pendingErr          error
	latest              store.PublishabilityState
	latestErr           error
	safeSequence        uint64
	safeSequenceErr     error
	savedPublishability []store.PublishabilityState
	markCalls           []recoveryCheckpoint
	markErr             error
	ackCalls            []recoveryCheckpoint
	ackErr              error
	saveErr             error
}

type recoveryCheckpoint struct {
	market string
	seq    uint64
	at     time.Time
}

func (storeDB *stubRecoveryStore) VerifyExact(context.Context, store.ExactQuery) (store.CycleRecord, error) {
	if storeDB.verifyExactErr != nil {
		return store.CycleRecord{}, storeDB.verifyExactErr
	}
	return storeDB.verifyExactResult, nil
}

func (storeDB *stubRecoveryStore) Audit(context.Context, store.ExactQuery) (store.AuditResult, error) {
	if storeDB.auditErr != nil {
		return store.AuditResult{}, storeDB.auditErr
	}
	return storeDB.auditResult, nil
}

func (storeDB *stubRecoveryStore) PreviousCycle(context.Context, string, string) (store.CycleRecord, bool, error) {
	if storeDB.previousErr != nil {
		return store.CycleRecord{}, false, storeDB.previousErr
	}
	return storeDB.previous, storeDB.previousFound, nil
}

func (storeDB *stubRecoveryStore) PendingPublications(context.Context, string) ([]store.PreparedPublication, error) {
	if storeDB.pendingErr != nil {
		return nil, storeDB.pendingErr
	}
	return append([]store.PreparedPublication(nil), storeDB.pending...), nil
}

func (storeDB *stubRecoveryStore) MarkPublicationSent(_ context.Context, market string, sequence uint64, at time.Time) error {
	if storeDB.markErr != nil {
		return storeDB.markErr
	}
	storeDB.markCalls = append(storeDB.markCalls, recoveryCheckpoint{market: market, seq: sequence, at: at.UTC()})
	return nil
}

func (storeDB *stubRecoveryStore) AcknowledgePublication(_ context.Context, market string, sequence uint64, at time.Time) error {
	if storeDB.ackErr != nil {
		return storeDB.ackErr
	}
	storeDB.ackCalls = append(storeDB.ackCalls, recoveryCheckpoint{market: market, seq: sequence, at: at.UTC()})
	storeDB.safeSequence = sequence
	return nil
}

func (storeDB *stubRecoveryStore) SafeSequence(context.Context, string) (uint64, error) {
	if storeDB.safeSequenceErr != nil {
		return 0, storeDB.safeSequenceErr
	}
	return storeDB.safeSequence, nil
}

func (storeDB *stubRecoveryStore) LatestPublishability(context.Context, string) (store.PublishabilityState, error) {
	if storeDB.latestErr != nil {
		return store.PublishabilityState{}, storeDB.latestErr
	}
	return storeDB.latest, nil
}

func (storeDB *stubRecoveryStore) SavePublishability(_ context.Context, state store.PublishabilityState) error {
	if storeDB.saveErr != nil {
		return storeDB.saveErr
	}
	storeDB.savedPublishability = append(storeDB.savedPublishability, state)
	return nil
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
