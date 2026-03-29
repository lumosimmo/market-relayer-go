package operator

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/lumosimmo/market-relayer-go/internal/fixedpoint"
	"github.com/lumosimmo/market-relayer-go/internal/pricing"
	"github.com/lumosimmo/market-relayer-go/internal/relayer"
	"github.com/lumosimmo/market-relayer-go/internal/status"
	"github.com/lumosimmo/market-relayer-go/internal/store"
)

func TestRunRecoveryAuditAndResumePendingEdges(t *testing.T) {
	t.Parallel()

	t.Run("exact mode requires a record id", func(t *testing.T) {
		t.Parallel()

		_, err := RunRecovery(context.Background(), RecoveryOptions{
			Mode:     ModeExact,
			Market:   recoveryMarket(),
			Metadata: recoveryMetadata(),
			Store:    &stubRecoveryStore{},
		})
		if err == nil {
			t.Fatal("RunRecovery(exact without record id) error = nil, want validation error")
		}
	})

	t.Run("audit can succeed without a prepared publication", func(t *testing.T) {
		t.Parallel()

		record := recoveryCycleRecord(t, "cycle-audit-clean", 1, recoveryMarket(), recoveryMetadata())
		recomputed, err := recomputeRecord(RecoveryOptions{Market: recoveryMarket()}, record, pricing.State{})
		if err != nil {
			t.Fatalf("recomputeRecord() error = %v", err)
		}
		record.Computed = recomputed
		record.PreparedPublication = nil
		record.Publishability.PendingSequence = 0
		record.Publishability.LastIdempotencyKey = ""
		record.Publishability.LastPayloadHash = ""

		report, err := RunRecovery(context.Background(), RecoveryOptions{
			Mode:     ModeAudit,
			Market:   recoveryMarket(),
			Metadata: recoveryMetadata(),
			Store: &stubRecoveryStore{
				auditResult: store.AuditResult{
					Record:        record,
					MetadataMatch: true,
				},
			},
			RecordID: record.RecordID,
		})
		if err != nil {
			t.Fatalf("RunRecovery(audit clean) error = %v", err)
		}
		if !report.MetadataMatch || !report.PricingMatch || len(report.Mismatches) != 0 {
			t.Fatalf("RunRecovery(audit clean) = %+v, want clean audit report", report)
		}
	})

	t.Run("audit propagates store and recompute failures", func(t *testing.T) {
		t.Parallel()

		_, err := RunRecovery(context.Background(), RecoveryOptions{
			Mode:     ModeAudit,
			Market:   recoveryMarket(),
			Metadata: recoveryMetadata(),
			Store:    &stubRecoveryStore{auditErr: errors.New("audit failed")},
			RecordID: "cycle-audit-error",
		})
		if err == nil || !strings.Contains(err.Error(), "audit failed") {
			t.Fatalf("RunRecovery(audit store error) error = %v, want audit failure", err)
		}

		record := recoveryCycleRecord(t, "cycle-audit-recompute", 2, recoveryMarket(), recoveryMetadata())
		record.RawQuotes[0].Symbol = "BTCUSD"
		_, err = RunRecovery(context.Background(), RecoveryOptions{
			Mode:     ModeAudit,
			Market:   recoveryMarket(),
			Metadata: recoveryMetadata(),
			Store: &stubRecoveryStore{
				auditResult: store.AuditResult{
					Record:        record,
					MetadataMatch: true,
				},
			},
			RecordID: record.RecordID,
		})
		if err == nil {
			t.Fatal("RunRecovery(audit recompute error) error = nil, want pricing failure")
		}
	})

	t.Run("resume pending with no publications returns next safe sequence", func(t *testing.T) {
		t.Parallel()

		report, err := RunRecovery(context.Background(), RecoveryOptions{
			Mode:     ModeResumePending,
			Market:   recoveryMarket(),
			Metadata: recoveryMetadata(),
			Store: &stubRecoveryStore{
				latest:       store.PublishabilityState{Market: "ETHUSD", Status: status.StatusAvailable, Reason: status.ReasonNone},
				safeSequence: 9,
			},
			Sink: &recoverySink{},
		})
		if err != nil {
			t.Fatalf("RunRecovery(resume-pending empty) error = %v", err)
		}
		if len(report.ReplayedSequences) != 0 {
			t.Fatalf("ReplayedSequences = %v, want empty", report.ReplayedSequences)
		}
		if report.NextSequence != 10 {
			t.Fatalf("NextSequence = %d, want 10", report.NextSequence)
		}
	})

	t.Run("resume pending propagates latest publishability and save failures", func(t *testing.T) {
		t.Parallel()

		publication := *recoveryCycleRecord(t, "cycle-resume-save", 3, recoveryMarket(), recoveryMetadata()).PreparedPublication
		ack := relayer.Ack{
			Sequence:       publication.Sequence,
			IdempotencyKey: publication.IdempotencyKey,
			PayloadHash:    publication.PayloadHash,
			AckedAt:        publication.PreparedAt.Add(time.Second),
		}

		_, err := RunRecovery(context.Background(), RecoveryOptions{
			Mode:     ModeResumePending,
			Market:   recoveryMarket(),
			Metadata: recoveryMetadata(),
			Store:    &stubRecoveryStore{latestErr: errors.New("latest failed")},
			Sink:     &recoverySink{acks: []relayer.Ack{ack}},
		})
		if err == nil || !strings.Contains(err.Error(), "latest failed") {
			t.Fatalf("RunRecovery(resume-pending latest error) error = %v, want latest failure", err)
		}

		_, err = RunRecovery(context.Background(), RecoveryOptions{
			Mode:     ModeResumePending,
			Market:   recoveryMarket(),
			Metadata: recoveryMetadata(),
			Store: &stubRecoveryStore{
				pending: []store.PreparedPublication{publication},
				saveErr: errors.New("save failed"),
			},
			Sink: &recoverySink{acks: []relayer.Ack{ack}},
		})
		if err == nil || !strings.Contains(err.Error(), "save failed") {
			t.Fatalf("RunRecovery(resume-pending save error) error = %v, want save failure", err)
		}
	})
}

func TestReplayPendingPublicationPropagatesPublishFailure(t *testing.T) {
	t.Parallel()

	publication := *recoveryCycleRecord(t, "cycle-replay-publish", 4, recoveryMarket(), recoveryMetadata()).PreparedPublication
	_, err := replayPendingPublication(context.Background(), RecoveryOptions{
		Store: &stubRecoveryStore{},
		Sink:  &recoverySink{},
	}, publication)
	if err == nil {
		t.Fatal("replayPendingPublication(publish error) error = nil, want publish failure")
	}
}

func TestRecomputeRecordRejectsInvalidPriorPricingState(t *testing.T) {
	t.Parallel()

	record := recoveryCycleRecord(t, "cycle-recompute-invalid", 5, recoveryMarket(), recoveryMetadata())
	_, err := recomputeRecord(RecoveryOptions{Market: recoveryMarket()}, record, pricing.State{
		LastOracle: &fixedpoint.Value{Int: 1, Scale: fixedpoint.MaxScale + 1},
	})
	if err == nil {
		t.Fatal("recomputeRecord(invalid prior state) error = nil, want fixedpoint failure")
	}
}
