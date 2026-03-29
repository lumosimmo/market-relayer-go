package operator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/lumosimmo/market-relayer-go/internal/config"
	"github.com/lumosimmo/market-relayer-go/internal/feeds"
	"github.com/lumosimmo/market-relayer-go/internal/fixedpoint"
	"github.com/lumosimmo/market-relayer-go/internal/pricing"
	"github.com/lumosimmo/market-relayer-go/internal/relayer"
	"github.com/lumosimmo/market-relayer-go/internal/sessions"
	"github.com/lumosimmo/market-relayer-go/internal/status"
	"github.com/lumosimmo/market-relayer-go/internal/store"
	"github.com/lumosimmo/market-relayer-go/internal/testdb"
)

func TestRunRecoveryExactIsSideEffectFree(t *testing.T) {
	t.Parallel()

	db := openRecoveryStore(t)
	record := recoveryCycleRecord(t, "cycle-0001", 1, recoveryMarket(), recoveryMetadata())
	if err := db.CommitCycle(context.Background(), record); err != nil {
		t.Fatalf("CommitCycle() error = %v", err)
	}

	sink := &recoverySink{}
	report, err := RunRecovery(context.Background(), RecoveryOptions{
		Mode:     ModeExact,
		Market:   recoveryMarket(),
		Metadata: recoveryMetadata(),
		Store:    db,
		Sink:     sink,
		RecordID: record.RecordID,
	})
	if err != nil {
		t.Fatalf("RunRecovery(exact) error = %v", err)
	}

	if report.Record.RecordID != record.RecordID {
		t.Fatalf("RunRecovery(exact).Record.RecordID = %q, want %q", report.Record.RecordID, record.RecordID)
	}
	if got := sink.callCount(); got != 0 {
		t.Fatalf("sink calls = %d, want 0", got)
	}
	safeSequence, err := db.SafeSequence(context.Background(), record.Market)
	if err != nil {
		t.Fatalf("SafeSequence() error = %v", err)
	}
	if safeSequence != 0 {
		t.Fatalf("SafeSequence() = %d, want 0", safeSequence)
	}
}

func TestRunRecoveryAuditReportsMismatchesWithoutPublishing(t *testing.T) {
	t.Parallel()

	db := openRecoveryStore(t)

	first := recoveryCycleRecord(t, "cycle-0002", 1, recoveryMarket(), recoveryMetadata())
	if err := db.CommitCycle(context.Background(), first); err != nil {
		t.Fatalf("CommitCycle(first) error = %v", err)
	}

	sink := &recoverySink{}
	report, err := RunRecovery(context.Background(), RecoveryOptions{
		Mode:   ModeAudit,
		Market: recoveryAuditMarket(),
		Metadata: store.Metadata{
			ConfigDigest:            "config-digest-audit",
			PricingAlgorithmVersion: pricing.AlgorithmVersion,
			SchemaVersion:           store.SchemaVersion,
			EnvelopeVersion:         relayer.EnvelopeVersion,
		},
		Store:    db,
		Sink:     sink,
		RecordID: first.RecordID,
	})
	if err != nil {
		t.Fatalf("RunRecovery(audit) error = %v", err)
	}

	if report.PricingMatch {
		t.Fatal("RunRecovery(audit).PricingMatch = true, want false")
	}
	if len(report.Mismatches) == 0 {
		t.Fatal("RunRecovery(audit).Mismatches = empty, want values")
	}
	if got := sink.callCount(); got != 0 {
		t.Fatalf("sink calls = %d, want 0", got)
	}
	safeSequence, err := db.SafeSequence(context.Background(), first.Market)
	if err != nil {
		t.Fatalf("SafeSequence() error = %v", err)
	}
	if safeSequence != 0 {
		t.Fatalf("SafeSequence() = %d, want 0", safeSequence)
	}
}

func TestRunRecoveryResumePendingReplaysStoredEnvelopeWithoutRecompute(t *testing.T) {
	t.Parallel()

	db := openRecoveryStore(t)
	record := recoveryCycleRecord(t, "cycle-0004", 1, recoveryMarket(), recoveryMetadata())
	record.Publishability = store.PublishabilityState{
		Market:             record.Market,
		Status:             status.StatusUnavailable,
		Reason:             status.ReasonSinkUnavailable,
		UpdatedAt:          record.Computed.ComputedAt,
		LastStableHash:     "stable-hash-1",
		PendingSequence:    record.PreparedPublication.Sequence,
		LastIdempotencyKey: record.PreparedPublication.IdempotencyKey,
		LastPayloadHash:    record.PreparedPublication.PayloadHash,
	}
	if err := db.CommitCycle(context.Background(), record); err != nil {
		t.Fatalf("CommitCycle() error = %v", err)
	}

	sink := &recoverySink{
		acks: []relayer.Ack{
			{
				Sequence:       record.PreparedPublication.Sequence,
				IdempotencyKey: record.PreparedPublication.IdempotencyKey,
				PayloadHash:    record.PreparedPublication.PayloadHash,
				AckedAt:        record.PreparedPublication.PreparedAt.Add(time.Second),
			},
		},
	}
	report, err := RunRecovery(context.Background(), RecoveryOptions{
		Mode:     ModeResumePending,
		Market:   recoveryMarket(),
		Metadata: recoveryMetadata(),
		Store:    db,
		Sink:     sink,
	})
	if err != nil {
		t.Fatalf("RunRecovery(resume-pending) error = %v", err)
	}

	if !slices.Equal(report.ReplayedSequences, []uint64{record.PreparedPublication.Sequence}) {
		t.Fatalf("RunRecovery(resume-pending).ReplayedSequences = %v, want [%d]", report.ReplayedSequences, record.PreparedPublication.Sequence)
	}
	if report.NextSequence != record.PreparedPublication.Sequence+1 {
		t.Fatalf("RunRecovery(resume-pending).NextSequence = %d, want %d", report.NextSequence, record.PreparedPublication.Sequence+1)
	}
	publications := sink.publications()
	if len(publications) != 1 {
		t.Fatalf("len(sink.publications()) = %d, want 1", len(publications))
	}
	if !bytes.Equal(publications[0].Envelope, record.PreparedPublication.Envelope) {
		t.Fatalf("sink envelope = %s, want %s", publications[0].Envelope, record.PreparedPublication.Envelope)
	}
	safeSequence, err := db.SafeSequence(context.Background(), record.Market)
	if err != nil {
		t.Fatalf("SafeSequence() error = %v", err)
	}
	if safeSequence != record.PreparedPublication.Sequence {
		t.Fatalf("SafeSequence() = %d, want %d", safeSequence, record.PreparedPublication.Sequence)
	}
	publishability, err := db.LatestPublishability(context.Background(), record.Market)
	if err != nil {
		t.Fatalf("LatestPublishability() error = %v", err)
	}
	if publishability.PendingSequence != 0 {
		t.Fatalf("LatestPublishability().PendingSequence = %d, want 0", publishability.PendingSequence)
	}
	if publishability.LastSequence != record.PreparedPublication.Sequence {
		t.Fatalf("LatestPublishability().LastSequence = %d, want %d", publishability.LastSequence, record.PreparedPublication.Sequence)
	}
}

func TestRunRecoveryResumePendingRejectsMetadataMismatch(t *testing.T) {
	t.Parallel()

	db := openRecoveryStore(t)
	record := recoveryCycleRecord(t, "cycle-0005", 1, recoveryMarket(), recoveryMetadata())
	mutated := *record.PreparedPublication
	mutated.Envelope = rewriteRecoveryEnvelopeMetadata(t, mutated.Envelope, "config-digest-stale", pricing.AlgorithmVersion)
	record.PreparedPublication = &mutated
	record.Publishability.PendingSequence = mutated.Sequence
	record.Publishability.LastIdempotencyKey = mutated.IdempotencyKey
	record.Publishability.LastPayloadHash = mutated.PayloadHash
	record.Publishability.Status = status.StatusUnavailable
	record.Publishability.Reason = status.ReasonSinkUnavailable
	if err := db.CommitCycle(context.Background(), record); err != nil {
		t.Fatalf("CommitCycle() error = %v", err)
	}

	sink := &recoverySink{
		acks: []relayer.Ack{
			{
				Sequence:       mutated.Sequence,
				IdempotencyKey: mutated.IdempotencyKey,
				PayloadHash:    mutated.PayloadHash,
				AckedAt:        mutated.PreparedAt.Add(time.Second),
			},
		},
	}

	_, err := RunRecovery(context.Background(), RecoveryOptions{
		Mode:     ModeResumePending,
		Market:   recoveryMarket(),
		Metadata: recoveryMetadata(),
		Store:    db,
		Sink:     sink,
	})
	if err == nil {
		t.Fatal("RunRecovery(resume-pending) error = nil, want metadata mismatch")
	}
	if got := err.Error(); !strings.Contains(got, "pending publication metadata mismatch") {
		t.Fatalf("RunRecovery(resume-pending) error = %q, want metadata mismatch", got)
	}
	if got := sink.callCount(); got != 0 {
		t.Fatalf("sink calls = %d, want 0", got)
	}
}

func TestRunRecoveryResumePendingRejectsPublicationIdentityMismatch(t *testing.T) {
	t.Parallel()

	db := openRecoveryStore(t)
	record := recoveryCycleRecord(t, "cycle-0005b", 1, recoveryMarket(), recoveryMetadata())
	mutated := *record.PreparedPublication
	mutated.PayloadHash = "row-payload-hash-mismatch"
	record.PreparedPublication = &mutated
	record.Publishability.PendingSequence = mutated.Sequence
	record.Publishability.LastIdempotencyKey = mutated.IdempotencyKey
	record.Publishability.LastPayloadHash = mutated.PayloadHash
	record.Publishability.Status = status.StatusUnavailable
	record.Publishability.Reason = status.ReasonSinkUnavailable
	if err := db.CommitCycle(context.Background(), record); err != nil {
		t.Fatalf("CommitCycle() error = %v", err)
	}

	sink := &recoverySink{}
	_, err := RunRecovery(context.Background(), RecoveryOptions{
		Mode:     ModeResumePending,
		Market:   recoveryMarket(),
		Metadata: recoveryMetadata(),
		Store:    db,
		Sink:     sink,
	})
	if err == nil {
		t.Fatal("RunRecovery(resume-pending identity mismatch) error = nil, want identity mismatch")
	}
	if got := err.Error(); !strings.Contains(got, "pending publication identity mismatch") || !strings.Contains(got, "payload_hash") {
		t.Fatalf("RunRecovery(resume-pending identity mismatch) error = %q, want payload hash identity mismatch", got)
	}
	if got := sink.callCount(); got != 0 {
		t.Fatalf("sink calls = %d, want 0", got)
	}
}

func openRecoveryStore(t *testing.T) *store.DB {
	t.Helper()

	cfg := config.StoreConfig{
		DSN:                strings.TrimSpace(testdb.DSN(t)),
		MaxOpenConns:       4,
		MinOpenConns:       0,
		ConnMaxLifetime:    30 * time.Minute,
		ConnectTimeout:     5 * time.Second,
		LockTimeout:        5 * time.Second,
		LeaseTTL:           15 * time.Second,
		LeaseRenewInterval: 5 * time.Second,
	}
	if _, err := store.Migrate(context.Background(), cfg); err != nil {
		t.Fatalf("store.Migrate() error = %v", err)
	}

	db, err := store.OpenConfigContext(context.Background(), cfg)
	if err != nil {
		t.Fatalf("store.OpenConfigContext() error = %v", err)
	}
	t.Cleanup(func() {
		if closeErr := db.Close(); closeErr != nil {
			t.Fatalf("store.Close() error = %v", closeErr)
		}
	})
	return db
}

func acknowledgeRecoveryPublication(t *testing.T, db *store.DB, record store.CycleRecord) {
	t.Helper()

	if record.PreparedPublication == nil {
		return
	}

	ackedAt := record.PreparedPublication.PreparedAt.Add(time.Second)
	if err := db.MarkPublicationSent(context.Background(), record.Market, record.PreparedPublication.Sequence, ackedAt); err != nil {
		t.Fatalf("MarkPublicationSent() error = %v", err)
	}
	if err := db.AcknowledgePublication(context.Background(), record.Market, record.PreparedPublication.Sequence, ackedAt); err != nil {
		t.Fatalf("AcknowledgePublication() error = %v", err)
	}
}

func recoveryMarket() config.MarketConfig {
	return config.MarketConfig{
		Symbol:                 "ETHUSD",
		SessionMode:            config.SessionModeAlwaysOpen,
		PublishInterval:        3 * time.Second,
		StalenessThreshold:     10 * time.Second,
		MaxFallbackAge:         30 * time.Second,
		ClampBPS:               50,
		DivergenceThresholdBPS: 250,
		PublishScale:           2,
		Sources: []config.MarketSourceRef{
			{Name: "coinbase"},
		},
	}
}

func recoveryAuditMarket() config.MarketConfig {
	market := recoveryMarket()
	market.PublishScale = 3
	return market
}

func recoveryMetadata() store.Metadata {
	return store.Metadata{
		ConfigDigest:            "config-digest-operator-recovery-test",
		PricingAlgorithmVersion: pricing.AlgorithmVersion,
		SchemaVersion:           store.SchemaVersion,
		EnvelopeVersion:         relayer.EnvelopeVersion,
	}
}

func recoveryCycleRecord(t *testing.T, recordID string, sequence uint64, market config.MarketConfig, metadata store.Metadata) store.CycleRecord {
	t.Helper()

	at := time.Date(2026, 3, 27, 12, 0, 0, 0, time.UTC).Add(time.Duration(sequence) * time.Second)
	value := mustRecoveryDecimal(t, fmt.Sprintf("100.%02d", sequence))
	sourceTS := at.UTC()
	quote := feeds.Quote{
		Source:             "coinbase",
		Symbol:             market.Symbol,
		Bid:                value,
		Ask:                value,
		Last:               value,
		Scale:              value.Scale,
		ReceivedAt:         at.UTC(),
		SourceTS:           &sourceTS,
		FreshnessBasisKind: feeds.FreshnessBasisSourceTimestamp,
		FreshnessBasisAt:   sourceTS,
	}
	computed, err := pricing.Compute(pricing.Inputs{
		Market: market,
		Quotes: []feeds.Quote{quote},
		At:     at.Add(time.Second),
	})
	if err != nil {
		t.Fatalf("pricing.Compute() error = %v", err)
	}
	computed = sessions.Apply(computed, pricing.State{}, sessions.OpenWindow())
	stableHash, publication, err := relayer.PreparePublication(metadata, computed, store.PublishabilityState{
		Market:    market.Symbol,
		Status:    status.StatusAvailable,
		Reason:    status.ReasonNone,
		UpdatedAt: computed.ComputedAt,
	}, sequence, at.Add(2*time.Second))
	if err != nil {
		t.Fatalf("relayer.PreparePublication() error = %v", err)
	}

	return store.CycleRecord{
		RecordID:  recordID,
		Market:    market.Symbol,
		RawQuotes: []feeds.Quote{quote},
		Computed:  computed,
		Metadata:  metadata,
		Publishability: store.PublishabilityState{
			Market:             market.Symbol,
			Status:             status.StatusAvailable,
			Reason:             status.ReasonNone,
			UpdatedAt:          computed.ComputedAt,
			LastStableHash:     stableHash,
			PendingSequence:    publication.Sequence,
			LastIdempotencyKey: publication.IdempotencyKey,
			LastPayloadHash:    publication.PayloadHash,
		},
		PreparedPublication: &publication,
	}
}

func recoveryScheduledClosedRecord(t *testing.T, recordID string, sequence uint64, metadata store.Metadata) store.CycleRecord {
	t.Helper()

	market := recoveryMarket()
	market.SessionMode = config.SessionModeScheduled

	at := time.Date(2026, 3, 27, 12, 0, 0, 0, time.UTC).Add(time.Duration(sequence) * time.Second)
	value := mustRecoveryDecimal(t, fmt.Sprintf("100.%02d", sequence))
	sourceTS := at.UTC()
	quote := feeds.Quote{
		Source:             "coinbase",
		Symbol:             market.Symbol,
		Bid:                value,
		Ask:                value,
		Last:               value,
		Scale:              value.Scale,
		ReceivedAt:         at.UTC(),
		SourceTS:           &sourceTS,
		FreshnessBasisKind: feeds.FreshnessBasisSourceTimestamp,
		FreshnessBasisAt:   sourceTS,
	}
	computed, err := pricing.Compute(pricing.Inputs{
		Market:         market,
		Quotes:         []feeds.Quote{quote},
		At:             at.Add(time.Second),
		ExternalClosed: true,
	})
	if err != nil {
		t.Fatalf("pricing.Compute(closed session) error = %v", err)
	}
	computed = sessions.Apply(computed, pricing.State{}, sessions.ExternalStaleWindow(status.ReasonSessionClosed))

	stableHash, publication, err := relayer.PreparePublication(metadata, computed, store.PublishabilityState{
		Market:    market.Symbol,
		Status:    status.StatusAvailable,
		Reason:    status.ReasonNone,
		UpdatedAt: computed.ComputedAt,
	}, sequence, at.Add(2*time.Second))
	if err != nil {
		t.Fatalf("relayer.PreparePublication(closed session) error = %v", err)
	}

	return store.CycleRecord{
		RecordID:  recordID,
		Market:    market.Symbol,
		RawQuotes: []feeds.Quote{quote},
		Computed:  computed,
		Metadata:  metadata,
		Publishability: store.PublishabilityState{
			Market:             market.Symbol,
			Status:             status.StatusAvailable,
			Reason:             status.ReasonNone,
			UpdatedAt:          computed.ComputedAt,
			LastStableHash:     stableHash,
			PendingSequence:    publication.Sequence,
			LastIdempotencyKey: publication.IdempotencyKey,
			LastPayloadHash:    publication.PayloadHash,
		},
		PreparedPublication: &publication,
	}
}

func mustRecoveryDecimal(t *testing.T, raw string) fixedpoint.Value {
	t.Helper()

	value, err := fixedpoint.ParseDecimal(raw)
	if err != nil {
		t.Fatalf("fixedpoint.ParseDecimal(%q) error = %v", raw, err)
	}
	return value
}

func rewriteRecoveryEnvelopeMetadata(t *testing.T, raw []byte, configDigest string, pricingVersion string) []byte {
	t.Helper()

	var envelope relayer.Envelope
	if err := json.Unmarshal(raw, &envelope); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	envelope.ConfigDigest = configDigest
	envelope.PricingAlgorithmVersion = pricingVersion

	rewritten, err := json.Marshal(envelope)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	return rewritten
}

type recoverySink struct {
	acks []relayer.Ack

	calls []store.PreparedPublication
}

func (sink *recoverySink) Kind() string {
	return "recovery"
}

func (sink *recoverySink) Close() error {
	return nil
}

func (sink *recoverySink) Publish(_ context.Context, publication store.PreparedPublication) (relayer.Ack, error) {
	sink.calls = append(sink.calls, publication)
	index := len(sink.calls) - 1
	if index >= len(sink.acks) {
		return relayer.Ack{}, errors.New("unexpected publish call")
	}
	return sink.acks[index], nil
}

func (sink *recoverySink) callCount() int {
	return len(sink.calls)
}

func (sink *recoverySink) publications() []store.PreparedPublication {
	return append([]store.PreparedPublication(nil), sink.calls...)
}
