package testutil

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/lumosimmo/market-relayer-go/internal/config"
	"github.com/lumosimmo/market-relayer-go/internal/feeds"
	"github.com/lumosimmo/market-relayer-go/internal/operator"
	"github.com/lumosimmo/market-relayer-go/internal/pricing"
	"github.com/lumosimmo/market-relayer-go/internal/relayer"
	"github.com/lumosimmo/market-relayer-go/internal/sessions"
	"github.com/lumosimmo/market-relayer-go/internal/status"
	"github.com/lumosimmo/market-relayer-go/internal/store"
)

type Harness struct {
	store        *store.DB
	market       config.MarketConfig
	metadata     store.Metadata
	tracker      *sessions.Tracker
	nextSequence uint64
}

type CycleInput struct {
	RecordID string
	At       time.Time
	Quotes   []feeds.Quote
	Payload  json.RawMessage
}

type PublishedCycleInput struct {
	RecordID string
	At       time.Time
	Quotes   []feeds.Quote
}

func NewHarness(dsn string, market config.MarketConfig, metadata store.Metadata) (*Harness, error) {
	storeCfg := config.DefaultStoreConfig(dsn)
	if _, err := store.Migrate(context.Background(), storeCfg); err != nil {
		return nil, err
	}

	storeDB, err := store.OpenConfigContext(context.Background(), storeCfg)
	if err != nil {
		return nil, err
	}

	safeSequence, err := storeDB.SafeSequence(context.Background(), market.Symbol)
	if err != nil {
		_ = storeDB.Close()
		return nil, err
	}

	tracker, err := sessions.NewTracker(market, sessions.TrackerOptions{})
	if err != nil {
		_ = storeDB.Close()
		return nil, err
	}

	return &Harness{
		store:        storeDB,
		market:       market,
		metadata:     metadata,
		tracker:      tracker,
		nextSequence: safeSequence + 1,
	}, nil
}

func (h *Harness) Close() error {
	return h.store.Close()
}

func (h *Harness) Store() *store.DB {
	return h.store
}

func (h *Harness) Market() config.MarketConfig {
	return h.market
}

func (h *Harness) Metadata() store.Metadata {
	return h.metadata
}

func (h *Harness) RestoreState(at time.Time) (pricing.State, error) {
	state, err := h.store.LatestState(context.Background(), h.market.Symbol)
	if err != nil {
		return pricing.State{}, err
	}
	return h.tracker.RestoreState(at, state), nil
}

func (h *Harness) Recover(ctx context.Context, mode operator.RecoveryMode, recordID string, sink operator.RecoverySink) (operator.RecoveryReport, error) {
	return operator.RunRecovery(ctx, operator.RecoveryOptions{
		Mode:     mode,
		Market:   h.market,
		Metadata: h.metadata,
		Store:    h.store,
		Sink:     sink,
		RecordID: recordID,
	})
}

func (h *Harness) PendingPublications() ([]store.PreparedPublication, error) {
	return h.store.PendingPublications(context.Background(), h.market.Symbol)
}

func (h *Harness) MarkPublicationSent(sequence uint64, sentAt time.Time) error {
	return h.store.MarkPublicationSent(context.Background(), h.market.Symbol, sequence, sentAt)
}

func (h *Harness) AcknowledgePublication(sequence uint64, ackedAt time.Time) error {
	return h.store.AcknowledgePublication(context.Background(), h.market.Symbol, sequence, ackedAt)
}

func (h *Harness) Snapshot() (store.Snapshot, error) {
	return h.store.LatestSnapshot(context.Background(), h.market.Symbol)
}

func (h *Harness) SafeSequence() (uint64, error) {
	return h.store.SafeSequence(context.Background(), h.market.Symbol)
}

func (h *Harness) RunCycle(input CycleInput) (store.CycleRecord, error) {
	computed, err := h.computeCycle(input.RecordID, input.At, input.Quotes)
	if err != nil {
		return store.CycleRecord{}, err
	}

	record := h.baseRecord(input.RecordID, input.Quotes, computed)
	if len(input.Payload) > 0 {
		publication, err := h.preparePublication(input.At, input.Payload)
		if err != nil {
			return store.CycleRecord{}, err
		}
		record.PreparedPublication = &publication
		h.nextSequence++
	}

	if err := h.store.CommitCycle(context.Background(), record); err != nil {
		return store.CycleRecord{}, err
	}

	return record, nil
}

func (h *Harness) RunPublishedCycle(input PublishedCycleInput) (store.CycleRecord, error) {
	computed, err := h.computeCycle(input.RecordID, input.At, input.Quotes)
	if err != nil {
		return store.CycleRecord{}, err
	}

	publishability := h.availablePublishability(input.At)
	stableHash, publication, err := relayer.PreparePublication(h.metadata, computed, publishability, h.nextSequence, input.At)
	if err != nil {
		return store.CycleRecord{}, err
	}

	record := h.baseRecord(input.RecordID, input.Quotes, computed)
	record.Publishability = publishability
	record.Publishability.LastStableHash = stableHash
	record.Publishability.PendingSequence = publication.Sequence
	record.Publishability.LastIdempotencyKey = publication.IdempotencyKey
	record.Publishability.LastPayloadHash = publication.PayloadHash
	record.PreparedPublication = &publication

	if err := h.store.CommitCycle(context.Background(), record); err != nil {
		return store.CycleRecord{}, err
	}

	h.nextSequence++
	return record, nil
}

func (h *Harness) computeCycle(recordID string, at time.Time, quotes []feeds.Quote) (pricing.Result, error) {
	if err := validateCycleInput(recordID, at); err != nil {
		return pricing.Result{}, err
	}

	prior, err := h.RestoreState(at)
	if err != nil {
		return pricing.Result{}, err
	}

	computed, err := pricing.Compute(pricing.Inputs{
		Market: h.market,
		Quotes: quotes,
		Prior:  prior,
		At:     at,
	})
	if err != nil {
		return pricing.Result{}, err
	}

	return h.tracker.Apply(computed, prior, sessions.CorrelationIDs{CycleID: recordID}), nil
}

func (h *Harness) baseRecord(recordID string, quotes []feeds.Quote, computed pricing.Result) store.CycleRecord {
	return store.CycleRecord{
		RecordID:  recordID,
		Market:    h.market.Symbol,
		RawQuotes: quotes,
		Computed:  computed,
		Metadata:  h.metadata,
	}
}

func (h *Harness) availablePublishability(at time.Time) store.PublishabilityState {
	return store.PublishabilityState{
		Market:    h.market.Symbol,
		Status:    status.StatusAvailable,
		Reason:    status.ReasonNone,
		UpdatedAt: at.UTC(),
	}
}

func validateCycleInput(recordID string, at time.Time) error {
	if strings.TrimSpace(recordID) == "" {
		return fmt.Errorf("testutil: record id is required")
	}
	if at.IsZero() {
		return fmt.Errorf("testutil: cycle time is required")
	}
	return nil
}

func (h *Harness) preparePublication(preparedAt time.Time, payload json.RawMessage) (store.PreparedPublication, error) {
	var compactPayload bytes.Buffer
	if err := json.Compact(&compactPayload, payload); err != nil {
		return store.PreparedPublication{}, fmt.Errorf("testutil: compact payload: %w", err)
	}

	payloadHash := sha256.Sum256(compactPayload.Bytes())
	hash := hex.EncodeToString(payloadHash[:])
	sequence := h.nextSequence
	idempotencyKey := fmt.Sprintf("%s-%d-%s", h.market.Symbol, sequence, hash[:8])
	envelope, err := json.Marshal(struct {
		EnvelopeVersion string          `json:"envelope_version"`
		Market          string          `json:"market"`
		Sequence        uint64          `json:"sequence"`
		IdempotencyKey  string          `json:"idempotency_key"`
		PayloadHash     string          `json:"payload_hash"`
		Payload         json.RawMessage `json:"payload"`
	}{
		EnvelopeVersion: relayer.EnvelopeVersion,
		Market:          h.market.Symbol,
		Sequence:        sequence,
		IdempotencyKey:  idempotencyKey,
		PayloadHash:     hash,
		Payload:         json.RawMessage(compactPayload.Bytes()),
	})
	if err != nil {
		return store.PreparedPublication{}, fmt.Errorf("testutil: marshal prepared envelope: %w", err)
	}

	return store.PreparedPublication{
		Market:          h.market.Symbol,
		Sequence:        sequence,
		State:           store.PublicationStatePrepared,
		PreparedAt:      preparedAt.UTC(),
		IdempotencyKey:  idempotencyKey,
		PayloadHash:     hash,
		EnvelopeVersion: relayer.EnvelopeVersion,
		Envelope:        envelope,
	}, nil
}
