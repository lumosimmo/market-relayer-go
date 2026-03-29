package operator

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/lumosimmo/market-relayer-go/internal/config"
	"github.com/lumosimmo/market-relayer-go/internal/fixedpoint"
	"github.com/lumosimmo/market-relayer-go/internal/pricing"
	"github.com/lumosimmo/market-relayer-go/internal/relayer"
	"github.com/lumosimmo/market-relayer-go/internal/sessions"
	"github.com/lumosimmo/market-relayer-go/internal/status"
	"github.com/lumosimmo/market-relayer-go/internal/store"
)

type RecoveryMode string

const (
	ModeExact         RecoveryMode = "exact"
	ModeAudit         RecoveryMode = "audit"
	ModeResumePending RecoveryMode = "resume-pending"
)

type RecoveryStore interface {
	VerifyExact(context.Context, store.ExactQuery) (store.CycleRecord, error)
	Audit(context.Context, store.ExactQuery) (store.AuditResult, error)
	PreviousCycle(context.Context, string, string) (store.CycleRecord, bool, error)
	PendingPublications(context.Context, string) ([]store.PreparedPublication, error)
	MarkPublicationSent(context.Context, string, uint64, time.Time) error
	AcknowledgePublication(context.Context, string, uint64, time.Time) error
	SafeSequence(context.Context, string) (uint64, error)
	LatestPublishability(context.Context, string) (store.PublishabilityState, error)
	SavePublishability(context.Context, store.PublishabilityState) error
}

type RecoverySink interface {
	Publish(context.Context, store.PreparedPublication) (relayer.Ack, error)
}

type RecoveryOptions struct {
	Mode     RecoveryMode
	Market   config.MarketConfig
	Metadata store.Metadata
	Store    RecoveryStore
	Sink     RecoverySink
	RecordID string
}

type RecoveryReport struct {
	Mode              RecoveryMode      `json:"mode"`
	Record            store.CycleRecord `json:"record,omitempty"`
	MetadataMatch     bool              `json:"metadata_match,omitempty"`
	PricingMatch      bool              `json:"pricing_match,omitempty"`
	Mismatches        []string          `json:"mismatches,omitempty"`
	ReplayedSequences []uint64          `json:"replayed_sequences,omitempty"`
	NextSequence      uint64            `json:"next_sequence,omitempty"`
}

func RunRecovery(ctx context.Context, options RecoveryOptions) (RecoveryReport, error) {
	if err := validateRecoveryOptions(options); err != nil {
		return RecoveryReport{}, err
	}

	switch options.Mode {
	case ModeExact:
		return runExact(ctx, options)
	case ModeAudit:
		return runAudit(ctx, options)
	case ModeResumePending:
		return runResumePending(ctx, options)
	default:
		return RecoveryReport{}, fmt.Errorf("operator: unsupported recovery mode %q", options.Mode)
	}
}

func runExact(ctx context.Context, options RecoveryOptions) (RecoveryReport, error) {
	query, err := exactQuery(options, ModeExact)
	if err != nil {
		return RecoveryReport{}, err
	}

	record, err := options.Store.VerifyExact(ctx, query)
	if err != nil {
		return RecoveryReport{}, err
	}

	return RecoveryReport{
		Mode:          ModeExact,
		Record:        record,
		MetadataMatch: true,
		PricingMatch:  true,
	}, nil
}

func runAudit(ctx context.Context, options RecoveryOptions) (RecoveryReport, error) {
	query, err := exactQuery(options, ModeAudit)
	if err != nil {
		return RecoveryReport{}, err
	}

	audit, err := options.Store.Audit(ctx, query)
	if err != nil {
		return RecoveryReport{}, err
	}

	priorState, err := loadPriorState(ctx, options, audit.Record)
	if err != nil {
		return RecoveryReport{}, err
	}

	recomputed, err := recomputeRecord(options, audit.Record, priorState)
	if err != nil {
		return RecoveryReport{}, err
	}

	mismatches := append([]string(nil), audit.Mismatches...)
	if !sameResult(audit.Record.Computed, recomputed) {
		mismatches = append(mismatches, "computed_result")
	}
	if audit.MetadataMatch && audit.Record.PreparedPublication != nil {
		_, prepared, err := relayer.PreparePublication(options.Metadata, recomputed, audit.Record.Publishability, audit.Record.PreparedPublication.Sequence, audit.Record.PreparedPublication.PreparedAt)
		if err != nil {
			return RecoveryReport{}, err
		}
		if !bytes.Equal(prepared.Envelope, audit.Record.PreparedPublication.Envelope) {
			mismatches = append(mismatches, "prepared_envelope")
		}
	}

	return RecoveryReport{
		Mode:          ModeAudit,
		Record:        audit.Record,
		MetadataMatch: audit.MetadataMatch,
		PricingMatch:  len(mismatches) == 0,
		Mismatches:    mismatches,
	}, nil
}

func runResumePending(ctx context.Context, options RecoveryOptions) (RecoveryReport, error) {
	if options.Sink == nil {
		return RecoveryReport{}, errors.New("operator: sink is required for resume-pending mode")
	}

	pending, err := options.Store.PendingPublications(ctx, options.Market.Symbol)
	if err != nil {
		return RecoveryReport{}, err
	}

	publishability, err := options.Store.LatestPublishability(ctx, options.Market.Symbol)
	if err != nil {
		return RecoveryReport{}, err
	}
	if publishability == (store.PublishabilityState{}) {
		publishability = defaultPublishability(options.Market.Symbol, time.Now().UTC())
	}

	report := RecoveryReport{
		Mode:              ModeResumePending,
		ReplayedSequences: make([]uint64, 0, len(pending)),
	}

	for _, publication := range pending {
		if err := relayer.ValidatePendingReplay(publication, options.Metadata); err != nil {
			return RecoveryReport{}, err
		}

		sentAt, err := replayPendingPublication(ctx, options, publication)
		if err != nil {
			return RecoveryReport{}, err
		}

		publishability.Status = status.StatusAvailable
		publishability.Reason = status.ReasonNone
		publishability.PendingSequence = 0
		publishability.LastSequence = publication.Sequence
		publishability.LastIdempotencyKey = publication.IdempotencyKey
		publishability.LastPayloadHash = publication.PayloadHash
		publishability.UpdatedAt = sentAt
		if err := options.Store.SavePublishability(ctx, publishability); err != nil {
			return RecoveryReport{}, err
		}

		report.ReplayedSequences = append(report.ReplayedSequences, publication.Sequence)
	}

	safeSequence, err := options.Store.SafeSequence(ctx, options.Market.Symbol)
	if err != nil {
		return RecoveryReport{}, err
	}
	report.NextSequence = safeSequence + 1
	return report, nil
}

func loadPriorState(ctx context.Context, options RecoveryOptions, record store.CycleRecord) (pricing.State, error) {
	previous, found, err := options.Store.PreviousCycle(ctx, options.Market.Symbol, record.RecordID)
	if err != nil {
		return pricing.State{}, err
	}
	if !found {
		return pricing.State{}, nil
	}
	return sessions.RestoreState(record.Computed.ComputedAt, previous.Computed.State), nil
}

func validateRecoveryOptions(options RecoveryOptions) error {
	if options.Store == nil {
		return errors.New("operator: store is required")
	}
	if strings.TrimSpace(options.Market.Symbol) == "" {
		return errors.New("operator: market is required")
	}
	return nil
}

func exactQuery(options RecoveryOptions, mode RecoveryMode) (store.ExactQuery, error) {
	if strings.TrimSpace(options.RecordID) == "" {
		return store.ExactQuery{}, fmt.Errorf("operator: record id is required for %s mode", mode)
	}

	return store.ExactQuery{
		Market:   options.Market.Symbol,
		RecordID: options.RecordID,
		Metadata: options.Metadata,
	}, nil
}

func recomputeRecord(options RecoveryOptions, record store.CycleRecord, prior pricing.State) (pricing.Result, error) {
	recomputed, err := pricing.Compute(pricing.Inputs{
		Market:         options.Market,
		Quotes:         record.RawQuotes,
		Book:           record.RawBook,
		Prior:          prior,
		At:             record.Computed.ComputedAt,
		ExternalClosed: recoveryExternalClosed(record),
	})
	if err != nil {
		return pricing.Result{}, err
	}
	return sessions.Apply(recomputed, prior, recoveryWindow(record)), nil
}

func recoveryExternalClosed(record store.CycleRecord) bool {
	return record.Computed.ExternalSelection.Rule == pricing.SelectionRuleExternalClosed
}

func recoveryWindow(record store.CycleRecord) sessions.Window {
	if recoveryExternalClosed(record) {
		return sessions.ExternalStaleWindow(status.ReasonSessionClosed)
	}
	return sessions.OpenWindow()
}

func defaultPublishability(market string, updatedAt time.Time) store.PublishabilityState {
	return store.PublishabilityState{
		Market:    market,
		Status:    status.StatusAvailable,
		Reason:    status.ReasonNone,
		UpdatedAt: updatedAt,
	}
}

func replayPendingPublication(ctx context.Context, options RecoveryOptions, publication store.PreparedPublication) (time.Time, error) {
	ack, err := options.Sink.Publish(ctx, publication)
	if err != nil {
		return time.Time{}, err
	}
	ack, err = relayer.ValidateAck(ack, publication)
	if err != nil {
		return time.Time{}, err
	}

	ackedAt := normalizedAckTime(ack)
	if err := options.Store.MarkPublicationSent(ctx, publication.Market, publication.Sequence, ackedAt); err != nil {
		return time.Time{}, err
	}
	if err := options.Store.AcknowledgePublication(ctx, publication.Market, publication.Sequence, ackedAt); err != nil {
		return time.Time{}, err
	}
	return ackedAt, nil
}

func normalizedAckTime(ack relayer.Ack) time.Time {
	if ack.AckedAt.IsZero() {
		return time.Now().UTC()
	}
	return ack.AckedAt.UTC()
}

func sameResult(left, right pricing.Result) bool {
	return left.Market == right.Market &&
		sameTime(left.ComputedAt, right.ComputedAt) &&
		left.PublishScale == right.PublishScale &&
		slices.EqualFunc(left.Inputs, right.Inputs, sameNormalizedInput) &&
		sameSelection(left.ExternalSelection, right.ExternalSelection) &&
		samePrice(left.External, right.External) &&
		samePrice(left.Oracle, right.Oracle) &&
		left.OracleClamped == right.OracleClamped &&
		samePrice(left.OraclePlusBasis, right.OraclePlusBasis) &&
		left.SessionState == right.SessionState &&
		left.SessionReason == right.SessionReason &&
		sameState(left.State, right.State)
}

func sameNormalizedInput(left, right pricing.NormalizedInput) bool {
	return left.Source == right.Source &&
		left.SourceOrder == right.SourceOrder &&
		left.Mid == right.Mid &&
		left.FreshnessBasisKind == right.FreshnessBasisKind &&
		sameTime(left.FreshnessBasisAt, right.FreshnessBasisAt)
}

func sameSelection(left, right pricing.ExternalSelection) bool {
	return left.Rule == right.Rule &&
		slices.Equal(left.SelectedSources, right.SelectedSources) &&
		left.BasisSource == right.BasisSource &&
		left.BasisKind == right.BasisKind &&
		sameTime(left.BasisAt, right.BasisAt)
}

func samePrice(left, right pricing.Price) bool {
	return left.Status == right.Status &&
		left.Reason == right.Reason &&
		left.Scale == right.Scale &&
		sameFixedpointValue(left.Value, right.Value)
}

func sameState(left, right pricing.State) bool {
	return sameFixedpointValue(left.LastExternal, right.LastExternal) &&
		sameFixedpointValue(left.LastOracle, right.LastOracle) &&
		sameFixedpointValue(left.LastOraclePlusBasis, right.LastOraclePlusBasis) &&
		sameFixedpointValue(left.LastBasisEMA, right.LastBasisEMA) &&
		left.FreshnessBasisKind == right.FreshnessBasisKind &&
		sameTime(left.FreshnessBasisAt, right.FreshnessBasisAt) &&
		sameTime(left.LastExternalAt, right.LastExternalAt) &&
		sameTime(left.LastOracleAt, right.LastOracleAt) &&
		sameTime(left.LastOraclePlusBasisAt, right.LastOraclePlusBasisAt) &&
		sameTime(left.LastBasisAt, right.LastBasisAt) &&
		sameTime(left.OracleFallbackExpiresAt, right.OracleFallbackExpiresAt) &&
		sameTime(left.OraclePlusBasisFallbackExpiresAt, right.OraclePlusBasisFallbackExpiresAt)
}

func sameFixedpointValue(left, right *fixedpoint.Value) bool {
	switch {
	case left == nil || right == nil:
		return left == nil && right == nil
	default:
		return *left == *right
	}
}

func sameTime(left, right time.Time) bool {
	if left.IsZero() || right.IsZero() {
		return left.IsZero() && right.IsZero()
	}
	return left.Equal(right)
}
