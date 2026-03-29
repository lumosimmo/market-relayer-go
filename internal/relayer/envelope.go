package relayer

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/lumosimmo/market-relayer-go/internal/feeds"
	"github.com/lumosimmo/market-relayer-go/internal/pricing"
	"github.com/lumosimmo/market-relayer-go/internal/status"
	"github.com/lumosimmo/market-relayer-go/internal/store"
)

const EnvelopeVersion = "1"

type Envelope struct {
	EnvelopeVersion         string    `json:"envelope_version"`
	Market                  string    `json:"market"`
	Sequence                uint64    `json:"sequence"`
	IdempotencyKey          string    `json:"idempotency_key"`
	ConfigDigest            string    `json:"config_digest"`
	PricingAlgorithmVersion string    `json:"pricing_algorithm_version"`
	PayloadHash             string    `json:"payload_hash"`
	PreparedAt              time.Time `json:"prepared_at"`
	Payload                 Payload   `json:"payload"`
}

type Payload struct {
	Session         SessionPayload   `json:"session"`
	Health          HealthPayload    `json:"health"`
	Selection       SelectionPayload `json:"selection"`
	Timestamps      TimestampPayload `json:"timestamps"`
	External        PricePayload     `json:"external"`
	Oracle          PricePayload     `json:"oracle"`
	OraclePlusBasis PricePayload     `json:"oracle_plus_basis"`
}

type SessionPayload struct {
	State  status.SessionState `json:"state"`
	Reason status.Reason       `json:"reason"`
}

type HealthPayload struct {
	Publishability status.Condition `json:"publishability"`
}

type SelectionPayload struct {
	Rule            pricing.SelectionRule    `json:"rule"`
	SelectedSources []string                 `json:"selected_sources,omitempty"`
	BasisKind       feeds.FreshnessBasisKind `json:"basis_kind,omitempty"`
}

type TimestampPayload struct {
	ComputedAt       time.Time `json:"computed_at"`
	FreshnessBasisAt time.Time `json:"freshness_basis_at,omitempty"`
}

type PricePayload struct {
	Status status.Status `json:"status"`
	Reason status.Reason `json:"reason"`
	Value  *int64        `json:"value,omitempty"`
	Scale  int32         `json:"scale"`
}

type stablePayload struct {
	Session         SessionPayload   `json:"session"`
	Health          HealthPayload    `json:"health"`
	Selection       SelectionPayload `json:"selection"`
	External        PricePayload     `json:"external"`
	Oracle          PricePayload     `json:"oracle"`
	OraclePlusBasis PricePayload     `json:"oracle_plus_basis"`
}

func PreparePublication(
	metadata store.Metadata,
	result pricing.Result,
	publishability store.PublishabilityState,
	sequence uint64,
	preparedAt time.Time,
) (string, store.PreparedPublication, error) {
	if sequence == 0 {
		return "", store.PreparedPublication{}, fmt.Errorf("relayer: sequence is required")
	}
	payload := buildPayload(result, publishability)
	stableHash, err := hashJSON(stablePayload{
		Session:         payload.Session,
		Health:          payload.Health,
		Selection:       payload.Selection,
		External:        payload.External,
		Oracle:          payload.Oracle,
		OraclePlusBasis: payload.OraclePlusBasis,
	})
	if err != nil {
		return "", store.PreparedPublication{}, err
	}
	payloadHash, err := hashJSON(payload)
	if err != nil {
		return "", store.PreparedPublication{}, err
	}

	idempotencyKey := publicationIdentity(sequence, result.Market, payloadHash)
	envelope := Envelope{
		EnvelopeVersion:         EnvelopeVersion,
		Market:                  result.Market,
		Sequence:                sequence,
		IdempotencyKey:          idempotencyKey,
		ConfigDigest:            metadata.ConfigDigest,
		PricingAlgorithmVersion: metadata.PricingAlgorithmVersion,
		PayloadHash:             payloadHash,
		PreparedAt:              preparedAt.UTC(),
		Payload:                 payload,
	}
	envelopeJSON, err := marshalJSON(envelope)
	if err != nil {
		return "", store.PreparedPublication{}, err
	}

	return stableHash, store.PreparedPublication{
		Market:          result.Market,
		Sequence:        sequence,
		State:           store.PublicationStatePrepared,
		PreparedAt:      preparedAt.UTC(),
		IdempotencyKey:  idempotencyKey,
		PayloadHash:     payloadHash,
		EnvelopeVersion: EnvelopeVersion,
		Envelope:        envelopeJSON,
	}, nil
}

func buildPayload(result pricing.Result, publishability store.PublishabilityState) Payload {
	return Payload{
		Session: SessionPayload{
			State:  result.SessionState,
			Reason: result.SessionReason,
		},
		Health: HealthPayload{
			Publishability: status.NewCondition(publishability.Status, publishability.Reason),
		},
		Selection: SelectionPayload{
			Rule:            result.ExternalSelection.Rule,
			SelectedSources: append([]string(nil), result.ExternalSelection.SelectedSources...),
			BasisKind:       result.State.FreshnessBasisKind,
		},
		Timestamps: TimestampPayload{
			ComputedAt:       result.ComputedAt.UTC(),
			FreshnessBasisAt: result.State.FreshnessBasisAt.UTC(),
		},
		External:        buildPricePayload(result.External),
		Oracle:          buildPricePayload(result.Oracle),
		OraclePlusBasis: buildPricePayload(result.OraclePlusBasis),
	}
}

func buildPricePayload(price pricing.Price) PricePayload {
	return PricePayload{
		Status: price.Status,
		Reason: price.Reason,
		Value:  clonePriceValue(price),
		Scale:  price.Scale,
	}
}

func ValidatePendingReplay(publication store.PreparedPublication, metadata store.Metadata) error {
	if publication.EnvelopeVersion != EnvelopeVersion {
		return fmt.Errorf("relayer: unsupported pending envelope version %q", publication.EnvelopeVersion)
	}

	var envelope Envelope
	if err := json.Unmarshal(publication.Envelope, &envelope); err != nil {
		return fmt.Errorf("relayer: decode pending envelope: %w", err)
	}
	if mismatches := pendingReplayIdentityMismatches(publication, envelope); len(mismatches) > 0 {
		return fmt.Errorf("relayer: pending publication identity mismatch: %s", strings.Join(mismatches, ", "))
	}

	mismatches := pendingReplayMismatches(envelope, metadata)
	if len(mismatches) > 0 {
		return fmt.Errorf("relayer: pending publication metadata mismatch: %s", strings.Join(mismatches, ", "))
	}
	return nil
}

func pendingReplayIdentityMismatches(publication store.PreparedPublication, envelope Envelope) []string {
	mismatches := make([]string, 0, 5)
	checks := []struct {
		name string
		got  string
		want string
	}{
		{name: "market", got: envelope.Market, want: publication.Market},
		{name: "idempotency_key", got: envelope.IdempotencyKey, want: publication.IdempotencyKey},
		{name: "payload_hash", got: envelope.PayloadHash, want: publication.PayloadHash},
		{name: "envelope_version", got: envelope.EnvelopeVersion, want: publication.EnvelopeVersion},
	}
	for _, check := range checks {
		if check.got != check.want {
			mismatches = append(mismatches, check.name)
		}
	}
	if envelope.Sequence != publication.Sequence {
		mismatches = append(mismatches, "sequence")
	}
	return mismatches
}

func pendingReplayMismatches(envelope Envelope, metadata store.Metadata) []string {
	mismatches := make([]string, 0, 3)
	checks := []struct {
		name string
		got  string
		want string
	}{
		{name: "config_digest", got: envelope.ConfigDigest, want: metadata.ConfigDigest},
		{name: "pricing_algorithm_version", got: envelope.PricingAlgorithmVersion, want: metadata.PricingAlgorithmVersion},
		{name: "envelope_version", got: envelope.EnvelopeVersion, want: metadata.EnvelopeVersion},
	}

	for _, check := range checks {
		if check.got != check.want {
			mismatches = append(mismatches, check.name)
		}
	}
	return mismatches
}

func clonePriceValue(price pricing.Price) *int64 {
	if price.Value == nil {
		return nil
	}
	cloned := price.Value.Int
	return &cloned
}

func publicationIdentity(sequence uint64, market string, payloadHash string) string {
	identity := EnvelopeVersion + "|" + market + "|" + strconv.FormatUint(sequence, 10) + "|" + payloadHash
	sum := sha256.Sum256([]byte(identity))
	return hex.EncodeToString(sum[:])
}

func hashJSON(value any) (string, error) {
	raw, err := marshalJSON(value)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

func marshalJSON(value any) ([]byte, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("relayer: marshal json: %w", err)
	}
	return raw, nil
}
