package relayer

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/lumosimmo/market-relayer-go/internal/feeds"
	"github.com/lumosimmo/market-relayer-go/internal/pricing"
	"github.com/lumosimmo/market-relayer-go/internal/status"
	"github.com/lumosimmo/market-relayer-go/internal/store"
)

func TestPreparePublicationSeparatesStableAndTimedHashes(t *testing.T) {
	t.Parallel()

	at := time.Date(2026, 3, 27, 9, 0, 0, 0, time.UTC)
	result := envelopeResult(t, at)
	publishability := store.PublishabilityState{
		Market:    testLoopMarket().Symbol,
		Status:    status.StatusAvailable,
		Reason:    status.ReasonNone,
		UpdatedAt: at,
	}

	stable1, publication1, err := PreparePublication(testLoopMetadata(), result, publishability, 1, at)
	if err != nil {
		t.Fatalf("PreparePublication(first) error = %v", err)
	}

	result.ComputedAt = at.Add(15 * time.Second)
	result.ExternalSelection.BasisAt = at.Add(30 * time.Second)

	stable2, publication2, err := PreparePublication(testLoopMetadata(), result, publishability, 2, at.Add(time.Minute))
	if err != nil {
		t.Fatalf("PreparePublication(second) error = %v", err)
	}

	if stable1 != stable2 {
		t.Fatalf("stable hash changed across timestamp-only mutation: %q != %q", stable1, stable2)
	}
	if publication1.PayloadHash == publication2.PayloadHash {
		t.Fatalf("payload hash = %q for both publications, want different hashes when timestamps differ", publication1.PayloadHash)
	}
	if publication1.IdempotencyKey == publication2.IdempotencyKey {
		t.Fatalf("idempotency key = %q for both publications, want sequence-specific identity", publication1.IdempotencyKey)
	}
}

func TestPreparePublicationAndReplayValidationErrors(t *testing.T) {
	t.Parallel()

	at := time.Date(2026, 3, 27, 10, 0, 0, 0, time.UTC)
	result := envelopeResult(t, at)
	publishability := store.PublishabilityState{
		Market:    testLoopMarket().Symbol,
		Status:    status.StatusAvailable,
		Reason:    status.ReasonNone,
		UpdatedAt: at,
	}

	if _, _, err := PreparePublication(testLoopMetadata(), result, publishability, 0, at); err == nil {
		t.Fatal("PreparePublication(sequence=0) error = nil, want validation error")
	}

	publication := pendingPublicationForMetadataTest(t, testLoopMetadata())
	publication.Envelope = []byte("not-json")
	if err := ValidatePendingReplay(publication, testLoopMetadata()); err == nil {
		t.Fatal("ValidatePendingReplay(invalid json) error = nil, want decode failure")
	}

	mismatched := pendingPublicationForMetadataTest(t, testLoopMetadata())
	mismatched.Envelope = rewriteLoopEnvelopeMetadata(t, mismatched.Envelope, "config-digest-stale", "9")
	err := ValidatePendingReplay(mismatched, testLoopMetadata())
	if err == nil {
		t.Fatal("ValidatePendingReplay(mismatched metadata) error = nil, want mismatch")
	}
	if got := err.Error(); !strings.Contains(got, "config_digest") || !strings.Contains(got, "pricing_algorithm_version") {
		t.Fatalf("ValidatePendingReplay(mismatched metadata) error = %q, want mismatch field names", got)
	}

	unsupported := pendingPublicationForMetadataTest(t, testLoopMetadata())
	unsupported.EnvelopeVersion = "9"
	if err := ValidatePendingReplay(unsupported, testLoopMetadata()); err == nil {
		t.Fatal("ValidatePendingReplay(unsupported version) error = nil, want validation error")
	}

	identityMismatch := pendingPublicationForMetadataTest(t, testLoopMetadata())
	identityMismatch.PayloadHash = "row-payload-hash-mismatch"
	err = ValidatePendingReplay(identityMismatch, testLoopMetadata())
	if err == nil {
		t.Fatal("ValidatePendingReplay(identity mismatch) error = nil, want mismatch")
	}
	if got := err.Error(); !strings.Contains(got, "pending publication identity mismatch") || !strings.Contains(got, "payload_hash") {
		t.Fatalf("ValidatePendingReplay(identity mismatch) error = %q, want payload hash identity mismatch", got)
	}
}

func TestEnvelopeHashAndMarshalRejectUnsupportedValues(t *testing.T) {
	t.Parallel()

	if _, err := hashJSON(make(chan int)); err == nil {
		t.Fatal("hashJSON(channel) error = nil, want marshal failure")
	}
	if _, err := marshalJSON(make(chan int)); err == nil {
		t.Fatal("marshalJSON(channel) error = nil, want marshal failure")
	}
}

func TestMarshalJSONMatchesEncodingJSONMarshal(t *testing.T) {
	t.Parallel()

	value := Envelope{
		EnvelopeVersion:         EnvelopeVersion,
		Market:                  "ETHUSD",
		Sequence:                7,
		IdempotencyKey:          "key-7",
		ConfigDigest:            "config-digest",
		PricingAlgorithmVersion: pricing.AlgorithmVersion,
		PayloadHash:             "payload-hash",
		PreparedAt:              time.Date(2026, 3, 27, 9, 0, 0, 0, time.UTC),
		Payload: Payload{
			Selection: SelectionPayload{
				Rule:            pricing.SelectionRuleSingleSource,
				SelectedSources: []string{"coinbase"},
				BasisKind:       feeds.FreshnessBasisSourceTimestamp,
			},
		},
	}

	got, err := marshalJSON(value)
	if err != nil {
		t.Fatalf("marshalJSON() error = %v", err)
	}
	want, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("marshalJSON() = %s, want %s", got, want)
	}
}

func envelopeResult(t *testing.T, at time.Time) pricing.Result {
	t.Helper()

	result, err := pricing.Compute(pricing.Inputs{
		Market: testLoopMarket(),
		Quotes: []feeds.Quote{
			relayerQuote(t, "coinbase", "ETHUSD", "1982.75", at),
		},
		At: at,
	})
	if err != nil {
		t.Fatalf("pricing.Compute() error = %v", err)
	}
	return result
}
