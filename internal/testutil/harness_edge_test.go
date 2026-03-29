package testutil

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"
	"time"

	"github.com/lumosimmo/market-relayer-go/internal/feeds"
	"github.com/lumosimmo/market-relayer-go/internal/operator"
	"github.com/lumosimmo/market-relayer-go/internal/pricing"
	"github.com/lumosimmo/market-relayer-go/internal/relayer"
	"github.com/lumosimmo/market-relayer-go/internal/store"
	"github.com/lumosimmo/market-relayer-go/internal/testdb"
)

func TestNewHarnessAccessorsAndValidationEdges(t *testing.T) {
	t.Parallel()

	storeDSN := testdb.DSN(t)
	market := harnessMarket(t)
	metadata := store.Metadata{
		ConfigDigest:            "config-digest",
		PricingAlgorithmVersion: pricing.AlgorithmVersion,
		SchemaVersion:           store.SchemaVersion,
		EnvelopeVersion:         relayer.EnvelopeVersion,
	}

	harness, err := NewHarness(storeDSN, market, metadata)
	if err != nil {
		t.Fatalf("NewHarness() error = %v", err)
	}
	t.Cleanup(func() {
		if closeErr := harness.Close(); closeErr != nil {
			t.Fatalf("harness.Close() error = %v", closeErr)
		}
	})

	if harness.Store() == nil {
		t.Fatal("Store() = nil, want store handle")
	}
	if got := harness.Market(); got.Symbol != market.Symbol {
		t.Fatalf("Market() = %+v, want %+v", got, market)
	}
	if got := harness.Metadata(); got != metadata {
		t.Fatalf("Metadata() = %+v, want %+v", got, metadata)
	}

	state, err := harness.RestoreState(time.Date(2026, 3, 27, 17, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("RestoreState() error = %v", err)
	}
	if state != (pricing.State{}) {
		t.Fatalf("RestoreState() = %+v, want zero state for empty store", state)
	}

	badMarket := harnessMarket(t)
	badMarket.SessionMode = "scheduled"
	if _, err := NewHarness(testdb.DSN(t), badMarket, metadata); err == nil {
		t.Fatal("NewHarness(invalid market) error = nil, want tracker failure")
	}
}

func TestHarnessCycleValidationAndPublicationPreparationEdges(t *testing.T) {
	t.Parallel()

	storeDSN := testdb.DSN(t)
	market := harnessMarket(t)
	metadata := store.Metadata{
		ConfigDigest:            "config-digest",
		PricingAlgorithmVersion: pricing.AlgorithmVersion,
		SchemaVersion:           store.SchemaVersion,
		EnvelopeVersion:         relayer.EnvelopeVersion,
	}
	harness, err := NewHarness(storeDSN, market, metadata)
	if err != nil {
		t.Fatalf("NewHarness() error = %v", err)
	}
	t.Cleanup(func() {
		if closeErr := harness.Close(); closeErr != nil {
			t.Fatalf("harness.Close() error = %v", closeErr)
		}
	})

	now := time.Date(2026, 3, 27, 18, 0, 0, 0, time.UTC)

	if _, err := harness.RunCycle(CycleInput{At: now}); err == nil {
		t.Fatal("RunCycle(missing record id) error = nil, want validation failure")
	}
	if _, err := harness.RunCycle(CycleInput{RecordID: "cycle-1"}); err == nil {
		t.Fatal("RunCycle(missing time) error = nil, want validation failure")
	}
	if _, err := harness.RunCycle(CycleInput{
		RecordID: "cycle-1",
		At:       now,
		Payload:  []byte(`{"bad"`),
	}); err == nil {
		t.Fatal("RunCycle(invalid payload) error = nil, want compact failure")
	}
	if _, err := harness.RunCycle(CycleInput{
		RecordID: "cycle-2",
		At:       now,
		Quotes: []feeds.Quote{
			harnessQuote(t, "coinbase", "BTCUSD", "65941.00", now, feeds.FreshnessBasisSourceTimestamp),
		},
	}); err == nil {
		t.Fatal("RunCycle(wrong symbol) error = nil, want pricing failure")
	}

	record, err := harness.RunPublishedCycle(PublishedCycleInput{
		RecordID: "cycle-published-1",
		At:       now,
		Quotes: []feeds.Quote{
			harnessQuote(t, "coinbase", "ETHUSD", "1981.75", now, feeds.FreshnessBasisSourceTimestamp),
		},
	})
	if err != nil {
		t.Fatalf("RunPublishedCycle(first) error = %v", err)
	}
	if record.PreparedPublication == nil || record.PreparedPublication.Sequence != 1 {
		t.Fatalf("RunPublishedCycle(first).PreparedPublication = %+v, want sequence 1", record.PreparedPublication)
	}
	sentAt := now.Add(time.Second)
	if err := harness.MarkPublicationSent(record.PreparedPublication.Sequence, sentAt); err != nil {
		t.Fatalf("MarkPublicationSent(first) error = %v", err)
	}
	if err := harness.AcknowledgePublication(record.PreparedPublication.Sequence, sentAt); err != nil {
		t.Fatalf("AcknowledgePublication(first) error = %v", err)
	}

	second, err := harness.RunPublishedCycle(PublishedCycleInput{
		RecordID: "cycle-published-2",
		At:       now.Add(5 * time.Second),
		Quotes: []feeds.Quote{
			harnessQuote(t, "coinbase", "ETHUSD", "1982.75", now.Add(5*time.Second), feeds.FreshnessBasisSourceTimestamp),
		},
	})
	if err != nil {
		t.Fatalf("RunPublishedCycle(second) error = %v", err)
	}
	if second.PreparedPublication == nil || second.PreparedPublication.Sequence != 2 {
		t.Fatalf("RunPublishedCycle(second).PreparedPublication = %+v, want sequence 2", second.PreparedPublication)
	}

	publication, err := harness.preparePublication(now, []byte("{\n  \"prices\": {\"oracle\": \"1981.75\"}\n}"))
	if err != nil {
		t.Fatalf("preparePublication() error = %v", err)
	}
	compact := `{"prices":{"oracle":"1981.75"}}`
	sum := sha256.Sum256([]byte(compact))
	hash := hex.EncodeToString(sum[:])
	if publication.PayloadHash != hash {
		t.Fatalf("preparePublication().PayloadHash = %q, want %q", publication.PayloadHash, hash)
	}
	if !strings.Contains(string(publication.Envelope), compact) {
		t.Fatalf("preparePublication().Envelope = %s, want compact payload", publication.Envelope)
	}
}

func TestHarnessRecoverUsesUnderlyingStore(t *testing.T) {
	t.Parallel()

	storeDSN := testdb.DSN(t)
	market := harnessMarket(t)
	metadata := store.Metadata{
		ConfigDigest:            "config-digest",
		PricingAlgorithmVersion: pricing.AlgorithmVersion,
		SchemaVersion:           store.SchemaVersion,
		EnvelopeVersion:         relayer.EnvelopeVersion,
	}
	harness, err := NewHarness(storeDSN, market, metadata)
	if err != nil {
		t.Fatalf("NewHarness() error = %v", err)
	}
	t.Cleanup(func() {
		if closeErr := harness.Close(); closeErr != nil {
			t.Fatalf("harness.Close() error = %v", closeErr)
		}
	})

	now := time.Date(2026, 3, 27, 19, 0, 0, 0, time.UTC)
	record, err := harness.RunPublishedCycle(PublishedCycleInput{
		RecordID: "cycle-recover",
		At:       now,
		Quotes: []feeds.Quote{
			harnessQuote(t, "coinbase", "ETHUSD", "1981.75", now, feeds.FreshnessBasisSourceTimestamp),
		},
	})
	if err != nil {
		t.Fatalf("RunPublishedCycle() error = %v", err)
	}

	report, err := harness.Recover(context.Background(), operator.ModeExact, record.RecordID, nil)
	if err != nil {
		t.Fatalf("Recover(exact) error = %v", err)
	}
	if report.Record.RecordID != record.RecordID {
		t.Fatalf("Recover(exact).Record.RecordID = %q, want %q", report.Record.RecordID, record.RecordID)
	}
}
