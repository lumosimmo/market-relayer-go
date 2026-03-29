package testutil

import (
	"testing"
	"time"

	"github.com/lumosimmo/market-relayer-go/internal/config"
	"github.com/lumosimmo/market-relayer-go/internal/feeds"
	"github.com/lumosimmo/market-relayer-go/internal/fixedpoint"
	"github.com/lumosimmo/market-relayer-go/internal/pricing"
	"github.com/lumosimmo/market-relayer-go/internal/relayer"
	"github.com/lumosimmo/market-relayer-go/internal/status"
	"github.com/lumosimmo/market-relayer-go/internal/store"
	"github.com/lumosimmo/market-relayer-go/internal/testdb"
)

func TestHarnessRestoresOracleStateAcrossRestart(t *testing.T) {
	t.Parallel()

	storeDSN := testdb.DSN(t)
	market := harnessMarket(t)
	metadata := store.Metadata{
		ConfigDigest:            "config-digest-123",
		PricingAlgorithmVersion: pricing.AlgorithmVersion,
		SchemaVersion:           store.SchemaVersion,
		EnvelopeVersion:         relayer.EnvelopeVersion,
	}

	now := time.Date(2026, 3, 27, 15, 0, 0, 0, time.UTC)
	harness, err := NewHarness(storeDSN, market, metadata)
	if err != nil {
		t.Fatalf("NewHarness() error = %v", err)
	}

	firstRecord, err := harness.RunCycle(CycleInput{
		RecordID: "cycle-1",
		At:       now,
		Quotes: []feeds.Quote{
			harnessQuote(t, "coinbase", "ETHUSD", "1981.75", now, feeds.FreshnessBasisSourceTimestamp),
		},
		Payload: []byte(`{"prices":{"oracle":"1981.75"}}`),
	})
	if err != nil {
		t.Fatalf("RunCycle(first) error = %v", err)
	}
	if firstRecord.PreparedPublication == nil {
		t.Fatal("first prepared publication = nil, want value")
	}

	if err := harness.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	restarted, err := NewHarness(storeDSN, market, metadata)
	if err != nil {
		t.Fatalf("NewHarness(restarted) error = %v", err)
	}
	t.Cleanup(func() {
		if closeErr := restarted.Close(); closeErr != nil {
			t.Fatalf("restarted.Close() error = %v", closeErr)
		}
	})

	nextAt := now.Add(10 * time.Second)
	restoredState, err := restarted.RestoreState(nextAt)
	if err != nil {
		t.Fatalf("RestoreState() error = %v", err)
	}
	assertHarnessValue(t, restoredState.LastOracle, firstRecord.Computed.State.LastOracle)
	if !restoredState.OracleFallbackExpiresAt.Equal(firstRecord.Computed.State.OracleFallbackExpiresAt) {
		t.Fatalf("OracleFallbackExpiresAt = %s, want %s", restoredState.OracleFallbackExpiresAt, firstRecord.Computed.State.OracleFallbackExpiresAt)
	}

	secondRecord, err := restarted.RunCycle(CycleInput{
		RecordID: "cycle-2",
		At:       nextAt,
	})
	if err != nil {
		t.Fatalf("RunCycle(second) error = %v", err)
	}
	if secondRecord.PreparedPublication != nil {
		t.Fatal("second prepared publication != nil, want nil")
	}
	if secondRecord.Computed.Oracle.Status != status.StatusDegraded || secondRecord.Computed.Oracle.Reason != status.ReasonFallbackActive {
		t.Fatalf("second oracle = {%q %q}, want degraded fallback_active", secondRecord.Computed.Oracle.Status, secondRecord.Computed.Oracle.Reason)
	}
	if secondRecord.Computed.OraclePlusBasis.Reason != status.ReasonColdStart {
		t.Fatalf("second oracle_plus_basis reason = %q, want cold_start", secondRecord.Computed.OraclePlusBasis.Reason)
	}
}

func TestHarnessDropsExpiredOracleFallbackAcrossRestart(t *testing.T) {
	t.Parallel()

	storeDSN := testdb.DSN(t)
	market := harnessMarket(t)
	metadata := store.Metadata{
		ConfigDigest:            "config-digest-123",
		PricingAlgorithmVersion: pricing.AlgorithmVersion,
		SchemaVersion:           store.SchemaVersion,
		EnvelopeVersion:         relayer.EnvelopeVersion,
	}

	now := time.Date(2026, 3, 27, 16, 0, 0, 0, time.UTC)
	harness, err := NewHarness(storeDSN, market, metadata)
	if err != nil {
		t.Fatalf("NewHarness() error = %v", err)
	}

	firstRecord, err := harness.RunCycle(CycleInput{
		RecordID: "cycle-1",
		At:       now,
		Quotes: []feeds.Quote{
			harnessQuote(t, "coinbase", "ETHUSD", "1981.75", now, feeds.FreshnessBasisSourceTimestamp),
		},
	})
	if err != nil {
		t.Fatalf("RunCycle(first) error = %v", err)
	}

	if err := harness.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	restarted, err := NewHarness(storeDSN, market, metadata)
	if err != nil {
		t.Fatalf("NewHarness(restarted) error = %v", err)
	}
	t.Cleanup(func() {
		if closeErr := restarted.Close(); closeErr != nil {
			t.Fatalf("restarted.Close() error = %v", closeErr)
		}
	})

	expiredAt := firstRecord.Computed.State.OracleFallbackExpiresAt.Add(time.Second)
	restoredState, err := restarted.RestoreState(expiredAt)
	if err != nil {
		t.Fatalf("RestoreState() error = %v", err)
	}
	assertHarnessValue(t, restoredState.LastExternal, firstRecord.Computed.State.LastExternal)
	assertHarnessValue(t, restoredState.LastOracle, firstRecord.Computed.State.LastOracle)
	assertHarnessValue(t, restoredState.LastOraclePlusBasis, firstRecord.Computed.State.LastOraclePlusBasis)
	if !restoredState.OracleFallbackExpiresAt.Equal(firstRecord.Computed.State.OracleFallbackExpiresAt) {
		t.Fatalf("OracleFallbackExpiresAt = %s, want %s", restoredState.OracleFallbackExpiresAt, firstRecord.Computed.State.OracleFallbackExpiresAt)
	}

	secondRecord, err := restarted.RunCycle(CycleInput{
		RecordID: "cycle-2",
		At:       expiredAt,
	})
	if err != nil {
		t.Fatalf("RunCycle(second) error = %v", err)
	}
	if secondRecord.Computed.SessionState != status.SessionStateExternalStale {
		t.Fatalf("second session state = %q, want %q", secondRecord.Computed.SessionState, status.SessionStateExternalStale)
	}
	if secondRecord.Computed.Oracle.Reason != status.ReasonFallbackExpired {
		t.Fatalf("second oracle reason = %q, want %q", secondRecord.Computed.Oracle.Reason, status.ReasonFallbackExpired)
	}
	if secondRecord.Computed.OraclePlusBasis.Reason != status.ReasonColdStart {
		t.Fatalf("second oracle_plus_basis reason = %q, want %q", secondRecord.Computed.OraclePlusBasis.Reason, status.ReasonColdStart)
	}
}

func harnessMarket(t *testing.T) config.MarketConfig {
	t.Helper()

	cfg, err := config.LoadBytes([]byte(`
version: 1
service:
  name: market-relayer-go
  instance_id: harness
store:
  dsn: postgres://ignored
api:
  listen_addr: 127.0.0.1:0
sink:
  kind: file
  path: /tmp/harness.out
  request_timeout: 1s
sources:
  - name: coinbase
    kind: coinbase
    product_id: ETH-USD
    request_timeout: 1s
    timestamp_mode: source_timestamp
  - name: kraken
    kind: kraken
    pair: ETH/USD
    request_timeout: 1s
    timestamp_mode: received_at
    max_unchanged_age: 10s
  - name: hl
    kind: hyperliquid
    market: ETH
    request_timeout: 1s
    timestamp_mode: source_timestamp
markets:
  - symbol: ETHUSD
    session_mode: always_open
    perp_book_source: hl
    impact_notional_usd: "100"
    publish_interval: 3s
    staleness_threshold: 10s
    max_fallback_age: 30s
    clamp_bps: 50
    divergence_threshold_bps: 100
    publish_scale: 2
    sources:
      - name: coinbase
      - name: kraken
`), config.LoadOptions{})
	if err != nil {
		t.Fatalf("config.LoadBytes() error = %v", err)
	}
	return cfg.Markets[0]
}

func harnessQuote(t *testing.T, source string, symbol string, mid string, basisAt time.Time, kind feeds.FreshnessBasisKind) feeds.Quote {
	t.Helper()

	value := mustHarnessDecimal(t, mid)
	quote := feeds.Quote{
		Source:             source,
		Symbol:             symbol,
		Bid:                value,
		Ask:                value,
		Last:               value,
		Scale:              value.Scale,
		ReceivedAt:         basisAt,
		FreshnessBasisKind: kind,
		FreshnessBasisAt:   basisAt,
	}
	if kind == feeds.FreshnessBasisSourceTimestamp {
		ts := basisAt
		quote.SourceTS = &ts
	}
	return quote
}

func mustHarnessDecimal(t *testing.T, raw string) fixedpoint.Value {
	t.Helper()

	value, err := fixedpoint.ParseDecimal(raw)
	if err != nil {
		t.Fatalf("fixedpoint.ParseDecimal(%q) error = %v", raw, err)
	}
	return value
}

func assertHarnessValue(t *testing.T, got *fixedpoint.Value, want *fixedpoint.Value) {
	t.Helper()

	switch {
	case got == nil && want == nil:
		return
	case got == nil || want == nil:
		t.Fatalf("value = %v, want %v", got, want)
	case *got != *want:
		t.Fatalf("value = %+v, want %+v", *got, *want)
	}
}
