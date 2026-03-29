package pricing

import (
	"testing"
	"time"

	"github.com/lumosimmo/market-relayer-go/internal/config"
	"github.com/lumosimmo/market-relayer-go/internal/feeds"
	"github.com/lumosimmo/market-relayer-go/internal/fixedpoint"
	"github.com/lumosimmo/market-relayer-go/internal/status"
)

func TestPricingHelpers(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 3, 26, 16, 0, 0, 0, time.UTC)

	t.Run("normalize inputs rejects wrong symbol duplicate and unknown source", func(t *testing.T) {
		t.Parallel()

		market := testMarket(50)
		if _, err := normalizeInputs(market, []feeds.Quote{
			testQuote(t, "coinbase", "BTCUSD", "65941.00", now, feeds.FreshnessBasisSourceTimestamp),
		}); err == nil {
			t.Fatal("normalizeInputs(wrong symbol) error = nil, want validation failure")
		}
		if _, err := normalizeInputs(market, []feeds.Quote{
			testQuote(t, "coinbase", "ETHUSD", "1981.75", now, feeds.FreshnessBasisSourceTimestamp),
			testQuote(t, "coinbase", "ETHUSD", "1982.75", now, feeds.FreshnessBasisSourceTimestamp),
		}); err == nil {
			t.Fatal("normalizeInputs(duplicate) error = nil, want validation failure")
		}
		if _, err := normalizeInputs(market, []feeds.Quote{
			testQuote(t, "unknown", "ETHUSD", "1981.75", now, feeds.FreshnessBasisSourceTimestamp),
		}); err == nil {
			t.Fatal("normalizeInputs(unknown source) error = nil, want validation failure")
		}
	})

	t.Run("apply selection chooses earliest basis and source order tie break", func(t *testing.T) {
		t.Parallel()

		selection := applySelection(ExternalSelection{}, []NormalizedInput{
			{Source: "kraken", SourceOrder: 1, FreshnessBasisKind: feeds.FreshnessBasisReceivedAt, FreshnessBasisAt: now},
			{Source: "coinbase", SourceOrder: 0, FreshnessBasisKind: feeds.FreshnessBasisSourceTimestamp, FreshnessBasisAt: now},
		})
		if got, want := selection.BasisSource, "coinbase"; got != want {
			t.Fatalf("BasisSource = %q, want %q", got, want)
		}
	})

	t.Run("within threshold accepts exact boundary and rejects invalid arguments", func(t *testing.T) {
		t.Parallel()

		ok, err := withinThreshold(mustDecimal(t, "1982.75"), mustDecimal(t, "1981.75"), 100)
		if err != nil {
			t.Fatalf("withinThreshold(boundary) error = %v", err)
		}
		if !ok {
			t.Fatal("withinThreshold(boundary) = false, want true")
		}
		if _, err := withinThreshold(mustDecimal(t, "1981.75"), fixedpoint.Value{Int: 0, Scale: 2}, 10); err == nil {
			t.Fatal("withinThreshold(non-positive base) error = nil, want validation failure")
		}
		if _, err := withinThreshold(mustDecimal(t, "1981.75"), mustDecimal(t, "1981.75"), -1); err == nil {
			t.Fatal("withinThreshold(negative threshold) error = nil, want validation failure")
		}
	})

	t.Run("pair within threshold covers exact boundary and invalid midpoint", func(t *testing.T) {
		t.Parallel()

		ok, err := pairWithinThreshold(mustDecimal(t, "1981.75"), mustDecimal(t, "1983.75"), mustDecimal(t, "1982.75"), 198)
		if err != nil {
			t.Fatalf("pairWithinThreshold(boundary) error = %v", err)
		}
		if !ok {
			t.Fatal("pairWithinThreshold(boundary) = false, want true")
		}
		if _, err := pairWithinThreshold(mustDecimal(t, "1981.75"), mustDecimal(t, "1983.75"), fixedpoint.Value{Int: 0, Scale: 2}, 10); err == nil {
			t.Fatal("pairWithinThreshold(non-positive midpoint) error = nil, want validation failure")
		}
	})

	t.Run("continuous ema caps dt", func(t *testing.T) {
		t.Parallel()

		got, err := smoothContinuous(mustDecimal(t, "1981.75"), mustDecimal(t, "1983.75"), time.Hour, oracleEMATau)
		if err != nil {
			t.Fatalf("smoothContinuous() error = %v", err)
		}
		want := mustDecimal(t, "1981.94")
		assertFixedpointString(t, got, mustFormat(t, want))
	})

	t.Run("frozen external reason reflects closure and divergence", func(t *testing.T) {
		t.Parallel()

		if got, want := externalFrozenReason(Inputs{ExternalClosed: true}, Price{}), status.ReasonSessionClosed; got != want {
			t.Fatalf("externalFrozenReason(closed) = %q, want %q", got, want)
		}
		if got, want := externalFrozenReason(Inputs{}, Price{Reason: status.ReasonSourceDivergence}), status.ReasonSourceDivergence; got != want {
			t.Fatalf("externalFrozenReason(divergence) = %q, want %q", got, want)
		}
		if got, want := externalFrozenReason(Inputs{}, Price{}), status.ReasonSourceStale; got != want {
			t.Fatalf("externalFrozenReason(default) = %q, want %q", got, want)
		}
	})
}

func testMarket(divergenceThresholdBPS int64) config.MarketConfig {
	market := testConfiguredMarket()
	market.DivergenceThresholdBPS = divergenceThresholdBPS
	return market
}
