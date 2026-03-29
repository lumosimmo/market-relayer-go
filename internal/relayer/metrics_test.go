package relayer

import (
	"context"
	"testing"
	"time"

	"github.com/lumosimmo/market-relayer-go/internal/config"
	"github.com/lumosimmo/market-relayer-go/internal/feeds"
	"github.com/lumosimmo/market-relayer-go/internal/metrics"
	"github.com/lumosimmo/market-relayer-go/internal/status"
	"github.com/lumosimmo/market-relayer-go/internal/testsupport"
)

func TestLoopObservesFetchClampAndLatencyMetrics(t *testing.T) {
	t.Parallel()

	storeDB := openLoopStore(t)
	metricsRegistry := metrics.NewRegistry()
	sink := newScriptedSink(
		sinkOutcome{ack: Ack{}},
		sinkOutcome{ack: Ack{}},
	)

	market := testLoopMarket()
	market.Sources = append(market.Sources, config.MarketSourceRef{Name: "kraken"})

	testClock := newManualClock(time.Date(2026, 3, 27, 20, 0, 0, 0, time.UTC))
	healthy := &scriptedSource{
		name: "coinbase",
		fn: func(_ context.Context, call int) (feeds.Quote, error) {
			value := "1981.75"
			if call > 0 {
				value = "2001.75"
			}
			return relayerQuote(t, "coinbase", "ETHUSD", value, testClock.Now()), nil
		},
	}
	frozen := &scriptedSource{
		name: "kraken",
		fn: func(_ context.Context, _ int) (feeds.Quote, error) {
			return feeds.Quote{}, &feeds.Error{
				Source: "kraken",
				Kind:   feeds.ErrorFrozenQuote,
				Reason: status.ReasonSourceFrozen,
				Err:    context.DeadlineExceeded,
			}
		},
	}

	loop, err := NewLoop(LoopOptions{
		Market:   market,
		Metadata: testLoopMetadata(),
		Store:    storeDB,
		Sink:     sink,
		Sources:  []feeds.Source{healthy, frozen},
		Clock:    testClock,
		Metrics:  metricsRegistry,
		Budgets:  storeBackedPhaseBudgets(200 * time.Millisecond),
		Attempts: AttemptBudgets{PublishAttemptTimeout: 100 * time.Millisecond},
	})
	if err != nil {
		t.Fatalf("NewLoop() error = %v", err)
	}

	if err := loop.RunCycle(context.Background()); err != nil {
		t.Fatalf("first RunCycle() error = %v", err)
	}
	testClock.Advance(3 * time.Second)
	if err := loop.RunCycle(context.Background()); err != nil {
		t.Fatalf("second RunCycle() error = %v", err)
	}

	families, err := metricsRegistry.Gatherer().Gather()
	if err != nil {
		t.Fatalf("Gather() error = %v", err)
	}
	testsupport.AssertMetricCounter(t, families, "market_relayer_quote_freeze_events_total", map[string]string{
		"market": "ETHUSD",
		"source": "kraken",
	}, 2)
	testsupport.AssertMetricCounter(t, families, "market_relayer_rejected_updates_total", map[string]string{
		"market": "ETHUSD",
		"source": "kraken",
		"reason": "source_frozen",
	}, 2)
	testsupport.AssertMetricCounter(t, families, "market_relayer_oracle_clamp_events_total", map[string]string{
		"market": "ETHUSD",
	}, 1)
	testsupport.AssertMetricGauge(t, families, "market_relayer_publishability_state", map[string]string{
		"market": "ETHUSD",
		"status": "available",
		"reason": "none",
	}, 1)
	testsupport.AssertMetricGauge(t, families, "market_relayer_source_freshness_seconds", map[string]string{
		"market":     "ETHUSD",
		"source":     "coinbase",
		"basis_kind": "source_ts",
	}, 0)
	testsupport.AssertMetricHistogramCount(t, families, "market_relayer_phase_latency_seconds", map[string]string{
		"market": "ETHUSD",
		"phase":  "fetch",
	}, 2)
	testsupport.AssertMetricHistogramCount(t, families, "market_relayer_phase_latency_seconds", map[string]string{
		"market": "ETHUSD",
		"phase":  "compute",
	}, 2)
	testsupport.AssertMetricHistogramCount(t, families, "market_relayer_phase_latency_seconds", map[string]string{
		"market": "ETHUSD",
		"phase":  "publish",
	}, 2)
}
