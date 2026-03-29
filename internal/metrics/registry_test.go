package metrics

import (
	"testing"
	"time"

	"github.com/lumosimmo/market-relayer-go/internal/feeds"
	"github.com/lumosimmo/market-relayer-go/internal/status"
	"github.com/lumosimmo/market-relayer-go/internal/testsupport"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestRegistryExportsStableMetricFamilies(t *testing.T) {
	t.Parallel()

	registry := NewRegistry()
	registry.ObserveSourceFreshness("ETHUSD", "coinbase", feeds.FreshnessBasisSourceTimestamp, 2*time.Second)
	registry.IncQuoteFreeze("ETHUSD", "kraken")
	registry.ObserveComputeLatency("ETHUSD", 25*time.Millisecond)
	registry.ObservePersistLatency("ETHUSD", 15*time.Millisecond)
	registry.ObservePublishLatency("ETHUSD", 40*time.Millisecond)
	registry.IncOracleClamp("ETHUSD")
	registry.IncRejectedUpdate("ETHUSD", "coinbase", status.ReasonSourceRejected)
	registry.IncTickOverrun("ETHUSD")
	registry.IncRetryBudgetExhausted("ETHUSD")
	registry.IncRateLimitCooldown("ETHUSD", "coinbase")
	registry.IncRecoveryReplay("ETHUSD")
	registry.IncPublishabilityTransition("ETHUSD", status.NewCondition(status.StatusUnavailable, status.ReasonSinkUnavailable))
	registry.IncStoreFailure("ETHUSD")

	if got, err := testutil.GatherAndCount(registry.Gatherer(), "market_relayer_source_freshness_seconds"); err != nil || got != 1 {
		if err != nil {
			t.Fatalf("GatherAndCount(source freshness) error = %v", err)
		}
		t.Fatalf("GatherAndCount(source freshness) = %d, want 1", got)
	}
	if got, err := testutil.GatherAndCount(registry.Gatherer(), "market_relayer_phase_latency_seconds"); err != nil || got != 3 {
		if err != nil {
			t.Fatalf("GatherAndCount(phase latency) error = %v", err)
		}
		t.Fatalf("GatherAndCount(phase latency) = %d, want 3", got)
	}

	families, err := registry.Gatherer().Gather()
	if err != nil {
		t.Fatalf("Gather() error = %v", err)
	}

	testsupport.AssertMetricGauge(t, families, "market_relayer_source_freshness_seconds", map[string]string{
		"market":     "ETHUSD",
		"source":     "coinbase",
		"basis_kind": "source_ts",
	}, 2)
	testsupport.AssertMetricCounter(t, families, "market_relayer_quote_freeze_events_total", map[string]string{
		"market": "ETHUSD",
		"source": "kraken",
	}, 1)
	testsupport.AssertMetricCounter(t, families, "market_relayer_oracle_clamp_events_total", map[string]string{
		"market": "ETHUSD",
	}, 1)
	testsupport.AssertMetricCounter(t, families, "market_relayer_rejected_updates_total", map[string]string{
		"market": "ETHUSD",
		"source": "coinbase",
		"reason": "source_rejected",
	}, 1)
	testsupport.AssertMetricCounter(t, families, "market_relayer_publishability_transitions_total", map[string]string{
		"market": "ETHUSD",
		"status": "unavailable",
		"reason": "sink_unavailable",
	}, 1)
	testsupport.AssertMetricCounter(t, families, "market_relayer_store_failures_total", map[string]string{
		"market": "ETHUSD",
	}, 1)
	testsupport.AssertMetricHistogramCount(t, families, "market_relayer_phase_latency_seconds", map[string]string{
		"market": "ETHUSD",
		"phase":  "publish",
	}, 1)
	testsupport.AssertMetricHistogramSum(t, families, "market_relayer_phase_latency_seconds", map[string]string{
		"market": "ETHUSD",
		"phase":  "publish",
	}, 0.04)
}
