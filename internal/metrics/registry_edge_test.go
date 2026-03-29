package metrics

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/lumosimmo/market-relayer-go/internal/feeds"
	"github.com/lumosimmo/market-relayer-go/internal/status"
	"github.com/lumosimmo/market-relayer-go/internal/testsupport"
)

func TestRegistryNilSafetyAndPublishabilitySemantics(t *testing.T) {
	t.Parallel()

	t.Run("nil registry is a safe no-op", func(t *testing.T) {
		t.Parallel()

		var registry *Registry
		registry.ObserveSourceFreshness("ETHUSD", "coinbase", feeds.FreshnessBasisSourceTimestamp, time.Second)
		registry.IncQuoteFreeze("ETHUSD", "coinbase")
		registry.ObserveComputeLatency("ETHUSD", 10*time.Millisecond)
		registry.ObserveFetchLatency("ETHUSD", 20*time.Millisecond)
		registry.ObservePersistLatency("ETHUSD", 30*time.Millisecond)
		registry.ObservePublishLatency("ETHUSD", 40*time.Millisecond)
		registry.IncOracleClamp("ETHUSD")
		registry.IncRejectedUpdate("ETHUSD", "coinbase", status.ReasonSourceRejected)
		registry.IncTickOverrun("ETHUSD")
		registry.IncRetryBudgetExhausted("ETHUSD")
		registry.IncRateLimitCooldown("ETHUSD", "coinbase")
		registry.IncRecoveryReplay("ETHUSD")
		registry.SetPublishabilityState("ETHUSD", status.Condition{}, status.Condition{})
		registry.IncPublishabilityTransition("ETHUSD", status.Condition{})
		registry.IncStoreFailure("ETHUSD")

		recorder := httptest.NewRecorder()
		registry.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
		if recorder.Code != http.StatusOK {
			t.Fatalf("Handler() status = %d, want %d", recorder.Code, http.StatusOK)
		}
	})

	t.Run("publishability state normalizes blank conditions and fetch latency is exported", func(t *testing.T) {
		t.Parallel()

		registry := NewRegistry()
		registry.ObserveFetchLatency("ETHUSD", 75*time.Millisecond)
		registry.SetPublishabilityState(
			"ETHUSD",
			status.Condition{},
			status.NewCondition(status.StatusUnavailable, status.ReasonSinkUnavailable),
		)

		normalized := normalizeCondition(status.Condition{})
		if normalized.Status != status.StatusUnknown || normalized.Reason != status.ReasonNone {
			t.Fatalf("normalizeCondition(blank) = %+v, want unknown/none", normalized)
		}

		families, err := registry.Gatherer().Gather()
		if err != nil {
			t.Fatalf("Gather() error = %v", err)
		}

		testsupport.AssertMetricGauge(t, families, "market_relayer_publishability_state", map[string]string{
			"market": "ETHUSD",
			"status": "unknown",
			"reason": "none",
		}, 0)
		testsupport.AssertMetricGauge(t, families, "market_relayer_publishability_state", map[string]string{
			"market": "ETHUSD",
			"status": "unavailable",
			"reason": "sink_unavailable",
		}, 1)
		testsupport.AssertMetricHistogramCount(t, families, "market_relayer_phase_latency_seconds", map[string]string{
			"market": "ETHUSD",
			"phase":  "fetch",
		}, 1)
		testsupport.AssertMetricHistogramSum(t, families, "market_relayer_phase_latency_seconds", map[string]string{
			"market": "ETHUSD",
			"phase":  "fetch",
		}, 0.075)

		recorder := httptest.NewRecorder()
		registry.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
		if recorder.Code != http.StatusOK {
			t.Fatalf("Handler() status = %d, want %d", recorder.Code, http.StatusOK)
		}
		if body := recorder.Body.String(); !strings.Contains(body, "market_relayer_phase_latency_seconds") {
			t.Fatalf("Handler() body = %q, want phase latency metric", body)
		}
	})
}

func TestRegistrySetPublishabilityGaugeWritesExpectedLabels(t *testing.T) {
	t.Parallel()

	registry := NewRegistry()
	registry.setPublishabilityGauge("ETHUSD", status.NewCondition(status.StatusAvailable, status.ReasonNone), 1)

	families, err := registry.Gatherer().Gather()
	if err != nil {
		t.Fatalf("Gather() error = %v", err)
	}

	metric := testsupport.LookupMetric(t, families, "market_relayer_publishability_state", map[string]string{
		"market": "ETHUSD",
		"status": "available",
		"reason": "none",
	})
	if got := metric.GetGauge().GetValue(); got != 1 {
		t.Fatalf("publishability gauge value = %v, want 1", got)
	}
}
