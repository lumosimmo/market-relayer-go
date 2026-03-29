package metrics

import (
	"net/http"
	"time"

	"github.com/lumosimmo/market-relayer-go/internal/feeds"
	"github.com/lumosimmo/market-relayer-go/internal/status"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Registry struct {
	registry *prometheus.Registry

	sourceFreshness           *prometheus.GaugeVec
	quoteFreezes              *prometheus.CounterVec
	phaseLatency              *prometheus.HistogramVec
	oracleClamp               *prometheus.CounterVec
	rejectedUpdates           *prometheus.CounterVec
	tickOverruns              *prometheus.CounterVec
	retryBudgetExhausted      *prometheus.CounterVec
	rateLimitCooldowns        *prometheus.CounterVec
	recoveryReplays           *prometheus.CounterVec
	publishabilityState       *prometheus.GaugeVec
	publishabilityTransitions *prometheus.CounterVec
	storeFailures             *prometheus.CounterVec
}

func NewRegistry() *Registry {
	registry := prometheus.NewRegistry()

	collector := &Registry{
		registry: registry,
		sourceFreshness: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "market_relayer_source_freshness_seconds",
			Help: "Latest observed freshness age by market, source, and freshness basis.",
		}, []string{"market", "source", "basis_kind"}),
		quoteFreezes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "market_relayer_quote_freeze_events_total",
			Help: "Count of timestamp-less quote freeze detections.",
		}, []string{"market", "source"}),
		phaseLatency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "market_relayer_phase_latency_seconds",
			Help:    "Phase latency by market and phase.",
			Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 3},
		}, []string{"market", "phase"}),
		oracleClamp: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "market_relayer_oracle_clamp_events_total",
			Help: "Count of oracle clamp events.",
		}, []string{"market"}),
		rejectedUpdates: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "market_relayer_rejected_updates_total",
			Help: "Count of rejected source updates by market, source, and reason.",
		}, []string{"market", "source", "reason"}),
		tickOverruns: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "market_relayer_tick_overruns_total",
			Help: "Count of relayer tick overruns.",
		}, []string{"market"}),
		retryBudgetExhausted: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "market_relayer_retry_budget_exhausted_total",
			Help: "Count of publish retries that exhausted the remaining budget.",
		}, []string{"market"}),
		rateLimitCooldowns: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "market_relayer_rate_limit_cooldowns_total",
			Help: "Count of source rate-limit cooldown activations.",
		}, []string{"market", "source"}),
		recoveryReplays: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "market_relayer_recovery_replays_total",
			Help: "Count of replayed prepared publications during recovery.",
		}, []string{"market"}),
		publishabilityState: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "market_relayer_publishability_state",
			Help: "Current publishability state by market, status, and reason.",
		}, []string{"market", "status", "reason"}),
		publishabilityTransitions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "market_relayer_publishability_transitions_total",
			Help: "Count of publishability state transitions by market, status, and reason.",
		}, []string{"market", "status", "reason"}),
		storeFailures: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "market_relayer_store_failures_total",
			Help: "Count of durable store failures that affect publishability.",
		}, []string{"market"}),
	}

	registry.MustRegister(
		collector.sourceFreshness,
		collector.quoteFreezes,
		collector.phaseLatency,
		collector.oracleClamp,
		collector.rejectedUpdates,
		collector.tickOverruns,
		collector.retryBudgetExhausted,
		collector.rateLimitCooldowns,
		collector.recoveryReplays,
		collector.publishabilityState,
		collector.publishabilityTransitions,
		collector.storeFailures,
	)
	return collector
}

func (registry *Registry) Gatherer() prometheus.Gatherer {
	if registry == nil {
		return prometheus.NewRegistry()
	}
	return registry.registry
}

func (registry *Registry) Handler() http.Handler {
	return promhttp.HandlerFor(registry.Gatherer(), promhttp.HandlerOpts{})
}

func (registry *Registry) ObserveSourceFreshness(market string, source string, kind feeds.FreshnessBasisKind, age time.Duration) {
	if registry == nil {
		return
	}
	registry.sourceFreshness.WithLabelValues(market, source, string(kind)).Set(age.Seconds())
}

func (registry *Registry) IncQuoteFreeze(market string, source string) {
	if registry == nil {
		return
	}
	registry.quoteFreezes.WithLabelValues(market, source).Inc()
}

func (registry *Registry) ObserveComputeLatency(market string, latency time.Duration) {
	registry.observePhaseLatency(market, "compute", latency)
}

func (registry *Registry) ObserveFetchLatency(market string, latency time.Duration) {
	registry.observePhaseLatency(market, "fetch", latency)
}

func (registry *Registry) ObservePersistLatency(market string, latency time.Duration) {
	registry.observePhaseLatency(market, "persist", latency)
}

func (registry *Registry) ObservePublishLatency(market string, latency time.Duration) {
	registry.observePhaseLatency(market, "publish", latency)
}

func (registry *Registry) observePhaseLatency(market string, phase string, latency time.Duration) {
	if registry == nil {
		return
	}
	registry.phaseLatency.WithLabelValues(market, phase).Observe(latency.Seconds())
}

func (registry *Registry) IncOracleClamp(market string) {
	if registry == nil {
		return
	}
	registry.oracleClamp.WithLabelValues(market).Inc()
}

func (registry *Registry) IncRejectedUpdate(market string, source string, reason status.Reason) {
	if registry == nil {
		return
	}
	registry.rejectedUpdates.WithLabelValues(market, source, string(reason)).Inc()
}

func (registry *Registry) IncTickOverrun(market string) {
	if registry == nil {
		return
	}
	registry.tickOverruns.WithLabelValues(market).Inc()
}

func (registry *Registry) IncRetryBudgetExhausted(market string) {
	if registry == nil {
		return
	}
	registry.retryBudgetExhausted.WithLabelValues(market).Inc()
}

func (registry *Registry) IncRateLimitCooldown(market string, source string) {
	if registry == nil {
		return
	}
	registry.rateLimitCooldowns.WithLabelValues(market, source).Inc()
}

func (registry *Registry) IncRecoveryReplay(market string) {
	if registry == nil {
		return
	}
	registry.recoveryReplays.WithLabelValues(market).Inc()
}

func (registry *Registry) SetPublishabilityState(market string, previous status.Condition, current status.Condition) {
	if registry == nil {
		return
	}
	previous = normalizeCondition(previous)
	current = normalizeCondition(current)
	registry.setPublishabilityGauge(market, previous, 0)
	registry.setPublishabilityGauge(market, current, 1)
}

func (registry *Registry) IncPublishabilityTransition(market string, condition status.Condition) {
	if registry == nil {
		return
	}
	registry.publishabilityTransitions.WithLabelValues(market, string(condition.Status), string(condition.Reason)).Inc()
}

func (registry *Registry) IncStoreFailure(market string) {
	if registry == nil {
		return
	}
	registry.storeFailures.WithLabelValues(market).Inc()
}

func normalizeCondition(condition status.Condition) status.Condition {
	if condition.Status == "" {
		condition.Status = status.StatusUnknown
	}
	if condition.Reason == "" {
		condition.Reason = status.ReasonNone
	}
	return condition
}

func (registry *Registry) setPublishabilityGauge(market string, condition status.Condition, value float64) {
	registry.publishabilityState.WithLabelValues(market, string(condition.Status), string(condition.Reason)).Set(value)
}
