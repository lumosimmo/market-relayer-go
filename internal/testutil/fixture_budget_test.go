package testutil

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/lumosimmo/market-relayer-go/internal/clock"
	"github.com/lumosimmo/market-relayer-go/internal/feeds"
	"github.com/lumosimmo/market-relayer-go/internal/metrics"
	"github.com/lumosimmo/market-relayer-go/internal/relayer"
	"github.com/lumosimmo/market-relayer-go/internal/status"
	"github.com/lumosimmo/market-relayer-go/internal/testdb"
	"github.com/lumosimmo/market-relayer-go/internal/testsupport"
	dto "github.com/prometheus/client_model/go"
)

func TestFixtureCycleBudgetsStayWithinTargets(t *testing.T) {
	fixtures := loadTestFixtures(t)
	budgets := relayer.DefaultPhaseBudgets(fixtures.Market.PublishInterval)

	for _, name := range []string{"happy-path-open", "retry-path"} {
		name := name
		t.Run(name, func(t *testing.T) {
			scenario := mustFixtureScenario(t, fixtures, name)
			result := runFixtureBudgetScenario(t, fixtures, scenario)
			assertBudgetWithinTarget(t, result.families, budgets, fixtures.Market.PublishInterval, result.totalElapsed)
		})
	}
}

func BenchmarkFixtureCycleBudgets(b *testing.B) {
	fixtures := loadTestFixtures(b)
	budgets := relayer.DefaultPhaseBudgets(fixtures.Market.PublishInterval)

	for _, name := range []string{"happy-path-open", "retry-path"} {
		name := name
		b.Run(name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				scenario := mustFixtureScenario(b, fixtures, name)
				result := runFixtureBudgetScenario(b, fixtures, scenario)
				assertBudgetWithinTarget(b, result.families, budgets, fixtures.Market.PublishInterval, result.totalElapsed)
			}
		})
	}
}

type fixtureBudgetResult struct {
	totalElapsed time.Duration
	families     []*dto.MetricFamily
}

func runFixtureBudgetScenario(tb testing.TB, fixtures FixtureSet, scenario FixtureScenario) fixtureBudgetResult {
	tb.Helper()

	storeDB := openMigratedStore(tb, testdb.DSN(tb))
	tb.Cleanup(func() {
		if closeErr := storeDB.Close(); closeErr != nil {
			tb.Fatalf("store.Close() error = %v", closeErr)
		}
	})

	metricsRegistry := metrics.NewRegistry()
	loop, err := relayer.NewLoop(relayer.LoopOptions{
		Market:   fixtures.Market,
		Metadata: fixtures.Metadata,
		Store:    storeDB,
		Sink:     newFixtureSink("fixture-loop", scenario.SinkOutcomes),
		Sources:  fixtureLoopSources(scenario, fixtures.SourceOrder),
		Clock:    clock.Real{},
		Metrics:  metricsRegistry,
		Budgets:  relayer.DefaultPhaseBudgets(fixtures.Market.PublishInterval),
		Attempts: relayer.AttemptBudgets{PublishAttemptTimeout: 100 * time.Millisecond},
	})
	if err != nil {
		tb.Fatalf("relayer.NewLoop() error = %v", err)
	}

	started := time.Now()
	if err := loop.RunCycle(context.Background()); err != nil {
		tb.Fatalf("RunCycle() error = %v", err)
	}
	elapsed := time.Since(started)

	families, err := metricsRegistry.Gatherer().Gather()
	if err != nil {
		tb.Fatalf("Gather() error = %v", err)
	}
	return fixtureBudgetResult{
		totalElapsed: elapsed,
		families:     families,
	}
}

func assertBudgetWithinTarget(tb testing.TB, families []*dto.MetricFamily, budgets relayer.PhaseBudgets, totalBudget time.Duration, totalElapsed time.Duration) {
	tb.Helper()

	for phase, limit := range map[string]time.Duration{
		"fetch":   budgets.Fetch,
		"compute": budgets.Compute,
		"persist": budgets.Persist,
		"publish": budgets.Publish,
	} {
		metric := testsupport.LookupMetric(tb, families, "market_relayer_phase_latency_seconds", map[string]string{
			"market": "ETHUSD",
			"phase":  phase,
		})
		if got := histogramSampleDuration(metric); got > limit {
			tb.Fatalf("%s latency = %s, want <= %s", phase, got, limit)
		}
	}
	if totalElapsed > totalBudget {
		tb.Fatalf("total cycle latency = %s, want <= %s", totalElapsed, totalBudget)
	}
}

func histogramSampleDuration(metric *dto.Metric) time.Duration {
	return time.Duration(metric.GetHistogram().GetSampleSum() * float64(time.Second))
}

func fixtureLoopSources(scenario FixtureScenario, sourceOrder []string) []feeds.Source {
	quotesBySource := make(map[string]feeds.Quote, len(scenario.Quotes))
	for _, quote := range scenario.Quotes {
		quotesBySource[quote.Source] = quote
	}

	sources := make([]feeds.Source, 0, len(sourceOrder))
	for _, sourceName := range sourceOrder {
		quote, ok := quotesBySource[sourceName]
		if ok {
			quote := quote
			sources = append(sources, &fixtureLoopSource{
				name: sourceName,
				fetch: func(context.Context) (feeds.Quote, error) {
					return quote, nil
				},
			})
			continue
		}

		name := sourceName
		sources = append(sources, &fixtureLoopSource{
			name: name,
			fetch: func(context.Context) (feeds.Quote, error) {
				return feeds.Quote{}, &feeds.Error{
					Source:    name,
					Kind:      feeds.ErrorTransport,
					Reason:    status.ReasonSourceRejected,
					Retryable: false,
					Err:       errors.New("fixture source unavailable"),
				}
			},
		})
	}
	return sources
}

type fixtureLoopSource struct {
	name  string
	fetch func(context.Context) (feeds.Quote, error)
}

func (source *fixtureLoopSource) Name() string {
	return source.name
}

func (source *fixtureLoopSource) Fetch(ctx context.Context, _ feeds.FetchBudget) (feeds.Quote, error) {
	return source.fetch(ctx)
}
