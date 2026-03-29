package relayer

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/lumosimmo/market-relayer-go/internal/clock"
	"github.com/lumosimmo/market-relayer-go/internal/feeds"
	"github.com/lumosimmo/market-relayer-go/internal/metrics"
	"github.com/lumosimmo/market-relayer-go/internal/pricing"
	"github.com/lumosimmo/market-relayer-go/internal/status"
	"github.com/lumosimmo/market-relayer-go/internal/store"
	"github.com/lumosimmo/market-relayer-go/internal/testsupport"
)

func TestNewLoopValidationAndDefaults(t *testing.T) {
	t.Parallel()

	source := &scriptedSource{
		name: "coinbase",
		fn: func(_ context.Context, _ int) (feeds.Quote, error) {
			return relayerQuote(t, "coinbase", "ETHUSD", "1981.75", time.Date(2026, 3, 27, 12, 0, 0, 0, time.UTC)), nil
		},
	}

	if _, err := NewLoop(LoopOptions{Market: testLoopMarket(), Sink: newScriptedSink(), Sources: []feeds.Source{source}}); err == nil {
		t.Fatal("NewLoop(nil store) error = nil, want validation error")
	}
	if _, err := NewLoop(LoopOptions{Market: testLoopMarket(), Store: &stubStore{}, Sources: []feeds.Source{source}}); err == nil {
		t.Fatal("NewLoop(nil sink) error = nil, want validation error")
	}
	if _, err := NewLoop(LoopOptions{Market: testLoopMarket(), Store: &stubStore{}, Sink: newScriptedSink()}); err == nil {
		t.Fatal("NewLoop(no sources) error = nil, want validation error")
	}

	loop, err := NewLoop(LoopOptions{
		Market:   testLoopMarket(),
		Metadata: testLoopMetadata(),
		Store:    &stubStore{},
		Sink:     newScriptedSink(),
		Sources:  []feeds.Source{source},
	})
	if err != nil {
		t.Fatalf("NewLoop(defaults) error = %v", err)
	}
	if loop.clock == nil {
		t.Fatal("loop.clock = nil, want default clock")
	}
	if loop.waiter == nil {
		t.Fatal("loop.waiter = nil, want default waiter")
	}
	if loop.budgets.Fetch <= 0 || loop.budgets.Compute <= 0 || loop.budgets.Persist <= 0 || loop.budgets.Publish <= 0 {
		t.Fatalf("loop.budgets = %+v, want all budgets populated", loop.budgets)
	}
	if got := loop.publishAttemptTimeout(); got != 500*time.Millisecond {
		t.Fatalf("publishAttemptTimeout() = %s, want 500ms", got)
	}
	if loop.publishability.Status != status.StatusAvailable || loop.publishability.Reason != status.ReasonNone {
		t.Fatalf("loop.publishability = %+v, want available/none", loop.publishability)
	}
}

func TestLoopComputeAndPublishPhaseEdgeCases(t *testing.T) {
	t.Parallel()

	t.Run("compute phase respects pre-canceled context", func(t *testing.T) {
		t.Parallel()

		loop := newLoopForEdgeTests(t)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err := loop.computePhase(ctx, pricing.State{}, []feeds.Quote{
			relayerQuote(t, "coinbase", "ETHUSD", "1981.75", time.Date(2026, 3, 27, 12, 30, 0, 0, time.UTC)),
		}, nil, "cycle-canceled")
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("computePhase() error = %v, want context.Canceled", err)
		}
	})

	t.Run("publish phase returns canceled when parent context is canceled mid-attempt", func(t *testing.T) {
		t.Parallel()

		sink := newScriptedSink(sinkOutcome{waitForContextCancel: true})
		loop := &Loop{
			market:   testLoopMarket(),
			sink:     sink,
			clock:    clock.Fixed{Time: time.Date(2026, 3, 27, 13, 0, 0, 0, time.UTC)},
			budgets:  PhaseBudgets{Publish: time.Second},
			attempts: AttemptBudgets{PublishAttemptTimeout: 100 * time.Millisecond},
		}

		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() {
			sink.waitCalls(t, 1)
			cancel()
			close(done)
		}()

		_, result, err := loop.publishPhase(ctx, testPreparedPublication(17), "cycle-cancel")
		<-done
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("publishPhase() error = %v, want context.Canceled", err)
		}
		if result != "canceled" {
			t.Fatalf("publishPhase() result = %q, want canceled", result)
		}
	})

	t.Run("publish phase reports budget exhaustion before first attempt", func(t *testing.T) {
		t.Parallel()

		sink := newScriptedSink()
		loop := &Loop{
			market:   testLoopMarket(),
			sink:     sink,
			clock:    clock.Fixed{Time: time.Date(2026, 3, 27, 13, 15, 0, 0, time.UTC)},
			budgets:  PhaseBudgets{Publish: time.Second},
			attempts: AttemptBudgets{PublishAttemptTimeout: 100 * time.Millisecond},
		}

		ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
		defer cancel()

		_, result, err := loop.publishPhase(ctx, testPreparedPublication(18), "cycle-budget")
		if err != nil {
			t.Fatalf("publishPhase() error = %v, want nil", err)
		}
		if result != "publish_budget_exhausted" {
			t.Fatalf("publishPhase() result = %q, want publish_budget_exhausted", result)
		}
		sink.assertCallCount(t, 0)
	})

	t.Run("publish phase converts retryable failure into retry budget exhaustion", func(t *testing.T) {
		t.Parallel()

		sink := newScriptedSink(sinkOutcome{err: &PublishError{Retryable: true, Err: context.DeadlineExceeded}})
		storeDB := &publishTrackingStore{stubStore: &stubStore{}}
		loop := &Loop{
			market:          testLoopMarket(),
			store:           storeDB,
			sink:            sink,
			clock:           clock.Fixed{Time: time.Date(2026, 3, 27, 13, 30, 0, 0, time.UTC)},
			metricsRegistry: metrics.NewRegistry(),
			publishability:  defaultPublishability("ETHUSD", time.Date(2026, 3, 27, 13, 30, 0, 0, time.UTC)),
			budgets:         PhaseBudgets{Publish: time.Millisecond},
			attempts:        AttemptBudgets{PublishAttemptTimeout: 10 * time.Millisecond},
		}

		_, result, err := loop.publishPhase(context.Background(), testPreparedPublication(19), "cycle-retry-budget")
		if err != nil {
			t.Fatalf("publishPhase() error = %v, want nil", err)
		}
		if result != "retry_budget_exhausted" {
			t.Fatalf("publishPhase() result = %q, want retry_budget_exhausted", result)
		}
		if state := loop.currentPublishability(); state.Reason != status.ReasonSinkUnavailable {
			t.Fatalf("publishability reason = %q, want %q", state.Reason, status.ReasonSinkUnavailable)
		}
	})
}

func TestLoopPublishSuccessAndSaveFailurePaths(t *testing.T) {
	t.Parallel()

	t.Run("handle publish success falls back to attempt start when ack time is empty", func(t *testing.T) {
		t.Parallel()

		storeDB := &publishTrackingStore{stubStore: &stubStore{}}
		attemptStarted := time.Date(2026, 3, 27, 14, 0, 0, 0, time.UTC)
		publication := testPreparedPublication(20)
		loop := &Loop{
			market:         testLoopMarket(),
			store:          storeDB,
			publishability: defaultPublishability("ETHUSD", attemptStarted),
			pending:        &publication,
		}

		latency, result, err := loop.handlePublishSuccess(context.Background(), publication, Ack{}, attemptStarted, 25*time.Millisecond, "cycle-success")
		if err != nil {
			t.Fatalf("handlePublishSuccess() error = %v", err)
		}
		if latency != 25*time.Millisecond || result != "acked" {
			t.Fatalf("handlePublishSuccess() = (%s, %q), want (25ms, acked)", latency, result)
		}
		if storeDB.markedSentAt.IsZero() || !storeDB.markedSentAt.Equal(attemptStarted.UTC()) {
			t.Fatalf("markedSentAt = %s, want %s", storeDB.markedSentAt, attemptStarted.UTC())
		}
		if storeDB.ackedAt.IsZero() || !storeDB.ackedAt.Equal(attemptStarted.UTC()) {
			t.Fatalf("ackedAt = %s, want %s", storeDB.ackedAt, attemptStarted.UTC())
		}
		if len(storeDB.saved) != 1 {
			t.Fatalf("saved publishability states = %d, want 1", len(storeDB.saved))
		}
		if loop.pending != nil {
			t.Fatalf("loop.pending = %+v, want nil", loop.pending)
		}
		if got := loop.Metrics().PublicationsAcked; got != 1 {
			t.Fatalf("PublicationsAcked = %d, want 1", got)
		}
	})

	t.Run("save publishability failure marks loop unpublishable", func(t *testing.T) {
		t.Parallel()

		storeDB := &publishTrackingStore{
			stubStore: &stubStore{},
			saveErr:   errors.New("save failed"),
		}
		now := time.Date(2026, 3, 27, 14, 15, 0, 0, time.UTC)
		loop := &Loop{
			market:          testLoopMarket(),
			store:           storeDB,
			clock:           clock.Fixed{Time: now},
			metricsRegistry: metrics.NewRegistry(),
			publishability:  defaultPublishability("ETHUSD", now),
		}

		err := loop.savePublishability(context.Background(), "cycle-save-fail", 21, "key-21")
		if err == nil {
			t.Fatal("savePublishability() error = nil, want failure")
		}
		if !loop.blocked {
			t.Fatal("loop.blocked = false, want true")
		}
		if state := loop.currentPublishability(); state.Status != status.StatusUnavailable || state.Reason != status.ReasonUnpublishable {
			t.Fatalf("publishability = {%q %q}, want {unavailable unpublishable}", state.Status, state.Reason)
		}
	})
}

func TestLoopObserveFetchMetricsTracksRateLimitsAndClampsNegativeAges(t *testing.T) {
	t.Parallel()

	registry := metrics.NewRegistry()
	loop := &Loop{
		market:          testLoopMarket(),
		metricsRegistry: registry,
	}
	observedAt := time.Date(2026, 3, 27, 15, 0, 0, 0, time.UTC)
	quote := relayerQuote(t, "coinbase", "ETHUSD", "1981.75", observedAt.Add(10*time.Second))

	loop.observeFetchMetrics(observedAt, []feeds.Quote{quote}, []error{
		&feeds.Error{Source: "coinbase", Kind: feeds.ErrorRateLimited, Reason: status.ReasonSourceRejected, Err: context.DeadlineExceeded},
		&feeds.Error{Source: "coinbase", Kind: feeds.ErrorCooldown, Reason: status.ReasonSourceRejected, Err: errors.New("cooling down")},
		errors.New("non-feed error"),
	})

	families, err := registry.Gatherer().Gather()
	if err != nil {
		t.Fatalf("Gather() error = %v", err)
	}

	testsupport.AssertMetricGauge(t, families, "market_relayer_source_freshness_seconds", map[string]string{
		"market":     "ETHUSD",
		"source":     "coinbase",
		"basis_kind": "source_ts",
	}, 0)
	testsupport.AssertMetricCounter(t, families, "market_relayer_rejected_updates_total", map[string]string{
		"market": "ETHUSD",
		"source": "coinbase",
		"reason": "source_rejected",
	}, 2)
	testsupport.AssertMetricCounter(t, families, "market_relayer_rate_limit_cooldowns_total", map[string]string{
		"market": "ETHUSD",
		"source": "coinbase",
	}, 2)
}

func newLoopForEdgeTests(t *testing.T) *Loop {
	t.Helper()

	loop, err := NewLoop(LoopOptions{
		Market:   testLoopMarket(),
		Metadata: testLoopMetadata(),
		Store:    &stubStore{},
		Sink:     newScriptedSink(),
		Sources: []feeds.Source{
			&scriptedSource{
				name: "coinbase",
				fn: func(_ context.Context, _ int) (feeds.Quote, error) {
					return relayerQuote(t, "coinbase", "ETHUSD", "1981.75", time.Date(2026, 3, 27, 12, 0, 0, 0, time.UTC)), nil
				},
			},
		},
		Clock: clock.Fixed{Time: time.Date(2026, 3, 27, 12, 0, 0, 0, time.UTC)},
	})
	if err != nil {
		t.Fatalf("NewLoop() error = %v", err)
	}
	return loop
}

type publishTrackingStore struct {
	*stubStore
	saved        []store.PublishabilityState
	saveErr      error
	markedSentAt time.Time
	ackedAt      time.Time
}

func (storeDB *publishTrackingStore) MarkPublicationSent(_ context.Context, _ string, _ uint64, sentAt time.Time) error {
	storeDB.markedSentAt = sentAt.UTC()
	return nil
}

func (storeDB *publishTrackingStore) AcknowledgePublication(_ context.Context, _ string, _ uint64, ackedAt time.Time) error {
	storeDB.ackedAt = ackedAt.UTC()
	return nil
}

func (storeDB *publishTrackingStore) SavePublishability(_ context.Context, state store.PublishabilityState) error {
	if storeDB.saveErr != nil {
		return storeDB.saveErr
	}
	storeDB.saved = append(storeDB.saved, state)
	return nil
}
