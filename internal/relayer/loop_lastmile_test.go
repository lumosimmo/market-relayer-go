package relayer

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/lumosimmo/market-relayer-go/internal/clock"
	"github.com/lumosimmo/market-relayer-go/internal/config"
	"github.com/lumosimmo/market-relayer-go/internal/feeds"
	"github.com/lumosimmo/market-relayer-go/internal/fixedpoint"
	"github.com/lumosimmo/market-relayer-go/internal/metrics"
	"github.com/lumosimmo/market-relayer-go/internal/pricing"
	"github.com/lumosimmo/market-relayer-go/internal/status"
	"github.com/lumosimmo/market-relayer-go/internal/store"
	"github.com/lumosimmo/market-relayer-go/internal/testsupport"
	dto "github.com/prometheus/client_model/go"
)

func TestLoopRunTracksTickOverrunsAndReturnsCanceledCycleErrors(t *testing.T) {
	t.Parallel()

	start := time.Date(2026, 3, 27, 16, 0, 0, 0, time.UTC)
	testClock := newManualClock(start)
	registry := metrics.NewRegistry()
	ctx, cancel := context.WithCancel(context.Background())
	sink := newScriptedSink(sinkOutcome{ack: Ack{}})
	source := &scriptedSource{
		name: "coinbase",
		fn: func(_ context.Context, call int) (feeds.Quote, error) {
			switch call {
			case 0:
				testClock.Advance(5 * time.Second)
			case 1:
				cancel()
			}
			return relayerQuote(t, "coinbase", "ETHUSD", "1981.75", testClock.Now()), nil
		},
	}

	loop, err := NewLoop(LoopOptions{
		Market:   testLoopMarket(),
		Metadata: testLoopMetadata(),
		Store:    &stubStore{},
		Sink:     sink,
		Sources:  []feeds.Source{source},
		Clock:    testClock,
		Waiter:   testClock,
		Metrics:  registry,
		Budgets:  PhaseBudgets{Fetch: 200 * time.Millisecond, Compute: 100 * time.Millisecond, Persist: 100 * time.Millisecond, Publish: 200 * time.Millisecond},
		Attempts: AttemptBudgets{PublishAttemptTimeout: 100 * time.Millisecond},
	})
	if err != nil {
		t.Fatalf("NewLoop() error = %v", err)
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- loop.Run(ctx)
	}()

	source.waitCalls(t, 1)
	sink.waitCalls(t, 1)
	testClock.waitForWaiter(t, start.Add(6*time.Second))
	testClock.Advance(time.Second)
	source.waitCalls(t, 2)
	if err := <-errCh; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() error = %v, want context.Canceled", err)
	}

	families, err := registry.Gatherer().Gather()
	if err != nil {
		t.Fatalf("Gather() error = %v", err)
	}
	testsupport.AssertMetricCounter(t, families, "market_relayer_tick_overruns_total", map[string]string{
		"market": "ETHUSD",
	}, 1)
}

func TestLoopLifecycleAndPrepareEdgeCases(t *testing.T) {
	t.Parallel()

	t.Run("new loop surfaces tracker validation errors", func(t *testing.T) {
		t.Parallel()

		market := testLoopMarket()
		market.SessionMode = config.SessionModeScheduled
		_, err := NewLoop(LoopOptions{
			Market:   market,
			Metadata: testLoopMetadata(),
			Store:    &stubStore{},
			Sink:     newScriptedSink(),
			Sources: []feeds.Source{
				&scriptedSource{
					name: "coinbase",
					fn: func(_ context.Context, _ int) (feeds.Quote, error) {
						return relayerQuote(t, "coinbase", "ETHUSD", "1981.75", time.Date(2026, 3, 27, 16, 15, 0, 0, time.UTC)), nil
					},
				},
			},
		})
		if err == nil {
			t.Fatal("NewLoop(scheduled session mode) error = nil, want validation error")
		}
	})

	t.Run("run and initialize surface store bootstrap errors", func(t *testing.T) {
		t.Parallel()

		runLoop := newLoopForEdgeTests(t)
		runLoop.store = &initErrorStore{
			stubStore:      &stubStore{},
			latestStateErr: errors.New("latest state failed"),
		}
		if err := runLoop.Run(context.Background()); err == nil || err.Error() != "latest state failed" {
			t.Fatalf("Run() error = %v, want latest state failure", err)
		}

		publishabilityLoop := newLoopForEdgeTests(t)
		publishabilityLoop.store = &initErrorStore{
			stubStore:               &stubStore{},
			latestPublishabilityErr: errors.New("latest publishability failed"),
		}
		if err := publishabilityLoop.initialize(context.Background()); err == nil || err.Error() != "latest publishability failed" {
			t.Fatalf("initialize() error = %v, want latest publishability failure", err)
		}

		safeSequenceLoop := newLoopForEdgeTests(t)
		safeSequenceLoop.store = &initErrorStore{
			stubStore:       &stubStore{},
			safeSequenceErr: errors.New("safe sequence failed"),
		}
		if err := safeSequenceLoop.initialize(context.Background()); err == nil || err.Error() != "safe sequence failed" {
			t.Fatalf("initialize() error = %v, want safe sequence failure", err)
		}
		if safeSequenceLoop.initialized {
			t.Fatal("initialize() marked loop initialized after safe sequence failure")
		}
	})

	t.Run("status and cycle publishability expose pending and default state", func(t *testing.T) {
		t.Parallel()

		now := time.Date(2026, 3, 27, 16, 30, 0, 0, time.UTC)
		publication := testPreparedPublication(30)
		loop := &Loop{
			market:  testLoopMarket(),
			pending: &publication,
		}

		snapshot := loop.Status()
		if snapshot.PendingSequence != publication.Sequence {
			t.Fatalf("Status().PendingSequence = %d, want %d", snapshot.PendingSequence, publication.Sequence)
		}

		loop.publishability = store.PublishabilityState{}
		state := loop.publishabilityForCycle(now, nil)
		if state.Status != status.StatusAvailable {
			t.Fatalf("publishabilityForCycle().Status = %q, want %q", state.Status, status.StatusAvailable)
		}
		if state.Reason != status.ReasonNone {
			t.Fatalf("publishabilityForCycle().Reason = %q, want %q", state.Reason, status.ReasonNone)
		}
	})

	t.Run("prepare cycle record surfaces compute and publication preparation errors", func(t *testing.T) {
		t.Parallel()

		now := time.Date(2026, 3, 27, 16, 45, 0, 0, time.UTC)
		loop := newLoopForEdgeTests(t)
		loop.clock = clock.Fixed{Time: now}

		if _, _, _, err := loop.prepareCycleRecord(context.Background(), "cycle-invalid", now, []feeds.Quote{
			relayerQuote(t, "coinbase", "BTCUSD", "65941.00", now),
		}, nil); err == nil {
			t.Fatal("prepareCycleRecord(invalid quote) error = nil, want compute failure")
		}

		pending := testPreparedPublication(31)
		loop.pending = &pending
		record, prepared, _, err := loop.prepareCycleRecord(context.Background(), "cycle-pending", now, []feeds.Quote{
			relayerQuote(t, "coinbase", "ETHUSD", "1981.75", now),
		}, nil)
		if err != nil {
			t.Fatalf("prepareCycleRecord(with pending) error = %v", err)
		}
		if prepared != nil || record.PreparedPublication != nil {
			t.Fatalf("prepareCycleRecord(with pending) prepared = %+v, want nil", prepared)
		}

		loop.pending = nil
		loop.nextSequence = 0
		if _, _, _, err := loop.prepareCycleRecord(context.Background(), "cycle-sequence", now, []feeds.Quote{
			relayerQuote(t, "coinbase", "ETHUSD", "1981.75", now),
		}, nil); err == nil {
			t.Fatal("prepareCycleRecord(sequence=0) error = nil, want preparation failure")
		}
	})

	t.Run("observe clamp ignores fallback or invalid clamp inputs", func(t *testing.T) {
		t.Parallel()

		registry := metrics.NewRegistry()
		market := testLoopMarket()
		market.ClampBPS = -1
		priorMark := mustParseFixedpoint(t, "1981.75")
		oracleValue := mustParseFixedpoint(t, "1982.75")
		loop := &Loop{
			market:          market,
			metricsRegistry: registry,
		}

		loop.observeClamp(pricing.State{
			LastOracle: &priorMark,
		}, pricing.Result{
			Oracle: pricing.Price{
				Status: status.StatusDegraded,
				Reason: status.ReasonFallbackActive,
				Value:  &oracleValue,
			},
		})
		loop.observeClamp(pricing.State{
			LastOracle: &priorMark,
		}, pricing.Result{
			Oracle: pricing.Price{
				Status: status.StatusAvailable,
				Reason: status.ReasonNone,
				Value:  &oracleValue,
			},
		})

		families, err := registry.Gatherer().Gather()
		if err != nil {
			t.Fatalf("Gather() error = %v", err)
		}
		assertMetricCounterMissing(t, families, "market_relayer_oracle_clamp_events_total", map[string]string{
			"market": "ETHUSD",
		})
	})
}

func TestLoopPublishAndReplayFailureEdges(t *testing.T) {
	t.Parallel()

	t.Run("publish phase converts terminal sink failures into terminal result", func(t *testing.T) {
		t.Parallel()

		now := time.Date(2026, 3, 27, 17, 0, 0, 0, time.UTC)
		storeDB := &checkpointStore{stubStore: &stubStore{}}
		loop := &Loop{
			market:         testLoopMarket(),
			store:          storeDB,
			sink:           newScriptedSink(sinkOutcome{err: errors.New("terminal sink failure")}),
			clock:          clock.Fixed{Time: now},
			publishability: defaultPublishability("ETHUSD", now),
			budgets:        PhaseBudgets{Publish: 100 * time.Millisecond},
			attempts:       AttemptBudgets{PublishAttemptTimeout: 50 * time.Millisecond},
		}

		_, result, err := loop.publishPhase(context.Background(), testPreparedPublication(32), "cycle-terminal")
		if err != nil {
			t.Fatalf("publishPhase() error = %v, want nil", err)
		}
		if result != "terminal_failure" {
			t.Fatalf("publishPhase() result = %q, want terminal_failure", result)
		}
		if !loop.blocked {
			t.Fatal("loop.blocked = false, want true")
		}
	})

	t.Run("handle publish success reports sent and ack checkpoint failures", func(t *testing.T) {
		t.Parallel()

		attemptStarted := time.Date(2026, 3, 27, 17, 15, 0, 0, time.UTC)
		ackedAt := time.Date(2026, 3, 27, 18, 15, 0, 0, time.FixedZone("CET", 3600))
		publication := testPreparedPublication(33)

		sentFailureStore := &checkpointStore{
			stubStore: &stubStore{},
			markErr:   errors.New("mark sent failed"),
		}
		sentFailureLoop := &Loop{
			market:          testLoopMarket(),
			store:           sentFailureStore,
			clock:           clock.Fixed{Time: attemptStarted},
			metricsRegistry: metrics.NewRegistry(),
			publishability:  defaultPublishability("ETHUSD", attemptStarted),
			pending:         &publication,
		}
		_, result, err := sentFailureLoop.handlePublishSuccess(context.Background(), publication, Ack{AckedAt: ackedAt}, attemptStarted, 15*time.Millisecond, "cycle-sent")
		if err != nil {
			t.Fatalf("handlePublishSuccess(sent failure) error = %v", err)
		}
		if result != "sent_checkpoint_failed" {
			t.Fatalf("handlePublishSuccess(sent failure) result = %q, want sent_checkpoint_failed", result)
		}
		if sentFailureStore.ackedAt != (time.Time{}) {
			t.Fatalf("ackedAt after sent failure = %s, want zero", sentFailureStore.ackedAt)
		}
		if !sentFailureLoop.blocked {
			t.Fatal("loop.blocked after sent checkpoint failure = false, want true")
		}

		ackFailureStore := &checkpointStore{
			stubStore: &stubStore{},
			ackErr:    errors.New("ack failed"),
		}
		ackFailureLoop := &Loop{
			market:          testLoopMarket(),
			store:           ackFailureStore,
			clock:           clock.Fixed{Time: attemptStarted},
			metricsRegistry: metrics.NewRegistry(),
			publishability:  defaultPublishability("ETHUSD", attemptStarted),
			pending:         &publication,
		}
		_, result, err = ackFailureLoop.handlePublishSuccess(context.Background(), publication, Ack{AckedAt: ackedAt}, attemptStarted, 15*time.Millisecond, "cycle-ack")
		if err != nil {
			t.Fatalf("handlePublishSuccess(ack failure) error = %v", err)
		}
		if result != "ack_checkpoint_failed" {
			t.Fatalf("handlePublishSuccess(ack failure) result = %q, want ack_checkpoint_failed", result)
		}
		if !ackFailureStore.ackedAt.Equal(ackedAt.UTC()) {
			t.Fatalf("ackedAt = %s, want %s", ackFailureStore.ackedAt, ackedAt.UTC())
		}
		if !ackFailureLoop.blocked {
			t.Fatal("loop.blocked after ack checkpoint failure = false, want true")
		}
	})

	t.Run("publishability persistence failures block successful or terminal publish paths", func(t *testing.T) {
		t.Parallel()

		now := time.Date(2026, 3, 27, 17, 30, 0, 0, time.UTC)
		publication := testPreparedPublication(34)

		successStore := &checkpointStore{
			stubStore: &stubStore{},
			saveErr:   errors.New("save failed"),
		}
		successLoop := &Loop{
			market:          testLoopMarket(),
			store:           successStore,
			clock:           clock.Fixed{Time: now},
			metricsRegistry: metrics.NewRegistry(),
			publishability:  defaultPublishability("ETHUSD", now),
			pending:         &publication,
		}
		_, result, err := successLoop.handlePublishSuccess(context.Background(), publication, Ack{}, now, 20*time.Millisecond, "cycle-save")
		if err != nil {
			t.Fatalf("handlePublishSuccess(save failure) error = %v", err)
		}
		if result != "publishability_persist_failed" {
			t.Fatalf("handlePublishSuccess(save failure) result = %q, want publishability_persist_failed", result)
		}
		if !successLoop.blocked {
			t.Fatal("loop.blocked after publishability persistence failure = false, want true")
		}

		phaseCtx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
		defer cancel()
		retryLoop := &Loop{
			market:          testLoopMarket(),
			store:           &checkpointStore{stubStore: &stubStore{}, saveErr: errors.New("retry save failed")},
			clock:           clock.Fixed{Time: now},
			metricsRegistry: metrics.NewRegistry(),
			publishability:  defaultPublishability("ETHUSD", now),
		}
		if retry := retryLoop.handleRetryablePublishFailure(phaseCtx, 100*time.Millisecond, publication, "cycle-retry-save", errors.New("temporary failure")); retry {
			t.Fatal("handleRetryablePublishFailure() = true, want false when save fails")
		}
		if !retryLoop.blocked {
			t.Fatal("loop.blocked after retry-budget save failure = false, want true")
		}

		terminalLoop := &Loop{
			market:          testLoopMarket(),
			store:           &checkpointStore{stubStore: &stubStore{}, saveErr: errors.New("terminal save failed")},
			clock:           clock.Fixed{Time: now},
			metricsRegistry: metrics.NewRegistry(),
			publishability:  defaultPublishability("ETHUSD", now),
		}
		terminalLoop.handleTerminalPublishFailure(context.Background(), publication, "cycle-terminal-save", errors.New("terminal failure"))
		if !terminalLoop.blocked {
			t.Fatal("loop.blocked after terminal save failure = false, want true")
		}
	})

	t.Run("resume pending surfaces store and publish errors", func(t *testing.T) {
		t.Parallel()

		now := time.Date(2026, 3, 27, 17, 45, 0, 0, time.UTC)
		pendingErrLoop := &Loop{
			market:   testLoopMarket(),
			metadata: testLoopMetadata(),
			store: &initErrorStore{
				stubStore:  &stubStore{},
				pendingErr: errors.New("pending failed"),
			},
			sink:    newScriptedSink(),
			clock:   clock.Fixed{Time: now},
			budgets: PhaseBudgets{Publish: 100 * time.Millisecond},
			attempts: AttemptBudgets{
				PublishAttemptTimeout: 50 * time.Millisecond,
			},
		}
		if err := pendingErrLoop.resumePendingLocked(context.Background()); err == nil || err.Error() != "pending failed" {
			t.Fatalf("resumePendingLocked() error = %v, want pending failure", err)
		}

		pending := pendingPublicationForMetadataTest(t, testLoopMetadata())
		replayCtx, replayCancel := context.WithCancel(context.Background())
		sink := newScriptedSink(sinkOutcome{waitForContextCancel: true})
		publishErrLoop := &Loop{
			market:          testLoopMarket(),
			metadata:        testLoopMetadata(),
			store:           &stubStore{pending: []store.PreparedPublication{pending}},
			sink:            sink,
			clock:           clock.Fixed{Time: now},
			metricsRegistry: metrics.NewRegistry(),
			publishability:  defaultPublishability("ETHUSD", now),
			budgets:         PhaseBudgets{Publish: 100 * time.Millisecond},
			attempts:        AttemptBudgets{PublishAttemptTimeout: 50 * time.Millisecond},
		}

		done := make(chan struct{})
		go func() {
			sink.waitCalls(t, 1)
			replayCancel()
			close(done)
		}()
		err := publishErrLoop.resumePendingLocked(replayCtx)
		<-done
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("resumePendingLocked() error = %v, want context.Canceled", err)
		}

		softFailureLoop := &Loop{
			market:   testLoopMarket(),
			metadata: testLoopMetadata(),
			store:    &stubStore{pending: []store.PreparedPublication{pending}},
			sink: newScriptedSink(sinkOutcome{
				err: &PublishError{Retryable: false, Err: errors.New("terminal failure")},
			}),
			clock:           clock.Fixed{Time: now},
			metricsRegistry: metrics.NewRegistry(),
			publishability:  defaultPublishability("ETHUSD", now),
			budgets:         PhaseBudgets{Publish: 100 * time.Millisecond},
			attempts:        AttemptBudgets{PublishAttemptTimeout: 50 * time.Millisecond},
		}
		if err := softFailureLoop.resumePendingLocked(context.Background()); err == nil || !strings.Contains(err.Error(), "startup replay") || !strings.Contains(err.Error(), "terminal_failure") {
			t.Fatalf("resumePendingLocked(soft failure) error = %v, want startup replay terminal_failure", err)
		}
	})
}

type initErrorStore struct {
	*stubStore
	latestStateErr          error
	latestPublishabilityErr error
	safeSequenceErr         error
	pendingErr              error
	latestPublishability    store.PublishabilityState
}

func (storeDB *initErrorStore) PendingPublications(_ context.Context, _ string) ([]store.PreparedPublication, error) {
	if storeDB.pendingErr != nil {
		return nil, storeDB.pendingErr
	}
	return append([]store.PreparedPublication(nil), storeDB.pending...), nil
}

func (storeDB *initErrorStore) SafeSequence(context.Context, string) (uint64, error) {
	if storeDB.safeSequenceErr != nil {
		return 0, storeDB.safeSequenceErr
	}
	return 0, nil
}

func (storeDB *initErrorStore) LatestState(context.Context, string) (pricing.State, error) {
	if storeDB.latestStateErr != nil {
		return pricing.State{}, storeDB.latestStateErr
	}
	return pricing.State{}, nil
}

func (storeDB *initErrorStore) LatestPublishability(context.Context, string) (store.PublishabilityState, error) {
	if storeDB.latestPublishabilityErr != nil {
		return store.PublishabilityState{}, storeDB.latestPublishabilityErr
	}
	return storeDB.latestPublishability, nil
}

type checkpointStore struct {
	*stubStore
	markErr      error
	ackErr       error
	saveErr      error
	markedSentAt time.Time
	ackedAt      time.Time
}

func (storeDB *checkpointStore) MarkPublicationSent(_ context.Context, _ string, _ uint64, sentAt time.Time) error {
	if storeDB.markErr != nil {
		return storeDB.markErr
	}
	storeDB.markedSentAt = sentAt.UTC()
	return nil
}

func (storeDB *checkpointStore) AcknowledgePublication(_ context.Context, _ string, _ uint64, ackedAt time.Time) error {
	storeDB.ackedAt = ackedAt.UTC()
	if storeDB.ackErr != nil {
		return storeDB.ackErr
	}
	return nil
}

func (storeDB *checkpointStore) SavePublishability(context.Context, store.PublishabilityState) error {
	return storeDB.saveErr
}

func assertMetricCounterMissing(t *testing.T, families []*dto.MetricFamily, name string, labels map[string]string) {
	t.Helper()

	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.Metric {
			if testsupport.MetricLabelsEqual(metric, labels) {
				t.Fatalf("metric %s labels=%v unexpectedly present", name, labels)
			}
		}
	}
}

func mustParseFixedpoint(t *testing.T, value string) fixedpoint.Value {
	t.Helper()

	parsed, err := fixedpoint.ParseDecimal(value)
	if err != nil {
		t.Fatalf("fixedpoint.ParseDecimal(%q) error = %v", value, err)
	}
	return parsed
}
