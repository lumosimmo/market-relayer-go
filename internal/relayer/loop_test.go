package relayer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lumosimmo/market-relayer-go/internal/clock"
	"github.com/lumosimmo/market-relayer-go/internal/config"
	"github.com/lumosimmo/market-relayer-go/internal/feeds"
	"github.com/lumosimmo/market-relayer-go/internal/fixedpoint"
	"github.com/lumosimmo/market-relayer-go/internal/pricing"
	"github.com/lumosimmo/market-relayer-go/internal/status"
	"github.com/lumosimmo/market-relayer-go/internal/store"
	"github.com/lumosimmo/market-relayer-go/internal/testdb"
)

func TestLoopPublishesAtConfiguredCadenceUnderManualClock(t *testing.T) {
	t.Parallel()

	start := time.Date(2026, 3, 27, 12, 0, 0, 0, time.UTC)
	testClock := newManualClock(start)
	storeDB := openLoopStore(t)
	sink := newScriptedSink(
		sinkOutcome{ack: Ack{}},
		sinkOutcome{ack: Ack{}},
		sinkOutcome{ack: Ack{}},
	)
	source := &scriptedSource{
		name: "coinbase",
		fn: func(_ context.Context, call int) (feeds.Quote, error) {
			return relayerQuote(t, "coinbase", "ETHUSD", fmt.Sprintf("1981.%02d", 75+call), testClock.Now()), nil
		},
	}

	loop, err := NewLoop(LoopOptions{
		Market:   testLoopMarket(),
		Metadata: testLoopMetadata(),
		Store:    storeDB,
		Sink:     sink,
		Sources:  []feeds.Source{source},
		Clock:    testClock,
		Waiter:   testClock,
		Budgets:  storeBackedPhaseBudgets(200 * time.Millisecond),
		Attempts: AttemptBudgets{PublishAttemptTimeout: 100 * time.Millisecond},
	})
	if err != nil {
		t.Fatalf("NewLoop() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- loop.Run(ctx)
	}()

	sink.waitCalls(t, 1)
	if got := len(sink.publications()); got != 1 {
		t.Fatalf("publications after immediate cycle = %d, want 1", got)
	}

	testClock.Advance(2 * time.Second)
	sink.assertCallCount(t, 1)

	testClock.Advance(time.Second)
	sink.waitCalls(t, 2)

	testClock.Advance(3 * time.Second)
	sink.waitCalls(t, 3)

	cancel()
	testClock.Advance(time.Second)

	if err := <-errCh; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() error = %v, want nil or context.Canceled", err)
	}
}

func TestLoopWaitsUntilNextFutureTickAfterPartialOverrun(t *testing.T) {
	t.Parallel()

	start := time.Date(2026, 3, 27, 12, 30, 0, 0, time.UTC)
	testClock := newManualClock(start)
	storeDB := openLoopStore(t)
	sink := newScriptedSink(
		sinkOutcome{ack: Ack{}},
		sinkOutcome{ack: Ack{}},
	)

	secondFetchRelease := make(chan struct{})
	source := &scriptedSource{
		name: "coinbase",
		fn: func(_ context.Context, call int) (feeds.Quote, error) {
			switch call {
			case 0:
				testClock.Advance(4 * time.Second)
			case 1:
				<-secondFetchRelease
			}
			return relayerQuote(t, "coinbase", "ETHUSD", fmt.Sprintf("1981.%02d", 75+call), testClock.Now()), nil
		},
	}

	loop, err := NewLoop(LoopOptions{
		Market:   testLoopMarket(),
		Metadata: testLoopMetadata(),
		Store:    storeDB,
		Sink:     sink,
		Sources:  []feeds.Source{source},
		Clock:    testClock,
		Waiter:   testClock,
		Budgets:  storeBackedPhaseBudgets(200 * time.Millisecond),
		Attempts: AttemptBudgets{PublishAttemptTimeout: 100 * time.Millisecond},
	})
	if err != nil {
		t.Fatalf("NewLoop() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- loop.Run(ctx)
	}()

	sink.waitCalls(t, 1)
	source.assertCallCount(t, 1)
	testClock.waitForWaiter(t, start.Add(6*time.Second))

	testClock.Advance(time.Second)
	source.assertCallCount(t, 1)

	testClock.Advance(time.Second)
	source.waitCalls(t, 2)
	close(secondFetchRelease)
	sink.waitCalls(t, 2)

	cancel()
	testClock.Advance(time.Second)

	if err := <-errCh; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() error = %v, want nil or context.Canceled", err)
	}
}

func TestLoopDeduplicatesTimestampOnlyChangesButRepublishesSessionTransition(t *testing.T) {
	t.Parallel()

	start := time.Date(2026, 3, 27, 13, 0, 0, 0, time.UTC)
	testClock := newManualClock(start)
	storeDB := openLoopStore(t)
	sink := newScriptedSink(
		sinkOutcome{ack: Ack{}},
		sinkOutcome{ack: Ack{}},
	)

	source := &scriptedSource{
		name: "coinbase",
		fn: func(_ context.Context, call int) (feeds.Quote, error) {
			switch call {
			case 0, 1:
				return relayerQuote(t, "coinbase", "ETHUSD", "1981.75", testClock.Now()), nil
			default:
				return feeds.Quote{}, &feeds.Error{
					Source:    "coinbase",
					Kind:      feeds.ErrorTransport,
					Reason:    status.ReasonSourceRejected,
					Retryable: false,
					Err:       errors.New("source offline"),
				}
			}
		},
	}

	loop, err := NewLoop(LoopOptions{
		Market:   testLoopMarket(),
		Metadata: testLoopMetadata(),
		Store:    storeDB,
		Sink:     sink,
		Sources:  []feeds.Source{source},
		Clock:    testClock,
		Waiter:   testClock,
		Budgets:  storeBackedPhaseBudgets(200 * time.Millisecond),
		Attempts: AttemptBudgets{PublishAttemptTimeout: 100 * time.Millisecond},
	})
	if err != nil {
		t.Fatalf("NewLoop() error = %v", err)
	}

	if err := loop.RunCycle(context.Background()); err != nil {
		t.Fatalf("first RunCycle() error = %v", err)
	}
	sink.assertCallCount(t, 1)
	first := sink.publications()[0]

	testClock.Advance(3 * time.Second)
	if err := loop.RunCycle(context.Background()); err != nil {
		t.Fatalf("second RunCycle() error = %v", err)
	}
	sink.assertCallCount(t, 1)

	testClock.Advance(3 * time.Second)
	if err := loop.RunCycle(context.Background()); err != nil {
		t.Fatalf("third RunCycle() error = %v", err)
	}
	sink.assertCallCount(t, 2)

	second := sink.publications()[1]
	firstEnvelope := decodeTestEnvelope(t, first.Envelope)
	secondEnvelope := decodeTestEnvelope(t, second.Envelope)
	if firstEnvelope.Payload.Session.State != status.SessionStateOpen {
		t.Fatalf("first session state = %q, want %q", firstEnvelope.Payload.Session.State, status.SessionStateOpen)
	}
	if secondEnvelope.Payload.Session.State != status.SessionStateFallbackOnly {
		t.Fatalf("second session state = %q, want %q", secondEnvelope.Payload.Session.State, status.SessionStateFallbackOnly)
	}
	if secondEnvelope.Payload.Oracle.Status != status.StatusDegraded {
		t.Fatalf("second oracle status = %q, want %q", secondEnvelope.Payload.Oracle.Status, status.StatusDegraded)
	}
	if secondEnvelope.Payload.Oracle.Reason != status.ReasonFallbackActive {
		t.Fatalf("second oracle reason = %q, want %q", secondEnvelope.Payload.Oracle.Reason, status.ReasonFallbackActive)
	}
}

func TestLoopRetriesRetryableSinkFailureWithStableSequenceAndIdempotency(t *testing.T) {
	t.Parallel()

	storeDB := openLoopStore(t)
	sink := newScriptedSink(
		sinkOutcome{err: &PublishError{Retryable: true, Err: context.DeadlineExceeded}},
		sinkOutcome{ack: Ack{}},
	)
	source := &scriptedSource{
		name: "coinbase",
		fn: func(_ context.Context, _ int) (feeds.Quote, error) {
			return relayerQuote(t, "coinbase", "ETHUSD", "1983.00", time.Date(2026, 3, 27, 14, 0, 0, 0, time.UTC)), nil
		},
	}

	loop, err := NewLoop(LoopOptions{
		Market:   testLoopMarket(),
		Metadata: testLoopMetadata(),
		Store:    storeDB,
		Sink:     sink,
		Sources:  []feeds.Source{source},
		Clock:    clock.Fixed{Time: time.Date(2026, 3, 27, 14, 0, 0, 0, time.UTC)},
		Budgets:  storeBackedPhaseBudgets(300 * time.Millisecond),
		Attempts: AttemptBudgets{PublishAttemptTimeout: 100 * time.Millisecond},
	})
	if err != nil {
		t.Fatalf("NewLoop() error = %v", err)
	}

	if err := loop.RunCycle(context.Background()); err != nil {
		t.Fatalf("RunCycle() error = %v", err)
	}

	publications := sink.publications()
	if len(publications) != 2 {
		t.Fatalf("len(publications) = %d, want 2", len(publications))
	}
	if publications[0].Sequence != publications[1].Sequence {
		t.Fatalf("sequence changed across retry: %d != %d", publications[0].Sequence, publications[1].Sequence)
	}
	if publications[0].IdempotencyKey != publications[1].IdempotencyKey {
		t.Fatalf("idempotency key changed across retry: %q != %q", publications[0].IdempotencyKey, publications[1].IdempotencyKey)
	}

	safeSequence, err := storeDB.SafeSequence(context.Background(), testLoopMarket().Symbol)
	if err != nil {
		t.Fatalf("SafeSequence() error = %v", err)
	}
	if safeSequence != publications[0].Sequence {
		t.Fatalf("SafeSequence() = %d, want %d", safeSequence, publications[0].Sequence)
	}
}

func TestLoopCoalescesMissedTicksWithoutOverlap(t *testing.T) {
	t.Parallel()

	start := time.Date(2026, 3, 27, 15, 0, 0, 0, time.UTC)
	testClock := newManualClock(start)
	storeDB := openLoopStore(t)
	sink := newScriptedSink(
		sinkOutcome{ack: Ack{}},
		sinkOutcome{ack: Ack{}},
	)
	release := make(chan struct{})
	source := &scriptedSource{
		name: "coinbase",
		fn: func(ctx context.Context, call int) (feeds.Quote, error) {
			if call == 0 {
				<-release
			}
			return relayerQuote(t, "coinbase", "ETHUSD", fmt.Sprintf("102.%02d", call), testClock.Now()), nil
		},
	}

	loop, err := NewLoop(LoopOptions{
		Market:   testLoopMarket(),
		Metadata: testLoopMetadata(),
		Store:    storeDB,
		Sink:     sink,
		Sources:  []feeds.Source{source},
		Clock:    testClock,
		Waiter:   testClock,
		Budgets:  storeBackedPhaseBudgets(200 * time.Millisecond),
		Attempts: AttemptBudgets{PublishAttemptTimeout: 100 * time.Millisecond},
	})
	if err != nil {
		t.Fatalf("NewLoop() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() {
		errCh <- loop.Run(ctx)
	}()

	source.waitCalls(t, 1)
	testClock.Advance(10 * time.Second)
	source.assertCallCount(t, 1)

	close(release)
	sink.waitCalls(t, 1)
	testClock.waitForWaiter(t, start.Add(12*time.Second))

	testClock.Advance(2 * time.Second)
	source.waitCalls(t, 2)
	sink.waitCalls(t, 2)
	source.assertCallCount(t, 2)

	testClock.waitForWaiter(t, start.Add(15*time.Second))
	testClock.Advance(3 * time.Second)
	source.waitCalls(t, 3)
	if got := source.maxConcurrent(); got != 1 {
		t.Fatalf("max concurrent fetches = %d, want 1", got)
	}

	cancel()
	testClock.Advance(time.Second)
	if err := <-errCh; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() error = %v, want nil or context.Canceled", err)
	}
}

func TestLoopPrepareFailureBecomesUnpublishableAndDoesNotSendBlindly(t *testing.T) {
	t.Parallel()

	baseStore := openLoopStore(t)
	storeWrapper := &failingStore{
		DB:                baseStore,
		failPreparedCycle: true,
	}
	sink := newScriptedSink(sinkOutcome{ack: Ack{}})
	source := &scriptedSource{
		name: "coinbase",
		fn: func(_ context.Context, _ int) (feeds.Quote, error) {
			return relayerQuote(t, "coinbase", "ETHUSD", "1984.85", time.Date(2026, 3, 27, 16, 0, 0, 0, time.UTC)), nil
		},
	}

	loop, err := NewLoop(LoopOptions{
		Market:   testLoopMarket(),
		Metadata: testLoopMetadata(),
		Store:    storeWrapper,
		Sink:     sink,
		Sources:  []feeds.Source{source},
		Clock:    clock.Fixed{Time: time.Date(2026, 3, 27, 16, 0, 0, 0, time.UTC)},
		Budgets:  storeBackedPhaseBudgets(200 * time.Millisecond),
		Attempts: AttemptBudgets{PublishAttemptTimeout: 100 * time.Millisecond},
	})
	if err != nil {
		t.Fatalf("NewLoop() error = %v", err)
	}

	if err := loop.RunCycle(context.Background()); err != nil {
		t.Fatalf("RunCycle() error = %v", err)
	}

	sink.assertCallCount(t, 0)
	snapshot := loop.Status()
	if snapshot.Publishability.Status != status.StatusUnavailable {
		t.Fatalf("publishability status = %q, want %q", snapshot.Publishability.Status, status.StatusUnavailable)
	}
	if snapshot.Publishability.Reason != status.ReasonUnpublishable {
		t.Fatalf("publishability reason = %q, want %q", snapshot.Publishability.Reason, status.ReasonUnpublishable)
	}
}

func TestLoopRestartRecoveryDrainsPendingBeforeAllocatingNewSequence(t *testing.T) {
	t.Parallel()

	storeDB := openLoopStore(t)
	t.Cleanup(func() {
		if closeErr := storeDB.Close(); closeErr != nil {
			t.Fatalf("store.Close() error = %v", closeErr)
		}
	})

	firstSink := newScriptedSink(sinkOutcome{ack: Ack{}})
	firstStore := &failingStore{
		DB:      storeDB,
		failAck: true,
	}
	firstSource := &scriptedSource{
		name: "coinbase",
		fn: func(_ context.Context, _ int) (feeds.Quote, error) {
			return relayerQuote(t, "coinbase", "ETHUSD", "1985.75", time.Date(2026, 3, 27, 17, 0, 0, 0, time.UTC)), nil
		},
	}

	firstLoop, err := NewLoop(LoopOptions{
		Market:   testLoopMarket(),
		Metadata: testLoopMetadata(),
		Store:    firstStore,
		Sink:     firstSink,
		Sources:  []feeds.Source{firstSource},
		Clock:    clock.Fixed{Time: time.Date(2026, 3, 27, 17, 0, 0, 0, time.UTC)},
		Budgets:  storeBackedPhaseBudgets(200 * time.Millisecond),
		Attempts: AttemptBudgets{PublishAttemptTimeout: 100 * time.Millisecond},
	})
	if err != nil {
		t.Fatalf("NewLoop(first) error = %v", err)
	}

	if err := firstLoop.RunCycle(context.Background()); err != nil {
		t.Fatalf("first RunCycle() error = %v", err)
	}

	pending, err := storeDB.PendingPublications(context.Background(), testLoopMarket().Symbol)
	if err != nil {
		t.Fatalf("PendingPublications() error = %v", err)
	}
	if len(pending) != 1 || pending[0].State != store.PublicationStateSent {
		t.Fatalf("pending publication after ack persistence failure = %+v, want one sent record", pending)
	}

	restartSink := newScriptedSink(
		sinkOutcome{ack: Ack{Duplicate: true}},
		sinkOutcome{ack: Ack{}},
	)
	restartSource := &scriptedSource{
		name: "coinbase",
		fn: func(_ context.Context, _ int) (feeds.Quote, error) {
			return relayerQuote(t, "coinbase", "ETHUSD", "1986.75", time.Date(2026, 3, 27, 17, 0, 3, 0, time.UTC)), nil
		},
	}

	restarted, err := NewLoop(LoopOptions{
		Market:   testLoopMarket(),
		Metadata: testLoopMetadata(),
		Store:    storeDB,
		Sink:     restartSink,
		Sources:  []feeds.Source{restartSource},
		Clock:    clock.Fixed{Time: time.Date(2026, 3, 27, 17, 0, 3, 0, time.UTC)},
		Budgets:  storeBackedPhaseBudgets(300 * time.Millisecond),
		Attempts: AttemptBudgets{PublishAttemptTimeout: 100 * time.Millisecond},
	})
	if err != nil {
		t.Fatalf("NewLoop(restarted) error = %v", err)
	}

	if err := restarted.RunCycle(context.Background()); err != nil {
		t.Fatalf("restarted RunCycle() error = %v", err)
	}

	publications := restartSink.publications()
	if len(publications) != 2 {
		t.Fatalf("len(restart publications) = %d, want 2", len(publications))
	}
	if publications[0].Sequence != 1 {
		t.Fatalf("recovery sequence = %d, want 1", publications[0].Sequence)
	}
	if publications[1].Sequence != 2 {
		t.Fatalf("new sequence after recovery = %d, want 2", publications[1].Sequence)
	}

	safeSequence, err := storeDB.SafeSequence(context.Background(), testLoopMarket().Symbol)
	if err != nil {
		t.Fatalf("SafeSequence() error = %v", err)
	}
	if safeSequence != 2 {
		t.Fatalf("SafeSequence() after recovery = %d, want 2", safeSequence)
	}
}

func TestLoopRejectsUnsupportedPendingEnvelopeVersionAtStartup(t *testing.T) {
	t.Parallel()

	stubStore := &stubStore{
		pending: []store.PreparedPublication{
			{
				Market:          "ETHUSD",
				Sequence:        1,
				State:           store.PublicationStatePrepared,
				PreparedAt:      time.Date(2026, 3, 27, 18, 0, 0, 0, time.UTC),
				IdempotencyKey:  "pending-key",
				PayloadHash:     "deadbeef",
				EnvelopeVersion: "9",
				Envelope:        []byte(`{"payload":{"market":"ETHUSD"}}`),
			},
		},
	}
	sink := newScriptedSink()
	source := &scriptedSource{
		name: "coinbase",
		fn: func(_ context.Context, _ int) (feeds.Quote, error) {
			return relayerQuote(t, "coinbase", "ETHUSD", "1987.75", time.Date(2026, 3, 27, 18, 0, 0, 0, time.UTC)), nil
		},
	}

	loop, err := NewLoop(LoopOptions{
		Market:   testLoopMarket(),
		Metadata: testLoopMetadata(),
		Store:    stubStore,
		Sink:     sink,
		Sources:  []feeds.Source{source},
		Clock:    clock.Fixed{Time: time.Date(2026, 3, 27, 18, 0, 0, 0, time.UTC)},
		Budgets:  PhaseBudgets{Fetch: 200 * time.Millisecond, Compute: 100 * time.Millisecond, Persist: 100 * time.Millisecond, Publish: 200 * time.Millisecond},
		Attempts: AttemptBudgets{PublishAttemptTimeout: 100 * time.Millisecond},
	})
	if err != nil {
		t.Fatalf("NewLoop() error = %v", err)
	}

	err = loop.RunCycle(context.Background())
	if err == nil {
		t.Fatal("RunCycle() error = nil, want unsupported envelope version")
	}
	if got := err.Error(); got == "" || !strings.Contains(got, "unsupported pending envelope version") {
		t.Fatalf("RunCycle() error = %q, want unsupported envelope version", got)
	}
}

func TestLoopRejectsPendingPublicationMetadataMismatchAtStartup(t *testing.T) {
	t.Parallel()

	pending := pendingPublicationForMetadataTest(t, testLoopMetadata())
	pending.Envelope = rewriteLoopEnvelopeMetadata(t, pending.Envelope, "config-digest-stale", pricing.AlgorithmVersion)

	stubStore := &stubStore{
		pending: []store.PreparedPublication{pending},
	}
	sink := newScriptedSink(sinkOutcome{ack: Ack{}})
	source := &scriptedSource{
		name: "coinbase",
		fn: func(_ context.Context, _ int) (feeds.Quote, error) {
			return relayerQuote(t, "coinbase", "ETHUSD", "1987.75", time.Date(2026, 3, 27, 18, 0, 0, 0, time.UTC)), nil
		},
	}

	loop, err := NewLoop(LoopOptions{
		Market:   testLoopMarket(),
		Metadata: testLoopMetadata(),
		Store:    stubStore,
		Sink:     sink,
		Sources:  []feeds.Source{source},
		Clock:    clock.Fixed{Time: time.Date(2026, 3, 27, 18, 0, 0, 0, time.UTC)},
		Budgets:  PhaseBudgets{Fetch: 200 * time.Millisecond, Compute: 100 * time.Millisecond, Persist: 100 * time.Millisecond, Publish: 200 * time.Millisecond},
		Attempts: AttemptBudgets{PublishAttemptTimeout: 100 * time.Millisecond},
	})
	if err != nil {
		t.Fatalf("NewLoop() error = %v", err)
	}

	err = loop.RunCycle(context.Background())
	if err == nil {
		t.Fatal("RunCycle() error = nil, want metadata mismatch")
	}
	if got := err.Error(); got == "" || !strings.Contains(got, "pending publication metadata mismatch") {
		t.Fatalf("RunCycle() error = %q, want metadata mismatch", got)
	}
	sink.assertCallCount(t, 0)
}

func TestLoopShutdownLeavesPreparedPublicationReplaySafe(t *testing.T) {
	t.Parallel()

	storeDB := openLoopStore(t)
	t.Cleanup(func() {
		if closeErr := storeDB.Close(); closeErr != nil {
			t.Fatalf("store.Close() error = %v", closeErr)
		}
	})

	blockedSink := newScriptedSink(sinkOutcome{waitForContextCancel: true})
	source := &scriptedSource{
		name: "coinbase",
		fn: func(_ context.Context, _ int) (feeds.Quote, error) {
			return relayerQuote(t, "coinbase", "ETHUSD", "1988.75", time.Date(2026, 3, 27, 19, 0, 0, 0, time.UTC)), nil
		},
	}

	loop, err := NewLoop(LoopOptions{
		Market:   testLoopMarket(),
		Metadata: testLoopMetadata(),
		Store:    storeDB,
		Sink:     blockedSink,
		Sources:  []feeds.Source{source},
		Clock:    clock.Fixed{Time: time.Date(2026, 3, 27, 19, 0, 0, 0, time.UTC)},
		Budgets:  storeBackedPhaseBudgets(300 * time.Millisecond),
		Attempts: AttemptBudgets{PublishAttemptTimeout: 200 * time.Millisecond},
	})
	if err != nil {
		t.Fatalf("NewLoop() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- loop.RunCycle(ctx)
	}()

	blockedSink.waitCalls(t, 1)
	cancel()

	if err := <-errCh; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("RunCycle() error = %v, want nil or context.Canceled", err)
	}

	pending, err := storeDB.PendingPublications(context.Background(), testLoopMarket().Symbol)
	if err != nil {
		t.Fatalf("PendingPublications() error = %v", err)
	}
	if len(pending) != 1 || pending[0].State != store.PublicationStatePrepared {
		t.Fatalf("pending after shutdown = %+v, want one prepared publication", pending)
	}

	restartSink := newScriptedSink(
		sinkOutcome{ack: Ack{}},
		sinkOutcome{ack: Ack{}},
	)
	restartSource := &scriptedSource{
		name: "coinbase",
		fn: func(_ context.Context, _ int) (feeds.Quote, error) {
			return relayerQuote(t, "coinbase", "ETHUSD", "1989.75", time.Date(2026, 3, 27, 19, 0, 3, 0, time.UTC)), nil
		},
	}
	restarted, err := NewLoop(LoopOptions{
		Market:   testLoopMarket(),
		Metadata: testLoopMetadata(),
		Store:    storeDB,
		Sink:     restartSink,
		Sources:  []feeds.Source{restartSource},
		Clock:    clock.Fixed{Time: time.Date(2026, 3, 27, 19, 0, 3, 0, time.UTC)},
		Budgets:  storeBackedPhaseBudgets(300 * time.Millisecond),
		Attempts: AttemptBudgets{PublishAttemptTimeout: 100 * time.Millisecond},
	})
	if err != nil {
		t.Fatalf("NewLoop(restarted) error = %v", err)
	}

	if err := restarted.RunCycle(context.Background()); err != nil {
		t.Fatalf("restarted RunCycle() error = %v", err)
	}

	publications := restartSink.publications()
	if len(publications) != 2 {
		t.Fatalf("len(restart publications) = %d, want 2", len(publications))
	}
	if publications[0].Sequence != 1 {
		t.Fatalf("recovered sequence = %d, want 1", publications[0].Sequence)
	}
	if publications[1].Sequence != 2 {
		t.Fatalf("new sequence after replay-safe shutdown = %d, want 2", publications[1].Sequence)
	}
}

func openLoopStore(t *testing.T) *store.DB {
	t.Helper()

	cfg := config.StoreConfig{
		DSN:                strings.TrimSpace(testdb.DSN(t)),
		MaxOpenConns:       4,
		MinOpenConns:       0,
		ConnMaxLifetime:    30 * time.Minute,
		ConnectTimeout:     5 * time.Second,
		LockTimeout:        5 * time.Second,
		LeaseTTL:           15 * time.Second,
		LeaseRenewInterval: 5 * time.Second,
	}
	if _, err := store.Migrate(context.Background(), cfg); err != nil {
		t.Fatalf("store.Migrate() error = %v", err)
	}

	storeDB, err := store.OpenConfigContext(context.Background(), cfg)
	if err != nil {
		t.Fatalf("store.OpenConfigContext() error = %v", err)
	}
	t.Cleanup(func() {
		if closeErr := storeDB.Close(); closeErr != nil {
			t.Fatalf("store.Close() error = %v", closeErr)
		}
	})
	return storeDB
}

func storeBackedPhaseBudgets(publish time.Duration) PhaseBudgets {
	return PhaseBudgets{
		Fetch:   time.Second,
		Compute: time.Second,
		// Real Postgres-backed tests can exceed the narrow persist budget under -race.
		Persist: 3 * time.Second,
		Publish: publish,
	}
}

func testLoopMarket() config.MarketConfig {
	return config.MarketConfig{
		Symbol:                 "ETHUSD",
		SessionMode:            config.SessionModeAlwaysOpen,
		PublishInterval:        3 * time.Second,
		StalenessThreshold:     10 * time.Second,
		MaxFallbackAge:         30 * time.Second,
		ClampBPS:               50,
		DivergenceThresholdBPS: 250,
		PublishScale:           2,
		Sources: []config.MarketSourceRef{
			{Name: "coinbase"},
		},
	}
}

func testLoopMetadata() store.Metadata {
	return store.Metadata{
		ConfigDigest:            "config-digest-relayer-loop-test",
		PricingAlgorithmVersion: pricing.AlgorithmVersion,
		SchemaVersion:           store.SchemaVersion,
		EnvelopeVersion:         EnvelopeVersion,
	}
}

func pendingPublicationForMetadataTest(t *testing.T, metadata store.Metadata) store.PreparedPublication {
	t.Helper()

	at := time.Date(2026, 3, 27, 18, 0, 0, 0, time.UTC)
	computed, err := pricing.Compute(pricing.Inputs{
		Market: testLoopMarket(),
		Quotes: []feeds.Quote{
			relayerQuote(t, "coinbase", "ETHUSD", "1987.75", at),
		},
		At: at,
	})
	if err != nil {
		t.Fatalf("pricing.Compute() error = %v", err)
	}

	_, publication, err := PreparePublication(metadata, computed, store.PublishabilityState{
		Market:    testLoopMarket().Symbol,
		Status:    status.StatusAvailable,
		Reason:    status.ReasonNone,
		UpdatedAt: at,
	}, 1, at)
	if err != nil {
		t.Fatalf("PreparePublication() error = %v", err)
	}
	return publication
}

func decodeTestEnvelope(t *testing.T, raw []byte) Envelope {
	t.Helper()

	var envelope Envelope
	if err := json.Unmarshal(raw, &envelope); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	return envelope
}

func rewriteLoopEnvelopeMetadata(t *testing.T, raw []byte, configDigest string, pricingVersion string) []byte {
	t.Helper()

	var envelope Envelope
	if err := json.Unmarshal(raw, &envelope); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	envelope.ConfigDigest = configDigest
	envelope.PricingAlgorithmVersion = pricingVersion

	rewritten, err := json.Marshal(envelope)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	return rewritten
}

func relayerQuote(t *testing.T, source string, symbol string, value string, basisAt time.Time) feeds.Quote {
	t.Helper()

	parsed, err := fixedpoint.ParseDecimal(value)
	if err != nil {
		t.Fatalf("fixedpoint.ParseDecimal(%q) error = %v", value, err)
	}
	sourceTS := basisAt.UTC()
	return feeds.Quote{
		Source:             source,
		Symbol:             symbol,
		Bid:                parsed,
		Ask:                parsed,
		Last:               parsed,
		Scale:              parsed.Scale,
		ReceivedAt:         basisAt.UTC(),
		SourceTS:           &sourceTS,
		FreshnessBasisKind: feeds.FreshnessBasisSourceTimestamp,
		FreshnessBasisAt:   sourceTS,
	}
}

type scriptedSource struct {
	name string
	fn   func(context.Context, int) (feeds.Quote, error)

	mu            sync.Mutex
	calls         int
	active        int
	maxActiveSeen int
	callCh        chan struct{}
}

func (source *scriptedSource) Name() string {
	return source.name
}

func (source *scriptedSource) Fetch(ctx context.Context, _ feeds.FetchBudget) (feeds.Quote, error) {
	source.mu.Lock()
	call := source.calls
	source.calls++
	source.active++
	if source.active > source.maxActiveSeen {
		source.maxActiveSeen = source.active
	}
	callCh := source.callCh
	if callCh == nil {
		callCh = make(chan struct{}, 32)
		source.callCh = callCh
	}
	source.mu.Unlock()

	callCh <- struct{}{}
	defer func() {
		source.mu.Lock()
		source.active--
		source.mu.Unlock()
	}()

	return source.fn(ctx, call)
}

func (source *scriptedSource) waitCalls(t *testing.T, want int) {
	t.Helper()

	deadline := time.After(2 * time.Second)
	for {
		if source.callCount() >= want {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("source calls = %d, want at least %d", source.callCount(), want)
		case <-source.callsChannel():
		}
	}
}

func (source *scriptedSource) assertCallCount(t *testing.T, want int) {
	t.Helper()

	if got := source.callCount(); got != want {
		t.Fatalf("source calls = %d, want %d", got, want)
	}
}

func (source *scriptedSource) callCount() int {
	source.mu.Lock()
	defer source.mu.Unlock()
	return source.calls
}

func (source *scriptedSource) maxConcurrent() int {
	source.mu.Lock()
	defer source.mu.Unlock()
	return source.maxActiveSeen
}

func (source *scriptedSource) callsChannel() <-chan struct{} {
	source.mu.Lock()
	defer source.mu.Unlock()
	if source.callCh == nil {
		source.callCh = make(chan struct{}, 32)
	}
	return source.callCh
}

type sinkOutcome struct {
	ack                  Ack
	err                  error
	waitForContextCancel bool
}

type scriptedSink struct {
	mu       sync.Mutex
	outcomes []sinkOutcome
	sent     []store.PreparedPublication
	callCh   chan struct{}
}

func newScriptedSink(outcomes ...sinkOutcome) *scriptedSink {
	return &scriptedSink{
		outcomes: outcomes,
		callCh:   make(chan struct{}, 32),
	}
}

func (sink *scriptedSink) Kind() string {
	return "scripted"
}

func (sink *scriptedSink) Close() error {
	return nil
}

func (sink *scriptedSink) Publish(ctx context.Context, publication store.PreparedPublication) (Ack, error) {
	sink.mu.Lock()
	sink.sent = append(sink.sent, publication)
	index := len(sink.sent) - 1
	var outcome sinkOutcome
	if index < len(sink.outcomes) {
		outcome = sink.outcomes[index]
	}
	sink.mu.Unlock()

	sink.callCh <- struct{}{}

	if outcome.waitForContextCancel {
		<-ctx.Done()
		return Ack{}, ctx.Err()
	}
	if outcome.err != nil {
		return Ack{}, outcome.err
	}
	return outcome.ack, nil
}

func (sink *scriptedSink) waitCalls(t *testing.T, want int) {
	t.Helper()

	deadline := time.After(2 * time.Second)
	for {
		if len(sink.publications()) >= want {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("sink calls = %d, want at least %d", len(sink.publications()), want)
		case <-sink.callCh:
		}
	}
}

func (sink *scriptedSink) assertCallCount(t *testing.T, want int) {
	t.Helper()

	if got := len(sink.publications()); got != want {
		t.Fatalf("sink calls = %d, want %d", got, want)
	}
}

func (sink *scriptedSink) publications() []store.PreparedPublication {
	sink.mu.Lock()
	defer sink.mu.Unlock()
	return append([]store.PreparedPublication(nil), sink.sent...)
}

type manualClock struct {
	mu      sync.Mutex
	now     time.Time
	waiters []*manualWaiter
}

type manualWaiter struct {
	until  time.Time
	ch     chan struct{}
	closed bool
}

func newManualClock(start time.Time) *manualClock {
	return &manualClock{now: start.UTC()}
}

func (clock *manualClock) Now() time.Time {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	return clock.now
}

func (clock *manualClock) Wait(ctx context.Context, until time.Time) error {
	clock.mu.Lock()
	if !clock.now.Before(until) {
		clock.mu.Unlock()
		return nil
	}
	waiter := &manualWaiter{
		until: until.UTC(),
		ch:    make(chan struct{}),
	}
	clock.waiters = append(clock.waiters, waiter)
	clock.mu.Unlock()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-waiter.ch:
		return nil
	}
}

func (clock *manualClock) Advance(delta time.Duration) {
	clock.mu.Lock()
	clock.now = clock.now.Add(delta)
	now := clock.now
	waiters := clock.waiters[:0]
	for _, waiter := range clock.waiters {
		if now.Before(waiter.until) {
			waiters = append(waiters, waiter)
			continue
		}
		if !waiter.closed {
			close(waiter.ch)
			waiter.closed = true
		}
	}
	clock.waiters = waiters
	clock.mu.Unlock()
}

func (clock *manualClock) waitForWaiter(t *testing.T, until time.Time) {
	t.Helper()

	deadline := time.After(2 * time.Second)
	target := until.UTC()
	for {
		if clock.hasWaiter(target) {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("manual clock never registered waiter for %s", target.Format(time.RFC3339Nano))
		default:
			time.Sleep(time.Millisecond)
		}
	}
}

func (clock *manualClock) hasWaiter(until time.Time) bool {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	for _, waiter := range clock.waiters {
		if waiter.until.Equal(until) {
			return true
		}
	}
	return false
}

type failingStore struct {
	*store.DB

	failPreparedCycle bool
	failAck           bool
}

func (storeDB *failingStore) CommitCycle(ctx context.Context, record store.CycleRecord) error {
	if storeDB.failPreparedCycle && record.PreparedPublication != nil {
		return errors.New("forced commit failure")
	}
	return storeDB.DB.CommitCycle(ctx, record)
}

func (storeDB *failingStore) AcknowledgePublication(ctx context.Context, market string, sequence uint64, ackedAt time.Time) error {
	if storeDB.failAck {
		return errors.New("forced ack persistence failure")
	}
	return storeDB.DB.AcknowledgePublication(ctx, market, sequence, ackedAt)
}

type stubStore struct {
	pending []store.PreparedPublication
}

func (storeDB *stubStore) CommitCycle(context.Context, store.CycleRecord) error {
	return nil
}

func (storeDB *stubStore) PendingPublications(context.Context, string) ([]store.PreparedPublication, error) {
	return append([]store.PreparedPublication(nil), storeDB.pending...), nil
}

func (storeDB *stubStore) MarkPublicationSent(context.Context, string, uint64, time.Time) error {
	return nil
}

func (storeDB *stubStore) AcknowledgePublication(context.Context, string, uint64, time.Time) error {
	return nil
}

func (storeDB *stubStore) SafeSequence(context.Context, string) (uint64, error) {
	return 0, nil
}

func (storeDB *stubStore) LatestState(context.Context, string) (pricing.State, error) {
	return pricing.State{}, nil
}

func (storeDB *stubStore) LatestPublishability(context.Context, string) (store.PublishabilityState, error) {
	return store.PublishabilityState{}, nil
}

func (storeDB *stubStore) SavePublishability(context.Context, store.PublishabilityState) error {
	return nil
}
