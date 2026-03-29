package relayer

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/lumosimmo/market-relayer-go/internal/clock"
	"github.com/lumosimmo/market-relayer-go/internal/metrics"
	"github.com/lumosimmo/market-relayer-go/internal/status"
	"github.com/lumosimmo/market-relayer-go/internal/store"
)

func TestLoopBudgetHelpersAndWaiter(t *testing.T) {
	t.Parallel()

	t.Run("default phase budgets use cadence defaults", func(t *testing.T) {
		t.Parallel()

		got := DefaultPhaseBudgets(0)
		if got.Fetch != 1800*time.Millisecond {
			t.Fatalf("Fetch = %s, want 1800ms", got.Fetch)
		}
		if got.Compute != 250*time.Millisecond {
			t.Fatalf("Compute = %s, want 250ms", got.Compute)
		}
		if got.Persist != 200*time.Millisecond {
			t.Fatalf("Persist = %s, want 200ms", got.Persist)
		}
		if got.Publish != 500*time.Millisecond {
			t.Fatalf("Publish = %s, want 500ms", got.Publish)
		}
	})

	t.Run("scale budget rescales and floors at one millisecond", func(t *testing.T) {
		t.Parallel()

		if got := scaleBudget(250*time.Millisecond, 6*time.Second); got != 500*time.Millisecond {
			t.Fatalf("scaleBudget() = %s, want 500ms", got)
		}
		if got := scaleBudget(time.Nanosecond, time.Nanosecond); got != time.Millisecond {
			t.Fatalf("scaleBudget() floor = %s, want 1ms", got)
		}
	})

	t.Run("real waiter returns immediately when deadline already passed", func(t *testing.T) {
		t.Parallel()

		var waiter realWaiter
		if err := waiter.Wait(context.Background(), time.Now().Add(-time.Millisecond)); err != nil {
			t.Fatalf("Wait() error = %v, want nil", err)
		}
	})

	t.Run("real waiter respects cancellation", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		var waiter realWaiter
		if err := waiter.Wait(ctx, time.Now().Add(time.Second)); !errors.Is(err, context.Canceled) {
			t.Fatalf("Wait() error = %v, want context.Canceled", err)
		}
	})

	t.Run("real waiter wakes when timer fires", func(t *testing.T) {
		t.Parallel()

		var waiter realWaiter
		if err := waiter.Wait(context.Background(), time.Now().Add(5*time.Millisecond)); err != nil {
			t.Fatalf("Wait() error = %v, want nil", err)
		}
	})
}

func TestLoopPublishabilityAndRetryHelpers(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 3, 27, 10, 0, 0, 0, time.UTC)
	publication := testPreparedPublication(7)

	t.Run("publishability reflects pending publication and blocked state", func(t *testing.T) {
		t.Parallel()

		loop := &Loop{
			market:          testLoopMarket(),
			clock:           clock.Fixed{Time: now},
			publishability:  defaultPublishability("ETHUSD", now),
			attempts:        AttemptBudgets{PublishAttemptTimeout: 75 * time.Millisecond},
			budgets:         PhaseBudgets{Publish: 500 * time.Millisecond},
			metricsRegistry: metrics.NewRegistry(),
		}

		withPending := loop.publishabilityForCycle(now, &publication)
		if withPending.Status != status.StatusUnavailable {
			t.Fatalf("publishability with pending status = %q, want %q", withPending.Status, status.StatusUnavailable)
		}
		if withPending.Reason != status.ReasonSinkUnavailable {
			t.Fatalf("publishability with pending reason = %q, want %q", withPending.Reason, status.ReasonSinkUnavailable)
		}
		if withPending.PendingSequence != publication.Sequence {
			t.Fatalf("publishability with pending sequence = %d, want %d", withPending.PendingSequence, publication.Sequence)
		}

		loop.blocked = true
		loop.blockedReason = status.ReasonUnpublishable
		blocked := loop.publishabilityForCycle(now, nil)
		if blocked.Status != status.StatusUnavailable {
			t.Fatalf("blocked publishability status = %q, want %q", blocked.Status, status.StatusUnavailable)
		}
		if blocked.Reason != status.ReasonUnpublishable {
			t.Fatalf("blocked publishability reason = %q, want %q", blocked.Reason, status.ReasonUnpublishable)
		}

		if loop.shouldPreparePublication(&publication) {
			t.Fatal("shouldPreparePublication() = true with pending publication, want false")
		}
		if loop.shouldPreparePublication(nil) {
			t.Fatal("shouldPreparePublication() = true while blocked, want false")
		}

		loop.blocked = false
		if !loop.shouldPreparePublication(nil) {
			t.Fatal("shouldPreparePublication() = false without pending publication, want true")
		}

		current := publishabilityWithPrepared(defaultPublishability("ETHUSD", now), "stable-hash", publication)
		if current.LastStableHash != "stable-hash" {
			t.Fatalf("LastStableHash = %q, want stable-hash", current.LastStableHash)
		}
		if current.PendingSequence != publication.Sequence {
			t.Fatalf("PendingSequence = %d, want %d", current.PendingSequence, publication.Sequence)
		}

		if got := loop.publishAttemptTimeout(); got != 75*time.Millisecond {
			t.Fatalf("publishAttemptTimeout() = %s, want 75ms", got)
		}
		loop.attempts.PublishAttemptTimeout = 0
		if got := loop.publishAttemptTimeout(); got != 500*time.Millisecond {
			t.Fatalf("publishAttemptTimeout() fallback = %s, want 500ms", got)
		}
	})

	t.Run("next attempt timeout and retry checks respect deadlines", func(t *testing.T) {
		t.Parallel()

		if got, ok := nextAttemptTimeout(context.Background(), 100*time.Millisecond); !ok || got != 100*time.Millisecond {
			t.Fatalf("nextAttemptTimeout(no deadline) = (%s, %t), want (100ms, true)", got, ok)
		}

		expiredCtx, cancelExpired := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
		defer cancelExpired()
		if got, ok := nextAttemptTimeout(expiredCtx, 100*time.Millisecond); ok || got != 0 {
			t.Fatalf("nextAttemptTimeout(expired) = (%s, %t), want (0, false)", got, ok)
		}

		if canRetry(context.Background(), 100*time.Millisecond) != true {
			t.Fatal("canRetry(no deadline) = false, want true")
		}
		if canRetry(context.Background(), 0) != false {
			t.Fatal("canRetry(zero timeout) = true, want false")
		}
		if canRetry(expiredCtx, 100*time.Millisecond) != false {
			t.Fatal("canRetry(expired deadline) = true, want false")
		}
		if got := minDuration(0, 5*time.Second); got != 5*time.Second {
			t.Fatalf("minDuration(0, 5s) = %s, want 5s", got)
		}
		if got := minDuration(5*time.Second, 0); got != 5*time.Second {
			t.Fatalf("minDuration(5s, 0) = %s, want 5s", got)
		}
		if got := minDuration(2*time.Second, time.Second); got != time.Second {
			t.Fatalf("minDuration(2s, 1s) = %s, want 1s", got)
		}
		if got := minDuration(-2*time.Second, time.Second); got != time.Second {
			t.Fatalf("minDuration(-2s, 1s) = %s, want 1s", got)
		}
		if got := minDuration(2*time.Second, 2*time.Second); got != 2*time.Second {
			t.Fatalf("minDuration(2s, 2s) = %s, want 2s", got)
		}
		large := time.Duration(1 << 62)
		if got := minDuration(large, large-1); got != large-1 {
			t.Fatalf("minDuration(large, large-1) = %d, want %d", got, large-1)
		}
	})
}

func TestLoopFailureAndRecoveryHelpers(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 3, 27, 11, 0, 0, 0, time.UTC)
	publication := testPreparedPublication(9)

	t.Run("retryable publish failure exhausts budget and persists sink unavailable state", func(t *testing.T) {
		t.Parallel()

		storeDB := &recordingStore{stubStore: &stubStore{}}
		loop := &Loop{
			market:          testLoopMarket(),
			clock:           clock.Fixed{Time: now},
			store:           storeDB,
			metricsRegistry: metrics.NewRegistry(),
			publishability:  defaultPublishability("ETHUSD", now),
		}

		phaseCtx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
		defer cancel()

		retry := loop.handleRetryablePublishFailure(phaseCtx, 100*time.Millisecond, publication, "cycle-1", errors.New("temporary failure"))
		if retry {
			t.Fatal("handleRetryablePublishFailure() = true, want false")
		}
		if got := loop.Metrics().RetryablePublishErrors; got != 1 {
			t.Fatalf("RetryablePublishErrors = %d, want 1", got)
		}
		state := loop.currentPublishability()
		if state.Status != status.StatusUnavailable {
			t.Fatalf("publishability status = %q, want %q", state.Status, status.StatusUnavailable)
		}
		if state.Reason != status.ReasonSinkUnavailable {
			t.Fatalf("publishability reason = %q, want %q", state.Reason, status.ReasonSinkUnavailable)
		}
		if len(storeDB.saved) != 1 {
			t.Fatalf("saved publishability states = %d, want 1", len(storeDB.saved))
		}
	})

	t.Run("retryable publish failure keeps retrying when budget remains", func(t *testing.T) {
		t.Parallel()

		storeDB := &recordingStore{stubStore: &stubStore{}}
		loop := &Loop{
			market:         testLoopMarket(),
			clock:          clock.Fixed{Time: now},
			store:          storeDB,
			publishability: defaultPublishability("ETHUSD", now),
		}

		phaseCtx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Hour))
		defer cancel()

		retry := loop.handleRetryablePublishFailure(phaseCtx, 100*time.Millisecond, publication, "cycle-2", errors.New("temporary failure"))
		if !retry {
			t.Fatal("handleRetryablePublishFailure() = false, want true")
		}
		if len(storeDB.saved) != 0 {
			t.Fatalf("saved publishability states = %d, want 0", len(storeDB.saved))
		}
	})

	t.Run("terminal and store failures block publication until cleared", func(t *testing.T) {
		t.Parallel()

		storeDB := &recordingStore{stubStore: &stubStore{}}
		loop := &Loop{
			market:          testLoopMarket(),
			clock:           clock.Fixed{Time: now},
			store:           storeDB,
			metricsRegistry: metrics.NewRegistry(),
			publishability:  defaultPublishability("ETHUSD", now),
		}

		loop.handleTerminalPublishFailure(context.Background(), publication, "cycle-3", errors.New("terminal failure"))
		if got := loop.Metrics().TerminalPublishErrors; got != 1 {
			t.Fatalf("TerminalPublishErrors = %d, want 1", got)
		}
		if !loop.blocked {
			t.Fatal("blocked = false, want true")
		}
		if loop.blockedAutoRecover {
			t.Fatal("blockedAutoRecover = true, want false")
		}
		if got := loop.currentPublishability().Reason; got != status.ReasonSinkUnavailable {
			t.Fatalf("publishability reason = %q, want %q", got, status.ReasonSinkUnavailable)
		}

		loop.clearBlocked(context.Background(), "cycle-4")
		if loop.blocked {
			t.Fatal("blocked = true after clearBlocked(), want false")
		}
		if got := loop.currentPublishability().Status; got != status.StatusAvailable {
			t.Fatalf("publishability status after clear = %q, want %q", got, status.StatusAvailable)
		}

		loop.handleStoreFailure()
		if !loop.blocked {
			t.Fatal("blocked after handleStoreFailure() = false, want true")
		}
		if !loop.blockedAutoRecover {
			t.Fatal("blockedAutoRecover after handleStoreFailure() = false, want true")
		}
		if got := loop.currentPublishability().Reason; got != status.ReasonUnpublishable {
			t.Fatalf("publishability reason after store failure = %q, want %q", got, status.ReasonUnpublishable)
		}
	})

	t.Run("resume pending publishes stored envelope and updates metrics", func(t *testing.T) {
		t.Parallel()

		pending := pendingPublicationForMetadataTest(t, testLoopMetadata())
		sink := newScriptedSink(sinkOutcome{ack: Ack{}})
		loop := &Loop{
			market:          testLoopMarket(),
			metadata:        testLoopMetadata(),
			store:           &stubStore{pending: []store.PreparedPublication{pending}},
			sink:            sink,
			clock:           clock.Fixed{Time: now},
			metricsRegistry: metrics.NewRegistry(),
			publishability:  defaultPublishability("ETHUSD", now),
			budgets:         PhaseBudgets{Publish: 250 * time.Millisecond},
			attempts:        AttemptBudgets{PublishAttemptTimeout: 100 * time.Millisecond},
		}

		if err := loop.ResumePending(context.Background()); err != nil {
			t.Fatalf("ResumePending() error = %v", err)
		}
		sink.assertCallCount(t, 1)
		if got := loop.Metrics().RecoveryReplays; got != 1 {
			t.Fatalf("RecoveryReplays = %d, want 1", got)
		}
		if got := loop.Metrics().PublicationsAcked; got != 1 {
			t.Fatalf("PublicationsAcked = %d, want 1", got)
		}
	})
}

type recordingStore struct {
	*stubStore
	saved []store.PublishabilityState
}

func (storeDB *recordingStore) SavePublishability(_ context.Context, state store.PublishabilityState) error {
	storeDB.saved = append(storeDB.saved, state)
	return nil
}
