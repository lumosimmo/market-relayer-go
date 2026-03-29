package relayer

import (
	"context"
	"errors"
	"time"

	"github.com/lumosimmo/market-relayer-go/internal/status"
	"github.com/lumosimmo/market-relayer-go/internal/store"
)

func (loop *Loop) publishPhase(ctx context.Context, publication store.PreparedPublication, recordID string) (time.Duration, string, error) {
	phaseCtx, cancel := context.WithTimeout(ctx, loop.budgets.Publish)
	defer cancel()

	phaseStarted := loop.clock.Now().UTC()
	attemptTimeout := loop.publishAttemptTimeout()

	for {
		timeout, ok := nextAttemptTimeout(phaseCtx, attemptTimeout)
		if !ok {
			return loop.clock.Now().UTC().Sub(phaseStarted), "publish_budget_exhausted", nil
		}

		attemptCtx, attemptCancel := context.WithTimeout(phaseCtx, timeout)
		attemptStarted := loop.clock.Now().UTC()
		ack, err := loop.sink.Publish(attemptCtx, publication)
		attemptCancel()
		publishLatency := loop.clock.Now().UTC().Sub(attemptStarted)
		if err == nil {
			return loop.handlePublishSuccess(phaseCtx, publication, ack, attemptStarted, publishLatency, recordID)
		}

		if errors.Is(err, context.Canceled) && ctx.Err() != nil {
			return publishLatency, "canceled", ctx.Err()
		}

		var publishErr *PublishError
		if errors.As(err, &publishErr) && publishErr.Retryable {
			if loop.handleRetryablePublishFailure(phaseCtx, attemptTimeout, publication, recordID, err) {
				continue
			}
			return publishLatency, "retry_budget_exhausted", nil
		}

		loop.handleTerminalPublishFailure(phaseCtx, publication, recordID, err)
		return publishLatency, "terminal_failure", nil
	}
}

func (loop *Loop) publishAttemptTimeout() time.Duration {
	attemptTimeout := loop.attempts.PublishAttemptTimeout
	if attemptTimeout > 0 {
		return attemptTimeout
	}
	return minDuration(loop.budgets.Publish, time.Second)
}

func nextAttemptTimeout(ctx context.Context, fullAttempt time.Duration) (time.Duration, bool) {
	deadline, ok := ctx.Deadline()
	if !ok {
		return fullAttempt, true
	}

	remaining := time.Until(deadline)
	if remaining <= 0 {
		return 0, false
	}
	return minDuration(fullAttempt, remaining), true
}

func (loop *Loop) handlePublishSuccess(
	ctx context.Context,
	publication store.PreparedPublication,
	ack Ack,
	attemptStarted time.Time,
	publishLatency time.Duration,
	recordID string,
) (time.Duration, string, error) {
	ackedAt := ack.AckedAt
	if ackedAt.IsZero() {
		ackedAt = attemptStarted.UTC()
	} else {
		ackedAt = ackedAt.UTC()
	}
	if err := loop.markSent(ctx, publication, attemptStarted); err != nil {
		loop.handleStoreFailure()
		loop.logError("publication sent but sent checkpoint failed", err, recordID, publication.Sequence, publication.IdempotencyKey)
		return publishLatency, "sent_checkpoint_failed", nil
	}
	if err := loop.acknowledge(ctx, publication, ack, attemptStarted); err != nil {
		loop.handleStoreFailure()
		loop.logError("publication acked but ack checkpoint failed", err, recordID, publication.Sequence, publication.IdempotencyKey)
		return publishLatency, "ack_checkpoint_failed", nil
	}

	loop.mu.Lock()
	previous := loop.publishability
	loop.pending = nil
	loop.publishability.PendingSequence = 0
	loop.publishability.LastSequence = publication.Sequence
	loop.publishability.LastIdempotencyKey = publication.IdempotencyKey
	loop.publishability.LastPayloadHash = publication.PayloadHash
	loop.publishability.Status = status.StatusAvailable
	loop.publishability.Reason = status.ReasonNone
	loop.publishability.UpdatedAt = ackedAt
	loop.metrics.PublicationsAcked++
	loop.recordPublishabilityTransition(previous, loop.publishability)
	loop.mu.Unlock()
	if err := loop.savePublishability(ctx, recordID, publication.Sequence, publication.IdempotencyKey); err != nil {
		return publishLatency, "publishability_persist_failed", nil
	}

	loop.logPublication("publication acked", recordID, publication, publishLatency, ack.Duplicate)
	return publishLatency, "acked", nil
}

func (loop *Loop) handleRetryablePublishFailure(
	phaseCtx context.Context,
	attemptTimeout time.Duration,
	publication store.PreparedPublication,
	recordID string,
	err error,
) bool {
	loop.mu.Lock()
	loop.metrics.RetryablePublishErrors++
	loop.mu.Unlock()

	if !canRetry(phaseCtx, attemptTimeout) {
		loop.setSinkUnavailable(publication.Sequence, publication.IdempotencyKey, publication.PayloadHash)
		if loop.metricsRegistry != nil {
			loop.metricsRegistry.IncRetryBudgetExhausted(loop.market.Symbol)
		}
		if err := loop.savePublishability(phaseCtx, recordID, publication.Sequence, publication.IdempotencyKey); err != nil {
			return false
		}
		loop.logError("retry budget exhausted", err, recordID, publication.Sequence, publication.IdempotencyKey)
		return false
	}

	loop.logError("retryable publish failure", err, recordID, publication.Sequence, publication.IdempotencyKey)
	return true
}

func (loop *Loop) handleTerminalPublishFailure(ctx context.Context, publication store.PreparedPublication, recordID string, err error) {
	loop.mu.Lock()
	loop.metrics.TerminalPublishErrors++
	loop.mu.Unlock()

	loop.blockTerminal(status.ReasonSinkUnavailable, publication.Sequence, publication.IdempotencyKey, publication.PayloadHash)
	if err := loop.savePublishability(ctx, recordID, publication.Sequence, publication.IdempotencyKey); err != nil {
		return
	}
	loop.logError("terminal publish failure", err, recordID, publication.Sequence, publication.IdempotencyKey)
}

func (loop *Loop) markSent(ctx context.Context, publication store.PreparedPublication, sentAt time.Time) error {
	if err := loop.store.MarkPublicationSent(ctx, publication.Market, publication.Sequence, sentAt); err != nil {
		return err
	}
	loop.mu.Lock()
	defer loop.mu.Unlock()
	if loop.pending != nil && loop.pending.Sequence == publication.Sequence {
		updated := *loop.pending
		updated.State = store.PublicationStateSent
		normalized := sentAt.UTC()
		updated.SentAt = &normalized
		loop.pending = &updated
	}
	return nil
}

func (loop *Loop) acknowledge(ctx context.Context, publication store.PreparedPublication, ack Ack, fallbackTime time.Time) error {
	ackedAt := ack.AckedAt
	if ackedAt.IsZero() {
		ackedAt = fallbackTime
	}
	return loop.store.AcknowledgePublication(ctx, publication.Market, publication.Sequence, ackedAt)
}

func canRetry(ctx context.Context, fullAttempt time.Duration) bool {
	if fullAttempt <= 0 {
		return false
	}
	deadline, ok := ctx.Deadline()
	if !ok {
		return true
	}
	return time.Until(deadline) >= fullAttempt
}
