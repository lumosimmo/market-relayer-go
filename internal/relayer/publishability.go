package relayer

import "context"

import (
	"time"

	"github.com/lumosimmo/market-relayer-go/internal/pricing"
	"github.com/lumosimmo/market-relayer-go/internal/status"
	"github.com/lumosimmo/market-relayer-go/internal/store"
)

func (loop *Loop) publishabilityForCycle(now time.Time, pending *store.PreparedPublication) store.PublishabilityState {
	loop.mu.RLock()
	current := loop.publishability
	blocked := loop.blocked
	blockedReason := loop.blockedReason
	loop.mu.RUnlock()

	current.Market = loop.market.Symbol
	current.UpdatedAt = now.UTC()
	if current.Status == "" {
		current.Status = status.StatusAvailable
	}
	if current.Reason == "" {
		current.Reason = status.ReasonNone
	}
	if blocked {
		current.Status = status.StatusUnavailable
		current.Reason = blockedReason
	}
	if pending != nil {
		current.PendingSequence = pending.Sequence
		current.LastIdempotencyKey = pending.IdempotencyKey
		current.LastPayloadHash = pending.PayloadHash
		if current.Status == status.StatusAvailable {
			current.Status = status.StatusUnavailable
			current.Reason = status.ReasonSinkUnavailable
		}
	} else {
		current.PendingSequence = 0
	}
	return current
}

func publishabilityWithPrepared(
	current store.PublishabilityState,
	stableHash string,
	publication store.PreparedPublication,
) store.PublishabilityState {
	current.LastStableHash = stableHash
	current.PendingSequence = publication.Sequence
	current.LastIdempotencyKey = publication.IdempotencyKey
	current.LastPayloadHash = publication.PayloadHash
	return current
}

func (loop *Loop) handleStoreFailure() {
	loop.mu.Lock()
	defer loop.mu.Unlock()
	previous := loop.publishability
	if !loop.blocked {
		loop.metrics.UnpublishableEvents++
	}
	if loop.metricsRegistry != nil {
		loop.metricsRegistry.IncStoreFailure(loop.market.Symbol)
	}
	loop.blocked = true
	loop.blockedReason = status.ReasonUnpublishable
	loop.blockedAutoRecover = true
	loop.publishability.Status = status.StatusUnavailable
	loop.publishability.Reason = status.ReasonUnpublishable
	loop.publishability.UpdatedAt = loop.clock.Now().UTC()
	loop.recordPublishabilityTransition(previous, loop.publishability)
}

func (loop *Loop) clearBlocked(ctx context.Context, recordID string) {
	loop.mu.Lock()
	previous := loop.publishability
	loop.blocked = false
	loop.blockedReason = status.ReasonNone
	loop.blockedAutoRecover = false
	loop.publishability.Status = status.StatusAvailable
	loop.publishability.Reason = status.ReasonNone
	loop.publishability.UpdatedAt = loop.clock.Now().UTC()
	loop.recordPublishabilityTransition(previous, loop.publishability)
	loop.mu.Unlock()
	_ = loop.savePublishability(ctx, recordID, 0, "")
}

func (loop *Loop) blockTerminal(reason status.Reason, sequence uint64, idempotencyKey string, payloadHash string) {
	loop.mu.Lock()
	defer loop.mu.Unlock()
	previous := loop.publishability
	if !loop.blocked {
		loop.metrics.UnpublishableEvents++
	}
	loop.blocked = true
	loop.blockedReason = reason
	loop.blockedAutoRecover = false
	loop.publishability.Status = status.StatusUnavailable
	loop.publishability.Reason = reason
	loop.publishability.PendingSequence = sequence
	loop.publishability.LastIdempotencyKey = idempotencyKey
	loop.publishability.LastPayloadHash = payloadHash
	loop.publishability.UpdatedAt = loop.clock.Now().UTC()
	loop.recordPublishabilityTransition(previous, loop.publishability)
}

func (loop *Loop) setSinkUnavailable(sequence uint64, idempotencyKey string, payloadHash string) {
	loop.mu.Lock()
	defer loop.mu.Unlock()
	previous := loop.publishability
	loop.publishability.Status = status.StatusUnavailable
	loop.publishability.Reason = status.ReasonSinkUnavailable
	loop.publishability.PendingSequence = sequence
	loop.publishability.LastIdempotencyKey = idempotencyKey
	loop.publishability.LastPayloadHash = payloadHash
	loop.publishability.UpdatedAt = loop.clock.Now().UTC()
	loop.recordPublishabilityTransition(previous, loop.publishability)
}

func defaultPublishability(market string, at time.Time) store.PublishabilityState {
	return store.PublishabilityState{
		Market:    market,
		Status:    status.StatusAvailable,
		Reason:    status.ReasonNone,
		UpdatedAt: at.UTC(),
	}
}

func (loop *Loop) recordPublishabilityTransition(previous store.PublishabilityState, current store.PublishabilityState) {
	if loop.metricsRegistry == nil {
		return
	}
	loop.metricsRegistry.SetPublishabilityState(loop.market.Symbol, status.NewCondition(previous.Status, previous.Reason), status.NewCondition(current.Status, current.Reason))
	if previous.Status == current.Status && previous.Reason == current.Reason {
		return
	}
	loop.metricsRegistry.IncPublishabilityTransition(loop.market.Symbol, status.NewCondition(current.Status, current.Reason))
}

func (loop *Loop) savePublishability(ctx context.Context, recordID string, sequence uint64, idempotencyKey string) error {
	state := loop.currentPublishability()
	if err := loop.store.SavePublishability(ctx, state); err != nil {
		loop.handleStoreFailure()
		loop.logError("persist publishability failed", err, recordID, sequence, idempotencyKey)
		return err
	}
	return nil
}

func (loop *Loop) currentPublishability() store.PublishabilityState {
	loop.mu.RLock()
	defer loop.mu.RUnlock()
	return loop.publishability
}

func (loop *Loop) restoreStateAt(at time.Time) pricing.State {
	loop.mu.RLock()
	defer loop.mu.RUnlock()
	return loop.tracker.RestoreState(at, loop.state)
}
