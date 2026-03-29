package relayer

import (
	"context"
	"fmt"

	"github.com/lumosimmo/market-relayer-go/internal/store"
)

func (loop *Loop) ResumePending(ctx context.Context) error {
	loop.runMu.Lock()
	defer loop.runMu.Unlock()
	return loop.resumePendingLocked(ctx)
}

func (loop *Loop) initialize(ctx context.Context) error {
	loop.mu.RLock()
	initialized := loop.initialized
	loop.mu.RUnlock()
	if initialized {
		return nil
	}

	loop.runMu.Lock()
	defer loop.runMu.Unlock()

	loop.mu.RLock()
	initialized = loop.initialized
	loop.mu.RUnlock()
	if initialized {
		return nil
	}

	restored, err := loop.store.LatestState(ctx, loop.market.Symbol)
	if err != nil {
		return err
	}
	publishability, err := loop.store.LatestPublishability(ctx, loop.market.Symbol)
	if err != nil {
		return err
	}
	if publishability == (store.PublishabilityState{}) {
		publishability = defaultPublishability(loop.market.Symbol, loop.clock.Now())
	}

	loop.mu.Lock()
	loop.state = loop.tracker.RestoreState(loop.clock.Now(), restored)
	loop.publishability = publishability
	loop.pending = nil
	loop.mu.Unlock()
	if err := loop.resumePendingLocked(ctx); err != nil {
		return err
	}

	safeSequence, err := loop.store.SafeSequence(ctx, loop.market.Symbol)
	if err != nil {
		return err
	}
	loop.mu.Lock()
	loop.nextSequence = safeSequence + 1
	loop.initialized = true
	loop.mu.Unlock()
	return nil
}

func (loop *Loop) resumePendingLocked(ctx context.Context) error {
	pending, err := loop.store.PendingPublications(ctx, loop.market.Symbol)
	if err != nil {
		return err
	}
	for _, current := range pending {
		if err := ValidatePendingReplay(current, loop.metadata); err != nil {
			return err
		}
	}
	for _, current := range pending {
		loop.mu.Lock()
		loop.metrics.RecoveryReplays++
		loop.mu.Unlock()
		if loop.metricsRegistry != nil {
			loop.metricsRegistry.IncRecoveryReplay(loop.market.Symbol)
		}
		_, result, err := loop.publishPhase(ctx, current, "startup-recovery")
		if err != nil {
			return err
		}
		if result != "acked" {
			return fmt.Errorf("relayer: startup replay for %s/%d ended with %s", current.Market, current.Sequence, result)
		}
	}
	return nil
}
