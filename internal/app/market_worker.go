package app

import (
	"context"
	"errors"
	"time"

	"github.com/lumosimmo/market-relayer-go/internal/config"
	"github.com/lumosimmo/market-relayer-go/internal/relayer"
	"github.com/lumosimmo/market-relayer-go/internal/store"
)

type marketWorker struct {
	market              config.MarketConfig
	owner               string
	db                  leaseManager
	loop                *relayer.Loop
	store               *store.LeaseBoundRuntimeStore
	leaseTTL            time.Duration
	leaseRenewInterval  time.Duration
	leaseReleaseTimeout time.Duration
}

func (worker *marketWorker) Run(ctx context.Context, state *runtimeState) error {
	retryDelay := worker.leaseRenewInterval
	if retryDelay <= 0 || retryDelay > time.Second {
		retryDelay = time.Second
	}

	for ctx.Err() == nil {
		lease, acquired, err := worker.db.TryAcquireLease(ctx, worker.market.Symbol, worker.owner, worker.leaseTTL)
		if err != nil {
			worker.resetLeaseState(state)
			if !waitForRetry(ctx, retryDelay) {
				return nil
			}
			continue
		}
		if !acquired {
			if state != nil {
				state.setLeaseStatus(worker.market.Symbol, lease, false)
			}
			if !waitForRetry(ctx, retryDelay) {
				return nil
			}
			continue
		}

		worker.store.UpdateLease(lease)
		worker.loop.Reset()
		if state != nil {
			state.setLeaseStatus(worker.market.Symbol, lease, true)
		}

		runCtx, cancel := context.WithCancel(ctx)
		go worker.renewLease(runCtx, cancel, state, lease)
		err = worker.loop.Run(runCtx)
		cancel()

		if err != nil && !errors.Is(err, context.Canceled) {
			worker.resetLeaseState(state)
			return err
		}
		worker.resetLeaseState(state)
	}

	return nil
}

func (worker *marketWorker) renewLease(ctx context.Context, cancel context.CancelFunc, state *runtimeState, initial store.Lease) {
	interval := worker.leaseRenewInterval
	if interval <= 0 {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	lease := initial
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			next, renewed, err := worker.db.RenewLease(ctx, lease, worker.leaseTTL)
			if err != nil || !renewed {
				cancel()
				return
			}
			lease = next
			worker.store.UpdateLease(lease)
			if state != nil {
				state.setLeaseStatus(worker.market.Symbol, lease, true)
			}
		}
	}
}

func (worker *marketWorker) resetLeaseState(state *runtimeState) {
	lease, err := worker.store.CurrentLease()
	if err == nil {
		releaseCtx, cancel := cleanupContext(worker.leaseReleaseTimeout)
		defer cancel()
		_ = worker.db.ReleaseLease(releaseCtx, lease)
	}
	worker.store.ClearLease()
	worker.loop.Reset()
	if state != nil {
		state.setLeaseStatus(worker.market.Symbol, store.Lease{}, false)
	}
}

func waitForRetry(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
