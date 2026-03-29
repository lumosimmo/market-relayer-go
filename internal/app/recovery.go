package app

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/lumosimmo/market-relayer-go/internal/config"
	"github.com/lumosimmo/market-relayer-go/internal/operator"
	"github.com/lumosimmo/market-relayer-go/internal/relayer"
	"github.com/lumosimmo/market-relayer-go/internal/store"
)

type RecoveryOptions struct {
	Mode     operator.RecoveryMode
	Market   string
	RecordID string
}

func Recover(ctx context.Context, options Options, recoveryOptions RecoveryOptions) (operator.RecoveryReport, error) {
	cfg, metadata, err := loadRuntimeMetadata(options)
	if err != nil {
		return operator.RecoveryReport{}, err
	}

	market, err := selectMarket(cfg, recoveryOptions.Market)
	if err != nil {
		return operator.RecoveryReport{}, err
	}

	storeOpen := options.StoreOpen
	if storeOpen == nil {
		storeOpen = func(ctx context.Context, cfg config.StoreConfig) (runtimeStore, error) {
			return store.OpenConfigContext(ctx, cfg)
		}
	}

	storeDB, err := storeOpen(ctx, cfg.Store)
	if err != nil {
		return operator.RecoveryReport{}, err
	}
	defer storeDB.Close()

	var sink relayer.Sink
	if recoveryOptions.Mode == operator.ModeResumePending {
		sink, err = relayer.NewSink(cfg.Sink)
		if err != nil {
			return operator.RecoveryReport{}, err
		}
		defer sink.Close()
	}

	recoveryStore := store.NewLeaseBoundRuntimeStore(storeDB)
	var leaseController *recoveryLeaseController
	if recoveryOptions.Mode == operator.ModeResumePending {
		lease, acquired, err := storeDB.TryAcquireLease(ctx, market.Symbol, cfg.Service.InstanceID+"-recovery", cfg.Store.LeaseTTL)
		if err != nil {
			return operator.RecoveryReport{}, err
		}
		if !acquired {
			return operator.RecoveryReport{}, errors.New("app: market lease is already held")
		}
		leaseStore := store.NewLeaseBoundRuntimeStore(storeDB)
		leaseStore.UpdateLease(lease)
		recoveryStore = leaseStore
		leaseController = startRecoveryLeaseController(ctx, storeDB, leaseStore, lease, cfg.Store.LeaseTTL, cfg.Store.LeaseRenewInterval)
		ctx = leaseController.Context()
	}

	report, runErr := operator.RunRecovery(ctx, operator.RecoveryOptions{
		Mode:   recoveryOptions.Mode,
		Market: market,
		Metadata: store.Metadata{
			ConfigDigest:            metadata.ConfigDigest,
			PricingAlgorithmVersion: metadata.PricingAlgorithmVersion,
			SchemaVersion:           store.SchemaVersion,
			EnvelopeVersion:         relayer.EnvelopeVersion,
		},
		Store:    recoveryStore,
		Sink:     sink,
		RecordID: recoveryOptions.RecordID,
	})
	if leaseController != nil {
		lease, renewErr := leaseController.Stop()
		releaseCtx, cancel := cleanupContext(cfg.Store.LockTimeout)
		defer cancel()
		_ = storeDB.ReleaseLease(releaseCtx, lease)
		if runErr != nil {
			if errors.Is(runErr, context.Canceled) && renewErr != nil {
				return report, renewErr
			}
			return report, runErr
		}
		if renewErr != nil {
			return report, renewErr
		}
	}
	if runErr != nil {
		return report, runErr
	}
	return report, nil
}

func selectMarket(cfg *config.Config, symbol string) (config.MarketConfig, error) {
	return selectMarketFromList(cfg.Markets, symbol)
}

type recoveryLeaseController struct {
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}

	mu    sync.Mutex
	lease store.Lease
	err   error
}

func startRecoveryLeaseController(
	parent context.Context,
	storeDB leaseManager,
	leaseStore *store.LeaseBoundRuntimeStore,
	initial store.Lease,
	ttl time.Duration,
	interval time.Duration,
) *recoveryLeaseController {
	if interval <= 0 {
		interval = time.Second
	}
	ctx, cancel := context.WithCancel(parent)
	controller := &recoveryLeaseController{
		ctx:    ctx,
		cancel: cancel,
		done:   make(chan struct{}),
		lease:  initial,
	}
	go controller.run(storeDB, leaseStore, ttl, interval)
	return controller
}

func (controller *recoveryLeaseController) Context() context.Context {
	return controller.ctx
}

func (controller *recoveryLeaseController) Stop() (store.Lease, error) {
	controller.cancel()
	<-controller.done

	controller.mu.Lock()
	defer controller.mu.Unlock()
	return controller.lease, controller.err
}

func (controller *recoveryLeaseController) run(
	storeDB leaseManager,
	leaseStore *store.LeaseBoundRuntimeStore,
	ttl time.Duration,
	interval time.Duration,
) {
	defer close(controller.done)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	current := controller.currentLease()
	for {
		select {
		case <-controller.ctx.Done():
			return
		case <-ticker.C:
			next, renewed, err := storeDB.RenewLease(controller.ctx, current, ttl)
			if err != nil {
				controller.fail(fmt.Errorf("app: recovery lease renewal failed: %w", err))
				return
			}
			if !renewed {
				controller.fail(errors.New("app: recovery lease lost during recovery"))
				return
			}
			current = next
			controller.setLease(next)
			leaseStore.UpdateLease(next)
		}
	}
}

func (controller *recoveryLeaseController) currentLease() store.Lease {
	controller.mu.Lock()
	defer controller.mu.Unlock()
	return controller.lease
}

func (controller *recoveryLeaseController) setLease(lease store.Lease) {
	controller.mu.Lock()
	defer controller.mu.Unlock()
	controller.lease = lease
}

func (controller *recoveryLeaseController) fail(err error) {
	controller.mu.Lock()
	if controller.err == nil {
		controller.err = err
	}
	controller.mu.Unlock()
	controller.cancel()
}
