package app

import (
	"errors"
	"log/slog"
	"time"

	"github.com/lumosimmo/market-relayer-go/internal/clock"
	"github.com/lumosimmo/market-relayer-go/internal/config"
	"github.com/lumosimmo/market-relayer-go/internal/feeds"
	"github.com/lumosimmo/market-relayer-go/internal/metrics"
	"github.com/lumosimmo/market-relayer-go/internal/relayer"
	"github.com/lumosimmo/market-relayer-go/internal/sessions"
	"github.com/lumosimmo/market-relayer-go/internal/status"
	"github.com/lumosimmo/market-relayer-go/internal/store"
)

func buildLoops(
	cfg *config.Config,
	metadata RuntimeMetadata,
	storeDB runtimeStore,
	sink relayer.Sink,
	feedBuild feeds.BuildOptions,
	serviceClock clock.Clock,
	metricsRegistry *metrics.Registry,
	logger *slog.Logger,
) ([]*relayer.Loop, []*marketWorker, error) {
	loops := make([]*relayer.Loop, 0, len(cfg.Markets))
	workers := make([]*marketWorker, 0, len(cfg.Markets))
	sharedMetadata := store.Metadata{
		ConfigDigest:            metadata.ConfigDigest,
		PricingAlgorithmVersion: metadata.PricingAlgorithmVersion,
		SchemaVersion:           store.SchemaVersion,
		EnvelopeVersion:         relayer.EnvelopeVersion,
	}

	for _, market := range cfg.Markets {
		sources, err := feeds.BuildMarketSources(cfg, market, feedBuild)
		if err != nil {
			return nil, nil, err
		}
		perpBookSource, err := feeds.BuildPerpBookSource(cfg, market, feedBuild)
		if err != nil {
			return nil, nil, err
		}

		window, err := marketWindow(market, sources)
		if err != nil {
			return nil, nil, err
		}

		leaseStore := store.NewLeaseBoundRuntimeStore(storeDB)

		loop, err := relayer.NewLoop(relayer.LoopOptions{
			Market:         market,
			Metadata:       sharedMetadata,
			Store:          leaseStore,
			Sink:           sink,
			Sources:        sources,
			PerpBookSource: perpBookSource,
			Clock:          serviceClock,
			Logger:         logger,
			Metrics:        metricsRegistry,
			Window:         window,
			Budgets:        relayer.DefaultPhaseBudgets(market.PublishInterval),
			Attempts:       relayer.AttemptBudgets{PublishAttemptTimeout: cfg.Sink.RequestTimeout},
		})
		if err != nil {
			return nil, nil, err
		}
		loops = append(loops, loop)
		workers = append(workers, &marketWorker{
			market:              market,
			owner:               cfg.Service.InstanceID,
			db:                  storeDB,
			loop:                loop,
			store:               leaseStore,
			leaseTTL:            cfg.Store.LeaseTTL,
			leaseRenewInterval:  cfg.Store.LeaseRenewInterval,
			leaseReleaseTimeout: cfg.Store.LockTimeout,
		})
	}

	return loops, workers, nil
}

func marketWindow(market config.MarketConfig, sources []feeds.Source) (sessions.WindowFunc, error) {
	if market.SessionMode != config.SessionModeScheduled {
		return nil, nil
	}

	if market.SessionSource == "" {
		return nil, errors.New("app: scheduled market requires session_source")
	}

	source, ok := feeds.SourceByName(sources, market.SessionSource)
	if !ok {
		return nil, errors.New("app: scheduled market session source was not built")
	}

	sessionAware, ok := source.(feeds.SessionAwareSource)
	if !ok {
		return nil, errors.New("app: scheduled market session source does not expose session windows")
	}

	return func(at time.Time) sessions.Window {
		if sessionAware.SessionOpenAt(at) {
			return sessions.OpenWindow()
		}
		return sessions.ExternalStaleWindow(status.ReasonSessionClosed)
	}, nil
}
