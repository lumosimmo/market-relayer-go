package app

import (
	"context"
	"log/slog"

	"github.com/lumosimmo/market-relayer-go/internal/api"
	"github.com/lumosimmo/market-relayer-go/internal/clock"
	"github.com/lumosimmo/market-relayer-go/internal/config"
	"github.com/lumosimmo/market-relayer-go/internal/feeds"
	"github.com/lumosimmo/market-relayer-go/internal/metrics"
	"github.com/lumosimmo/market-relayer-go/internal/pricing"
	"github.com/lumosimmo/market-relayer-go/internal/relayer"
	"github.com/lumosimmo/market-relayer-go/internal/store"
)

const Name = "market-relayer-go"

type Options struct {
	ConfigPath string
	Clock      clock.Clock
	FeedBuild  feeds.BuildOptions
	LookupEnv  func(string) (string, bool)
	Logger     *slog.Logger
	StoreOpen  func(context.Context, config.StoreConfig) (runtimeStore, error)
}

type RuntimeMetadata struct {
	ConfigDigest            string
	PricingAlgorithmVersion string
}

func Bootstrap(ctx context.Context, options Options) (*Runtime, error) {
	cfg, metadata, err := loadRuntimeMetadata(options)
	if err != nil {
		return nil, err
	}

	storeOpen := options.StoreOpen
	if storeOpen == nil {
		storeOpen = func(ctx context.Context, cfg config.StoreConfig) (runtimeStore, error) {
			return store.OpenConfigContext(ctx, cfg)
		}
	}

	storeDB, err := storeOpen(ctx, cfg.Store)
	if err != nil {
		return nil, err
	}

	sink, err := relayer.NewSink(cfg.Sink)
	if err != nil {
		_ = storeDB.Close()
		return nil, err
	}

	serviceClock := options.Clock
	if serviceClock == nil {
		serviceClock = clock.Real{}
	}

	metricsRegistry := metrics.NewRegistry()
	feedBuild := options.FeedBuild
	feedBuild.Clock = serviceClock
	feedBuild.Logger = options.Logger
	loops, workers, err := buildLoops(cfg, metadata, storeDB, sink, feedBuild, serviceClock, metricsRegistry, options.Logger)
	if err != nil {
		_ = sink.Close()
		_ = storeDB.Close()
		return nil, err
	}

	apiMetadata := api.Metadata{
		ServiceName:             cfg.Service.Name,
		InstanceID:              cfg.Service.InstanceID,
		ConfigDigest:            metadata.ConfigDigest,
		PricingAlgorithmVersion: metadata.PricingAlgorithmVersion,
		BootedAt:                serviceClock.Now(),
	}
	state := newRuntimeState(apiMetadata, serviceClock, cfg, storeDB, loops)

	server, err := api.NewServer(api.Options{
		ListenAddr:     cfg.API.ListenAddr,
		BaseContext:    ctx,
		Metadata:       apiMetadata,
		Reporter:       state,
		MetricsHandler: metricsRegistry.Handler(),
	})
	if err != nil {
		_ = sink.Close()
		_ = storeDB.Close()
		return nil, err
	}
	server.Start()

	return &Runtime{
		Config:   cfg,
		Metadata: metadata,
		Store:    storeDB,
		Sink:     sink,
		Metrics:  metricsRegistry,
		API:      server,
		Loops:    loops,
		workers:  workers,
		state:    state,
	}, nil
}

func ValidateConfig(options Options) (RuntimeMetadata, error) {
	_, metadata, err := loadRuntimeMetadata(options)
	if err != nil {
		return RuntimeMetadata{}, err
	}
	return metadata, nil
}

func loadRuntimeMetadata(options Options) (*config.Config, RuntimeMetadata, error) {
	cfg, err := config.LoadFile(options.ConfigPath, config.LoadOptions{
		LookupEnv: options.LookupEnv,
	})
	if err != nil {
		return nil, RuntimeMetadata{}, err
	}
	digest, err := cfg.Digest()
	if err != nil {
		return nil, RuntimeMetadata{}, err
	}

	return cfg, RuntimeMetadata{
		ConfigDigest:            digest,
		PricingAlgorithmVersion: pricing.AlgorithmVersion,
	}, nil
}
