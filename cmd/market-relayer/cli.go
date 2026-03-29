package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/lumosimmo/market-relayer-go/internal/app"
	"github.com/lumosimmo/market-relayer-go/internal/operator"
)

type commandDeps struct {
	signalContext      func(context.Context) (context.Context, context.CancelFunc)
	validateConfig     func(app.Options) (app.RuntimeMetadata, error)
	migrateStore       func(context.Context, app.Options) (int64, error)
	currentStoreSchema func(context.Context, app.Options) (int64, error)
	recover            func(context.Context, app.Options, app.RecoveryOptions) (operator.RecoveryReport, error)
	run                func(context.Context, app.Options) error
}

func defaultCommandDeps() commandDeps {
	return commandDeps{
		signalContext: func(parent context.Context) (context.Context, context.CancelFunc) {
			return signal.NotifyContext(parent, os.Interrupt, syscall.SIGTERM)
		},
		validateConfig:     app.ValidateConfig,
		migrateStore:       app.MigrateStore,
		currentStoreSchema: app.CurrentStoreVersion,
		recover:            app.Recover,
		run:                app.Run,
	}
}

type mode int

const (
	modeRun mode = iota
	modeValidateConfig
	modeMigrateStore
	modeStoreVersion
	modeRecover
)

type runConfig struct {
	configPath     string
	mode           mode
	recoveryMode   string
	recoveryMarket string
	recoveryRecord string
	logLevel       slog.Level
}

func run(args []string, stdout io.Writer, stderr io.Writer, deps commandDeps) int {
	cfg, exitCode := parseRunConfig(args, stderr)
	if exitCode != 0 {
		return exitCode
	}

	logger := newLogger(stderr, cfg.logLevel)
	options := app.Options{
		ConfigPath: cfg.configPath,
		Logger:     logger,
	}

	switch cfg.mode {
	case modeValidateConfig:
		return runValidateConfig(stdout, logger, options, deps)
	case modeMigrateStore:
		return runMigrateStore(stdout, logger, options, deps)
	case modeStoreVersion:
		return runStoreVersion(stdout, logger, options, deps)
	case modeRecover:
		return runRecovery(stdout, logger, options, deps, cfg)
	default:
		return runService(stdout, logger, options, deps)
	}
}

func parseRunConfig(args []string, stderr io.Writer) (runConfig, int) {
	cfg := runConfig{
		configPath: "configs/markets.example.yaml",
		mode:       modeRun,
		logLevel:   slog.LevelInfo,
	}
	var (
		validateConfig bool
		migrateStore   bool
		storeVersion   bool
		recoveryMode   string
		logLevelRaw    string
		quiet          bool
	)

	flags := flag.NewFlagSet(Name(), flag.ContinueOnError)
	flags.SetOutput(stderr)
	flags.StringVar(&cfg.configPath, "config", cfg.configPath, "path to YAML config")
	flags.StringVar(&logLevelRaw, "log-level", "info", "stderr log level: debug, info, warn, or error")
	flags.BoolVar(&quiet, "quiet", false, "shorthand for -log-level warn")
	flags.BoolVar(&validateConfig, "validate-config", false, "validate config and print runtime metadata")
	flags.BoolVar(&migrateStore, "migrate-store", false, "apply Postgres store schema migrations and exit")
	flags.BoolVar(&storeVersion, "store-version", false, "print current Postgres store schema version and exit")
	flags.StringVar(&recoveryMode, "recover", "", "operator recovery mode: exact, audit, or resume-pending")
	flags.StringVar(&cfg.recoveryMarket, "market", "", "market symbol for recovery commands")
	flags.StringVar(&cfg.recoveryRecord, "record-id", "", "stored cycle record ID for exact/audit recovery")
	if err := flags.Parse(args); err != nil {
		return runConfig{}, 2
	}

	modeCount := 0
	if validateConfig {
		cfg.mode = modeValidateConfig
		modeCount++
	}
	if migrateStore {
		cfg.mode = modeMigrateStore
		modeCount++
	}
	if storeVersion {
		cfg.mode = modeStoreVersion
		modeCount++
	}
	if recoveryMode != "" {
		cfg.mode = modeRecover
		cfg.recoveryMode = recoveryMode
		modeCount++
	}
	if modeCount > 1 {
		fmt.Fprintln(stderr, "exactly one of -validate-config, -migrate-store, -store-version, or -recover may be set")
		return runConfig{}, 2
	}

	if quiet {
		if !strings.EqualFold(strings.TrimSpace(logLevelRaw), "info") {
			fmt.Fprintln(stderr, "-quiet cannot be combined with -log-level")
			return runConfig{}, 2
		}
		logLevelRaw = "warn"
	}

	logLevel, err := parseLogLevel(logLevelRaw)
	if err != nil {
		fmt.Fprintf(stderr, "invalid -log-level %q: %v\n", logLevelRaw, err)
		return runConfig{}, 2
	}
	cfg.logLevel = logLevel

	return cfg, 0
}

func newLogger(output io.Writer, level slog.Level) *slog.Logger {
	return slog.New(slog.NewTextHandler(output, &slog.HandlerOptions{Level: level}))
}

func parseLogLevel(raw string) (slog.Level, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		raw = slog.LevelInfo.String()
	}
	if strings.EqualFold(raw, "warning") {
		raw = slog.LevelWarn.String()
	}

	var level slog.Level
	if err := level.UnmarshalText([]byte(raw)); err != nil {
		return 0, fmt.Errorf("supported levels are debug, info, warn, and error")
	}

	return level, nil
}

func runValidateConfig(stdout io.Writer, logger *slog.Logger, options app.Options, deps commandDeps) int {
	metadata, err := deps.validateConfig(options)
	if err != nil {
		logger.Error("config validation failed", "error", err)
		return 1
	}
	fmt.Fprintf(stdout, "config_digest=%s pricing_algorithm_version=%s\n", metadata.ConfigDigest, metadata.PricingAlgorithmVersion)
	return 0
}

func runMigrateStore(stdout io.Writer, logger *slog.Logger, options app.Options, deps commandDeps) int {
	ctx, stop := newSignalContext(deps)
	defer stop()

	version, err := deps.migrateStore(ctx, options)
	if err != nil {
		logger.Error("store migration failed", "error", err)
		return 1
	}
	fmt.Fprintf(stdout, "schema_version=%d\n", version)
	return 0
}

func runStoreVersion(stdout io.Writer, logger *slog.Logger, options app.Options, deps commandDeps) int {
	ctx, stop := newSignalContext(deps)
	defer stop()

	version, err := deps.currentStoreSchema(ctx, options)
	if err != nil {
		logger.Error("store version lookup failed", "error", err)
		return 1
	}
	fmt.Fprintf(stdout, "schema_version=%d\n", version)
	return 0
}

func runRecovery(stdout io.Writer, logger *slog.Logger, options app.Options, deps commandDeps, cfg runConfig) int {
	ctx, stop := newSignalContext(deps)
	defer stop()

	report, err := deps.recover(ctx, options, app.RecoveryOptions{
		Mode:     operator.RecoveryMode(cfg.recoveryMode),
		Market:   cfg.recoveryMarket,
		RecordID: cfg.recoveryRecord,
	})
	if err != nil {
		logger.Error("recovery failed", "mode", cfg.recoveryMode, "market", cfg.recoveryMarket, "record_id", cfg.recoveryRecord, "error", err)
		return 1
	}
	if err := json.NewEncoder(stdout).Encode(report); err != nil {
		logger.Error("encode recovery report failed", "error", err)
		return 1
	}
	return 0
}

func runService(_ io.Writer, logger *slog.Logger, options app.Options, deps commandDeps) int {
	ctx, stop := newSignalContext(deps)
	defer stop()
	if err := deps.run(ctx, options); err != nil {
		logger.Error("service exited", "error", err)
		return 1
	}
	return 0
}

func newSignalContext(deps commandDeps) (context.Context, context.CancelFunc) {
	if deps.signalContext == nil {
		return context.Background(), func() {}
	}
	return deps.signalContext(context.Background())
}
