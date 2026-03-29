package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/lumosimmo/market-relayer-go/internal/app"
	"github.com/lumosimmo/market-relayer-go/internal/operator"
)

func TestRunValidateConfigSuccess(t *testing.T) {
	t.Parallel()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	var gotOptions app.Options

	exitCode := run([]string{"-config", "/tmp/config.yaml", "-validate-config"}, &stdout, &stderr, commandDeps{
		signalContext: func(parent context.Context) (context.Context, context.CancelFunc) {
			t.Fatal("signalContext() should not be called for validate-config mode")
			return parent, func() {}
		},
		validateConfig: func(options app.Options) (app.RuntimeMetadata, error) {
			gotOptions = options
			return app.RuntimeMetadata{
				ConfigDigest:            "digest-123",
				PricingAlgorithmVersion: "1",
			}, nil
		},
		migrateStore: func(context.Context, app.Options) (int64, error) {
			t.Fatal("migrateStore() should not be called for validate-config mode")
			return 0, nil
		},
		currentStoreSchema: func(context.Context, app.Options) (int64, error) {
			t.Fatal("currentStoreSchema() should not be called for validate-config mode")
			return 0, nil
		},
		recover: func(context.Context, app.Options, app.RecoveryOptions) (operator.RecoveryReport, error) {
			t.Fatal("recover() should not be called for validate-config mode")
			return operator.RecoveryReport{}, nil
		},
		run: func(context.Context, app.Options) error {
			t.Fatal("run() should not be called for validate-config mode")
			return nil
		},
	})
	if exitCode != 0 {
		t.Fatalf("exit code = %d, want 0", exitCode)
	}
	if gotOptions.ConfigPath != "/tmp/config.yaml" {
		t.Fatalf("ConfigPath = %q, want /tmp/config.yaml", gotOptions.ConfigPath)
	}
	if got := stdout.String(); got != "config_digest=digest-123 pricing_algorithm_version=1\n" {
		t.Fatalf("stdout = %q, want metadata line", got)
	}
	if stderr.Len() != 0 {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}

func TestRunValidateConfigFailure(t *testing.T) {
	t.Parallel()

	var stdout bytes.Buffer
	var stderr bytes.Buffer

	exitCode := run([]string{"-validate-config"}, &stdout, &stderr, commandDeps{
		signalContext: func(parent context.Context) (context.Context, context.CancelFunc) {
			return parent, func() {}
		},
		validateConfig: func(app.Options) (app.RuntimeMetadata, error) {
			return app.RuntimeMetadata{}, errors.New("bad config")
		},
		migrateStore: func(context.Context, app.Options) (int64, error) {
			return 0, nil
		},
		currentStoreSchema: func(context.Context, app.Options) (int64, error) {
			return 0, nil
		},
		recover: func(context.Context, app.Options, app.RecoveryOptions) (operator.RecoveryReport, error) {
			return operator.RecoveryReport{}, nil
		},
		run: func(context.Context, app.Options) error {
			return nil
		},
	})
	if exitCode != 1 {
		t.Fatalf("exit code = %d, want 1", exitCode)
	}
	if stdout.Len() != 0 {
		t.Fatalf("stdout = %q, want empty", stdout.String())
	}
	if got := stderr.String(); !strings.Contains(got, "config validation failed") || !strings.Contains(got, "bad config") {
		t.Fatalf("stderr = %q, want validation failure log", got)
	}
}

func TestRunRecoverySuccess(t *testing.T) {
	t.Parallel()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	var gotOptions app.Options
	var gotRecovery app.RecoveryOptions
	var signalUsed bool

	exitCode := run([]string{"-config", "/tmp/recovery.yaml", "-recover", "resume-pending", "-market", "ETHUSD", "-record-id", "cycle-1"}, &stdout, &stderr, commandDeps{
		signalContext: func(parent context.Context) (context.Context, context.CancelFunc) {
			signalUsed = true
			return parent, func() {}
		},
		validateConfig: func(app.Options) (app.RuntimeMetadata, error) {
			t.Fatal("validateConfig() should not be called for recovery mode")
			return app.RuntimeMetadata{}, nil
		},
		migrateStore: func(context.Context, app.Options) (int64, error) {
			t.Fatal("migrateStore() should not be called for recovery mode")
			return 0, nil
		},
		currentStoreSchema: func(context.Context, app.Options) (int64, error) {
			t.Fatal("currentStoreSchema() should not be called for recovery mode")
			return 0, nil
		},
		recover: func(_ context.Context, options app.Options, recovery app.RecoveryOptions) (operator.RecoveryReport, error) {
			gotOptions = options
			gotRecovery = recovery
			return operator.RecoveryReport{
				Mode:              operator.ModeResumePending,
				ReplayedSequences: []uint64{7},
				NextSequence:      8,
			}, nil
		},
		run: func(context.Context, app.Options) error {
			t.Fatal("run() should not be called for recovery mode")
			return nil
		},
	})
	if exitCode != 0 {
		t.Fatalf("exit code = %d, want 0", exitCode)
	}
	if gotOptions.ConfigPath != "/tmp/recovery.yaml" {
		t.Fatalf("ConfigPath = %q, want /tmp/recovery.yaml", gotOptions.ConfigPath)
	}
	if gotRecovery.Mode != operator.ModeResumePending || gotRecovery.Market != "ETHUSD" || gotRecovery.RecordID != "cycle-1" {
		t.Fatalf("RecoveryOptions = %+v, want resume-pending ETHUSD cycle-1", gotRecovery)
	}
	if !signalUsed {
		t.Fatal("signalContext() was not called for recovery mode")
	}
	var report operator.RecoveryReport
	if err := json.Unmarshal(stdout.Bytes(), &report); err != nil {
		t.Fatalf("json.Unmarshal(stdout) error = %v", err)
	}
	if report.NextSequence != 8 || len(report.ReplayedSequences) != 1 || report.ReplayedSequences[0] != 7 {
		t.Fatalf("report = %+v, want replayed 7 next 8", report)
	}
	if stderr.Len() != 0 {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}

func TestRunServiceModeUsesSignalContext(t *testing.T) {
	t.Parallel()

	type contextKey string

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	var gotOptions app.Options
	var signalUsed bool

	exitCode := run([]string{"-config", "/tmp/service.yaml"}, &stdout, &stderr, commandDeps{
		signalContext: func(parent context.Context) (context.Context, context.CancelFunc) {
			signalUsed = true
			return context.WithValue(parent, contextKey("signal"), "installed"), func() {}
		},
		validateConfig: func(app.Options) (app.RuntimeMetadata, error) {
			return app.RuntimeMetadata{}, nil
		},
		migrateStore: func(context.Context, app.Options) (int64, error) {
			return 0, nil
		},
		currentStoreSchema: func(context.Context, app.Options) (int64, error) {
			return 0, nil
		},
		recover: func(context.Context, app.Options, app.RecoveryOptions) (operator.RecoveryReport, error) {
			return operator.RecoveryReport{}, nil
		},
		run: func(ctx context.Context, options app.Options) error {
			gotOptions = options
			if got := ctx.Value(contextKey("signal")); got != "installed" {
				t.Fatalf("signal context value = %v, want installed", got)
			}
			return nil
		},
	})
	if exitCode != 0 {
		t.Fatalf("exit code = %d, want 0", exitCode)
	}
	if !signalUsed {
		t.Fatal("signalContext() was not called")
	}
	if gotOptions.ConfigPath != "/tmp/service.yaml" {
		t.Fatalf("ConfigPath = %q, want /tmp/service.yaml", gotOptions.ConfigPath)
	}
	if stdout.Len() != 0 || stderr.Len() != 0 {
		t.Fatalf("output = stdout:%q stderr:%q, want empty", stdout.String(), stderr.String())
	}
}

func TestRunServiceQuietSuppressesInfoLogs(t *testing.T) {
	t.Parallel()

	var stdout bytes.Buffer
	var stderr bytes.Buffer

	exitCode := run([]string{"-quiet", "-config", "/tmp/service.yaml"}, &stdout, &stderr, commandDeps{
		signalContext: func(parent context.Context) (context.Context, context.CancelFunc) {
			return parent, func() {}
		},
		validateConfig: func(app.Options) (app.RuntimeMetadata, error) {
			return app.RuntimeMetadata{}, nil
		},
		migrateStore: func(context.Context, app.Options) (int64, error) {
			return 0, nil
		},
		currentStoreSchema: func(context.Context, app.Options) (int64, error) {
			return 0, nil
		},
		recover: func(context.Context, app.Options, app.RecoveryOptions) (operator.RecoveryReport, error) {
			return operator.RecoveryReport{}, nil
		},
		run: func(_ context.Context, options app.Options) error {
			options.Logger.Info("suppressed info log")
			options.Logger.Warn("visible warn log")
			return nil
		},
	})
	if exitCode != 0 {
		t.Fatalf("exit code = %d, want 0", exitCode)
	}
	if stdout.Len() != 0 {
		t.Fatalf("stdout = %q, want empty", stdout.String())
	}
	got := stderr.String()
	if strings.Contains(got, "suppressed info log") {
		t.Fatalf("stderr = %q, want info log suppressed", got)
	}
	if !strings.Contains(got, "visible warn log") {
		t.Fatalf("stderr = %q, want warn log visible", got)
	}
}

func TestRunServiceDebugEmitsDebugLogs(t *testing.T) {
	t.Parallel()

	var stdout bytes.Buffer
	var stderr bytes.Buffer

	exitCode := run([]string{"-log-level", "debug", "-config", "/tmp/service.yaml"}, &stdout, &stderr, commandDeps{
		signalContext: func(parent context.Context) (context.Context, context.CancelFunc) {
			return parent, func() {}
		},
		validateConfig: func(app.Options) (app.RuntimeMetadata, error) {
			return app.RuntimeMetadata{}, nil
		},
		migrateStore: func(context.Context, app.Options) (int64, error) {
			return 0, nil
		},
		currentStoreSchema: func(context.Context, app.Options) (int64, error) {
			return 0, nil
		},
		recover: func(context.Context, app.Options, app.RecoveryOptions) (operator.RecoveryReport, error) {
			return operator.RecoveryReport{}, nil
		},
		run: func(_ context.Context, options app.Options) error {
			options.Logger.Log(context.Background(), slog.LevelDebug, "visible debug log")
			return nil
		},
	})
	if exitCode != 0 {
		t.Fatalf("exit code = %d, want 0", exitCode)
	}
	if stdout.Len() != 0 {
		t.Fatalf("stdout = %q, want empty", stdout.String())
	}
	if got := stderr.String(); !strings.Contains(got, "visible debug log") {
		t.Fatalf("stderr = %q, want debug log", got)
	}
}

func TestRunParseErrorReturnsTwo(t *testing.T) {
	t.Parallel()

	var stdout bytes.Buffer
	var stderr bytes.Buffer

	exitCode := run([]string{"-unknown"}, &stdout, &stderr, commandDeps{})
	if exitCode != 2 {
		t.Fatalf("exit code = %d, want 2", exitCode)
	}
	if stdout.Len() != 0 {
		t.Fatalf("stdout = %q, want empty", stdout.String())
	}
	if got := stderr.String(); !strings.Contains(got, "flag provided but not defined") {
		t.Fatalf("stderr = %q, want parse error", got)
	}
}

func TestRunMigrateStoreSuccess(t *testing.T) {
	t.Parallel()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	var gotOptions app.Options
	var signalUsed bool

	exitCode := run([]string{"-config", "/tmp/store.yaml", "-migrate-store"}, &stdout, &stderr, commandDeps{
		signalContext: func(parent context.Context) (context.Context, context.CancelFunc) {
			signalUsed = true
			return parent, func() {}
		},
		validateConfig: func(app.Options) (app.RuntimeMetadata, error) {
			t.Fatal("validateConfig() should not be called for migrate-store mode")
			return app.RuntimeMetadata{}, nil
		},
		migrateStore: func(_ context.Context, options app.Options) (int64, error) {
			gotOptions = options
			return 7, nil
		},
		currentStoreSchema: func(context.Context, app.Options) (int64, error) {
			t.Fatal("currentStoreSchema() should not be called for migrate-store mode")
			return 0, nil
		},
		recover: func(context.Context, app.Options, app.RecoveryOptions) (operator.RecoveryReport, error) {
			t.Fatal("recover() should not be called for migrate-store mode")
			return operator.RecoveryReport{}, nil
		},
		run: func(context.Context, app.Options) error {
			t.Fatal("run() should not be called for migrate-store mode")
			return nil
		},
	})
	if exitCode != 0 {
		t.Fatalf("exit code = %d, want 0", exitCode)
	}
	if gotOptions.ConfigPath != "/tmp/store.yaml" {
		t.Fatalf("ConfigPath = %q, want /tmp/store.yaml", gotOptions.ConfigPath)
	}
	if !signalUsed {
		t.Fatal("signalContext() was not called for migrate-store mode")
	}
	if got := stdout.String(); got != "schema_version=7\n" {
		t.Fatalf("stdout = %q, want schema version line", got)
	}
	if stderr.Len() != 0 {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}

func TestRunStoreVersionSuccess(t *testing.T) {
	t.Parallel()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	var signalUsed bool

	exitCode := run([]string{"-store-version"}, &stdout, &stderr, commandDeps{
		signalContext: func(parent context.Context) (context.Context, context.CancelFunc) {
			signalUsed = true
			return parent, func() {}
		},
		validateConfig: func(app.Options) (app.RuntimeMetadata, error) {
			t.Fatal("validateConfig() should not be called for store-version mode")
			return app.RuntimeMetadata{}, nil
		},
		migrateStore: func(context.Context, app.Options) (int64, error) {
			t.Fatal("migrateStore() should not be called for store-version mode")
			return 0, nil
		},
		currentStoreSchema: func(context.Context, app.Options) (int64, error) {
			return 3, nil
		},
		recover: func(context.Context, app.Options, app.RecoveryOptions) (operator.RecoveryReport, error) {
			t.Fatal("recover() should not be called for store-version mode")
			return operator.RecoveryReport{}, nil
		},
		run: func(context.Context, app.Options) error {
			t.Fatal("run() should not be called for store-version mode")
			return nil
		},
	})
	if exitCode != 0 {
		t.Fatalf("exit code = %d, want 0", exitCode)
	}
	if !signalUsed {
		t.Fatal("signalContext() was not called for store-version mode")
	}
	if got := stdout.String(); got != "schema_version=3\n" {
		t.Fatalf("stdout = %q, want schema version line", got)
	}
	if stderr.Len() != 0 {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}
