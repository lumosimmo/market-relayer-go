package main

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/lumosimmo/market-relayer-go/internal/app"
	"github.com/lumosimmo/market-relayer-go/internal/operator"
)

func TestRunRecoveryFailureAndEncodeFailure(t *testing.T) {
	t.Parallel()

	t.Run("recovery failure", func(t *testing.T) {
		t.Parallel()

		var stdout bytes.Buffer
		var stderr bytes.Buffer

		exitCode := run([]string{"-recover", "exact", "-market", "ETHUSD", "-record-id", "cycle-1"}, &stdout, &stderr, commandDeps{
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
				return operator.RecoveryReport{}, errors.New("recovery failed")
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
		if got := stderr.String(); !strings.Contains(got, "recovery failed") {
			t.Fatalf("stderr = %q, want recovery failure log", got)
		}
	})

	t.Run("recovery report encode failure", func(t *testing.T) {
		t.Parallel()

		var stderr bytes.Buffer
		exitCode := run([]string{"-recover", "resume-pending", "-market", "ETHUSD"}, failingWriter{}, &stderr, commandDeps{
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
				return operator.RecoveryReport{
					Mode:              operator.ModeResumePending,
					ReplayedSequences: []uint64{7},
				}, nil
			},
			run: func(context.Context, app.Options) error {
				return nil
			},
		})
		if exitCode != 1 {
			t.Fatalf("exit code = %d, want 1", exitCode)
		}
		if got := stderr.String(); !strings.Contains(got, "encode recovery report failed") {
			t.Fatalf("stderr = %q, want encode failure log", got)
		}
	})
}

func TestRunServiceFailureReturnsOne(t *testing.T) {
	t.Parallel()

	var stdout bytes.Buffer
	var stderr bytes.Buffer

	exitCode := run([]string{"-config", "/tmp/service.yaml"}, &stdout, &stderr, commandDeps{
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
		run: func(context.Context, app.Options) error {
			return errors.New("service failed")
		},
	})
	if exitCode != 1 {
		t.Fatalf("exit code = %d, want 1", exitCode)
	}
	if stdout.Len() != 0 {
		t.Fatalf("stdout = %q, want empty", stdout.String())
	}
	if got := stderr.String(); !strings.Contains(got, "service exited") || !strings.Contains(got, "service failed") {
		t.Fatalf("stderr = %q, want service failure log", got)
	}
}

func TestRunRejectsInvalidLogLevelAndQuietConflict(t *testing.T) {
	t.Parallel()

	t.Run("invalid log level", func(t *testing.T) {
		t.Parallel()

		var stdout bytes.Buffer
		var stderr bytes.Buffer

		exitCode := run([]string{"-log-level", "loud"}, &stdout, &stderr, commandDeps{})
		if exitCode != 2 {
			t.Fatalf("exit code = %d, want 2", exitCode)
		}
		if stdout.Len() != 0 {
			t.Fatalf("stdout = %q, want empty", stdout.String())
		}
		if got := stderr.String(); !strings.Contains(got, "invalid -log-level") {
			t.Fatalf("stderr = %q, want invalid log-level error", got)
		}
	})

	t.Run("quiet conflicts with explicit log level", func(t *testing.T) {
		t.Parallel()

		var stdout bytes.Buffer
		var stderr bytes.Buffer

		exitCode := run([]string{"-quiet", "-log-level", "error"}, &stdout, &stderr, commandDeps{})
		if exitCode != 2 {
			t.Fatalf("exit code = %d, want 2", exitCode)
		}
		if stdout.Len() != 0 {
			t.Fatalf("stdout = %q, want empty", stdout.String())
		}
		if got := stderr.String(); !strings.Contains(got, "-quiet cannot be combined with -log-level") {
			t.Fatalf("stderr = %q, want quiet/log-level conflict", got)
		}
	})
}

func TestParseRunConfigAcceptsCompatibleLogLevels(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		args  []string
		level slog.Level
	}{
		{
			name:  "defaults empty level to info",
			args:  []string{"-log-level", ""},
			level: slog.LevelInfo,
		},
		{
			name:  "accepts warning alias",
			args:  []string{"-log-level", "warning"},
			level: slog.LevelWarn,
		},
		{
			name:  "accepts stdlib case-insensitive names",
			args:  []string{"-log-level", "ERROR"},
			level: slog.LevelError,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var stderr bytes.Buffer

			cfg, exitCode := parseRunConfig(tt.args, &stderr)
			if exitCode != 0 {
				t.Fatalf("exit code = %d, want 0 (stderr=%q)", exitCode, stderr.String())
			}
			if got := cfg.logLevel; got != tt.level {
				t.Fatalf("cfg.logLevel = %v, want %v", got, tt.level)
			}
			if stderr.Len() != 0 {
				t.Fatalf("stderr = %q, want empty", stderr.String())
			}
		})
	}
}

func TestMainValidateConfigSubprocess(t *testing.T) {
	if os.Getenv("MARKET_RELAYER_RUN_MAIN") == "1" {
		split := -1
		for index, arg := range os.Args {
			if arg == "--" {
				split = index
				break
			}
		}
		if split < 0 {
			os.Exit(2)
		}
		os.Args = append([]string{"market-relayer"}, os.Args[split+1:]...)
		main()
		return
	}

	configPath := writeMainConfig(t)
	command := exec.Command(os.Args[0], "-test.run=TestMainValidateConfigSubprocess", "--", "-validate-config", "-config", configPath)
	command.Env = append(os.Environ(), "MARKET_RELAYER_RUN_MAIN=1")

	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("subprocess error = %v\noutput:\n%s", err, output)
	}
	if got := string(output); !strings.Contains(got, "config_digest=") || !strings.Contains(got, "pricing_algorithm_version=") {
		t.Fatalf("subprocess output = %q, want validation metadata", got)
	}
}

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) {
	return 0, errors.New("write failed")
}

func writeMainConfig(t *testing.T) string {
	t.Helper()

	configPath := filepath.Join(t.TempDir(), "config.yaml")
	config := `version: 1
service:
  name: market-relayer-go
  instance_id: main-test
store:
  dsn: postgres://postgres:postgres@127.0.0.1:5432/market_relayer?sslmode=disable
api:
  listen_addr: 127.0.0.1:0
sink:
  kind: http
  url: http://127.0.0.1:18080/publish
  request_timeout: 500ms
sources:
  - name: coinbase-primary
    kind: coinbase
    product_id: ETH-USD
    request_timeout: 1s
    timestamp_mode: source_timestamp
  - name: kraken-secondary
    kind: kraken
    pair: ETH/USD
    request_timeout: 1s
    timestamp_mode: received_at
    max_unchanged_age: 10s
  - name: hyperliquid-eth
    kind: hyperliquid
    market: ETH
    request_timeout: 1s
    timestamp_mode: source_timestamp
markets:
  - symbol: ETHUSD
    session_mode: always_open
    perp_book_source: hyperliquid-eth
    impact_notional_usd: "100"
    publish_interval: 3s
    staleness_threshold: 10s
    max_fallback_age: 30s
    clamp_bps: 50
    divergence_threshold_bps: 250
    publish_scale: 2
    sources:
      - name: coinbase-primary
      - name: kraken-secondary
`
	if err := os.WriteFile(configPath, []byte(config), 0o600); err != nil {
		t.Fatalf("os.WriteFile() error = %v", err)
	}
	return configPath
}
