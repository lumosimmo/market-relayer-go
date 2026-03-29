package config

import (
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestConfigLastMileValidationBranches(t *testing.T) {
	t.Parallel()

	t.Run("timestamp modes load file and secret resolution surface remaining errors", func(t *testing.T) {
		t.Parallel()

		if TimestampMode("bad").Valid() {
			t.Fatal("TimestampMode(bad).Valid() = true, want false")
		}

		if _, err := LoadFile(filepath.Join(t.TempDir(), "missing-config.yaml"), LoadOptions{}); err == nil {
			t.Fatal("LoadFile(missing) error = nil, want read failure")
		}

		input := strings.Replace(validConfigYAML(), "request_timeout: 500ms", "request_timeout: 500ms\n  bearer_token_env: MARKET_RELAYER_MISSING_TOKEN", 1)
		if _, err := LoadBytes([]byte(input), LoadOptions{
			LookupEnv: func(string) (string, bool) { return "", false },
		}); err == nil {
			t.Fatal("LoadBytes(missing bearer token env) error = nil, want secret resolution failure")
		}
	})

	t.Run("ordered sources and top-level validation cover missing-reference and required-field branches", func(t *testing.T) {
		t.Parallel()

		cfg, err := LoadBytes([]byte(validConfigYAML()), LoadOptions{})
		if err != nil {
			t.Fatalf("LoadBytes() error = %v", err)
		}
		cfg.Markets[0].Sources[0].Name = "missing-source"
		if _, err := cfg.OrderedSourcesForMarket("ETHUSD"); err == nil {
			t.Fatal("OrderedSourcesForMarket(missing source) error = nil, want failure")
		}

		cfg, err = LoadBytes([]byte(validConfigYAML()), LoadOptions{})
		if err != nil {
			t.Fatalf("LoadBytes() error = %v", err)
		}
		cfg.Service.InstanceID = ""
		if err := cfg.validateTopLevel(); err == nil {
			t.Fatal("validateTopLevel(missing instance id) error = nil, want validation failure")
		}

		cfg, err = LoadBytes([]byte(validConfigYAML()), LoadOptions{})
		if err != nil {
			t.Fatalf("LoadBytes() error = %v", err)
		}
		cfg.API.ShutdownTimeout = maxFreshnessDuration + time.Second
		if err := cfg.validateTopLevel(); err == nil {
			t.Fatal("validateTopLevel(shutdown timeout too large) error = nil, want validation failure")
		}

		cfg, err = LoadBytes([]byte(validConfigYAML()), LoadOptions{})
		if err != nil {
			t.Fatalf("LoadBytes() error = %v", err)
		}
		cfg.Sources = nil
		if err := cfg.validateTopLevel(); err == nil {
			t.Fatal("validateTopLevel(no sources) error = nil, want validation failure")
		}

		cfg, err = LoadBytes([]byte(validConfigYAML()), LoadOptions{})
		if err != nil {
			t.Fatalf("LoadBytes() error = %v", err)
		}
		cfg.Markets = nil
		if err := cfg.validateTopLevel(); err == nil {
			t.Fatal("validateTopLevel(no markets) error = nil, want validation failure")
		}
	})

	t.Run("source and market validators cover remaining direct edge branches", func(t *testing.T) {
		t.Parallel()

		cfg, err := LoadBytes([]byte(validConfigYAML()), LoadOptions{})
		if err != nil {
			t.Fatalf("LoadBytes() error = %v", err)
		}

		duplicate := &Config{Sources: []SourceConfig{
			{Name: "dup", Kind: "coinbase", ProductID: "ETH-USD", RequestTimeout: time.Second, TimestampMode: TimestampModeSourceTimestamp},
			{Name: "dup", Kind: "coinbase", ProductID: "ETH-USD", RequestTimeout: time.Second, TimestampMode: TimestampModeSourceTimestamp},
		}}
		if err := duplicate.buildSourceIndex(); err == nil {
			t.Fatal("buildSourceIndex(duplicate names) error = nil, want failure")
		}

		market := MarketConfig{
			Symbol:                 "ETHUSD",
			SessionMode:            SessionMode("bad"),
			PerpBookSource:         "hyperliquid-eth",
			ImpactNotionalUSD:      "10000",
			PublishInterval:        time.Second,
			StalenessThreshold:     time.Second,
			MaxFallbackAge:         time.Second,
			ClampBPS:               1,
			DivergenceThresholdBPS: 1,
			PublishScale:           2,
			Sources:                []MarketSourceRef{{Name: "coinbase-primary"}},
		}
		if err := cfg.validateMarket(&market); err == nil {
			t.Fatal("validateMarket(invalid session mode) error = nil, want validation failure")
		}

		market = MarketConfig{
			Symbol:                 "ETHUSD",
			SessionMode:            SessionModeAlwaysOpen,
			PerpBookSource:         "hyperliquid-eth",
			ImpactNotionalUSD:      "10000",
			PublishInterval:        time.Second,
			StalenessThreshold:     maxFreshnessDuration + time.Second,
			MaxFallbackAge:         time.Second,
			ClampBPS:               1,
			DivergenceThresholdBPS: 1,
			PublishScale:           2,
			Sources:                []MarketSourceRef{{Name: "coinbase-primary"}},
		}
		if err := cfg.validateMarket(&market); err == nil {
			t.Fatal("validateMarket(staleness too large) error = nil, want validation failure")
		}

		market = MarketConfig{
			Symbol:                 "ETHUSD",
			SessionMode:            SessionModeAlwaysOpen,
			PerpBookSource:         "hyperliquid-eth",
			ImpactNotionalUSD:      "10000",
			PublishInterval:        time.Second,
			StalenessThreshold:     time.Second,
			MaxFallbackAge:         maxFreshnessDuration + time.Second,
			ClampBPS:               1,
			DivergenceThresholdBPS: 1,
			PublishScale:           2,
			Sources:                []MarketSourceRef{{Name: "coinbase-primary"}},
		}
		if err := cfg.validateMarket(&market); err == nil {
			t.Fatal("validateMarket(max fallback too large) error = nil, want validation failure")
		}

		if err := validateSource(SourceConfig{Name: "bad", Kind: "feed", RequestTimeout: time.Second, TimestampMode: TimestampModeSourceTimestamp}); err == nil {
			t.Fatal("validateSource(unsupported kind) error = nil, want failure")
		}
		if err := validateSource(SourceConfig{Name: "coinbase", Kind: "coinbase", ProductID: "ETH-USD", RequestTimeout: time.Second, TimestampMode: TimestampMode("bad")}); err == nil {
			t.Fatal("validateSource(invalid timestamp mode) error = nil, want failure")
		}
		if err := validateSource(SourceConfig{Name: "coinbase", Kind: "coinbase", ProductID: "ETH-USD", RequestTimeout: maxTimeoutDuration + time.Second, TimestampMode: TimestampModeSourceTimestamp}); err == nil {
			t.Fatal("validateSource(request timeout too large) error = nil, want failure")
		}
		if err := validateSource(SourceConfig{
			Name:             "pyth-wti",
			Kind:             SourceKindPyth,
			AssetType:        "Commodities",
			RootSymbol:       "WTI",
			QuoteCurrency:    "USD",
			CurveMode:        PythCurveModeNearbyLinearRoll,
			RollWindowDays:   5,
			MaxConfidenceBPS: 250,
			RequestTimeout:   time.Second,
			TimestampMode:    TimestampModeSourceTimestamp,
		}); err != nil {
			t.Fatalf("validateSource(valid pyth) error = %v", err)
		}
		if err := validateMaxUnchangedAge(SourceConfig{Name: "kraken", Kind: "kraken", Pair: "ETH/USD", RequestTimeout: time.Second, TimestampMode: TimestampModeReceivedAt, MaxUnchangedAge: maxFreshnessDuration + time.Second}); err == nil {
			t.Fatal("validateMaxUnchangedAge(too large) error = nil, want failure")
		}
		if err := validateDuration("test.duration", maxFreshnessDuration+time.Second, 0, maxFreshnessDuration); err == nil {
			t.Fatal("validateDuration(too large) error = nil, want failure")
		}
	})

	t.Run("load bytes reports invalid yaml without alias pre-pass", func(t *testing.T) {
		t.Parallel()

		if _, err := LoadBytes([]byte("service: ["), LoadOptions{}); err == nil {
			t.Fatal("LoadBytes(invalid yaml) error = nil, want decode failure")
		}
	})
}
