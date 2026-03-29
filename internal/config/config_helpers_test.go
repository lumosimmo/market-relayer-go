package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestConfigHelperBranches(t *testing.T) {
	t.Run("load file and source lookups cover file path branch and unknown market handling", func(t *testing.T) {
		configPath := filepath.Join(t.TempDir(), "config.yaml")
		if err := os.WriteFile(configPath, []byte(validConfigYAML()), 0o600); err != nil {
			t.Fatalf("os.WriteFile() error = %v", err)
		}

		cfg, err := LoadFile(configPath, LoadOptions{})
		if err != nil {
			t.Fatalf("LoadFile() error = %v", err)
		}

		if _, ok := (&Config{}).SourceByName("coinbase-primary"); ok {
			t.Fatal("SourceByName() with nil index = true, want false")
		}
		if _, ok := cfg.SourceByName("coinbase-primary"); !ok {
			t.Fatal("SourceByName(existing) = false, want true")
		}
		if _, ok := cfg.SourceByName("missing-source"); ok {
			t.Fatal("SourceByName(missing) = true, want false")
		}
		if _, err := cfg.OrderedSourcesForMarket("BTCUSD"); err == nil {
			t.Fatal("OrderedSourcesForMarket(unknown market) error = nil, want unknown market")
		}
	})

	t.Run("defaults and validators cover file sink source and market edge cases", func(t *testing.T) {
		cfg := &Config{}
		cfg.applyDefaults()
		if cfg.Service.Name != DefaultServiceName {
			t.Fatalf("applyDefaults().Service.Name = %q, want %q", cfg.Service.Name, DefaultServiceName)
		}
		if cfg.API.ShutdownTimeout != defaultShutdownGrace {
			t.Fatalf("applyDefaults().API.ShutdownTimeout = %s, want %s", cfg.API.ShutdownTimeout, defaultShutdownGrace)
		}

		if err := validateSink(SinkConfig{Kind: "file", Path: "./out.jsonl", RequestTimeout: time.Second}); err != nil {
			t.Fatalf("validateSink(file) error = %v", err)
		}
		if err := validateStore(StoreConfig{
			DSN:                "postgres://relayer:relayer@127.0.0.1:5432/market_relayer?sslmode=disable",
			MaxOpenConns:       4,
			MinOpenConns:       2,
			ConnMaxLifetime:    time.Minute,
			ConnectTimeout:     time.Second,
			LockTimeout:        time.Second,
			LeaseTTL:           15 * time.Second,
			LeaseRenewInterval: 5 * time.Second,
		}); err != nil {
			t.Fatalf("validateStore(valid) error = %v", err)
		}
		if err := validateStore(StoreConfig{
			DSN:                "postgres://relayer:relayer@127.0.0.1:5432/market_relayer?sslmode=disable",
			MaxOpenConns:       1,
			MinOpenConns:       2,
			ConnMaxLifetime:    time.Minute,
			ConnectTimeout:     time.Second,
			LockTimeout:        time.Second,
			LeaseTTL:           15 * time.Second,
			LeaseRenewInterval: 15 * time.Second,
		}); err == nil {
			t.Fatal("validateStore(invalid pool and lease settings) error = nil, want validation")
		}
		if err := validateStore(StoreConfig{
			DSN:                "postgres://relayer:relayer@127.0.0.1:5432/market_relayer?sslmode=disable",
			MaxOpenConns:       4,
			MinOpenConns:       -1,
			ConnMaxLifetime:    time.Minute,
			ConnectTimeout:     time.Second,
			LockTimeout:        time.Second,
			LeaseTTL:           15 * time.Second,
			LeaseRenewInterval: 5 * time.Second,
		}); err == nil || !strings.Contains(err.Error(), "store.min_open_conns must be greater than or equal to 0") {
			t.Fatalf("validateStore(negative min open conns) error = %v, want min_open_conns validation", err)
		}
		if err := validateStore(StoreConfig{
			DSN:                "postgres://relayer:relayer@127.0.0.1:5432/market_relayer?sslmode=disable",
			MaxOpenConns:       4,
			MinOpenConns:       2,
			MaxIdleConns:       3,
			ConnMaxLifetime:    time.Minute,
			ConnectTimeout:     time.Second,
			LockTimeout:        time.Second,
			LeaseTTL:           15 * time.Second,
			LeaseRenewInterval: 5 * time.Second,
		}); err == nil || !strings.Contains(err.Error(), "store.max_idle_conns is deprecated") {
			t.Fatalf("validateStore(conflicting deprecated alias) error = %v, want deprecation validation", err)
		}
		if err := validateSink(SinkConfig{Kind: "http", URL: "/relative", RequestTimeout: time.Second}); err == nil {
			t.Fatal("validateSink(relative url) error = nil, want absolute URL validation")
		}
		if err := validateSink(SinkConfig{Kind: "file", RequestTimeout: time.Second}); err == nil {
			t.Fatal("validateSink(file without path) error = nil, want path validation")
		}
		if err := validateSink(SinkConfig{Kind: "queue", RequestTimeout: time.Second}); err == nil {
			t.Fatal("validateSink(unsupported) error = nil, want unsupported sink kind")
		}

		if err := validateSource(SourceConfig{Kind: "coinbase", ProductID: "ETH-USD", RequestTimeout: time.Second, TimestampMode: TimestampModeSourceTimestamp}); err == nil {
			t.Fatal("validateSource(missing name) error = nil, want name validation")
		}
		if err := validateSource(SourceConfig{Name: "kraken", Kind: "kraken", RequestTimeout: time.Second, TimestampMode: TimestampModeReceivedAt}); err == nil {
			t.Fatal("validateSource(kraken missing pair) error = nil, want pair validation")
		}
		if err := validateSource(SourceConfig{Name: "kraken", Kind: "kraken", Pair: "ETH/USD", RequestTimeout: time.Second, TimestampMode: TimestampModeSourceTimestamp}); err == nil {
			t.Fatal("validateSource(kraken wrong timestamp mode) error = nil, want timestamp mode validation")
		}
		if err := validateSource(SourceConfig{Name: "coinbase", Kind: "coinbase", ProductID: "ETH-USD", RequestTimeout: time.Second, TimestampMode: TimestampModeReceivedAt, MaxUnchangedAge: -time.Second}); err == nil {
			t.Fatal("validateSource(negative max unchanged age) error = nil, want validation")
		}
		if err := validateSource(SourceConfig{Name: "coinbase", Kind: "coinbase", ProductID: "ETH-USD", RequestTimeout: time.Second, TimestampMode: TimestampModeSourceTimestamp}); err != nil {
			t.Fatalf("validateSource(valid coinbase) error = %v", err)
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

		sourceCfg, err := LoadBytes([]byte(validConfigYAML()), LoadOptions{})
		if err != nil {
			t.Fatalf("LoadBytes() error = %v", err)
		}
		market := MarketConfig{
			Symbol:                 "bad-symbol",
			SessionMode:            SessionModeAlwaysOpen,
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
		if err := sourceCfg.validateMarket(&market); err == nil {
			t.Fatal("validateMarket(invalid symbol) error = nil, want symbol validation")
		}
		market = MarketConfig{
			Symbol:                 "ETHUSD",
			SessionMode:            SessionMode("fallback_only"),
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
		if err := sourceCfg.validateMarket(&market); err == nil {
			t.Fatal("validateMarket(runtime session state) error = nil, want runtime-state rejection")
		}
		market = MarketConfig{
			Symbol:                 "ETHUSD",
			SessionMode:            SessionModeAlwaysOpen,
			PerpBookSource:         "hyperliquid-eth",
			ImpactNotionalUSD:      "10000",
			PublishInterval:        time.Second,
			StalenessThreshold:     time.Second,
			MaxFallbackAge:         time.Second,
			ClampBPS:               1,
			DivergenceThresholdBPS: 1,
			PublishScale:           2,
		}
		if err := sourceCfg.validateMarket(&market); err == nil {
			t.Fatal("validateMarket(no sources) error = nil, want source validation")
		}
		market = MarketConfig{
			Symbol:                 "ETHUSD",
			SessionMode:            SessionModeAlwaysOpen,
			SessionSource:          "coinbase-primary",
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
		if err := sourceCfg.validateMarket(&market); err == nil {
			t.Fatal("validateMarket(always-open session_source) error = nil, want session_source validation")
		}
		if err := sourceCfg.validateMarketSourceRef("ETHUSD", 0, MarketSourceRef{}); err == nil {
			t.Fatal("validateMarketSourceRef(blank name) error = nil, want validation")
		}

		scheduledCfg, err := LoadBytes([]byte(validScheduledPythConfigYAML()), LoadOptions{})
		if err != nil {
			t.Fatalf("LoadBytes(scheduled) error = %v", err)
		}
		if err := scheduledCfg.validateMarket(&scheduledCfg.Markets[0]); err != nil {
			t.Fatalf("validateMarket(valid scheduled) error = %v", err)
		}
	})

	t.Run("env lookup prefers explicit lookup and falls back to process environment", func(t *testing.T) {
		t.Setenv("CONFIG_HELPER_BRANCH", "from-env")

		if got, ok := envLookup(LoadOptions{})("CONFIG_HELPER_BRANCH"); !ok || got != "from-env" {
			t.Fatalf("envLookup(default) = (%q, %t), want (from-env, true)", got, ok)
		}
		if got, ok := envLookup(LoadOptions{
			LookupEnv: func(string) (string, bool) { return "from-option", true },
		})("IGNORED"); !ok || got != "from-option" {
			t.Fatalf("envLookup(custom) = (%q, %t), want (from-option, true)", got, ok)
		}
	})
}
