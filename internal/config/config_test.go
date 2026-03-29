package config

import (
	"strings"
	"testing"

	"github.com/lumosimmo/market-relayer-go/internal/fixedpoint"
)

func TestLoadBytesValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   string
		wantErr string
	}{
		{
			name:    "unknown field",
			input:   strings.Replace(validConfigYAML(), "version: 1\n", "version: 1\nunknown_field: true\n", 1),
			wantErr: "field unknown_field not found",
		},
		{
			name:    "deprecated session alias",
			input:   strings.Replace(validConfigYAML(), "session_mode: always_open", "session: always_open", 1),
			wantErr: "deprecated alias",
		},
		{
			name:    "scheduled session mode requires session source",
			input:   strings.Replace(validScheduledPythConfigYAML(), "    session_source: pyth-wti\n", "", 1),
			wantErr: "requires session_source",
		},
		{
			name:    "runtime state cannot be used as session mode",
			input:   strings.Replace(validConfigYAML(), "session_mode: always_open", "session_mode: fallback_only", 1),
			wantErr: "runtime state",
		},
		{
			name: "duplicate market symbol",
			input: validConfigYAML() + `
  - symbol: ETHUSD
    session_mode: always_open
    perp_book_source: hyperliquid-eth
    impact_notional_usd: "10000"
    publish_interval: 3s
    staleness_threshold: 10s
    max_fallback_age: 30s
    clamp_bps: 50
    divergence_threshold_bps: 250
    publish_scale: 2
    sources:
      - name: coinbase-primary
`,
			wantErr: "duplicate market symbol",
		},
		{
			name:    "kraken requires received at mode",
			input:   strings.Replace(validConfigYAML(), "timestamp_mode: received_at", "timestamp_mode: source_timestamp", 1),
			wantErr: "must use timestamp_mode",
		},
		{
			name:    "unknown source reference",
			input:   strings.Replace(validConfigYAML(), "- name: kraken-secondary", "- name: missing-source", 1),
			wantErr: "unknown source reference",
		},
		{
			name:    "invalid publish scale",
			input:   strings.Replace(validConfigYAML(), "publish_scale: 2", "publish_scale: 19", 1),
			wantErr: "publish_scale must be between 0 and 18",
		},
		{
			name:    "invalid market symbol",
			input:   strings.Replace(validConfigYAML(), "symbol: ETHUSD", "symbol: eth-usd", 1),
			wantErr: "market symbol",
		},
		{
			name:    "invalid clamp bps",
			input:   strings.Replace(validConfigYAML(), "clamp_bps: 50", "clamp_bps: 10001", 1),
			wantErr: "clamp_bps must be between 0 and 10000",
		},
		{
			name:    "invalid divergence threshold",
			input:   strings.Replace(validConfigYAML(), "divergence_threshold_bps: 250", "divergence_threshold_bps: -1", 1),
			wantErr: "divergence_threshold_bps must be between 0 and 10000",
		},
		{
			name:    "invalid listen address",
			input:   strings.Replace(validConfigYAML(), "listen_addr: 127.0.0.1:0", "listen_addr: bad-listen-addr", 1),
			wantErr: "api.listen_addr must be a valid TCP listen address",
		},
		{
			name:    "missing store dsn",
			input:   strings.Replace(validConfigYAML(), "dsn: postgres://relayer:relayer@127.0.0.1:5432/market_relayer?sslmode=disable", "dsn: \"\"", 1),
			wantErr: "store.dsn is required",
		},
		{
			name: "conflicting store min open connection settings",
			input: strings.Replace(
				validConfigYAML(),
				"store:\n  dsn: postgres://relayer:relayer@127.0.0.1:5432/market_relayer?sslmode=disable",
				"store:\n  dsn: postgres://relayer:relayer@127.0.0.1:5432/market_relayer?sslmode=disable\n  min_open_conns: 1\n  max_idle_conns: 2",
				1,
			),
			wantErr: "store.max_idle_conns is deprecated; use store.min_open_conns",
		},
		{
			name:    "missing sink url",
			input:   strings.Replace(validConfigYAML(), "url: http://127.0.0.1:18080/publish", "url: \"\"", 1),
			wantErr: "sink.url is required",
		},
		{
			name:    "invalid sink url with bare ipv6 host",
			input:   strings.Replace(validConfigYAML(), "url: http://127.0.0.1:18080/publish", "url: http://::1/publish", 1),
			wantErr: "sink.url must be a valid absolute URL",
		},
		{
			name:    "invalid publish interval",
			input:   strings.Replace(validConfigYAML(), "publish_interval: 3s", "publish_interval: 0s", 1),
			wantErr: "markets.publish_interval must be greater than 0s",
		},
		{
			name:    "missing perp book source",
			input:   strings.Replace(validConfigYAML(), "    perp_book_source: hyperliquid-eth\n", "", 1),
			wantErr: "requires perp_book_source",
		},
		{
			name:    "missing impact notional",
			input:   strings.Replace(validConfigYAML(), "    impact_notional_usd: \"10000\"\n", "", 1),
			wantErr: "requires impact_notional_usd",
		},
		{
			name: "missing required source config",
			input: `version: 1
service:
  name: market-relayer-go
  instance_id: local-test
store:
  dsn: postgres://relayer:relayer@127.0.0.1:5432/market_relayer?sslmode=disable
api:
  listen_addr: 127.0.0.1:0
sink:
  kind: http
  url: http://127.0.0.1:18080/publish
  request_timeout: 500ms
sources:
  - name: coinbase-primary
    kind: coinbase
    request_timeout: 1s
    timestamp_mode: source_timestamp
markets:
  - symbol: ETHUSD
    session_mode: always_open
    publish_interval: 3s
    staleness_threshold: 10s
    max_fallback_age: 30s
    clamp_bps: 50
    divergence_threshold_bps: 250
    publish_scale: 2
    sources:
      - name: coinbase-primary
`,
			wantErr: "coinbase source requires product_id",
		},
		{
			name:    "pyth requires source timestamp mode",
			input:   strings.Replace(validScheduledPythConfigYAML(), "    timestamp_mode: source_timestamp", "    timestamp_mode: received_at", 1),
			wantErr: "must use timestamp_mode",
		},
		{
			name:    "hyperliquid requires market",
			input:   strings.Replace(validConfigYAML(), "    market: ETH\n", "", 1),
			wantErr: "hyperliquid source requires market",
		},
		{
			name:    "pyth requires supported curve mode",
			input:   strings.Replace(validScheduledPythConfigYAML(), "    curve_mode: nearby_linear_roll", "    curve_mode: front_month", 1),
			wantErr: "must use curve_mode",
		},
		{
			name: "scheduled session source must be market source",
			input: strings.Replace(
				strings.Replace(
					validScheduledPythConfigYAML(),
					"markets:\n",
					`  - name: pyth-wti-2
    kind: pyth
    asset_type: Commodities
    root_symbol: WTI
    quote_currency: USD
    curve_mode: nearby_linear_roll
    roll_window_days: 5
    max_confidence_bps: 250
    request_timeout: 1s
    timestamp_mode: source_timestamp
markets:
`,
					1,
				),
				"    session_source: pyth-wti",
				"    session_source: pyth-wti-2",
				1,
			),
			wantErr: "must also appear in market.sources",
		},
		{
			name:    "session source only valid on scheduled markets",
			input:   strings.Replace(validConfigYAML(), "    session_mode: always_open", "    session_mode: always_open\n    session_source: coinbase-primary", 1),
			wantErr: "session_source is only valid",
		},
		{
			name: "timestampless primary missing guardrails",
			input: strings.Replace(
				strings.Replace(validConfigYAML(), "timestamp_mode: source_timestamp", "timestamp_mode: received_at", 1),
				"      - name: coinbase-primary",
				"      - name: coinbase-primary",
				1,
			),
			wantErr: "timestamp-less primary source",
		},
		{
			name: "timestampless primary missing opt-in",
			input: strings.Replace(
				strings.Replace(validConfigYAML(), "timestamp_mode: source_timestamp", "timestamp_mode: received_at\n    max_unchanged_age: 10s", 1),
				"      - name: coinbase-primary",
				"      - name: coinbase-primary",
				1,
			),
			wantErr: "allow_timestampless_primary",
		},
		{
			name:    "unsupported version",
			input:   strings.Replace(validConfigYAML(), "version: 1", "version: 2", 1),
			wantErr: "unsupported config version",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := LoadBytes([]byte(tt.input), LoadOptions{})
			if err == nil {
				t.Fatalf("LoadBytes() error = nil, want %q", tt.wantErr)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("LoadBytes() error = %q, want substring %q", err.Error(), tt.wantErr)
			}
		})
	}
}

func TestLoadBytesSuccess(t *testing.T) {
	t.Parallel()

	cfg, err := LoadBytes([]byte(validConfigYAML()), LoadOptions{})
	if err != nil {
		t.Fatalf("LoadBytes() error = %v", err)
	}

	if got, want := cfg.Service.Name, "market-relayer-go"; got != want {
		t.Fatalf("cfg.Service.Name = %q, want %q", got, want)
	}

	if got, want := cfg.API.ShutdownTimeout.String(), "5s"; got != want {
		t.Fatalf("cfg.API.ShutdownTimeout = %s, want %s", got, want)
	}
	if got, want := cfg.Store.LeaseTTL.String(), "15s"; got != want {
		t.Fatalf("cfg.Store.LeaseTTL = %s, want %s", got, want)
	}
	if got, want := cfg.Store.LeaseRenewInterval.String(), "5s"; got != want {
		t.Fatalf("cfg.Store.LeaseRenewInterval = %s, want %s", got, want)
	}
	if got, want := cfg.Store.MaxOpenConns, int32(4); got != want {
		t.Fatalf("cfg.Store.MaxOpenConns = %d, want %d", got, want)
	}
	if got, want := cfg.Store.MinOpenConns, int32(0); got != want {
		t.Fatalf("cfg.Store.MinOpenConns = %d, want %d", got, want)
	}

	names := cfg.SourceNames()
	if len(names) != 3 || names[0] != "coinbase-primary" || names[1] != "kraken-secondary" || names[2] != "hyperliquid-eth" {
		t.Fatalf("cfg.SourceNames() = %v, want [coinbase-primary kraken-secondary hyperliquid-eth]", names)
	}

	orderedSources, err := cfg.OrderedSourcesForMarket("ETHUSD")
	if err != nil {
		t.Fatalf("cfg.OrderedSourcesForMarket() error = %v", err)
	}
	if len(orderedSources) != 2 || orderedSources[0].Name != "coinbase-primary" || orderedSources[1].Name != "kraken-secondary" {
		t.Fatalf("cfg.OrderedSourcesForMarket() = %+v, want ordered sources", orderedSources)
	}

	refs := cfg.Markets[0].SourceNames()
	if len(refs) != 2 || refs[0] != "coinbase-primary" || refs[1] != "kraken-secondary" {
		t.Fatalf("cfg.Markets[0].SourceNames() = %v, want [coinbase-primary kraken-secondary]", refs)
	}
	if got, want := cfg.Markets[0].PerpBookSource, "hyperliquid-eth"; got != want {
		t.Fatalf("cfg.Markets[0].PerpBookSource = %q, want %q", got, want)
	}
	if got, want := cfg.Markets[0].ImpactNotional(), (fixedpoint.Value{Int: 10000, Scale: 0}); got != want {
		t.Fatalf("cfg.Markets[0].ImpactNotional() = %+v, want %+v", got, want)
	}
	perpBook, err := cfg.PerpBookSourceForMarket("ETHUSD")
	if err != nil {
		t.Fatalf("cfg.PerpBookSourceForMarket() error = %v", err)
	}
	if got, want := perpBook.Name, "hyperliquid-eth"; got != want {
		t.Fatalf("cfg.PerpBookSourceForMarket() = %q, want %q", got, want)
	}

	scheduled, err := LoadBytes([]byte(validScheduledPythConfigYAML()), LoadOptions{})
	if err != nil {
		t.Fatalf("LoadBytes(scheduled) error = %v", err)
	}
	if got, want := scheduled.Markets[0].SessionSource, "pyth-wti"; got != want {
		t.Fatalf("scheduled.Markets[0].SessionSource = %q, want %q", got, want)
	}
	if got, want := scheduled.Markets[0].PerpBookSource, "hyperliquid-wti"; got != want {
		t.Fatalf("scheduled.Markets[0].PerpBookSource = %q, want %q", got, want)
	}
}

func TestLoadBytesStoreMinOpenConns(t *testing.T) {
	t.Parallel()

	t.Run("reads min_open_conns directly", func(t *testing.T) {
		t.Parallel()

		input := strings.Replace(
			validConfigYAML(),
			"store:\n  dsn: postgres://relayer:relayer@127.0.0.1:5432/market_relayer?sslmode=disable",
			"store:\n  dsn: postgres://relayer:relayer@127.0.0.1:5432/market_relayer?sslmode=disable\n  min_open_conns: 2",
			1,
		)

		cfg, err := LoadBytes([]byte(input), LoadOptions{})
		if err != nil {
			t.Fatalf("LoadBytes() error = %v", err)
		}

		if got, want := cfg.Store.MinOpenConns, int32(2); got != want {
			t.Fatalf("cfg.Store.MinOpenConns = %d, want %d", got, want)
		}
	})

	t.Run("rejects deprecated max_idle_conns alias", func(t *testing.T) {
		t.Parallel()

		input := strings.Replace(
			validConfigYAML(),
			"store:\n  dsn: postgres://relayer:relayer@127.0.0.1:5432/market_relayer?sslmode=disable",
			"store:\n  dsn: postgres://relayer:relayer@127.0.0.1:5432/market_relayer?sslmode=disable\n  max_idle_conns: 2",
			1,
		)

		if _, err := LoadBytes([]byte(input), LoadOptions{}); err == nil || !strings.Contains(err.Error(), "store.max_idle_conns is deprecated") {
			t.Fatalf("LoadBytes() error = %v, want deprecated max_idle_conns failure", err)
		}
	})
}

func TestLoadBytesAcceptsWildcardListenAddr(t *testing.T) {
	t.Parallel()

	input := strings.Replace(validConfigYAML(), "listen_addr: 127.0.0.1:0", "listen_addr: \":0\"", 1)

	cfg, err := LoadBytes([]byte(input), LoadOptions{})
	if err != nil {
		t.Fatalf("LoadBytes() error = %v", err)
	}

	if got, want := cfg.API.ListenAddr, ":0"; got != want {
		t.Fatalf("cfg.API.ListenAddr = %q, want %q", got, want)
	}
}

func TestValidateListenAddr(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		addr    string
		wantErr bool
	}{
		{name: "wildcard host", addr: ":8080"},
		{name: "ephemeral wildcard host", addr: ":0"},
		{name: "ipv4 host", addr: "127.0.0.1:0"},
		{name: "ipv6 host", addr: "[::1]:8080"},
		{name: "missing port separator", addr: "bad-listen-addr", wantErr: true},
		{name: "empty port", addr: "localhost:", wantErr: true},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := validateListenAddr(tt.addr)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("validateListenAddr(%q) error = nil, want error", tt.addr)
				}
				return
			}
			if err != nil {
				t.Fatalf("validateListenAddr(%q) error = %v", tt.addr, err)
			}
		})
	}
}

func TestLoadBytesAllowsBracketedIPv6SinkURL(t *testing.T) {
	t.Parallel()

	input := strings.Replace(validConfigYAML(), "url: http://127.0.0.1:18080/publish", "url: http://[::1]/publish", 1)

	cfg, err := LoadBytes([]byte(input), LoadOptions{})
	if err != nil {
		t.Fatalf("LoadBytes() error = %v", err)
	}

	if got, want := cfg.Sink.URL, "http://[::1]/publish"; got != want {
		t.Fatalf("cfg.Sink.URL = %q, want %q", got, want)
	}
}

func TestLoadBytesResolvesSecretEnv(t *testing.T) {
	t.Parallel()

	input := strings.Replace(validConfigYAML(), "request_timeout: 500ms", "request_timeout: 500ms\n  bearer_token_env: MARKET_RELAYER_SINK_BEARER_TOKEN", 1)

	cfg, err := LoadBytes([]byte(input), LoadOptions{
		LookupEnv: func(key string) (string, bool) {
			if key == "MARKET_RELAYER_SINK_BEARER_TOKEN" {
				return "super-secret", true
			}
			return "", false
		},
	})
	if err != nil {
		t.Fatalf("LoadBytes() error = %v", err)
	}

	if got, want := cfg.Sink.BearerToken, "super-secret"; got != want {
		t.Fatalf("cfg.Sink.BearerToken = %q, want %q", got, want)
	}
}

func TestSessionModeValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		value SessionMode
		valid bool
	}{
		{name: "always open", value: SessionModeAlwaysOpen, valid: true},
		{name: "scheduled", value: SessionModeScheduled, valid: true},
		{name: "invalid", value: SessionMode("bad"), valid: false},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := tt.value.Valid(); got != tt.valid {
				t.Fatalf("SessionMode(%q).Valid() = %v, want %v", tt.value, got, tt.valid)
			}
		})
	}
}

func TestDigestStable(t *testing.T) {
	t.Parallel()

	cfgA, err := LoadBytes([]byte(validConfigYAML()), LoadOptions{})
	if err != nil {
		t.Fatalf("LoadBytes() error = %v", err)
	}
	cfgB, err := LoadBytes([]byte(validConfigYAML()), LoadOptions{})
	if err != nil {
		t.Fatalf("LoadBytes() error = %v", err)
	}

	digestA, err := cfgA.Digest()
	if err != nil {
		t.Fatalf("cfgA.Digest() error = %v", err)
	}
	digestB, err := cfgB.Digest()
	if err != nil {
		t.Fatalf("cfgB.Digest() error = %v", err)
	}

	if digestA != digestB {
		t.Fatalf("digest mismatch: %q != %q", digestA, digestB)
	}

	reordered := strings.Replace(validConfigYAML(),
		"  - name: coinbase-primary\n    kind: coinbase\n    product_id: ETH-USD\n    request_timeout: 1s\n    timestamp_mode: source_timestamp\n  - name: kraken-secondary\n    kind: kraken\n    pair: ETH/USD\n    request_timeout: 1s\n    timestamp_mode: received_at\n    max_unchanged_age: 10s\n",
		"  - name: kraken-secondary\n    kind: kraken\n    pair: ETH/USD\n    request_timeout: 1s\n    timestamp_mode: received_at\n    max_unchanged_age: 10s\n  - name: coinbase-primary\n    kind: coinbase\n    product_id: ETH-USD\n    request_timeout: 1s\n    timestamp_mode: source_timestamp\n",
		1,
	)
	cfgC, err := LoadBytes([]byte(reordered), LoadOptions{})
	if err != nil {
		t.Fatalf("LoadBytes() error = %v", err)
	}
	digestC, err := cfgC.Digest()
	if err != nil {
		t.Fatalf("cfgC.Digest() error = %v", err)
	}

	if digestA == digestC {
		t.Fatalf("digestA == digestC = %q, want different digests when source order changes", digestA)
	}
}

func validConfigYAML() string {
	return `version: 1
service:
  name: market-relayer-go
  instance_id: local-test
store:
  dsn: postgres://relayer:relayer@127.0.0.1:5432/market_relayer?sslmode=disable
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
    impact_notional_usd: "10000"
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
}

func validScheduledPythConfigYAML() string {
	return `version: 1
service:
  name: market-relayer-go
  instance_id: local-test
store:
  dsn: postgres://relayer:relayer@127.0.0.1:5432/market_relayer?sslmode=disable
api:
  listen_addr: 127.0.0.1:0
sink:
  kind: http
  url: http://127.0.0.1:18080/publish
  request_timeout: 500ms
sources:
  - name: pyth-wti
    kind: pyth
    asset_type: Commodities
    root_symbol: WTI
    quote_currency: USD
    curve_mode: nearby_linear_roll
    roll_window_days: 5
    max_confidence_bps: 250
    request_timeout: 1s
    timestamp_mode: source_timestamp
  - name: hyperliquid-wti
    kind: hyperliquid
    market: xyz:CL
    request_timeout: 1s
    timestamp_mode: source_timestamp
markets:
  - symbol: WTIUSD
    session_mode: scheduled
    session_source: pyth-wti
    perp_book_source: hyperliquid-wti
    impact_notional_usd: "10000"
    publish_interval: 3s
    staleness_threshold: 10s
    max_fallback_age: 30s
    clamp_bps: 50
    divergence_threshold_bps: 250
    publish_scale: 2
    sources:
      - name: pyth-wti
`
}
