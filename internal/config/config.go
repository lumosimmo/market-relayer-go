package config

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/lumosimmo/market-relayer-go/internal/fixedpoint"
	"github.com/lumosimmo/market-relayer-go/internal/status"
	"go.yaml.in/yaml/v3"
)

const (
	SupportedVersion   = 1
	DefaultServiceName = "market-relayer-go"
)

const (
	maxTimeoutDuration         = time.Minute
	maxPublishInterval         = time.Minute
	maxFreshnessDuration       = 24 * time.Hour
	defaultShutdownGrace       = 5 * time.Second
	maxBasisPoints             = 10_000
	maxPublishScale            = 18
	defaultStoreMaxOpenConns   = 4
	defaultStoreConnLifetime   = 30 * time.Minute
	defaultStoreConnectTimeout = 5 * time.Second
	defaultStoreLockTimeout    = 5 * time.Second
	defaultStoreLeaseTTL       = 15 * time.Second
	defaultStoreLeaseRenew     = 5 * time.Second
)

var marketSymbolPattern = regexp.MustCompile(`^[A-Z0-9_]+$`)

type LoadOptions struct {
	LookupEnv func(string) (string, bool)
}

type Config struct {
	Version int            `yaml:"version" json:"version"`
	Service ServiceConfig  `yaml:"service" json:"service"`
	Store   StoreConfig    `yaml:"store" json:"store"`
	API     APIConfig      `yaml:"api" json:"api"`
	Sink    SinkConfig     `yaml:"sink" json:"sink"`
	Sources []SourceConfig `yaml:"sources" json:"sources"`
	Markets []MarketConfig `yaml:"markets" json:"markets"`

	sourceIndex map[string]int `yaml:"-" json:"-"`
}

type ServiceConfig struct {
	Name       string `yaml:"name" json:"name"`
	InstanceID string `yaml:"instance_id" json:"instance_id"`
}

type StoreConfig struct {
	DSN                string        `yaml:"dsn" json:"dsn"`
	MaxOpenConns       int32         `yaml:"max_open_conns" json:"max_open_conns"`
	MinOpenConns       int32         `yaml:"min_open_conns,omitempty" json:"min_open_conns,omitempty"`
	MaxIdleConns       int32         `yaml:"max_idle_conns,omitempty" json:"-"`
	ConnMaxLifetime    time.Duration `yaml:"conn_max_lifetime" json:"conn_max_lifetime"`
	ConnectTimeout     time.Duration `yaml:"connect_timeout" json:"connect_timeout"`
	LockTimeout        time.Duration `yaml:"lock_timeout" json:"lock_timeout"`
	LeaseTTL           time.Duration `yaml:"lease_ttl" json:"lease_ttl"`
	LeaseRenewInterval time.Duration `yaml:"lease_renew_interval" json:"lease_renew_interval"`
}

type APIConfig struct {
	ListenAddr      string        `yaml:"listen_addr" json:"listen_addr"`
	ShutdownTimeout time.Duration `yaml:"shutdown_timeout" json:"shutdown_timeout"`
}

type SinkConfig struct {
	Kind           string        `yaml:"kind" json:"kind"`
	URL            string        `yaml:"url,omitempty" json:"url,omitempty"`
	Path           string        `yaml:"path,omitempty" json:"path,omitempty"`
	RequestTimeout time.Duration `yaml:"request_timeout" json:"request_timeout"`
	BearerTokenEnv string        `yaml:"bearer_token_env,omitempty" json:"bearer_token_env,omitempty"`
	BearerToken    string        `yaml:"-" json:"-"`
}

type TimestampMode string

const (
	TimestampModeSourceTimestamp TimestampMode = "source_timestamp"
	TimestampModeReceivedAt      TimestampMode = "received_at"
)

func (mode TimestampMode) Valid() bool {
	switch mode {
	case TimestampModeSourceTimestamp, TimestampModeReceivedAt:
		return true
	default:
		return false
	}
}

type SourceConfig struct {
	Name             string        `yaml:"name" json:"name"`
	Kind             string        `yaml:"kind" json:"kind"`
	Market           string        `yaml:"market,omitempty" json:"market,omitempty"`
	ProductID        string        `yaml:"product_id,omitempty" json:"product_id,omitempty"`
	Pair             string        `yaml:"pair,omitempty" json:"pair,omitempty"`
	AssetType        string        `yaml:"asset_type,omitempty" json:"asset_type,omitempty"`
	RootSymbol       string        `yaml:"root_symbol,omitempty" json:"root_symbol,omitempty"`
	QuoteCurrency    string        `yaml:"quote_currency,omitempty" json:"quote_currency,omitempty"`
	CurveMode        string        `yaml:"curve_mode,omitempty" json:"curve_mode,omitempty"`
	RollWindowDays   int           `yaml:"roll_window_days,omitempty" json:"roll_window_days,omitempty"`
	MaxConfidenceBPS int64         `yaml:"max_confidence_bps,omitempty" json:"max_confidence_bps,omitempty"`
	RequestTimeout   time.Duration `yaml:"request_timeout" json:"request_timeout"`
	TimestampMode    TimestampMode `yaml:"timestamp_mode" json:"timestamp_mode"`
	MaxUnchangedAge  time.Duration `yaml:"max_unchanged_age,omitempty" json:"max_unchanged_age,omitempty"`
}

type SessionMode string

const (
	SessionModeAlwaysOpen SessionMode = "always_open"
	SessionModeScheduled  SessionMode = "scheduled"
)

func (mode SessionMode) Valid() bool {
	switch mode {
	case SessionModeAlwaysOpen, SessionModeScheduled:
		return true
	default:
		return false
	}
}

type MarketSourceRef struct {
	Name                      string `yaml:"name" json:"name"`
	AllowTimestamplessPrimary bool   `yaml:"allow_timestampless_primary,omitempty" json:"allow_timestampless_primary,omitempty"`
}

type MarketConfig struct {
	Symbol                 string            `yaml:"symbol" json:"symbol"`
	SessionMode            SessionMode       `yaml:"session_mode" json:"session_mode"`
	DeprecatedSessionMode  *SessionMode      `yaml:"session,omitempty" json:"-"`
	SessionSource          string            `yaml:"session_source,omitempty" json:"session_source,omitempty"`
	PerpBookSource         string            `yaml:"perp_book_source" json:"perp_book_source"`
	ImpactNotionalUSD      string            `yaml:"impact_notional_usd" json:"impact_notional_usd"`
	PublishInterval        time.Duration     `yaml:"publish_interval" json:"publish_interval"`
	StalenessThreshold     time.Duration     `yaml:"staleness_threshold" json:"staleness_threshold"`
	MaxFallbackAge         time.Duration     `yaml:"max_fallback_age" json:"max_fallback_age"`
	ClampBPS               int64             `yaml:"clamp_bps" json:"clamp_bps"`
	DivergenceThresholdBPS int64             `yaml:"divergence_threshold_bps" json:"divergence_threshold_bps"`
	PublishScale           int32             `yaml:"publish_scale" json:"publish_scale"`
	Sources                []MarketSourceRef `yaml:"sources" json:"sources"`

	impactNotional fixedpoint.Value `yaml:"-" json:"-"`
}

func DefaultStoreConfig(dsn string) StoreConfig {
	return StoreConfig{
		DSN:                strings.TrimSpace(dsn),
		MaxOpenConns:       defaultStoreMaxOpenConns,
		MinOpenConns:       0,
		ConnMaxLifetime:    defaultStoreConnLifetime,
		ConnectTimeout:     defaultStoreConnectTimeout,
		LockTimeout:        defaultStoreLockTimeout,
		LeaseTTL:           defaultStoreLeaseTTL,
		LeaseRenewInterval: defaultStoreLeaseRenew,
	}
}

func (market MarketConfig) SourceNames() []string {
	names := make([]string, 0, len(market.Sources))
	for _, source := range market.Sources {
		names = append(names, source.Name)
	}
	return names
}

func (market MarketConfig) ImpactNotional() fixedpoint.Value {
	return market.impactNotional
}

func LoadFile(path string, options LoadOptions) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("config: read %s: %w", path, err)
	}

	return LoadBytes(data, options)
}

func LoadBytes(data []byte, options LoadOptions) (*Config, error) {
	var cfg Config
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	if err := decoder.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("config: decode: %w", err)
	}

	cfg.applyDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	if err := cfg.resolveSecrets(options); err != nil {
		return nil, err
	}

	return &cfg, nil
}

func (cfg *Config) Digest() (string, error) {
	payload, err := json.Marshal(cfg)
	if err != nil {
		return "", fmt.Errorf("config: marshal digest payload: %w", err)
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:]), nil
}

func (cfg *Config) SourceNames() []string {
	names := make([]string, 0, len(cfg.Sources))
	for _, source := range cfg.Sources {
		names = append(names, source.Name)
	}
	return names
}

func (cfg *Config) SourceByName(name string) (SourceConfig, bool) {
	if cfg.sourceIndex == nil {
		return SourceConfig{}, false
	}

	index, ok := cfg.sourceIndex[name]
	if !ok {
		return SourceConfig{}, false
	}

	return cfg.Sources[index], true
}

func (cfg *Config) OrderedSourcesForMarket(symbol string) ([]SourceConfig, error) {
	for _, market := range cfg.Markets {
		if market.Symbol != symbol {
			continue
		}

		ordered := make([]SourceConfig, 0, len(market.Sources))
		for _, ref := range market.Sources {
			source, ok := cfg.SourceByName(ref.Name)
			if !ok {
				return nil, fmt.Errorf("config: unknown source reference %q for market %s", ref.Name, symbol)
			}
			ordered = append(ordered, source)
		}
		return ordered, nil
	}

	return nil, fmt.Errorf("config: unknown market %q", symbol)
}

func (cfg *Config) PerpBookSourceForMarket(symbol string) (SourceConfig, error) {
	for _, market := range cfg.Markets {
		if market.Symbol != symbol {
			continue
		}
		return cfg.marketSource(symbol, market.PerpBookSource)
	}

	return SourceConfig{}, fmt.Errorf("config: unknown market %q", symbol)
}

func (cfg *Config) applyDefaults() {
	if strings.TrimSpace(cfg.Service.Name) == "" {
		cfg.Service.Name = DefaultServiceName
	}
	storeDefaults := DefaultStoreConfig(cfg.Store.DSN)

	if cfg.API.ShutdownTimeout == 0 {
		cfg.API.ShutdownTimeout = defaultShutdownGrace
	}
	if cfg.Store.MaxOpenConns == 0 {
		cfg.Store.MaxOpenConns = storeDefaults.MaxOpenConns
	}
	if cfg.Store.ConnMaxLifetime == 0 {
		cfg.Store.ConnMaxLifetime = storeDefaults.ConnMaxLifetime
	}
	if cfg.Store.ConnectTimeout == 0 {
		cfg.Store.ConnectTimeout = storeDefaults.ConnectTimeout
	}
	if cfg.Store.LockTimeout == 0 {
		cfg.Store.LockTimeout = storeDefaults.LockTimeout
	}
	if cfg.Store.LeaseTTL == 0 {
		cfg.Store.LeaseTTL = storeDefaults.LeaseTTL
	}
	if cfg.Store.LeaseRenewInterval == 0 {
		cfg.Store.LeaseRenewInterval = storeDefaults.LeaseRenewInterval
	}
}

func (cfg *Config) validate() error {
	if err := cfg.validateTopLevel(); err != nil {
		return err
	}
	if err := cfg.buildSourceIndex(); err != nil {
		return err
	}
	return cfg.validateMarkets()
}

func (cfg *Config) validateTopLevel() error {
	if cfg.Version != SupportedVersion {
		return fmt.Errorf("config: unsupported config version %d", cfg.Version)
	}
	if strings.TrimSpace(cfg.Service.InstanceID) == "" {
		return errors.New("config: service.instance_id is required")
	}
	if err := validateStore(cfg.Store); err != nil {
		return err
	}
	if err := validateListenAddr(cfg.API.ListenAddr); err != nil {
		return err
	}
	if err := validateDuration("api.shutdown_timeout", cfg.API.ShutdownTimeout, 0, maxFreshnessDuration); err != nil {
		return err
	}
	if err := validateSink(cfg.Sink); err != nil {
		return err
	}
	if len(cfg.Sources) == 0 {
		return errors.New("config: at least one source is required")
	}
	if len(cfg.Markets) == 0 {
		return errors.New("config: at least one market is required")
	}
	return nil
}

func (cfg *Config) buildSourceIndex() error {
	cfg.sourceIndex = make(map[string]int, len(cfg.Sources))
	for index, source := range cfg.Sources {
		if err := validateSource(source); err != nil {
			return err
		}
		if _, exists := cfg.sourceIndex[source.Name]; exists {
			return fmt.Errorf("config: duplicate source name %q", source.Name)
		}
		cfg.sourceIndex[source.Name] = index
	}
	return nil
}

func (cfg *Config) validateMarkets() error {
	seenMarkets := make(map[string]struct{}, len(cfg.Markets))
	for index := range cfg.Markets {
		market := &cfg.Markets[index]
		if err := cfg.validateMarket(market); err != nil {
			return err
		}
		if _, exists := seenMarkets[market.Symbol]; exists {
			return fmt.Errorf("config: duplicate market symbol %q", market.Symbol)
		}
		seenMarkets[market.Symbol] = struct{}{}
	}
	return nil
}

func (cfg *Config) validateMarket(market *MarketConfig) error {
	if !marketSymbolPattern.MatchString(market.Symbol) {
		return fmt.Errorf("config: market symbol %q must match %s", market.Symbol, marketSymbolPattern.String())
	}
	if market.DeprecatedSessionMode != nil {
		return fmt.Errorf("config: deprecated alias %q for market %s; use session_mode", "session", market.Symbol)
	}
	if state := status.SessionState(market.SessionMode); state.Valid() {
		return fmt.Errorf("config: session_mode %q is a runtime state; use the minimal supported config mode always_open", market.SessionMode)
	}
	if !market.SessionMode.Valid() {
		return fmt.Errorf("config: session_mode %q is invalid", market.SessionMode)
	}
	if err := validateDuration("markets.publish_interval", market.PublishInterval, 0, maxPublishInterval); err != nil {
		return err
	}
	if err := validateDuration("markets.staleness_threshold", market.StalenessThreshold, 0, maxFreshnessDuration); err != nil {
		return err
	}
	if err := validateDuration("markets.max_fallback_age", market.MaxFallbackAge, 0, maxFreshnessDuration); err != nil {
		return err
	}
	if err := validateScale("publish_scale", market.PublishScale, maxPublishScale); err != nil {
		return err
	}
	if err := validateBasisPoints("clamp_bps", market.ClampBPS); err != nil {
		return err
	}
	if err := validateBasisPoints("divergence_threshold_bps", market.DivergenceThresholdBPS); err != nil {
		return err
	}
	if len(market.Sources) == 0 {
		return fmt.Errorf("config: market %s must reference at least one source", market.Symbol)
	}
	if strings.TrimSpace(market.PerpBookSource) == "" {
		return fmt.Errorf("config: market %s requires perp_book_source", market.Symbol)
	}
	if strings.TrimSpace(market.ImpactNotionalUSD) == "" {
		return fmt.Errorf("config: market %s requires impact_notional_usd", market.Symbol)
	}

	for index, ref := range market.Sources {
		if err := cfg.validateMarketSourceRef(market.Symbol, index, ref); err != nil {
			return err
		}
	}
	if err := cfg.validatePerpBookSource(*market); err != nil {
		return err
	}
	if err := cfg.validateImpactNotional(market); err != nil {
		return err
	}

	if err := cfg.validateMarketSessionSource(*market); err != nil {
		return err
	}

	return nil
}

func validateListenAddr(addr string) error {
	_, port, err := net.SplitHostPort(strings.TrimSpace(addr))
	if err != nil || port == "" {
		return fmt.Errorf("config: api.listen_addr must be a valid TCP listen address")
	}
	return nil
}

func validateSink(sink SinkConfig) error {
	switch sink.Kind {
	case "http":
		if strings.TrimSpace(sink.URL) == "" {
			return errors.New("config: sink.url is required for http sink")
		}
		parsed, err := url.Parse(sink.URL)
		if err != nil || parsed.Scheme == "" || parsed.Host == "" {
			return fmt.Errorf("config: sink.url must be a valid absolute URL")
		}
	case "file":
		if strings.TrimSpace(sink.Path) == "" {
			return errors.New("config: sink.path is required for file sink")
		}
	default:
		return fmt.Errorf("config: unsupported sink.kind %q", sink.Kind)
	}

	return validateDuration("sink.request_timeout", sink.RequestTimeout, 0, maxTimeoutDuration)
}

func validateStore(store StoreConfig) error {
	if strings.TrimSpace(store.DSN) == "" {
		return errors.New("config: store.dsn is required")
	}
	if store.MaxOpenConns <= 0 {
		return errors.New("config: store.max_open_conns must be greater than 0")
	}
	if store.MaxIdleConns > 0 {
		return errors.New("config: store.max_idle_conns is deprecated; use store.min_open_conns")
	}
	if store.MinOpenConns < 0 {
		return errors.New("config: store.min_open_conns must be greater than or equal to 0")
	}
	if store.MinOpenConns > store.MaxOpenConns {
		return errors.New("config: store.min_open_conns must be less than or equal to store.max_open_conns")
	}
	if err := validateDuration("store.conn_max_lifetime", store.ConnMaxLifetime, 0, maxFreshnessDuration); err != nil {
		return err
	}
	if err := validateDuration("store.connect_timeout", store.ConnectTimeout, 0, maxTimeoutDuration); err != nil {
		return err
	}
	if err := validateDuration("store.lock_timeout", store.LockTimeout, 0, maxFreshnessDuration); err != nil {
		return err
	}
	if err := validateDuration("store.lease_ttl", store.LeaseTTL, 0, maxFreshnessDuration); err != nil {
		return err
	}
	if err := validateDuration("store.lease_renew_interval", store.LeaseRenewInterval, 0, maxFreshnessDuration); err != nil {
		return err
	}
	if store.LeaseRenewInterval >= store.LeaseTTL {
		return errors.New("config: store.lease_renew_interval must be less than store.lease_ttl")
	}
	return nil
}

func validateSource(source SourceConfig) error {
	if strings.TrimSpace(source.Name) == "" {
		return errors.New("config: source.name is required")
	}
	if err := validateSourceKind(source); err != nil {
		return err
	}
	if !source.TimestampMode.Valid() {
		return fmt.Errorf("config: source %s has invalid timestamp_mode %q", source.Name, source.TimestampMode)
	}
	if err := validateDuration("sources.request_timeout", source.RequestTimeout, 0, maxTimeoutDuration); err != nil {
		return err
	}
	return validateMaxUnchangedAge(source)
}

func validateDuration(name string, value time.Duration, minimum time.Duration, maximum time.Duration) error {
	if value <= minimum {
		return fmt.Errorf("config: %s must be greater than %s", name, minimum)
	}
	if value > maximum {
		return fmt.Errorf("config: %s must be less than or equal to %s", name, maximum)
	}
	return nil
}

func validateScale(name string, value int32, maximum int32) error {
	if value < 0 || value > maximum {
		return fmt.Errorf("config: %s must be between 0 and %d", name, maximum)
	}
	return nil
}

func validateBasisPoints(name string, value int64) error {
	if value < 0 || value > maxBasisPoints {
		return fmt.Errorf("config: %s must be between 0 and %d", name, maxBasisPoints)
	}
	return nil
}

func validateMaxUnchangedAge(source SourceConfig) error {
	if source.MaxUnchangedAge < 0 {
		return errors.New("config: sources.max_unchanged_age must be non-negative")
	}
	if source.TimestampMode == TimestampModeReceivedAt && source.MaxUnchangedAge > maxFreshnessDuration {
		return fmt.Errorf("config: sources.max_unchanged_age must be less than or equal to %s", maxFreshnessDuration)
	}
	return nil
}

func (cfg *Config) resolveSecrets(options LoadOptions) error {
	if cfg.Sink.BearerTokenEnv == "" {
		return nil
	}

	value, ok := envLookup(options)(cfg.Sink.BearerTokenEnv)
	if !ok || strings.TrimSpace(value) == "" {
		return fmt.Errorf("config: env %s is required for sink bearer token", cfg.Sink.BearerTokenEnv)
	}
	cfg.Sink.BearerToken = value
	return nil
}

func (cfg *Config) validateMarketSourceRef(symbol string, index int, ref MarketSourceRef) error {
	if strings.TrimSpace(ref.Name) == "" {
		return fmt.Errorf("config: market %s has a source entry with no name", symbol)
	}

	source, err := cfg.marketSource(symbol, ref.Name)
	if err != nil {
		return err
	}
	if source.TimestampMode == TimestampModeReceivedAt && source.MaxUnchangedAge <= 0 {
		return fmt.Errorf("config: timestamp-less primary source %q for market %s requires max_unchanged_age", ref.Name, symbol)
	}
	if index == 0 && source.TimestampMode == TimestampModeReceivedAt && !ref.AllowTimestamplessPrimary {
		return fmt.Errorf("config: market %s source %q requires allow_timestampless_primary when used as primary", symbol, ref.Name)
	}
	return nil
}

func (cfg *Config) validateMarketSessionSource(market MarketConfig) error {
	sessionSource := strings.TrimSpace(market.SessionSource)
	if market.SessionMode != SessionModeScheduled {
		if sessionSource != "" {
			return fmt.Errorf("config: market %s session_source is only valid when session_mode is %q", market.Symbol, SessionModeScheduled)
		}
		return nil
	}

	if sessionSource == "" {
		return fmt.Errorf("config: market %s requires session_source when session_mode is %q", market.Symbol, market.SessionMode)
	}

	source, err := cfg.marketSource(market.Symbol, sessionSource)
	if err != nil {
		return err
	}

	if !marketHasSourceRef(market, sessionSource) {
		return fmt.Errorf("config: market %s session_source %q must also appear in market.sources", market.Symbol, sessionSource)
	}
	if !sourceKindSessionCapable(source.Kind) {
		return fmt.Errorf("config: market %s session_source %q kind %q does not support scheduled sessions", market.Symbol, sessionSource, source.Kind)
	}

	return nil
}

func (cfg *Config) validatePerpBookSource(market MarketConfig) error {
	source, err := cfg.marketSource(market.Symbol, market.PerpBookSource)
	if err != nil {
		return err
	}
	if source.Kind != SourceKindHyperliquid {
		return fmt.Errorf("config: market %s perp_book_source %q kind %q must be %q", market.Symbol, source.Name, source.Kind, SourceKindHyperliquid)
	}
	return nil
}

func (cfg *Config) validateImpactNotional(market *MarketConfig) error {
	value, err := fixedpoint.ParseDecimal(market.ImpactNotionalUSD)
	if err != nil {
		return fmt.Errorf("config: market %s impact_notional_usd is invalid: %w", market.Symbol, err)
	}
	if value.Int <= 0 {
		return fmt.Errorf("config: market %s impact_notional_usd must be positive", market.Symbol)
	}
	market.impactNotional = value
	return nil
}

func (cfg *Config) marketSource(symbol string, name string) (SourceConfig, error) {
	source, ok := cfg.SourceByName(name)
	if !ok {
		return SourceConfig{}, fmt.Errorf("config: unknown source reference %q for market %s", name, symbol)
	}
	return source, nil
}

func envLookup(options LoadOptions) func(string) (string, bool) {
	if options.LookupEnv != nil {
		return options.LookupEnv
	}
	return os.LookupEnv
}

func marketHasSourceRef(market MarketConfig, name string) bool {
	for _, ref := range market.Sources {
		if ref.Name == name {
			return true
		}
	}
	return false
}
