package testutil

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/lumosimmo/market-relayer-go/internal/config"
	"github.com/lumosimmo/market-relayer-go/internal/feeds"
	"github.com/lumosimmo/market-relayer-go/internal/fixedpoint"
	"github.com/lumosimmo/market-relayer-go/internal/store"
	"go.yaml.in/yaml/v3"
)

type FixtureSet struct {
	Name        string
	Metadata    store.Metadata
	Market      config.MarketConfig
	SourceOrder []string
	Scenarios   []FixtureScenario
}

type FixtureScenario struct {
	Name         string
	RecordID     string
	At           time.Time
	Quotes       []feeds.Quote
	SinkOutcomes []FixtureSinkOutcome
}

type FixtureSinkOutcome struct {
	Kind      string
	Duplicate bool
	AckedAt   time.Time
}

func LoadFixtureSet(path string) (FixtureSet, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return FixtureSet{}, fmt.Errorf("testutil: read fixture set: %w", err)
	}

	var raw fixtureSetFile
	if err := yaml.Unmarshal(data, &raw); err != nil {
		return FixtureSet{}, fmt.Errorf("testutil: decode fixture set: %w", err)
	}

	if strings.TrimSpace(raw.Name) == "" {
		return FixtureSet{}, fmt.Errorf("testutil: fixture set name is required")
	}
	if len(raw.SourceOrder) == 0 {
		return FixtureSet{}, fmt.Errorf("testutil: source_order is required")
	}

	market, err := raw.Market.marketConfig(raw.SourceOrder)
	if err != nil {
		return FixtureSet{}, err
	}
	metadata := raw.Metadata.normalize()

	scenarios := make([]FixtureScenario, 0, len(raw.Scenarios))
	for _, current := range raw.Scenarios {
		scenario, err := current.normalize(market.Symbol)
		if err != nil {
			return FixtureSet{}, err
		}
		scenarios = append(scenarios, scenario)
	}

	return FixtureSet{
		Name:        strings.TrimSpace(raw.Name),
		Metadata:    metadata,
		Market:      market,
		SourceOrder: append([]string(nil), raw.SourceOrder...),
		Scenarios:   scenarios,
	}, nil
}

func (set FixtureSet) Scenario(name string) (FixtureScenario, bool) {
	for _, current := range set.Scenarios {
		if current.Name == name {
			return current, true
		}
	}
	return FixtureScenario{}, false
}

type fixtureSetFile struct {
	Name        string                `yaml:"name"`
	Metadata    fixtureMetadataFile   `yaml:"metadata"`
	Market      fixtureMarketFile     `yaml:"market"`
	SourceOrder []string              `yaml:"source_order"`
	Scenarios   []fixtureScenarioFile `yaml:"scenarios"`
}

type fixtureMetadataFile struct {
	ConfigDigest            string `yaml:"config_digest"`
	PricingAlgorithmVersion string `yaml:"pricing_algorithm_version"`
	SchemaVersion           string `yaml:"schema_version"`
	EnvelopeVersion         string `yaml:"envelope_version"`
}

func (raw fixtureMetadataFile) normalize() store.Metadata {
	return store.Metadata{
		ConfigDigest:            strings.TrimSpace(raw.ConfigDigest),
		PricingAlgorithmVersion: strings.TrimSpace(raw.PricingAlgorithmVersion),
		SchemaVersion:           strings.TrimSpace(raw.SchemaVersion),
		EnvelopeVersion:         strings.TrimSpace(raw.EnvelopeVersion),
	}
}

type fixtureMarketFile struct {
	Symbol                 string `yaml:"symbol"`
	SessionMode            string `yaml:"session_mode"`
	PublishInterval        string `yaml:"publish_interval"`
	StalenessThreshold     string `yaml:"staleness_threshold"`
	MaxFallbackAge         string `yaml:"max_fallback_age"`
	ClampBPS               int64  `yaml:"clamp_bps"`
	DivergenceThresholdBPS int64  `yaml:"divergence_threshold_bps"`
	PublishScale           int32  `yaml:"publish_scale"`
}

func (raw fixtureMarketFile) marketConfig(sourceOrder []string) (config.MarketConfig, error) {
	publishInterval, err := parseFixtureDuration("publish_interval", raw.PublishInterval)
	if err != nil {
		return config.MarketConfig{}, err
	}
	stalenessThreshold, err := parseFixtureDuration("staleness_threshold", raw.StalenessThreshold)
	if err != nil {
		return config.MarketConfig{}, err
	}
	maxFallbackAge, err := parseFixtureDuration("max_fallback_age", raw.MaxFallbackAge)
	if err != nil {
		return config.MarketConfig{}, err
	}

	sources := make([]config.MarketSourceRef, 0, len(sourceOrder))
	for _, source := range sourceOrder {
		source = strings.TrimSpace(source)
		if source == "" {
			return config.MarketConfig{}, fmt.Errorf("testutil: source_order contains an empty value")
		}
		sources = append(sources, config.MarketSourceRef{Name: source})
	}

	return config.MarketConfig{
		Symbol:                 strings.TrimSpace(raw.Symbol),
		SessionMode:            config.SessionMode(strings.TrimSpace(raw.SessionMode)),
		PublishInterval:        publishInterval,
		StalenessThreshold:     stalenessThreshold,
		MaxFallbackAge:         maxFallbackAge,
		ClampBPS:               raw.ClampBPS,
		DivergenceThresholdBPS: raw.DivergenceThresholdBPS,
		PublishScale:           raw.PublishScale,
		Sources:                sources,
	}, nil
}

type fixtureScenarioFile struct {
	Name         string                   `yaml:"name"`
	RecordID     string                   `yaml:"record_id"`
	At           time.Time                `yaml:"at"`
	Quotes       []fixtureQuoteFile       `yaml:"quotes"`
	SinkOutcomes []fixtureSinkOutcomeFile `yaml:"sink_outcomes"`
}

func (raw fixtureScenarioFile) normalize(symbol string) (FixtureScenario, error) {
	if strings.TrimSpace(raw.Name) == "" {
		return FixtureScenario{}, fmt.Errorf("testutil: scenario name is required")
	}
	if strings.TrimSpace(raw.RecordID) == "" {
		return FixtureScenario{}, fmt.Errorf("testutil: scenario %q record_id is required", raw.Name)
	}
	if raw.At.IsZero() {
		return FixtureScenario{}, fmt.Errorf("testutil: scenario %q time is required", raw.Name)
	}

	quotes := make([]feeds.Quote, 0, len(raw.Quotes))
	for _, current := range raw.Quotes {
		quote, err := current.quote(symbol)
		if err != nil {
			return FixtureScenario{}, fmt.Errorf("testutil: scenario %q: %w", raw.Name, err)
		}
		quotes = append(quotes, quote)
	}

	outcomes := make([]FixtureSinkOutcome, 0, len(raw.SinkOutcomes))
	for _, current := range raw.SinkOutcomes {
		outcome, err := current.normalize()
		if err != nil {
			return FixtureScenario{}, fmt.Errorf("testutil: scenario %q: %w", raw.Name, err)
		}
		outcomes = append(outcomes, outcome)
	}

	return FixtureScenario{
		Name:         strings.TrimSpace(raw.Name),
		RecordID:     strings.TrimSpace(raw.RecordID),
		At:           raw.At.UTC(),
		Quotes:       quotes,
		SinkOutcomes: outcomes,
	}, nil
}

type fixtureQuoteFile struct {
	Source    string    `yaml:"source"`
	Mid       string    `yaml:"mid"`
	BasisKind string    `yaml:"basis_kind"`
	BasisAt   time.Time `yaml:"basis_at"`
}

func (raw fixtureQuoteFile) quote(symbol string) (feeds.Quote, error) {
	value, err := fixedpoint.ParseDecimal(strings.TrimSpace(raw.Mid))
	if err != nil {
		return feeds.Quote{}, fmt.Errorf("parse quote mid: %w", err)
	}
	source := strings.TrimSpace(raw.Source)
	if source == "" {
		return feeds.Quote{}, fmt.Errorf("quote source is required")
	}
	if raw.BasisAt.IsZero() {
		return feeds.Quote{}, fmt.Errorf("quote basis_at is required")
	}

	kind, err := parseFixtureBasisKind(raw.BasisKind)
	if err != nil {
		return feeds.Quote{}, err
	}
	basisAt := raw.BasisAt.UTC()
	quote := feeds.Quote{
		Source:             source,
		Symbol:             symbol,
		Bid:                value,
		Ask:                value,
		Last:               value,
		Scale:              value.Scale,
		ReceivedAt:         basisAt,
		FreshnessBasisKind: kind,
		FreshnessBasisAt:   basisAt,
	}
	switch kind {
	case feeds.FreshnessBasisSourceTimestamp:
		sourceTS := basisAt
		quote.SourceTS = &sourceTS
	case feeds.FreshnessBasisReceivedAt:
	default:
		return feeds.Quote{}, fmt.Errorf("unsupported basis_kind %q", raw.BasisKind)
	}
	return quote, nil
}

type fixtureSinkOutcomeFile struct {
	Kind      string    `yaml:"kind"`
	Duplicate bool      `yaml:"duplicate"`
	AckedAt   time.Time `yaml:"acked_at"`
}

func (raw fixtureSinkOutcomeFile) normalize() (FixtureSinkOutcome, error) {
	kind := strings.TrimSpace(raw.Kind)
	switch kind {
	case "ack":
		if raw.AckedAt.IsZero() {
			return FixtureSinkOutcome{}, fmt.Errorf("ack outcome requires acked_at")
		}
	case "retryable_timeout", "terminal_error":
	default:
		return FixtureSinkOutcome{}, fmt.Errorf("unsupported sink outcome kind %q", raw.Kind)
	}

	return FixtureSinkOutcome{
		Kind:      kind,
		Duplicate: raw.Duplicate,
		AckedAt:   raw.AckedAt.UTC(),
	}, nil
}

func parseFixtureDuration(field string, raw string) (time.Duration, error) {
	duration, err := time.ParseDuration(strings.TrimSpace(raw))
	if err != nil {
		return 0, fmt.Errorf("testutil: parse %s: %w", field, err)
	}
	return duration, nil
}

func parseFixtureBasisKind(raw string) (feeds.FreshnessBasisKind, error) {
	switch strings.TrimSpace(raw) {
	case "source_timestamp", "source_ts":
		return feeds.FreshnessBasisSourceTimestamp, nil
	case "received_at":
		return feeds.FreshnessBasisReceivedAt, nil
	default:
		return "", fmt.Errorf("unsupported basis_kind %q", raw)
	}
}
