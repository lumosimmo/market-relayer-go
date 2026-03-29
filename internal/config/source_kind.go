package config

import (
	"errors"
	"fmt"
	"slices"
	"strings"
)

const (
	SourceKindCoinbase    = "coinbase"
	SourceKindKraken      = "kraken"
	SourceKindPyth        = "pyth"
	SourceKindHyperliquid = "hyperliquid"

	PythCurveModeNearbyLinearRoll = "nearby_linear_roll"
)

type sourceKindValidator func(SourceConfig) error

var sourceKindValidators = map[string]sourceKindValidator{
	SourceKindCoinbase:    validateCoinbaseSourceConfig,
	SourceKindKraken:      validateKrakenSourceConfig,
	SourceKindPyth:        validatePythSourceConfig,
	SourceKindHyperliquid: validateHyperliquidSourceConfig,
}

var sessionCapableSourceKinds = map[string]struct{}{
	SourceKindPyth: {},
}

func validateSourceKind(source SourceConfig) error {
	validate, ok := sourceKindValidators[source.Kind]
	if !ok {
		return fmt.Errorf("config: unsupported source.kind %q", source.Kind)
	}
	return validate(source)
}

func sourceKinds() []string {
	if len(sourceKindValidators) == 0 {
		return nil
	}

	kinds := make([]string, 0, len(sourceKindValidators))
	for kind := range sourceKindValidators {
		kinds = append(kinds, kind)
	}
	slices.Sort(kinds)
	return kinds
}

func sourceKindSessionCapable(kind string) bool {
	_, ok := sessionCapableSourceKinds[kind]
	return ok
}

func validateCoinbaseSourceConfig(source SourceConfig) error {
	if strings.TrimSpace(source.ProductID) == "" {
		return errors.New("config: coinbase source requires product_id")
	}
	return nil
}

func validateKrakenSourceConfig(source SourceConfig) error {
	if strings.TrimSpace(source.Pair) == "" {
		return errors.New("config: kraken source requires pair")
	}
	if source.TimestampMode != TimestampModeReceivedAt {
		return fmt.Errorf("config: kraken source %s must use timestamp_mode %q", source.Name, TimestampModeReceivedAt)
	}
	return nil
}

func validatePythSourceConfig(source SourceConfig) error {
	if strings.TrimSpace(source.AssetType) == "" {
		return errors.New("config: pyth source requires asset_type")
	}
	if strings.TrimSpace(source.RootSymbol) == "" {
		return errors.New("config: pyth source requires root_symbol")
	}
	if strings.TrimSpace(source.QuoteCurrency) == "" {
		return errors.New("config: pyth source requires quote_currency")
	}
	if source.CurveMode != PythCurveModeNearbyLinearRoll {
		return fmt.Errorf("config: pyth source %s must use curve_mode %q", source.Name, PythCurveModeNearbyLinearRoll)
	}
	if source.RollWindowDays <= 0 {
		return errors.New("config: pyth source requires roll_window_days greater than 0")
	}
	if source.MaxConfidenceBPS <= 0 || source.MaxConfidenceBPS > maxBasisPoints {
		return fmt.Errorf("config: pyth source %s max_confidence_bps must be between 1 and %d", source.Name, maxBasisPoints)
	}
	if source.TimestampMode != TimestampModeSourceTimestamp {
		return fmt.Errorf("config: pyth source %s must use timestamp_mode %q", source.Name, TimestampModeSourceTimestamp)
	}
	return nil
}

func validateHyperliquidSourceConfig(source SourceConfig) error {
	if strings.TrimSpace(source.Market) == "" {
		return errors.New("config: hyperliquid source requires market")
	}
	if source.TimestampMode != TimestampModeSourceTimestamp {
		return fmt.Errorf("config: hyperliquid source %s must use timestamp_mode %q", source.Name, TimestampModeSourceTimestamp)
	}
	return nil
}
