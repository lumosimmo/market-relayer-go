package config

import "testing"

func TestSourceKindHelpers(t *testing.T) {
	t.Parallel()

	t.Run("reports supported kinds in sorted order", func(t *testing.T) {
		t.Parallel()

		got := sourceKinds()
		want := []string{SourceKindCoinbase, SourceKindHyperliquid, SourceKindKraken, SourceKindPyth}
		if len(got) != len(want) {
			t.Fatalf("len(sourceKinds()) = %d, want %d", len(got), len(want))
		}
		for index := range want {
			if got[index] != want[index] {
				t.Fatalf("sourceKinds()[%d] = %q, want %q", index, got[index], want[index])
			}
		}
	})

	t.Run("validates supported kinds", func(t *testing.T) {
		t.Parallel()

		if err := validateSourceKind(SourceConfig{
			Name:           "coinbase-primary",
			Kind:           SourceKindCoinbase,
			ProductID:      "ETH-USD",
			RequestTimeout: 1,
		}); err != nil {
			t.Fatalf("validateSourceKind(coinbase) error = %v", err)
		}

		if err := validateSourceKind(SourceConfig{
			Name:           "kraken-secondary",
			Kind:           SourceKindKraken,
			Pair:           "ETH/USD",
			TimestampMode:  TimestampModeReceivedAt,
			RequestTimeout: 1,
		}); err != nil {
			t.Fatalf("validateSourceKind(kraken) error = %v", err)
		}

		if err := validateSourceKind(SourceConfig{
			Name:             "pyth-wti",
			Kind:             SourceKindPyth,
			AssetType:        "Commodities",
			RootSymbol:       "WTI",
			QuoteCurrency:    "USD",
			CurveMode:        PythCurveModeNearbyLinearRoll,
			RollWindowDays:   5,
			MaxConfidenceBPS: 250,
			TimestampMode:    TimestampModeSourceTimestamp,
			RequestTimeout:   1,
		}); err != nil {
			t.Fatalf("validateSourceKind(pyth) error = %v", err)
		}

		if !sourceKindSessionCapable(SourceKindPyth) {
			t.Fatal("sourceKindSessionCapable(pyth) = false, want true")
		}
		if sourceKindSessionCapable(SourceKindCoinbase) {
			t.Fatal("sourceKindSessionCapable(coinbase) = true, want false")
		}
	})

	t.Run("reports unsupported and provider-specific validation errors", func(t *testing.T) {
		t.Parallel()

		if err := validateSourceKind(SourceConfig{Kind: "bad"}); err == nil {
			t.Fatal("validateSourceKind(unsupported) error = nil, want error")
		}
		if err := validateSourceKind(SourceConfig{Kind: SourceKindCoinbase}); err == nil {
			t.Fatal("validateSourceKind(coinbase missing product_id) error = nil, want error")
		}
		if err := validateSourceKind(SourceConfig{
			Name:          "kraken-secondary",
			Kind:          SourceKindKraken,
			Pair:          "ETH/USD",
			TimestampMode: TimestampModeSourceTimestamp,
		}); err == nil {
			t.Fatal("validateSourceKind(kraken wrong timestamp) error = nil, want error")
		}
		if err := validateSourceKind(SourceConfig{
			Name:             "pyth-wti",
			Kind:             SourceKindPyth,
			AssetType:        "Commodities",
			RootSymbol:       "WTI",
			QuoteCurrency:    "USD",
			CurveMode:        "front_month",
			RollWindowDays:   5,
			MaxConfidenceBPS: 250,
			TimestampMode:    TimestampModeSourceTimestamp,
		}); err == nil {
			t.Fatal("validateSourceKind(pyth unsupported curve mode) error = nil, want error")
		}
	})
}
