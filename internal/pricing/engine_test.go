package pricing

import (
	"testing"
	"time"

	"github.com/lumosimmo/market-relayer-go/internal/feeds"
	"github.com/lumosimmo/market-relayer-go/internal/fixedpoint"
	"github.com/lumosimmo/market-relayer-go/internal/status"
)

func TestComputeExternalSelectionRules(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 3, 26, 11, 0, 0, 0, time.UTC)

	t.Run("single source", func(t *testing.T) {
		t.Parallel()

		result := mustCompute(t, Inputs{
			Market: testMarket(50),
			Quotes: []feeds.Quote{
				testQuote(t, "coinbase", "ETHUSD", "1981.75", now, feeds.FreshnessBasisSourceTimestamp),
			},
			Book: testBook(t, "hl", "ETHUSD", "ETH", []testLevel{{Price: "1982.25", Size: "100"}}, []testLevel{{Price: "1982.45", Size: "100"}}, now),
			At:   now,
		})

		if got, want := result.ExternalSelection.Rule, SelectionRuleSingleSource; got != want {
			t.Fatalf("ExternalSelection.Rule = %q, want %q", got, want)
		}
		assertValue(t, result.External.Value, mustDecimal(t, "1981.75"))
		assertValue(t, result.Oracle.Value, mustDecimal(t, "1981.75"))
		assertValue(t, result.OraclePlusBasis.Value, mustDecimal(t, "1982.35"))
	})

	t.Run("two source divergence", func(t *testing.T) {
		t.Parallel()

		result := mustCompute(t, Inputs{
			Market: testMarket(50),
			Quotes: []feeds.Quote{
				testQuote(t, "coinbase", "ETHUSD", "1981.75", now, feeds.FreshnessBasisSourceTimestamp),
				testQuote(t, "kraken", "ETHUSD", "2021.39", now.Add(time.Second), feeds.FreshnessBasisReceivedAt),
			},
			Book: testBook(t, "hl", "ETHUSD", "ETH", []testLevel{{Price: "2011.48", Size: "100"}}, []testLevel{{Price: "2015.45", Size: "100"}}, now),
			Prior: State{
				LastOracle:              valuePtr(mustDecimal(t, "1981.75")),
				LastOracleAt:            now.Add(-time.Minute),
				OracleFallbackExpiresAt: now.Add(time.Minute),
			},
			At: now,
		})

		if got, want := result.ExternalSelection.Rule, SelectionRuleTwoSourceDiverged; got != want {
			t.Fatalf("ExternalSelection.Rule = %q, want %q", got, want)
		}
		if got, want := result.External.Status, status.StatusUnavailable; got != want {
			t.Fatalf("External.Status = %q, want %q", got, want)
		}
		if got, want := result.Oracle.Status, status.StatusAvailable; got != want {
			t.Fatalf("Oracle.Status = %q, want %q", got, want)
		}
		if got, want := result.External.Reason, status.ReasonSourceDivergence; got != want {
			t.Fatalf("External.Reason = %q, want %q", got, want)
		}
	})

	t.Run("survivor midpoint", func(t *testing.T) {
		t.Parallel()

		result := mustCompute(t, Inputs{
			Market: testMarket(50),
			Quotes: []feeds.Quote{
				testQuote(t, "coinbase", "ETHUSD", "1981.75", now, feeds.FreshnessBasisSourceTimestamp),
				testQuote(t, "kraken", "ETHUSD", "1985.71", now.Add(time.Second), feeds.FreshnessBasisReceivedAt),
				testQuote(t, "backup", "ETHUSD", "2041.20", now.Add(2*time.Second), feeds.FreshnessBasisSourceTimestamp),
			},
			Book: testBook(t, "hl", "ETHUSD", "ETH", []testLevel{{Price: "1989.68", Size: "100"}}, []testLevel{{Price: "1993.64", Size: "100"}}, now),
			At:   now,
		})

		if got, want := result.ExternalSelection.Rule, SelectionRuleSurvivorMidpoint; got != want {
			t.Fatalf("ExternalSelection.Rule = %q, want %q", got, want)
		}
		if got, want := result.ExternalSelection.SelectedSources, []string{"coinbase", "kraken"}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
			t.Fatalf("SelectedSources = %v, want %v", got, want)
		}
		assertValue(t, result.Oracle.Value, mustDecimal(t, "1983.73"))
	})
}

func TestComputeFreshExternalAndBasisClamp(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 3, 26, 12, 0, 0, 0, time.UTC)
	market := testMarket(50)

	t.Run("fresh external updates all three prices", func(t *testing.T) {
		t.Parallel()

		result := mustCompute(t, Inputs{
			Market: market,
			Quotes: []feeds.Quote{
				testQuote(t, "coinbase", "ETHUSD", "1981.75", now, feeds.FreshnessBasisSourceTimestamp),
			},
			Book: testBook(t, "hl", "ETHUSD", "ETH", []testLevel{{Price: "1982.75", Size: "100"}}, []testLevel{{Price: "1982.95", Size: "100"}}, now),
			At:   now,
		})

		assertPrice(t, result.External, status.StatusAvailable, status.ReasonNone, "1981.75")
		assertPrice(t, result.Oracle, status.StatusAvailable, status.ReasonNone, "1981.75")
		assertPrice(t, result.OraclePlusBasis, status.StatusAvailable, status.ReasonNone, "1982.85")
		assertValue(t, result.State.LastExternal, mustDecimal(t, "1981.75"))
		assertValue(t, result.State.LastOracle, mustDecimal(t, "1981.75"))
		assertValue(t, result.State.LastBasisEMA, mustDecimal(t, "1.10"))
		assertValue(t, result.State.LastOraclePlusBasis, mustDecimal(t, "1982.85"))
		if got, want := result.State.FreshnessBasisKind, feeds.FreshnessBasisSourceTimestamp; got != want {
			t.Fatalf("FreshnessBasisKind = %q, want %q", got, want)
		}
	})

	t.Run("fresh external clamp uses prior oracle and prior oracle_plus_basis", func(t *testing.T) {
		t.Parallel()

		result := mustCompute(t, Inputs{
			Market: market,
			Quotes: []feeds.Quote{
				testQuote(t, "coinbase", "ETHUSD", "2001.75", now, feeds.FreshnessBasisSourceTimestamp),
			},
			Book: testBook(t, "hl", "ETHUSD", "ETH", []testLevel{{Price: "2004.75", Size: "100"}}, []testLevel{{Price: "2004.95", Size: "100"}}, now),
			Prior: State{
				LastExternal:                     valuePtr(mustDecimal(t, "1981.75")),
				LastOracle:                       valuePtr(mustDecimal(t, "1981.75")),
				LastOraclePlusBasis:              valuePtr(mustDecimal(t, "1981.95")),
				LastBasisEMA:                     valuePtr(mustDecimal(t, "0.20")),
				LastExternalAt:                   now.Add(-time.Minute),
				LastOracleAt:                     now.Add(-time.Minute),
				LastOraclePlusBasisAt:            now.Add(-time.Minute),
				LastBasisAt:                      now.Add(-time.Minute),
				OracleFallbackExpiresAt:          now.Add(time.Minute),
				OraclePlusBasisFallbackExpiresAt: now.Add(time.Minute),
			},
			At: now,
		})

		assertPrice(t, result.External, status.StatusAvailable, status.ReasonNone, "1991.66")
		assertPrice(t, result.Oracle, status.StatusAvailable, status.ReasonNone, "1991.66")
		assertPrice(t, result.OraclePlusBasis, status.StatusAvailable, status.ReasonNone, "1991.86")
	})
}

func TestComputeInternalOracleAndBasisEMA(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 3, 26, 13, 0, 0, 0, time.UTC)
	market := testMarket(50)

	prior := State{
		LastExternal:                     valuePtr(mustDecimal(t, "1981.75")),
		LastOracle:                       valuePtr(mustDecimal(t, "1981.75")),
		LastOraclePlusBasis:              valuePtr(mustDecimal(t, "1982.75")),
		LastBasisEMA:                     valuePtr(mustDecimal(t, "1.00")),
		LastExternalAt:                   now.Add(-time.Minute),
		LastOracleAt:                     now.Add(-time.Hour),
		LastOraclePlusBasisAt:            now.Add(-time.Minute),
		LastBasisAt:                      now.Add(-time.Minute),
		OracleFallbackExpiresAt:          now.Add(time.Minute),
		OraclePlusBasisFallbackExpiresAt: now.Add(time.Minute),
	}
	book := testBook(t, "hl", "ETHUSD", "ETH",
		[]testLevel{{Price: "1983.75", Size: "100"}},
		[]testLevel{{Price: "1983.95", Size: "100"}},
		now,
	)

	result := mustCompute(t, Inputs{
		Market: market,
		Book:   book,
		Prior:  prior,
		At:     now,
	})

	target, err := internalOracleTarget(*prior.LastOracle, *book, market.ImpactNotional(), market.PublishScale)
	if err != nil {
		t.Fatalf("internalOracleTarget() error = %v", err)
	}
	wantOracle, err := smoothContinuous(*prior.LastOracle, target, now.Sub(prior.LastOracleAt), oracleEMATau)
	if err != nil {
		t.Fatalf("smoothContinuous(oracle) error = %v", err)
	}
	wantOracle, err = fixedpoint.ClampDelta(*prior.LastOracle, wantOracle, market.ClampBPS)
	if err != nil {
		t.Fatalf("ClampDelta(oracle) error = %v", err)
	}
	mid, ok, err := bookMidpoint(*book, market.PublishScale)
	if err != nil {
		t.Fatalf("bookMidpoint() error = %v", err)
	}
	if !ok {
		t.Fatal("bookMidpoint() = unavailable, want midpoint")
	}
	wantBasisSample, err := subtractValues(mid, wantOracle)
	if err != nil {
		t.Fatalf("subtractValues() error = %v", err)
	}
	wantBasis, err := smoothContinuous(*prior.LastBasisEMA, wantBasisSample, now.Sub(prior.LastBasisAt), basisEMATau)
	if err != nil {
		t.Fatalf("smoothContinuous(basis) error = %v", err)
	}
	wantBasis, err = fixedpoint.Normalize(wantBasis, market.PublishScale)
	if err != nil {
		t.Fatalf("Normalize(basis) error = %v", err)
	}
	wantOraclePlusBasis, err := addValues(wantOracle, wantBasis)
	if err != nil {
		t.Fatalf("addValues() error = %v", err)
	}
	wantOraclePlusBasis, err = fixedpoint.ClampDelta(*prior.LastOraclePlusBasis, wantOraclePlusBasis, market.ClampBPS)
	if err != nil {
		t.Fatalf("ClampDelta(oracle_plus_basis) error = %v", err)
	}

	oracleText := mustFormat(t, wantOracle)
	oraclePlusBasisText := mustFormat(t, wantOraclePlusBasis)
	basisText := mustFormat(t, wantBasis)

	assertPrice(t, result.External, status.StatusDegraded, status.ReasonSourceStale, "1981.75")
	assertPrice(t, result.Oracle, status.StatusAvailable, status.ReasonNone, oracleText)
	assertPrice(t, result.OraclePlusBasis, status.StatusAvailable, status.ReasonNone, oraclePlusBasisText)
	assertValue(t, result.State.LastBasisEMA, mustDecimal(t, basisText))
	assertValue(t, result.State.LastOracle, mustDecimal(t, oracleText))
	if got, want := result.ExternalSelection.BasisSource, "hl"; got != want {
		t.Fatalf("BasisSource = %q, want %q", got, want)
	}
}

func TestComputeExternalFreezeFallbackAndColdStart(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 3, 26, 14, 0, 0, 0, time.UTC)
	market := testMarket(50)

	t.Run("scheduled closure freezes external while internal pricing stays live", func(t *testing.T) {
		t.Parallel()

		result := mustCompute(t, Inputs{
			Market: market,
			Book: testBook(t, "hl", "ETHUSD", "ETH",
				[]testLevel{{Price: "1983.75", Size: "100"}},
				[]testLevel{{Price: "1983.95", Size: "100"}},
				now,
			),
			Prior: State{
				LastExternal:                     valuePtr(mustDecimal(t, "1981.75")),
				LastOracle:                       valuePtr(mustDecimal(t, "1981.75")),
				LastOraclePlusBasis:              valuePtr(mustDecimal(t, "1982.75")),
				LastBasisEMA:                     valuePtr(mustDecimal(t, "1.00")),
				LastExternalAt:                   now.Add(-time.Minute),
				LastOracleAt:                     now.Add(-time.Minute),
				LastOraclePlusBasisAt:            now.Add(-time.Minute),
				LastBasisAt:                      now.Add(-time.Minute),
				OracleFallbackExpiresAt:          now.Add(time.Minute),
				OraclePlusBasisFallbackExpiresAt: now.Add(time.Minute),
			},
			ExternalClosed: true,
			At:             now,
		})

		assertPrice(t, result.External, status.StatusDegraded, status.ReasonSessionClosed, "1981.75")
		if got, want := result.ExternalSelection.Rule, SelectionRuleExternalClosed; got != want {
			t.Fatalf("ExternalSelection.Rule = %q, want %q", got, want)
		}
		if result.Oracle.Status != status.StatusAvailable {
			t.Fatalf("Oracle.Status = %q, want available", result.Oracle.Status)
		}
	})

	t.Run("oracle_plus_basis falls back when book disappears", func(t *testing.T) {
		t.Parallel()

		result := mustCompute(t, Inputs{
			Market: market,
			Quotes: []feeds.Quote{
				testQuote(t, "coinbase", "ETHUSD", "1981.75", now, feeds.FreshnessBasisSourceTimestamp),
			},
			Prior: State{
				LastOraclePlusBasis:              valuePtr(mustDecimal(t, "1982.75")),
				LastOraclePlusBasisAt:            now.Add(-time.Minute),
				OraclePlusBasisFallbackExpiresAt: now.Add(time.Minute),
			},
			At: now,
		})

		assertPrice(t, result.Oracle, status.StatusAvailable, status.ReasonNone, "1981.75")
		assertPrice(t, result.OraclePlusBasis, status.StatusDegraded, status.ReasonFallbackActive, "1982.75")
	})

	t.Run("oracle and oracle_plus_basis fallback expire independently", func(t *testing.T) {
		t.Parallel()

		expired := now.Add(2 * time.Minute)
		result := mustCompute(t, Inputs{
			Market: market,
			Prior: State{
				LastExternal:                     valuePtr(mustDecimal(t, "1981.75")),
				LastOracle:                       valuePtr(mustDecimal(t, "1981.75")),
				LastOraclePlusBasis:              valuePtr(mustDecimal(t, "1982.75")),
				LastExternalAt:                   now,
				LastOracleAt:                     now,
				LastOraclePlusBasisAt:            now,
				OracleFallbackExpiresAt:          now.Add(time.Minute),
				OraclePlusBasisFallbackExpiresAt: now.Add(time.Minute),
			},
			At: expired,
		})

		assertPrice(t, result.External, status.StatusDegraded, status.ReasonSourceStale, "1981.75")
		assertPrice(t, result.Oracle, status.StatusUnavailable, status.ReasonFallbackExpired, "")
		assertPrice(t, result.OraclePlusBasis, status.StatusUnavailable, status.ReasonFallbackExpired, "")
	})

	t.Run("cold start without external price keeps oracle and oracle_plus_basis unavailable", func(t *testing.T) {
		t.Parallel()

		result := mustCompute(t, Inputs{
			Market: market,
			Book: testBook(t, "hl", "ETHUSD", "ETH",
				[]testLevel{{Price: "1982.75", Size: "100"}},
				[]testLevel{{Price: "1982.95", Size: "100"}},
				now,
			),
			At: now,
		})

		assertPrice(t, result.External, status.StatusUnavailable, status.ReasonSourceStale, "")
		assertPrice(t, result.Oracle, status.StatusUnavailable, status.ReasonColdStart, "")
		assertPrice(t, result.OraclePlusBasis, status.StatusUnavailable, status.ReasonColdStart, "")
	})
}

func TestComputeRejectsInvalidPriorState(t *testing.T) {
	t.Parallel()

	_, err := Compute(Inputs{
		Market: testMarket(50),
		Prior: State{
			LastOracle: &fixedpoint.Value{Int: 1, Scale: fixedpoint.MaxScale + 1},
		},
		At: time.Date(2026, 3, 26, 15, 0, 0, 0, time.UTC),
	})
	if err == nil {
		t.Fatal("Compute(invalid prior state) error = nil, want validation failure")
	}
}
