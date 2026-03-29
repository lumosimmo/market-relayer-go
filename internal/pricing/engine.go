package pricing

import (
	"errors"
	"fmt"
	"math"
	"math/big"
	"slices"
	"strings"
	"time"

	"github.com/lumosimmo/market-relayer-go/internal/config"
	"github.com/lumosimmo/market-relayer-go/internal/feeds"
	"github.com/lumosimmo/market-relayer-go/internal/fixedpoint"
	"github.com/lumosimmo/market-relayer-go/internal/status"
)

const (
	oracleEMATau    = time.Hour
	basisEMATau     = 150 * time.Second
	emaCapFraction  = 0.1
	alphaFloatScale = 1_000_000_000
)

type Inputs struct {
	Market         config.MarketConfig
	Quotes         []feeds.Quote
	Book           *feeds.BookSnapshot
	Prior          State
	At             time.Time
	ExternalClosed bool
}

type Result struct {
	Market            string
	ComputedAt        time.Time
	PublishScale      int32
	Inputs            []NormalizedInput
	ExternalSelection ExternalSelection
	External          Price
	Oracle            Price
	OracleClamped     bool
	OraclePlusBasis   Price
	SessionState      status.SessionState
	SessionReason     status.Reason
	State             State
}

type Price struct {
	Status status.Status
	Reason status.Reason
	Value  *fixedpoint.Value
	Scale  int32
}

func (price Price) Available() bool {
	return price.Value != nil && price.Status != status.StatusUnavailable
}

type NormalizedInput struct {
	Source             string
	SourceOrder        int
	Mid                fixedpoint.Value
	FreshnessBasisKind feeds.FreshnessBasisKind
	FreshnessBasisAt   time.Time
}

type SelectionRule string

const (
	SelectionRuleNoSources         SelectionRule = "no_sources"
	SelectionRuleSingleSource      SelectionRule = "single_source"
	SelectionRuleTwoSourceMidpoint SelectionRule = "two_source_midpoint"
	SelectionRuleTwoSourceDiverged SelectionRule = "two_source_diverged"
	SelectionRuleSurvivorSingle    SelectionRule = "survivor_single"
	SelectionRuleSurvivorMidpoint  SelectionRule = "survivor_midpoint"
	SelectionRuleSurvivorMedian    SelectionRule = "survivor_median"
	SelectionRuleNoSurvivors       SelectionRule = "no_survivors"
	SelectionRuleExternalClosed    SelectionRule = "external_closed"
)

type ExternalSelection struct {
	Rule            SelectionRule
	SelectedSources []string
	BasisSource     string
	BasisKind       feeds.FreshnessBasisKind
	BasisAt         time.Time
}

type State struct {
	LastExternal                     *fixedpoint.Value
	LastOracle                       *fixedpoint.Value
	LastOraclePlusBasis              *fixedpoint.Value
	LastBasisEMA                     *fixedpoint.Value
	FreshnessBasisKind               feeds.FreshnessBasisKind
	FreshnessBasisAt                 time.Time
	LastExternalAt                   time.Time
	LastOracleAt                     time.Time
	LastOraclePlusBasisAt            time.Time
	LastBasisAt                      time.Time
	OracleFallbackExpiresAt          time.Time
	OraclePlusBasisFallbackExpiresAt time.Time
}

func Compute(inputs Inputs) (Result, error) {
	at := inputs.At.UTC()
	if err := validateInputs(inputs, at); err != nil {
		return Result{}, err
	}

	normalized, err := normalizeInputs(inputs.Market, inputs.Quotes)
	if err != nil {
		return Result{}, err
	}

	externalCandidate, selection, err := computeExternal(inputs.Market, normalized)
	if err != nil {
		return Result{}, err
	}
	if inputs.ExternalClosed {
		selection = ExternalSelection{Rule: SelectionRuleExternalClosed}
	}

	result := Result{
		Market:            inputs.Market.Symbol,
		ComputedAt:        at,
		PublishScale:      inputs.Market.PublishScale,
		Inputs:            normalized,
		ExternalSelection: selection,
	}

	state := cloneState(inputs.Prior)

	oracle, state, usedInternal, oracleClamped, selection, err := computeOracle(inputs, at, externalCandidate, selection, state)
	if err != nil {
		return Result{}, err
	}
	result.Oracle = oracle
	result.OracleClamped = oracleClamped
	result.ExternalSelection = selection

	external, state := computePublishedExternal(inputs, at, state, oracle, externalCandidate)
	result.External = external

	oraclePlusBasis, state, err := computeOraclePlusBasis(inputs, at, state, oracle)
	if err != nil {
		return Result{}, err
	}
	result.OraclePlusBasis = oraclePlusBasis
	if usedInternal && result.ExternalSelection.BasisAt.IsZero() && inputs.Book != nil {
		result.ExternalSelection.BasisKind = inputs.Book.FreshnessBasisKind
		result.ExternalSelection.BasisAt = inputs.Book.FreshnessBasisAt.UTC()
		result.ExternalSelection.BasisSource = inputs.Book.Source
	}
	result.State = state
	return result, nil
}

func validateInputs(inputs Inputs, at time.Time) error {
	if at.IsZero() {
		return errors.New("pricing: compute time is required")
	}
	if strings.TrimSpace(inputs.Market.Symbol) == "" {
		return errors.New("pricing: market symbol is required")
	}
	if _, err := fixedpoint.Normalize(fixedpoint.Value{Int: 1, Scale: 0}, inputs.Market.PublishScale); err != nil {
		return fmt.Errorf("pricing: invalid publish scale: %w", err)
	}
	if inputs.Book != nil && inputs.Market.ImpactNotional().Int <= 0 {
		return errors.New("pricing: impact notional must be positive")
	}
	return validateState(inputs.Prior)
}

func validateState(state State) error {
	values := []struct {
		name  string
		value *fixedpoint.Value
	}{
		{name: "LastExternal", value: state.LastExternal},
		{name: "LastOracle", value: state.LastOracle},
		{name: "LastOraclePlusBasis", value: state.LastOraclePlusBasis},
		{name: "LastBasisEMA", value: state.LastBasisEMA},
	}
	for _, current := range values {
		if current.value == nil {
			continue
		}
		if _, err := fixedpoint.Normalize(*current.value, current.value.Scale); err != nil {
			return fmt.Errorf("pricing: invalid %s: %w", current.name, err)
		}
	}
	return nil
}

func normalizeInputs(market config.MarketConfig, quotes []feeds.Quote) ([]NormalizedInput, error) {
	if len(quotes) == 0 {
		return nil, nil
	}

	sourceOrder := make(map[string]int, len(market.Sources))
	for index, source := range market.Sources {
		sourceOrder[source.Name] = index
	}

	normalized := make([]NormalizedInput, 0, len(quotes))
	seen := make(map[string]struct{}, len(quotes))
	for _, quote := range quotes {
		if quote.Symbol != market.Symbol {
			return nil, fmt.Errorf("pricing: quote source %s symbol %q does not match market %s", quote.Source, quote.Symbol, market.Symbol)
		}
		order, ok := sourceOrder[quote.Source]
		if !ok {
			return nil, fmt.Errorf("pricing: quote source %s is not configured for market %s", quote.Source, market.Symbol)
		}
		if _, duplicate := seen[quote.Source]; duplicate {
			return nil, fmt.Errorf("pricing: duplicate quote for source %s", quote.Source)
		}
		seen[quote.Source] = struct{}{}

		mid, err := fixedpoint.Midpoint(quote.Bid, quote.Ask, market.PublishScale)
		if err != nil {
			return nil, fmt.Errorf("pricing: midpoint for source %s: %w", quote.Source, err)
		}
		normalized = append(normalized, NormalizedInput{
			Source:             quote.Source,
			SourceOrder:        order,
			Mid:                mid,
			FreshnessBasisKind: quote.FreshnessBasisKind,
			FreshnessBasisAt:   quote.FreshnessBasisAt.UTC(),
		})
	}

	slices.SortFunc(normalized, compareBySourceOrder)
	return normalized, nil
}

func computeExternal(market config.MarketConfig, inputs []NormalizedInput) (Price, ExternalSelection, error) {
	scale := market.PublishScale
	if len(inputs) == 0 {
		return unavailablePrice(scale, status.ReasonSourceRejected), ExternalSelection{Rule: SelectionRuleNoSources}, nil
	}
	if len(inputs) == 1 {
		selection := applySelection(ExternalSelection{Rule: SelectionRuleSingleSource}, inputs)
		return availablePrice(inputs[0].Mid, status.ReasonNone, scale), selection, nil
	}
	if len(inputs) == 2 {
		return computeTwoSourceExternal(scale, market.DivergenceThresholdBPS, inputs)
	}

	mids := make([]fixedpoint.Value, 0, len(inputs))
	for _, input := range inputs {
		mids = append(mids, input.Mid)
	}
	median, err := fixedpoint.Median(mids, scale)
	if err != nil {
		return Price{}, ExternalSelection{}, err
	}

	survivors := make([]NormalizedInput, 0, len(inputs))
	for _, input := range inputs {
		within, err := withinThreshold(input.Mid, median, market.DivergenceThresholdBPS)
		if err != nil {
			return Price{}, ExternalSelection{}, err
		}
		if within {
			survivors = append(survivors, input)
		}
	}

	switch len(survivors) {
	case 0:
		return unavailablePrice(scale, status.ReasonSourceDivergence), ExternalSelection{Rule: SelectionRuleNoSurvivors}, nil
	case 1:
		selection := applySelection(ExternalSelection{Rule: SelectionRuleSurvivorSingle}, survivors)
		return availablePrice(survivors[0].Mid, status.ReasonNone, scale), selection, nil
	case 2:
		price, err := midpointPrice(scale, survivors[0].Mid, survivors[1].Mid, status.ReasonNone)
		if err != nil {
			return Price{}, ExternalSelection{}, err
		}
		return price, applySelection(ExternalSelection{Rule: SelectionRuleSurvivorMidpoint}, survivors), nil
	default:
		survivorMids := make([]fixedpoint.Value, 0, len(survivors))
		for _, survivor := range survivors {
			survivorMids = append(survivorMids, survivor.Mid)
		}
		value, err := fixedpoint.Median(survivorMids, scale)
		if err != nil {
			return Price{}, ExternalSelection{}, err
		}
		return availablePrice(value, status.ReasonNone, scale), applySelection(ExternalSelection{Rule: SelectionRuleSurvivorMedian}, survivors), nil
	}
}

func computeTwoSourceExternal(scale int32, thresholdBPS int64, inputs []NormalizedInput) (Price, ExternalSelection, error) {
	if len(inputs) != 2 {
		return Price{}, ExternalSelection{}, fmt.Errorf("pricing: two-source selection requires exactly two inputs, got %d", len(inputs))
	}
	price, err := midpointPrice(scale, inputs[0].Mid, inputs[1].Mid, status.ReasonNone)
	if err != nil {
		return Price{}, ExternalSelection{}, err
	}
	within, err := pairWithinThreshold(inputs[0].Mid, inputs[1].Mid, mustValue(price.Value), thresholdBPS)
	if err != nil {
		return Price{}, ExternalSelection{}, err
	}
	if !within {
		return unavailablePrice(scale, status.ReasonSourceDivergence), ExternalSelection{Rule: SelectionRuleTwoSourceDiverged}, nil
	}
	return price, applySelection(ExternalSelection{Rule: SelectionRuleTwoSourceMidpoint}, inputs), nil
}

func computeOracle(
	inputs Inputs,
	at time.Time,
	externalCandidate Price,
	selection ExternalSelection,
	state State,
) (Price, State, bool, bool, ExternalSelection, error) {
	scale := inputs.Market.PublishScale
	if !inputs.ExternalClosed && externalCandidate.Available() && externalCandidate.Value != nil {
		value := *externalCandidate.Value
		clampedApplied := false
		if state.LastOracle != nil {
			clamped, err := fixedpoint.ClampDelta(*state.LastOracle, value, inputs.Market.ClampBPS)
			if err != nil {
				return Price{}, State{}, false, false, ExternalSelection{}, err
			}
			clampedApplied = clamped != value
			value = clamped
		}
		value, err := fixedpoint.Normalize(value, scale)
		if err != nil {
			return Price{}, State{}, false, false, ExternalSelection{}, err
		}
		oracle := availablePrice(value, status.ReasonNone, scale)
		state.LastOracle = cloneValue(oracle.Value)
		state.LastOracleAt = at
		state.OracleFallbackExpiresAt = at.Add(inputs.Market.MaxFallbackAge)
		state.FreshnessBasisKind = selection.BasisKind
		state.FreshnessBasisAt = selection.BasisAt.UTC()
		return oracle, state, false, clampedApplied, selection, nil
	}

	internalSelection := selection
	if inputs.ExternalClosed {
		internalSelection.Rule = SelectionRuleExternalClosed
	}

	if inputs.Book != nil && state.LastOracle != nil {
		impact, err := internalOracleTarget(*state.LastOracle, *inputs.Book, inputs.Market.ImpactNotional(), scale)
		if err != nil {
			return Price{}, State{}, false, false, ExternalSelection{}, err
		}
		value, err := smoothContinuous(*state.LastOracle, impact, at.Sub(state.LastOracleAt), oracleEMATau)
		if err != nil {
			return Price{}, State{}, false, false, ExternalSelection{}, err
		}
		clamped, err := fixedpoint.ClampDelta(*state.LastOracle, value, inputs.Market.ClampBPS)
		if err != nil {
			return Price{}, State{}, false, false, ExternalSelection{}, err
		}
		clampedApplied := clamped != value
		clamped, err = fixedpoint.Normalize(clamped, scale)
		if err != nil {
			return Price{}, State{}, false, false, ExternalSelection{}, err
		}
		oracle := availablePrice(clamped, status.ReasonNone, scale)
		state.LastOracle = cloneValue(oracle.Value)
		state.LastOracleAt = at
		state.OracleFallbackExpiresAt = at.Add(inputs.Market.MaxFallbackAge)
		state.FreshnessBasisKind = inputs.Book.FreshnessBasisKind
		state.FreshnessBasisAt = inputs.Book.FreshnessBasisAt.UTC()
		internalSelection.BasisKind = inputs.Book.FreshnessBasisKind
		internalSelection.BasisAt = inputs.Book.FreshnessBasisAt.UTC()
		internalSelection.BasisSource = inputs.Book.Source
		return oracle, state, true, clampedApplied, internalSelection, nil
	}

	if activeOracleFallback(at, state) {
		oracle := degradedPrice(*state.LastOracle, status.ReasonFallbackActive, scale)
		internalSelection.BasisKind = state.FreshnessBasisKind
		internalSelection.BasisAt = state.FreshnessBasisAt.UTC()
		return oracle, state, false, false, internalSelection, nil
	}
	if state.LastOracle != nil && !state.OracleFallbackExpiresAt.IsZero() {
		internalSelection.BasisKind = state.FreshnessBasisKind
		internalSelection.BasisAt = state.FreshnessBasisAt.UTC()
		return unavailablePrice(scale, status.ReasonFallbackExpired), state, false, false, internalSelection, nil
	}
	return unavailablePrice(scale, status.ReasonColdStart), state, false, false, internalSelection, nil
}

func computePublishedExternal(inputs Inputs, at time.Time, state State, oracle Price, externalCandidate Price) (Price, State) {
	scale := inputs.Market.PublishScale
	if !inputs.ExternalClosed && externalCandidate.Available() && oracle.Value != nil && oracle.Status == status.StatusAvailable {
		external := availablePrice(*oracle.Value, status.ReasonNone, scale)
		state.LastExternal = cloneValue(external.Value)
		state.LastExternalAt = at
		return external, state
	}

	if state.LastExternal != nil {
		reason := externalFrozenReason(inputs, externalCandidate)
		return degradedPrice(*state.LastExternal, reason, scale), state
	}
	return unavailablePrice(scale, externalFrozenReason(inputs, externalCandidate)), state
}

func computeOraclePlusBasis(inputs Inputs, at time.Time, state State, oracle Price) (Price, State, error) {
	scale := inputs.Market.PublishScale
	if oracle.Value != nil && inputs.Book != nil {
		mid, ok, err := bookMidpoint(*inputs.Book, scale)
		if err != nil {
			return Price{}, State{}, err
		}
		if ok {
			sample, err := subtractValues(mid, *oracle.Value)
			if err != nil {
				return Price{}, State{}, err
			}

			basis := sample
			if state.LastBasisEMA != nil && !state.LastBasisAt.IsZero() {
				basis, err = smoothContinuous(*state.LastBasisEMA, sample, at.Sub(state.LastBasisAt), basisEMATau)
				if err != nil {
					return Price{}, State{}, err
				}
			}
			basis, err = fixedpoint.Normalize(basis, scale)
			if err != nil {
				return Price{}, State{}, err
			}
			value, err := addValues(*oracle.Value, basis)
			if err != nil {
				return Price{}, State{}, err
			}
			if state.LastOraclePlusBasis != nil {
				value, err = fixedpoint.ClampDelta(*state.LastOraclePlusBasis, value, inputs.Market.ClampBPS)
				if err != nil {
					return Price{}, State{}, err
				}
			}
			value, err = fixedpoint.Normalize(value, scale)
			if err != nil {
				return Price{}, State{}, err
			}

			price := availablePrice(value, status.ReasonNone, scale)
			state.LastBasisEMA = cloneValue(&basis)
			state.LastBasisAt = at
			state.LastOraclePlusBasis = cloneValue(price.Value)
			state.LastOraclePlusBasisAt = at
			state.OraclePlusBasisFallbackExpiresAt = at.Add(inputs.Market.MaxFallbackAge)
			return price, state, nil
		}
	}

	if activeOraclePlusBasisFallback(at, state) {
		return degradedPrice(*state.LastOraclePlusBasis, status.ReasonFallbackActive, scale), state, nil
	}
	if state.LastOraclePlusBasis != nil && !state.OraclePlusBasisFallbackExpiresAt.IsZero() {
		return unavailablePrice(scale, status.ReasonFallbackExpired), state, nil
	}
	return unavailablePrice(scale, status.ReasonColdStart), state, nil
}

func internalOracleTarget(prev fixedpoint.Value, book feeds.BookSnapshot, impactNotional fixedpoint.Value, scale int32) (fixedpoint.Value, error) {
	impactBid, bidOK, err := impactPrice(book.Bids, impactNotional, scale)
	if err != nil {
		return fixedpoint.Value{}, err
	}
	impactAsk, askOK, err := impactPrice(book.Asks, impactNotional, scale)
	if err != nil {
		return fixedpoint.Value{}, err
	}

	ipd := fixedpoint.Value{Int: 0, Scale: scale}
	if bidOK {
		delta, err := subtractValues(impactBid, prev)
		if err != nil {
			return fixedpoint.Value{}, err
		}
		if delta.Int > 0 {
			ipd = delta
		}
	}
	if askOK {
		delta, err := subtractValues(prev, impactAsk)
		if err != nil {
			return fixedpoint.Value{}, err
		}
		if delta.Int > 0 {
			ipd, err = subtractValues(ipd, delta)
			if err != nil {
				return fixedpoint.Value{}, err
			}
		}
	}
	return addValues(prev, ipd)
}

func bookMidpoint(book feeds.BookSnapshot, scale int32) (fixedpoint.Value, bool, error) {
	if len(book.Bids) == 0 || len(book.Asks) == 0 {
		return fixedpoint.Value{}, false, nil
	}
	mid, err := fixedpoint.Midpoint(book.Bids[0].Price, book.Asks[0].Price, scale)
	if err != nil {
		return fixedpoint.Value{}, false, err
	}
	return mid, true, nil
}

func impactPrice(levels []feeds.BookLevel, impactNotional fixedpoint.Value, scale int32) (fixedpoint.Value, bool, error) {
	if len(levels) == 0 {
		return fixedpoint.Value{}, false, nil
	}

	remaining := valueToRat(impactNotional)
	totalNotional := new(big.Rat)
	totalSize := new(big.Rat)
	zero := new(big.Rat)

	for _, level := range levels {
		if level.Price.Int <= 0 || level.Size.Int <= 0 {
			continue
		}
		levelPrice := valueToRat(level.Price)
		levelSize := valueToRat(level.Size)
		levelNotional := new(big.Rat).Mul(levelPrice, levelSize)

		takeNotional := minRat(remaining, levelNotional)
		if takeNotional.Cmp(zero) <= 0 {
			continue
		}
		takeSize := new(big.Rat).Quo(takeNotional, levelPrice)

		totalNotional.Add(totalNotional, takeNotional)
		totalSize.Add(totalSize, takeSize)
		remaining.Sub(remaining, takeNotional)
		if remaining.Cmp(zero) <= 0 {
			break
		}
	}

	if remaining.Cmp(zero) > 0 || totalSize.Sign() == 0 {
		return fixedpoint.Value{}, false, nil
	}

	average := new(big.Rat).Quo(totalNotional, totalSize)
	value, err := ratToValue(average, scale)
	if err != nil {
		return fixedpoint.Value{}, false, err
	}
	return value, true, nil
}

func applySelection(selection ExternalSelection, inputs []NormalizedInput) ExternalSelection {
	ordered := append([]NormalizedInput(nil), inputs...)
	slices.SortFunc(ordered, compareBySourceOrder)
	selection.SelectedSources = make([]string, 0, len(ordered))
	for _, input := range ordered {
		selection.SelectedSources = append(selection.SelectedSources, input.Source)
		if selection.BasisAt.IsZero() || input.FreshnessBasisAt.Before(selection.BasisAt) {
			selection.BasisSource = input.Source
			selection.BasisKind = input.FreshnessBasisKind
			selection.BasisAt = input.FreshnessBasisAt.UTC()
		}
	}
	return selection
}

func compareBySourceOrder(left, right NormalizedInput) int {
	switch {
	case left.SourceOrder < right.SourceOrder:
		return -1
	case left.SourceOrder > right.SourceOrder:
		return 1
	case left.Source < right.Source:
		return -1
	case left.Source > right.Source:
		return 1
	default:
		return 0
	}
}

func withinThreshold(value fixedpoint.Value, base fixedpoint.Value, thresholdBPS int64) (bool, error) {
	normalizedValue, normalizedBase, err := normalizePair(value, base)
	if err != nil {
		return false, err
	}
	return differenceWithinThreshold(big.NewInt(absDiff(normalizedValue.Int, normalizedBase.Int)), normalizedBase.Int, thresholdBPS, "base")
}

func pairWithinThreshold(left fixedpoint.Value, right fixedpoint.Value, midpoint fixedpoint.Value, thresholdBPS int64) (bool, error) {
	normalizedLeft, normalizedMid, err := normalizePair(left, midpoint)
	if err != nil {
		return false, err
	}
	normalizedRight, normalizedMid, err := normalizePair(right, normalizedMid)
	if err != nil {
		return false, err
	}
	if normalizedMid.Int <= 0 {
		return false, fmt.Errorf("pricing: midpoint must be positive")
	}
	leftWithin, err := differenceWithinThreshold(big.NewInt(absDiff(normalizedLeft.Int, normalizedMid.Int)), normalizedMid.Int, thresholdBPS, "midpoint")
	if err != nil || !leftWithin {
		return leftWithin, err
	}
	return differenceWithinThreshold(big.NewInt(absDiff(normalizedRight.Int, normalizedMid.Int)), normalizedMid.Int, thresholdBPS, "midpoint")
}

func differenceWithinThreshold(diff *big.Int, base int64, thresholdBPS int64, label string) (bool, error) {
	if thresholdBPS < 0 {
		return false, fmt.Errorf("pricing: threshold bps must be non-negative")
	}
	if base <= 0 {
		return false, fmt.Errorf("pricing: %s must be positive", label)
	}
	limit := new(big.Int).Mul(big.NewInt(base), big.NewInt(thresholdBPS))
	limit = fixedpoint.MustDivRoundHalfAwayFromZero(limit, big.NewInt(fixedpoint.BasisPointsDenominator))
	return diff.Cmp(limit) <= 0, nil
}

func midpointPrice(scale int32, left fixedpoint.Value, right fixedpoint.Value, reason status.Reason) (Price, error) {
	value, err := fixedpoint.Midpoint(left, right, scale)
	if err != nil {
		return Price{}, err
	}
	return availablePrice(value, reason, scale), nil
}

func smoothContinuous(base fixedpoint.Value, target fixedpoint.Value, dt time.Duration, tau time.Duration) (fixedpoint.Value, error) {
	if tau <= 0 {
		return fixedpoint.Value{}, errors.New("pricing: tau must be positive")
	}
	if dt < 0 {
		dt = 0
	}
	capped := dt
	limit := time.Duration(float64(tau) * emaCapFraction)
	if capped > limit {
		capped = limit
	}
	if capped == 0 {
		return fixedpoint.Normalize(base, max(base.Scale, target.Scale))
	}

	scale := max(base.Scale, target.Scale)
	normalizedBase, err := fixedpoint.Normalize(base, scale)
	if err != nil {
		return fixedpoint.Value{}, err
	}
	normalizedTarget, err := fixedpoint.Normalize(target, scale)
	if err != nil {
		return fixedpoint.Value{}, err
	}

	alpha := 1 - math.Exp(-float64(capped)/float64(tau))
	if alpha <= 0 {
		return normalizedBase, nil
	}
	if alpha >= 1 {
		return normalizedTarget, nil
	}

	alphaInt := int64(math.Round(alpha * alphaFloatScale))
	if alphaInt <= 0 {
		return normalizedBase, nil
	}
	if alphaInt >= alphaFloatScale {
		return normalizedTarget, nil
	}

	delta := big.NewInt(normalizedTarget.Int - normalizedBase.Int)
	moveNum := new(big.Int).Mul(delta, big.NewInt(alphaInt))
	move := fixedpoint.MustDivRoundHalfAwayFromZero(moveNum, big.NewInt(alphaFloatScale))
	result := new(big.Int).Add(big.NewInt(normalizedBase.Int), move)
	if !result.IsInt64() {
		return fixedpoint.Value{}, errors.New("pricing: continuous ema overflow")
	}
	return fixedpoint.Value{Int: result.Int64(), Scale: scale}, nil
}

func normalizePair(left fixedpoint.Value, right fixedpoint.Value) (fixedpoint.Value, fixedpoint.Value, error) {
	scale := max(left.Scale, right.Scale)
	normalizedLeft, err := fixedpoint.Normalize(left, scale)
	if err != nil {
		return fixedpoint.Value{}, fixedpoint.Value{}, err
	}
	normalizedRight, err := fixedpoint.Normalize(right, scale)
	if err != nil {
		return fixedpoint.Value{}, fixedpoint.Value{}, err
	}
	return normalizedLeft, normalizedRight, nil
}

func availablePrice(value fixedpoint.Value, reason status.Reason, scale int32) Price {
	normalized, err := fixedpoint.Normalize(value, scale)
	if err != nil {
		panic(err)
	}
	return Price{
		Status: status.StatusAvailable,
		Reason: defaultReason(reason, status.ReasonNone),
		Value:  cloneValue(&normalized),
		Scale:  scale,
	}
}

func degradedPrice(value fixedpoint.Value, reason status.Reason, scale int32) Price {
	normalized, err := fixedpoint.Normalize(value, scale)
	if err != nil {
		panic(err)
	}
	return Price{
		Status: status.StatusDegraded,
		Reason: reason,
		Value:  cloneValue(&normalized),
		Scale:  scale,
	}
}

func unavailablePrice(scale int32, reason status.Reason) Price {
	if reason == "" || reason == status.ReasonNone {
		reason = status.ReasonSourceRejected
	}
	return Price{
		Status: status.StatusUnavailable,
		Reason: reason,
		Scale:  scale,
	}
}

func activeOracleFallback(at time.Time, state State) bool {
	return state.LastOracle != nil &&
		!state.OracleFallbackExpiresAt.IsZero() &&
		!at.After(state.OracleFallbackExpiresAt.UTC())
}

func activeOraclePlusBasisFallback(at time.Time, state State) bool {
	return state.LastOraclePlusBasis != nil &&
		!state.OraclePlusBasisFallbackExpiresAt.IsZero() &&
		!at.After(state.OraclePlusBasisFallbackExpiresAt.UTC())
}

func externalFrozenReason(inputs Inputs, externalCandidate Price) status.Reason {
	if inputs.ExternalClosed {
		return status.ReasonSessionClosed
	}
	switch externalCandidate.Reason {
	case status.ReasonSourceDivergence:
		return status.ReasonSourceDivergence
	case status.ReasonFallbackExpired:
		return status.ReasonFallbackExpired
	default:
		return status.ReasonSourceStale
	}
}

func defaultReason(reason status.Reason, fallback status.Reason) status.Reason {
	if reason == "" {
		return fallback
	}
	return reason
}

func cloneState(input State) State {
	return State{
		LastExternal:                     cloneValue(input.LastExternal),
		LastOracle:                       cloneValue(input.LastOracle),
		LastOraclePlusBasis:              cloneValue(input.LastOraclePlusBasis),
		LastBasisEMA:                     cloneValue(input.LastBasisEMA),
		FreshnessBasisKind:               input.FreshnessBasisKind,
		FreshnessBasisAt:                 input.FreshnessBasisAt.UTC(),
		LastExternalAt:                   input.LastExternalAt.UTC(),
		LastOracleAt:                     input.LastOracleAt.UTC(),
		LastOraclePlusBasisAt:            input.LastOraclePlusBasisAt.UTC(),
		LastBasisAt:                      input.LastBasisAt.UTC(),
		OracleFallbackExpiresAt:          input.OracleFallbackExpiresAt.UTC(),
		OraclePlusBasisFallbackExpiresAt: input.OraclePlusBasisFallbackExpiresAt.UTC(),
	}
}

func cloneValue(value *fixedpoint.Value) *fixedpoint.Value {
	if value == nil {
		return nil
	}
	copied := *value
	return &copied
}

func mustValue(value *fixedpoint.Value) fixedpoint.Value {
	if value == nil {
		panic("pricing: expected value")
	}
	return *value
}

func addValues(left fixedpoint.Value, right fixedpoint.Value) (fixedpoint.Value, error) {
	normalizedLeft, normalizedRight, err := normalizePair(left, right)
	if err != nil {
		return fixedpoint.Value{}, err
	}
	sum := new(big.Int).Add(big.NewInt(normalizedLeft.Int), big.NewInt(normalizedRight.Int))
	if !sum.IsInt64() {
		return fixedpoint.Value{}, errors.New("pricing: addition overflow")
	}
	return fixedpoint.Value{Int: sum.Int64(), Scale: normalizedLeft.Scale}, nil
}

func subtractValues(left fixedpoint.Value, right fixedpoint.Value) (fixedpoint.Value, error) {
	normalizedLeft, normalizedRight, err := normalizePair(left, right)
	if err != nil {
		return fixedpoint.Value{}, err
	}
	diff := new(big.Int).Sub(big.NewInt(normalizedLeft.Int), big.NewInt(normalizedRight.Int))
	if !diff.IsInt64() {
		return fixedpoint.Value{}, errors.New("pricing: subtraction overflow")
	}
	return fixedpoint.Value{Int: diff.Int64(), Scale: normalizedLeft.Scale}, nil
}

func valueToRat(value fixedpoint.Value) *big.Rat {
	denom := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(value.Scale)), nil)
	return new(big.Rat).SetFrac(big.NewInt(value.Int), denom)
}

func ratToValue(value *big.Rat, scale int32) (fixedpoint.Value, error) {
	multiplier := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scale)), nil)
	scaledNum := new(big.Int).Mul(value.Num(), multiplier)
	rounded := fixedpoint.MustDivRoundHalfAwayFromZero(scaledNum, value.Denom())
	if !rounded.IsInt64() {
		return fixedpoint.Value{}, errors.New("pricing: rational conversion overflow")
	}
	return fixedpoint.Value{Int: rounded.Int64(), Scale: scale}, nil
}

func minRat(left *big.Rat, right *big.Rat) *big.Rat {
	if left.Cmp(right) <= 0 {
		return new(big.Rat).Set(left)
	}
	return new(big.Rat).Set(right)
}

func absDiff(left int64, right int64) int64 {
	if left >= right {
		return left - right
	}
	return right - left
}
