package feeds

import (
	"fmt"
	"math"
	"math/big"
	"strings"
	"time"

	"github.com/lumosimmo/market-relayer-go/internal/fixedpoint"
)

func (source *pythSource) syntheticQuote(receivedAt time.Time, live []liveContract) (Quote, error) {
	scale := quoteScale(live)
	last, err := weightedValue(scale, live, func(current liveContract) fixedpoint.Value {
		return current.price
	})
	if err != nil {
		return Quote{}, invalidQuoteError(source.config.Name, "compute synthetic last", err)
	}
	rawConf, err := weightedValue(scale, live, func(current liveContract) fixedpoint.Value {
		return current.conf
	})
	if err != nil {
		return Quote{}, invalidQuoteError(source.config.Name, "compute synthetic confidence", err)
	}

	if err := validateConfidence(last, rawConf, source.config.MaxConfidenceBPS); err != nil {
		return Quote{}, invalidQuoteError(source.config.Name, "confidence threshold exceeded", err)
	}

	if last.Int <= 0 {
		return Quote{}, invalidQuoteError(source.config.Name, "synthetic price must be positive", nil)
	}

	sourceTS := earliestPublishTime(live)
	return Quote{
		Source:             source.config.Name,
		Symbol:             source.symbol,
		Bid:                last,
		Ask:                last,
		Last:               last,
		Scale:              scale,
		ReceivedAt:         receivedAt.UTC(),
		SourceTS:           &sourceTS,
		FreshnessBasisKind: FreshnessBasisSourceTimestamp,
		FreshnessBasisAt:   sourceTS,
	}, nil
}

func parsePythLatest(value pythLatestValue) (fixedpoint.Value, fixedpoint.Value, time.Time, error) {
	price, err := parsePythFixedPoint(value.Price, value.Expo)
	if err != nil {
		return fixedpoint.Value{}, fixedpoint.Value{}, time.Time{}, err
	}
	conf, err := parsePythFixedPoint(value.Conf, value.Expo)
	if err != nil {
		return fixedpoint.Value{}, fixedpoint.Value{}, time.Time{}, err
	}
	return price, conf, time.Unix(value.PublishTime, 0).UTC(), nil
}

func parsePythFixedPoint(raw string, expo int32) (fixedpoint.Value, error) {
	input := strings.TrimSpace(raw)
	if input == "" {
		return fixedpoint.Value{}, fmt.Errorf("empty pyth numeric value")
	}

	sign := ""
	switch input[0] {
	case '-', '+':
		sign = string(input[0])
		input = input[1:]
	}
	if input == "" {
		return fixedpoint.Value{}, fmt.Errorf("invalid pyth numeric value")
	}

	switch {
	case expo >= 0:
		return fixedpoint.ParseDecimal(sign + input + strings.Repeat("0", int(expo)))
	case -expo > fixedpoint.MaxScale:
		return fixedpoint.Value{}, fmt.Errorf("pyth exponent %d exceeds supported scale", expo)
	default:
		scale := int(-expo)
		if len(input) <= scale {
			input = strings.Repeat("0", scale-len(input)+1) + input
		}
		index := len(input) - scale
		return fixedpoint.ParseDecimal(sign + input[:index] + "." + input[index:])
	}
}

func quoteScale(live []liveContract) int32 {
	var scale int32
	for _, current := range live {
		scale = max(scale, max(current.price.Scale, current.conf.Scale))
	}
	return scale
}

func weightedValue(scale int32, live []liveContract, pick func(liveContract) fixedpoint.Value) (fixedpoint.Value, error) {
	numerator := big.NewInt(0)
	for _, current := range live {
		value, err := fixedpoint.Normalize(pick(current), scale)
		if err != nil {
			return fixedpoint.Value{}, err
		}
		term := new(big.Int).Mul(big.NewInt(value.Int), big.NewInt(current.weightBPS))
		numerator.Add(numerator, term)
	}

	rounded := fixedpoint.MustDivRoundHalfAwayFromZero(numerator, big.NewInt(fixedpoint.BasisPointsDenominator))
	if rounded.Cmp(big.NewInt(math.MinInt64)) < 0 || rounded.Cmp(big.NewInt(math.MaxInt64)) > 0 {
		return fixedpoint.Value{}, fmt.Errorf("overflow")
	}
	return fixedpoint.Value{Int: rounded.Int64(), Scale: scale}, nil
}

func renormalizeLiveContracts(live []liveContract) ([]liveContract, error) {
	if len(live) == 1 {
		live[0].weightBPS = fixedpoint.BasisPointsDenominator
		return live, nil
	}

	total := int64(0)
	for _, current := range live {
		total += current.weightBPS
	}
	if total <= 0 {
		return nil, fmt.Errorf("no live contract weight")
	}

	firstWeight := fixedpoint.MustRoundRatioHalfAway(live[0].weightBPS*fixedpoint.BasisPointsDenominator, total)
	live[0].weightBPS = firstWeight
	live[1].weightBPS = fixedpoint.BasisPointsDenominator - firstWeight
	return live, nil
}

func validateConfidence(price fixedpoint.Value, conf fixedpoint.Value, maxConfidenceBPS int64) error {
	normalizedPrice, normalizedConf, err := normalizePairForCompare(price, conf)
	if err != nil {
		return err
	}
	if normalizedPrice.Int == 0 {
		return fmt.Errorf("price is zero")
	}

	absPrice := normalizedPrice.Int
	if absPrice < 0 {
		absPrice = -absPrice
	}
	absConf := normalizedConf.Int
	if absConf < 0 {
		absConf = -absConf
	}

	actual := fixedpoint.MustRoundRatioHalfAway(absConf*fixedpoint.BasisPointsDenominator, absPrice)
	if actual > maxConfidenceBPS {
		return fmt.Errorf("confidence %d bps exceeds %d", actual, maxConfidenceBPS)
	}
	return nil
}

func normalizePairForCompare(left fixedpoint.Value, right fixedpoint.Value) (fixedpoint.Value, fixedpoint.Value, error) {
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

func earliestPublishTime(live []liveContract) time.Time {
	earliest := live[0].publishTime.UTC()
	for _, current := range live[1:] {
		if current.publishTime.Before(earliest) {
			earliest = current.publishTime.UTC()
		}
	}
	return earliest
}
