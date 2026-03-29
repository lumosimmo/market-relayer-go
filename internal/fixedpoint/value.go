package fixedpoint

import (
	"errors"
	"fmt"
	"math"
	"math/big"
	"slices"
	"strings"
)

const (
	MaxScale               int32 = 18
	BasisPointsDenominator int64 = 10_000
)

const GlobalRoundingMode = "half_away_from_zero"

var (
	errEmptyInput      = errors.New("fixedpoint: empty decimal input")
	errInvalidFormat   = errors.New("fixedpoint: invalid decimal format")
	errScaleOutOfRange = errors.New("fixedpoint: scale must be between 0 and 18")
	errOverflow        = errors.New("fixedpoint: overflow")
)

var pow10 = [...]int64{
	1,
	10,
	100,
	1_000,
	10_000,
	100_000,
	1_000_000,
	10_000_000,
	100_000_000,
	1_000_000_000,
	10_000_000_000,
	100_000_000_000,
	1_000_000_000_000,
	10_000_000_000_000,
	100_000_000_000_000,
	1_000_000_000_000_000,
	10_000_000_000_000_000,
	100_000_000_000_000_000,
	1_000_000_000_000_000_000,
}

var (
	minInt64 = big.NewInt(math.MinInt64)
	maxInt64 = big.NewInt(math.MaxInt64)
	oneBig   = big.NewInt(1)
	twoBig   = big.NewInt(2)
	bpsDenom = big.NewInt(BasisPointsDenominator)
)

type Value struct {
	Int   int64 `json:"int"`
	Scale int32 `json:"scale"`
}

func ParseDecimal(input string) (Value, error) {
	if input == "" {
		return Value{}, errEmptyInput
	}

	raw := strings.TrimSpace(input)
	if raw == "" {
		return Value{}, errEmptyInput
	}

	sign := int64(1)
	switch raw[0] {
	case '-':
		sign = -1
		raw = raw[1:]
	case '+':
		raw = raw[1:]
	}

	if raw == "" {
		return Value{}, errInvalidFormat
	}

	parts := strings.Split(raw, ".")
	if len(parts) > 2 {
		return Value{}, errInvalidFormat
	}

	intPart := parts[0]
	fracPart := ""
	if len(parts) == 2 {
		fracPart = parts[1]
	}

	if intPart == "" {
		intPart = "0"
	}

	if !digitsOnly(intPart) || !digitsOnly(fracPart) {
		return Value{}, errInvalidFormat
	}

	scale := int32(len(fracPart))
	if err := validateScale(scale); err != nil {
		return Value{}, err
	}

	combined := strings.TrimLeft(intPart+fracPart, "0")
	if combined == "" {
		return Value{Int: 0, Scale: scale}, nil
	}

	value, ok := new(big.Int).SetString(combined, 10)
	if !ok {
		return Value{}, errInvalidFormat
	}

	if sign < 0 {
		value.Neg(value)
	}

	intValue, err := bigIntToInt64(value)
	if err != nil {
		return Value{}, err
	}

	return Value{Int: intValue, Scale: scale}, nil
}

func Normalize(value Value, targetScale int32) (Value, error) {
	if err := validateScale(value.Scale); err != nil {
		return Value{}, err
	}
	if err := validateScale(targetScale); err != nil {
		return Value{}, err
	}

	if value.Scale == targetScale {
		return value, nil
	}

	diff := targetScale - value.Scale
	if diff > 0 {
		scaled := new(big.Int).Mul(big.NewInt(value.Int), scaleFactor(diff))
		intValue, err := bigIntToInt64(scaled)
		if err != nil {
			return Value{}, err
		}
		return Value{Int: intValue, Scale: targetScale}, nil
	}

	rounded := divRoundHalfAwayFromZero(big.NewInt(value.Int), scaleFactor(-diff))
	intValue, err := bigIntToInt64(rounded)
	if err != nil {
		return Value{}, err
	}

	return Value{Int: intValue, Scale: targetScale}, nil
}

func Midpoint(left, right Value, targetScale int32) (Value, error) {
	lhs, rhs, err := normalizePair(left, right, targetScale)
	if err != nil {
		return Value{}, err
	}

	sum, err := checkedAddInt64(lhs.Int, rhs.Int)
	if err != nil {
		return Value{}, err
	}

	rounded := divRoundHalfAwayFromZero(big.NewInt(sum), twoBig)
	intValue, err := bigIntToInt64(rounded)
	if err != nil {
		return Value{}, err
	}

	return Value{Int: intValue, Scale: targetScale}, nil
}

func Median(values []Value, targetScale int32) (Value, error) {
	if len(values) == 0 {
		return Value{}, errors.New("fixedpoint: median requires at least one value")
	}

	normalized := make([]Value, len(values))
	for i, value := range values {
		scaled, err := Normalize(value, targetScale)
		if err != nil {
			return Value{}, err
		}
		normalized[i] = scaled
	}

	slices.SortFunc(normalized, func(a, b Value) int {
		switch {
		case a.Int < b.Int:
			return -1
		case a.Int > b.Int:
			return 1
		default:
			return 0
		}
	})

	middle := len(normalized) / 2
	if len(normalized)%2 == 1 {
		return normalized[middle], nil
	}

	return Midpoint(normalized[middle-1], normalized[middle], targetScale)
}

func ClampDelta(base, candidate Value, maxBPS int64) (Value, error) {
	if maxBPS < 0 {
		return Value{}, fmt.Errorf("fixedpoint: clamp bps must be non-negative: %d", maxBPS)
	}

	targetScale := max(base.Scale, candidate.Scale)
	normalizedBase, normalizedCandidate, err := normalizePair(base, candidate, targetScale)
	if err != nil {
		return Value{}, err
	}

	absoluteBase, err := absInt64Exact(normalizedBase.Int)
	if err != nil {
		return Value{}, err
	}

	limit, err := percentageAmount(absoluteBase, maxBPS)
	if err != nil {
		return Value{}, err
	}

	lower, err := checkedAddInt64(normalizedBase.Int, -limit)
	if err != nil {
		return Value{}, err
	}
	upper, err := checkedAddInt64(normalizedBase.Int, limit)
	if err != nil {
		return Value{}, err
	}

	clamped := normalizedCandidate.Int
	if clamped < lower {
		clamped = lower
	}
	if clamped > upper {
		clamped = upper
	}

	return Value{Int: clamped, Scale: targetScale}, nil
}

func Smooth(base, target Value, weightBPS int64) (Value, error) {
	if weightBPS < 0 || weightBPS > BasisPointsDenominator {
		return Value{}, fmt.Errorf("fixedpoint: smoothing bps must be between 0 and %d: %d", BasisPointsDenominator, weightBPS)
	}

	targetScale := max(base.Scale, target.Scale)
	normalizedBase, normalizedTarget, err := normalizePair(base, target, targetScale)
	if err != nil {
		return Value{}, err
	}

	delta, err := checkedSubInt64(normalizedTarget.Int, normalizedBase.Int)
	if err != nil {
		return Value{}, err
	}

	move, err := percentageSigned(delta, weightBPS)
	if err != nil {
		return Value{}, err
	}

	smoothed, err := checkedAddInt64(normalizedBase.Int, move)
	if err != nil {
		return Value{}, err
	}

	return Value{Int: smoothed, Scale: targetScale}, nil
}

func validateScale(scale int32) error {
	if scale < 0 || scale > MaxScale {
		return errScaleOutOfRange
	}
	return nil
}

func normalizePair(left, right Value, targetScale int32) (Value, Value, error) {
	lhs, err := Normalize(left, targetScale)
	if err != nil {
		return Value{}, Value{}, err
	}

	rhs, err := Normalize(right, targetScale)
	if err != nil {
		return Value{}, Value{}, err
	}

	return lhs, rhs, nil
}

func digitsOnly(input string) bool {
	for _, r := range input {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

func bigIntToInt64(value *big.Int) (int64, error) {
	if value.Cmp(minInt64) < 0 || value.Cmp(maxInt64) > 0 {
		return 0, errOverflow
	}
	return value.Int64(), nil
}

func percentageAmount(base, bps int64) (int64, error) {
	return percentageBPS(base, bps)
}

func percentageSigned(base, bps int64) (int64, error) {
	return percentageBPS(base, bps)
}

func percentageBPS(base, bps int64) (int64, error) {
	numerator := new(big.Int).Mul(big.NewInt(base), big.NewInt(bps))
	rounded := divRoundHalfAwayFromZero(numerator, bpsDenom)
	return bigIntToInt64(rounded)
}

func checkedAddInt64(left, right int64) (int64, error) {
	sum := new(big.Int).Add(big.NewInt(left), big.NewInt(right))
	return bigIntToInt64(sum)
}

func checkedSubInt64(left, right int64) (int64, error) {
	diff := new(big.Int).Sub(big.NewInt(left), big.NewInt(right))
	return bigIntToInt64(diff)
}

func absInt64Exact(value int64) (int64, error) {
	if value >= 0 {
		return value, nil
	}
	if value == math.MinInt64 {
		return 0, errOverflow
	}
	return -value, nil
}

func scaleFactor(scale int32) *big.Int {
	return big.NewInt(pow10[scale])
}
