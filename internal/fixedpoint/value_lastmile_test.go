package fixedpoint

import (
	"errors"
	"math"
	"math/big"
	"testing"
)

func TestFixedPointLastMileBranches(t *testing.T) {
	t.Parallel()

	t.Run("parse decimal median and absolute value cover remaining non-error paths", func(t *testing.T) {
		t.Parallel()

		value, err := ParseDecimal(".5")
		if err != nil {
			t.Fatalf("ParseDecimal(.5) error = %v", err)
		}
		if value != (Value{Int: 5, Scale: 1}) {
			t.Fatalf("ParseDecimal(.5) = %+v, want {5 1}", value)
		}

		median, err := Median([]Value{
			{Int: 2, Scale: 0},
			{Int: 1, Scale: 0},
			{Int: 2, Scale: 0},
		}, 0)
		if err != nil {
			t.Fatalf("Median(duplicates) error = %v", err)
		}
		if median != (Value{Int: 2, Scale: 0}) {
			t.Fatalf("Median(duplicates) = %+v, want {2 0}", median)
		}

		absolute, err := absInt64Exact(-7)
		if err != nil {
			t.Fatalf("absInt64Exact(-7) error = %v", err)
		}
		if absolute != 7 {
			t.Fatalf("absInt64Exact(-7) = %d, want 7", absolute)
		}
	})

	t.Run("clamp delta surfaces normalization percentage and bound overflow errors", func(t *testing.T) {
		t.Parallel()

		if _, err := ClampDelta(Value{Int: 1, Scale: 0}, Value{Int: 1, Scale: -1}, 1); !errors.Is(err, errScaleOutOfRange) {
			t.Fatalf("ClampDelta(invalid candidate scale) error = %v, want %v", err, errScaleOutOfRange)
		}

		if _, err := ClampDelta(Value{Int: math.MaxInt64, Scale: 0}, Value{Int: 0, Scale: 0}, math.MaxInt64); !errors.Is(err, errOverflow) {
			t.Fatalf("ClampDelta(percentage overflow) error = %v, want %v", err, errOverflow)
		}

		if _, err := ClampDelta(Value{Int: math.MinInt64 + 1, Scale: 0}, Value{Int: math.MinInt64 + 1, Scale: 0}, BasisPointsDenominator); !errors.Is(err, errOverflow) {
			t.Fatalf("ClampDelta(lower bound overflow) error = %v, want %v", err, errOverflow)
		}

		if _, err := ClampDelta(Value{Int: math.MaxInt64, Scale: 0}, Value{Int: math.MaxInt64, Scale: 0}, 1); !errors.Is(err, errOverflow) {
			t.Fatalf("ClampDelta(upper bound overflow) error = %v, want %v", err, errOverflow)
		}
	})

	t.Run("smooth and normalize pair surface remaining validation errors", func(t *testing.T) {
		t.Parallel()

		if _, err := Smooth(Value{Int: 1, Scale: 0}, Value{Int: 1, Scale: -1}, 100); !errors.Is(err, errScaleOutOfRange) {
			t.Fatalf("Smooth(invalid target scale) error = %v, want %v", err, errScaleOutOfRange)
		}

		if _, _, err := normalizePair(Value{Int: 1, Scale: 0}, Value{Int: 1, Scale: -1}, 0); !errors.Is(err, errScaleOutOfRange) {
			t.Fatalf("normalizePair(invalid right scale) error = %v, want %v", err, errScaleOutOfRange)
		}
	})

	t.Run("div round panics for non-positive divisors", func(t *testing.T) {
		t.Parallel()

		defer func() {
			if recovered := recover(); recovered == nil {
				t.Fatal("divRoundHalfAwayFromZero(divisor=0) panic = nil, want panic")
			}
		}()
		_ = divRoundHalfAwayFromZero(big.NewInt(1), big.NewInt(0))
	})

	t.Run("exported rounding helpers preserve half-away behavior", func(t *testing.T) {
		t.Parallel()

		if got := MustRoundRatioHalfAway(-5, 2); got != -3 {
			t.Fatalf("MustRoundRatioHalfAway(-5, 2) = %d, want -3", got)
		}
		if got := MustDivRoundHalfAwayFromZero(big.NewInt(5), big.NewInt(2)); got.Int64() != 3 {
			t.Fatalf("MustDivRoundHalfAwayFromZero(5, 2) = %s, want 3", got.String())
		}

		defer func() {
			if recovered := recover(); recovered == nil {
				t.Fatal("MustRoundRatioHalfAway(denominator=0) panic = nil, want panic")
			}
		}()
		_ = MustRoundRatioHalfAway(1, 0)
	})
}
