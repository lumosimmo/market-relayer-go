package fixedpoint

import (
	"errors"
	"math"
	"strings"
	"testing"
)

func TestFixedPointHelperBranches(t *testing.T) {
	t.Parallel()

	t.Run("parse decimal rejects malformed and overflowing values", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			input string
			want  error
		}{
			{input: "", want: errEmptyInput},
			{input: "   ", want: errEmptyInput},
			{input: "+", want: errInvalidFormat},
			{input: "-", want: errInvalidFormat},
			{input: "1.2.3", want: errInvalidFormat},
			{input: "12a", want: errInvalidFormat},
			{input: "0." + strings.Repeat("1", int(MaxScale)+1), want: errScaleOutOfRange},
			{input: "9223372036854775808", want: errOverflow},
		}

		for _, tt := range tests {
			_, err := ParseDecimal(tt.input)
			if !errors.Is(err, tt.want) {
				t.Fatalf("ParseDecimal(%q) error = %v, want %v", tt.input, err, tt.want)
			}
		}
	})

	t.Run("normalize midpoint median clamp and smooth surface validation errors", func(t *testing.T) {
		t.Parallel()

		if _, err := Normalize(Value{Int: 1, Scale: -1}, 0); !errors.Is(err, errScaleOutOfRange) {
			t.Fatalf("Normalize(invalid source scale) error = %v, want %v", err, errScaleOutOfRange)
		}
		if _, err := Normalize(Value{Int: 1, Scale: 0}, MaxScale+1); !errors.Is(err, errScaleOutOfRange) {
			t.Fatalf("Normalize(invalid target scale) error = %v, want %v", err, errScaleOutOfRange)
		}
		if _, err := Normalize(Value{Int: math.MaxInt64, Scale: 0}, 1); !errors.Is(err, errOverflow) {
			t.Fatalf("Normalize(overflow) error = %v, want %v", err, errOverflow)
		}

		if _, err := Midpoint(Value{Int: math.MaxInt64, Scale: 0}, Value{Int: 1, Scale: 0}, 0); !errors.Is(err, errOverflow) {
			t.Fatalf("Midpoint(overflow) error = %v, want %v", err, errOverflow)
		}
		if _, err := Median(nil, 0); err == nil {
			t.Fatal("Median(nil) error = nil, want validation error")
		}

		if _, err := ClampDelta(Value{Int: 1, Scale: 0}, Value{Int: 2, Scale: 0}, -1); err == nil {
			t.Fatal("ClampDelta(negative bps) error = nil, want validation error")
		}
		if _, err := ClampDelta(Value{Int: math.MinInt64, Scale: 0}, Value{Int: 0, Scale: 0}, 1); !errors.Is(err, errOverflow) {
			t.Fatalf("ClampDelta(abs overflow) error = %v, want %v", err, errOverflow)
		}

		if _, err := Smooth(Value{Int: 1, Scale: 0}, Value{Int: 2, Scale: 0}, -1); err == nil {
			t.Fatal("Smooth(negative bps) error = nil, want validation error")
		}
		if _, err := Smooth(Value{Int: 1, Scale: 0}, Value{Int: 2, Scale: 0}, BasisPointsDenominator+1); err == nil {
			t.Fatal("Smooth(weight above denominator) error = nil, want validation error")
		}
		if _, err := Smooth(Value{Int: math.MaxInt64, Scale: 0}, Value{Int: math.MinInt64, Scale: 0}, BasisPointsDenominator); !errors.Is(err, errOverflow) {
			t.Fatalf("Smooth(delta overflow) error = %v, want %v", err, errOverflow)
		}
	})

	t.Run("low level helpers handle invalid scales and exact abs overflow", func(t *testing.T) {
		t.Parallel()

		if _, err := Midpoint(Value{Int: 1, Scale: 0}, Value{Int: 2, Scale: 0}, MaxScale+1); !errors.Is(err, errScaleOutOfRange) {
			t.Fatalf("Midpoint(invalid target scale) error = %v, want %v", err, errScaleOutOfRange)
		}
		if _, err := Median([]Value{{Int: 1, Scale: MaxScale + 1}}, 0); !errors.Is(err, errScaleOutOfRange) {
			t.Fatalf("Median(invalid value scale) error = %v, want %v", err, errScaleOutOfRange)
		}
		if _, _, err := normalizePair(Value{Int: 1, Scale: 0}, Value{Int: 1, Scale: 0}, MaxScale+1); !errors.Is(err, errScaleOutOfRange) {
			t.Fatalf("normalizePair(invalid target scale) error = %v, want %v", err, errScaleOutOfRange)
		}
		if _, err := absInt64Exact(math.MinInt64); !errors.Is(err, errOverflow) {
			t.Fatalf("absInt64Exact(min int64) error = %v, want %v", err, errOverflow)
		}
	})

	t.Run("clamp and smooth respect zero and full-weight boundaries", func(t *testing.T) {
		t.Parallel()

		clamped, err := ClampDelta(Value{Int: 10_000, Scale: 2}, Value{Int: 20_000, Scale: 2}, 100)
		if err != nil {
			t.Fatalf("ClampDelta(upper clamp) error = %v", err)
		}
		if clamped != (Value{Int: 10_100, Scale: 2}) {
			t.Fatalf("ClampDelta(upper clamp) = %+v, want {10100 2}", clamped)
		}

		smoothed, err := Smooth(Value{Int: 10_000, Scale: 2}, Value{Int: 20_000, Scale: 2}, 0)
		if err != nil {
			t.Fatalf("Smooth(weight 0) error = %v", err)
		}
		if smoothed != (Value{Int: 10_000, Scale: 2}) {
			t.Fatalf("Smooth(weight 0) = %+v, want {10000 2}", smoothed)
		}

		smoothed, err = Smooth(Value{Int: 10_000, Scale: 2}, Value{Int: 20_000, Scale: 2}, BasisPointsDenominator)
		if err != nil {
			t.Fatalf("Smooth(full weight) error = %v", err)
		}
		if smoothed != (Value{Int: 20_000, Scale: 2}) {
			t.Fatalf("Smooth(full weight) = %+v, want {20000 2}", smoothed)
		}
	})
}
