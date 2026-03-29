package fixedpoint

import (
	"testing"
)

func TestParseDecimal(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   string
		want    Value
		wantErr bool
	}{
		{
			name:  "integer",
			input: "123",
			want:  Value{Int: 123, Scale: 0},
		},
		{
			name:  "negative with trailing zeroes",
			input: "-1.2300",
			want:  Value{Int: -12300, Scale: 4},
		},
		{
			name:  "zero",
			input: "0.000",
			want:  Value{Int: 0, Scale: 3},
		},
		{
			name:    "blank",
			input:   "",
			wantErr: true,
		},
		{
			name:    "invalid character",
			input:   "12a.3",
			wantErr: true,
		},
		{
			name:    "duplicate decimal separator",
			input:   "1.2.3",
			wantErr: true,
		},
		{
			name:    "too many fractional digits",
			input:   "0.1234567890123456789",
			wantErr: true,
		},
		{
			name:    "overflow",
			input:   "9223372036854775808",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := ParseDecimal(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("ParseDecimal(%q) error = nil, want error", tt.input)
				}
				return
			}

			if err != nil {
				t.Fatalf("ParseDecimal(%q) error = %v", tt.input, err)
			}

			if got != tt.want {
				t.Fatalf("ParseDecimal(%q) = %+v, want %+v", tt.input, got, tt.want)
			}
		})
	}
}

func TestNormalize(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		input       Value
		targetScale int32
		want        Value
		wantErr     bool
	}{
		{
			name:        "upscale",
			input:       Value{Int: 123, Scale: 2},
			targetScale: 4,
			want:        Value{Int: 12300, Scale: 4},
		},
		{
			name:        "downscale rounds half away from zero",
			input:       Value{Int: 12350, Scale: 4},
			targetScale: 2,
			want:        Value{Int: 124, Scale: 2},
		},
		{
			name:        "downscale negative rounds half away from zero",
			input:       Value{Int: -12350, Scale: 4},
			targetScale: 2,
			want:        Value{Int: -124, Scale: 2},
		},
		{
			name:        "invalid target scale",
			input:       Value{Int: 1, Scale: 0},
			targetScale: 19,
			wantErr:     true,
		},
		{
			name:        "upscale overflow",
			input:       Value{Int: 922337203685477581, Scale: 0},
			targetScale: 1,
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := Normalize(tt.input, tt.targetScale)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("Normalize(%+v, %d) error = nil, want error", tt.input, tt.targetScale)
				}
				return
			}

			if err != nil {
				t.Fatalf("Normalize(%+v, %d) error = %v", tt.input, tt.targetScale, err)
			}

			if got != tt.want {
				t.Fatalf("Normalize(%+v, %d) = %+v, want %+v", tt.input, tt.targetScale, got, tt.want)
			}
		})
	}
}

func TestMidpoint(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		left        Value
		right       Value
		targetScale int32
		want        Value
		wantErr     bool
	}{
		{
			name:        "same scale",
			left:        Value{Int: 101, Scale: 2},
			right:       Value{Int: 102, Scale: 2},
			targetScale: 2,
			want:        Value{Int: 102, Scale: 2},
		},
		{
			name:        "mixed scale",
			left:        Value{Int: 120, Scale: 2},
			right:       Value{Int: 1234, Scale: 3},
			targetScale: 3,
			want:        Value{Int: 1217, Scale: 3},
		},
		{
			name:        "overflow",
			left:        Value{Int: 9223372036854775807, Scale: 0},
			right:       Value{Int: 9223372036854775807, Scale: 0},
			targetScale: 0,
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := Midpoint(tt.left, tt.right, tt.targetScale)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("Midpoint(%+v, %+v, %d) error = nil, want error", tt.left, tt.right, tt.targetScale)
				}
				return
			}

			if err != nil {
				t.Fatalf("Midpoint(%+v, %+v, %d) error = %v", tt.left, tt.right, tt.targetScale, err)
			}

			if got != tt.want {
				t.Fatalf("Midpoint(%+v, %+v, %d) = %+v, want %+v", tt.left, tt.right, tt.targetScale, got, tt.want)
			}
		})
	}
}

func TestMedian(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		values      []Value
		targetScale int32
		want        Value
		wantErr     bool
	}{
		{
			name: "odd count",
			values: []Value{
				{Int: 103, Scale: 2},
				{Int: 101, Scale: 2},
				{Int: 102, Scale: 2},
			},
			targetScale: 2,
			want:        Value{Int: 102, Scale: 2},
		},
		{
			name: "even count uses midpoint rounding",
			values: []Value{
				{Int: 100, Scale: 2},
				{Int: 101, Scale: 2},
				{Int: 102, Scale: 2},
				{Int: 103, Scale: 2},
			},
			targetScale: 2,
			want:        Value{Int: 102, Scale: 2},
		},
		{
			name:        "empty input",
			values:      nil,
			targetScale: 2,
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := Median(tt.values, tt.targetScale)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("Median(%v, %d) error = nil, want error", tt.values, tt.targetScale)
				}
				return
			}

			if err != nil {
				t.Fatalf("Median(%v, %d) error = %v", tt.values, tt.targetScale, err)
			}

			if got != tt.want {
				t.Fatalf("Median(%v, %d) = %+v, want %+v", tt.values, tt.targetScale, got, tt.want)
			}
		})
	}
}

func TestClampDelta(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		base      Value
		candidate Value
		maxBPS    int64
		want      Value
		wantErr   bool
	}{
		{
			name:      "clamps upward move",
			base:      Value{Int: 10000, Scale: 2},
			candidate: Value{Int: 10200, Scale: 2},
			maxBPS:    50,
			want:      Value{Int: 10050, Scale: 2},
		},
		{
			name:      "clamps downward move",
			base:      Value{Int: 10000, Scale: 2},
			candidate: Value{Int: 9800, Scale: 2},
			maxBPS:    50,
			want:      Value{Int: 9950, Scale: 2},
		},
		{
			name:      "rejects negative bps",
			base:      Value{Int: 10000, Scale: 2},
			candidate: Value{Int: 10100, Scale: 2},
			maxBPS:    -1,
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := ClampDelta(tt.base, tt.candidate, tt.maxBPS)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("ClampDelta(%+v, %+v, %d) error = nil, want error", tt.base, tt.candidate, tt.maxBPS)
				}
				return
			}

			if err != nil {
				t.Fatalf("ClampDelta(%+v, %+v, %d) error = %v", tt.base, tt.candidate, tt.maxBPS, err)
			}

			if got != tt.want {
				t.Fatalf("ClampDelta(%+v, %+v, %d) = %+v, want %+v", tt.base, tt.candidate, tt.maxBPS, got, tt.want)
			}
		})
	}
}

func TestSmooth(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		base      Value
		target    Value
		weightBPS int64
		want      Value
		wantErr   bool
	}{
		{
			name:      "quarter weight",
			base:      Value{Int: 10000, Scale: 2},
			target:    Value{Int: 10400, Scale: 2},
			weightBPS: 2500,
			want:      Value{Int: 10100, Scale: 2},
		},
		{
			name:      "full weight",
			base:      Value{Int: 10000, Scale: 2},
			target:    Value{Int: 10400, Scale: 2},
			weightBPS: 10000,
			want:      Value{Int: 10400, Scale: 2},
		},
		{
			name:      "invalid weight",
			base:      Value{Int: 10000, Scale: 2},
			target:    Value{Int: 10400, Scale: 2},
			weightBPS: 10001,
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := Smooth(tt.base, tt.target, tt.weightBPS)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("Smooth(%+v, %+v, %d) error = nil, want error", tt.base, tt.target, tt.weightBPS)
				}
				return
			}

			if err != nil {
				t.Fatalf("Smooth(%+v, %+v, %d) error = %v", tt.base, tt.target, tt.weightBPS, err)
			}

			if got != tt.want {
				t.Fatalf("Smooth(%+v, %+v, %d) = %+v, want %+v", tt.base, tt.target, tt.weightBPS, got, tt.want)
			}
		})
	}
}
