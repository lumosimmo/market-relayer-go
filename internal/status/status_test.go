package status

import "testing"

func TestStatusValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		value Status
		valid bool
	}{
		{name: "unknown", value: StatusUnknown, valid: true},
		{name: "available", value: StatusAvailable, valid: true},
		{name: "degraded", value: StatusDegraded, valid: true},
		{name: "unavailable", value: StatusUnavailable, valid: true},
		{name: "invalid", value: Status("bad"), valid: false},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := tt.value.Valid(); got != tt.valid {
				t.Fatalf("Status(%q).Valid() = %v, want %v", tt.value, got, tt.valid)
			}
		})
	}
}

func TestReasonValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		value Reason
		valid bool
	}{
		{name: "none", value: ReasonNone, valid: true},
		{name: "fallback active", value: ReasonFallbackActive, valid: true},
		{name: "source divergence", value: ReasonSourceDivergence, valid: true},
		{name: "cold start", value: ReasonColdStart, valid: true},
		{name: "source stale", value: ReasonSourceStale, valid: true},
		{name: "timestampless primary not allowed", value: ReasonTimestamplessPrimaryNotAllowed, valid: true},
		{name: "invalid", value: Reason("bad"), valid: false},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := tt.value.Valid(); got != tt.valid {
				t.Fatalf("Reason(%q).Valid() = %v, want %v", tt.value, got, tt.valid)
			}
		})
	}
}

func TestSessionStateValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		value SessionState
		valid bool
	}{
		{name: "open", value: SessionStateOpen, valid: true},
		{name: "external stale", value: SessionStateExternalStale, valid: true},
		{name: "fallback only", value: SessionStateFallbackOnly, valid: true},
		{name: "closed", value: SessionStateClosed, valid: true},
		{name: "invalid", value: SessionState("bad"), valid: false},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := tt.value.Valid(); got != tt.valid {
				t.Fatalf("SessionState(%q).Valid() = %v, want %v", tt.value, got, tt.valid)
			}
		})
	}
}
