package sessions

import (
	"testing"
	"time"

	"github.com/lumosimmo/market-relayer-go/internal/config"
	"github.com/lumosimmo/market-relayer-go/internal/fixedpoint"
	"github.com/lumosimmo/market-relayer-go/internal/pricing"
	"github.com/lumosimmo/market-relayer-go/internal/status"
)

func TestRestoreStateUsesOracleFallbackWindow(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 3, 26, 17, 0, 0, 0, time.UTC)
	persisted := pricing.State{
		LastExternal:            sessionValuePtr(t, "1981.75"),
		LastOracle:              sessionValuePtr(t, "1982.25"),
		LastOraclePlusBasis:     sessionValuePtr(t, "1983.00"),
		FreshnessBasisAt:        now.Add(-5 * time.Second),
		LastExternalAt:          now.Add(-5 * time.Second),
		LastOracleAt:            now.Add(-5 * time.Second),
		LastOraclePlusBasisAt:   now.Add(-5 * time.Second),
		OracleFallbackExpiresAt: now.Add(25 * time.Second),
	}

	got := RestoreState(now, persisted)
	assertSessionValue(t, got.LastOracle, persisted.LastOracle)
	assertSessionValue(t, got.LastOraclePlusBasis, persisted.LastOraclePlusBasis)

	expired := RestoreState(persisted.OracleFallbackExpiresAt.Add(time.Second), persisted)
	assertSessionValue(t, expired.LastOracle, persisted.LastOracle)
	assertSessionValue(t, expired.LastOraclePlusBasis, persisted.LastOraclePlusBasis)
	if !expired.OracleFallbackExpiresAt.Equal(persisted.OracleFallbackExpiresAt) {
		t.Fatalf("expired.OracleFallbackExpiresAt = %s, want %s", expired.OracleFallbackExpiresAt, persisted.OracleFallbackExpiresAt)
	}
}

func TestApplyPreservesInternalPricesDuringExternalStaleWindow(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 3, 26, 17, 30, 0, 0, time.UTC)
	prior := pricing.State{
		LastExternal:            sessionValuePtr(t, "1981.75"),
		LastOracle:              sessionValuePtr(t, "1981.85"),
		LastOraclePlusBasis:     sessionValuePtr(t, "1982.15"),
		LastOracleAt:            now.Add(-time.Minute),
		OracleFallbackExpiresAt: now.Add(time.Minute),
	}
	result := pricing.Result{
		ComputedAt:      now,
		PublishScale:    2,
		External:        priced(status.StatusDegraded, status.ReasonSessionClosed, "1981.75"),
		Oracle:          priced(status.StatusAvailable, status.ReasonNone, "1981.95"),
		OraclePlusBasis: priced(status.StatusAvailable, status.ReasonNone, "1982.30"),
		State:           prior,
	}

	applied := Apply(result, prior, ExternalStaleWindow(status.ReasonSessionClosed))
	if got, want := applied.SessionState, status.SessionStateExternalStale; got != want {
		t.Fatalf("SessionState = %q, want %q", got, want)
	}
	assertSessionPrice(t, applied.External, status.StatusDegraded, status.ReasonSessionClosed, "1981.75")
	assertSessionPrice(t, applied.Oracle, status.StatusAvailable, status.ReasonNone, "1981.95")
	assertSessionPrice(t, applied.OraclePlusBasis, status.StatusAvailable, status.ReasonNone, "1982.30")
}

func TestApplyClosedWindowBlanksAllPublishedPrices(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 3, 26, 18, 0, 0, 0, time.UTC)
	prior := pricing.State{
		LastExternal:            sessionValuePtr(t, "1981.75"),
		LastOracle:              sessionValuePtr(t, "1981.85"),
		LastOraclePlusBasis:     sessionValuePtr(t, "1982.15"),
		LastOracleAt:            now.Add(-time.Minute),
		OracleFallbackExpiresAt: now.Add(time.Minute),
	}
	applied := Apply(pricing.Result{
		ComputedAt:      now,
		PublishScale:    2,
		External:        priced(status.StatusAvailable, status.ReasonNone, "1981.75"),
		Oracle:          priced(status.StatusAvailable, status.ReasonNone, "1981.85"),
		OraclePlusBasis: priced(status.StatusAvailable, status.ReasonNone, "1982.15"),
	}, prior, ClosedWindow(status.ReasonSessionClosed))

	assertSessionPrice(t, applied.External, status.StatusUnavailable, status.ReasonSessionClosed, "")
	assertSessionPrice(t, applied.Oracle, status.StatusUnavailable, status.ReasonSessionClosed, "")
	assertSessionPrice(t, applied.OraclePlusBasis, status.StatusUnavailable, status.ReasonSessionClosed, "")
	assertSessionValue(t, applied.State.LastOracle, prior.LastOracle)
}

func TestTrackerAndClassifyUseNewExternalStaleSemantics(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 3, 26, 18, 30, 0, 0, time.UTC)
	market := sessionMarket()
	tracker, err := NewTracker(market, TrackerOptions{
		Window: func(at time.Time) Window {
			if at.Before(now.Add(time.Minute)) {
				return OpenWindow()
			}
			return ExternalStaleWindow(status.ReasonSessionClosed)
		},
	})
	if err != nil {
		t.Fatalf("NewTracker() error = %v", err)
	}

	open := tracker.Apply(pricing.Result{
		ComputedAt:      now,
		PublishScale:    2,
		External:        priced(status.StatusAvailable, status.ReasonNone, "1981.75"),
		Oracle:          priced(status.StatusAvailable, status.ReasonNone, "1981.75"),
		OraclePlusBasis: priced(status.StatusAvailable, status.ReasonNone, "1981.85"),
	}, pricing.State{}, CorrelationIDs{})
	if got, want := open.SessionState, status.SessionStateOpen; got != want {
		t.Fatalf("open.SessionState = %q, want %q", got, want)
	}

	stale := tracker.Apply(pricing.Result{
		ComputedAt:      now.Add(2 * time.Minute),
		PublishScale:    2,
		External:        priced(status.StatusDegraded, status.ReasonSessionClosed, "1981.75"),
		Oracle:          priced(status.StatusAvailable, status.ReasonNone, "1981.85"),
		OraclePlusBasis: priced(status.StatusAvailable, status.ReasonNone, "1982.05"),
	}, pricing.State{}, CorrelationIDs{})
	if got, want := stale.SessionState, status.SessionStateExternalStale; got != want {
		t.Fatalf("stale.SessionState = %q, want %q", got, want)
	}

	fallback := classify(pricing.Result{
		ComputedAt: now,
		Oracle:     priced(status.StatusDegraded, status.ReasonFallbackActive, "1981.75"),
	}, pricing.State{
		LastOracle:              sessionValuePtr(t, "1981.75"),
		OracleFallbackExpiresAt: now.Add(time.Minute),
	}, OpenWindow())
	if got, want := fallback.State, status.SessionStateFallbackOnly; got != want {
		t.Fatalf("classify(fallback).State = %q, want %q", got, want)
	}
}

func TestWindowHelpersAndValidation(t *testing.T) {
	t.Parallel()

	if got := ClosedWindow(status.ReasonNone); got.Reason != status.ReasonSessionClosed {
		t.Fatalf("ClosedWindow(default).Reason = %q, want %q", got.Reason, status.ReasonSessionClosed)
	}
	if got := ExternalStaleWindow(status.ReasonNone); got.Reason != status.ReasonSourceStale {
		t.Fatalf("ExternalStaleWindow(default).Reason = %q, want %q", got.Reason, status.ReasonSourceStale)
	}
	if got := normalizeWindow(Window{State: "broken"}); got.State != status.SessionStateOpen {
		t.Fatalf("normalizeWindow(invalid).State = %q, want %q", got.State, status.SessionStateOpen)
	}

	scheduled := sessionMarket()
	scheduled.SessionMode = config.SessionModeScheduled
	if _, err := NewTracker(scheduled, TrackerOptions{}); err == nil {
		t.Fatal("NewTracker(scheduled without window) error = nil, want validation failure")
	}
}

func sessionMarket() config.MarketConfig {
	return config.MarketConfig{
		Symbol:      "ETHUSD",
		SessionMode: config.SessionModeAlwaysOpen,
	}
}

func priced(state status.Status, reason status.Reason, raw string) pricing.Price {
	price := pricing.Price{
		Status: state,
		Reason: reason,
		Scale:  2,
	}
	if raw == "" {
		return price
	}
	value, err := fixedpoint.ParseDecimal(raw)
	if err != nil {
		panic(err)
	}
	price.Value = &value
	return price
}

func sessionValuePtr(t *testing.T, raw string) *fixedpoint.Value {
	t.Helper()

	value, err := fixedpoint.ParseDecimal(raw)
	if err != nil {
		t.Fatalf("ParseDecimal(%q) error = %v", raw, err)
	}
	return &value
}

func assertSessionValue(t *testing.T, got *fixedpoint.Value, want *fixedpoint.Value) {
	t.Helper()

	if got == nil || want == nil {
		if got != want {
			t.Fatalf("value = %v, want %v", got, want)
		}
		return
	}
	if *got != *want {
		t.Fatalf("value = %+v, want %+v", *got, *want)
	}
}

func assertSessionPrice(t *testing.T, got pricing.Price, wantStatus status.Status, wantReason status.Reason, wantRaw string) {
	t.Helper()

	if got.Status != wantStatus || got.Reason != wantReason {
		t.Fatalf("price = {%q %q}, want {%q %q}", got.Status, got.Reason, wantStatus, wantReason)
	}
	if wantRaw == "" {
		if got.Value != nil {
			t.Fatalf("price.Value = %+v, want nil", *got.Value)
		}
		return
	}
	want, err := fixedpoint.ParseDecimal(wantRaw)
	if err != nil {
		t.Fatalf("ParseDecimal(%q) error = %v", wantRaw, err)
	}
	assertSessionValue(t, got.Value, &want)
}
