package sessions

import (
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/lumosimmo/market-relayer-go/internal/config"
	"github.com/lumosimmo/market-relayer-go/internal/fixedpoint"
	"github.com/lumosimmo/market-relayer-go/internal/pricing"
	"github.com/lumosimmo/market-relayer-go/internal/status"
)

type Window struct {
	State  status.SessionState `json:"state"`
	Reason status.Reason       `json:"reason"`
}

type WindowFunc func(time.Time) Window

type TrackerOptions struct {
	Logger *slog.Logger
	Window WindowFunc
}

type CorrelationIDs struct {
	CycleID   string
	RequestID string
}

type Tracker struct {
	market config.MarketConfig
	logger *slog.Logger
	window WindowFunc

	lastState status.SessionState
}

func OpenWindow() Window {
	return Window{
		State:  status.SessionStateOpen,
		Reason: status.ReasonNone,
	}
}

func ClosedWindow(reason status.Reason) Window {
	if reason == "" || reason == status.ReasonNone {
		reason = status.ReasonSessionClosed
	}

	return Window{
		State:  status.SessionStateClosed,
		Reason: reason,
	}
}

func ExternalStaleWindow(reason status.Reason) Window {
	if reason == "" || reason == status.ReasonNone {
		reason = status.ReasonSourceStale
	}

	return Window{
		State:  status.SessionStateExternalStale,
		Reason: reason,
	}
}

func NewTracker(market config.MarketConfig, options TrackerOptions) (*Tracker, error) {
	if err := validateMarket(market, options.Window); err != nil {
		return nil, err
	}

	window := options.Window
	if window == nil {
		window = defaultWindow
	}

	return &Tracker{
		market: market,
		logger: options.Logger,
		window: window,
	}, nil
}

func RestoreState(at time.Time, persisted pricing.State) pricing.State {
	if !canRestoreState(at, persisted) {
		return pricing.State{}
	}

	return cloneState(persisted)
}

func Apply(result pricing.Result, prior pricing.State, window Window) pricing.Result {
	runtime := classify(result, prior, normalizeWindow(window))
	result.SessionState = runtime.State
	result.SessionReason = runtime.Reason

	if runtime.State == status.SessionStateClosed {
		result.State = cloneState(prior)
		result.External = unavailablePrice(result.PublishScale, status.ReasonSessionClosed)
		result.Oracle = unavailablePrice(result.PublishScale, status.ReasonSessionClosed)
		result.OraclePlusBasis = unavailablePrice(result.PublishScale, status.ReasonSessionClosed)
	}

	return result
}

func (tracker *Tracker) RestoreState(at time.Time, persisted pricing.State) pricing.State {
	return RestoreState(at, persisted)
}

func (tracker *Tracker) Apply(result pricing.Result, prior pricing.State, ids CorrelationIDs) pricing.Result {
	evaluated := Apply(result, prior, tracker.window(result.ComputedAt))

	priorState := tracker.lastState
	if priorState != evaluated.SessionState {
		tracker.logTransition(priorState, evaluated.SessionState, evaluated.SessionReason, ids)
	}
	tracker.lastState = evaluated.SessionState

	return evaluated
}

func (tracker *Tracker) logTransition(priorState status.SessionState, nextState status.SessionState, cause status.Reason, ids CorrelationIDs) {
	if tracker.logger == nil {
		return
	}

	tracker.logger.Info(
		"session transition",
		"market", tracker.market.Symbol,
		"prior_state", sessionStateLabel(priorState),
		"state", nextState,
		"cause", cause,
		"cycle_id", strings.TrimSpace(ids.CycleID),
		"request_id", strings.TrimSpace(ids.RequestID),
	)
}

func classify(result pricing.Result, prior pricing.State, window Window) Window {
	switch {
	case window.State == status.SessionStateClosed:
		return window
	case window.State == status.SessionStateExternalStale:
		return window
	case fallbackOnly(result, prior):
		return Window{
			State:  status.SessionStateFallbackOnly,
			Reason: status.ReasonFallbackActive,
		}
	case result.External.Reason == status.ReasonSessionClosed:
		return Window{
			State:  status.SessionStateExternalStale,
			Reason: status.ReasonSessionClosed,
		}
	case result.External.Reason == status.ReasonSourceStale:
		return ExternalStaleWindow(status.ReasonSourceStale)
	case result.Oracle.Reason == status.ReasonFallbackExpired:
		return ExternalStaleWindow(status.ReasonFallbackExpired)
	case result.Oracle.Available() || result.External.Available():
		return OpenWindow()
	default:
		return ExternalStaleWindow(status.ReasonSourceStale)
	}
}

func normalizeWindow(window Window) Window {
	switch window.State {
	case "":
		return OpenWindow()
	case status.SessionStateOpen:
		window.Reason = defaultReason(window.Reason, status.ReasonNone)
	case status.SessionStateExternalStale:
		window.Reason = defaultReason(window.Reason, status.ReasonSourceStale)
	case status.SessionStateClosed:
		window.Reason = defaultReason(window.Reason, status.ReasonSessionClosed)
	default:
		return OpenWindow()
	}

	return window
}

func fallbackOnly(result pricing.Result, prior pricing.State) bool {
	if prior.LastOracle == nil || prior.OracleFallbackExpiresAt.IsZero() {
		return false
	}
	if result.ComputedAt.After(prior.OracleFallbackExpiresAt) {
		return false
	}
	return result.Oracle.Status == status.StatusDegraded && result.Oracle.Reason == status.ReasonFallbackActive
}

func sessionStateLabel(state status.SessionState) string {
	if state == "" {
		return "unknown"
	}
	return string(state)
}

func validateMarket(market config.MarketConfig, window WindowFunc) error {
	if market.SessionMode == config.SessionModeScheduled && window == nil {
		return fmt.Errorf("sessions: market %s session_mode %q requires a real window function", market.Symbol, market.SessionMode)
	}
	return nil
}

func defaultWindow(time.Time) Window {
	return OpenWindow()
}

func canRestoreState(at time.Time, state pricing.State) bool {
	switch {
	case at.IsZero():
		return false
	case state.LastOracle == nil:
		return false
	case state.FreshnessBasisAt.IsZero(), state.OracleFallbackExpiresAt.IsZero():
		return false
	case state.OracleFallbackExpiresAt.Before(state.FreshnessBasisAt):
		return false
	default:
		return true
	}
}

func defaultReason(reason status.Reason, fallback status.Reason) status.Reason {
	if reason == "" || reason == status.ReasonNone {
		return fallback
	}
	return reason
}

func cloneState(input pricing.State) pricing.State {
	return pricing.State{
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

func unavailablePrice(scale int32, reason status.Reason) pricing.Price {
	return pricing.Price{
		Status: status.StatusUnavailable,
		Reason: reason,
		Scale:  scale,
	}
}
