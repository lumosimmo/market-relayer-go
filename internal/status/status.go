package status

type Status string

const (
	StatusUnknown     Status = "unknown"
	StatusAvailable   Status = "available"
	StatusDegraded    Status = "degraded"
	StatusUnavailable Status = "unavailable"
)

func (status Status) Valid() bool {
	switch status {
	case StatusUnknown, StatusAvailable, StatusDegraded, StatusUnavailable:
		return true
	default:
		return false
	}
}

type Reason string

const (
	ReasonNone                           Reason = "none"
	ReasonConfigInvalid                  Reason = "config_invalid"
	ReasonMissingConfig                  Reason = "missing_config"
	ReasonDuplicateMarket                Reason = "duplicate_market"
	ReasonUnknownSource                  Reason = "unknown_source"
	ReasonDeprecatedAlias                Reason = "deprecated_alias"
	ReasonTimestamplessPrimaryNotAllowed Reason = "timestampless_primary_not_allowed"
	ReasonSourceRejected                 Reason = "source_rejected"
	ReasonSourceDivergence               Reason = "source_divergence"
	ReasonSourceStale                    Reason = "source_stale"
	ReasonSourceFrozen                   Reason = "source_frozen"
	ReasonFallbackActive                 Reason = "fallback_active"
	ReasonFallbackExpired                Reason = "fallback_expired"
	ReasonColdStart                      Reason = "cold_start"
	ReasonSessionClosed                  Reason = "session_closed"
	ReasonStoreUnavailable               Reason = "store_unavailable"
	ReasonSinkUnavailable                Reason = "sink_unavailable"
	ReasonUnpublishable                  Reason = "unpublishable"
)

func (reason Reason) Valid() bool {
	switch reason {
	case ReasonNone,
		ReasonConfigInvalid,
		ReasonMissingConfig,
		ReasonDuplicateMarket,
		ReasonUnknownSource,
		ReasonDeprecatedAlias,
		ReasonTimestamplessPrimaryNotAllowed,
		ReasonSourceRejected,
		ReasonSourceDivergence,
		ReasonSourceStale,
		ReasonSourceFrozen,
		ReasonFallbackActive,
		ReasonFallbackExpired,
		ReasonColdStart,
		ReasonSessionClosed,
		ReasonStoreUnavailable,
		ReasonSinkUnavailable,
		ReasonUnpublishable:
		return true
	default:
		return false
	}
}

type SessionState string

const (
	SessionStateOpen          SessionState = "open"
	SessionStateExternalStale SessionState = "external_stale"
	SessionStateFallbackOnly  SessionState = "fallback_only"
	SessionStateClosed        SessionState = "closed"
)

func (state SessionState) Valid() bool {
	switch state {
	case SessionStateOpen, SessionStateExternalStale, SessionStateFallbackOnly, SessionStateClosed:
		return true
	default:
		return false
	}
}

type Condition struct {
	Status Status `json:"status"`
	Reason Reason `json:"reason"`
}

func NewCondition(status Status, reason Reason) Condition {
	return Condition{
		Status: status,
		Reason: reason,
	}
}
