package feeds

import (
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/lumosimmo/market-relayer-go/internal/status"
)

type ErrorKind string

const (
	ErrorInvalidQuote     ErrorKind = "invalid_quote"
	ErrorMalformedPayload ErrorKind = "malformed_payload"
	ErrorTransport        ErrorKind = "transport"
	ErrorTimeout          ErrorKind = "timeout"
	ErrorHTTPStatus       ErrorKind = "http_status"
	ErrorRateLimited      ErrorKind = "rate_limited"
	ErrorCooldown         ErrorKind = "cooldown"
	ErrorStaleQuote       ErrorKind = "stale_quote"
	ErrorFutureQuote      ErrorKind = "future_quote"
	ErrorFrozenQuote      ErrorKind = "frozen_quote"
)

type Error struct {
	Source        string
	Kind          ErrorKind
	Retryable     bool
	Reason        status.Reason
	StatusCode    int
	CooldownUntil *time.Time
	Err           error
}

func (err *Error) Error() string {
	if err == nil {
		return "<nil>"
	}

	message := fmt.Sprintf("feeds: %s %s", err.Source, err.Kind)
	switch {
	case err.StatusCode != 0:
		message = fmt.Sprintf("%s (status %d)", message, err.StatusCode)
	case err.CooldownUntil != nil:
		message = fmt.Sprintf("%s until %s", message, err.CooldownUntil.UTC().Format(time.RFC3339))
	}
	if err.Err != nil {
		message = fmt.Sprintf("%s: %v", message, err.Err)
	}
	return message
}

func (err *Error) Unwrap() error {
	if err == nil {
		return nil
	}
	return err.Err
}

func IsRetryable(err error) bool {
	var fetchErr *Error
	if !errors.As(err, &fetchErr) {
		return false
	}
	return fetchErr.Retryable
}

func invalidQuoteError(source string, message string, cause error) *Error {
	err := errors.New(message)
	if cause != nil {
		err = fmt.Errorf("%v: %w", message, cause)
	}
	return &Error{
		Source:    source,
		Kind:      ErrorInvalidQuote,
		Reason:    status.ReasonSourceRejected,
		Retryable: false,
		Err:       err,
	}
}

func malformedPayloadError(source string, cause error) *Error {
	return &Error{
		Source:    source,
		Kind:      ErrorMalformedPayload,
		Reason:    status.ReasonSourceRejected,
		Retryable: false,
		Err:       cause,
	}
}

func staleQuoteError(source string, age time.Duration) *Error {
	return &Error{
		Source:    source,
		Kind:      ErrorStaleQuote,
		Reason:    status.ReasonSourceStale,
		Retryable: false,
		Err:       fmt.Errorf("source timestamp age %s exceeds staleness threshold", age),
	}
}

func futureQuoteError(source string, skew time.Duration) *Error {
	return &Error{
		Source:    source,
		Kind:      ErrorFutureQuote,
		Reason:    status.ReasonSourceRejected,
		Retryable: false,
		Err:       fmt.Errorf("source timestamp is %s in the future", skew),
	}
}

func frozenQuoteError(source string, unchangedFor time.Duration) *Error {
	return &Error{
		Source:    source,
		Kind:      ErrorFrozenQuote,
		Reason:    status.ReasonSourceFrozen,
		Retryable: false,
		Err:       fmt.Errorf("quote unchanged for %s", unchangedFor),
	}
}

func timeoutError(source string, cause error) *Error {
	return &Error{
		Source:    source,
		Kind:      ErrorTimeout,
		Reason:    status.ReasonSourceRejected,
		Retryable: true,
		Err:       cause,
	}
}

func transportError(source string, cause error) *Error {
	return &Error{
		Source:    source,
		Kind:      ErrorTransport,
		Reason:    status.ReasonSourceRejected,
		Retryable: true,
		Err:       cause,
	}
}

func httpStatusError(source string, statusCode int, retryable bool) *Error {
	return &Error{
		Source:     source,
		Kind:       ErrorHTTPStatus,
		Reason:     status.ReasonSourceRejected,
		Retryable:  retryable,
		StatusCode: statusCode,
		Err:        fmt.Errorf("unexpected HTTP status %s", http.StatusText(statusCode)),
	}
}

func rateLimitError(source string, cooldownUntil time.Time, cause error) *Error {
	return &Error{
		Source:        source,
		Kind:          ErrorRateLimited,
		Reason:        status.ReasonSourceRejected,
		Retryable:     false,
		StatusCode:    http.StatusTooManyRequests,
		CooldownUntil: &cooldownUntil,
		Err:           cause,
	}
}

func cooldownError(source string, cooldownUntil time.Time) *Error {
	return &Error{
		Source:        source,
		Kind:          ErrorCooldown,
		Reason:        status.ReasonSourceRejected,
		Retryable:     false,
		CooldownUntil: &cooldownUntil,
		Err:           errors.New("source is cooling down after a rate limit"),
	}
}
