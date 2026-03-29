package feeds

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/lumosimmo/market-relayer-go/internal/clock"
	"github.com/lumosimmo/market-relayer-go/internal/config"
)

type httpRequester struct {
	sourceName     string
	timeout        time.Duration
	client         *http.Client
	clock          clock.Clock
	cooldownJitter func(time.Duration) time.Duration

	mu            sync.Mutex
	cooldownUntil time.Time
}

func newHTTPRequester(source config.SourceConfig, options HTTPSourceOptions) *httpRequester {
	client := options.Client
	if client == nil {
		client = http.DefaultClient
	}

	serviceClock := options.Clock
	if serviceClock == nil {
		serviceClock = clock.Real{}
	}

	cooldownJitter := options.CooldownJitter
	if cooldownJitter == nil {
		cooldownJitter = defaultCooldownJitter
	}

	return &httpRequester{
		sourceName:     source.Name,
		timeout:        source.RequestTimeout,
		client:         client,
		clock:          serviceClock,
		cooldownJitter: cooldownJitter,
	}
}

func (requester *httpRequester) get(ctx context.Context, budget FetchBudget, requestURL string) ([]byte, time.Time, error) {
	return requester.do(ctx, budget, http.MethodGet, requestURL, "", nil)
}

func (requester *httpRequester) postJSON(ctx context.Context, budget FetchBudget, requestURL string, body []byte) ([]byte, time.Time, error) {
	return requester.do(ctx, budget, http.MethodPost, requestURL, "application/json", body)
}

func (requester *httpRequester) do(ctx context.Context, budget FetchBudget, method string, requestURL string, contentType string, body []byte) ([]byte, time.Time, error) {
	now := requester.clock.Now().UTC()
	if cooldownUntil, active := requester.activeCooldown(now); active {
		return nil, time.Time{}, cooldownError(requester.sourceName, cooldownUntil)
	}

	requestTimeout, err := requester.requestTimeout(budget)
	if err != nil {
		return nil, time.Time{}, err
	}

	requestCtx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()

	request, err := http.NewRequestWithContext(requestCtx, method, requestURL, bytes.NewReader(body))
	if err != nil {
		return nil, time.Time{}, transportError(requester.sourceName, err)
	}
	if contentType != "" {
		request.Header.Set("Content-Type", contentType)
	}

	response, err := requester.client.Do(request)
	if err != nil {
		if isTimeoutError(err) || errors.Is(requestCtx.Err(), context.DeadlineExceeded) {
			return nil, time.Time{}, timeoutError(requester.sourceName, err)
		}
		return nil, time.Time{}, transportError(requester.sourceName, err)
	}
	defer response.Body.Close()

	receivedAt := requester.clock.Now().UTC()
	if response.StatusCode == http.StatusTooManyRequests {
		cooldownUntil := requester.applyCooldown(receivedAt, parseRetryAfter(response.Header.Get("Retry-After"), receivedAt, requester.timeout))
		return nil, time.Time{}, rateLimitError(requester.sourceName, cooldownUntil, errors.New("received HTTP 429"))
	}
	if response.StatusCode >= http.StatusInternalServerError {
		return nil, time.Time{}, httpStatusError(requester.sourceName, response.StatusCode, true)
	}
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return nil, time.Time{}, httpStatusError(requester.sourceName, response.StatusCode, false)
	}

	body, err = io.ReadAll(response.Body)
	if err != nil {
		return nil, time.Time{}, transportError(requester.sourceName, err)
	}
	return body, receivedAt, nil
}

func (requester *httpRequester) canRetry(budget FetchBudget) bool {
	if budget.Deadline.IsZero() {
		return true
	}
	return requester.remainingBudget(budget) >= requester.timeout
}

func (requester *httpRequester) requestTimeout(budget FetchBudget) (time.Duration, error) {
	if requester.timeout <= 0 {
		return 0, invalidQuoteError(requester.sourceName, "request timeout must be positive", nil)
	}
	if budget.Deadline.IsZero() {
		return requester.timeout, nil
	}

	remaining := requester.remainingBudget(budget)
	if remaining <= 0 {
		return 0, timeoutError(requester.sourceName, context.DeadlineExceeded)
	}
	if remaining < requester.timeout {
		return remaining, nil
	}

	return requester.timeout, nil
}

func (requester *httpRequester) remainingBudget(budget FetchBudget) time.Duration {
	return budget.Deadline.Sub(requester.clock.Now().UTC())
}

func (requester *httpRequester) activeCooldown(now time.Time) (time.Time, bool) {
	requester.mu.Lock()
	defer requester.mu.Unlock()

	if requester.cooldownUntil.IsZero() || !now.Before(requester.cooldownUntil) {
		return time.Time{}, false
	}
	return requester.cooldownUntil, true
}

func (requester *httpRequester) applyCooldown(now time.Time, base time.Duration) time.Time {
	if base < 0 {
		base = 0
	}

	requester.mu.Lock()
	defer requester.mu.Unlock()

	cooldownUntil := now.Add(base + requester.cooldownJitter(base))
	if cooldownUntil.After(requester.cooldownUntil) {
		requester.cooldownUntil = cooldownUntil
	}
	return requester.cooldownUntil
}

func parseRetryAfter(header string, now time.Time, fallback time.Duration) time.Duration {
	value := strings.TrimSpace(header)
	if value == "" {
		return fallback
	}

	if seconds, err := strconv.Atoi(value); err == nil {
		return time.Duration(seconds) * time.Second
	}

	if deadline, err := http.ParseTime(value); err == nil {
		if deadline.Before(now) {
			return 0
		}
		return deadline.Sub(now)
	}

	return fallback
}

func isTimeoutError(err error) bool {
	type timeout interface {
		Timeout() bool
	}

	if err == nil {
		return false
	}

	var timeoutErr timeout
	if errors.As(err, &timeoutErr) && timeoutErr.Timeout() {
		return true
	}

	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}

func defaultCooldownJitter(base time.Duration) time.Duration {
	if base <= 0 {
		return 0
	}

	jitterLimit := base / 10
	if jitterLimit <= 0 {
		return 0
	}

	return time.Duration(rand.Int64N(int64(jitterLimit) + 1))
}
