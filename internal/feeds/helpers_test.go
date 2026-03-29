package feeds

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/lumosimmo/market-relayer-go/internal/config"
	"github.com/lumosimmo/market-relayer-go/internal/fixedpoint"
)

func TestFeedHelperBranches(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 3, 27, 15, 0, 0, 0, time.UTC)

	t.Run("feed errors format unwrap and report retryability", func(t *testing.T) {
		t.Parallel()

		var nilErr *Error
		if got := nilErr.Error(); got != "<nil>" {
			t.Fatalf("(*Error)(nil).Error() = %q, want <nil>", got)
		}
		if nilErr.Unwrap() != nil {
			t.Fatal("(*Error)(nil).Unwrap() != nil")
		}

		baseErr := errors.New("boom")
		cooldownUntil := now.Add(time.Minute)
		err := &Error{
			Source:        "coinbase",
			Kind:          ErrorCooldown,
			CooldownUntil: &cooldownUntil,
			Err:           baseErr,
		}
		if got := err.Error(); got == "" || !IsRetryable(transportError("coinbase", context.DeadlineExceeded)) {
			t.Fatalf("Error() = %q, retryable transport = false", got)
		}
		if !errors.Is(err, baseErr) {
			t.Fatalf("errors.Is(err, baseErr) = false, want true")
		}
		if IsRetryable(err) {
			t.Fatal("IsRetryable(cooldown error) = true, want false")
		}
	})

	t.Run("normalize quote validates required fields positivity and timestamp mode", func(t *testing.T) {
		t.Parallel()

		_, err := NormalizeQuote(QuoteInput{}, ValidationOptions{})
		if err == nil {
			t.Fatal("NormalizeQuote(empty) error = nil, want validation error")
		}

		_, err = NormalizeQuote(QuoteInput{
			Source:     "coinbase",
			Symbol:     "ETHUSD",
			Bid:        "-1",
			Ask:        "1",
			Last:       "1",
			ReceivedAt: now,
		}, ValidationOptions{})
		if err == nil {
			t.Fatal("NormalizeQuote(non-positive) error = nil, want validation error")
		}

		_, err = NormalizeQuote(QuoteInput{
			Source:     "coinbase",
			Symbol:     "ETHUSD",
			Bid:        "2",
			Ask:        "1",
			Last:       "1",
			ReceivedAt: now,
		}, ValidationOptions{})
		if err == nil {
			t.Fatal("NormalizeQuote(bid > ask) error = nil, want validation error")
		}

		_, err = NormalizeQuote(QuoteInput{
			Source:        "coinbase",
			Symbol:        "ETHUSD",
			Bid:           "1",
			Ask:           "1",
			Last:          "1",
			ReceivedAt:    now,
			TimestampMode: config.TimestampModeSourceTimestamp,
		}, ValidationOptions{})
		if err == nil {
			t.Fatal("NormalizeQuote(missing source timestamp) error = nil, want validation error")
		}

		_, err = NormalizeQuote(QuoteInput{
			Source:        "coinbase",
			Symbol:        "ETHUSD",
			Bid:           "1",
			Ask:           "1",
			Last:          "1",
			ReceivedAt:    now,
			TimestampMode: config.TimestampMode("bad"),
		}, ValidationOptions{})
		if err == nil {
			t.Fatal("NormalizeQuote(invalid timestamp mode) error = nil, want validation error")
		}
	})

	t.Run("normalize quote success and source constructors preserve endpoint shape", func(t *testing.T) {
		t.Parallel()

		sourceTS := now.Add(-time.Second)
		quote, err := NormalizeQuote(QuoteInput{
			Source:        " coinbase ",
			Symbol:        " ETHUSD ",
			Bid:           "1981.8",
			Ask:           "1982.0",
			Last:          "1981.9",
			ReceivedAt:    now.In(time.FixedZone("CET", 3600)),
			SourceTS:      &sourceTS,
			TimestampMode: config.TimestampModeSourceTimestamp,
		}, ValidationOptions{
			StalenessThreshold: time.Minute,
			MaxFutureSkew:      2 * time.Second,
		})
		if err != nil {
			t.Fatalf("NormalizeQuote(success) error = %v", err)
		}
		if quote.Source != "coinbase" || quote.Symbol != "ETHUSD" {
			t.Fatalf("NormalizeQuote() identifiers = {%q %q}, want {coinbase ETHUSD}", quote.Source, quote.Symbol)
		}
		if quote.FreshnessBasisKind != FreshnessBasisSourceTimestamp || !quote.ReceivedAt.Equal(now.UTC()) {
			t.Fatalf("NormalizeQuote() freshness = {%q %s}, want {source_ts %s}", quote.FreshnessBasisKind, quote.ReceivedAt, now.UTC())
		}

		coinbaseSource, err := NewCoinbaseSource("ETHUSD", config.SourceConfig{
			Name:           "coinbase",
			Kind:           "coinbase",
			ProductID:      "ETH-USD",
			RequestTimeout: time.Second,
			TimestampMode:  config.TimestampModeSourceTimestamp,
		}, HTTPSourceOptions{})
		if err != nil {
			t.Fatalf("NewCoinbaseSource() error = %v", err)
		}
		if got := coinbaseSource.(*httpSource).requestURL; !strings.Contains(got, defaultCoinbaseBaseURL+"/products/ETH-USD/ticker") {
			t.Fatalf("NewCoinbaseSource() request URL = %q, want default coinbase endpoint", got)
		}

		krakenSource, err := NewKrakenSource("ETHUSD", config.SourceConfig{
			Name:            "kraken",
			Kind:            "kraken",
			Pair:            "ETH/USD",
			RequestTimeout:  time.Second,
			TimestampMode:   config.TimestampModeReceivedAt,
			MaxUnchangedAge: 10 * time.Second,
		}, HTTPSourceOptions{})
		if err != nil {
			t.Fatalf("NewKrakenSource() error = %v", err)
		}
		if got := krakenSource.(*httpSource).requestURL; !strings.Contains(got, defaultKrakenBaseURL+"/0/public/Ticker?pair=ETH%2FUSD") {
			t.Fatalf("NewKrakenSource() request URL = %q, want default kraken endpoint", got)
		}
	})

	t.Run("source helpers cover construction budget cooldown and retry parsing branches", func(t *testing.T) {
		t.Parallel()

		if _, err := NewSource("ETHUSD", config.SourceConfig{Kind: "bad"}, HTTPSourceOptions{}); err == nil {
			t.Fatal("NewSource(unsupported) error = nil, want unsupported kind")
		}
		if _, err := newHTTPSource("", config.SourceConfig{Name: "coinbase"}, "http://example.com", HTTPSourceOptions{}, nil); err == nil {
			t.Fatal("newHTTPSource(blank symbol) error = nil, want validation error")
		}
		if _, err := newHTTPSource("ETHUSD", config.SourceConfig{}, "http://example.com", HTTPSourceOptions{}, nil); err == nil {
			t.Fatal("newHTTPSource(blank name) error = nil, want validation error")
		}

		clock := &mutableClock{now: now}
		sourceIface, err := newHTTPSource("ETHUSD", config.SourceConfig{
			Name:            "coinbase",
			Kind:            "coinbase",
			ProductID:       "ETH-USD",
			RequestTimeout:  time.Second,
			TimestampMode:   config.TimestampModeSourceTimestamp,
			MaxUnchangedAge: 2 * time.Second,
		}, "http://example.com", HTTPSourceOptions{
			Clock:          clock,
			CooldownJitter: noJitter,
		}, func(receivedAt time.Time, body []byte) (QuoteInput, error) {
			return QuoteInput{}, nil
		})
		if err != nil {
			t.Fatalf("newHTTPSource() error = %v", err)
		}
		source := sourceIface.(*httpSource)
		if source.requester == nil || source.requester.client == nil || source.requester.clock == nil || source.requester.cooldownJitter == nil {
			t.Fatal("newHTTPSource() did not populate default helpers")
		}

		if !source.requester.canRetry(FetchBudget{}) {
			t.Fatal("canRetry(no deadline) = false, want true")
		}
		if timeout, err := source.requester.requestTimeout(FetchBudget{}); err != nil || timeout != time.Second {
			t.Fatalf("requestTimeout(no deadline) = (%s, %v), want (1s, nil)", timeout, err)
		}

		deadline := now.Add(250 * time.Millisecond)
		if timeout, err := source.requester.requestTimeout(FetchBudget{Deadline: deadline}); err != nil || timeout != 250*time.Millisecond {
			t.Fatalf("requestTimeout(short remaining) = (%s, %v), want (250ms, nil)", timeout, err)
		}
		if source.requester.canRetry(FetchBudget{Deadline: deadline}) {
			t.Fatal("canRetry(short budget) = true, want false")
		}

		clock.Set(now.Add(2 * time.Second))
		if _, err := source.requester.requestTimeout(FetchBudget{Deadline: now.Add(time.Second)}); err == nil {
			t.Fatal("requestTimeout(expired budget) error = nil, want timeout error")
		}
		source.requester.timeout = 0
		if _, err := source.requester.requestTimeout(FetchBudget{}); err == nil {
			t.Fatal("requestTimeout(non-positive timeout) error = nil, want invalid quote error")
		}
		source.requester.timeout = time.Second

		clock.Set(now)
		cooldownUntil := source.requester.applyCooldown(now, -time.Second)
		if !cooldownUntil.Equal(now) {
			t.Fatalf("applyCooldown(negative) = %s, want %s", cooldownUntil, now)
		}
		cooldownUntil = source.requester.applyCooldown(now, time.Second)
		if got, active := source.requester.activeCooldown(now.Add(500 * time.Millisecond)); !active || !got.Equal(cooldownUntil) {
			t.Fatalf("activeCooldown() = (%s, %t), want (%s, true)", got, active, cooldownUntil)
		}
		if _, active := source.requester.activeCooldown(now.Add(2 * time.Second)); active {
			t.Fatal("activeCooldown(expired) = true, want false")
		}

		quote := Quote{
			Source:     "coinbase",
			Symbol:     "ETHUSD",
			Bid:        mustFeedDecimal(t, "1981.75"),
			Ask:        mustFeedDecimal(t, "1981.75"),
			Last:       mustFeedDecimal(t, "1981.75"),
			Scale:      2,
			ReceivedAt: now,
		}
		if err := source.guardUnchangedQuote(quote); err != nil {
			t.Fatalf("guardUnchangedQuote(first) error = %v", err)
		}
		quote.ReceivedAt = now.Add(3 * time.Second)
		if err := source.guardUnchangedQuote(quote); err == nil {
			t.Fatal("guardUnchangedQuote(frozen) error = nil, want frozen quote")
		}

		if got := parseRetryAfter("", now, time.Second); got != time.Second {
			t.Fatalf("parseRetryAfter(empty) = %s, want 1s", got)
		}
		if got := parseRetryAfter("7", now, time.Second); got != 7*time.Second {
			t.Fatalf("parseRetryAfter(seconds) = %s, want 7s", got)
		}
		if got := parseRetryAfter("not-a-date", now, time.Second); got != time.Second {
			t.Fatalf("parseRetryAfter(invalid date string) = %s, want fallback 1s", got)
		}
		pastHeader := now.Add(-time.Minute).Format(http.TimeFormat)
		if got := parseRetryAfter(pastHeader, now, time.Second); got != 0 {
			t.Fatalf("parseRetryAfter(past) = %s, want 0", got)
		}

		if got := defaultCooldownJitter(0); got != 0 {
			t.Fatalf("defaultCooldownJitter(0) = %s, want 0", got)
		}
		if got := defaultCooldownJitter(100 * time.Millisecond); got < 0 || got > 10*time.Millisecond {
			t.Fatalf("defaultCooldownJitter(100ms) = %s, want range [0,10ms]", got)
		}

		if _, err := resolveURL("://bad", "/ticker", nil); err == nil {
			t.Fatal("resolveURL(invalid base) error = nil, want parse error")
		}
		resolved, err := resolveURL("https://example.com/base", "/ticker", url.Values{"pair": []string{"ETH/USD"}})
		if err != nil {
			t.Fatalf("resolveURL() error = %v", err)
		}
		if resolved != "https://example.com/ticker?pair=ETH%2FUSD" {
			t.Fatalf("resolveURL() = %q, want resolved absolute URL", resolved)
		}
	})
}

func mustFeedDecimal(t *testing.T, raw string) fixedpoint.Value {
	t.Helper()

	value, err := fixedpoint.ParseDecimal(raw)
	if err != nil {
		t.Fatalf("fixedpoint.ParseDecimal(%q) error = %v", raw, err)
	}
	return value
}
