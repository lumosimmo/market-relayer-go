package feeds

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/lumosimmo/market-relayer-go/internal/config"
)

func TestFeedErrorAndNormalizeLastMileBranches(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 3, 27, 18, 0, 0, 0, time.UTC)

	t.Run("feed error formatting includes status and retryability rejects non-feed errors", func(t *testing.T) {
		t.Parallel()

		err := httpStatusError("coinbase", http.StatusBadGateway, true)
		if got := err.Error(); !strings.Contains(got, "status 502") {
			t.Fatalf("Error() = %q, want status code text", got)
		}
		if !IsRetryable(err) {
			t.Fatal("IsRetryable(http status retryable) = false, want true")
		}
		if IsRetryable(errors.New("boom")) {
			t.Fatal("IsRetryable(non-feed error) = true, want false")
		}
	})

	t.Run("normalize quote validates symbol timestamps parsing and normalization edges", func(t *testing.T) {
		t.Parallel()

		if _, err := NormalizeQuote(QuoteInput{
			Source:     "coinbase",
			Bid:        "1",
			Ask:        "1",
			Last:       "1",
			ReceivedAt: now,
		}, ValidationOptions{}); err == nil {
			t.Fatal("NormalizeQuote(missing symbol) error = nil, want validation error")
		}

		if _, err := NormalizeQuote(QuoteInput{
			Source: "coinbase",
			Symbol: "ETHUSD",
			Bid:    "1",
			Ask:    "1",
			Last:   "1",
		}, ValidationOptions{}); err == nil {
			t.Fatal("NormalizeQuote(missing received_at) error = nil, want validation error")
		}

		if _, err := NormalizeQuote(QuoteInput{
			Source:     "coinbase",
			Symbol:     "ETHUSD",
			Bid:        "1",
			Ask:        "bad",
			Last:       "1",
			ReceivedAt: now,
		}, ValidationOptions{}); err == nil {
			t.Fatal("NormalizeQuote(invalid ask) error = nil, want validation error")
		}

		if _, err := NormalizeQuote(QuoteInput{
			Source:     "coinbase",
			Symbol:     "ETHUSD",
			Bid:        "1",
			Ask:        "1",
			Last:       "bad",
			ReceivedAt: now,
		}, ValidationOptions{}); err == nil {
			t.Fatal("NormalizeQuote(invalid last) error = nil, want validation error")
		}

		if _, err := NormalizeQuote(QuoteInput{
			Source:     "coinbase",
			Symbol:     "ETHUSD",
			Bid:        "1.123456789012345678",
			Ask:        "9223372036854775807",
			Last:       "1",
			ReceivedAt: now,
		}, ValidationOptions{}); err == nil {
			t.Fatal("NormalizeQuote(ask normalize overflow) error = nil, want validation error")
		}

		if _, err := NormalizeQuote(QuoteInput{
			Source:     "coinbase",
			Symbol:     "ETHUSD",
			Bid:        "1.123456789012345678",
			Ask:        "1",
			Last:       "9223372036854775807",
			ReceivedAt: now,
		}, ValidationOptions{}); err == nil {
			t.Fatal("NormalizeQuote(last normalize overflow) error = nil, want validation error")
		}
	})
}

func TestFeedSourceLastMileBranches(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 3, 27, 18, 15, 0, 0, time.UTC)

	t.Run("constructors reject invalid base urls", func(t *testing.T) {
		t.Parallel()

		if _, err := NewCoinbaseSource("ETHUSD", config.SourceConfig{
			Name:           "coinbase",
			Kind:           "coinbase",
			ProductID:      "ETH-USD",
			RequestTimeout: time.Second,
			TimestampMode:  config.TimestampModeSourceTimestamp,
		}, HTTPSourceOptions{BaseURL: "://bad"}); err == nil {
			t.Fatal("NewCoinbaseSource(invalid base url) error = nil, want failure")
		}

		if _, err := NewKrakenSource("ETHUSD", config.SourceConfig{
			Name:           "kraken",
			Kind:           "kraken",
			Pair:           "ETH/USD",
			RequestTimeout: time.Second,
			TimestampMode:  config.TimestampModeReceivedAt,
		}, HTTPSourceOptions{BaseURL: "://bad"}); err == nil {
			t.Fatal("NewKrakenSource(invalid base url) error = nil, want failure")
		}
	})

	t.Run("coinbase and kraken payload decoders reject malformed edge cases", func(t *testing.T) {
		t.Parallel()

		coinbaseServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"bid":"1981.85","ask":"1981.87","price":"1981.86","time":"not-a-time"}`))
		}))
		t.Cleanup(coinbaseServer.Close)

		coinbaseSource, err := NewCoinbaseSource("ETHUSD", config.SourceConfig{
			Name:           "coinbase",
			Kind:           "coinbase",
			ProductID:      "ETH-USD",
			RequestTimeout: time.Second,
			TimestampMode:  config.TimestampModeSourceTimestamp,
		}, HTTPSourceOptions{
			BaseURL:            coinbaseServer.URL,
			Client:             coinbaseServer.Client(),
			Clock:              &mutableClock{now: now},
			StalenessThreshold: time.Minute,
			MaxFutureSkew:      time.Second,
			CooldownJitter:     noJitter,
		})
		if err != nil {
			t.Fatalf("NewCoinbaseSource() error = %v", err)
		}
		_, err = coinbaseSource.Fetch(context.Background(), FetchBudget{Deadline: now.Add(time.Second)})
		assertFetchErrorKind(t, err, ErrorMalformedPayload)

		tests := []struct {
			name string
			body string
		}{
			{name: "malformed json", body: `{"error":[]`},
			{name: "error list", body: `{"error":["EGeneral:boom"],"result":{}}`},
			{name: "empty result", body: `{"error":[],"result":{}}`},
			{name: "missing arrays", body: `{"error":[],"result":{"XETHZUSD":{"a":[],"b":["1"],"c":["1"]}}}`},
		}

		for _, tt := range tests {
			tt := tt
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()

				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					w.Header().Set("Content-Type", "application/json")
					_, _ = w.Write([]byte(tt.body))
				}))
				t.Cleanup(server.Close)

				source, err := NewKrakenSource("ETHUSD", config.SourceConfig{
					Name:           "kraken",
					Kind:           "kraken",
					Pair:           "ETH/USD",
					RequestTimeout: time.Second,
					TimestampMode:  config.TimestampModeReceivedAt,
				}, HTTPSourceOptions{
					BaseURL:            server.URL,
					Client:             server.Client(),
					Clock:              &mutableClock{now: now},
					StalenessThreshold: time.Minute,
					MaxFutureSkew:      time.Second,
					CooldownJitter:     noJitter,
				})
				if err != nil {
					t.Fatalf("NewKrakenSource() error = %v", err)
				}
				_, err = source.Fetch(context.Background(), FetchBudget{Deadline: now.Add(time.Second)})
				assertFetchErrorKind(t, err, ErrorMalformedPayload)
			})
		}
	})

	t.Run("fetch once surfaces timeout request construction transport status and body read failures", func(t *testing.T) {
		t.Parallel()

		clock := &mutableClock{now: now}
		source := &httpSource{
			config: config.SourceConfig{
				Name:           "coinbase",
				RequestTimeout: time.Second,
				TimestampMode:  config.TimestampModeReceivedAt,
			},
			symbol:             "ETHUSD",
			requestURL:         "http://example.com",
			requester:          newHTTPRequester(config.SourceConfig{Name: "coinbase", RequestTimeout: time.Second}, HTTPSourceOptions{Clock: clock, CooldownJitter: noJitter}),
			stalenessThreshold: time.Minute,
			maxFutureSkew:      time.Second,
			decodeQuoteInput: func(receivedAt time.Time, _ []byte) (QuoteInput, error) {
				return QuoteInput{
					Source:        "coinbase",
					Symbol:        "ETHUSD",
					Bid:           "1981.85",
					Ask:           "1981.87",
					Last:          "1981.86",
					ReceivedAt:    receivedAt,
					TimestampMode: config.TimestampModeReceivedAt,
				}, nil
			},
		}

		source.requester.timeout = 0
		_, err := source.fetchOnce(context.Background(), FetchBudget{})
		assertFetchErrorKind(t, err, ErrorInvalidQuote)

		source.requester.timeout = time.Second
		source.requestURL = "://bad"
		_, err = source.fetchOnce(context.Background(), FetchBudget{Deadline: now.Add(time.Second)})
		assertFetchErrorKind(t, err, ErrorTransport)

		source.requestURL = "http://example.com"
		source.requester.client = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			return nil, errors.New("transport failed")
		})}
		_, err = source.fetchOnce(context.Background(), FetchBudget{Deadline: now.Add(time.Second)})
		assertFetchErrorKind(t, err, ErrorTransport)

		source.requester.client = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusNotFound,
				Body:       io.NopCloser(strings.NewReader("not found")),
				Header:     http.Header{},
			}, nil
		})}
		_, err = source.fetchOnce(context.Background(), FetchBudget{Deadline: now.Add(time.Second)})
		assertFetchErrorKind(t, err, ErrorHTTPStatus)

		source.requester.client = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       errReadCloser{err: errors.New("read failed")},
				Header:     http.Header{},
			}, nil
		})}
		_, err = source.fetchOnce(context.Background(), FetchBudget{Deadline: now.Add(time.Second)})
		assertFetchErrorKind(t, err, ErrorTransport)
	})

	t.Run("unchanged quote guard handles disabled and zero-since states", func(t *testing.T) {
		t.Parallel()

		quote := Quote{
			Source:     "kraken",
			Symbol:     "ETHUSD",
			Bid:        mustFeedDecimal(t, "1981.8"),
			Ask:        mustFeedDecimal(t, "1981.9"),
			Last:       mustFeedDecimal(t, "1981.7"),
			Scale:      1,
			ReceivedAt: now,
		}
		source := &httpSource{
			config: config.SourceConfig{
				Name:            "kraken",
				TimestampMode:   config.TimestampModeReceivedAt,
				MaxUnchangedAge: 0,
			},
		}
		if err := source.guardUnchangedQuote(quote); err != nil {
			t.Fatalf("guardUnchangedQuote(disabled) error = %v", err)
		}

		source.config.MaxUnchangedAge = time.Second
		source.unchangedKey = quote.StableKey()
		if err := source.guardUnchangedQuote(quote); err != nil {
			t.Fatalf("guardUnchangedQuote(zero since) error = %v", err)
		}
		if source.unchangedSince.IsZero() {
			t.Fatal("unchangedSince = zero, want initialized timestamp")
		}
	})

	t.Run("build market sources surfaces unknown sources and constructor failures", func(t *testing.T) {
		t.Parallel()

		cfg, err := config.LoadBytes([]byte(`version: 1
service:
  name: market-relayer-go
  instance_id: feed-lastmile
store:
  dsn: postgres://postgres:postgres@127.0.0.1:5432/market_relayer?sslmode=disable
api:
  listen_addr: 127.0.0.1:0
sink:
  kind: http
  url: http://127.0.0.1:18080/publish
  request_timeout: 500ms
sources:
  - name: coinbase-primary
    kind: coinbase
    product_id: ETH-USD
    request_timeout: 1s
    timestamp_mode: source_timestamp
  - name: hyperliquid-eth
    kind: hyperliquid
    market: ETH
    request_timeout: 1s
    timestamp_mode: source_timestamp
markets:
  - symbol: ETHUSD
    session_mode: always_open
    perp_book_source: hyperliquid-eth
    impact_notional_usd: "10000"
    publish_interval: 3s
    staleness_threshold: 10s
    max_fallback_age: 30s
    clamp_bps: 50
    divergence_threshold_bps: 250
    publish_scale: 2
    sources:
      - name: coinbase-primary
`), config.LoadOptions{})
		if err != nil {
			t.Fatalf("config.LoadBytes() error = %v", err)
		}
		cfg.Markets[0].Sources[0].Name = "missing"
		if _, err := BuildMarketSources(cfg, cfg.Markets[0], BuildOptions{}); err == nil {
			t.Fatal("BuildMarketSources(unknown source) error = nil, want failure")
		}

		cfg, err = config.LoadBytes([]byte(`version: 1
service:
  name: market-relayer-go
  instance_id: feed-lastmile
store:
  dsn: postgres://postgres:postgres@127.0.0.1:5432/market_relayer?sslmode=disable
api:
  listen_addr: 127.0.0.1:0
sink:
  kind: http
  url: http://127.0.0.1:18080/publish
  request_timeout: 500ms
sources:
  - name: coinbase-primary
    kind: coinbase
    product_id: ETH-USD
    request_timeout: 1s
    timestamp_mode: source_timestamp
  - name: hyperliquid-eth
    kind: hyperliquid
    market: ETH
    request_timeout: 1s
    timestamp_mode: source_timestamp
markets:
  - symbol: ETHUSD
    session_mode: always_open
    perp_book_source: hyperliquid-eth
    impact_notional_usd: "10000"
    publish_interval: 3s
    staleness_threshold: 10s
    max_fallback_age: 30s
    clamp_bps: 50
    divergence_threshold_bps: 250
    publish_scale: 2
    sources:
      - name: coinbase-primary
`), config.LoadOptions{})
		if err != nil {
			t.Fatalf("config.LoadBytes() error = %v", err)
		}
		if _, err := BuildMarketSources(cfg, cfg.Markets[0], BuildOptions{
			EndpointOverrides: map[string]string{"coinbase-primary": "://bad"},
		}); err == nil {
			t.Fatal("BuildMarketSources(bad endpoint override) error = nil, want failure")
		}
	})

	t.Run("retry-after and jitter cover remaining small-duration branches", func(t *testing.T) {
		t.Parallel()

		futureHeader := now.Add(2 * time.Second).Format(http.TimeFormat)
		if got := parseRetryAfter(futureHeader, now, time.Second); got <= 0 {
			t.Fatalf("parseRetryAfter(future header) = %s, want positive duration", got)
		}
		if got := defaultCooldownJitter(time.Nanosecond); got != 0 {
			t.Fatalf("defaultCooldownJitter(1ns) = %s, want 0", got)
		}
	})
}

func TestResolveURLHostValidation(t *testing.T) {
	t.Parallel()

	if _, err := resolveURL("http://::1", "/products/ETH-USD/ticker", nil); err == nil {
		t.Fatal("resolveURL(bare ipv6 host) error = nil, want failure")
	}

	got, err := resolveURL("http://[::1]", "/products/ETH-USD/ticker", nil)
	if err != nil {
		t.Fatalf("resolveURL(bracketed ipv6 host) error = %v", err)
	}
	if want := "http://[::1]/products/ETH-USD/ticker"; got != want {
		t.Fatalf("resolveURL(bracketed ipv6 host) = %q, want %q", got, want)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return fn(request)
}

type errReadCloser struct {
	err error
}

func (reader errReadCloser) Read([]byte) (int, error) {
	return 0, reader.err
}

func (errReadCloser) Close() error {
	return nil
}
