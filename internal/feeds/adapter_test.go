package feeds

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lumosimmo/market-relayer-go/internal/config"
	"github.com/lumosimmo/market-relayer-go/internal/fixedpoint"
)

func TestNormalizeQuote(t *testing.T) {
	t.Parallel()

	receivedAt := time.Date(2026, 3, 27, 12, 0, 5, 0, time.UTC)
	sourceTS := receivedAt.Add(-time.Second)

	t.Run("source timestamp basis", func(t *testing.T) {
		t.Parallel()

		got, err := NormalizeQuote(QuoteInput{
			Source:        "coinbase-primary",
			Symbol:        "ETHUSD",
			Bid:           "1981.85",
			Ask:           "1981.87",
			Last:          "1981.86",
			ReceivedAt:    receivedAt,
			SourceTS:      &sourceTS,
			TimestampMode: config.TimestampModeSourceTimestamp,
		}, ValidationOptions{
			StalenessThreshold: 10 * time.Second,
			MaxFutureSkew:      2 * time.Second,
		})
		if err != nil {
			t.Fatalf("NormalizeQuote() error = %v", err)
		}

		if got.Scale != 2 {
			t.Fatalf("quote scale = %d, want 2", got.Scale)
		}
		if got.FreshnessBasisKind != FreshnessBasisSourceTimestamp {
			t.Fatalf("freshness basis kind = %q, want %q", got.FreshnessBasisKind, FreshnessBasisSourceTimestamp)
		}
		if !got.FreshnessBasisAt.Equal(sourceTS) {
			t.Fatalf("freshness basis at = %s, want %s", got.FreshnessBasisAt, sourceTS)
		}
		if got.SourceTS == nil || !got.SourceTS.Equal(sourceTS) {
			t.Fatalf("source timestamp = %v, want %s", got.SourceTS, sourceTS)
		}
		if got.StableKey() != "coinbase-primary|ETHUSD|2|198185|198187|198186" {
			t.Fatalf("stable key = %q", got.StableKey())
		}
	})

	t.Run("received at basis preserves source scale", func(t *testing.T) {
		t.Parallel()

		got, err := NormalizeQuote(QuoteInput{
			Source:        "kraken-secondary",
			Symbol:        "ETHUSD",
			Bid:           "1981.8",
			Ask:           "1981.9",
			Last:          "1981.7",
			ReceivedAt:    receivedAt,
			TimestampMode: config.TimestampModeReceivedAt,
		}, ValidationOptions{
			StalenessThreshold: 10 * time.Second,
			MaxFutureSkew:      2 * time.Second,
		})
		if err != nil {
			t.Fatalf("NormalizeQuote() error = %v", err)
		}

		if got.Scale != 1 {
			t.Fatalf("quote scale = %d, want 1", got.Scale)
		}
		if got.FreshnessBasisKind != FreshnessBasisReceivedAt {
			t.Fatalf("freshness basis kind = %q, want %q", got.FreshnessBasisKind, FreshnessBasisReceivedAt)
		}
		if !got.FreshnessBasisAt.Equal(receivedAt) {
			t.Fatalf("freshness basis at = %s, want %s", got.FreshnessBasisAt, receivedAt)
		}
		if got.SourceTS != nil {
			t.Fatalf("source timestamp = %v, want nil", got.SourceTS)
		}
	})
}

func TestCoinbaseSourceFetch(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 3, 27, 12, 0, 5, 0, time.UTC)
	sourceTS := "2026-03-27T12:00:04.123456Z"

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got, want := r.URL.Path, "/products/ETH-USD/ticker"; got != want {
			t.Fatalf("request path = %q, want %q", got, want)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"bid":"1981.85","ask":"1981.87","price":"1981.86","time":"` + sourceTS + `"}`))
	}))
	t.Cleanup(server.Close)

	source, err := NewCoinbaseSource("ETHUSD", config.SourceConfig{
		Name:           "coinbase-primary",
		Kind:           "coinbase",
		ProductID:      "ETH-USD",
		RequestTimeout: 100 * time.Millisecond,
		TimestampMode:  config.TimestampModeSourceTimestamp,
	}, HTTPSourceOptions{
		BaseURL:            server.URL,
		Client:             server.Client(),
		Clock:              &mutableClock{now: now},
		StalenessThreshold: 10 * time.Second,
		MaxFutureSkew:      2 * time.Second,
		CooldownJitter:     noJitter,
	})
	if err != nil {
		t.Fatalf("NewCoinbaseSource() error = %v", err)
	}

	got, err := source.Fetch(context.Background(), FetchBudget{Deadline: now.Add(time.Second)})
	if err != nil {
		t.Fatalf("source.Fetch() error = %v", err)
	}

	if got.Source != "coinbase-primary" || got.Symbol != "ETHUSD" {
		t.Fatalf("quote identity = %+v", got)
	}
	if got.Scale != 2 {
		t.Fatalf("quote scale = %d, want 2", got.Scale)
	}
	if got.Bid != (fixedpoint.Value{Int: 198185, Scale: 2}) {
		t.Fatalf("bid = %+v", got.Bid)
	}
	if got.Ask != (fixedpoint.Value{Int: 198187, Scale: 2}) {
		t.Fatalf("ask = %+v", got.Ask)
	}
	if got.Last != (fixedpoint.Value{Int: 198186, Scale: 2}) {
		t.Fatalf("last = %+v", got.Last)
	}
	if got.FreshnessBasisKind != FreshnessBasisSourceTimestamp {
		t.Fatalf("freshness basis kind = %q", got.FreshnessBasisKind)
	}
}

func TestKrakenSourceFetch(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 3, 27, 12, 0, 5, 0, time.UTC)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got, want := r.URL.Path, "/0/public/Ticker"; got != want {
			t.Fatalf("request path = %q, want %q", got, want)
		}
		if got, want := r.URL.Query().Get("pair"), "ETH/USD"; got != want {
			t.Fatalf("request pair = %q, want %q", got, want)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"error":[],"result":{"XETHZUSD":{"a":["1981.9","1","1.000"],"b":["1981.8","1","1.000"],"c":["1981.7","0.1"]}}}`))
	}))
	t.Cleanup(server.Close)

	source, err := NewKrakenSource("ETHUSD", config.SourceConfig{
		Name:            "kraken-secondary",
		Kind:            "kraken",
		Pair:            "ETH/USD",
		RequestTimeout:  100 * time.Millisecond,
		TimestampMode:   config.TimestampModeReceivedAt,
		MaxUnchangedAge: 10 * time.Second,
	}, HTTPSourceOptions{
		BaseURL:            server.URL,
		Client:             server.Client(),
		Clock:              &mutableClock{now: now},
		StalenessThreshold: 10 * time.Second,
		MaxFutureSkew:      2 * time.Second,
		CooldownJitter:     noJitter,
	})
	if err != nil {
		t.Fatalf("NewKrakenSource() error = %v", err)
	}

	got, err := source.Fetch(context.Background(), FetchBudget{Deadline: now.Add(time.Second)})
	if err != nil {
		t.Fatalf("source.Fetch() error = %v", err)
	}

	if got.Source != "kraken-secondary" || got.Symbol != "ETHUSD" {
		t.Fatalf("quote identity = %+v", got)
	}
	if got.Scale != 1 {
		t.Fatalf("quote scale = %d, want 1", got.Scale)
	}
	if got.SourceTS != nil {
		t.Fatalf("source timestamp = %v, want nil", got.SourceTS)
	}
	if got.FreshnessBasisKind != FreshnessBasisReceivedAt {
		t.Fatalf("freshness basis kind = %q", got.FreshnessBasisKind)
	}
}

func TestCoinbaseSourceRejectsBadPayloads(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 3, 27, 12, 0, 5, 0, time.UTC)
	tests := []struct {
		name     string
		body     string
		wantKind ErrorKind
	}{
		{
			name:     "malformed json",
			body:     `{"bid":"1981.85"`,
			wantKind: ErrorMalformedPayload,
		},
		{
			name:     "empty prices",
			body:     `{"bid":"","ask":"1981.87","price":"1981.86","time":"2026-03-27T12:00:04Z"}`,
			wantKind: ErrorInvalidQuote,
		},
		{
			name:     "crossed book",
			body:     `{"bid":"1981.88","ask":"1981.87","price":"1981.86","time":"2026-03-27T12:00:04Z"}`,
			wantKind: ErrorInvalidQuote,
		},
		{
			name:     "precision overflow",
			body:     `{"bid":"9223372036854775807","ask":"1.1","price":"1.0","time":"2026-03-27T12:00:04Z"}`,
			wantKind: ErrorInvalidQuote,
		},
		{
			name:     "too many fractional digits",
			body:     `{"bid":"1.1234567890123456789","ask":"1.2","price":"1.1","time":"2026-03-27T12:00:04Z"}`,
			wantKind: ErrorInvalidQuote,
		},
		{
			name:     "stale timestamp",
			body:     `{"bid":"1981.85","ask":"1981.87","price":"1981.86","time":"2026-03-27T11:59:40Z"}`,
			wantKind: ErrorStaleQuote,
		},
		{
			name:     "future timestamp",
			body:     `{"bid":"1981.85","ask":"1981.87","price":"1981.86","time":"2026-03-27T12:00:08Z"}`,
			wantKind: ErrorFutureQuote,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(tt.body))
			}))
			t.Cleanup(server.Close)

			source, err := NewCoinbaseSource("ETHUSD", config.SourceConfig{
				Name:           "coinbase-primary",
				Kind:           "coinbase",
				ProductID:      "ETH-USD",
				RequestTimeout: 100 * time.Millisecond,
				TimestampMode:  config.TimestampModeSourceTimestamp,
			}, HTTPSourceOptions{
				BaseURL:            server.URL,
				Client:             server.Client(),
				Clock:              &mutableClock{now: now},
				StalenessThreshold: 10 * time.Second,
				MaxFutureSkew:      2 * time.Second,
				CooldownJitter:     noJitter,
			})
			if err != nil {
				t.Fatalf("NewCoinbaseSource() error = %v", err)
			}

			_, err = source.Fetch(context.Background(), FetchBudget{Deadline: now.Add(time.Second)})
			assertFetchErrorKind(t, err, tt.wantKind)
		})
	}
}

func TestCoinbaseSourceSurfacesTimeouts(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 3, 27, 12, 0, 5, 0, time.UTC)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(100 * time.Millisecond)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"bid":"1981.85","ask":"1981.87","price":"1981.86","time":"2026-03-27T12:00:04Z"}`))
	}))
	t.Cleanup(server.Close)

	source, err := NewCoinbaseSource("ETHUSD", config.SourceConfig{
		Name:           "coinbase-primary",
		Kind:           "coinbase",
		ProductID:      "ETH-USD",
		RequestTimeout: 25 * time.Millisecond,
		TimestampMode:  config.TimestampModeSourceTimestamp,
	}, HTTPSourceOptions{
		BaseURL:            server.URL,
		Client:             server.Client(),
		Clock:              &mutableClock{now: now},
		StalenessThreshold: 10 * time.Second,
		MaxFutureSkew:      2 * time.Second,
		CooldownJitter:     noJitter,
	})
	if err != nil {
		t.Fatalf("NewCoinbaseSource() error = %v", err)
	}

	_, err = source.Fetch(context.Background(), FetchBudget{Deadline: now.Add(time.Second)})
	assertFetchErrorKind(t, err, ErrorTimeout)
}

func TestKrakenSourceRejectsFrozenQuotes(t *testing.T) {
	t.Parallel()

	start := time.Date(2026, 3, 27, 12, 0, 0, 0, time.UTC)
	testClock := &mutableClock{now: start}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"error":[],"result":{"XETHZUSD":{"a":["1981.9","1","1.000"],"b":["1981.8","1","1.000"],"c":["1981.7","0.1"]}}}`))
	}))
	t.Cleanup(server.Close)

	source, err := NewKrakenSource("ETHUSD", config.SourceConfig{
		Name:            "kraken-secondary",
		Kind:            "kraken",
		Pair:            "ETH/USD",
		RequestTimeout:  100 * time.Millisecond,
		TimestampMode:   config.TimestampModeReceivedAt,
		MaxUnchangedAge: 10 * time.Second,
	}, HTTPSourceOptions{
		BaseURL:            server.URL,
		Client:             server.Client(),
		Clock:              testClock,
		StalenessThreshold: 10 * time.Second,
		MaxFutureSkew:      2 * time.Second,
		CooldownJitter:     noJitter,
	})
	if err != nil {
		t.Fatalf("NewKrakenSource() error = %v", err)
	}

	if _, err := source.Fetch(context.Background(), FetchBudget{Deadline: start.Add(time.Second)}); err != nil {
		t.Fatalf("first fetch error = %v", err)
	}

	testClock.Set(start.Add(5 * time.Second))
	if _, err := source.Fetch(context.Background(), FetchBudget{Deadline: start.Add(6 * time.Second)}); err != nil {
		t.Fatalf("second fetch error = %v", err)
	}

	testClock.Set(start.Add(11 * time.Second))
	_, err = source.Fetch(context.Background(), FetchBudget{Deadline: start.Add(12 * time.Second)})
	assertFetchErrorKind(t, err, ErrorFrozenQuote)
}

func TestCoinbaseSourceAppliesRateLimitCooldown(t *testing.T) {
	t.Parallel()

	start := time.Date(2026, 3, 27, 12, 0, 0, 0, time.UTC)
	testClock := &mutableClock{now: start}
	var calls int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.AddInt32(&calls, 1) == 1 {
			w.Header().Set("Retry-After", "30")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"bid":"1981.85","ask":"1981.87","price":"1981.86","time":"2026-03-27T12:00:00Z"}`))
	}))
	t.Cleanup(server.Close)

	source, err := NewCoinbaseSource("ETHUSD", config.SourceConfig{
		Name:           "coinbase-primary",
		Kind:           "coinbase",
		ProductID:      "ETH-USD",
		RequestTimeout: 100 * time.Millisecond,
		TimestampMode:  config.TimestampModeSourceTimestamp,
	}, HTTPSourceOptions{
		BaseURL:            server.URL,
		Client:             server.Client(),
		Clock:              testClock,
		StalenessThreshold: time.Minute,
		MaxFutureSkew:      2 * time.Second,
		CooldownJitter:     noJitter,
	})
	if err != nil {
		t.Fatalf("NewCoinbaseSource() error = %v", err)
	}

	_, err = source.Fetch(context.Background(), FetchBudget{Deadline: start.Add(time.Minute)})
	fetchErr := assertFetchErrorKind(t, err, ErrorRateLimited)
	if fetchErr.CooldownUntil == nil {
		t.Fatal("cooldown until = nil, want value")
	}
	if got, want := fetchErr.CooldownUntil.UTC(), start.Add(30*time.Second); !got.Equal(want) {
		t.Fatalf("cooldown until = %s, want %s", got, want)
	}

	_, err = source.Fetch(context.Background(), FetchBudget{Deadline: start.Add(time.Minute)})
	assertFetchErrorKind(t, err, ErrorCooldown)
	if got, want := atomic.LoadInt32(&calls), int32(1); got != want {
		t.Fatalf("server calls = %d, want %d", got, want)
	}

	testClock.Set(start.Add(31 * time.Second))
	if _, err := source.Fetch(context.Background(), FetchBudget{Deadline: start.Add(time.Minute)}); err != nil {
		t.Fatalf("fetch after cooldown error = %v", err)
	}
	if got, want := atomic.LoadInt32(&calls), int32(2); got != want {
		t.Fatalf("server calls = %d, want %d", got, want)
	}
}

func TestCoinbaseSourceRetriesOnceForServerErrors(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 3, 27, 12, 0, 5, 0, time.UTC)
	var calls int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.AddInt32(&calls, 1) == 1 {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"bid":"1981.85","ask":"1981.87","price":"1981.86","time":"2026-03-27T12:00:04Z"}`))
	}))
	t.Cleanup(server.Close)

	source, err := NewCoinbaseSource("ETHUSD", config.SourceConfig{
		Name:           "coinbase-primary",
		Kind:           "coinbase",
		ProductID:      "ETH-USD",
		RequestTimeout: 100 * time.Millisecond,
		TimestampMode:  config.TimestampModeSourceTimestamp,
	}, HTTPSourceOptions{
		BaseURL:            server.URL,
		Client:             server.Client(),
		Clock:              &mutableClock{now: now},
		StalenessThreshold: time.Minute,
		MaxFutureSkew:      2 * time.Second,
		CooldownJitter:     noJitter,
	})
	if err != nil {
		t.Fatalf("NewCoinbaseSource() error = %v", err)
	}

	if _, err := source.Fetch(context.Background(), FetchBudget{Deadline: now.Add(time.Second)}); err != nil {
		t.Fatalf("source.Fetch() error = %v", err)
	}
	if got, want := atomic.LoadInt32(&calls), int32(2); got != want {
		t.Fatalf("server calls = %d, want %d", got, want)
	}
}

func TestCoinbaseSourceSkipsRetryWhenBudgetTooSmall(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 3, 27, 12, 0, 5, 0, time.UTC)
	var calls int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		w.WriteHeader(http.StatusBadGateway)
	}))
	t.Cleanup(server.Close)

	source, err := NewCoinbaseSource("ETHUSD", config.SourceConfig{
		Name:           "coinbase-primary",
		Kind:           "coinbase",
		ProductID:      "ETH-USD",
		RequestTimeout: 100 * time.Millisecond,
		TimestampMode:  config.TimestampModeSourceTimestamp,
	}, HTTPSourceOptions{
		BaseURL:            server.URL,
		Client:             server.Client(),
		Clock:              &mutableClock{now: now},
		StalenessThreshold: time.Minute,
		MaxFutureSkew:      2 * time.Second,
		CooldownJitter:     noJitter,
	})
	if err != nil {
		t.Fatalf("NewCoinbaseSource() error = %v", err)
	}

	_, err = source.Fetch(context.Background(), FetchBudget{Deadline: now.Add(50 * time.Millisecond)})
	assertFetchErrorKind(t, err, ErrorHTTPStatus)
	if got, want := atomic.LoadInt32(&calls), int32(1); got != want {
		t.Fatalf("server calls = %d, want %d", got, want)
	}
}

func TestFetchAllIsolatesSourcesAndPreservesOrder(t *testing.T) {
	t.Parallel()

	slowServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(150 * time.Millisecond)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"bid":"1981.85","ask":"1981.87","price":"1981.86","time":"2026-03-27T12:00:04Z"}`))
	}))
	t.Cleanup(slowServer.Close)

	fastServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"error":[],"result":{"XETHZUSD":{"a":["1981.9","1","1.000"],"b":["1981.8","1","1.000"],"c":["1981.7","0.1"]}}}`))
	}))
	t.Cleanup(fastServer.Close)

	now := time.Date(2026, 3, 27, 12, 0, 5, 0, time.UTC)
	slowSource, err := NewCoinbaseSource("ETHUSD", config.SourceConfig{
		Name:           "coinbase-primary",
		Kind:           "coinbase",
		ProductID:      "ETH-USD",
		RequestTimeout: time.Second,
		TimestampMode:  config.TimestampModeSourceTimestamp,
	}, HTTPSourceOptions{
		BaseURL:            slowServer.URL,
		Client:             slowServer.Client(),
		Clock:              &mutableClock{now: now},
		StalenessThreshold: time.Minute,
		MaxFutureSkew:      2 * time.Second,
		CooldownJitter:     noJitter,
	})
	if err != nil {
		t.Fatalf("NewCoinbaseSource() error = %v", err)
	}
	fastSource, err := NewKrakenSource("ETHUSD", config.SourceConfig{
		Name:            "kraken-secondary",
		Kind:            "kraken",
		Pair:            "ETH/USD",
		RequestTimeout:  time.Second,
		TimestampMode:   config.TimestampModeReceivedAt,
		MaxUnchangedAge: 10 * time.Second,
	}, HTTPSourceOptions{
		BaseURL:            fastServer.URL,
		Client:             fastServer.Client(),
		Clock:              &mutableClock{now: now},
		StalenessThreshold: time.Minute,
		MaxFutureSkew:      2 * time.Second,
		CooldownJitter:     noJitter,
	})
	if err != nil {
		t.Fatalf("NewKrakenSource() error = %v", err)
	}
	sources := []Source{
		slowSource,
		fastSource,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()

	results := FetchAll(ctx, sources, FetchBudget{Deadline: now.Add(time.Minute)})
	if len(results) != 2 {
		t.Fatalf("len(results) = %d, want 2", len(results))
	}
	if results[0].Source != "coinbase-primary" {
		t.Fatalf("results[0].Source = %q, want coinbase-primary", results[0].Source)
	}
	if results[1].Source != "kraken-secondary" {
		t.Fatalf("results[1].Source = %q, want kraken-secondary", results[1].Source)
	}
	if results[0].Err == nil {
		t.Fatal("results[0].Err = nil, want error")
	}
	if results[1].Err != nil {
		t.Fatalf("results[1].Err = %v, want nil", results[1].Err)
	}
	if results[1].Quote.Source != "kraken-secondary" {
		t.Fatalf("results[1].Quote.Source = %q, want kraken-secondary", results[1].Quote.Source)
	}
}

func TestBuildMarketSourcesFetchesTwoLiveQuotes(t *testing.T) {
	t.Parallel()

	coinbaseServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"bid":"1981.85","ask":"1981.87","price":"1981.86","time":"2026-03-27T12:00:04Z"}`))
	}))
	t.Cleanup(coinbaseServer.Close)

	krakenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"error":[],"result":{"XETHZUSD":{"a":["1981.9","1","1.000"],"b":["1981.8","1","1.000"],"c":["1981.7","0.1"]}}}`))
	}))
	t.Cleanup(krakenServer.Close)

	cfg, err := config.LoadBytes([]byte(`version: 1
service:
  name: market-relayer-go
  instance_id: feed-test
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
  - name: kraken-secondary
    kind: kraken
    pair: ETH/USD
    request_timeout: 1s
    timestamp_mode: received_at
    max_unchanged_age: 10s
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
      - name: kraken-secondary
`), config.LoadOptions{})
	if err != nil {
		t.Fatalf("config.LoadBytes() error = %v", err)
	}

	now := time.Date(2026, 3, 27, 12, 0, 5, 0, time.UTC)
	sources, err := BuildMarketSources(cfg, cfg.Markets[0], BuildOptions{
		Clock:  &mutableClock{now: now},
		Client: http.DefaultClient,
		EndpointOverrides: map[string]string{
			"coinbase-primary": coinbaseServer.URL,
			"kraken-secondary": krakenServer.URL,
		},
		MaxFutureSkew:  2 * time.Second,
		CooldownJitter: noJitter,
	})
	if err != nil {
		t.Fatalf("BuildMarketSources() error = %v", err)
	}

	results := FetchAll(context.Background(), sources, FetchBudget{Deadline: now.Add(time.Second)})
	if len(results) != 2 {
		t.Fatalf("len(results) = %d, want 2", len(results))
	}
	for _, result := range results {
		if result.Err != nil {
			t.Fatalf("result for %s err = %v", result.Source, result.Err)
		}
	}
}

func TestHyperliquidBookSourceFetch(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 3, 27, 12, 0, 5, 0, time.UTC)
	sourceTS := now.Add(-1500 * time.Millisecond)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got, want := r.Method, http.MethodPost; got != want {
			t.Fatalf("request method = %q, want %q", got, want)
		}
		if got, want := r.URL.Path, "/info"; got != want {
			t.Fatalf("request path = %q, want %q", got, want)
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("io.ReadAll() error = %v", err)
		}
		if got, want := string(body), `{"type":"l2Book","coin":"xyz:CL"}`; got != want {
			t.Fatalf("request body = %q, want %q", got, want)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"coin":"xyz:CL","time":` + fixedpointInt64String(sourceTS.UnixMilli()) + `,"levels":[[{"px":"95.274","sz":"11.461"}],[{"px":"95.283","sz":"2.651"}]]}`))
	}))
	t.Cleanup(server.Close)

	source, err := NewHyperliquidBookSource("WTIUSD", config.SourceConfig{
		Name:           "hyperliquid-wti",
		Kind:           config.SourceKindHyperliquid,
		Market:         "xyz:CL",
		RequestTimeout: 100 * time.Millisecond,
		TimestampMode:  config.TimestampModeSourceTimestamp,
	}, HTTPSourceOptions{
		BaseURL:            server.URL,
		Client:             server.Client(),
		Clock:              &mutableClock{now: now},
		StalenessThreshold: 10 * time.Second,
		MaxFutureSkew:      2 * time.Second,
		CooldownJitter:     noJitter,
	})
	if err != nil {
		t.Fatalf("NewHyperliquidBookSource() error = %v", err)
	}

	book, err := source.FetchBook(context.Background(), FetchBudget{Deadline: now.Add(time.Second)})
	if err != nil {
		t.Fatalf("source.FetchBook() error = %v", err)
	}

	if got, want := book.Source, "hyperliquid-wti"; got != want {
		t.Fatalf("book.Source = %q, want %q", got, want)
	}
	if got, want := book.Symbol, "WTIUSD"; got != want {
		t.Fatalf("book.Symbol = %q, want %q", got, want)
	}
	if got, want := book.Market, "xyz:CL"; got != want {
		t.Fatalf("book.Market = %q, want %q", got, want)
	}
	if len(book.Bids) != 1 || len(book.Asks) != 1 {
		t.Fatalf("book depth = (%d bids, %d asks), want (1, 1)", len(book.Bids), len(book.Asks))
	}
	if got, want := book.Bids[0].Price, (fixedpoint.Value{Int: 95274, Scale: 3}); got != want {
		t.Fatalf("book.Bids[0].Price = %+v, want %+v", got, want)
	}
	if got, want := book.Asks[0].Price, (fixedpoint.Value{Int: 95283, Scale: 3}); got != want {
		t.Fatalf("book.Asks[0].Price = %+v, want %+v", got, want)
	}
	if book.SourceTS == nil || !book.SourceTS.Equal(sourceTS) {
		t.Fatalf("book.SourceTS = %v, want %s", book.SourceTS, sourceTS)
	}
	if got, want := book.FreshnessBasisKind, FreshnessBasisSourceTimestamp; got != want {
		t.Fatalf("book.FreshnessBasisKind = %q, want %q", got, want)
	}
	if got, want := book.FreshnessBasisAt, sourceTS; !got.Equal(want) {
		t.Fatalf("book.FreshnessBasisAt = %s, want %s", got, want)
	}
}

func TestHyperliquidBookSourceRejectsBadPayloads(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 3, 27, 12, 0, 5, 0, time.UTC)
	tests := []struct {
		name     string
		body     string
		wantKind ErrorKind
	}{
		{
			name:     "malformed json",
			body:     `{"coin":"xyz:CL"`,
			wantKind: ErrorMalformedPayload,
		},
		{
			name:     "invalid level",
			body:     `{"coin":"xyz:CL","time":` + strconv.FormatInt(now.Add(-time.Second).UnixMilli(), 10) + `,"levels":[[{"px":"bad","sz":"1"}],[{"px":"95.283","sz":"2"}]]}`,
			wantKind: ErrorInvalidQuote,
		},
		{
			name:     "stale timestamp",
			body:     `{"coin":"xyz:CL","time":` + strconv.FormatInt(now.Add(-11*time.Second).UnixMilli(), 10) + `,"levels":[[{"px":"95.274","sz":"1"}],[{"px":"95.283","sz":"2"}]]}`,
			wantKind: ErrorStaleQuote,
		},
		{
			name:     "future timestamp",
			body:     `{"coin":"xyz:CL","time":` + strconv.FormatInt(now.Add(3*time.Second).UnixMilli(), 10) + `,"levels":[[{"px":"95.274","sz":"1"}],[{"px":"95.283","sz":"2"}]]}`,
			wantKind: ErrorFutureQuote,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(tt.body))
			}))
			t.Cleanup(server.Close)

			source, err := NewHyperliquidBookSource("WTIUSD", config.SourceConfig{
				Name:           "hyperliquid-wti",
				Kind:           config.SourceKindHyperliquid,
				Market:         "xyz:CL",
				RequestTimeout: 100 * time.Millisecond,
				TimestampMode:  config.TimestampModeSourceTimestamp,
			}, HTTPSourceOptions{
				BaseURL:            server.URL,
				Client:             server.Client(),
				Clock:              &mutableClock{now: now},
				StalenessThreshold: 10 * time.Second,
				MaxFutureSkew:      2 * time.Second,
				CooldownJitter:     noJitter,
			})
			if err != nil {
				t.Fatalf("NewHyperliquidBookSource() error = %v", err)
			}

			_, err = source.FetchBook(context.Background(), FetchBudget{Deadline: now.Add(time.Second)})
			assertFetchErrorKind(t, err, tt.wantKind)
		})
	}
}

func TestBuildPerpBookSourceFetchesLiveBook(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 3, 27, 12, 0, 5, 0, time.UTC)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"coin":"ETH","time":` + strconv.FormatInt(now.Add(-time.Second).UnixMilli(), 10) + `,"levels":[[{"px":"1981.85","sz":"1.5"}],[{"px":"1981.95","sz":"2.0"}]]}`))
	}))
	t.Cleanup(server.Close)

	cfg, err := config.LoadBytes([]byte(validBootstrapBookConfig()), config.LoadOptions{})
	if err != nil {
		t.Fatalf("config.LoadBytes() error = %v", err)
	}

	source, err := BuildPerpBookSource(cfg, cfg.Markets[0], BuildOptions{
		Clock:             &mutableClock{now: now},
		Client:            server.Client(),
		EndpointOverrides: map[string]string{"hyperliquid-eth": server.URL},
		MaxFutureSkew:     2 * time.Second,
		CooldownJitter:    noJitter,
	})
	if err != nil {
		t.Fatalf("BuildPerpBookSource() error = %v", err)
	}

	book, err := source.FetchBook(context.Background(), FetchBudget{Deadline: now.Add(time.Second)})
	if err != nil {
		t.Fatalf("source.FetchBook() error = %v", err)
	}
	if got, want := book.Market, "ETH"; got != want {
		t.Fatalf("book.Market = %q, want %q", got, want)
	}
}

func fixedpointInt64String(value int64) string {
	return strconv.FormatInt(value, 10)
}

func validBootstrapBookConfig() string {
	return `version: 1
service:
  name: market-relayer-go
  instance_id: feed-test
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
`
}

type mutableClock struct {
	mu  sync.Mutex
	now time.Time
}

func (clock *mutableClock) Now() time.Time {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	return clock.now
}

func (clock *mutableClock) Set(now time.Time) {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	clock.now = now
}

func noJitter(time.Duration) time.Duration {
	return 0
}

func assertFetchErrorKind(t *testing.T, err error, want ErrorKind) *Error {
	t.Helper()

	if err == nil {
		t.Fatalf("error = nil, want %q", want)
	}

	var fetchErr *Error
	if !errors.As(err, &fetchErr) {
		t.Fatalf("error type = %T, want *Error", err)
	}
	if fetchErr.Kind != want {
		t.Fatalf("error kind = %q, want %q", fetchErr.Kind, want)
	}
	return fetchErr
}
