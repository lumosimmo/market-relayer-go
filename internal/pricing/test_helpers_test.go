package pricing

import (
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/lumosimmo/market-relayer-go/internal/config"
	"github.com/lumosimmo/market-relayer-go/internal/feeds"
	"github.com/lumosimmo/market-relayer-go/internal/fixedpoint"
	"github.com/lumosimmo/market-relayer-go/internal/status"
)

type testLevel struct {
	Price string
	Size  string
}

func testQuote(t *testing.T, source string, symbol string, mid string, basisAt time.Time, kind feeds.FreshnessBasisKind) feeds.Quote {
	t.Helper()

	value := mustDecimal(t, mid)
	quote := feeds.Quote{
		Source:             source,
		Symbol:             symbol,
		Bid:                value,
		Ask:                value,
		Last:               value,
		Scale:              value.Scale,
		ReceivedAt:         basisAt.UTC(),
		FreshnessBasisKind: kind,
		FreshnessBasisAt:   basisAt.UTC(),
	}
	if kind == feeds.FreshnessBasisSourceTimestamp {
		sourceTS := basisAt.UTC()
		quote.SourceTS = &sourceTS
	}
	return quote
}

func testBook(t *testing.T, source string, symbol string, market string, bids []testLevel, asks []testLevel, basisAt time.Time) *feeds.BookSnapshot {
	t.Helper()

	book := &feeds.BookSnapshot{
		Source:             source,
		Symbol:             symbol,
		Market:             market,
		ReceivedAt:         basisAt.UTC(),
		FreshnessBasisKind: feeds.FreshnessBasisReceivedAt,
		FreshnessBasisAt:   basisAt.UTC(),
	}
	for _, bid := range bids {
		book.Bids = append(book.Bids, feeds.BookLevel{
			Price: mustDecimal(t, bid.Price),
			Size:  mustDecimal(t, bid.Size),
		})
	}
	for _, ask := range asks {
		book.Asks = append(book.Asks, feeds.BookLevel{
			Price: mustDecimal(t, ask.Price),
			Size:  mustDecimal(t, ask.Size),
		})
	}
	return book
}

func mustCompute(t *testing.T, inputs Inputs) Result {
	t.Helper()

	result, err := Compute(inputs)
	if err != nil {
		t.Fatalf("Compute() error = %v", err)
	}
	return result
}

func mustDecimal(t *testing.T, raw string) fixedpoint.Value {
	t.Helper()

	value, err := fixedpoint.ParseDecimal(raw)
	if err != nil {
		t.Fatalf("ParseDecimal(%q) error = %v", raw, err)
	}
	return value
}

func valuePtr(value fixedpoint.Value) *fixedpoint.Value {
	return &value
}

func assertPrice(t *testing.T, price Price, wantStatus status.Status, wantReason status.Reason, wantValue string) {
	t.Helper()

	if got := price.Status; got != wantStatus {
		t.Fatalf("Price.Status = %q, want %q", got, wantStatus)
	}
	if got := price.Reason; got != wantReason {
		t.Fatalf("Price.Reason = %q, want %q", got, wantReason)
	}
	if wantValue == "" {
		if price.Value != nil {
			t.Fatalf("Price.Value = %s, want nil", mustFormat(t, *price.Value))
		}
		return
	}
	if price.Value == nil {
		t.Fatalf("Price.Value = nil, want %s", wantValue)
	}
	assertFixedpointString(t, *price.Value, wantValue)
}

func assertValue(t *testing.T, got *fixedpoint.Value, want fixedpoint.Value) {
	t.Helper()

	if got == nil {
		t.Fatalf("value = nil, want %s", mustFormat(t, want))
	}
	assertFixedpointString(t, *got, mustFormat(t, want))
}

func assertFixedpointString(t *testing.T, got fixedpoint.Value, want string) {
	t.Helper()

	if actual := mustFormat(t, got); actual != want {
		t.Fatalf("value = %s, want %s", actual, want)
	}
}

func mustFormat(t *testing.T, value fixedpoint.Value) string {
	t.Helper()

	if value.Scale <= 0 {
		return strconv.FormatInt(value.Int, 10)
	}

	raw := value.Int
	sign := ""
	if raw < 0 {
		sign = "-"
		raw = -raw
	}

	digits := strconv.FormatInt(raw, 10)
	scale := int(value.Scale)
	if len(digits) <= scale {
		digits = strings.Repeat("0", scale-len(digits)+1) + digits
	}
	split := len(digits) - scale
	return sign + digits[:split] + "." + digits[split:]
}

func testConfiguredMarket() config.MarketConfig {
	cfg, err := config.LoadBytes([]byte(`
version: 1
service:
  name: market-relayer-go
  instance_id: test
store:
  dsn: postgres://ignored
api:
  listen_addr: 127.0.0.1:0
sink:
  kind: file
  path: /tmp/pricing-test.out
  request_timeout: 1s
sources:
  - name: coinbase
    kind: coinbase
    product_id: ETH-USD
    request_timeout: 1s
    timestamp_mode: source_timestamp
  - name: kraken
    kind: kraken
    pair: ETH/USD
    request_timeout: 1s
    timestamp_mode: received_at
    max_unchanged_age: 10s
  - name: backup
    kind: coinbase
    product_id: ETH-USD
    request_timeout: 1s
    timestamp_mode: source_timestamp
  - name: hl
    kind: hyperliquid
    market: ETH
    request_timeout: 1s
    timestamp_mode: source_timestamp
markets:
  - symbol: ETHUSD
    session_mode: always_open
    perp_book_source: hl
    impact_notional_usd: "100"
    publish_interval: 1s
    staleness_threshold: 10s
    max_fallback_age: 30s
    clamp_bps: 50
    divergence_threshold_bps: 50
    publish_scale: 2
    sources:
      - name: coinbase
      - name: kraken
      - name: backup
`), config.LoadOptions{})
	if err != nil {
		panic(err)
	}
	return cfg.Markets[0]
}
