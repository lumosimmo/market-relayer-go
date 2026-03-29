package feeds

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lumosimmo/market-relayer-go/internal/config"
	"github.com/lumosimmo/market-relayer-go/internal/fixedpoint"
	"github.com/lumosimmo/market-relayer-go/internal/markettime"
)

func TestPythSourceFetchBuildsSyntheticQuoteAndSessionWindow(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 3, 27, 16, 0, 0, 0, time.UTC)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v2/price_feeds":
			if got, want := r.URL.Query().Get("query"), "WTI"; got != want {
				t.Fatalf("metadata query = %q, want %q", got, want)
			}
			writeJSON(t, w, `[
				{"id":"front","attributes":{"asset_type":"Commodities","description":"PYTH WTI 29 MARCH 2026 / US DOLLAR","generic_symbol":"WTIJ6","quote_currency":"USD","schedule":"America/New_York;0000-1700&1800-2400,0000-1700&1800-2400,0000-1700&1800-2400,0000-1700&1800-2400,0000-1700,C,1800-2400;0403/C"}},
				{"id":"next","attributes":{"asset_type":"Commodities","description":"PYTH WTI 21 APRIL 2026 / US DOLLAR","generic_symbol":"WTIK6","quote_currency":"USD","schedule":"America/New_York;0000-1700&1800-2400,0000-1700&1800-2400,0000-1700&1800-2400,0000-1700&1800-2400,0000-1700,C,1800-2400;0403/C"}},
				{"id":"deprecated","attributes":{"asset_type":"Commodities","description":"DEPRECATED FEED - PYTH WTI 20 FEBRUARY 2026 / US DOLLAR","generic_symbol":"WTIH6","quote_currency":"USD","schedule":"America/New_York;0000-1700&1800-2400,0000-1700&1800-2400,0000-1700&1800-2400,0000-1700&1800-2400,0000-1700,C,1800-2400;0403/C"}}
			]`)
		case "/v2/updates/price/latest":
			if got := r.URL.Query()["ids[]"]; len(got) != 2 {
				t.Fatalf("latest ids = %v, want 2 ids", got)
			}
			writeJSON(t, w, `{
				"parsed":[
					{"id":"front","price":{"price":"9551579","conf":"3961","expo":-5,"publish_time":1774627199}},
					{"id":"next","price":{"price":"9527668","conf":"4320","expo":-5,"publish_time":1774627198}}
				]
			}`)
		default:
			t.Fatalf("unexpected path %q", r.URL.Path)
		}
	}))
	t.Cleanup(server.Close)

	source := mustNewPythSource(t, now, server.URL)
	quote, err := source.Fetch(context.Background(), FetchBudget{Deadline: now.Add(time.Second)})
	if err != nil {
		t.Fatalf("source.Fetch() error = %v", err)
	}

	if quote.Source != "pyth-wti" || quote.Symbol != "WTIUSD" {
		t.Fatalf("quote identity = %+v", quote)
	}
	if got, want := quote.Last, (fixedpoint.Value{Int: 9537232, Scale: 5}); got != want {
		t.Fatalf("quote.Last = %+v, want %+v", got, want)
	}
	if got, want := quote.Bid, (fixedpoint.Value{Int: 9537232, Scale: 5}); got != want {
		t.Fatalf("quote.Bid = %+v, want %+v", got, want)
	}
	if got, want := quote.Ask, (fixedpoint.Value{Int: 9537232, Scale: 5}); got != want {
		t.Fatalf("quote.Ask = %+v, want %+v", got, want)
	}
	if quote.SourceTS == nil || !quote.SourceTS.Equal(time.Unix(1774627198, 0).UTC()) {
		t.Fatalf("quote.SourceTS = %v, want earliest publish time", quote.SourceTS)
	}

	if !source.SessionOpenAt(time.Date(2026, 3, 27, 15, 0, 0, 0, time.UTC)) {
		t.Fatal("SessionOpenAt(open) = false, want true")
	}
	if source.SessionOpenAt(time.Date(2026, 3, 27, 21, 30, 0, 0, time.UTC)) {
		t.Fatal("SessionOpenAt(maintenance) = true, want false")
	}
}

func TestPythSourceFetchRenormalizesToRemainingLiveContract(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 3, 27, 16, 0, 0, 0, time.UTC)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v2/price_feeds":
			writeJSON(t, w, `[
				{"id":"front","attributes":{"asset_type":"Commodities","description":"PYTH WTI 29 MARCH 2026 / US DOLLAR","generic_symbol":"WTIJ6","quote_currency":"USD","schedule":"America/New_York;0000-1700&1800-2400,0000-1700&1800-2400,0000-1700&1800-2400,0000-1700&1800-2400,0000-1700,C,1800-2400;0403/C"}},
				{"id":"next","attributes":{"asset_type":"Commodities","description":"PYTH WTI 21 APRIL 2026 / US DOLLAR","generic_symbol":"WTIK6","quote_currency":"USD","schedule":"America/New_York;0000-1700&1800-2400,0000-1700&1800-2400,0000-1700&1800-2400,0000-1700&1800-2400,0000-1700,C,1800-2400;0403/C"}}
			]`)
		case "/v2/updates/price/latest":
			writeJSON(t, w, `{
				"parsed":[
					{"id":"front","price":{"price":"9551579","conf":"3961","expo":-5,"publish_time":1774627199}}
				]
			}`)
		default:
			t.Fatalf("unexpected path %q", r.URL.Path)
		}
	}))
	t.Cleanup(server.Close)

	source := mustNewPythSource(t, now, server.URL)
	quote, err := source.Fetch(context.Background(), FetchBudget{Deadline: now.Add(time.Second)})
	if err != nil {
		t.Fatalf("source.Fetch() error = %v", err)
	}

	if got, want := quote.Last, (fixedpoint.Value{Int: 9551579, Scale: 5}); got != want {
		t.Fatalf("quote.Last = %+v, want %+v", got, want)
	}
}

func TestPythSourceConfidenceThresholdAndFlatQuote(t *testing.T) {
	t.Parallel()

	source := &pythSource{
		config: config.SourceConfig{
			Name:             "pyth-wti",
			MaxConfidenceBPS: 5000,
		},
		symbol: "WTIUSD",
	}
	quote, err := source.syntheticQuote(time.Date(2026, 3, 27, 16, 0, 0, 0, time.UTC), []liveContract{
		{
			weightBPS:   fixedpoint.BasisPointsDenominator,
			price:       fixedpoint.Value{Int: 9550, Scale: 2},
			conf:        fixedpoint.Value{Int: 96, Scale: 2},
			publishTime: time.Date(2026, 3, 27, 15, 59, 59, 0, time.UTC),
		},
	})
	if err != nil {
		t.Fatalf("syntheticQuote() error = %v", err)
	}
	if got, want := quote.Bid, (fixedpoint.Value{Int: 9550, Scale: 2}); got != want {
		t.Fatalf("quote.Bid = %+v, want %+v", got, want)
	}
	if got, want := quote.Ask, (fixedpoint.Value{Int: 9550, Scale: 2}); got != want {
		t.Fatalf("quote.Ask = %+v, want %+v", got, want)
	}

	source.config.MaxConfidenceBPS = 50
	if _, err := source.syntheticQuote(time.Date(2026, 3, 27, 16, 0, 0, 0, time.UTC), []liveContract{
		{
			weightBPS:   fixedpoint.BasisPointsDenominator,
			price:       fixedpoint.Value{Int: 9550, Scale: 2},
			conf:        fixedpoint.Value{Int: 96, Scale: 2},
			publishTime: time.Date(2026, 3, 27, 15, 59, 59, 0, time.UTC),
		},
	}); err == nil {
		t.Fatal("syntheticQuote(confidence threshold) error = nil, want failure")
	}
}

func TestPythSourceFetchRetriesLatestRequest(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 3, 27, 16, 0, 0, 0, time.UTC)
	var latestCalls int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v2/price_feeds":
			writeJSON(t, w, `[
				{"id":"front","attributes":{"asset_type":"Commodities","description":"PYTH WTI 29 MARCH 2026 / US DOLLAR","generic_symbol":"WTIJ6","quote_currency":"USD","schedule":"America/New_York;0000-1700&1800-2400,0000-1700&1800-2400,0000-1700&1800-2400,0000-1700&1800-2400,0000-1700,C,1800-2400;0403/C"}},
				{"id":"next","attributes":{"asset_type":"Commodities","description":"PYTH WTI 21 APRIL 2026 / US DOLLAR","generic_symbol":"WTIK6","quote_currency":"USD","schedule":"America/New_York;0000-1700&1800-2400,0000-1700&1800-2400,0000-1700&1800-2400,0000-1700&1800-2400,0000-1700,C,1800-2400;0403/C"}}
			]`)
		case "/v2/updates/price/latest":
			if atomic.AddInt32(&latestCalls, 1) == 1 {
				w.WriteHeader(http.StatusBadGateway)
				return
			}
			writeJSON(t, w, `{
				"parsed":[
					{"id":"front","price":{"price":"9551579","conf":"3961","expo":-5,"publish_time":1774627199}},
					{"id":"next","price":{"price":"9527668","conf":"4320","expo":-5,"publish_time":1774627198}}
				]
			}`)
		default:
			t.Fatalf("unexpected path %q", r.URL.Path)
		}
	}))
	t.Cleanup(server.Close)

	source := mustNewPythSource(t, now, server.URL)
	if _, err := source.Fetch(context.Background(), FetchBudget{Deadline: now.Add(time.Second)}); err != nil {
		t.Fatalf("source.Fetch() error = %v", err)
	}
	if got, want := atomic.LoadInt32(&latestCalls), int32(2); got != want {
		t.Fatalf("latest calls = %d, want %d", got, want)
	}
}

func TestPythContractParsingAndRollWeights(t *testing.T) {
	t.Parallel()

	schedule, err := markettime.ParseSchedule("America/New_York;0000-1700&1800-2400,0000-1700&1800-2400,0000-1700&1800-2400,0000-1700&1800-2400,0000-1700,C,1800-2400;0403/C")
	if err != nil {
		t.Fatalf("ParseSchedule() error = %v", err)
	}

	source := &pythSource{
		config: config.SourceConfig{
			Name:           "pyth-wti",
			RootSymbol:     "WTI",
			AssetType:      "Commodities",
			QuoteCurrency:  "USD",
			RollWindowDays: 5,
		},
	}
	contracts, err := source.parseContracts([]pythMetadataResponse{
		{
			ID: "deprecated",
			Attributes: struct {
				AssetType     string "json:\"asset_type\""
				Description   string "json:\"description\""
				GenericSymbol string "json:\"generic_symbol\""
				QuoteCurrency string "json:\"quote_currency\""
				Schedule      string "json:\"schedule\""
			}{
				AssetType:     "Commodities",
				Description:   "DEPRECATED FEED - PYTH WTI 20 FEBRUARY 2026 / US DOLLAR",
				GenericSymbol: "WTIH6",
				QuoteCurrency: "USD",
				Schedule:      "America/New_York;0000-1700&1800-2400,0000-1700&1800-2400,0000-1700&1800-2400,0000-1700&1800-2400,0000-1700,C,1800-2400;0403/C",
			},
		},
		{
			ID: "next",
			Attributes: struct {
				AssetType     string "json:\"asset_type\""
				Description   string "json:\"description\""
				GenericSymbol string "json:\"generic_symbol\""
				QuoteCurrency string "json:\"quote_currency\""
				Schedule      string "json:\"schedule\""
			}{
				AssetType:     "Commodities",
				Description:   "PYTH WTI 21 APRIL 2026 / US DOLLAR",
				GenericSymbol: "WTIK6",
				QuoteCurrency: "USD",
				Schedule:      "America/New_York;0000-1700&1800-2400,0000-1700&1800-2400,0000-1700&1800-2400,0000-1700&1800-2400,0000-1700,C,1800-2400;0403/C",
			},
		},
		{
			ID: "front",
			Attributes: struct {
				AssetType     string "json:\"asset_type\""
				Description   string "json:\"description\""
				GenericSymbol string "json:\"generic_symbol\""
				QuoteCurrency string "json:\"quote_currency\""
				Schedule      string "json:\"schedule\""
			}{
				AssetType:     "Commodities",
				Description:   "PYTH WTI 29 MARCH 2026 / US DOLLAR",
				GenericSymbol: "WTIJ6",
				QuoteCurrency: "USD",
				Schedule:      "America/New_York;0000-1700&1800-2400,0000-1700&1800-2400,0000-1700&1800-2400,0000-1700&1800-2400,0000-1700,C,1800-2400;0403/C",
			},
		},
	})
	if err != nil {
		t.Fatalf("parseContracts() error = %v", err)
	}

	if got, want := len(contracts), 2; got != want {
		t.Fatalf("len(contracts) = %d, want %d", got, want)
	}
	if contracts[0].id != "front" || contracts[1].id != "next" {
		t.Fatalf("contracts order = %+v, want front then next", contracts)
	}

	beforeRoll := source.selectWeightedContracts(time.Date(2026, 3, 23, 16, 0, 0, 0, time.UTC), contracts)
	if beforeRoll[0].weightBPS != fixedpoint.BasisPointsDenominator || beforeRoll[1].weightBPS != 0 {
		t.Fatalf("beforeRoll weights = %+v, want 10000/0", beforeRoll)
	}

	inRoll := source.selectWeightedContracts(time.Date(2026, 3, 27, 16, 0, 0, 0, time.UTC), contracts)
	if inRoll[0].weightBPS != 4000 || inRoll[1].weightBPS != 6000 {
		t.Fatalf("inRoll weights = %+v, want 4000/6000", inRoll)
	}

	onExpiry := source.selectWeightedContracts(time.Date(2026, 3, 29, 16, 0, 0, 0, time.UTC), contracts)
	if onExpiry[0].weightBPS != 0 || onExpiry[1].weightBPS != fixedpoint.BasisPointsDenominator {
		t.Fatalf("onExpiry weights = %+v, want 0/10000", onExpiry)
	}

	if !schedule.OpenAt(time.Date(2026, 3, 27, 16, 0, 0, 0, time.UTC)) {
		t.Fatal("schedule.OpenAt(open) = false, want true")
	}
}

func TestPythSourceSessionDefaultsOpenWithoutMetadata(t *testing.T) {
	t.Parallel()

	source := &pythSource{}
	if !source.SessionOpenAt(time.Date(2026, 3, 27, 16, 0, 0, 0, time.UTC)) {
		t.Fatal("SessionOpenAt(no metadata) = false, want true")
	}
}

func mustNewPythSource(t *testing.T, now time.Time, baseURL string) *pythSource {
	t.Helper()

	source, err := NewPythSource("WTIUSD", config.SourceConfig{
		Name:             "pyth-wti",
		Kind:             config.SourceKindPyth,
		AssetType:        "Commodities",
		RootSymbol:       "WTI",
		QuoteCurrency:    "USD",
		CurveMode:        config.PythCurveModeNearbyLinearRoll,
		RollWindowDays:   5,
		MaxConfidenceBPS: 500,
		RequestTimeout:   100 * time.Millisecond,
		TimestampMode:    config.TimestampModeSourceTimestamp,
	}, HTTPSourceOptions{
		BaseURL:            baseURL,
		Client:             http.DefaultClient,
		Clock:              &mutableClock{now: now},
		StalenessThreshold: 10 * time.Second,
		MaxFutureSkew:      2 * time.Second,
		CooldownJitter:     noJitter,
	})
	if err != nil {
		t.Fatalf("NewPythSource() error = %v", err)
	}

	pyth, ok := source.(*pythSource)
	if !ok {
		t.Fatalf("source type = %T, want *pythSource", source)
	}
	return pyth
}

func writeJSON(t *testing.T, w http.ResponseWriter, body string) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte(strings.TrimSpace(body)))
}
