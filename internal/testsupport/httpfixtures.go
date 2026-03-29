package testsupport

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func NewMarketFeedServer(tb testing.TB) *httptest.Server {
	tb.Helper()
	return httptest.NewServer(newMarketFeedHandler(tb, nil))
}

func NewCountingMarketFeedServer(tb testing.TB, counter *atomic.Int32) *httptest.Server {
	tb.Helper()
	return httptest.NewServer(newMarketFeedHandler(tb, counter))
}

func newMarketFeedHandler(tb testing.TB, counter *atomic.Int32) http.Handler {
	tb.Helper()

	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if counter != nil {
			counter.Add(1)
		}
		switch request.URL.Path {
		case "/products/ETH-USD/ticker":
			writer.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(writer).Encode(map[string]string{
				"bid":   "1981.85",
				"ask":   "1981.95",
				"price": "1981.90",
				"time":  time.Now().UTC().Format(time.RFC3339Nano),
			}); err != nil {
				tb.Fatalf("json.Encode(coinbase) error = %v", err)
			}
		case "/0/public/Ticker":
			if got := request.URL.Query().Get("pair"); got != "ETH/USD" {
				tb.Fatalf("pair query = %q, want ETH/USD", got)
			}
			writer.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(writer).Encode(map[string]any{
				"error": []string{},
				"result": map[string]any{
					"XETHZUSD": map[string]any{
						"a": []string{"1981.95", "1", "1.0"},
						"b": []string{"1981.85", "1", "1.0"},
						"c": []string{"1981.90", "0.5"},
					},
				},
			}); err != nil {
				tb.Fatalf("json.Encode(kraken) error = %v", err)
			}
		case "/info":
			body, err := io.ReadAll(request.Body)
			if err != nil {
				tb.Fatalf("io.ReadAll(hyperliquid) error = %v", err)
			}
			if got := strings.TrimSpace(string(body)); got != `{"type":"l2Book","coin":"ETH"}` {
				tb.Fatalf("hyperliquid request body = %q, want %q", got, `{"type":"l2Book","coin":"ETH"}`)
			}
			writer.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(writer).Encode(map[string]any{
				"coin": "ETH",
				"time": time.Now().UTC().UnixMilli(),
				"levels": [2][]map[string]string{
					{{"px": "1981.85", "sz": "5.0"}},
					{{"px": "1981.95", "sz": "5.0"}},
				},
			}); err != nil {
				tb.Fatalf("json.Encode(hyperliquid) error = %v", err)
			}
		default:
			http.NotFound(writer, request)
		}
	})
}

func NewRecordingSinkServer(tb testing.TB, handler func([]byte, http.ResponseWriter, *http.Request)) *httptest.Server {
	tb.Helper()

	return httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		body, err := io.ReadAll(request.Body)
		if err != nil {
			tb.Fatalf("io.ReadAll() error = %v", err)
		}
		handler(body, writer, request)
	}))
}

func WaitForBody(tb testing.TB, calls <-chan []byte) []byte {
	tb.Helper()

	select {
	case body := <-calls:
		return body
	case <-time.After(2 * time.Second):
		tb.Fatal("sink did not receive a publication")
		return nil
	}
}
