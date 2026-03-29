package testsupport

import (
	"os"
	"path/filepath"
	"testing"
)

const PlaceholderStoreDSN = "postgres://postgres:postgres@127.0.0.1:1/market_relayer_test?sslmode=disable"

func WriteConfig(tb testing.TB, contents string) string {
	tb.Helper()

	path := filepath.Join(tb.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		tb.Fatalf("os.WriteFile() error = %v", err)
	}
	return path
}

func ValidBootstrapConfig(tb testing.TB) string {
	tb.Helper()
	return ValidBootstrapConfigWithStoreDSN(PlaceholderStoreDSN)
}

func ValidBootstrapConfigWithStoreDSN(storeDSN string) string {
	return `version: 1
service:
  name: market-relayer-go
  instance_id: bootstrap-test
store:
  dsn: "` + storeDSN + `"
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
`
}
