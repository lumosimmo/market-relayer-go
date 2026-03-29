version: 1
service:
  name: market-relayer-go
  instance_id: ${MARKET_RELAYER_INSTANCE_ID}
store:
  dsn: ${MARKET_RELAYER_STORE_DSN}
  max_open_conns: ${MARKET_RELAYER_STORE_MAX_OPEN_CONNS}
  min_open_conns: ${MARKET_RELAYER_STORE_MIN_OPEN_CONNS}
  conn_max_lifetime: ${MARKET_RELAYER_STORE_CONN_MAX_LIFETIME}
  connect_timeout: ${MARKET_RELAYER_STORE_CONNECT_TIMEOUT}
  lock_timeout: ${MARKET_RELAYER_STORE_LOCK_TIMEOUT}
  lease_ttl: ${MARKET_RELAYER_STORE_LEASE_TTL}
  lease_renew_interval: ${MARKET_RELAYER_STORE_LEASE_RENEW_INTERVAL}
api:
  listen_addr: ${MARKET_RELAYER_API_LISTEN_ADDR}
sink:
  kind: http
  url: ${MARKET_RELAYER_SINK_URL}
  request_timeout: 500ms
  bearer_token_env: MARKET_RELAYER_SINK_BEARER_TOKEN
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
