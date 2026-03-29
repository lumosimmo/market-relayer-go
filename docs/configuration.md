# Configuration

The runtime config is a strict versioned YAML file. Start from [`configs/markets.example.yaml`](../configs/markets.example.yaml).

The loader rejects unknown fields, missing required values, and deprecated aliases. Effective config changes, including source order, change the config digest used by recovery and verification paths.

## Top-Level Shape

```yaml
version: 1
service:
  name: market-relayer-go
  instance_id: local-dev
store:
  dsn: postgres://postgres:postgres@127.0.0.1:55432/market_relayer_test?sslmode=disable
api:
  listen_addr: 127.0.0.1:8080
sink:
  kind: http
  url: http://127.0.0.1:18080/publish
  request_timeout: 500ms
sources:
  - ...
markets:
  - ...
```

`version` must currently be `1`.

## Core Sections

### Service

| Field                 | Required | Notes                           |
| --------------------- | -------- | ------------------------------- |
| `service.name`        | no       | Defaults to `market-relayer-go` |
| `service.instance_id` | yes      | Surfaced through API metadata   |

### Store

| Field                        | Required | Notes                                            |
| ---------------------------- | -------- | ------------------------------------------------ |
| `store.dsn`                  | yes      | Postgres DSN for runtime state and recovery data |
| `store.max_open_conns`       | no       | Defaults to `4`                                  |
| `store.min_open_conns`       | no       | pgxpool minimum open connections, defaults to `0` |
| `store.conn_max_lifetime`    | no       | Defaults to `30m`                                |
| `store.connect_timeout`      | no       | Defaults to `5s`                                 |
| `store.lock_timeout`         | no       | Defaults to `5s`                                 |
| `store.lease_ttl`            | no       | Defaults to `15s`                                |
| `store.lease_renew_interval` | no       | Defaults to `5s`                                 |

### API

| Field                  | Required | Notes               |
| ---------------------- | -------- | ------------------- |
| `api.listen_addr`      | yes      | TCP listen address accepted by `net.Listen`, for example `127.0.0.1:8080`, `:8080`, or `[::1]:8080` |
| `api.shutdown_timeout` | no       | Defaults to `5s`    |

### Sink

Two sink kinds are supported.

HTTP sink:

```yaml
sink:
  kind: http
  url: http://127.0.0.1:18080/publish
  request_timeout: 500ms
  bearer_token_env: MARKET_RELAYER_SINK_BEARER_TOKEN
```

- `url` must be absolute
- `request_timeout` is required
- `bearer_token_env` is optional, but if set the env var must exist and be non-empty

File sink:

```yaml
sink:
  kind: file
  path: ./tmp/publish.jsonl
  request_timeout: 500ms
```

- appends one compact JSON envelope per line

## Sources

Sources define transport and timestamp behavior. A market then references external sources in the order used by external-price selection, plus one dedicated Hyperliquid perp-book source for internal oracle and basis computation.

Common fields:

| Field               | Required    | Notes                                                         |
| ------------------- | ----------- | ------------------------------------------------------------- |
| `name`              | yes         | Unique across all sources                                     |
| `kind`              | yes         | Built-ins are `coinbase`, `kraken`, `pyth`, and `hyperliquid` |
| `request_timeout`   | yes         | Greater than `0` and at most `1m`                             |
| `timestamp_mode`    | yes         | `source_timestamp` or `received_at`                           |
| `max_unchanged_age` | conditional | Required for `received_at` sources used by markets            |

### Coinbase

```yaml
- name: coinbase-primary
  kind: coinbase
  product_id: ETH-USD
  request_timeout: 1s
  timestamp_mode: source_timestamp
```

- `product_id` is required

### Kraken

```yaml
- name: kraken-secondary
  kind: kraken
  pair: ETH/USD
  request_timeout: 1s
  timestamp_mode: received_at
  max_unchanged_age: 10s
```

- `pair` is required
- Kraken must use `timestamp_mode: received_at`

### Pyth

```yaml
- name: pyth-wti
  kind: pyth
  asset_type: Commodities
  root_symbol: WTI
  quote_currency: USD
  curve_mode: nearby_linear_roll
  roll_window_days: 5
  max_confidence_bps: 250
  request_timeout: 1s
  timestamp_mode: source_timestamp
```

- `asset_type`, `root_symbol`, and `quote_currency` are required
- `curve_mode` must currently be `nearby_linear_roll`
- `roll_window_days` must be greater than `0`
- `max_confidence_bps` must be within `1..10000`
- Pyth must use `timestamp_mode: source_timestamp`

The built-in Pyth adapter resolves candidate contracts from Hermes metadata, applies a nearby linear roll, and can supply scheduled market windows from provider metadata.

### Hyperliquid

```yaml
- name: hyperliquid-wti
  kind: hyperliquid
  market: xyz:CL
  request_timeout: 1s
  timestamp_mode: source_timestamp
```

- `market` is required and must be the Hyperliquid perp identifier passed to `l2Book`
- Hyperliquid book sources must use `timestamp_mode: source_timestamp`
- the adapter fetches the full `l2Book` depth and uses the response timestamp as the source freshness basis

## Markets

Each market defines publish behavior and an ordered source list.

```yaml
markets:
  - symbol: WTIUSD
    session_mode: scheduled
    session_source: pyth-wti
    perp_book_source: hyperliquid-wti
    impact_notional_usd: "10000"
    publish_interval: 3s
    staleness_threshold: 15s
    max_fallback_age: 30s
    clamp_bps: 50
    divergence_threshold_bps: 250
    publish_scale: 2
    sources:
      - name: pyth-wti
```

| Field                      | Required    | Notes                                                               |
| -------------------------- | ----------- | ------------------------------------------------------------------- |
| `symbol`                   | yes         | Must match `^[A-Z0-9_]+$`                                           |
| `session_mode`             | yes         | `always_open` or `scheduled`                                        |
| `session_source`           | conditional | Required for scheduled markets                                      |
| `perp_book_source`         | yes         | Exactly one `hyperliquid` source used for internal oracle and basis |
| `impact_notional_usd`      | yes         | Positive decimal notional used to walk the Hyperliquid book         |
| `publish_interval`         | yes         | Greater than `0` and at most `1m`                                   |
| `staleness_threshold`      | yes         | Greater than `0` and at most `24h`                                  |
| `max_fallback_age`         | yes         | Greater than `0` and at most `24h`                                  |
| `clamp_bps`                | yes         | `0..10000`                                                          |
| `divergence_threshold_bps` | yes         | `0..10000`                                                          |
| `publish_scale`            | yes         | `0..18`                                                             |
| `sources`                  | yes         | Ordered market source references                                    |

Notes:

- source order is part of the pricing contract
- `session_mode` is config, not runtime state
- a scheduled market must name a session-capable external source that is already referenced by the market
- `perp_book_source` is separate from `sources`; it does not participate in external selection
- scheduled tradfi markets freeze `external` when the external venue closes, but `oracle` and `oracle_plus_basis` can continue from the Hyperliquid perp book
- the sample WTI market uses Hyperliquid `xyz:CL` for its perp book

### Market Source References

```yaml
sources:
  - name: kraken-secondary
    allow_timestampless_primary: true
```

`allow_timestampless_primary` is only needed when the first source for a market uses `timestamp_mode: received_at`.

## Validation Rules

The loader rejects:

- unknown fields
- duplicate source names or market symbols
- unsupported source kinds
- missing source references
- invalid durations, scales, or basis-point values
- invalid `listen_addr`
- missing required sink or store settings
- scheduled markets without a valid `session_source`
- `received_at` primary sources without `max_unchanged_age`
- `received_at` primary sources without `allow_timestampless_primary`
- missing bearer-token environment variables when `bearer_token_env` is set

## Metadata Locks

At boot the app computes a SHA-256 digest of the normalized config and pairs it with pricing, schema, and envelope version metadata. Recovery paths use those values to decide whether a stored record can be verified or replayed under the current binary.

- pricing algorithm version
- store schema version
- envelope version

Those metadata fields are persisted with cycle records and publications. Exact verification and pending replay rely on them to decide whether old state is still safe to trust.
