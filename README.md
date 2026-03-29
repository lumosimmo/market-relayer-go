# market-relayer-go

`market-relayer-go` is a market data relayer designed for hyperliquid-like perps exchanges, built around deterministic pricing and explicit durability boundaries. It polls prices from various external feeds, computes prices, stores replayable state, and publishes restart-safe envelopes on a fixed cadence under per-market lease ownership.

## Features

- Deterministic multi-source pricing from Coinbase, Kraken, and Pyth inputs
- Easily extendable adapter model for new sources
- Nearby-contract linear roll and confidence-checked synthetic pricing for futures-style reference feeds (Pyth Hermes for now)
- Market session handling for both always-open markets (crypto) and provider-driven scheduled markets (tradfi)
- Restart-safe publication flow with durable checkpoints, replayable state, and per-market HA lease ownership
- Publication to various sink types
- Recovery workflows for exact verification, audit recomputation, and byte-for-byte replay of pending publications

## Price Model

Each price is represented as a signed integer plus an explicit scale. Decimal strings are parsed once, kept at source scale, and normalized into the market publish scale with the shared rounding mode `half_away_from_zero`.

On perps exchanges like Hyperliquid, the [exchange-side mark](https://docs.trade.xyz/perp-mechanics/mark-price) incorporates its own internal median of best bid, best ask, and last trade. This service publishes the other two prices that the exchange needs to derive its mark price: `oracle` and `oracle_plus_basis`.

The service computes three prices:

- `external`: the externally-derived close used while external markets are fresh; it freezes at the last good close when external inputs go stale or scheduled tradfi sessions close
- `oracle`: the validated reference price that follows fresh external selection and otherwise continues from the configured Hyperliquid perp `l2Book` using impact-price EMA
- `oracle_plus_basis`: `oracle` plus an EMA of `(perp_mid - oracle)` from the same Hyperliquid perp book

External selection follows a fixed order:

- one healthy source: use its midpoint
- two healthy sources: use their midpoint only when the pair stays within the configured divergence threshold
- three or more healthy sources: compute a provisional median, keep survivors within threshold of that median, then re-run the one/two/many-source rules on the survivor set

Configured source order is preserved end to end. Map iteration never decides winners, survivor sets, replay order, or publication identity.

Each market also names one Hyperliquid perp-book source separately from its ordered external sources. For example, the sample WTI market uses the Hyperliquid `xyz:CL` perp book via `l2Book` while still taking its external session and reference price from Pyth.

## Determinism And Recovery

We use 2 hashes to anchor things:

- `stable_hash`: built from the published health, session, selection, and price fields; used for dedupe so timestamp-only churn does not republish
- `payload_hash`: built from the full payload; used in the publication identity and sink acknowledgements

Publication safety looks like this:

1. The relayer computes a cycle and optionally materializes a prepared envelope.
2. The cycle record, latest pricing state, latest publishability, and prepared publication commit in one Postgres transaction while the current market lease is fenced.
3. The sink call happens only after that durable prepare step succeeds.
4. Safe sequence advances only after the ack checkpoint is durably persisted.

Recovery modes are:

- `exact`: read-only verification of one stored record when config digest, pricing algorithm version, schema version, and envelope version all match
- `audit`: read-only recomputation and comparison, with mismatches reported explicitly
- `resume-pending`: the only sink-writing mode; it replays stored prepared envelopes byte-for-byte and refuses to recompute them under new metadata

## Quick Start

Use Go `1.26` or newer.

The sample config at [`configs/markets.example.yaml`](./configs/markets.example.yaml) expects Postgres on `127.0.0.1:55432` and an HTTP sink on `127.0.0.1:18080`.

```bash
docker compose -f docker-compose.test.yml up -d postgres
go run ./cmd/sink-printer
make migrate-store
make run
```

For the simplest local path, switch the sample config to a file sink and skip `sink-printer`.

Use `make validate-config` to check the sample config before running. For recovery commands, alternative run modes, Compose targets, and endpoint details, see [`docs/operations.md`](./docs/operations.md). For config shape, see [`docs/configuration.md`](./docs/configuration.md).

## HTTP Surface

| Endpoint                   | Purpose                                                                                   |
| -------------------------- | ----------------------------------------------------------------------------------------- |
| `/healthz`                 | Process liveness plus store and lease-controller health                                   |
| `/readyz`                  | Boot readiness only: config loaded, store open, sink initialized, loops started           |
| `/statusz`                 | Per-market state, ownership, lease metadata, and price availability reasons               |
| `/snapshotz?market=ETHUSD` | Latest persisted snapshot, lease metadata, recent cycles, and last successful publication |
| `/metrics`                 | Prometheus metrics                                                                        |

## Verification Assets

The repository includes a small frozen fixture harness for recovery and cycle-budget coverage:

```bash
make test
make test-local
make smoke-fixtures
make bench
```

- `make test` runs the default Postgres-backed suite in Docker so the store and lease paths are exercised
- `make test-local` runs `go test ./...` directly for a faster host-only loop with `MARKET_RELAYER_SKIP_POSTGRES_TESTS=1`, so Postgres-backed tests are skipped intentionally
- plain `go test ./...` now expects Postgres-backed tests to be runnable; if you want to skip them on purpose, set `MARKET_RELAYER_SKIP_POSTGRES_TESTS=1`
- `make smoke-fixtures` runs the end-to-end fixture harness: happy path, fallback transition, exact verification, pending-publication recovery, and duplicate-ack crash recovery
- `make bench` runs the fixture-driven cycle-budget assertions and benchmark

## Caveats

- persistence currently has no built-in retention or pruning workflow
- the HTTP sink currently only supports a bearer token loaded at boot
- sink retry handling is only budget-bounded, without more advanced backoff and circuit-breaker patterns yet
