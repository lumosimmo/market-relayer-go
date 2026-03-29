# Operations

This guide covers the common runtime commands, endpoint behavior, recovery modes, and verification assets.

## Local Run

`make run` uses [`configs/markets.example.yaml`](../configs/markets.example.yaml). The sample config expects:

- Postgres on `127.0.0.1:55432`
- an HTTP sink on `127.0.0.1:18080`, or a local switch to the file sink
- Hyperliquid `l2Book` access for the configured perp-book sources
- for the sample WTI market, the Hyperliquid perp book is `xyz:CL`

Bring up Postgres:

```bash
docker compose -f docker-compose.test.yml up -d postgres
```

If your local Postgres state is stale or incompatible, reset it completely before rerunning migrations:

```bash
docker compose -f docker-compose.test.yml down -v
docker compose -f docker-compose.test.yml up -d postgres
```

Optional local sink:

```bash
go run ./cmd/sink-printer
```

Then:

```bash
make migrate-store
make run
```

For the simplest local path, switch the sink to `kind: file`.

If you want local runs to suppress the per-cycle info chatter on `stderr`, use `-quiet`. That keeps warnings and errors while leaving sink output unchanged.

```bash
go run ./cmd/market-relayer -quiet -config configs/markets.example.yaml
```

Use `-log-level debug` when you want the inverse and need extra runtime detail.

## Common Commands

```bash
go run ./cmd/market-relayer -validate-config -config configs/markets.example.yaml
go run ./cmd/market-relayer -migrate-store -config configs/markets.example.yaml
go run ./cmd/market-relayer -store-version -config configs/markets.example.yaml
go run ./cmd/market-relayer -config configs/markets.example.yaml
go run ./cmd/market-relayer -quiet -config configs/markets.example.yaml
go run ./cmd/market-relayer -log-level debug -config configs/markets.example.yaml
```

Recovery commands:

```bash
go run ./cmd/market-relayer -recover exact -market ETHUSD -record-id <record-id> -config configs/markets.example.yaml
go run ./cmd/market-relayer -recover audit -market ETHUSD -record-id <record-id> -config configs/markets.example.yaml
go run ./cmd/market-relayer -recover resume-pending -market ETHUSD -config configs/markets.example.yaml
```

Useful make targets:

```bash
make validate-config
make migrate-store
make test
make test-local
make test-compose
make smoke-compose
make integration-compose
make smoke-fixtures
make bench
```

- `make test` is the default full suite and runs the Postgres-backed tests in Docker
- `make test-local` runs `go test ./...` directly for a quicker host-only pass and sets `MARKET_RELAYER_SKIP_POSTGRES_TESTS=1` so Postgres-backed tests are skipped intentionally

If you run `go test ./...` yourself, Postgres-backed tests are expected to run. To skip them on purpose for a lighter host-only pass, set `MARKET_RELAYER_SKIP_POSTGRES_TESTS=1`.

## Endpoints

| Endpoint                | Meaning                                                                   |
| ----------------------- | ------------------------------------------------------------------------- |
| `/healthz`              | Liveness of the process, store, and lease controllers                     |
| `/readyz`               | Boot readiness                                                            |
| `/statusz`              | Current market state, ownership, session reason, and availability details |
| `/snapshotz?market=...` | Latest persisted state and recent cycle history for one market            |
| `/metrics`              | Prometheus metrics                                                        |

`/snapshotz` auto-selects the only configured market when exactly one market exists.

Per-market status collapses into:

- `fresh`
- `fallback_only`
- `unavailable`
- `unpublishable`

`unpublishable` is reserved for store or sink durability problems. `unavailable` means the market currently cannot produce a publishable oracle from the available inputs.

## Recovery Modes

### `exact`

Read-only verification of a stored record. It requires matching config digest, pricing version, schema version, and envelope version.

### `audit`

Read-only recomputation and comparison. When metadata matches and a prepared publication exists, audit also compares the rebuilt envelope byte-for-byte.

### `resume-pending`

The only sink-writing recovery path. It:

1. loads pending `prepared` or `sent` publications
2. validates stored metadata against the running config and version locks
3. republishes the stored envelope byte-for-byte
4. persists `sent` and `acked`
5. reports the next safe sequence

If metadata does not match, replay stops. Pending recovery never regenerates a payload.

## Sink Behavior

### HTTP sink

Sends a `POST` with the prepared envelope and includes:

- `Content-Type: application/json`
- `Idempotency-Key`
- `X-Payload-Hash`
- `Authorization: Bearer ...` when `bearer_token_env` is configured

Response handling:

- `200`, `201`, `204`: success
- `409`: duplicate ack if the ack still matches the prepared identity
- `5xx` and transport timeouts: retryable
- other `4xx`: terminal

### File sink

Appends one compact envelope JSON document per line and returns a synthetic acknowledgement immediately.

## Verification Assets

`make smoke-fixtures` and `make bench` share one frozen fixture set so the recovery scenarios and cycle-budget assertions stay aligned.

- `make smoke-fixtures` runs the end-to-end fixture harness
- `make bench` runs fixture-driven cycle-budget assertions and the benchmark

The repository also ships:

- [`docker-compose.test.yml`](../docker-compose.test.yml) for Postgres-backed integration and HA smoke coverage
- [`docker-compose.prod-template.yml`](../docker-compose.prod-template.yml) as a production-like multi-replica reference stack
- [`.env.example`](../.env.example) for the template environment
