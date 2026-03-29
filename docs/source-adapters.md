# Source Adapters

This guide is for adding a new upstream market-data source.

The runtime config only accepts the built-in `source.kind` values today, but the code is split so new kinds can be added without touching the relayer loop or pricing engine.

## Where Source Logic Lives

- `internal/config`: source-kind-specific config fields and validation
- `internal/feeds/registry.go`: adapter registration keyed by `source.kind`
- `internal/feeds/http_requester.go` and `internal/feeds/http_source.go`: shared HTTP transport, retries, cooldowns, freshness, and freeze detection
- provider files in `internal/feeds/`: request building, metadata handling, and payload decoding
- `internal/feeds/book.go`: shared `BookSnapshot` and `BookLevel` types for full-depth perp books
- `SessionAwareSource`: optional interface for sources that can drive scheduled market windows

There are two common adapter shapes:

- quote adapters that implement `Source` and return one normalized `feeds.Quote`
- perp-book adapters that implement `BookSource` and return one normalized `feeds.BookSnapshot`

The built-in split today is:

- external quote sources: `coinbase`, `kraken`, and `pyth`
- perp-book sources: `hyperliquid`

## Adding An External Quote Source

1. Add provider-specific config fields to `config.SourceConfig`.
2. Add validation in `internal/config/source_kind.go`.
3. Register the kind in the built-in source-kind list.
4. Add a provider file in `internal/feeds/` for URL construction and payload decoding.
5. Register the adapter builder in `internal/feeds/registry.go`.
6. Add tests for config validation, registration, payload decoding, and timestamp behavior.

External quote adapters plug into `builtInSourceAdapters`, are built through `NewSource`, and participate in the ordered `markets[].sources` selection path.

## Adding A Perp-Book Source

1. Add provider-specific config fields to `config.SourceConfig`.
2. Add validation in `internal/config/source_kind.go`.
3. Register the kind in `builtInBookSourceAdapters`.
4. Add a provider file in `internal/feeds/` that implements `FetchBook`.
5. Decode the upstream payload into a `feeds.BookSnapshot` with full bid and ask depth.
6. Add tests for config validation, registration, payload decoding, timestamp behavior, and malformed book levels.

Perp-book adapters are built through `BuildPerpBookSource`, fetched in parallel with the external quote set, and persisted into each cycle record as raw book input for deterministic replay.

## Adding A Synthetic Or Session-Aware Quote Source

1. Add provider-specific config and validation in `internal/config`.
2. Decide whether the adapter should implement `SessionAwareSource`.
3. Reuse the shared HTTP requester for multi-endpoint fetches.
4. Keep provider logic focused on metadata discovery, instrument selection, schedule parsing, and quote synthesis.
5. Register the builder in `internal/feeds/registry.go`.
6. Add tests for metadata filtering, synthesized quote behavior, and session-window handling.

## Guardrails

An adapter should decide:

- where to fetch from
- how to decode provider responses
- whether the source uses `source_timestamp` or `received_at`
- whether it returns a `Quote` or a `BookSnapshot`
- whether it can expose runtime session windows

An adapter should not duplicate:

- retry policy
- timeout budgeting
- cooldown handling
- staleness validation
- freeze detection
- shared quote or book normalization

Those behaviors belong in the shared feed and relayer layers.

## Hyperliquid Notes

The built-in `hyperliquid` adapter is a perp-book source, not an external quote source.

- it requires `source.market`, for example `ETH` or `xyz:CL`
- it posts `{"type":"l2Book","coin":"..."}` to Hyperliquid `/info`
- it uses the response `time` field as the source freshness basis
- it returns the full decoded bid and ask depth as `BookSnapshot`

Markets reference that source through `perp_book_source`; it does not participate in external source ordering.
