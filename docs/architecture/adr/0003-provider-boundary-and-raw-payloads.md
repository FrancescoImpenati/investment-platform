# ADR 0003: Vendor-neutral provider boundary and scalable raw payloads

- **Status:** Accepted
- **Date:** 2026-08-14
- **Phase:** 0 — Foundation

## Context

The primary provider will be selected only after a bake-off. The platform must preserve provider
payloads before normalization and support future backfill, incremental, and repair requests without
binding business logic to one SDK or forcing arbitrarily large responses into memory.

## Decision

Define a synchronous `MarketDataProvider` protocol for instrument snapshots plus bounded bar and
corporate-action requests. Methods return `Iterable[RawBatch]`, allowing pages to be consumed
progressively.

Separate serializable batch metadata from a `RawPayload` resource that opens a binary reader.
`RawBatchStore` consumes bounded reads while computing size and checksum. Phase 0 supplies an
in-memory payload adapter for fixtures; the contract permits future file-backed or stream-backed
implementations without changing metadata or storage APIs.

Backfill, incremental update, and repair are future planner intents that all map to bounded provider
requests; they are not provider-specific modes.

## Consequences

- Vendor SDKs and wire formats remain at the edge.
- Raw persistence can occur before provider-specific normalization.
- Pagination can be added without changing a `Sequence`-based contract.
- The synchronous interface is intentionally small; concurrency policy remains undecided until the
  bake-off measures latency and rate limits.

## Alternatives considered

- **Call a vendor SDK directly from ingestion/business code:** rejected because it would spread
  provider identifiers and response semantics across the application.
- **Return only canonical `PriceBar` records:** rejected because it would hide or discard the raw
  response before immutable persistence.
- **Store `payload: bytes` on every batch:** rejected because bulk and deeper intraday responses may
  exceed comfortable memory limits.
- **Async, retry, cache, and rate-limit framework now:** deferred until real provider behavior is
  measured.
