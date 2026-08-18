# ADR 0001: Local-first modular monolith

- **Status:** Accepted
- **Date:** 2026-08-14
- **Phase:** 0 — Foundation

## Context

The platform must grow from a personal daily research tool into a credible professional project.
Its first workloads are local batch and analytical workflows, not independent services with
different scaling or deployment requirements.

## Decision

Build a Python-first, local-first modular monolith. Keep domain areas in purposeful package modules
and enforce boundaries between domain models, provider adapters, validation, storage, analytics,
and later application concerns. Run the Phase 0 workflow on one machine without Docker, service
discovery, queues, or a network deployment.

Create a new package only when it owns implemented behavior or a concrete contract; do not mirror
the long-term roadmap with empty directories.

## Consequences

- Setup, debugging, tests, and data inspection remain simple.
- Module boundaries can be exercised before there is evidence that process boundaries are useful.
- Local-first does not forbid later cloud execution or a transactional database; those additions
  require demonstrated operational needs.
- A future extraction into services would require explicit APIs and deployment work, but no such
  cost is justified in Phase 0.

## Alternatives considered

- **Microservices now:** rejected because they add deployment, networking, observability, and data
  consistency work without an independent scaling need.
- **Notebook-only project:** rejected because it weakens reusable contracts, testing, and portfolio
  quality. Notebooks may later consume the package for exploration.
- **Docker-first development:** deferred until a real service or reproducibility gap requires it.
