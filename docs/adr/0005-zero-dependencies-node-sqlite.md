# ADR 0005: Zero runtime dependencies; `node:sqlite` for durable storage

- Status: Accepted
- Date: 2026-10-30

## Context

Every runtime dependency adds supply-chain risk, upgrade work and install weight to the projects
that adopt a library. A durable store is still needed for anything beyond tests. Candidates were
`better-sqlite3` (fast and mature, but a native add-on with prebuilt binaries), an external
database client, or Node's built-in `node:sqlite` (available without flags since Node 22.13).

## Decision

- The library has **no runtime dependencies**. Everything it needs comes from Node itself
  (`node:crypto`, `node:timers/promises`, `node:sqlite`).
- The durable adapter uses `node:sqlite`, exposed through a separate entry point
  (`@mengxie/durably/sqlite`) so applications using another store never load it.
- Storage is a port (`WorkflowStore`). Additional adapters can live in separate packages with
  their own dependencies.

## Consequences

- `npm install` pulls in nothing at runtime; the minimum Node version is 22.13.
- `node:sqlite` is still marked experimental and prints a warning on first use. Its API is small
  and the adapter isolates it, so a future change is contained to one file.
- SQLite fits a single host. Multi-host deployments need a networked adapter (Postgres is the
  first item on the roadmap) that must pass the same contract suite.
