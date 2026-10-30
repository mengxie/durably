# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses
[Semantic Versioning](https://semver.org/).

## [0.1.0] - 2026-10-30

### Added

- Workflow engine with event-sourced history and deterministic replay.
- Durable steps with retry policies (exponential backoff, jitter, non-retryable errors) and timeouts.
- Durable timers, signals with timeouts, and saga compensation.
- Non-determinism detection and side-by-side workflow versions.
- Multi-worker execution with leases, heartbeats and fencing tokens.
- In-memory and SQLite (`node:sqlite`) stores, verified by a shared contract suite.
- `ManualClock` and `runUntilIdle()` for fast, deterministic workflow tests.
- Order-fulfilment saga example, architecture guide and ADRs.
