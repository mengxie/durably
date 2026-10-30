# ADR 0003: At-least-once steps with per-step checkpoints and idempotency keys

- Status: Accepted
- Date: 2026-10-30

## Context

A step performs a side effect (charge a card, send an email) and then its result is persisted.
A crash can always happen between the two. No system can make an arbitrary external side effect
exactly-once on its own; the realistic choices are at-most-once (risking lost work) or
at-least-once (risking duplicates).

The other question is _when_ to persist: once per activation (cheaper) or after every step.

## Decision

- Steps are **at least once**. Losing a payment or a shipment is worse than a duplicate that the
  downstream system can detect.
- Every step result is **checkpointed immediately**, so a crash re-executes at most the step that
  was running, not everything since the activation began. The checkpoint also renews the lease.
- Each step receives an **`idempotencyKey`** (`<instanceId>:<op>`) that is stable across retries
  and replays. Passing it to downstream APIs (most payment providers accept one) turns
  at-least-once into effectively-once.
- Step results are normalised through JSON on first execution, so the live value and the replayed
  value are identical (for example, a `Date` is a string both times).

## Consequences

- One write per step instead of per activation. Acceptable for the workflow granularity this
  library targets (business steps, not tight loops).
- Step authors must make side effects idempotent or pass the key along. The README and the
  example demonstrate this.
- The crash-recovery test (`test/resilience.test.ts`) asserts the exact re-execution boundary.
