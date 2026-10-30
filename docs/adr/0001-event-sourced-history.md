# ADR 0001: Event-sourced history as the single source of truth

- Status: Accepted
- Date: 2026-10-30

## Context

A durable workflow must survive process crashes at any point and resume with the same state.
Options for persisting that state:

1. **Snapshot the workflow state** (local variables, position) after each step.
2. **Explicit state machines** where developers model states and transitions by hand.
3. **Event sourcing**: record what happened (step completed, timer fired, signal received) and
   rebuild state by replaying the workflow code against that log.

JavaScript cannot serialise a suspended `async` function's stack, which rules out true snapshots.
Hand-written state machines work but push the complexity onto every workflow author and make
business logic hard to read.

## Decision

Each instance has an append-only, ordered history of typed events (`src/core/events.ts`).
History is the source of truth. The instance row (status, `wakeAt`, lease) is a derived view used
only for scheduling.

## Consequences

- Workflow code stays plain `async`/`await`, readable as the business process it describes.
- A complete audit trail comes for free: every attempt, error, timer and signal, with timestamps.
- Debugging is reproducible: replaying a history reproduces the exact decisions.
- Workflow code must be deterministic (see ADR 0002), which authors must learn.
- Histories grow with the number of operations; very long-lived workflows will need
  `continueAsNew` (roadmap).
