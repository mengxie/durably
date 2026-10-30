# ADR 0002: Replay-based execution, suspending with never-settling promises

- Status: Accepted
- Date: 2026-10-30

## Context

With history as the source of truth (ADR 0001), an activation re-runs the workflow function from
the start and short-circuits operations whose outcome is already recorded. When it reaches an
operation that cannot complete yet (a timer that is not due, a missing signal), it must stop the
workflow without completing or failing it.

Common approaches:

1. **Throw a sentinel exception** and catch it at the top. Simple, but any `try/catch` in workflow
   code (which is common: "try the payment, fall back on failure") can swallow it and continue
   with corrupted control flow.
2. **Generators** (`function*` with `yield`). Robust, but unfamiliar and awkward with `Promise.all`.
3. **Sandboxed deterministic runtimes** (as Temporal does with V8 isolates). Very robust, but
   heavy and complex.
4. **Return a promise that never settles** and race the workflow body against an interruption
   signal.

## Decision

Option 4. A blocked operation records an interruption and returns a fresh never-settling promise.
The activation races the body against the first interruption, then waits for quiescence (no step
in flight, microtasks drained) before choosing the outcome.

Determinism is enforced by assigning operations sequential indices and checking each operation's
kind and name against history; mismatches raise `NonDeterminismError`.

## Consequences

- `try/catch` in workflow code cannot interfere with suspension.
- Ordinary `Promise.all` / `Promise.race` work for parallelism and timeouts.
- A fresh promise per suspension (not a shared singleton) is required to avoid accumulating
  reactions on a long-lived object; abandoned replays are garbage-collected.
- Workflow code must only await durable operations. Awaiting arbitrary I/O in the workflow body
  would break both determinism and quiescence detection; such I/O belongs in steps. This is
  documented, not enforced by a sandbox.
