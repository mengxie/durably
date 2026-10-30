# ADR 0004: Leases with fencing tokens for multi-worker safety

- Status: Accepted
- Date: 2026-10-30

## Context

Several workers (threads, processes or hosts) share one store for throughput and availability.
Exactly one worker must drive an instance at a time, and a crashed worker's instances must be
picked up by others.

Leases with expiry handle crashes, but not _stalls_: a worker can pause (GC, CPU starvation,
network partition) past its lease expiry, another worker takes over, and then the first one wakes
up and writes stale results. Expiry alone cannot prevent this, because the stalled worker does not
know time has passed.

## Decision

- `claimDue` atomically leases instances and issues a **new random token per claim**.
- Every write (`appendEvents`, `heartbeat`, `completeActivation`) must present the current token
  and fails with `LeaseLostError` otherwise. The token, not the clock, decides who may write.
- Writing a checkpoint renews the lease; a heartbeat every third of the lease duration renews it
  during long steps. A failed heartbeat aborts the activation.
- Leases are only renewed by their holder, so expiry is still how crashed workers are detected.

## Consequences

- Correct under stalls and partitions, as long as the store checks the token atomically with the
  write. Both adapters do, and the contract suite verifies it.
- A stalled worker may still have performed a side effect before discovering it lost the lease;
  that is the at-least-once case of ADR 0003, mitigated by idempotency keys.
- Lease duration trades failover speed against heartbeat traffic; it defaults to 30 seconds and
  must be at least 1 second.
