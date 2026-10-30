import type { SerializedError } from '../core/errors.js';
import type { HistoryEvent, SignalReceived, StoredEvent } from '../core/events.js';

export type WorkflowStatus = 'pending' | 'suspended' | 'completed' | 'failed';

export interface Lease {
  readonly owner: string;
  /** Fencing token: changes on every claim, so a stale holder can never write. */
  readonly token: string;
  readonly expiresAt: number;
}

/** The materialised view of an instance; history remains the source of truth. */
export interface WorkflowInstance {
  readonly id: string;
  readonly workflowName: string;
  readonly workflowVersion: number;
  readonly status: WorkflowStatus;
  readonly input: unknown;
  readonly output: unknown;
  readonly error: SerializedError | null;
  /** When the instance should next be activated; `null` means only a signal can wake it. */
  readonly wakeAt: number | null;
  /** Incremented on every delivered signal; used to prevent lost wake-ups. */
  readonly signalCount: number;
  readonly lease: Lease | null;
  readonly createdAt: number;
  readonly updatedAt: number;
}

export interface NewWorkflowInstance {
  readonly id: string;
  readonly workflowName: string;
  readonly workflowVersion: number;
  readonly input: unknown;
  readonly now: number;
}

export interface ClaimRequest {
  readonly now: number;
  readonly limit: number;
  readonly workerId: string;
  readonly leaseMs: number;
}

export interface Claim {
  readonly instance: WorkflowInstance;
  readonly leaseToken: string;
}

export interface AppendEventsRequest {
  readonly instanceId: string;
  readonly leaseToken: string;
  readonly events: readonly HistoryEvent[];
  readonly now: number;
  /** Writing progress also renews the lease. */
  readonly leaseExpiresAt: number;
}

export interface HeartbeatRequest {
  readonly instanceId: string;
  readonly leaseToken: string;
  readonly leaseExpiresAt: number;
}

export interface NextState {
  readonly status: WorkflowStatus;
  readonly wakeAt: number | null;
  readonly output?: unknown;
  readonly error?: SerializedError;
}

export interface CompleteActivationRequest {
  readonly instanceId: string;
  readonly leaseToken: string;
  readonly events: readonly HistoryEvent[];
  readonly next: NextState;
  readonly now: number;
  /** `signalCount` as seen when the instance was claimed. */
  readonly observedSignalCount: number;
}

export interface DeliverSignalRequest {
  readonly instanceId: string;
  readonly signal: Omit<SignalReceived, 'type'>;
  readonly now: number;
}

/**
 * Persistence port. Implementations must make each method atomic; the engine relies on
 * that (plus fencing tokens) for correctness when many workers share one store.
 *
 * Every implementation is verified by the shared contract suite in `test/store-contract.ts`.
 */
export interface WorkflowStore {
  /** Idempotent: returns the existing instance with `created: false` if the id is taken. */
  createInstance(
    instance: NewWorkflowInstance,
  ): Promise<{ readonly created: boolean; readonly instance: WorkflowInstance }>;
  getInstance(instanceId: string): Promise<WorkflowInstance | undefined>;
  getHistory(instanceId: string): Promise<StoredEvent[]>;
  /** Atomically leases up to `limit` due instances, oldest `wakeAt` first. */
  claimDue(request: ClaimRequest): Promise<Claim[]>;
  /** @throws LeaseLostError when `leaseToken` is no longer current. */
  appendEvents(request: AppendEventsRequest): Promise<void>;
  /** @throws LeaseLostError when `leaseToken` is no longer current. */
  heartbeat(request: HeartbeatRequest): Promise<void>;
  /** Writes final events and the next state, and releases the lease. */
  completeActivation(request: CompleteActivationRequest): Promise<void>;
  /**
   * @throws WorkflowNotFoundError for unknown instances.
   * @throws WorkflowClosedError when the instance has already finished.
   */
  deliverSignal(request: DeliverSignalRequest): Promise<void>;
}
