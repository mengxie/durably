import { randomUUID } from 'node:crypto';
import { LeaseLostError, WorkflowClosedError, WorkflowNotFoundError } from '../core/errors.js';
import type { HistoryEvent, StoredEvent } from '../core/events.js';
import { isClaimable, isTerminal, nextWakeAt, wakeAtAfterSignal } from './store-rules.js';
import type {
  AppendEventsRequest,
  Claim,
  ClaimRequest,
  CompleteActivationRequest,
  DeliverSignalRequest,
  HeartbeatRequest,
  Lease,
  NewWorkflowInstance,
  WorkflowInstance,
  WorkflowStore,
} from './workflow-store.js';

type Mutable<T> = { -readonly [K in keyof T]: T[K] };

export interface InMemoryWorkflowStoreOptions {
  readonly tokenGenerator?: () => string;
}

/**
 * Process-local store for tests, demos and single-process deployments. Every method runs
 * synchronously to completion, which makes each one atomic on the JavaScript event loop.
 * Values are deep-cloned on the way in and out to mimic real persistence.
 */
export class InMemoryWorkflowStore implements WorkflowStore {
  readonly #instances = new Map<string, Mutable<WorkflowInstance>>();
  readonly #events = new Map<string, StoredEvent[]>();
  readonly #newToken: () => string;

  constructor(options: InMemoryWorkflowStoreOptions = {}) {
    this.#newToken = options.tokenGenerator ?? randomUUID;
  }

  createInstance(
    request: NewWorkflowInstance,
  ): Promise<{ created: boolean; instance: WorkflowInstance }> {
    const existing = this.#instances.get(request.id);
    if (existing) return Promise.resolve({ created: false, instance: structuredClone(existing) });

    const instance: Mutable<WorkflowInstance> = {
      id: request.id,
      workflowName: request.workflowName,
      workflowVersion: request.workflowVersion,
      status: 'pending',
      input: structuredClone(request.input),
      output: undefined,
      error: null,
      wakeAt: request.now,
      signalCount: 0,
      lease: null,
      createdAt: request.now,
      updatedAt: request.now,
    };
    this.#instances.set(request.id, instance);
    this.#events.set(request.id, []);
    this.#append(request.id, request.now, [
      {
        type: 'WorkflowStarted',
        workflowName: request.workflowName,
        workflowVersion: request.workflowVersion,
        input: request.input,
      },
    ]);
    return Promise.resolve({ created: true, instance: structuredClone(instance) });
  }

  getInstance(instanceId: string): Promise<WorkflowInstance | undefined> {
    const instance = this.#instances.get(instanceId);
    return Promise.resolve(instance ? structuredClone(instance) : undefined);
  }

  getHistory(instanceId: string): Promise<StoredEvent[]> {
    return Promise.resolve(structuredClone(this.#events.get(instanceId) ?? []));
  }

  claimDue(request: ClaimRequest): Promise<Claim[]> {
    const due = [...this.#instances.values()]
      .filter((instance) => isClaimable(instance, request.now))
      .sort((a, b) => (a.wakeAt ?? 0) - (b.wakeAt ?? 0))
      .slice(0, request.limit);

    const claims = due.map((instance) => {
      const token = this.#newToken();
      instance.lease = {
        owner: request.workerId,
        token,
        expiresAt: request.now + request.leaseMs,
      };
      instance.updatedAt = request.now;
      return { instance: structuredClone(instance), leaseToken: token };
    });
    return Promise.resolve(claims);
  }

  appendEvents(request: AppendEventsRequest): Promise<void> {
    return this.#run(() => {
      const { instance, lease } = this.#requireLease(request.instanceId, request.leaseToken);
      this.#append(request.instanceId, request.now, request.events);
      instance.lease = { ...lease, expiresAt: request.leaseExpiresAt };
      instance.updatedAt = request.now;
    });
  }

  heartbeat(request: HeartbeatRequest): Promise<void> {
    return this.#run(() => {
      const { instance, lease } = this.#requireLease(request.instanceId, request.leaseToken);
      instance.lease = { ...lease, expiresAt: request.leaseExpiresAt };
    });
  }

  completeActivation(request: CompleteActivationRequest): Promise<void> {
    return this.#run(() => {
      const { instance } = this.#requireLease(request.instanceId, request.leaseToken);
      this.#append(request.instanceId, request.now, request.events);
      instance.status = request.next.status;
      instance.wakeAt = nextWakeAt(request, instance.signalCount);
      if ('output' in request.next) instance.output = structuredClone(request.next.output);
      if (request.next.error) instance.error = structuredClone(request.next.error);
      instance.lease = null;
      instance.updatedAt = request.now;
    });
  }

  deliverSignal(request: DeliverSignalRequest): Promise<void> {
    return this.#run(() => {
      const instance = this.#instances.get(request.instanceId);
      if (!instance) throw new WorkflowNotFoundError(request.instanceId);
      if (isTerminal(instance.status)) {
        throw new WorkflowClosedError(request.instanceId, instance.status);
      }
      this.#append(request.instanceId, request.now, [
        { type: 'SignalReceived', ...request.signal },
      ]);
      instance.signalCount += 1;
      instance.wakeAt = wakeAtAfterSignal(instance.wakeAt, request.now);
      instance.updatedAt = request.now;
    });
  }

  #append(instanceId: string, now: number, events: readonly HistoryEvent[]): void {
    const stream = this.#events.get(instanceId);
    if (!stream) throw new WorkflowNotFoundError(instanceId);
    for (const event of events) {
      stream.push({ ...structuredClone(event), seq: stream.length + 1, at: now });
    }
  }

  #requireLease(
    instanceId: string,
    leaseToken: string,
  ): { instance: Mutable<WorkflowInstance>; lease: Lease } {
    const instance = this.#instances.get(instanceId);
    if (!instance) throw new WorkflowNotFoundError(instanceId);
    const { lease } = instance;
    if (lease?.token !== leaseToken) throw new LeaseLostError(instanceId);
    return { instance, lease };
  }

  /** Runs a synchronous mutation and surfaces any error as a rejected promise. */
  #run(mutation: () => void): Promise<void> {
    try {
      mutation();
      return Promise.resolve();
    } catch (error) {
      return Promise.reject(error instanceof Error ? error : new Error(String(error)));
    }
  }
}
