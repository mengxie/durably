import { randomUUID } from 'node:crypto';
import { setTimeout as delay } from 'node:timers/promises';
import { systemClock, type Clock } from '../core/clock.js';
import { toMilliseconds, type Duration } from '../core/duration.js';
import {
  ConfigurationError,
  ResultTimeoutError,
  WorkflowAlreadyExistsError,
  WorkflowFailedError,
  WorkflowNotFoundError,
  WorkflowNotRegisteredError,
} from '../core/errors.js';
import type { StoredEvent } from '../core/events.js';
import { DEFAULT_RETRY_POLICY, resolveRetryPolicy, type RetryPolicy } from '../core/retry.js';
import type { AnyWorkflowDefinition, WorkflowDefinition } from '../core/workflow.js';
import { toJsonValue } from '../internal/json.js';
import type {
  Claim,
  NextState,
  WorkflowInstance,
  WorkflowStore,
} from '../storage/workflow-store.js';
import { Activation, type ActivationOutcome } from './activation.js';
import type { WorkflowLogEntry } from './replay-context.js';
import { Worker, type WorkerOptions } from './worker.js';

export interface ActivationReport {
  readonly instanceId: string;
  readonly workflowName: string;
  readonly outcome: ActivationOutcome['kind'] | 'deferred';
  readonly durationMs: number;
  readonly wakeAt?: number | null;
}

export type EnginePhase = 'claim' | 'activation' | 'commit' | 'heartbeat';

/** Observability hooks. Wire them to your logger, metrics or OpenTelemetry. */
export interface EngineObserver {
  onWorkflowStarted?(event: {
    readonly instanceId: string;
    readonly workflowName: string;
    readonly workflowVersion: number;
  }): void;
  onActivation?(report: ActivationReport): void;
  onLog?(entry: WorkflowLogEntry): void;
  onError?(
    error: unknown,
    context: { readonly phase: EnginePhase; readonly instanceId?: string },
  ): void;
}

export interface EngineOptions {
  readonly store: WorkflowStore;
  readonly workflows: readonly AnyWorkflowDefinition[];
  readonly clock?: Clock;
  readonly random?: () => number;
  readonly workerId?: string;
  /** How long a claimed instance stays exclusively owned without a heartbeat. */
  readonly leaseDuration?: Duration;
  readonly defaultRetryPolicy?: Partial<RetryPolicy>;
  /** How long to wait before retrying an instance whose workflow is not registered here. */
  readonly unregisteredWorkflowDelay?: Duration;
  readonly observer?: EngineObserver;
  readonly idGenerator?: () => string;
}

export interface StartOptions {
  /** Business key such as an order id. Starting twice with the same id is idempotent. */
  readonly id?: string;
}

export interface ResultOptions {
  readonly timeout?: Duration;
  readonly pollInterval?: Duration;
  readonly signal?: AbortSignal;
}

export interface WorkflowHandle<TOutput> {
  readonly instanceId: string;
  /** False when `start` found an existing instance with the same id. */
  readonly created: boolean;
  result(options?: ResultOptions): Promise<TOutput>;
  describe(): Promise<WorkflowInstance>;
  signal(name: string, payload?: unknown): Promise<void>;
}

export interface RunUntilIdleOptions {
  /** Safety net against workflows that never become idle. */
  readonly maxActivations?: number;
}

/**
 * The application service: starts workflows, delivers signals and runs activations.
 * Stateless apart from configuration, so any number of engines (in any number of
 * processes) can share one store.
 */
export class Engine {
  readonly #store: WorkflowStore;
  readonly #registry = new Map<string, AnyWorkflowDefinition>();
  readonly #clock: Clock;
  readonly #random: () => number;
  readonly #workerId: string;
  readonly #leaseMs: number;
  readonly #defaultRetryPolicy: RetryPolicy;
  readonly #unregisteredDelayMs: number;
  readonly #observer: EngineObserver;
  readonly #newId: () => string;

  constructor(options: EngineOptions) {
    this.#store = options.store;
    this.#clock = options.clock ?? systemClock;
    this.#random = options.random ?? Math.random;
    this.#workerId = options.workerId ?? `worker-${randomUUID().slice(0, 8)}`;
    this.#leaseMs = toMilliseconds(options.leaseDuration ?? '30s');
    this.#defaultRetryPolicy = resolveRetryPolicy(DEFAULT_RETRY_POLICY, options.defaultRetryPolicy);
    this.#unregisteredDelayMs = toMilliseconds(options.unregisteredWorkflowDelay ?? '1m');
    this.#observer = options.observer ?? {};
    this.#newId = options.idGenerator ?? randomUUID;

    if (this.#leaseMs < 1_000) throw new ConfigurationError('leaseDuration must be at least 1s.');
    for (const workflow of options.workflows) {
      const key = registryKey(workflow.name, workflow.version);
      if (this.#registry.has(key)) {
        throw new ConfigurationError(
          `Workflow "${workflow.name}" v${workflow.version} is registered twice.`,
        );
      }
      this.#registry.set(key, workflow);
    }
  }

  get workerId(): string {
    return this.#workerId;
  }

  async start<TInput, TOutput>(
    workflow: WorkflowDefinition<TInput, TOutput>,
    input: TInput,
    options: StartOptions = {},
  ): Promise<WorkflowHandle<TOutput>> {
    if (!this.#registry.has(registryKey(workflow.name, workflow.version))) {
      throw new WorkflowNotRegisteredError(workflow.name, workflow.version);
    }
    const parsed = workflow.parseInput ? workflow.parseInput(input) : input;
    const { created, instance } = await this.#store.createInstance({
      id: options.id ?? this.#newId(),
      workflowName: workflow.name,
      workflowVersion: workflow.version,
      input: toJsonValue(parsed, 'workflow input'),
      now: this.#clock.now(),
    });
    if (!created && instance.workflowName !== workflow.name) {
      throw new WorkflowAlreadyExistsError(instance.id, instance.workflowName);
    }
    if (created) {
      this.#notify('onWorkflowStarted', {
        instanceId: instance.id,
        workflowName: workflow.name,
        workflowVersion: workflow.version,
      });
    }
    return this.handle<TOutput>(instance.id, created);
  }

  /** A typed handle to an existing instance. */
  handle<TOutput = unknown>(instanceId: string, created = false): WorkflowHandle<TOutput> {
    return {
      instanceId,
      created,
      result: (options) => this.result<TOutput>(instanceId, options),
      describe: async () => {
        const instance = await this.describe(instanceId);
        if (!instance) throw new WorkflowNotFoundError(instanceId);
        return instance;
      },
      signal: (name, payload) => this.signal(instanceId, name, payload),
    };
  }

  async signal(instanceId: string, name: string, payload?: unknown): Promise<void> {
    await this.#store.deliverSignal({
      instanceId,
      signal: { signalId: this.#newId(), name, payload: toJsonValue(payload, 'signal payload') },
      now: this.#clock.now(),
    });
  }

  describe(instanceId: string): Promise<WorkflowInstance | undefined> {
    return this.#store.getInstance(instanceId);
  }

  history(instanceId: string): Promise<StoredEvent[]> {
    return this.#store.getHistory(instanceId);
  }

  /** Polls until the instance finishes. Resolves with its output or rejects with its error. */
  async result<TOutput = unknown>(
    instanceId: string,
    options: ResultOptions = {},
  ): Promise<TOutput> {
    const deadline =
      options.timeout === undefined ? Infinity : Date.now() + toMilliseconds(options.timeout);
    const pollMs = toMilliseconds(options.pollInterval ?? '250ms');
    for (;;) {
      const instance = await this.#store.getInstance(instanceId);
      if (!instance) throw new WorkflowNotFoundError(instanceId);
      if (instance.status === 'completed') return instance.output as TOutput;
      if (instance.status === 'failed') {
        throw new WorkflowFailedError(
          instanceId,
          instance.error ?? { name: 'Error', message: 'Unknown failure' },
        );
      }
      if (Date.now() >= deadline) throw new ResultTimeoutError(instanceId);
      await delay(pollMs, undefined, options.signal ? { signal: options.signal } : {});
    }
  }

  /** Claims and runs up to `limit` due activations concurrently. Returns how many ran. */
  async tick(limit = 10): Promise<number> {
    let claims: Claim[];
    try {
      claims = await this.#store.claimDue({
        now: this.#clock.now(),
        limit,
        workerId: this.#workerId,
        leaseMs: this.#leaseMs,
      });
    } catch (error) {
      this.#notify('onError', error, { phase: 'claim' });
      return 0;
    }
    await Promise.all(claims.map((claim) => this.#process(claim)));
    return claims.length;
  }

  /** Runs activations until nothing is due. Ideal for tests together with `ManualClock`. */
  async runUntilIdle(options: RunUntilIdleOptions = {}): Promise<number> {
    const max = options.maxActivations ?? 10_000;
    let total = 0;
    for (;;) {
      const ran = await this.tick();
      total += ran;
      if (ran === 0) return total;
      if (total > max) {
        throw new ConfigurationError(
          `runUntilIdle exceeded ${max} activations; is a workflow looping?`,
        );
      }
    }
  }

  createWorker(options: WorkerOptions = {}): Worker {
    return new Worker(this, options);
  }

  async #process({ instance, leaseToken }: Claim): Promise<void> {
    const startedAt = performance.now();
    const definition = this.#registry.get(
      registryKey(instance.workflowName, instance.workflowVersion),
    );
    const report = (outcome: ActivationReport['outcome'], wakeAt?: number | null): void => {
      this.#notify('onActivation', {
        instanceId: instance.id,
        workflowName: instance.workflowName,
        outcome,
        durationMs: performance.now() - startedAt,
        ...(wakeAt === undefined ? {} : { wakeAt }),
      });
    };

    if (!definition) {
      // Probably a rolling deployment: release the instance for a worker that knows it.
      this.#notify(
        'onError',
        new WorkflowNotRegisteredError(instance.workflowName, instance.workflowVersion),
        { phase: 'activation', instanceId: instance.id },
      );
      const wakeAt = this.#clock.now() + this.#unregisteredDelayMs;
      await this.#commit(instance, leaseToken, [], { status: instance.status, wakeAt });
      report('deferred', wakeAt);
      return;
    }

    let outcome: ActivationOutcome;
    try {
      const activation = new Activation({
        definition,
        input: instance.input,
        instanceId: instance.id,
        workflowName: instance.workflowName,
        history: await this.#store.getHistory(instance.id),
        clock: this.#clock,
        random: this.#random,
        defaultRetryPolicy: this.#defaultRetryPolicy,
        checkpoint: (events) =>
          this.#store.appendEvents({
            instanceId: instance.id,
            leaseToken,
            events,
            now: this.#clock.now(),
            leaseExpiresAt: this.#clock.now() + this.#leaseMs,
          }),
        onLog: (entry) => {
          this.#notify('onLog', entry);
        },
      });
      const stopHeartbeat = this.#startHeartbeat(instance.id, leaseToken, activation);
      try {
        outcome = await activation.run();
      } finally {
        stopHeartbeat();
      }
    } catch (error) {
      // Unexpected (e.g. history could not be read). The lease expires and another
      // worker retries the activation; nothing was committed.
      this.#notify('onError', error, { phase: 'activation', instanceId: instance.id });
      return;
    }

    switch (outcome.kind) {
      case 'aborted':
        this.#notify('onError', outcome.error, { phase: 'activation', instanceId: instance.id });
        report('aborted');
        return;
      case 'completed':
        await this.#commit(instance, leaseToken, outcome.events, {
          status: 'completed',
          wakeAt: null,
          output: outcome.output,
        });
        break;
      case 'failed':
        await this.#commit(instance, leaseToken, outcome.events, {
          status: 'failed',
          wakeAt: null,
          error: outcome.error,
        });
        break;
      case 'suspended':
        await this.#commit(instance, leaseToken, outcome.events, {
          status: 'suspended',
          wakeAt: outcome.wakeAt,
        });
        report(outcome.kind, outcome.wakeAt);
        return;
    }
    report(outcome.kind);
  }

  async #commit(
    instance: WorkflowInstance,
    leaseToken: string,
    events: Parameters<WorkflowStore['completeActivation']>[0]['events'],
    next: NextState,
  ): Promise<void> {
    try {
      await this.#store.completeActivation({
        instanceId: instance.id,
        leaseToken,
        events,
        next,
        now: this.#clock.now(),
        observedSignalCount: instance.signalCount,
      });
    } catch (error) {
      this.#notify('onError', error, { phase: 'commit', instanceId: instance.id });
    }
  }

  /** Renews the lease while long-running steps execute, and aborts if it was lost. */
  #startHeartbeat(instanceId: string, leaseToken: string, activation: Activation): () => void {
    const timer = setInterval(
      () => {
        this.#store
          .heartbeat({ instanceId, leaseToken, leaseExpiresAt: this.#clock.now() + this.#leaseMs })
          .catch((error: unknown) => {
            this.#notify('onError', error, { phase: 'heartbeat', instanceId });
            activation.abort(error);
          });
      },
      Math.max(250, Math.floor(this.#leaseMs / 3)),
    );
    timer.unref();
    return () => {
      clearInterval(timer);
    };
  }

  /** Observer failures must never break the engine. */
  #notify<K extends keyof EngineObserver>(
    hook: K,
    ...args: Parameters<NonNullable<EngineObserver[K]>>
  ): void {
    const fn = this.#observer[hook] as ((...a: typeof args) => void) | undefined;
    try {
      fn?.apply(this.#observer, args);
    } catch {
      // Intentionally ignored: observability is best-effort.
    }
  }
}

function registryKey(name: string, version: number): string {
  return `${name}@v${version}`;
}
