import type { Clock } from '../core/clock.js';
import { toMilliseconds, type Duration } from '../core/duration.js';
import {
  NonDeterminismError,
  serializeError,
  SignalTimeoutError,
  StepFailedError,
  StepTimeoutError,
  type SerializedError,
} from '../core/errors.js';
import {
  operationKindOf,
  type HistoryEvent,
  type OperationEvent,
  type OperationKind,
} from '../core/events.js';
import {
  computeBackoffMs,
  isRetryable,
  resolveRetryPolicy,
  type RetryPolicy,
} from '../core/retry.js';
import type {
  SignalOptions,
  StepExecution,
  StepFunction,
  StepOptions,
  WorkflowContext,
} from '../core/workflow.js';
import { toJsonValue } from '../internal/json.js';
import { handled, never, nextMacrotask } from '../internal/promises.js';
import { HistoryIndex, operationKey } from './history-index.js';

/**
 * Why an activation stopped making progress, in increasing order of severity:
 * - `suspend`: waiting on a timer, a retry delay or a signal. Other branches keep running.
 * - `fatal`: replay diverged from history; the workflow must fail.
 * - `abort`: persistence failed or the lease was lost; nothing more may be written.
 */
export type Interruption =
  | { readonly kind: 'suspend' }
  | { readonly kind: 'fatal'; readonly error: Error }
  | { readonly kind: 'abort'; readonly error: unknown };

const SEVERITY: Readonly<Record<Interruption['kind'], number>> = { suspend: 0, fatal: 1, abort: 2 };

export interface WorkflowLogEntry {
  readonly instanceId: string;
  readonly workflowName: string;
  readonly message: string;
  readonly data?: Readonly<Record<string, unknown>>;
}

export interface ReplayContextOptions {
  readonly instanceId: string;
  readonly workflowName: string;
  readonly history: readonly HistoryEvent[];
  readonly clock: Clock;
  readonly random: () => number;
  readonly defaultRetryPolicy: RetryPolicy;
  /** Persists events immediately (and renews the lease). Throws if the lease was lost. */
  readonly checkpoint: (events: readonly HistoryEvent[]) => Promise<void>;
  readonly onLog: (entry: WorkflowLogEntry) => void;
}

interface StepCall<T> {
  readonly op: number;
  readonly compensation: boolean;
  readonly name: string;
  readonly fn: StepFunction<T>;
  readonly options: StepOptions<T>;
}

type StepResolution<T> =
  | { readonly kind: 'value'; readonly value: T }
  | { readonly kind: 'error'; readonly error: Error }
  | { readonly kind: 'parked' };

const PARKED = { kind: 'parked' } as const;

interface Compensation {
  readonly name: string;
  readonly run: () => Promise<void> | void;
  readonly retry: Partial<RetryPolicy> | undefined;
}

/**
 * Implements {@link WorkflowContext} by replaying history.
 *
 * Every durable operation gets the next operation index. If history already holds a
 * terminal event for that index, the recorded outcome is returned without side effects;
 * otherwise the operation runs live and its outcome is appended to history.
 *
 * Operations that cannot finish yet return a promise that never settles and record an
 * interruption. Using never-settling promises rather than throwing a sentinel means a
 * `try/catch` in workflow code can never accidentally swallow a suspension.
 */
export class ReplayContext implements WorkflowContext {
  readonly workflowId: string;
  readonly #options: ReplayContextOptions;
  readonly #history: HistoryIndex;
  readonly #pending: HistoryEvent[] = [];
  readonly #inFlight = new Set<Promise<unknown>>();
  readonly #compensations = new Map<number, Compensation>();
  readonly #interruptionListeners: (() => void)[] = [];
  #interruption: Interruption | undefined;
  #nextOperation = 0;
  #replaying: boolean;
  #earliestWakeAt: number | null = null;

  constructor(options: ReplayContextOptions) {
    this.workflowId = options.instanceId;
    this.#options = options;
    this.#history = new HistoryIndex(options.history);
    this.#replaying = this.#history.highestOperation >= 0;
  }

  // ---------------------------------------------------------------- workflow-facing API

  get isReplaying(): boolean {
    return this.#replaying;
  }

  step<T>(name: string, fn: StepFunction<T>, options: StepOptions<T> = {}): Promise<T> {
    const op = this.#beginOperation('step', name);
    if (op === undefined) return never();
    return this.#step({ op, compensation: false, name, fn, options });
  }

  sleep(name: string, duration: Duration): Promise<void> {
    const durationMs = toMilliseconds(duration);
    const op = this.#beginOperation('sleep', name);
    if (op === undefined) return never();

    const recorded = this.#history.eventsFor(operationKey(op, false));
    if (findEvent(recorded, 'TimerFired')) return Promise.resolve();
    this.#replaying = false;

    const now = this.#options.clock.now();
    let fireAt = findEvent(recorded, 'TimerStarted')?.fireAt;
    if (fireAt === undefined) {
      fireAt = now + durationMs;
      this.#pending.push({ type: 'TimerStarted', op, name, fireAt });
    }
    if (now >= fireAt) {
      this.#pending.push({ type: 'TimerFired', op, name });
      return Promise.resolve();
    }
    this.#suspend(fireAt);
    return never();
  }

  waitForSignal<T = unknown>(name: string, options: SignalOptions = {}): Promise<T> {
    const timeoutMs = options.timeout === undefined ? undefined : toMilliseconds(options.timeout);
    const op = this.#beginOperation('signal', name);
    if (op === undefined) return never();

    const recorded = this.#history.eventsFor(operationKey(op, false));
    const consumed = findEvent(recorded, 'SignalConsumed');
    if (consumed) return Promise.resolve(this.#history.signal(consumed.signalId)?.payload as T);
    if (findEvent(recorded, 'SignalTimedOut')) {
      return handled(Promise.reject(new SignalTimeoutError(name)));
    }
    this.#replaying = false;

    const now = this.#options.clock.now();
    let timeoutAt = findEvent(recorded, 'SignalWaitStarted')?.timeoutAt;
    if (timeoutAt === undefined) {
      timeoutAt = timeoutMs === undefined ? null : now + timeoutMs;
      this.#pending.push({ type: 'SignalWaitStarted', op, name, timeoutAt });
    }
    const signal = this.#history.nextUnconsumedSignal(name);
    if (signal) {
      this.#history.markConsumed(signal.signalId);
      this.#pending.push({ type: 'SignalConsumed', op, name, signalId: signal.signalId });
      return Promise.resolve(signal.payload as T);
    }
    if (timeoutAt !== null && now >= timeoutAt) {
      this.#pending.push({ type: 'SignalTimedOut', op, name });
      return handled(Promise.reject(new SignalTimeoutError(name)));
    }
    this.#suspend(timeoutAt);
    return never();
  }

  log(message: string, data?: Readonly<Record<string, unknown>>): void {
    if (this.#replaying) return;
    this.#options.onLog({
      instanceId: this.workflowId,
      workflowName: this.#options.workflowName,
      message,
      ...(data ? { data } : {}),
    });
  }

  // ------------------------------------------------------------------ engine-facing API

  get interruption(): Interruption | undefined {
    return this.#interruption;
  }

  /** True once nothing more may be executed or written in this activation. */
  get isHalted(): boolean {
    const kind = this.#interruption?.kind;
    return kind === 'abort' || kind === 'fatal';
  }

  get earliestWakeAt(): number | null {
    return this.#earliestWakeAt;
  }

  get operationsIssued(): number {
    return this.#nextOperation;
  }

  get highestRecordedOperation(): number {
    return this.#history.highestOperation;
  }

  /**
   * Resolves on the next interruption recorded after this call. Register it before starting
   * the work it guards, because operations may interrupt synchronously.
   */
  nextInterruption(): Promise<void> {
    if (this.isHalted) return Promise.resolve();
    return new Promise((resolve) => this.#interruptionListeners.push(resolve));
  }

  abort(error: unknown): void {
    this.#interrupt({ kind: 'abort', error });
  }

  fail(error: Error): void {
    this.#interrupt({ kind: 'fatal', error });
  }

  takePendingEvents(): HistoryEvent[] {
    return this.#pending.splice(0);
  }

  /**
   * Waits until no step is executing and every continuation of workflow code has run.
   * Workflow code only awaits durable operations, so once no step is in flight and the
   * microtask queue has drained, the workflow cannot make further progress by itself.
   */
  async quiesce(): Promise<void> {
    do {
      while (this.#inFlight.size > 0) await Promise.allSettled([...this.#inFlight]);
      await nextMacrotask();
    } while (this.#inFlight.size > 0);
  }

  /**
   * Runs the compensations of completed steps durably, latest first. Each compensation is
   * keyed by the operation it undoes, so the order steps happened to finish in during live
   * execution can never cause a replay mismatch.
   */
  async compensate(): Promise<SerializedError[]> {
    const failures: SerializedError[] = [];
    const latestFirst = [...this.#compensations.entries()].sort(([a], [b]) => b - a);
    for (const [op, compensation] of latestFirst) {
      const name = `compensate:${compensation.name}`;
      const recorded = this.#history.eventsFor(operationKey(op, true))[0];
      if (recorded !== undefined && recorded.name !== name) {
        this.fail(
          new NonDeterminismError(
            `Compensation for operation #${op} was recorded as "${recorded.name}" but is now "${name}".`,
          ),
        );
      }
      if (this.isHalted) return never();
      try {
        await this.#step({
          op,
          compensation: true,
          name,
          fn: () => compensation.run(),
          options: compensation.retry ? { retry: compensation.retry } : {},
        });
      } catch (error) {
        failures.push(serializeError(error));
      }
    }
    return failures;
  }

  // -------------------------------------------------------------------------- internals

  #beginOperation(kind: OperationKind, name: string): number | undefined {
    if (this.isHalted) return undefined;
    const op = this.#nextOperation++;
    const recorded = this.#history.eventsFor(operationKey(op, false))[0];
    if (recorded !== undefined) {
      const recordedKind = operationKindOf(recorded);
      if (recordedKind !== kind || recorded.name !== name) {
        this.fail(
          new NonDeterminismError(
            `Operation #${op} was recorded as ${recordedKind} "${recorded.name}", but the workflow ` +
              `now issued ${kind} "${name}". Incompatible code changes need a new workflow version.`,
          ),
        );
        return undefined;
      }
    }
    return op;
  }

  #step<T>(call: StepCall<T>): Promise<T> {
    const recorded = this.#history.eventsFor(operationKey(call.op, call.compensation));
    const completed = findEvent(recorded, 'StepCompleted');
    if (completed) {
      const result = completed.result as T;
      this.#registerCompensation(call, result);
      return Promise.resolve(result);
    }
    const failed = findEvent(recorded, 'StepFailed');
    if (failed) {
      return handled(Promise.reject(new StepFailedError(call.name, failed.attempt, failed.error)));
    }
    this.#replaying = false;

    const policy = resolveRetryPolicy(this.#options.defaultRetryPolicy, call.options.retry);
    const failures = recorded.filter((event) => event.type === 'StepAttemptFailed');
    const lastFailure = failures.at(-1);
    if (lastFailure && this.#options.clock.now() < lastFailure.retryAt) {
      this.#suspend(lastFailure.retryAt);
      return never();
    }

    const execution = this.#execute(call, policy, failures.length + 1);
    this.#track(execution);
    return handled(
      execution.then((resolution) => {
        switch (resolution.kind) {
          case 'value':
            return resolution.value;
          case 'error':
            throw resolution.error;
          case 'parked':
            return never<T>();
        }
      }),
    );
  }

  async #execute<T>(
    call: StepCall<T>,
    policy: RetryPolicy,
    attempt: number,
  ): Promise<StepResolution<T>> {
    let outcome: { ok: true; value: unknown } | { ok: false; error: unknown };
    try {
      const value = await this.#invoke(call, attempt);
      outcome = { ok: true, value: toJsonValue(value, `result of step "${call.name}"`) };
    } catch (error) {
      outcome = { ok: false, error };
    }

    const base = {
      op: call.op,
      name: call.name,
      attempt,
      ...(call.compensation ? { compensation: true as const } : {}),
    };

    if (outcome.ok) {
      if (!(await this.#record({ type: 'StepCompleted', ...base, result: outcome.value }))) {
        return PARKED;
      }
      const value = outcome.value as T;
      this.#registerCompensation(call, value);
      return { kind: 'value', value };
    }

    const error = serializeError(outcome.error);
    if (attempt >= policy.maxAttempts || !isRetryable(outcome.error, policy)) {
      if (!(await this.#record({ type: 'StepFailed', ...base, error }))) return PARKED;
      return { kind: 'error', error: new StepFailedError(call.name, attempt, error) };
    }

    const retryAt =
      this.#options.clock.now() + computeBackoffMs(policy, attempt, this.#options.random);
    if (await this.#record({ type: 'StepAttemptFailed', ...base, error, retryAt })) {
      this.#suspend(retryAt);
    }
    return PARKED;
  }

  async #invoke<T>(call: StepCall<T>, attempt: number): Promise<T> {
    const controller = new AbortController();
    const execution: StepExecution = {
      attempt,
      idempotencyKey: `${this.workflowId}:${operationKey(call.op, call.compensation)}`,
      signal: controller.signal,
    };
    const task = Promise.resolve().then(() => call.fn(execution));
    if (call.options.timeout === undefined) return await task;

    const timeoutMs = toMilliseconds(call.options.timeout);
    let timer: NodeJS.Timeout | undefined;
    const timeout = new Promise<never>((_, reject) => {
      timer = setTimeout(() => {
        const error = new StepTimeoutError(call.name, timeoutMs);
        controller.abort(error);
        reject(error);
      }, timeoutMs);
    });
    try {
      return await Promise.race([task, timeout]);
    } finally {
      clearTimeout(timer);
    }
  }

  /** Checkpoints immediately so a crash re-executes at most the step that was running. */
  async #record(event: HistoryEvent): Promise<boolean> {
    this.#pending.push(event);
    const batch = this.#pending.splice(0);
    try {
      await this.#options.checkpoint(batch);
      return true;
    } catch (error) {
      this.abort(error);
      return false;
    }
  }

  #registerCompensation<T>(call: StepCall<T>, result: T): void {
    const { compensate } = call.options;
    if (call.compensation || !compensate) return;
    this.#compensations.set(call.op, {
      name: call.name,
      run: () => compensate(result),
      retry: call.options.compensationRetry,
    });
  }

  #suspend(wakeAt: number | null): void {
    if (wakeAt !== null) {
      this.#earliestWakeAt =
        this.#earliestWakeAt === null ? wakeAt : Math.min(this.#earliestWakeAt, wakeAt);
    }
    this.#interrupt({ kind: 'suspend' });
  }

  #interrupt(next: Interruption): void {
    const current = this.#interruption;
    if (current === undefined || SEVERITY[next.kind] > SEVERITY[current.kind]) {
      this.#interruption = next;
    }
    for (const notify of this.#interruptionListeners.splice(0)) notify();
  }

  #track(promise: Promise<unknown>): void {
    this.#inFlight.add(promise);
    const settle = (): void => {
      this.#inFlight.delete(promise);
    };
    promise.then(settle, settle);
  }
}

function findEvent<K extends OperationEvent['type']>(
  events: readonly OperationEvent[],
  type: K,
): Extract<OperationEvent, { type: K }> | undefined {
  return events.find((event): event is Extract<OperationEvent, { type: K }> => event.type === type);
}
