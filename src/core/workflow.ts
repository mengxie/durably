import type { Duration } from './duration.js';
import { ConfigurationError } from './errors.js';
import type { RetryPolicy } from './retry.js';

/** Passed to every step invocation. */
export interface StepExecution {
  /** 1-based attempt number. */
  readonly attempt: number;
  /**
   * Stable across retries and replays of the same step. Pass it to downstream APIs
   * (payment providers, message brokers) so that at-least-once execution stays safe.
   */
  readonly idempotencyKey: string;
  /** Aborted when the step exceeds its `timeout`. */
  readonly signal: AbortSignal;
}

export type StepFunction<T> = (execution: StepExecution) => Promise<T> | T;

export interface StepOptions<T> {
  readonly retry?: Partial<RetryPolicy>;
  /** Per-attempt timeout. A timed-out attempt counts as a retryable failure. */
  readonly timeout?: Duration;
  /**
   * Undo action for the saga pattern. If the workflow later fails, compensations of all
   * completed steps run durably, in reverse order, before the workflow is marked failed.
   */
  readonly compensate?: (result: T) => Promise<void> | void;
  readonly compensationRetry?: Partial<RetryPolicy>;
}

export interface SignalOptions {
  /** When reached, `waitForSignal` rejects with `SignalTimeoutError`. */
  readonly timeout?: Duration;
}

/**
 * The only way workflow code interacts with the outside world. Every method is durable:
 * its outcome is recorded in history and replayed, never re-executed, after a restart.
 *
 * Workflow code must be deterministic. Do I/O, read the clock or generate randomness
 * inside steps, never directly in the workflow body.
 */
export interface WorkflowContext {
  readonly workflowId: string;
  /** True while the context is replaying operations that already completed. */
  readonly isReplaying: boolean;
  step<T>(name: string, fn: StepFunction<T>, options?: StepOptions<T>): Promise<T>;
  sleep(name: string, duration: Duration): Promise<void>;
  waitForSignal<T = unknown>(name: string, options?: SignalOptions): Promise<T>;
  /** Replay-aware logging: messages are emitted once, not on every replay. */
  log(message: string, data?: Readonly<Record<string, unknown>>): void;
}

export interface WorkflowDefinition<TInput, TOutput> {
  readonly name: string;
  readonly version: number;
  /** Optional input validation at start time, e.g. `schema.parse` from zod or valibot. */
  parseInput?(raw: unknown): TInput;
  run(ctx: WorkflowContext, input: TInput): Promise<TOutput>;
}

export type AnyWorkflowDefinition = WorkflowDefinition<unknown, unknown>;

export interface WorkflowSpec<TInput, TOutput> {
  readonly name: string;
  /** Bump when changing workflow code incompatibly; in-flight instances keep their version. */
  readonly version?: number;
  readonly parseInput?: (raw: unknown) => TInput;
  readonly run: (ctx: WorkflowContext, input: TInput) => Promise<TOutput>;
}

const WORKFLOW_NAME_PATTERN = /^[a-zA-Z][\w.-]{0,127}$/;

export function defineWorkflow<TInput = void, TOutput = void>(
  spec: WorkflowSpec<TInput, TOutput>,
): WorkflowDefinition<TInput, TOutput> {
  if (!WORKFLOW_NAME_PATTERN.test(spec.name)) {
    throw new ConfigurationError(
      `Invalid workflow name "${spec.name}": use letters, digits, ".", "_" or "-", starting with a letter.`,
    );
  }
  const version = spec.version ?? 1;
  if (!Number.isInteger(version) || version < 1) {
    throw new ConfigurationError(`Workflow version must be a positive integer, got ${version}.`);
  }
  const { parseInput, run } = spec;
  return Object.freeze({
    name: spec.name,
    version,
    ...(parseInput ? { parseInput } : {}),
    run,
  });
}
