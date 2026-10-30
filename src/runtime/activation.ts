import { NonDeterminismError, serializeError, type SerializedError } from '../core/errors.js';
import type { HistoryEvent } from '../core/events.js';
import type { AnyWorkflowDefinition } from '../core/workflow.js';
import { toJsonValue } from '../internal/json.js';
import { ReplayContext, type ReplayContextOptions } from './replay-context.js';

export type ActivationOutcome =
  | { readonly kind: 'completed'; readonly output: unknown; readonly events: HistoryEvent[] }
  | {
      readonly kind: 'failed';
      readonly error: SerializedError;
      readonly compensationErrors: SerializedError[];
      readonly events: HistoryEvent[];
    }
  | { readonly kind: 'suspended'; readonly wakeAt: number | null; readonly events: HistoryEvent[] }
  | { readonly kind: 'aborted'; readonly error: unknown };

type BodyOutcome =
  | { readonly kind: 'returned'; readonly output: unknown }
  | { readonly kind: 'threw'; readonly error: unknown };

export interface ActivationOptions extends ReplayContextOptions {
  readonly definition: AnyWorkflowDefinition;
  readonly input: unknown;
}

/**
 * One activation = replay the workflow from the start against its history, let it make as
 * much progress as it can, then report how it stopped. It is a pure function of
 * (definition, history, clock, step outcomes); all persistence goes through `checkpoint`.
 */
export class Activation {
  readonly #context: ReplayContext;
  readonly #definition: AnyWorkflowDefinition;
  readonly #input: unknown;

  constructor(options: ActivationOptions) {
    this.#context = new ReplayContext(options);
    this.#definition = options.definition;
    this.#input = options.input;
  }

  /** Stops the activation from writing anything else, e.g. after a failed lease heartbeat. */
  abort(error: unknown): void {
    this.#context.abort(error);
  }

  async run(): Promise<ActivationOutcome> {
    const ctx = this.#context;
    const state: { body?: BodyOutcome; compensationErrors?: SerializedError[] } = {};

    const interrupted = ctx.nextInterruption();
    const body = this.#runBody().then((outcome) => {
      state.body = outcome;
    });
    await Promise.race([body, interrupted]);
    await ctx.quiesce();

    if (!ctx.isHalted && state.body?.kind === 'threw') {
      // Saga: undo completed steps. Compensations are durable steps and may suspend too.
      const compensationInterrupted = ctx.nextInterruption();
      const compensated = ctx.compensate().then((errors) => {
        state.compensationErrors = errors;
      });
      await Promise.race([compensated, compensationInterrupted]);
      await ctx.quiesce();
    }

    const interruption = ctx.interruption;
    if (interruption?.kind === 'abort') return { kind: 'aborted', error: interruption.error };
    if (interruption?.kind === 'fatal') return this.#failed(interruption.error, []);
    if (state.body?.kind === 'returned') return this.#completed(state.body.output);
    if (state.body?.kind === 'threw' && state.compensationErrors) {
      return this.#failed(state.body.error, state.compensationErrors);
    }
    return { kind: 'suspended', wakeAt: ctx.earliestWakeAt, events: ctx.takePendingEvents() };
  }

  async #runBody(): Promise<BodyOutcome> {
    try {
      return { kind: 'returned', output: await this.#definition.run(this.#context, this.#input) };
    } catch (error) {
      return { kind: 'threw', error };
    }
  }

  #completed(output: unknown): ActivationOutcome {
    const ctx = this.#context;
    if (ctx.highestRecordedOperation >= ctx.operationsIssued) {
      return this.#failed(
        new NonDeterminismError(
          `History contains operation #${ctx.highestRecordedOperation}, but the workflow completed ` +
            `after issuing only ${ctx.operationsIssued} operation(s).`,
        ),
        [],
      );
    }
    let json: unknown;
    try {
      json = toJsonValue(output, 'workflow output');
    } catch (error) {
      return this.#failed(error, []);
    }
    const events = ctx.takePendingEvents();
    events.push({ type: 'WorkflowCompleted', output: json });
    return { kind: 'completed', output: json, events };
  }

  #failed(error: unknown, compensationErrors: SerializedError[]): ActivationOutcome {
    const serialized = serializeError(error);
    const events = this.#context.takePendingEvents();
    events.push({ type: 'WorkflowFailed', error: serialized, compensationErrors });
    return { kind: 'failed', error: serialized, compensationErrors, events };
  }
}
