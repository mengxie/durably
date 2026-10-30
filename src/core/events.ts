import type { SerializedError } from './errors.js';

/**
 * The append-only history of a workflow instance. History is the single source of truth:
 * the state of an instance is whatever replaying its history produces.
 *
 * Operation events carry `op`, the zero-based index of the durable operation in the order
 * the workflow issued it. Replay matches operations to history by this index, which is why
 * workflow code must be deterministic.
 */
export type HistoryEvent =
  | WorkflowStarted
  | StepCompleted
  | StepAttemptFailed
  | StepFailed
  | TimerStarted
  | TimerFired
  | SignalReceived
  | SignalWaitStarted
  | SignalConsumed
  | SignalTimedOut
  | WorkflowCompleted
  | WorkflowFailed;

/** A history event as persisted: with its position in the stream and the time it was written. */
export type StoredEvent = HistoryEvent & { readonly seq: number; readonly at: number };

export interface WorkflowStarted {
  readonly type: 'WorkflowStarted';
  readonly workflowName: string;
  readonly workflowVersion: number;
  readonly input: unknown;
}

interface StepEventBase {
  readonly op: number;
  readonly name: string;
  readonly attempt: number;
  /** Set when this step is the compensation of the step recorded at `op`. */
  readonly compensation?: true;
}

export interface StepCompleted extends StepEventBase {
  readonly type: 'StepCompleted';
  readonly result: unknown;
}

export interface StepAttemptFailed extends StepEventBase {
  readonly type: 'StepAttemptFailed';
  readonly error: SerializedError;
  readonly retryAt: number;
}

export interface StepFailed extends StepEventBase {
  readonly type: 'StepFailed';
  readonly error: SerializedError;
}

export interface TimerStarted {
  readonly type: 'TimerStarted';
  readonly op: number;
  readonly name: string;
  readonly fireAt: number;
}

export interface TimerFired {
  readonly type: 'TimerFired';
  readonly op: number;
  readonly name: string;
}

/** Written by the engine when a signal is delivered; it is not tied to an operation. */
export interface SignalReceived {
  readonly type: 'SignalReceived';
  readonly signalId: string;
  readonly name: string;
  readonly payload: unknown;
}

export interface SignalWaitStarted {
  readonly type: 'SignalWaitStarted';
  readonly op: number;
  readonly name: string;
  readonly timeoutAt: number | null;
}

export interface SignalConsumed {
  readonly type: 'SignalConsumed';
  readonly op: number;
  readonly name: string;
  readonly signalId: string;
}

export interface SignalTimedOut {
  readonly type: 'SignalTimedOut';
  readonly op: number;
  readonly name: string;
}

export interface WorkflowCompleted {
  readonly type: 'WorkflowCompleted';
  readonly output: unknown;
}

export interface WorkflowFailed {
  readonly type: 'WorkflowFailed';
  readonly error: SerializedError;
  readonly compensationErrors: readonly SerializedError[];
}

export type OperationEvent = Extract<HistoryEvent, { readonly op: number }>;
export type OperationKind = 'step' | 'sleep' | 'signal';

export function isOperationEvent(event: HistoryEvent): event is OperationEvent {
  return 'op' in event;
}

export function operationKindOf(event: OperationEvent): OperationKind {
  switch (event.type) {
    case 'StepCompleted':
    case 'StepAttemptFailed':
    case 'StepFailed':
      return 'step';
    case 'TimerStarted':
    case 'TimerFired':
      return 'sleep';
    case 'SignalWaitStarted':
    case 'SignalConsumed':
    case 'SignalTimedOut':
      return 'signal';
  }
}
