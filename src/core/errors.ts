/** A plain, JSON-safe representation of an error, as stored in workflow history. */
export interface SerializedError {
  readonly name: string;
  readonly message: string;
  readonly code?: string;
  readonly stack?: string;
}

/** Base class for every error raised by durably. `code` is stable and safe to match on. */
export abstract class DurablyError extends Error {
  abstract readonly code: string;

  constructor(message: string, options?: ErrorOptions) {
    super(message, options);
    this.name = new.target.name;
  }
}

/** Invalid engine, workflow or retry configuration. Raised eagerly, never during replay. */
export class ConfigurationError extends DurablyError {
  readonly code = 'CONFIGURATION';
}

export class InvalidDurationError extends DurablyError {
  readonly code = 'INVALID_DURATION';

  constructor(readonly value: string) {
    super(
      `"${value}" is not a valid duration. Use milliseconds or a string like "500ms", "30s", "5m", "2h" or "1d".`,
    );
  }
}

/**
 * Throw this (or a subclass) from a step to fail it immediately without consuming
 * further retry attempts, e.g. for validation failures or business rule rejections.
 */
export class NonRetryableError extends DurablyError {
  readonly code: string = 'NON_RETRYABLE';
}

/** A step result could not be represented as JSON and therefore cannot be made durable. */
export class SerializationError extends NonRetryableError {
  override readonly code = 'SERIALIZATION';
}

/** Raised inside workflow code when a step has exhausted its retry policy. Catchable. */
export class StepFailedError extends DurablyError {
  readonly code = 'STEP_FAILED';

  constructor(
    readonly stepName: string,
    readonly attempts: number,
    readonly failure: SerializedError,
  ) {
    super(`Step "${stepName}" failed after ${attempts} attempt(s): ${failure.message}`);
  }
}

export class StepTimeoutError extends DurablyError {
  readonly code = 'STEP_TIMEOUT';

  constructor(
    readonly stepName: string,
    readonly timeoutMs: number,
  ) {
    super(`Step "${stepName}" did not finish within ${timeoutMs}ms.`);
  }
}

/** Raised inside workflow code when `waitForSignal` reaches its timeout. Catchable. */
export class SignalTimeoutError extends DurablyError {
  readonly code = 'SIGNAL_TIMEOUT';

  constructor(readonly signalName: string) {
    super(`Timed out waiting for signal "${signalName}".`);
  }
}

/** Replay diverged from recorded history: the workflow code changed in an incompatible way. */
export class NonDeterminismError extends DurablyError {
  readonly code = 'NON_DETERMINISM';
}

export class WorkflowNotFoundError extends DurablyError {
  readonly code = 'WORKFLOW_NOT_FOUND';

  constructor(readonly instanceId: string) {
    super(`Workflow instance "${instanceId}" does not exist.`);
  }
}

export class WorkflowAlreadyExistsError extends DurablyError {
  readonly code = 'WORKFLOW_ALREADY_EXISTS';

  constructor(
    readonly instanceId: string,
    readonly existingWorkflow: string,
  ) {
    super(`Instance id "${instanceId}" is already used by workflow "${existingWorkflow}".`);
  }
}

export class WorkflowNotRegisteredError extends DurablyError {
  readonly code = 'WORKFLOW_NOT_REGISTERED';

  constructor(
    readonly workflowName: string,
    readonly workflowVersion: number,
  ) {
    super(`Workflow "${workflowName}" v${workflowVersion} is not registered with this engine.`);
  }
}

/** A signal was sent to a workflow that has already completed or failed. */
export class WorkflowClosedError extends DurablyError {
  readonly code = 'WORKFLOW_CLOSED';

  constructor(
    readonly instanceId: string,
    readonly status: string,
  ) {
    super(`Workflow instance "${instanceId}" is ${status} and no longer accepts signals.`);
  }
}

export class WorkflowFailedError extends DurablyError {
  readonly code = 'WORKFLOW_FAILED';

  constructor(
    readonly instanceId: string,
    readonly failure: SerializedError,
  ) {
    super(`Workflow instance "${instanceId}" failed: ${failure.message}`);
  }
}

export class ResultTimeoutError extends DurablyError {
  readonly code = 'RESULT_TIMEOUT';

  constructor(readonly instanceId: string) {
    super(`Timed out waiting for workflow instance "${instanceId}" to finish.`);
  }
}

/**
 * The worker no longer holds the lease for an instance (it expired and another worker
 * claimed it). The fencing-token check turns a would-be split brain into this error.
 */
export class LeaseLostError extends DurablyError {
  readonly code = 'LEASE_LOST';

  constructor(readonly instanceId: string) {
    super(`Lease for workflow instance "${instanceId}" is no longer held by this worker.`);
  }
}

export function serializeError(error: unknown): SerializedError {
  if (error instanceof Error) {
    const code = (error as { code?: unknown }).code;
    return {
      name: error.name,
      message: error.message,
      ...(typeof code === 'string' ? { code } : {}),
      ...(error.stack !== undefined ? { stack: error.stack } : {}),
    };
  }
  return { name: 'NonErrorThrown', message: describeThrown(error) };
}

/** `JSON.stringify` is typed as returning string, but returns undefined for functions and symbols. */
const stringify: (value: unknown) => string | undefined = JSON.stringify;

function describeThrown(value: unknown): string {
  if (typeof value === 'string') return value;
  try {
    return stringify(value) ?? String(value);
  } catch {
    return Object.prototype.toString.call(value);
  }
}
