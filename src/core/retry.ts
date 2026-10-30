import { toMilliseconds, type Duration } from './duration.js';
import { ConfigurationError, NonRetryableError } from './errors.js';

export interface RetryPolicy {
  /** Total attempts including the first one. `1` disables retries. */
  readonly maxAttempts: number;
  readonly initialInterval: Duration;
  readonly backoffCoefficient: number;
  readonly maxInterval: Duration;
  /** Randomise each delay within [50%, 100%] of its value to avoid thundering herds. */
  readonly jitter: boolean;
  /** Error `name`s that fail the step immediately, in addition to `NonRetryableError`. */
  readonly nonRetryableErrors: readonly string[];
}

export const DEFAULT_RETRY_POLICY: RetryPolicy = Object.freeze({
  maxAttempts: 3,
  initialInterval: '1s',
  backoffCoefficient: 2,
  maxInterval: '1m',
  jitter: true,
  nonRetryableErrors: Object.freeze([]),
});

export function resolveRetryPolicy(
  base: RetryPolicy,
  override?: Partial<RetryPolicy>,
): RetryPolicy {
  const policy: RetryPolicy = { ...base, ...override };
  if (!Number.isInteger(policy.maxAttempts) || policy.maxAttempts < 1) {
    throw new ConfigurationError(
      `retry.maxAttempts must be an integer >= 1, got ${policy.maxAttempts}.`,
    );
  }
  if (!(policy.backoffCoefficient >= 1)) {
    throw new ConfigurationError(
      `retry.backoffCoefficient must be >= 1, got ${policy.backoffCoefficient}.`,
    );
  }
  if (toMilliseconds(policy.maxInterval) < toMilliseconds(policy.initialInterval)) {
    throw new ConfigurationError(
      'retry.maxInterval must be greater than or equal to retry.initialInterval.',
    );
  }
  return policy;
}

/** Delay before the attempt that follows `failedAttempt` (1-based). Pure, so easy to test. */
export function computeBackoffMs(
  policy: RetryPolicy,
  failedAttempt: number,
  random: () => number = Math.random,
): number {
  const initial = toMilliseconds(policy.initialInterval);
  const max = toMilliseconds(policy.maxInterval);
  const exponential = Math.min(max, initial * policy.backoffCoefficient ** (failedAttempt - 1));
  if (!policy.jitter) return Math.round(exponential);
  const half = exponential / 2;
  return Math.round(half + random() * half);
}

export function isRetryable(error: unknown, policy: RetryPolicy): boolean {
  if (error instanceof NonRetryableError) return false;
  return !(error instanceof Error && policy.nonRetryableErrors.includes(error.name));
}
