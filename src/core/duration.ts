import { InvalidDurationError } from './errors.js';

export type DurationUnit = 'ms' | 's' | 'm' | 'h' | 'd';

/** Milliseconds, or a human-readable string such as `"500ms"`, `"30s"`, `"5m"`, `"2h"`, `"1d"`. */
export type Duration = number | `${number}${DurationUnit}`;

const UNIT_MS: Readonly<Record<DurationUnit, number>> = {
  ms: 1,
  s: 1_000,
  m: 60_000,
  h: 3_600_000,
  d: 86_400_000,
};

const DURATION_PATTERN = /^(\d+(?:\.\d+)?)(ms|s|m|h|d)$/;

export function toMilliseconds(duration: Duration): number {
  if (typeof duration === 'number') {
    if (!Number.isFinite(duration) || duration < 0) {
      throw new InvalidDurationError(String(duration));
    }
    return Math.round(duration);
  }
  const match = DURATION_PATTERN.exec(duration.trim());
  const amount = match?.[1];
  const unit = match?.[2] as DurationUnit | undefined;
  if (amount === undefined || unit === undefined) {
    throw new InvalidDurationError(duration);
  }
  return Math.round(Number(amount) * UNIT_MS[unit]);
}
