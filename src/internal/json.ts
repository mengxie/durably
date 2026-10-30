import { SerializationError } from '../core/errors.js';

/** `JSON.stringify` is typed as returning string, but returns undefined for functions and symbols. */
const stringify: (value: unknown) => string | undefined = JSON.stringify;

/**
 * Round-trips a value through JSON so that what workflow code sees on first execution is
 * exactly what it will see on replay (e.g. a `Date` becomes an ISO string both times).
 */
export function toJsonValue(value: unknown, what = 'value'): unknown {
  if (value === undefined) return undefined;
  let text: string | undefined;
  try {
    text = stringify(value);
  } catch (error) {
    throw new SerializationError(`The ${what} is not JSON-serializable.`, { cause: error });
  }
  if (text === undefined) {
    throw new SerializationError(`The ${what} is not JSON-serializable.`);
  }
  return JSON.parse(text) as unknown;
}
