/**
 * A promise that never settles. A fresh one is created per call on purpose: awaiting a
 * shared never-settling promise would accumulate reactions on it forever and leak memory,
 * whereas an abandoned fresh one is garbage-collected together with its awaiters.
 */
export function never<T>(): Promise<T> {
  return new Promise<T>(() => undefined);
}

/** Marks a promise as handled so that fire-and-forget usage cannot crash the process. */
export function handled<T>(promise: Promise<T>): Promise<T> {
  promise.catch(() => undefined);
  return promise;
}

/** Yields to the macrotask queue, letting every pending microtask continuation run first. */
export function nextMacrotask(): Promise<void> {
  return new Promise((resolve) => setImmediate(resolve));
}
