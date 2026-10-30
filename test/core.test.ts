import { describe, expect, it } from 'vitest';
import {
  ConfigurationError,
  computeBackoffMs,
  DEFAULT_RETRY_POLICY,
  defineWorkflow,
  Engine,
  InMemoryWorkflowStore,
  InvalidDurationError,
  ManualClock,
  NonRetryableError,
  serializeError,
  toMilliseconds,
} from '../src/index.js';
import { isRetryable, resolveRetryPolicy } from '../src/core/retry.js';

describe('toMilliseconds', () => {
  it.each([
    [250, 250],
    ['250ms', 250],
    ['1.5s', 1500],
    ['5m', 300_000],
    ['2h', 7_200_000],
    ['1d', 86_400_000],
  ] as const)('converts %s to %d ms', (input, expected) => {
    expect(toMilliseconds(input)).toBe(expected);
  });

  it.each([-1, Number.NaN, Infinity, '5 minutes', 'ms', '-1s'])('rejects %s', (input) => {
    expect(() => toMilliseconds(input as never)).toThrow(InvalidDurationError);
  });
});

describe('retry policy', () => {
  const policy = resolveRetryPolicy(DEFAULT_RETRY_POLICY, {
    initialInterval: '1s',
    maxInterval: '10s',
    jitter: false,
  });

  it('backs off exponentially up to the maximum interval', () => {
    expect([1, 2, 3, 4, 5].map((attempt) => computeBackoffMs(policy, attempt))).toEqual([
      1000, 2000, 4000, 8000, 10_000,
    ]);
  });

  it('applies jitter within [50%, 100%] of the delay', () => {
    const jittered = { ...policy, jitter: true };
    expect(computeBackoffMs(jittered, 3, () => 0)).toBe(2000);
    expect(computeBackoffMs(jittered, 3, () => 1)).toBe(4000);
  });

  it.each([
    [{ maxAttempts: 0 }, 'maxAttempts'],
    [{ maxAttempts: 1.5 }, 'maxAttempts'],
    [{ backoffCoefficient: 0.5 }, 'backoffCoefficient'],
    [{ initialInterval: '1m', maxInterval: '1s' }, 'maxInterval'],
  ] as const)('rejects invalid policy %o', (override, message) => {
    expect(() => resolveRetryPolicy(DEFAULT_RETRY_POLICY, override)).toThrow(message);
  });

  it('classifies retryable errors', () => {
    const named = { ...policy, nonRetryableErrors: ['TypeError'] };
    expect(isRetryable(new Error('x'), named)).toBe(true);
    expect(isRetryable(new TypeError('x'), named)).toBe(false);
    expect(isRetryable(new NonRetryableError('x'), named)).toBe(false);
    expect(isRetryable('not an error', named)).toBe(true);
  });
});

describe('serializeError', () => {
  it('keeps name, message, code and stack', () => {
    const error = Object.assign(new Error('boom'), { code: 'E_BOOM' });
    expect(serializeError(error)).toMatchObject({ name: 'Error', message: 'boom', code: 'E_BOOM' });
    expect(serializeError(error).stack).toContain('boom');
  });

  it('handles values that are not errors', () => {
    expect(serializeError('plain')).toEqual({ name: 'NonErrorThrown', message: 'plain' });
    expect(serializeError({ a: 1 })).toEqual({ name: 'NonErrorThrown', message: '{"a":1}' });
    expect(serializeError(Symbol.for('s')).message).toBe('Symbol(s)');
    const circular: { self?: unknown } = {};
    circular.self = circular;
    expect(serializeError(circular).message).toBe('[object Object]');
  });
});

describe('defineWorkflow', () => {
  it('defaults the version to 1 and freezes the definition', () => {
    const wf = defineWorkflow({ name: 'orders.fulfil-v2', run: async () => {} });
    expect(wf.version).toBe(1);
    expect(Object.isFrozen(wf)).toBe(true);
  });

  it.each(['', '1st', 'has space', 'x'.repeat(129)])('rejects the name %j', (name) => {
    expect(() => defineWorkflow({ name, run: async () => {} })).toThrow(ConfigurationError);
  });

  it.each([0, -1, 1.5])('rejects version %d', (version) => {
    expect(() => defineWorkflow({ name: 'wf', version, run: async () => {} })).toThrow(
      ConfigurationError,
    );
  });
});

describe('ManualClock', () => {
  it('only moves when told to', () => {
    const clock = new ManualClock(new Date('2026-10-30T00:00:00Z'));
    clock.advance('1h');
    expect(new Date(clock.now()).toISOString()).toBe('2026-10-30T01:00:00.000Z');
    clock.set(new Date('2027-01-01T00:00:00Z'));
    expect(clock.now()).toBe(Date.UTC(2027, 0, 1));
  });
});

describe('Engine configuration', () => {
  const store = new InMemoryWorkflowStore();
  const wf = defineWorkflow({ name: 'wf', run: async () => {} });

  it('rejects duplicate registrations', () => {
    expect(() => new Engine({ store, workflows: [wf, wf] })).toThrow(/registered twice/);
  });

  it('rejects leases that are too short to be safe', () => {
    expect(() => new Engine({ store, workflows: [wf], leaseDuration: 10 })).toThrow(/at least 1s/);
  });

  it('generates a worker id when none is given', () => {
    expect(new Engine({ store, workflows: [wf] }).workerId).toMatch(/^worker-/);
  });
});
