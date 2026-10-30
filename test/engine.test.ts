import { describe, expect, it } from 'vitest';
import {
  defineWorkflow,
  WorkflowAlreadyExistsError,
  WorkflowFailedError,
  WorkflowNotFoundError,
  WorkflowNotRegisteredError,
  ResultTimeoutError,
} from '../src/index.js';
import { CallLog, createHarness, eventTypes, storeFactories } from './helpers.js';

describe.each(storeFactories)('Engine basics (%s)', (_, storeFactory) => {
  it('runs a workflow to completion and returns its typed output', async () => {
    const greet = defineWorkflow({
      name: 'greet',
      run: async (ctx, input: { name: string }) => {
        const greeting = await ctx.step('compose', () => `Kia ora, ${input.name}`);
        return { greeting, length: greeting.length };
      },
    });
    const { engine } = createHarness(storeFactory, [greet]);

    const handle = await engine.start(greet, { name: 'Meng' });
    await engine.runUntilIdle();

    const output: { greeting: string; length: number } = await handle.result();
    expect(output).toEqual({ greeting: 'Kia ora, Meng', length: 13 });
    expect(await handle.describe()).toMatchObject({ status: 'completed', lease: null });
    expect(await eventTypes(engine, handle.instanceId)).toEqual([
      'WorkflowStarted',
      'StepCompleted',
      'WorkflowCompleted',
    ]);
  });

  it('never re-executes a completed step when the workflow is replayed', async () => {
    const calls = new CallLog();
    const twoPhase = defineWorkflow({
      name: 'two-phase',
      run: async (ctx) => {
        await ctx.step('charge', () => {
          calls.record('charge');
        });
        await ctx.sleep('cool-off', '1h'); // forces a second activation, i.e. a replay
        await ctx.step('ship', () => {
          calls.record('ship');
        });
      },
    });
    const { engine, clock } = createHarness(storeFactory, [twoPhase]);

    const handle = await engine.start(twoPhase, undefined);
    await engine.runUntilIdle();
    clock.advance('1h');
    await engine.runUntilIdle();

    await handle.result();
    expect(calls.count('charge')).toBe(1);
    expect(calls.count('ship')).toBe(1);
  });

  it('gives steps a stable idempotency key across retries', async () => {
    const keys: string[] = [];
    const flaky = defineWorkflow({
      name: 'flaky-key',
      run: async (ctx) =>
        ctx.step('call', ({ attempt, idempotencyKey }) => {
          keys.push(idempotencyKey);
          if (attempt === 1) throw new Error('transient');
          return attempt;
        }),
    });
    const { engine, clock } = createHarness(storeFactory, [flaky]);

    const handle = await engine.start(flaky, undefined, { id: 'order-42' });
    await engine.runUntilIdle();
    clock.advance('1s');
    await engine.runUntilIdle();

    expect(await handle.result()).toBe(2);
    expect(keys).toEqual(['order-42:0', 'order-42:0']);
  });

  it('normalises step results through JSON so live runs and replays agree', async () => {
    const dates = defineWorkflow({
      name: 'dates',
      run: async (ctx) => {
        const when = await ctx.step('when', () => new Date(Date.UTC(2026, 9, 30)));
        return typeof when;
      },
    });
    const { engine } = createHarness(storeFactory, [dates]);

    const handle = await engine.start(dates, undefined);
    await engine.runUntilIdle();

    expect(await handle.result()).toBe('string');
  });

  it('treats start with an existing id as idempotent', async () => {
    const calls = new CallLog();
    const once = defineWorkflow({
      name: 'once',
      run: async (ctx) => {
        await ctx.step('work', () => {
          calls.record('work');
        });
      },
    });
    const { engine } = createHarness(storeFactory, [once]);

    const first = await engine.start(once, undefined, { id: 'order-1' });
    const second = await engine.start(once, undefined, { id: 'order-1' });
    await engine.runUntilIdle();

    expect(first.created).toBe(true);
    expect(second.created).toBe(false);
    expect(calls.count('work')).toBe(1);
  });

  it('refuses to reuse an id that belongs to a different workflow', async () => {
    const a = defineWorkflow({ name: 'a', run: async () => {} });
    const b = defineWorkflow({ name: 'b', run: async () => {} });
    const { engine } = createHarness(storeFactory, [a, b]);

    await engine.start(a, undefined, { id: 'shared' });
    await expect(engine.start(b, undefined, { id: 'shared' })).rejects.toBeInstanceOf(
      WorkflowAlreadyExistsError,
    );
  });

  it('validates input with parseInput before anything is persisted', async () => {
    const strict = defineWorkflow({
      name: 'strict',
      parseInput: (raw: unknown) => {
        if (typeof raw !== 'number' || raw < 0) throw new TypeError('expected a positive number');
        return raw;
      },
      run: async (_ctx, input: number) => input * 2,
    });
    const { engine } = createHarness(storeFactory, [strict]);

    await expect(engine.start(strict, -1, { id: 'bad' })).rejects.toThrow(
      'expected a positive number',
    );
    expect(await engine.describe('bad')).toBeUndefined();

    const handle = await engine.start(strict, 21);
    await engine.runUntilIdle();
    expect(await handle.result()).toBe(42);
  });

  it('fails the workflow when its code throws, and surfaces the error to callers', async () => {
    const broken = defineWorkflow({
      name: 'broken',
      run: () => Promise.reject(new RangeError('invalid quantity')),
    });
    const { engine } = createHarness(storeFactory, [broken]);

    const handle = await engine.start(broken, undefined);
    await engine.runUntilIdle();

    const failure = handle.result();
    await expect(failure).rejects.toBeInstanceOf(WorkflowFailedError);
    await expect(failure).rejects.toThrow('invalid quantity');
    expect((await handle.describe()).error).toMatchObject({ name: 'RangeError' });
  });

  it('fails a workflow whose output cannot be stored as JSON', async () => {
    const bigint = defineWorkflow({ name: 'bigint', run: async () => 10n });
    const { engine } = createHarness(storeFactory, [bigint]);

    const handle = await engine.start(bigint, undefined);
    await engine.runUntilIdle();

    await expect(handle.result()).rejects.toThrow('not JSON-serializable');
  });

  it('suppresses workflow logs while replaying', async () => {
    const chatty = defineWorkflow({
      name: 'chatty',
      run: async (ctx) => {
        ctx.log('starting');
        await ctx.sleep('pause', '1m');
        ctx.log('resumed', { replaying: ctx.isReplaying });
      },
    });
    const { engine, clock, logs } = createHarness(storeFactory, [chatty]);

    await engine.start(chatty, undefined);
    await engine.runUntilIdle();
    clock.advance('1m');
    await engine.runUntilIdle();

    expect(logs).toEqual(['starting', 'resumed']);
  });

  it('rejects unknown workflows and instances', async () => {
    const known = defineWorkflow({ name: 'known', run: async () => {} });
    const unknown = defineWorkflow({ name: 'unknown', run: async () => {} });
    const { engine } = createHarness(storeFactory, [known]);

    await expect(engine.start(unknown, undefined)).rejects.toBeInstanceOf(
      WorkflowNotRegisteredError,
    );
    await expect(engine.result('missing')).rejects.toBeInstanceOf(WorkflowNotFoundError);
    await expect(engine.handle('missing').describe()).rejects.toBeInstanceOf(WorkflowNotFoundError);
  });

  it('times out while waiting for a result', async () => {
    const slow = defineWorkflow({ name: 'slow', run: (ctx) => ctx.sleep('forever', '365d') });
    const { engine } = createHarness(storeFactory, [slow]);

    const handle = await engine.start(slow, undefined);
    await engine.runUntilIdle();

    await expect(handle.result({ timeout: 30, pollInterval: 10 })).rejects.toBeInstanceOf(
      ResultTimeoutError,
    );
  });

  it('defers instances of workflows this engine does not know (rolling deploys)', async () => {
    const v2 = defineWorkflow({ name: 'evolving', version: 2, run: async () => 'v2' });
    const v1 = defineWorkflow({ name: 'evolving', version: 1, run: async () => 'v1' });
    const { engine, clock, errors, redeploy } = createHarness(storeFactory, [v1, v2]);

    const oldHandle = await engine.start(v1, undefined);
    const newEngine = redeploy([v2]); // v1 code was removed too early
    await newEngine.runUntilIdle();

    expect((await oldHandle.describe()).status).toBe('pending');
    expect(errors[0]).toBeInstanceOf(WorkflowNotRegisteredError);

    clock.advance('1m');
    await engine.runUntilIdle(); // an engine that still has v1 picks it up
    expect(await oldHandle.result()).toBe('v1');
  });
});
