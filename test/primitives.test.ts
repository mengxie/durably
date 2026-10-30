import { describe, expect, it } from 'vitest';
import {
  defineWorkflow,
  NonRetryableError,
  SignalTimeoutError,
  StepFailedError,
  WorkflowClosedError,
} from '../src/index.js';
import { CallLog, createHarness, eventTypes, storeFactories } from './helpers.js';

class PaymentDeclined extends Error {
  override readonly name = 'PaymentDeclined';
}

describe.each(storeFactories)('Durable retries (%s)', (_, storeFactory) => {
  it('retries with exponential backoff, durably, until the step succeeds', async () => {
    const attempts: number[] = [];
    const flaky = defineWorkflow({
      name: 'flaky',
      run: (ctx) =>
        ctx.step(
          'call-api',
          ({ attempt }) => {
            attempts.push(attempt);
            if (attempt < 3) throw new Error(`503 on attempt ${attempt}`);
            return 'ok';
          },
          { retry: { maxAttempts: 5 } },
        ),
    });
    const { engine, clock } = createHarness(storeFactory, [flaky]);
    const handle = await engine.start(flaky, undefined);

    await engine.runUntilIdle();
    expect(await handle.describe()).toMatchObject({
      status: 'suspended',
      wakeAt: clock.now() + 1_000,
    });

    clock.advance(999);
    await engine.runUntilIdle();
    expect(attempts).toEqual([1]); // not due yet

    clock.advance(1);
    await engine.runUntilIdle(); // attempt 2 fails, next delay is 2s
    clock.advance('2s');
    await engine.runUntilIdle();

    expect(await handle.result()).toBe('ok');
    expect(attempts).toEqual([1, 2, 3]);
    expect(await eventTypes(engine, handle.instanceId)).toEqual([
      'WorkflowStarted',
      'StepAttemptFailed',
      'StepAttemptFailed',
      'StepCompleted',
      'WorkflowCompleted',
    ]);
  });

  it('lets workflow code catch a step that exhausted its retries', async () => {
    const fallback = defineWorkflow({
      name: 'fallback',
      run: async (ctx) => {
        try {
          return await ctx.step(
            'primary',
            () => {
              throw new Error('down');
            },
            { retry: { maxAttempts: 2 } },
          );
        } catch (error) {
          if (!(error instanceof StepFailedError)) throw error;
          return ctx.step('secondary', () => `fallback after ${error.attempts} attempts`);
        }
      },
    });
    const { engine, clock } = createHarness(storeFactory, [fallback]);
    const handle = await engine.start(fallback, undefined);

    await engine.runUntilIdle();
    clock.advance('1s');
    await engine.runUntilIdle();

    expect(await handle.result()).toBe('fallback after 2 attempts');
  });

  it('does not retry NonRetryableError or configured error names', async () => {
    const calls = new CallLog();
    const strict = defineWorkflow({
      name: 'strict-errors',
      run: async (ctx) => {
        const outcomes: string[] = [];
        for (const [name, error] of [
          ['validate', new NonRetryableError('bad request')],
          ['charge', new PaymentDeclined('card declined')],
        ] as const) {
          try {
            await ctx.step(
              name,
              () => {
                calls.record(name);
                throw error;
              },
              { retry: { maxAttempts: 5, nonRetryableErrors: ['PaymentDeclined'] } },
            );
          } catch (caught) {
            outcomes.push((caught as StepFailedError).failure.name);
          }
        }
        return outcomes;
      },
    });
    const { engine } = createHarness(storeFactory, [strict]);
    const handle = await engine.start(strict, undefined);
    await engine.runUntilIdle();

    expect(await handle.result()).toEqual(['NonRetryableError', 'PaymentDeclined']);
    expect(calls.count('validate')).toBe(1);
    expect(calls.count('charge')).toBe(1);
  });

  it('times out slow attempts, aborts their signal and retries them', async () => {
    let aborted = false;
    const slow = defineWorkflow({
      name: 'slow-step',
      run: (ctx) =>
        ctx.step(
          'slow',
          async ({ attempt, signal }) => {
            if (attempt === 1) {
              signal.addEventListener('abort', () => (aborted = true));
              await new Promise((resolve) => setTimeout(resolve, 200));
            }
            return attempt;
          },
          { timeout: 20, retry: { maxAttempts: 2 } },
        ),
    });
    const { engine, clock } = createHarness(storeFactory, [slow]);
    const handle = await engine.start(slow, undefined);

    await engine.runUntilIdle();
    clock.advance('1s');
    await engine.runUntilIdle();

    expect(await handle.result()).toBe(2);
    expect(aborted).toBe(true);
    const [, firstAttempt] = await engine.history(handle.instanceId);
    expect(firstAttempt).toMatchObject({
      type: 'StepAttemptFailed',
      error: { code: 'STEP_TIMEOUT' },
    });
  });
});

describe.each(storeFactories)('Durable timers (%s)', (_, storeFactory) => {
  it('sleeps across activations and resumes exactly when due', async () => {
    const reminder = defineWorkflow({
      name: 'reminder',
      run: async (ctx) => {
        await ctx.sleep('wait-a-day', '1d');
        return ctx.step('remind', () => 'sent');
      },
    });
    const { engine, clock } = createHarness(storeFactory, [reminder]);
    const handle = await engine.start(reminder, undefined);
    const startedAt = clock.now();

    await engine.runUntilIdle();
    expect(await handle.describe()).toMatchObject({
      status: 'suspended',
      wakeAt: startedAt + 86_400_000,
    });

    clock.advance('23h');
    expect(await engine.runUntilIdle()).toBe(0);

    clock.advance('1h');
    await engine.runUntilIdle();
    expect(await handle.result()).toBe('sent');
  });

  it('runs parallel branches and wakes for the earliest timer', async () => {
    const race = defineWorkflow({
      name: 'race',
      run: async (ctx) => {
        const winner = await Promise.race([
          ctx.sleep('sla', '1h').then(() => 'timeout' as const),
          ctx.sleep('quick', '5m').then(() => ctx.step('work', () => 'work-done')),
        ]);
        return winner;
      },
    });
    const { engine, clock } = createHarness(storeFactory, [race]);
    const handle = await engine.start(race, undefined);

    await engine.runUntilIdle();
    expect((await handle.describe()).wakeAt).toBe(clock.now() + 300_000);

    clock.advance('5m');
    await engine.runUntilIdle();
    expect(await handle.result()).toBe('work-done');
  });

  it('completes immediately for zero-length sleeps', async () => {
    const instant = defineWorkflow({
      name: 'instant',
      run: async (ctx) => {
        await ctx.sleep('nothing', 0);
        return 'done';
      },
    });
    const { engine } = createHarness(storeFactory, [instant]);
    const handle = await engine.start(instant, undefined);
    await engine.runUntilIdle();

    expect(await handle.result()).toBe('done');
  });
});

describe.each(storeFactories)('Signals (%s)', (_, storeFactory) => {
  const approval = defineWorkflow({
    name: 'approval',
    run: async (ctx, input: { amount: number }) => {
      if (input.amount < 1000) return 'auto-approved';
      try {
        const decision = await ctx.waitForSignal<{ approved: boolean; by: string }>('decision', {
          timeout: '2d',
        });
        return decision.approved ? `approved by ${decision.by}` : `rejected by ${decision.by}`;
      } catch (error) {
        if (error instanceof SignalTimeoutError) return 'escalated';
        throw error;
      }
    },
  });

  it('suspends until a signal arrives, then resumes with its payload', async () => {
    const { engine } = createHarness(storeFactory, [approval]);
    const handle = await engine.start(approval, { amount: 5000 });

    await engine.runUntilIdle();
    expect(await handle.describe()).toMatchObject({ status: 'suspended' });

    await handle.signal('decision', { approved: true, by: 'meng' });
    await engine.runUntilIdle();
    expect(await handle.result()).toBe('approved by meng');
  });

  it('buffers signals that arrive before the workflow waits for them', async () => {
    const { engine } = createHarness(storeFactory, [approval]);
    const handle = await engine.start(approval, { amount: 5000 });

    await handle.signal('decision', { approved: false, by: 'finance' });
    await engine.runUntilIdle();

    expect(await handle.result()).toBe('rejected by finance');
  });

  it('raises a catchable SignalTimeoutError when the timeout elapses', async () => {
    const { engine, clock } = createHarness(storeFactory, [approval]);
    const handle = await engine.start(approval, { amount: 5000 });

    await engine.runUntilIdle();
    clock.advance('2d');
    await engine.runUntilIdle();

    expect(await handle.result()).toBe('escalated');
  });

  it('consumes multiple signals of the same name in arrival order', async () => {
    const collector = defineWorkflow({
      name: 'collector',
      run: async (ctx) => {
        const items: string[] = [];
        for (let i = 0; i < 3; i++) items.push(await ctx.waitForSignal<string>('item'));
        return items;
      },
    });
    const { engine } = createHarness(storeFactory, [collector]);
    const handle = await engine.start(collector, undefined);

    await handle.signal('item', 'a');
    await engine.runUntilIdle();
    await handle.signal('item', 'b');
    await handle.signal('item', 'c');
    await engine.runUntilIdle();

    expect(await handle.result()).toEqual(['a', 'b', 'c']);
  });

  it('rejects signals to finished workflows', async () => {
    const { engine } = createHarness(storeFactory, [approval]);
    const handle = await engine.start(approval, { amount: 1 });
    await engine.runUntilIdle();

    await expect(handle.signal('decision', {})).rejects.toBeInstanceOf(WorkflowClosedError);
  });
});

describe.each(storeFactories)('Saga compensation (%s)', (_, storeFactory) => {
  function bookTrip(log: string[], failAt: 'car' | 'none') {
    return defineWorkflow({
      name: 'book-trip',
      run: async (ctx) => {
        const flight = await ctx.step('book-flight', () => 'FL-1', {
          compensate: (id) => {
            log.push(`cancel ${id}`);
          },
        });
        const hotel = await ctx.step('book-hotel', () => 'HT-7', {
          compensate: (id) => {
            log.push(`cancel ${id}`);
          },
        });
        await ctx.step('notify', () => undefined); // no compensation needed
        const car = await ctx.step(
          'book-car',
          () => {
            if (failAt === 'car') throw new NonRetryableError('no cars left');
            return 'CR-3';
          },
          { compensate: (id) => void log.push(`cancel ${id}`) },
        );
        return [flight, hotel, car];
      },
    });
  }

  it('runs compensations of completed steps in reverse order, then fails', async () => {
    const log: string[] = [];
    const trip = bookTrip(log, 'car');
    const { engine } = createHarness(storeFactory, [trip]);
    const handle = await engine.start(trip, undefined);

    await engine.runUntilIdle();

    await expect(handle.result()).rejects.toThrow('no cars left');
    expect(log).toEqual(['cancel HT-7', 'cancel FL-1']);
    const history = await engine.history(handle.instanceId);
    expect(history.filter((e) => e.type === 'StepCompleted').map((e) => e.name)).toEqual([
      'book-flight',
      'book-hotel',
      'notify',
      'compensate:book-hotel',
      'compensate:book-flight',
    ]);
  });

  it('does not compensate a successful workflow', async () => {
    const log: string[] = [];
    const trip = bookTrip(log, 'none');
    const { engine } = createHarness(storeFactory, [trip]);
    const handle = await engine.start(trip, undefined);
    await engine.runUntilIdle();

    expect(await handle.result()).toEqual(['FL-1', 'HT-7', 'CR-3']);
    expect(log).toEqual([]);
  });

  it('retries compensations durably and reports those that still fail', async () => {
    let refundAttempts = 0;
    const order = defineWorkflow({
      name: 'order-saga',
      run: async (ctx) => {
        await ctx.step('charge', () => 'ch_1', {
          compensate: () => {
            refundAttempts++;
            if (refundAttempts < 2) throw new Error('refund API unavailable');
          },
          compensationRetry: { maxAttempts: 3 },
        });
        await ctx.step('reserve', () => 'rs_1', {
          compensate: () => {
            throw new NonRetryableError('reservation already expired');
          },
        });
        throw new NonRetryableError('shipping impossible');
      },
    });
    const { engine, clock } = createHarness(storeFactory, [order]);
    const handle = await engine.start(order, undefined);

    await engine.runUntilIdle(); // refund attempt 1 fails and schedules a retry
    expect((await handle.describe()).status).toBe('suspended');

    clock.advance('1s');
    await engine.runUntilIdle();

    expect(refundAttempts).toBe(2);
    expect((await handle.describe()).status).toBe('failed');
    const failed = (await engine.history(handle.instanceId)).at(-1);
    expect(failed).toMatchObject({
      type: 'WorkflowFailed',
      error: { message: 'shipping impossible' },
      compensationErrors: [{ name: 'StepFailedError' }],
    });
  });
});

describe.each(storeFactories)('Determinism guard (%s)', (_, storeFactory) => {
  it('fails the workflow when changed code no longer matches its history', async () => {
    const v1 = defineWorkflow({
      name: 'evolving',
      run: async (ctx) => {
        await ctx.step('reserve-stock', () => 'ok');
        await ctx.sleep('wait', '1h');
      },
    });
    const v1Edited = defineWorkflow({
      name: 'evolving',
      run: async (ctx) => {
        await ctx.step('check-fraud', () => 'ok'); // inserted before existing steps
        await ctx.step('reserve-stock', () => 'ok');
        await ctx.sleep('wait', '1h');
      },
    });
    const { engine, clock, redeploy } = createHarness(storeFactory, [v1]);
    const handle = await engine.start(v1, undefined);
    await engine.runUntilIdle();

    clock.advance('1h');
    await redeploy([v1Edited]).runUntilIdle();

    await expect(handle.result()).rejects.toThrow(
      /Operation #0 was recorded as step "reserve-stock"/,
    );
    expect((await handle.describe()).error).toMatchObject({ code: 'NON_DETERMINISM' });
  });

  it('detects operations that were removed from the end of a workflow', async () => {
    const before = defineWorkflow({
      name: 'shrinking',
      run: async (ctx) => {
        await Promise.all([ctx.step('a', () => 1), ctx.sleep('b', '1h')]);
      },
    });
    const after = defineWorkflow({
      name: 'shrinking',
      run: async (ctx) => {
        await ctx.step('a', () => 1);
      },
    });
    const { engine, clock, redeploy } = createHarness(storeFactory, [before]);
    const handle = await engine.start(before, undefined);
    await engine.runUntilIdle();

    clock.advance('1h');
    await redeploy([after]).runUntilIdle();

    await expect(handle.result()).rejects.toThrow(/History contains operation #1/);
  });

  it('lets versioned workflows run side by side', async () => {
    const v1 = defineWorkflow({
      name: 'pricing',
      version: 1,
      run: (ctx) => ctx.step('price', () => 10),
    });
    const v2 = defineWorkflow({
      name: 'pricing',
      version: 2,
      run: async (ctx) => (await ctx.step('base', () => 10)) + (await ctx.step('tax', () => 1.5)),
    });
    const { engine } = createHarness(storeFactory, [v1, v2]);

    const old = await engine.start(v1, undefined);
    const current = await engine.start(v2, undefined);
    await engine.runUntilIdle();

    expect(await old.result()).toBe(10);
    expect(await current.result()).toBe(11.5);
  });
});
