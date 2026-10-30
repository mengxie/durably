import { describe, expect, it, vi } from 'vitest';
import {
  defineWorkflow,
  Engine,
  InMemoryWorkflowStore,
  LeaseLostError,
  type AppendEventsRequest,
  type HeartbeatRequest,
  type WorkflowStore,
} from '../src/index.js';
import { CallLog, createHarness, storeFactories } from './helpers.js';

/** Wraps a store and fails selected calls, to simulate crashes and network partitions. */
function faultyStore(
  inner: WorkflowStore,
  faults: {
    appendEvents?: (request: AppendEventsRequest, call: number) => boolean;
    heartbeat?: (request: HeartbeatRequest) => boolean;
  },
): WorkflowStore {
  let appendCalls = 0;
  return {
    createInstance: (r) => inner.createInstance(r),
    getInstance: (id) => inner.getInstance(id),
    getHistory: (id) => inner.getHistory(id),
    claimDue: (r) => inner.claimDue(r),
    completeActivation: (r) => inner.completeActivation(r),
    deliverSignal: (r) => inner.deliverSignal(r),
    appendEvents: (r) =>
      faults.appendEvents?.(r, ++appendCalls)
        ? Promise.reject(new Error('connection reset'))
        : inner.appendEvents(r),
    heartbeat: (r) =>
      faults.heartbeat?.(r) ? Promise.reject(new LeaseLostError(r.instanceId)) : inner.heartbeat(r),
  };
}

describe.each(storeFactories)('Crash recovery (%s)', (_, storeFactory) => {
  it('resumes after a crash, re-executing at most the step that was in flight', async () => {
    const calls = new CallLog();
    const pipeline = defineWorkflow({
      name: 'pipeline',
      run: async (ctx) => {
        for (const step of ['extract', 'transform', 'load']) {
          await ctx.step(step, () => {
            calls.record(step);
          });
        }
        return 'done';
      },
    });
    const { engine, clock, store, errors } = createHarness(storeFactory, [pipeline]);
    // Worker A crashes while persisting the result of the second step.
    const crashing = new Engine({
      store: faultyStore(store, { appendEvents: (_r, call) => call === 2 }),
      clock,
      workflows: [pipeline],
      observer: { onError: (error) => errors.push(error) },
    });

    const handle = await crashing.start(pipeline, undefined);
    await crashing.runUntilIdle();
    expect(errors).toHaveLength(1);
    expect((await handle.describe()).lease).not.toBeNull(); // still held by the dead worker

    clock.advance('30s'); // lease expires
    await engine.runUntilIdle(); // worker B takes over

    expect(await handle.result()).toBe('done');
    expect(calls.count('extract')).toBe(1); // checkpointed before the crash
    expect(calls.count('transform')).toBe(2); // ran, but its result was never recorded
    expect(calls.count('load')).toBe(1);
  });

  it('fences out a worker whose lease was taken over', async () => {
    const calls = new CallLog();
    let releaseSlowStep: () => void = () => undefined;
    const slow = defineWorkflow({
      name: 'slow',
      run: async (ctx) => {
        await ctx.step('long-call', async () => {
          calls.record('long-call');
          if (calls.count('long-call') === 1) {
            await new Promise<void>((resolve) => (releaseSlowStep = resolve));
          }
        });
        await ctx.step('after', () => {
          calls.record('after');
        });
      },
    });
    const { clock, store, errors } = createHarness(storeFactory, [slow]);
    const options = {
      store,
      clock,
      workflows: [slow],
      observer: { onError: (e: unknown) => errors.push(e) },
    };
    const workerA = new Engine({ ...options, workerId: 'A' });
    const workerB = new Engine({ ...options, workerId: 'B' });

    const handle = await workerA.start(slow, undefined);
    const stalled = workerA.tick(); // A is stuck inside the step (e.g. a GC pause)
    await vi.waitFor(() => {
      expect(calls.count('long-call')).toBe(1);
    });

    clock.advance('31s');
    await workerB.runUntilIdle(); // B finishes the workflow

    releaseSlowStep();
    await stalled; // A wakes up and tries to record its stale result

    await expect(handle.result()).resolves.toBeUndefined();
    expect(calls.count('after')).toBe(1);
    expect(errors.some((error) => error instanceof LeaseLostError)).toBe(true);
    expect((await handle.describe()).status).toBe('completed');
  });

  it('aborts an activation when its heartbeat discovers the lease was lost', async () => {
    const steps = new CallLog();
    const longRunning = defineWorkflow({
      name: 'long-running',
      run: async (ctx) => {
        await ctx.step('slow', async () => {
          steps.record('slow');
          await new Promise((resolve) => setTimeout(resolve, 400));
        });
      },
    });
    const { clock, store, errors } = createHarness(storeFactory, [longRunning]);
    const engine = new Engine({
      store: faultyStore(store, { heartbeat: () => true }),
      clock,
      workflows: [longRunning],
      leaseDuration: '1s', // heartbeat every ~333ms
      observer: { onError: (error) => errors.push(error) },
    });

    const handle = await engine.start(longRunning, undefined);
    await engine.runUntilIdle();

    expect(errors.some((error) => error instanceof LeaseLostError)).toBe(true);
    expect((await handle.describe()).status).toBe('pending'); // nothing committed
  });
});

describe.each(storeFactories)('Concurrency (%s)', (_, storeFactory) => {
  it('lets many workers share one store without executing any step twice', async () => {
    const calls = new CallLog();
    const job = defineWorkflow({
      name: 'job',
      run: async (ctx, input: { id: number }) => {
        const a = await ctx.step('a', () => {
          calls.record(`${input.id}:a`);
          return input.id;
        });
        const [b, c] = await Promise.all([
          ctx.step('b', () => {
            calls.record(`${input.id}:b`);
            return a * 2;
          }),
          ctx.step('c', () => {
            calls.record(`${input.id}:c`);
            return a * 3;
          }),
        ]);
        return b + c;
      },
    });
    const { clock, store } = createHarness(storeFactory, [job]);
    const workers = ['w1', 'w2', 'w3'].map(
      (workerId) => new Engine({ store, clock, workflows: [job], workerId }),
    );
    const [first] = workers;

    const handles = await Promise.all(
      Array.from({ length: 30 }, (_, id) => first!.start(job, { id })),
    );
    await Promise.all(workers.map((worker) => worker.runUntilIdle()));

    for (const [id, handle] of handles.entries()) {
      expect(await handle.result()).toBe(id * 5);
      for (const step of ['a', 'b', 'c']) expect(calls.count(`${id}:${step}`)).toBe(1);
    }
  });
});

describe('Observer isolation', () => {
  it('keeps running when an observer throws', async () => {
    const wf = defineWorkflow({ name: 'observed', run: (ctx) => ctx.step('x', () => 1) });
    const engine = new Engine({
      store: new InMemoryWorkflowStore(),
      workflows: [wf],
      observer: {
        onWorkflowStarted: () => {
          throw new Error('metrics backend down');
        },
        onActivation: () => {
          throw new Error('metrics backend down');
        },
      },
    });

    const handle = await engine.start(wf, undefined);
    await engine.runUntilIdle();

    expect(await handle.result()).toBe(1);
  });

  it('reports activation outcomes', async () => {
    const outcomes: string[] = [];
    const wf = defineWorkflow({
      name: 'reported',
      run: (ctx) => ctx.sleep('nap', 0),
    });
    const engine = new Engine({
      store: new InMemoryWorkflowStore(),
      workflows: [wf],
      observer: { onActivation: (report) => outcomes.push(report.outcome) },
    });

    await engine.start(wf, undefined);
    await engine.runUntilIdle();

    expect(outcomes).toEqual(['completed']);
  });
});
