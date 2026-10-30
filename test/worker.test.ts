import { afterEach, describe, expect, it } from 'vitest';
import { defineWorkflow, Engine, InMemoryWorkflowStore, Worker } from '../src/index.js';

describe('Worker (real time)', () => {
  let worker: Worker | undefined;

  afterEach(async () => {
    await worker?.stop();
  });

  it('drives timers, retries and signals end to end with the system clock', async () => {
    const wf = defineWorkflow({
      name: 'realtime',
      run: async (ctx) => {
        await ctx.sleep('short', 30);
        const value = await ctx.step(
          'flaky',
          ({ attempt }) => {
            if (attempt === 1) throw new Error('first attempt fails');
            return attempt;
          },
          { retry: { initialInterval: 20, maxInterval: 20 } },
        );
        const extra = await ctx.waitForSignal<number>('bonus');
        return value + extra;
      },
    });
    const engine = new Engine({ store: new InMemoryWorkflowStore(), workflows: [wf] });
    worker = engine.createWorker({ pollInterval: 5, concurrency: 4 });
    worker.start();
    worker.start(); // idempotent

    const handle = await engine.start(wf, undefined);
    setTimeout(() => void handle.signal('bonus', 40), 50);

    expect(await handle.result({ timeout: '5s', pollInterval: 10 })).toBe(42);
    expect(worker.running).toBe(true);

    await worker.stop();
    expect(worker.running).toBe(false);
  });

  it('reports poll errors and keeps polling', async () => {
    const errors: unknown[] = [];
    let ticks = 0;
    worker = new Worker(
      {
        tick: () => {
          ticks++;
          return ticks === 1 ? Promise.reject(new Error('db unavailable')) : Promise.resolve(0);
        },
      },
      { pollInterval: 1, onError: (error) => errors.push(error) },
    );

    worker.start();
    await new Promise((resolve) => setTimeout(resolve, 30));
    await worker.stop();

    expect(errors).toHaveLength(1);
    expect(ticks).toBeGreaterThan(1);
  });
});
