import { beforeEach, describe, expect, it } from 'vitest';
import {
  LeaseLostError,
  WorkflowClosedError,
  WorkflowNotFoundError,
  type WorkflowStore,
} from '../src/index.js';

const T0 = 1_000_000;
const LEASE = 30_000;

/**
 * Behavioural contract every WorkflowStore adapter must satisfy. A new adapter (Postgres,
 * DynamoDB, ...) is done when it passes this suite unchanged.
 */
export function describeStoreContract(name: string, createStore: () => WorkflowStore): void {
  describe(`WorkflowStore contract: ${name}`, () => {
    let store: WorkflowStore;

    beforeEach(() => {
      store = createStore();
    });

    const create = (id = 'wf-1', now = T0) =>
      store.createInstance({ id, workflowName: 'demo', workflowVersion: 1, input: { n: 1 }, now });

    const claimOne = async (now = T0) => {
      const [claim] = await store.claimDue({ now, limit: 1, workerId: 'w1', leaseMs: LEASE });
      if (!claim) throw new Error('expected a claim');
      return claim;
    };

    it('creates a pending, immediately due instance with a WorkflowStarted event', async () => {
      const { created, instance } = await create();

      expect(created).toBe(true);
      expect(instance).toMatchObject({
        status: 'pending',
        wakeAt: T0,
        input: { n: 1 },
        lease: null,
      });
      expect(await store.getHistory('wf-1')).toEqual([
        expect.objectContaining({ seq: 1, at: T0, type: 'WorkflowStarted', input: { n: 1 } }),
      ]);
    });

    it('makes createInstance idempotent per id', async () => {
      await create();
      const second = await create('wf-1', T0 + 5);

      expect(second.created).toBe(false);
      expect(second.instance.createdAt).toBe(T0);
      expect(await store.getHistory('wf-1')).toHaveLength(1);
    });

    it('returns undefined and an empty history for unknown instances', async () => {
      expect(await store.getInstance('nope')).toBeUndefined();
      expect(await store.getHistory('nope')).toEqual([]);
    });

    it('claims due instances oldest first, up to the limit', async () => {
      await create('late', T0 + 10);
      await create('early', T0);
      await create('future', T0 + 60_000);

      const claims = await store.claimDue({
        now: T0 + 10,
        limit: 5,
        workerId: 'w1',
        leaseMs: LEASE,
      });

      expect(claims.map((c) => c.instance.id)).toEqual(['early', 'late']);
      expect(claims[0]?.instance.lease).toMatchObject({ owner: 'w1', expiresAt: T0 + 10 + LEASE });
    });

    it('does not hand out a leased instance until its lease expires', async () => {
      await create();
      const first = await claimOne();

      expect(
        await store.claimDue({ now: T0 + 1, limit: 1, workerId: 'w2', leaseMs: LEASE }),
      ).toEqual([]);

      const [second] = await store.claimDue({
        now: T0 + LEASE,
        limit: 1,
        workerId: 'w2',
        leaseMs: LEASE,
      });
      expect(second?.leaseToken).toBeDefined();
      expect(second?.leaseToken).not.toBe(first.leaseToken);
    });

    it('appends events with increasing sequence numbers and renews the lease', async () => {
      await create();
      const { leaseToken } = await claimOne();

      await store.appendEvents({
        instanceId: 'wf-1',
        leaseToken,
        now: T0 + 5,
        leaseExpiresAt: T0 + 99_999,
        events: [
          { type: 'TimerStarted', op: 0, name: 'nap', fireAt: T0 + 100 },
          { type: 'TimerFired', op: 0, name: 'nap' },
        ],
      });

      const history = await store.getHistory('wf-1');
      expect(history.map((e) => [e.seq, e.type])).toEqual([
        [1, 'WorkflowStarted'],
        [2, 'TimerStarted'],
        [3, 'TimerFired'],
      ]);
      expect((await store.getInstance('wf-1'))?.lease?.expiresAt).toBe(T0 + 99_999);
    });

    it('fences out stale lease holders', async () => {
      await create();
      const stale = await claimOne();
      await claimOne(T0 + LEASE); // lease expired; another worker takes over

      const write = {
        instanceId: 'wf-1',
        leaseToken: stale.leaseToken,
        now: T0,
        leaseExpiresAt: T0,
      };
      await expect(store.appendEvents({ ...write, events: [] })).rejects.toBeInstanceOf(
        LeaseLostError,
      );
      await expect(store.heartbeat(write)).rejects.toBeInstanceOf(LeaseLostError);
      await expect(
        store.completeActivation({
          ...write,
          events: [],
          next: { status: 'completed', wakeAt: null, output: 1 },
          observedSignalCount: 0,
        }),
      ).rejects.toBeInstanceOf(LeaseLostError);
    });

    it('extends a lease on heartbeat', async () => {
      await create();
      const { leaseToken } = await claimOne();
      await store.heartbeat({ instanceId: 'wf-1', leaseToken, leaseExpiresAt: T0 + 123_456 });

      expect((await store.getInstance('wf-1'))?.lease?.expiresAt).toBe(T0 + 123_456);
    });

    it('completes an activation: writes events, sets state and releases the lease', async () => {
      await create();
      const { leaseToken } = await claimOne();

      await store.completeActivation({
        instanceId: 'wf-1',
        leaseToken,
        now: T0 + 7,
        observedSignalCount: 0,
        events: [{ type: 'WorkflowCompleted', output: { ok: true } }],
        next: { status: 'completed', wakeAt: null, output: { ok: true } },
      });

      expect(await store.getInstance('wf-1')).toMatchObject({
        status: 'completed',
        output: { ok: true },
        wakeAt: null,
        lease: null,
        updatedAt: T0 + 7,
      });
      expect((await store.getHistory('wf-1')).at(-1)?.type).toBe('WorkflowCompleted');
    });

    it('records the error of a failed workflow', async () => {
      await create();
      const { leaseToken } = await claimOne();
      const error = { name: 'Error', message: 'boom' };

      await store.completeActivation({
        instanceId: 'wf-1',
        leaseToken,
        now: T0,
        observedSignalCount: 0,
        events: [],
        next: { status: 'failed', wakeAt: null, error },
      });

      expect(await store.getInstance('wf-1')).toMatchObject({ status: 'failed', error });
    });

    it('delivers signals and makes a waiting instance due', async () => {
      await create();
      const { leaseToken } = await claimOne();
      await store.completeActivation({
        instanceId: 'wf-1',
        leaseToken,
        now: T0,
        observedSignalCount: 0,
        events: [],
        next: { status: 'suspended', wakeAt: null },
      });

      await store.deliverSignal({
        instanceId: 'wf-1',
        now: T0 + 50,
        signal: { signalId: 's-1', name: 'approve', payload: { by: 'meng' } },
      });

      expect(await store.getInstance('wf-1')).toMatchObject({ wakeAt: T0 + 50, signalCount: 1 });
      expect((await store.getHistory('wf-1')).at(-1)).toMatchObject({
        type: 'SignalReceived',
        signalId: 's-1',
        payload: { by: 'meng' },
      });
    });

    it('prevents lost wake-ups when a signal arrives during an activation', async () => {
      await create();
      const claim = await claimOne();
      await store.deliverSignal({
        instanceId: 'wf-1',
        now: T0 + 1,
        signal: { signalId: 's-1', name: 'approve', payload: null },
      });

      await store.completeActivation({
        instanceId: 'wf-1',
        leaseToken: claim.leaseToken,
        now: T0 + 2,
        observedSignalCount: claim.instance.signalCount,
        events: [],
        next: { status: 'suspended', wakeAt: null },
      });

      expect((await store.getInstance('wf-1'))?.wakeAt).toBe(T0 + 2);
    });

    it('rejects signals for unknown or finished instances', async () => {
      const signal = { signalId: 's', name: 'x', payload: null };
      await expect(
        store.deliverSignal({ instanceId: 'nope', now: T0, signal }),
      ).rejects.toBeInstanceOf(WorkflowNotFoundError);

      await create();
      const { leaseToken } = await claimOne();
      await store.completeActivation({
        instanceId: 'wf-1',
        leaseToken,
        now: T0,
        observedSignalCount: 0,
        events: [],
        next: { status: 'completed', wakeAt: null, output: null },
      });
      await expect(
        store.deliverSignal({ instanceId: 'wf-1', now: T0, signal }),
      ).rejects.toBeInstanceOf(WorkflowClosedError);
    });

    it('isolates stored data from caller mutation', async () => {
      const input = { nested: { value: 1 } };
      await store.createInstance({
        id: 'iso',
        workflowName: 'demo',
        workflowVersion: 1,
        input,
        now: T0,
      });
      input.nested.value = 2;

      expect((await store.getInstance('iso'))?.input).toEqual({ nested: { value: 1 } });
    });
  });
}
