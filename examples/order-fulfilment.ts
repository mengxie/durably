/**
 * Order fulfilment saga, end to end:
 *
 *   reserve stock -> charge card (flaky, retried) -> [manager approval if > $1,000]
 *   -> create shipment -> email customer
 *
 * If anything fails permanently, completed steps are compensated in reverse order
 * (refund the card, release the stock). State lives in SQLite, so you can kill the
 * process at any point and a new worker will carry on where it stopped.
 *
 * Run with: npm run example:order
 */
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import {
  defineWorkflow,
  Engine,
  NonRetryableError,
  SignalTimeoutError,
  WorkflowFailedError,
} from '../src/index.js';
import { SqliteWorkflowStore } from '../src/storage/sqlite-store.js';

// ------------------------------------------------------------------ domain & fake services

interface OrderInput {
  readonly orderId: string;
  readonly customerEmail: string;
  readonly sku: string;
  readonly quantity: number;
  readonly amountCents: number;
}

interface OrderResult {
  readonly orderId: string;
  readonly chargeId: string;
  readonly trackingNumber: string;
}

interface Approval {
  readonly approved: boolean;
  readonly approver: string;
}

class OutOfStock extends NonRetryableError {}

const inventory = {
  async reserve(sku: string, quantity: number, key: string): Promise<string> {
    await pause(20);
    if (sku === 'SOLD-OUT') throw new OutOfStock(`${sku} is out of stock`);
    console.log(`    [inventory] reserved ${quantity} x ${sku} (key ${key})`);
    return `res_${key}`;
  },
  async release(reservationId: string): Promise<void> {
    await pause(10);
    console.log(`    [inventory] released ${reservationId}`);
  },
};

const payments = {
  async charge(amountCents: number, attempt: number, key: string): Promise<string> {
    await pause(20);
    // Payment providers have bad days: the first attempt always times out.
    if (attempt === 1) throw new Error('payment gateway timeout (HTTP 504)');
    console.log(`    [payments] charged $${(amountCents / 100).toFixed(2)} (key ${key})`);
    return `ch_${key}`;
  },
  async refund(chargeId: string): Promise<void> {
    await pause(10);
    console.log(`    [payments] refunded ${chargeId}`);
  },
};

const shipping = {
  async createShipment(orderId: string, sku: string): Promise<string> {
    await pause(20);
    if (sku === 'OVERSIZED') throw new NonRetryableError('carrier rejected oversized parcel');
    return `NZ${orderId.replace(/\D/g, '').padStart(9, '0')}`;
  },
};

// ------------------------------------------------------------------------------ workflow

export const fulfilOrder = defineWorkflow({
  name: 'fulfil-order',
  version: 1,
  parseInput: (raw: unknown): OrderInput => {
    const input = raw as OrderInput;
    if (!Number.isInteger(input.quantity) || input.quantity <= 0) {
      throw new TypeError('quantity must be a positive integer');
    }
    return input;
  },
  run: async (ctx, order: OrderInput): Promise<OrderResult> => {
    await ctx.step(
      'reserve-stock',
      ({ idempotencyKey }) => inventory.reserve(order.sku, order.quantity, idempotencyKey),
      { compensate: (id) => inventory.release(id) },
    );

    const chargeId = await ctx.step(
      'charge-card',
      ({ attempt, idempotencyKey }) => payments.charge(order.amountCents, attempt, idempotencyKey),
      {
        retry: { maxAttempts: 4, initialInterval: '200ms', maxInterval: '2s' },
        timeout: '5s',
        compensate: (id) => payments.refund(id),
      },
    );

    if (order.amountCents > 100_000) {
      ctx.log('waiting for manager approval', { orderId: order.orderId });
      try {
        const approval = await ctx.waitForSignal<Approval>('manager-approval', { timeout: '3s' });
        if (!approval.approved) {
          throw new NonRetryableError(`order rejected by ${approval.approver}`);
        }
      } catch (error) {
        if (error instanceof SignalTimeoutError) {
          throw new NonRetryableError('approval not received in time');
        }
        throw error;
      }
    }

    const trackingNumber = await ctx.step('create-shipment', () =>
      shipping.createShipment(order.orderId, order.sku),
    );

    await ctx.step('email-customer', () => {
      console.log(`    [email] ${order.customerEmail}: your order ships as ${trackingNumber}`);
    });

    return { orderId: order.orderId, chargeId, trackingNumber };
  },
});

// ---------------------------------------------------------------------------------- main

async function main(): Promise<void> {
  const databasePath = join(mkdtempSync(join(tmpdir(), 'durably-example-')), 'orders.db');
  const store = new SqliteWorkflowStore({ path: databasePath });
  const engine = new Engine({
    store,
    workflows: [fulfilOrder],
    observer: {
      onLog: ({ instanceId, message }) => {
        console.log(`  ${instanceId}: ${message}`);
      },
      onActivation: ({ instanceId, outcome, durationMs }) => {
        console.log(`  ${instanceId}: activation ${outcome} in ${durationMs.toFixed(0)}ms`);
      },
    },
  });
  const worker = engine.createWorker({ pollInterval: '50ms' });
  worker.start();
  console.log(`Worker ${engine.workerId} started, state in ${databasePath}\n`);

  const order = (orderId: string, sku: string, amountCents: number) =>
    engine.start(
      fulfilOrder,
      { orderId, sku, amountCents, quantity: 1, customerEmail: `${orderId}@example.co.nz` },
      { id: orderId },
    );

  const handles = await Promise.all([
    order('order-1001', 'KEYBOARD', 18_900), // happy path, with one payment retry
    order('order-1002', 'LAPTOP', 349_900), // needs approval, which arrives
    order('order-1003', 'OVERSIZED', 59_900), // shipping fails: refund + release
    order('order-1004', 'SOLD-OUT', 9_900), // fails at the first step: nothing to undo
  ]);
  setTimeout(() => {
    void engine.signal('order-1002', 'manager-approval', { approved: true, approver: 'Aroha' });
  }, 1_000);

  const outcomes = await Promise.allSettled(handles.map((h) => h.result({ timeout: '20s' })));

  console.log('\nResults:');
  outcomes.forEach((outcome, index) => {
    const id = handles[index]?.instanceId ?? '?';
    if (outcome.status === 'fulfilled') {
      console.log(`  ✓ ${id} shipped as ${outcome.value.trackingNumber}`);
    } else {
      const error: unknown = outcome.reason;
      const reason = error instanceof WorkflowFailedError ? error.failure.message : String(error);
      console.log(`  ✗ ${id} failed: ${reason}`);
    }
  });

  console.log('\nEvent history of order-1003 (the compensated one):');
  for (const event of await engine.history('order-1003')) {
    const name = 'name' in event ? ` ${event.name}` : '';
    console.log(`  #${event.seq} ${event.type}${name}`);
  }

  await worker.stop();
  store.close();
}

function pause(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

await main();
