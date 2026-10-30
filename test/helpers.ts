import {
  Engine,
  InMemoryWorkflowStore,
  ManualClock,
  type AnyWorkflowDefinition,
  type EngineOptions,
  type WorkflowStore,
} from '../src/index.js';
import { SqliteWorkflowStore } from '../src/storage/sqlite-store.js';

export type StoreFactory = () => WorkflowStore;

/** Engine-level suites run against every adapter to prove they are interchangeable. */
export const storeFactories: readonly (readonly [string, StoreFactory])[] = [
  ['in-memory store', () => new InMemoryWorkflowStore()],
  ['sqlite store', () => new SqliteWorkflowStore()],
];

export interface Harness {
  readonly clock: ManualClock;
  readonly store: WorkflowStore;
  readonly engine: Engine;
  readonly errors: unknown[];
  readonly logs: string[];
  /** A second engine on the same store, as if a new version of the code was deployed. */
  readonly redeploy: (workflows: readonly AnyWorkflowDefinition[]) => Engine;
}

export function createHarness(
  storeFactory: StoreFactory,
  workflows: readonly AnyWorkflowDefinition[],
  overrides: Partial<EngineOptions> = {},
): Harness {
  const clock = new ManualClock();
  const store = storeFactory();
  const errors: unknown[] = [];
  const logs: string[] = [];
  const options = (list: readonly AnyWorkflowDefinition[]): EngineOptions => ({
    store,
    clock,
    workflows: list,
    defaultRetryPolicy: { jitter: false, initialInterval: '1s', backoffCoefficient: 2 },
    observer: {
      onError: (error) => errors.push(error),
      onLog: (entry) => logs.push(entry.message),
    },
    ...overrides,
  });
  return {
    clock,
    store,
    engine: new Engine(options(workflows)),
    errors,
    logs,
    redeploy: (list) => new Engine(options(list)),
  };
}

/** Counts invocations per key: the main tool for asserting "executed exactly once". */
export class CallLog {
  readonly #counts = new Map<string, number>();

  record(key: string): void {
    this.#counts.set(key, this.count(key) + 1);
  }

  count(key: string): number {
    return this.#counts.get(key) ?? 0;
  }
}

export async function eventTypes(engine: Engine, instanceId: string): Promise<string[]> {
  return (await engine.history(instanceId)).map((event) => event.type);
}
