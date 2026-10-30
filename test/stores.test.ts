import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterAll, describe, expect, it } from 'vitest';
import { InMemoryWorkflowStore } from '../src/index.js';
import { SqliteWorkflowStore } from '../src/storage/sqlite-store.js';
import { describeStoreContract } from './store-contract.js';

describeStoreContract('InMemoryWorkflowStore', () => new InMemoryWorkflowStore());
describeStoreContract('SqliteWorkflowStore (in-memory)', () => new SqliteWorkflowStore());

describe('SqliteWorkflowStore on disk', () => {
  const dir = mkdtempSync(join(tmpdir(), 'durably-'));
  afterAll(() => {
    rmSync(dir, { recursive: true, force: true });
  });

  describeStoreContract('SqliteWorkflowStore (file)', () => {
    return new SqliteWorkflowStore({ path: join(dir, `${crypto.randomUUID()}.db`) });
  });

  it('persists across connections and migrates idempotently', async () => {
    const path = join(dir, 'reopen.db');
    const first = new SqliteWorkflowStore({ path });
    await first.createInstance({
      id: 'a',
      workflowName: 'demo',
      workflowVersion: 1,
      input: 1,
      now: 5,
    });
    first.close();

    const second = new SqliteWorkflowStore({ path });
    expect(await second.getInstance('a')).toMatchObject({ id: 'a', input: 1, status: 'pending' });
    expect(await second.getHistory('a')).toHaveLength(1);
    second.close();
  });
});
