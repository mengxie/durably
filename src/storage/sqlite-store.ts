import { randomUUID } from 'node:crypto';
import { DatabaseSync, type SQLInputValue, type SQLOutputValue } from 'node:sqlite';
import {
  LeaseLostError,
  WorkflowClosedError,
  WorkflowNotFoundError,
  type SerializedError,
} from '../core/errors.js';
import type { HistoryEvent, StoredEvent } from '../core/events.js';
import { isTerminal, nextWakeAt, wakeAtAfterSignal } from './store-rules.js';
import type {
  AppendEventsRequest,
  Claim,
  ClaimRequest,
  CompleteActivationRequest,
  DeliverSignalRequest,
  HeartbeatRequest,
  NewWorkflowInstance,
  WorkflowInstance,
  WorkflowStatus,
  WorkflowStore,
} from './workflow-store.js';

type Row = Record<string, SQLOutputValue>;

export interface SqliteWorkflowStoreOptions {
  /** Database file path. Defaults to an in-memory database. */
  readonly path?: string;
  /** Bring your own connection, e.g. to share it with application tables. */
  readonly database?: DatabaseSync;
  readonly tokenGenerator?: () => string;
}

/**
 * Ordered, append-only schema migrations, tracked with `PRAGMA user_version`.
 * Never edit a released migration; add a new one instead.
 */
const MIGRATIONS: readonly string[] = [
  `CREATE TABLE workflow_instances (
     id               TEXT PRIMARY KEY,
     workflow_name    TEXT    NOT NULL,
     workflow_version INTEGER NOT NULL,
     status           TEXT    NOT NULL CHECK (status IN ('pending','suspended','completed','failed')),
     input            TEXT,
     output           TEXT,
     error            TEXT,
     wake_at          INTEGER,
     signal_count     INTEGER NOT NULL DEFAULT 0,
     lease_owner      TEXT,
     lease_token      TEXT,
     lease_expires_at INTEGER,
     created_at       INTEGER NOT NULL,
     updated_at       INTEGER NOT NULL
   );
   CREATE INDEX idx_workflow_instances_due
     ON workflow_instances (wake_at)
     WHERE status IN ('pending','suspended') AND wake_at IS NOT NULL;
   CREATE TABLE workflow_events (
     instance_id TEXT    NOT NULL REFERENCES workflow_instances (id) ON DELETE CASCADE,
     seq         INTEGER NOT NULL,
     at          INTEGER NOT NULL,
     type        TEXT    NOT NULL,
     data        TEXT    NOT NULL,
     PRIMARY KEY (instance_id, seq)
   ) WITHOUT ROWID;`,
];

/**
 * Durable store on SQLite via Node's built-in `node:sqlite` (no native add-ons).
 * Uses WAL mode and `BEGIN IMMEDIATE` transactions, so several worker processes can
 * safely share one database file.
 */
export class SqliteWorkflowStore implements WorkflowStore {
  readonly #db: DatabaseSync;
  readonly #newToken: () => string;

  constructor(options: SqliteWorkflowStoreOptions = {}) {
    this.#db = options.database ?? new DatabaseSync(options.path ?? ':memory:');
    this.#newToken = options.tokenGenerator ?? randomUUID;
    this.#db.exec(
      'PRAGMA journal_mode = WAL; PRAGMA busy_timeout = 5000; PRAGMA foreign_keys = ON;',
    );
    this.#migrate();
  }

  close(): void {
    this.#db.close();
  }

  createInstance(
    request: NewWorkflowInstance,
  ): Promise<{ created: boolean; instance: WorkflowInstance }> {
    return this.#transaction(() => {
      const existing = this.#selectInstance(request.id);
      if (existing) return { created: false, instance: existing };
      this.#db
        .prepare(
          `INSERT INTO workflow_instances
             (id, workflow_name, workflow_version, status, input, wake_at, created_at, updated_at)
           VALUES (?, ?, ?, 'pending', ?, ?, ?, ?)`,
        )
        .run(
          request.id,
          request.workflowName,
          request.workflowVersion,
          toJson(request.input),
          request.now,
          request.now,
          request.now,
        );
      this.#appendEvents(request.id, request.now, [
        {
          type: 'WorkflowStarted',
          workflowName: request.workflowName,
          workflowVersion: request.workflowVersion,
          input: request.input,
        },
      ]);
      return { created: true, instance: this.#requireInstance(request.id) };
    });
  }

  getInstance(instanceId: string): Promise<WorkflowInstance | undefined> {
    return this.#query(() => this.#selectInstance(instanceId));
  }

  getHistory(instanceId: string): Promise<StoredEvent[]> {
    return this.#query(() =>
      this.#db
        .prepare('SELECT seq, at, data FROM workflow_events WHERE instance_id = ? ORDER BY seq')
        .all(instanceId)
        .map((row) => ({
          ...(JSON.parse(text(row, 'data')) as HistoryEvent),
          seq: int(row, 'seq'),
          at: int(row, 'at'),
        })),
    );
  }

  claimDue(request: ClaimRequest): Promise<Claim[]> {
    return this.#transaction(() => {
      const ids = this.#db
        .prepare(
          `SELECT id FROM workflow_instances
           WHERE status IN ('pending','suspended')
             AND wake_at IS NOT NULL AND wake_at <= ?
             AND (lease_token IS NULL OR lease_expires_at <= ?)
           ORDER BY wake_at
           LIMIT ?`,
        )
        .all(request.now, request.now, request.limit)
        .map((row) => text(row, 'id'));

      const lease = this.#db.prepare(
        `UPDATE workflow_instances
         SET lease_owner = ?, lease_token = ?, lease_expires_at = ?, updated_at = ?
         WHERE id = ?`,
      );
      return ids.map((id) => {
        const token = this.#newToken();
        lease.run(request.workerId, token, request.now + request.leaseMs, request.now, id);
        return { instance: this.#requireInstance(id), leaseToken: token };
      });
    });
  }

  appendEvents(request: AppendEventsRequest): Promise<void> {
    return this.#transaction(() => {
      this.#requireLease(request.instanceId, request.leaseToken);
      this.#appendEvents(request.instanceId, request.now, request.events);
      this.#db
        .prepare('UPDATE workflow_instances SET lease_expires_at = ?, updated_at = ? WHERE id = ?')
        .run(request.leaseExpiresAt, request.now, request.instanceId);
    });
  }

  heartbeat(request: HeartbeatRequest): Promise<void> {
    return this.#transaction(() => {
      this.#requireLease(request.instanceId, request.leaseToken);
      this.#db
        .prepare('UPDATE workflow_instances SET lease_expires_at = ? WHERE id = ?')
        .run(request.leaseExpiresAt, request.instanceId);
    });
  }

  completeActivation(request: CompleteActivationRequest): Promise<void> {
    return this.#transaction(() => {
      const instance = this.#requireLease(request.instanceId, request.leaseToken);
      this.#appendEvents(request.instanceId, request.now, request.events);
      const { next } = request;
      this.#db
        .prepare(
          `UPDATE workflow_instances
           SET status = ?, wake_at = ?, output = ?, error = ?,
               lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL, updated_at = ?
           WHERE id = ?`,
        )
        .run(
          next.status,
          nextWakeAt(request, instance.signalCount),
          'output' in next ? toJson(next.output) : toJson(instance.output),
          next.error ? JSON.stringify(next.error) : toJson(instance.error ?? undefined),
          request.now,
          request.instanceId,
        );
    });
  }

  deliverSignal(request: DeliverSignalRequest): Promise<void> {
    return this.#transaction(() => {
      const instance = this.#selectInstance(request.instanceId);
      if (!instance) throw new WorkflowNotFoundError(request.instanceId);
      if (isTerminal(instance.status)) {
        throw new WorkflowClosedError(request.instanceId, instance.status);
      }
      this.#appendEvents(request.instanceId, request.now, [
        { type: 'SignalReceived', ...request.signal },
      ]);
      this.#db
        .prepare(
          `UPDATE workflow_instances
           SET signal_count = signal_count + 1, wake_at = ?, updated_at = ?
           WHERE id = ?`,
        )
        .run(wakeAtAfterSignal(instance.wakeAt, request.now), request.now, request.instanceId);
    });
  }

  #migrate(): void {
    const current = int(this.#db.prepare('PRAGMA user_version').get() ?? {}, 'user_version');
    MIGRATIONS.slice(current).forEach((sql, index) => {
      this.#db.exec('BEGIN IMMEDIATE');
      try {
        this.#db.exec(sql);
        this.#db.exec(`PRAGMA user_version = ${current + index + 1}`);
        this.#db.exec('COMMIT');
      } catch (error) {
        this.#db.exec('ROLLBACK');
        throw error;
      }
    });
  }

  #appendEvents(instanceId: string, now: number, events: readonly HistoryEvent[]): void {
    if (events.length === 0) return;
    const last = this.#db
      .prepare('SELECT COALESCE(MAX(seq), 0) AS seq FROM workflow_events WHERE instance_id = ?')
      .get(instanceId);
    let seq = int(last ?? {}, 'seq');
    const insert = this.#db.prepare(
      'INSERT INTO workflow_events (instance_id, seq, at, type, data) VALUES (?, ?, ?, ?, ?)',
    );
    for (const event of events) {
      seq += 1;
      insert.run(instanceId, seq, now, event.type, JSON.stringify(event));
    }
  }

  #requireLease(instanceId: string, leaseToken: string): WorkflowInstance {
    const instance = this.#requireInstance(instanceId);
    if (instance.lease?.token !== leaseToken) throw new LeaseLostError(instanceId);
    return instance;
  }

  #requireInstance(instanceId: string): WorkflowInstance {
    const instance = this.#selectInstance(instanceId);
    if (!instance) throw new WorkflowNotFoundError(instanceId);
    return instance;
  }

  #selectInstance(instanceId: string): WorkflowInstance | undefined {
    const row = this.#db.prepare('SELECT * FROM workflow_instances WHERE id = ?').get(instanceId);
    return row ? toInstance(row) : undefined;
  }

  /** `node:sqlite` is synchronous, so a transaction can never interleave with other work. */
  #transaction<T>(work: () => T): Promise<T> {
    try {
      this.#db.exec('BEGIN IMMEDIATE');
      try {
        const result = work();
        this.#db.exec('COMMIT');
        return Promise.resolve(result);
      } catch (error) {
        this.#db.exec('ROLLBACK');
        throw error;
      }
    } catch (error) {
      return Promise.reject(error instanceof Error ? error : new Error(String(error)));
    }
  }

  #query<T>(read: () => T): Promise<T> {
    try {
      return Promise.resolve(read());
    } catch (error) {
      return Promise.reject(error instanceof Error ? error : new Error(String(error)));
    }
  }
}

function toInstance(row: Row): WorkflowInstance {
  const leaseToken = nullableText(row, 'lease_token');
  const errorJson = nullableText(row, 'error');
  return {
    id: text(row, 'id'),
    workflowName: text(row, 'workflow_name'),
    workflowVersion: int(row, 'workflow_version'),
    status: text(row, 'status') as WorkflowStatus,
    input: fromJson(nullableText(row, 'input')),
    output: fromJson(nullableText(row, 'output')),
    error: errorJson === null ? null : (JSON.parse(errorJson) as SerializedError),
    wakeAt: nullableInt(row, 'wake_at'),
    signalCount: int(row, 'signal_count'),
    lease:
      leaseToken === null
        ? null
        : {
            owner: text(row, 'lease_owner'),
            token: leaseToken,
            expiresAt: int(row, 'lease_expires_at'),
          },
    createdAt: int(row, 'created_at'),
    updatedAt: int(row, 'updated_at'),
  };
}

function toJson(value: unknown): SQLInputValue {
  return value === undefined ? null : JSON.stringify(value);
}

function fromJson(value: string | null): unknown {
  return value === null ? undefined : (JSON.parse(value) as unknown);
}

function text(row: Row, column: string): string {
  const value = row[column];
  if (typeof value !== 'string') throw new TypeError(`Column "${column}" is not text.`);
  return value;
}

function nullableText(row: Row, column: string): string | null {
  return row[column] === null ? null : text(row, column);
}

function int(row: Row, column: string): number {
  const value = row[column];
  if (typeof value === 'bigint') return Number(value);
  if (typeof value !== 'number') throw new TypeError(`Column "${column}" is not an integer.`);
  return value;
}

function nullableInt(row: Row, column: string): number | null {
  return row[column] === null ? null : int(row, column);
}
