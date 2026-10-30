import {
  isOperationEvent,
  type HistoryEvent,
  type OperationEvent,
  type SignalReceived,
} from '../core/events.js';

/** Identifies an operation in history. Compensations get their own key space. */
export function operationKey(op: number, compensation: boolean): string {
  return compensation ? `c${op}` : String(op);
}

function keyOf(event: OperationEvent): string {
  return operationKey(event.op, 'compensation' in event);
}

/** Read model over a history snapshot, built once per activation. */
export class HistoryIndex {
  readonly #byOperation = new Map<string, OperationEvent[]>();
  readonly #signals: SignalReceived[] = [];
  readonly #signalsById = new Map<string, SignalReceived>();
  readonly #consumedSignals = new Set<string>();
  #highestOperation = -1;

  constructor(events: readonly HistoryEvent[]) {
    for (const event of events) {
      if (event.type === 'SignalReceived') {
        this.#signals.push(event);
        this.#signalsById.set(event.signalId, event);
      } else if (isOperationEvent(event)) {
        const key = keyOf(event);
        const list = this.#byOperation.get(key) ?? [];
        list.push(event);
        this.#byOperation.set(key, list);
        if (key === String(event.op)) {
          this.#highestOperation = Math.max(this.#highestOperation, event.op);
        }
        if (event.type === 'SignalConsumed') this.#consumedSignals.add(event.signalId);
      }
    }
  }

  /** Highest non-compensation operation index in history, or -1 when there is none. */
  get highestOperation(): number {
    return this.#highestOperation;
  }

  eventsFor(key: string): readonly OperationEvent[] {
    return this.#byOperation.get(key) ?? [];
  }

  signal(signalId: string): SignalReceived | undefined {
    return this.#signalsById.get(signalId);
  }

  /** Signals are consumed strictly in arrival order, which keeps consumption deterministic. */
  nextUnconsumedSignal(name: string): SignalReceived | undefined {
    return this.#signals.find(
      (signal) => signal.name === name && !this.#consumedSignals.has(signal.signalId),
    );
  }

  markConsumed(signalId: string): void {
    this.#consumedSignals.add(signalId);
  }
}
