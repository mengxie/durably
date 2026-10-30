import { setTimeout as delay } from 'node:timers/promises';
import { toMilliseconds, type Duration } from '../core/duration.js';

export interface WorkerOptions {
  /** Maximum activations run concurrently per poll. */
  readonly concurrency?: number;
  /** How long to wait after a poll that found no work. */
  readonly pollInterval?: Duration;
  readonly onError?: (error: unknown) => void;
}

/** The slice of the engine a worker needs; keeps the dependency one-directional. */
export interface ActivationSource {
  tick(limit: number): Promise<number>;
}

/**
 * Long-running poll loop. Polls again immediately while there is a backlog and backs off
 * when idle. `stop()` is graceful: it lets in-flight activations finish.
 */
export class Worker {
  readonly #source: ActivationSource;
  readonly #concurrency: number;
  readonly #pollMs: number;
  readonly #onError: (error: unknown) => void;
  #controller: AbortController | undefined;
  #loop: Promise<void> | undefined;

  constructor(source: ActivationSource, options: WorkerOptions = {}) {
    this.#source = source;
    this.#concurrency = options.concurrency ?? 10;
    this.#pollMs = toMilliseconds(options.pollInterval ?? '1s');
    this.#onError = options.onError ?? (() => undefined);
  }

  get running(): boolean {
    return this.#loop !== undefined;
  }

  start(): void {
    if (this.#loop) return;
    const controller = new AbortController();
    this.#controller = controller;
    this.#loop = this.#run(controller.signal);
  }

  async stop(): Promise<void> {
    this.#controller?.abort();
    await this.#loop;
    this.#loop = undefined;
    this.#controller = undefined;
  }

  async #run(signal: AbortSignal): Promise<void> {
    while (!signal.aborted) {
      let ran = 0;
      try {
        ran = await this.#source.tick(this.#concurrency);
      } catch (error) {
        this.#onError(error);
      }
      if (ran < this.#concurrency) {
        await delay(this.#pollMs, undefined, { signal }).catch(() => undefined);
      }
    }
  }
}
