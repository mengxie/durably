import { toMilliseconds, type Duration } from './duration.js';

/** Source of time for the engine. Injected so that tests can control time precisely. */
export interface Clock {
  now(): number;
}

export const systemClock: Clock = Object.freeze({ now: () => Date.now() });

/** A clock that only moves when told to. Ideal for testing timers and retries. */
export class ManualClock implements Clock {
  #now: number;

  constructor(start: number | Date = Date.UTC(2026, 0, 1)) {
    this.#now = typeof start === 'number' ? start : start.getTime();
  }

  now(): number {
    return this.#now;
  }

  advance(duration: Duration): void {
    this.#now += toMilliseconds(duration);
  }

  set(time: number | Date): void {
    this.#now = typeof time === 'number' ? time : time.getTime();
  }
}
