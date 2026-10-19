import type { Teardown } from '../core/types.js';

/**
 * A Scheduler is Paul Taylor's "central dispatcher", reduced to its two
 * essential capabilities: a clock and a way to run work later.
 *
 * It is passed explicitly (dependency injection) to every time-based
 * function. Nothing is patched globally, so swapping real time for
 * virtual time in tests is just passing a different value.
 */
export interface Scheduler {
  /** Current time in milliseconds. */
  readonly now: () => number;
  /** Runs `work` after `delay` ms. Returns a function that cancels it. */
  readonly schedule: (work: () => void, delay?: number) => Teardown;
}

/** Macrotask scheduler (setTimeout). The default for time-based operators. */
export const asyncScheduler: Scheduler = Object.freeze({
  now: () => Date.now(),
  schedule: (work: () => void, delay = 0) => {
    const id = setTimeout(work, Math.max(0, delay));
    return () => clearTimeout(id);
  },
});

/** Microtask scheduler (`queueMicrotask`) for delay 0; falls back to async for delays. */
export const asapScheduler: Scheduler = Object.freeze({
  now: () => Date.now(),
  schedule: (work: () => void, delay = 0) => {
    if (delay > 0) return asyncScheduler.schedule(work, delay);
    let cancelled = false;
    queueMicrotask(() => {
      if (!cancelled) work();
    });
    return () => {
      cancelled = true;
    };
  },
});

/** requestAnimationFrame scheduler (browser); falls back to ~16ms timeouts elsewhere. */
export const animationFrameScheduler: Scheduler = Object.freeze({
  now: () => (typeof performance !== 'undefined' ? performance.now() : Date.now()),
  schedule: (work: () => void, delay = 0) => {
    if (delay > 0 || typeof requestAnimationFrame !== 'function') {
      return asyncScheduler.schedule(work, delay > 0 ? delay : 16);
    }
    const id = requestAnimationFrame(() => work());
    return () => cancelAnimationFrame(id);
  },
});

/**
 * A scheduler with a manually advanced virtual clock. Time only moves when
 * you call `advanceBy`, `advanceTo` or `flush`. Deterministic: ties are run
 * in scheduling order (FIFO).
 */
export interface VirtualScheduler extends Scheduler {
  /** Runs everything due up to and including `now + ms`. */
  readonly advanceBy: (ms: number) => void;
  /** Runs everything due up to and including `time`. */
  readonly advanceTo: (time: number) => void;
  /**
   * Runs until the queue is empty or `maxTime` is reached. Throws after
   * `maxActions` actions (protects against zero-delay infinite loops).
   */
  readonly flush: (maxTime?: number, maxActions?: number) => void;
  /** Number of pending actions. */
  readonly pending: () => number;
}

interface Entry {
  readonly time: number;
  readonly seq: number;
  readonly work: () => void;
  cancelled: boolean;
}

export const createVirtualScheduler = (startTime = 0): VirtualScheduler => {
  let clock = startTime;
  let seq = 0;
  let queue: Entry[] = [];

  const insert = (e: Entry): void => {
    // keep queue sorted by (time, seq); binary search for the insertion point
    let lo = 0;
    let hi = queue.length;
    while (lo < hi) {
      const mid = (lo + hi) >>> 1;
      const m = queue[mid]!;
      if (m.time < e.time || (m.time === e.time && m.seq < e.seq)) lo = mid + 1;
      else hi = mid;
    }
    queue.splice(lo, 0, e);
  };

  const schedule = (work: () => void, delay = 0): Teardown => {
    const entry: Entry = { time: clock + Math.max(0, delay), seq: seq++, work, cancelled: false };
    insert(entry);
    return () => {
      entry.cancelled = true;
    };
  };

  const advanceTo = (time: number, maxActions = 1_000_000): void => {
    let ran = 0;
    for (;;) {
      const head = queue[0];
      if (!head || head.time > time) break;
      queue.shift();
      if (head.cancelled) continue;
      if (++ran > maxActions) {
        throw new Error(`rxjs-fp VirtualScheduler: more than ${maxActions} actions at t=${clock}; zero-delay loop?`);
      }
      clock = head.time;
      head.work();
    }
    clock = Math.max(clock, time);
    queue = queue.filter((e) => !e.cancelled);
  };

  const flush = (maxTime = Number.POSITIVE_INFINITY, maxActions = 1_000_000): void => {
    let budget = maxActions;
    for (;;) {
      const head = queue.find((e) => !e.cancelled);
      if (!head || head.time > maxTime) break;
      const before = queue.length;
      advanceTo(head.time, budget);
      budget -= Math.max(1, before - queue.length);
      if (budget <= 0) throw new Error(`rxjs-fp VirtualScheduler: more than ${maxActions} actions; zero-delay loop?`);
    }
  };

  return Object.freeze({
    now: () => clock,
    schedule,
    advanceTo: (time: number) => advanceTo(time),
    advanceBy: (ms: number) => advanceTo(clock + ms),
    flush,
    pending: () => queue.filter((e) => !e.cancelled).length,
  });
};

/**
 * A trampoline. Work with delay 0 runs synchronously, but work scheduled
 * while other work is running is queued and run afterwards on the same
 * stack, so recursive scheduling never grows the stack. Delays > 0 fall
 * back to the async scheduler.
 *
 * The queue is the scheduler's own runtime state, private to this closure.
 */
export const createQueueScheduler = (): Scheduler => {
  const queue: Array<{ readonly work: () => void; cancelled: boolean }> = [];
  let running = false;

  const schedule = (work: () => void, delay = 0): Teardown => {
    if (delay > 0) return asyncScheduler.schedule(work, delay);
    const entry = { work, cancelled: false };
    queue.push(entry);
    if (!running) {
      running = true;
      try {
        for (;;) {
          const next = queue.shift();
          if (!next) break;
          if (!next.cancelled) next.work();
        }
      } catch (e) {
        // like RxJS: a failing action drops the rest of this flush
        queue.length = 0;
        throw e;
      } finally {
        running = false;
      }
    }
    return () => {
      entry.cancelled = true;
    };
  };

  return Object.freeze({ now: () => Date.now(), schedule });
};

/** The shared trampoline scheduler. */
export const queueScheduler: Scheduler = createQueueScheduler();
