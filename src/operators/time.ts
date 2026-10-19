import { observable, subscribeChild } from '../core/observable.js';
import { timeoutError } from '../core/errors.js';
import type { Operator, Teardown } from '../core/types.js';
import { asyncScheduler, type Scheduler } from '../scheduler/scheduler.js';

/**
 * Time-based operators. Every one takes the Scheduler as an explicit last
 * argument (default: `asyncScheduler`). Tests pass a virtual scheduler;
 * nothing is patched globally.
 */

const noop: Teardown = () => {};

/** Shifts every value (and completion) forward in time by `ms`. Errors are immediate. */
export const delay =
  <A>(ms: number, scheduler: Scheduler = asyncScheduler): Operator<A, A> =>
  (source) =>
    observable((sink) => {
      const pending = new Set<Teardown>();
      let sourceDone = false;
      const run = (f: () => void): void => {
        const cancel = scheduler.schedule(() => {
          pending.delete(cancel);
          f();
        }, ms);
        pending.add(cancel);
      };
      const sub = subscribeChild(source, sink, {
        next: (a) => run(() => sink.next(a)),
        complete: () => {
          sourceDone = true;
          run(() => sourceDone && sink.complete());
        },
      });
      return () => {
        sub.unsubscribe();
        pending.forEach((c) => c());
        pending.clear();
      };
    });

/** Emits a value only after `ms` of silence. Pending value is flushed on completion. */
export const debounceTime =
  <A>(ms: number, scheduler: Scheduler = asyncScheduler): Operator<A, A> =>
  (source) =>
    observable((sink) => {
      let cancel: Teardown = noop;
      let has = false;
      let last: A | undefined;
      const emit = (): void => {
        if (!has) return;
        has = false;
        const v = last as A;
        last = undefined;
        sink.next(v);
      };
      const sub = subscribeChild(source, sink, {
        next: (a) => {
          cancel();
          has = true;
          last = a;
          cancel = scheduler.schedule(emit, ms);
        },
        complete: () => {
          cancel();
          emit();
          sink.complete();
        },
      });
      return () => {
        sub.unsubscribe();
        cancel();
      };
    });

export interface ThrottleConfig {
  readonly leading?: boolean;
  readonly trailing?: boolean;
}

/** Emits a value, then ignores values for `ms` (leading by default; trailing optional). */
export const throttleTime =
  <A>(ms: number, scheduler: Scheduler = asyncScheduler, config: ThrottleConfig = {}): Operator<A, A> =>
  (source) =>
    observable((sink) => {
      const leading = config.leading ?? true;
      const trailing = config.trailing ?? false;
      let throttling = false;
      let cancel: Teardown = noop;
      let hasTrailing = false;
      let trailingValue: A | undefined;

      const startWindow = (): void => {
        throttling = true;
        cancel = scheduler.schedule(endWindow, ms);
      };
      const endWindow = (): void => {
        throttling = false;
        if (trailing && hasTrailing) {
          hasTrailing = false;
          const v = trailingValue as A;
          trailingValue = undefined;
          sink.next(v);
          startWindow();
        }
      };
      const sub = subscribeChild(source, sink, {
        next: (a) => {
          if (!throttling) {
            if (leading) sink.next(a);
            else {
              hasTrailing = true;
              trailingValue = a;
            }
            startWindow();
          } else if (trailing) {
            hasTrailing = true;
            trailingValue = a;
          }
        },
        complete: () => {
          if (trailing && hasTrailing) sink.next(trailingValue as A);
          sink.complete();
        },
      });
      return () => {
        sub.unsubscribe();
        cancel();
      };
    });

/** After a value arrives, waits `ms` and then emits the most recent value. */
export const auditTime =
  <A>(ms: number, scheduler: Scheduler = asyncScheduler): Operator<A, A> =>
  (source) =>
    observable((sink) => {
      let cancel: Teardown | null = null;
      let last: A | undefined;
      const sub = subscribeChild(source, sink, {
        next: (a) => {
          last = a;
          if (cancel === null) {
            cancel = scheduler.schedule(() => {
              cancel = null;
              sink.next(last as A);
            }, ms);
          }
        },
      });
      return () => {
        sub.unsubscribe();
        cancel?.();
      };
    });

/** Emits the most recent value (if any new one arrived) every `ms`. */
export const sampleTime =
  <A>(ms: number, scheduler: Scheduler = asyncScheduler): Operator<A, A> =>
  (source) =>
    observable((sink) => {
      let fresh = false;
      let last: A | undefined;
      let cancel: Teardown = noop;
      const tick = (): void => {
        if (fresh) {
          fresh = false;
          sink.next(last as A);
        }
        if (!sink.closed) cancel = scheduler.schedule(tick, ms);
      };
      cancel = scheduler.schedule(tick, ms);
      const sub = subscribeChild(source, sink, {
        next: (a) => {
          fresh = true;
          last = a;
        },
      });
      return () => {
        sub.unsubscribe();
        cancel();
      };
    });

/** Errors with `TimeoutError` if the first value, or the gap between values, exceeds `ms`. */
export const timeout =
  <A>(ms: number, scheduler: Scheduler = asyncScheduler): Operator<A, A> =>
  (source) =>
    observable((sink) => {
      let cancel: Teardown = noop;
      const arm = (): void => {
        cancel();
        cancel = scheduler.schedule(() => sink.error(timeoutError()), ms);
      };
      arm();
      const sub = subscribeChild(source, sink, {
        next: (a) => {
          arm();
          sink.next(a);
        },
      });
      return () => {
        sub.unsubscribe();
        cancel();
      };
    });

/** Subscribes to the source through the scheduler (after `delayMs`). */
export const subscribeOn =
  <A>(scheduler: Scheduler, delayMs = 0): Operator<A, A> =>
  (source) =>
    observable((sink) => {
      let inner: (() => void) | null = null;
      const cancel = scheduler.schedule(() => {
        inner = subscribeChild(source, sink).unsubscribe;
      }, delayMs);
      return () => {
        cancel();
        inner?.();
      };
    });

/** Re-delivers every notification through the scheduler. */
export const observeOn =
  <A>(scheduler: Scheduler, delayMs = 0): Operator<A, A> =>
  (source) =>
    observable((sink) => {
      const pending = new Set<Teardown>();
      const run = (f: () => void): void => {
        const cancel = scheduler.schedule(() => {
          pending.delete(cancel);
          f();
        }, delayMs);
        pending.add(cancel);
      };
      const sub = subscribeChild(source, sink, {
        next: (a) => run(() => sink.next(a)),
        error: (e) => run(() => sink.error(e)),
        complete: () => run(() => sink.complete()),
      });
      return () => {
        sub.unsubscribe();
        pending.forEach((c) => c());
      };
    });
