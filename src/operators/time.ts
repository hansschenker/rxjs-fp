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

/**
 * Shifts every value forward in time by `ms`. Completes once the source has
 * completed and the last delayed value is out. Errors are immediate.
 */
export const delay =
  <A>(ms: number, scheduler: Scheduler = asyncScheduler): Operator<A, A> =>
  (source) =>
    observable((sink) => {
      const pending = new Set<Teardown>();
      let sourceDone = false;
      const maybeComplete = (): void => {
        if (sourceDone && pending.size === 0) sink.complete();
      };
      const sub = subscribeChild(source, sink, {
        next: (a) => {
          const cancel = scheduler.schedule(() => {
            pending.delete(cancel);
            sink.next(a);
            maybeComplete();
          }, ms);
          pending.add(cancel);
        },
        complete: () => {
          sourceDone = true;
          maybeComplete();
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

/**
 * Emits a value, then ignores values for `ms` (leading by default). With
 * `trailing`, the last value seen during a window is emitted when it ends;
 * a pending trailing value delays completion until then.
 */
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
      let sourceDone = false;

      const startWindow = (): void => {
        if (sink.closed) return;
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
          if (sourceDone) sink.complete();
          else startWindow();
        } else if (sourceDone) {
          sink.complete();
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
          sourceDone = true;
          if (!(throttling && trailing && hasTrailing)) sink.complete();
        },
      });
      return () => {
        sub.unsubscribe();
        cancel();
      };
    });

/**
 * After a value arrives, waits `ms` and then emits the most recent value.
 * If the source completes during a window, the pending value is still
 * emitted at the end of the window, then the result completes.
 */
export const auditTime =
  <A>(ms: number, scheduler: Scheduler = asyncScheduler): Operator<A, A> =>
  (source) =>
    observable((sink) => {
      let cancel: Teardown | null = null;
      let last: A | undefined;
      let sourceDone = false;
      const sub = subscribeChild(source, sink, {
        next: (a) => {
          last = a;
          if (cancel === null) {
            cancel = scheduler.schedule(() => {
              cancel = null;
              sink.next(last as A);
              if (sourceDone) sink.complete();
            }, ms);
          }
        },
        complete: () => {
          sourceDone = true;
          if (cancel === null) sink.complete();
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
