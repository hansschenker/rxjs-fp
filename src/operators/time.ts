import { observable, subscribeChild } from '../core/observable.js';
import { from } from '../creation/creation.js';
import { timeoutError } from '../core/errors.js';
import type { Operator, Subscription, Teardown } from '../core/types.js';
import { asyncScheduler, type Scheduler } from '../scheduler/scheduler.js';
import type { ObservableInput } from '../creation/creation.js';
import { subscribeInner, type Inner } from './inner.js';
import { mergeMap } from './flatten.js';
import { take } from './filtering.js';
import { as, map } from './transform.js';

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

export interface TimeoutInfo<A> {
  /** Number of values seen before the timeout. */
  readonly seen: number;
  /** The last value seen, or `null` if none. */
  readonly lastValue: A | null;
}

export interface TimeoutConfig<A, B = never> {
  /** Maximum gap (ms) between subscription and the first value, and between values. */
  readonly each?: number;
  /** Deadline for the first value: ms after subscription, or an absolute `Date`. Defaults to `each`. */
  readonly first?: number | Date;
  /** Switch to this Observable on timeout instead of erroring with `TimeoutError`. */
  readonly with?: (info: TimeoutInfo<A>) => ObservableInput<B>;
  readonly scheduler?: Scheduler;
}

/**
 * Base case: `timeout(ms, scheduler)` errors with `TimeoutError` if the
 * first value, or the gap between two values, exceeds `ms`.
 *
 * The config form adds a separate deadline for the first value (`first`,
 * alone it only guards the first value) and a fallback Observable (`with`).
 */
export function timeout<A>(ms: number, scheduler?: Scheduler): Operator<A, A>;
export function timeout<A, B = never>(config: TimeoutConfig<A, B>): Operator<A, A | B>;
export function timeout<A, B>(config: number | TimeoutConfig<A, B>, scheduler: Scheduler = asyncScheduler): Operator<A, A | B> {
  const cfg: TimeoutConfig<A, B> = typeof config === 'number' ? { each: config, scheduler } : config;
  if (cfg.each == null && cfg.first == null) throw new TypeError('rxjs-fp timeout: provide `each` or `first`');
  const sch = cfg.scheduler ?? asyncScheduler;
  return (source) =>
    observable<A | B>((sink) => {
      let seen = 0;
      let lastValue: A | null = null;
      let timedOut = false;
      let cancel: Teardown = noop;
      let src: Subscription | null = null;
      let fallback: Subscription | null = null;

      const onTimeout = (): void => {
        timedOut = true;
        src?.unsubscribe();
        if (cfg.with) fallback = subscribeChild(from(cfg.with({ seen, lastValue })), sink);
        else sink.error(timeoutError());
      };
      const arm = (ms: number): void => {
        cancel();
        cancel = sch.schedule(onTimeout, ms);
      };
      src = subscribeChild(source, sink, {
        next: (a) => {
          if (timedOut) return;
          seen++;
          lastValue = a;
          cancel();
          cancel = noop;
          if (cfg.each != null) arm(cfg.each);
          sink.next(a);
        },
        complete: () => {
          if (!timedOut) sink.complete();
        },
      });
      if (timedOut) src.unsubscribe();
      // Armed after subscribing, like RxJS: source events due at the same
      // virtual time as the deadline were scheduled first and win the tie.
      const first = cfg.first;
      if (seen === 0 && !sink.closed) {
        if (first == null) arm(cfg.each as number);
        else arm(typeof first === 'number' ? first : first.getTime() - sch.now());
      }
      return () => {
        cancel();
        src?.unsubscribe();
        fallback?.unsubscribe();
      };
    });
}

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

// ---------------------------------------------------------------------------
// Durations given by Observables instead of milliseconds
// ---------------------------------------------------------------------------

/**
 * Like `auditTime`, but each silence window lasts until
 * `durationSelector(value)` emits. Then the most recent value is emitted.
 * A duration that completes without emitting ends the window silently.
 */
export const audit =
  <A>(durationSelector: (value: A) => ObservableInput<unknown>): Operator<A, A> =>
  (source) =>
    observable<A>((sink) => {
      let has = false;
      let last: A | undefined;
      let duration: Inner | null = null;
      let sourceDone = false;
      const endDuration = (_: unknown, self: Inner): void => {
        self.close();
        duration = null;
        if (has) {
          has = false;
          const v = last as A;
          last = undefined;
          sink.next(v);
        }
        if (sourceDone) sink.complete();
      };
      const cleanupDuration = (): void => {
        duration = null;
        if (sourceDone) sink.complete();
      };
      return subscribeChild(source, sink, {
        next: (a) => {
          has = true;
          last = a;
          if (duration === null) {
            const d = subscribeInner(durationSelector(a), sink, { next: endDuration, complete: cleanupDuration });
            if (!d.closed) duration = d;
          }
        },
        complete: () => {
          sourceDone = true;
          if (!has || duration === null) sink.complete();
        },
      });
    });

/**
 * Like `debounceTime`, but the silence each value needs is given by
 * `durationSelector(value)`: the value is emitted when its duration emits,
 * unless a newer value arrived first. A pending value is flushed on completion.
 */
export const debounce =
  <A>(durationSelector: (value: A) => ObservableInput<unknown>): Operator<A, A> =>
  (source) =>
    observable<A>((sink) => {
      let has = false;
      let last: A | undefined;
      let duration: Inner | null = null;
      const emit = (): void => {
        duration?.close();
        duration = null;
        if (has) {
          has = false;
          const v = last as A;
          last = undefined;
          sink.next(v);
        }
      };
      return subscribeChild(source, sink, {
        next: (a) => {
          duration?.close();
          has = true;
          last = a;
          duration = subscribeInner(durationSelector(a), sink, {
            next: (_, self) => {
              self.close();
              emit();
            },
            complete: () => {},
          });
        },
        complete: () => {
          emit();
          sink.complete();
        },
      });
    });

/**
 * Like `throttleTime`, but each throttle window lasts until
 * `durationSelector(value)` emits. Leading by default; with `trailing`, the
 * last value seen during a window is emitted when it ends.
 */
export const throttle =
  <A>(durationSelector: (value: A) => ObservableInput<unknown>, config: ThrottleConfig = {}): Operator<A, A> =>
  (source) =>
    observable<A>((sink) => {
      const leading = config.leading ?? true;
      const trailing = config.trailing ?? false;
      let has = false;
      let sendValue: A | undefined;
      let throttled: Inner | null = null;
      let sourceDone = false;

      const isThrottled = (): boolean => throttled !== null && !throttled.closed;
      const endThrottling = (_: unknown, self: Inner): void => {
        self.close();
        throttled = null;
        if (trailing) {
          send();
          if (sourceDone) sink.complete();
        }
      };
      const cleanupThrottling = (): void => {
        throttled = null;
        if (sourceDone) sink.complete();
      };
      const startThrottle = (a: A): void => {
        const t = subscribeInner(durationSelector(a), sink, { next: endThrottling, complete: cleanupThrottling });
        // a duration that already ended synchronously must not count as active
        if (!t.closed) throttled = t;
      };
      const send = (): void => {
        if (!has) return;
        has = false;
        const v = sendValue as A;
        sendValue = undefined;
        sink.next(v);
        if (!sourceDone) startThrottle(v);
      };
      return subscribeChild(source, sink, {
        next: (a) => {
          has = true;
          sendValue = a;
          if (!isThrottled()) {
            if (leading) send();
            else startThrottle(a);
          }
        },
        complete: () => {
          sourceDone = true;
          if (!(trailing && has && isThrottled())) sink.complete();
        },
      });
    });

/** Emits the most recent source value whenever `notifier` emits (if a new one arrived since). */
export const sample =
  <A>(notifier: ObservableInput<unknown>): Operator<A, A> =>
  (source) =>
    observable<A>((sink) => {
      let has = false;
      let last: A | undefined;
      const src = subscribeChild(source, sink, {
        next: (a) => {
          has = true;
          last = a;
        },
      });
      const n = subscribeInner(notifier, sink, {
        next: () => {
          if (!has) return;
          has = false;
          const v = last as A;
          last = undefined;
          sink.next(v);
        },
      });
      return () => {
        src.unsubscribe();
        n.close();
      };
    });

/**
 * Delays each value until `delayDurationSelector(value, index)` emits its
 * first value. A duration that completes without emitting drops the value.
 * Built from `mergeMap`, `take` and `as`, like RxJS 7.
 */
export const delayWhen = <A>(delayDurationSelector: (value: A, index: number) => ObservableInput<unknown>): Operator<A, A> =>
  mergeMap((a: A, i: number) => from(delayDurationSelector(a, i)).pipe(take(1), as(a)));

/** Pairs every value with the scheduler time at which it arrived. */
export const timestamp = <A>(scheduler: Scheduler = asyncScheduler): Operator<A, { readonly value: A; readonly timestamp: number }> =>
  map((value: A) => ({ value, timestamp: scheduler.now() }));

/** Pairs every value with the time (ms) since the previous value, or since subscription for the first. */
export const timeInterval =
  <A>(scheduler: Scheduler = asyncScheduler): Operator<A, { readonly value: A; readonly interval: number }> =>
  (source) =>
    observable((sink) => {
      let last = scheduler.now();
      return subscribeChild(source, sink, {
        next: (value) => {
          const now = scheduler.now();
          const interval = now - last;
          last = now;
          sink.next({ value, interval });
        },
      });
    });
