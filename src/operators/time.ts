import { observable, subscribeChild } from '../core/observable.js';
import { timeoutError, type TimeoutInfo } from '../core/errors.js';
import { createSubscription } from '../core/subscription.js';
import type { Operator, Sink, Teardown } from '../core/types.js';
import { asyncScheduler, type Scheduler } from '../scheduler/scheduler.js';
import type { ObservableInput } from '../creation/creation.js';
import { subscribeInner, type Inner } from './inner.js';
import {
  after,
  assertNever,
  epochMilliseconds,
  positiveMilliseconds,
  type AfterDelay,
  type EpochMilliseconds,
  type PositiveMilliseconds,
} from '../policies/core.js';

/**
 * Time-based operators. Every one takes the Scheduler as an explicit last
 * argument (default: `asyncScheduler`). Tests pass a virtual scheduler;
 * nothing is patched globally.
 */

const noop: Teardown = () => {};

/**
 * The four complete leading/trailing emission choices. SuppressValues still
 * evaluates duration selectors and connects to their signals.
 */
export type ThrottleMode = 'leading-only' | 'trailing-only' | 'leading-and-trailing' | 'suppress-values';

/** Vocabulary aliases for the same canonical mode; no separate interpreter. */
export type ThrottleStrategy = ThrottleMode;
export const Leading: ThrottleMode = 'leading-only';
export const Trailing: ThrottleMode = 'trailing-only';
export const LeadingAndTrailing: ThrottleMode = 'leading-and-trailing';

const throttleEdges = (mode: ThrottleMode): { readonly leading: boolean; readonly trailing: boolean } => {
  switch (mode) {
    case 'leading-only':
      return { leading: true, trailing: false };
    case 'trailing-only':
      return { leading: false, trailing: true };
    case 'leading-and-trailing':
      return { leading: true, trailing: true };
    case 'suppress-values':
      return { leading: false, trailing: false };
    default:
      return assertNever(mode);
  }
};

/** A relative duration, or an immutable snapshot of an absolute deadline. */
export type Deadline = AfterDelay | Readonly<{ kind: 'at'; epochMillis: EpochMilliseconds }>;

export type TimeoutTiming =
  | Readonly<{ kind: 'first-only'; deadline: Deadline }>
  | Readonly<{ kind: 'each-gap'; duration: PositiveMilliseconds }>
  | Readonly<{ kind: 'first-then-each'; deadline: Deadline; duration: PositiveMilliseconds }>;

export type RaiseTimeoutError = Readonly<{ kind: 'raise-timeout-error' }>;
export type SwitchTo<A, B, M> = Readonly<{
  kind: 'switch-to';
  fallback: (info: TimeoutInfo<A, M>) => ObservableInput<B>;
}>;
export type TimeoutOutcome<A, B, M> = RaiseTimeoutError | SwitchTo<A, B, M>;

/** Scheduler is execution context: it remains the operator's last argument. */
export type TimeoutPolicy<A, B, M = unknown> = Readonly<{
  timing: TimeoutTiming;
  outcome: TimeoutOutcome<A, B, M>;
  meta: M;
}>;
export type TimeoutErrorPolicy<A = unknown, M = null> = TimeoutPolicy<A, never, M> & Readonly<{
  outcome: RaiseTimeoutError;
}>;
export type TimeoutSwitchPolicy<A, B, M = unknown> = TimeoutPolicy<A, B, M> & Readonly<{
  outcome: SwitchTo<A, B, M>;
}>;

export const deadlineAfter = after;
export const deadlineAt = (date: Date): Deadline => Object.freeze({
  kind: 'at',
  epochMillis: epochMilliseconds(date.getTime()),
});

/** Timing is mandatory; there is no empty default timeout. */
export const defaultTimeoutErrorPolicy = <A = unknown>(timing: TimeoutTiming): TimeoutErrorPolicy<A, null> => Object.freeze({
  timing,
  outcome: Object.freeze({ kind: 'raise-timeout-error' }),
  meta: null,
});

type TimeoutWaits = Readonly<{ first: Deadline | undefined; each: PositiveMilliseconds | undefined }>;
const snapshotDeadline = (deadline: Deadline): Deadline => {
  switch (deadline.kind) {
    case 'after':
      return Object.freeze({ kind: 'after', duration: deadline.duration });
    case 'at':
      return Object.freeze({ kind: 'at', epochMillis: deadline.epochMillis });
    default:
      return assertNever(deadline);
  }
};
const timeoutWaits = (timing: TimeoutTiming): TimeoutWaits => {
  switch (timing.kind) {
    case 'first-only':
      return { first: snapshotDeadline(timing.deadline), each: undefined };
    case 'each-gap':
      return { first: undefined, each: timing.duration };
    case 'first-then-each':
      return { first: snapshotDeadline(timing.deadline), each: timing.duration };
    default:
      return assertNever(timing);
  }
};

const deadlineDelay = (deadline: Deadline, scheduler: Scheduler): number => {
  switch (deadline.kind) {
    case 'after':
      return deadline.duration;
    case 'at':
      return deadline.epochMillis - scheduler.now();
    default:
      return assertNever(deadline);
  }
};

/**
 * Interpret timing and outcome directly. A timeout closes the source before
 * reporting an error or calling the fallback factory. A fallback connection
 * is not subject to the source's timeout again.
 */
export function timeout<A, M = null>(policy: TimeoutErrorPolicy<A, M>, scheduler?: Scheduler): Operator<A, A>;
export function timeout<A, B, M = unknown>(policy: TimeoutSwitchPolicy<A, B, M>, scheduler?: Scheduler): Operator<A, A | B>;
export function timeout<A, B, M = unknown>(policy: TimeoutPolicy<A, B, M>, scheduler?: Scheduler): Operator<A, A | B>;
export function timeout<A, B, M>(
  policy: TimeoutPolicy<A, B, M>,
  scheduler: Scheduler = asyncScheduler,
): Operator<A, A | B> {
  const { first, each } = timeoutWaits(policy.timing);
  const meta = policy.meta;
  const outcome = policy.outcome;
  const fallback = outcome.kind === 'switch-to' ? outcome.fallback : undefined;
  switch (outcome.kind) {
    case 'raise-timeout-error':
    case 'switch-to':
      break;
    default:
      return assertNever(outcome);
  }

  return (source) =>
    observable<A | B>((sink) => {
      let seen = 0;
      let lastValue: A | null = null;
      let phase: 'source' | 'fallback' | 'done' = 'source';
      let cancel: Teardown = noop;
      let timerGeneration = 0;
      let sourceReady = false;
      let pendingTimeout: TimeoutInfo<A, M> | undefined;

      // Link a scope before running the source. Timeout can close even a
      // synchronous source whose subscribeChild call has not returned yet.
      const sourceScope = createSubscription();
      sourceScope.add(sink.add(sourceScope.unsubscribe));
      const sourceSink: Sink<A | B> = {
        next: sink.next,
        error: sink.error,
        complete: sink.complete,
        get closed() {
          return sink.closed || sourceScope.closed;
        },
        add: sourceScope.add,
      };

      const clearTimer = (): void => {
        timerGeneration++;
        const pending = cancel;
        cancel = noop;
        pending();
      };

      // Register resource cleanup before either scheduler or source can run.
      sink.add(() => {
        phase = 'done';
        clearTimer();
        sourceScope.unsubscribe();
        lastValue = null;
        pendingTimeout = undefined;
      });

      const dispatchTimeout = (info: TimeoutInfo<A, M>): void => {
        if (sink.closed) return;
        try {
          if (fallback) {
            subscribeInner(fallback(info), sink, {
              next: (value) => sink.next(value),
              complete: () => sink.complete(),
            });
          } else {
            sink.error(timeoutError(info));
          }
        } catch (error) {
          sink.error(error);
        }
      };
      const onTimeout = (): void => {
        if (phase !== 'source' || sink.closed) return;
        phase = 'fallback';
        clearTimer();
        // Preserve the last value before teardown can run user finalizers.
        const info: TimeoutInfo<A, M> = { seen, lastValue, meta };
        try {
          sourceScope.unsubscribe();
          if (sink.closed) return;
          if (sourceReady) dispatchTimeout(info);
          else pendingTimeout = info;
        } catch (error) {
          sink.error(error);
        }
      };
      const arm = (ms: number): void => {
        clearTimer();
        const generation = timerGeneration;
        const scheduled = scheduler.schedule(() => {
          if (generation === timerGeneration) onTimeout();
        }, ms);
        // Schedulers may execute immediately. Never retain a stale handle
        // after that work terminated or switched the output.
        if (generation === timerGeneration && phase === 'source' && !sink.closed) cancel = scheduled;
        else scheduled();
      };

      subscribeChild(source, sourceSink, {
        next: (a) => {
          if (phase !== 'source' || sink.closed) return;
          clearTimer();
          seen++;
          lastValue = a;
          sink.next(a);
          if (phase === 'source' && !sink.closed && each !== undefined) arm(each);
        },
        error: (error) => {
          if (phase !== 'source') return;
          phase = 'done';
          clearTimer();
          sink.error(error);
        },
        complete: () => {
          if (phase !== 'source') return;
          phase = 'done';
          clearTimer();
          sink.complete();
        },
      });
      sourceReady = true;
      if (pendingTimeout !== undefined) {
        const info = pendingTimeout;
        pendingTimeout = undefined;
        dispatchTimeout(info);
      }
      // Armed after subscribing, like RxJS: source events due at the same
      // virtual time as the deadline were scheduled first and win the tie.
      if (seen === 0 && phase === 'source' && !sink.closed) {
        arm(first === undefined ? each as number : deadlineDelay(first, scheduler));
      }
    });
}

/**
 * Errors with `TimeoutError` if the first value does not arrive within
 * `first`: ms after subscription, or by a `Date`. Later values are not timed.
 */
export const timeoutFirst = <A>(first: number | Date, scheduler: Scheduler = asyncScheduler): Operator<A, A> =>
  timeout(defaultTimeoutErrorPolicy<A>({
    kind: 'first-only',
    deadline: typeof first === 'number' ? deadlineAfter(first) : deadlineAt(first),
  }), scheduler);

/**
 * Errors with `TimeoutError` if the first value does not arrive within
 * `first`, or if a later gap between two values takes longer than `each`.
 */
export const timeoutFirstAndEach = <A>(
  first: number | Date,
  each: number,
  scheduler: Scheduler = asyncScheduler,
): Operator<A, A> => timeout(defaultTimeoutErrorPolicy<A>({
  kind: 'first-then-each',
  deadline: typeof first === 'number' ? deadlineAfter(first) : deadlineAt(first),
  duration: positiveMilliseconds(each),
}), scheduler);

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
 * Interpret the emission mode directly. Each window lasts until the duration
 * signal emits or completes; empty completion closes it without a trailing
 * emission. An active trailing value can delay source completion.
 */
export const throttle = <A>(
  durationSelector: (value: A) => ObservableInput<unknown>,
  mode: ThrottleMode = 'leading-only',
): Operator<A, A> => {
  const { leading, trailing } = throttleEdges(mode);
  return (source) =>
    observable<A>((sink) => {
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
        const input = durationSelector(a);
        const durationScope = createSubscription();
        durationScope.add(sink.add(durationScope.unsubscribe));
        const durationSink: Sink<A> = {
          next: sink.next,
          error: sink.error,
          complete: sink.complete,
          get closed() {
            return sink.closed || durationScope.closed;
          },
          add: durationScope.add,
        };
        const t = subscribeInner(input, durationSink, {
          next: (value, self) => {
            // Make the duration logically closed before running its teardown:
            // that teardown may synchronously send another source value.
            self.close();
            // Close the actual duration source even before subscribeInner
            // returns; a synchronous producer can observe closed immediately.
            durationScope.unsubscribe();
            endThrottling(value, self);
          },
          complete: () => {
            durationScope.unsubscribe();
            cleanupThrottling();
          },
          error: (error) => {
            durationScope.unsubscribe();
            sink.error(error);
          },
        });
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
};

/** Partial application of the canonical operator, retained as vocabulary. */
export const throttleWith =
  (mode: ThrottleMode) =>
  <A>(durationSelector: (value: A) => ObservableInput<unknown>): Operator<A, A> =>
    throttle(durationSelector, mode);

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
