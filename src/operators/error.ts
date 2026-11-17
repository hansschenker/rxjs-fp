import { observable, subscribeChild } from '../core/observable.js';
import type { Observable, Operator, Subscription, Teardown } from '../core/types.js';
import { from, throwError, timer, type ObservableInput } from '../creation/creation.js';
import { isTimeoutError, type TimeoutInfo } from '../core/errors.js';
import { asyncScheduler, type Scheduler } from '../scheduler/scheduler.js';

/**
 * On error, switches to the Observable returned by `handler`. Like RxJS,
 * `caught` is the resulting Observable (source + catchError), so returning
 * it restarts the source with the same error handling in place.
 */
export const catchError =
  <A, B>(handler: (err: unknown, caught: Observable<A | B>) => ObservableInput<B>): Operator<A, A | B> =>
  (source) => {
    const self: Observable<A | B> = observable<A | B>((sink) => {
      let current: Subscription | null = null;
      current = subscribeChild(source, sink, {
        error: (err) => {
          current?.unsubscribe();
          current = subscribeChild(from(handler(err, self)), sink);
        },
      });
      return () => current?.unsubscribe();
    });
    return self;
  };

/**
 * Recovers from a timeout only. On a `TimeoutError`, switches to
 * `handler(info)`, where `info` holds the values seen before the timeout;
 * any other error is passed on unchanged.
 *
 *   source.pipe(timeout(3000), catchTimeout(() => of(fallback)))
 */
export const catchTimeout = <A, B>(handler: (info: TimeoutInfo<A>) => ObservableInput<B>): Operator<A, A | B> =>
  catchError<A, B>((err) => (isTimeoutError(err) ? handler(err.info as TimeoutInfo<A>) : throwError(() => err)));

/**
 * What a retry policy decides after an error. Decisions are data, like
 * flattening and throttle strategies:
 *
 *   retryNow       resubscribe at once, synchronously
 *   retryAfter(ms) resubscribe after `ms`, on the scheduler
 *   giveUp         pass the error on
 */
export type RetryDecision =
  | { readonly _tag: 'RetryNow' }
  | { readonly _tag: 'RetryAfter'; readonly ms: number }
  | { readonly _tag: 'GiveUp' };

export const retryNow: RetryDecision = Object.freeze({ _tag: 'RetryNow' });
export const retryAfter = (ms: number): RetryDecision => Object.freeze({ _tag: 'RetryAfter', ms });
export const giveUp: RetryDecision = Object.freeze({ _tag: 'GiveUp' });

/**
 * A pure function from an error and the number of the retry being decided
 * (1 for the first) to a decision. Testable without any Observable:
 * `policy(new Error('x'), 1)`.
 */
export type RetryPolicy = (error: unknown, attempt: number) => RetryDecision;

/** Retries at once, up to `count` times. */
export const times =
  (count: number): RetryPolicy =>
  (_error, attempt) =>
    attempt <= count ? retryNow : giveUp;

/** Retries up to `count` times, waiting `ms` before each retry. */
export const timesWithDelay =
  (count: number, ms: number): RetryPolicy =>
  (_error, attempt) =>
    attempt <= count ? retryAfter(ms) : giveUp;

/** Retries up to `count` times, waiting `initialMs`, then twice as long before each further retry. */
export const exponentialBackoff =
  (count: number, initialMs: number): RetryPolicy =>
  (_error, attempt) =>
    attempt <= count ? retryAfter(initialMs * 2 ** (attempt - 1)) : giveUp;

/**
 * Shared by `retryWith` and `retryConsecutiveWith`. On an error, asks the
 * policy and resubscribes to the source, or passes the error on. With
 * `resetOnValue`, every value resets the attempt counter, so the policy
 * sees the number of consecutive failures.
 */
const retryCore =
  <A>(policy: RetryPolicy, resetOnValue: boolean, scheduler: Scheduler): Operator<A, A> =>
  (source) =>
    observable<A>((sink) => {
      let attempt = 0;
      let current: Subscription | null = null;
      let cancelDelay: Teardown | null = null;
      const subscribe = (): void => {
        current = subscribeChild(source, sink, {
          next: (a) => {
            if (resetOnValue) attempt = 0;
            sink.next(a);
          },
          error: (err) => {
            attempt++;
            const decision = policy(err, attempt);
            if (decision._tag === 'GiveUp') {
              sink.error(err);
              return;
            }
            current?.unsubscribe();
            if (decision._tag === 'RetryNow') subscribe();
            else cancelDelay = scheduler.schedule(subscribe, decision.ms);
          },
        });
      };
      subscribe();
      return () => {
        cancelDelay?.();
        current?.unsubscribe();
      };
    });

/**
 * Resubscribes to the source when it errors, as `policy` decides. The
 * attempt counter counts every failure since subscription.
 *
 *   source.pipe(retryWith(timesWithDelay(3, 1000)))
 *   source.pipe(retryWith((e, n) => (isNetworkError(e) && n <= 3 ? retryAfter(1000) : giveUp)))
 */
export const retryWith = <A>(policy: RetryPolicy, scheduler: Scheduler = asyncScheduler): Operator<A, A> =>
  retryCore<A>(policy, false, scheduler);

/**
 * Like `retryWith`, but every value resets the attempt counter, so the
 * policy limits consecutive failures. Suits long-lived connections:
 *
 *   socket$.pipe(retryConsecutiveWith(exponentialBackoff(5, 100)))
 */
export const retryConsecutiveWith = <A>(policy: RetryPolicy, scheduler: Scheduler = asyncScheduler): Operator<A, A> =>
  retryCore<A>(policy, true, scheduler);

/** Resubscribes at once when the source errors, up to `count` times (default: forever). */
export const retry = <A>(count: number = Number.POSITIVE_INFINITY): Operator<A, A> => retryWith<A>(times(count));

/** Resubscribes on completion, `count` times in total (default: forever). */
export const repeat =
  <A>(count: number = Number.POSITIVE_INFINITY, delayMs?: number, scheduler: Scheduler = asyncScheduler): Operator<A, A> =>
  (source) =>
    observable<A>((sink) => {
      let done = 0;
      let current: Subscription | null = null;
      let cancel: Teardown | null = null;
      const subscribe = (): void => {
        current = subscribeChild(source, sink, {
          complete: () => {
            done++;
            if (done >= count) {
              sink.complete();
              return;
            }
            current?.unsubscribe();
            if (delayMs === undefined) subscribe();
            else cancel = timer(delayMs, undefined, scheduler).subscribe({ complete: subscribe }).unsubscribe;
          },
        });
      };
      if (count <= 0) sink.complete();
      else subscribe();
      return () => {
        cancel?.();
        current?.unsubscribe();
      };
    });

/** Runs `f` when the execution ends for any reason (error, complete, unsubscribe). */
export const finalize =
  <A>(f: () => void): Operator<A, A> =>
  (source) =>
    observable<A>((sink) => {
      const sub = subscribeChild(source, sink);
      return () => {
        sub.unsubscribe();
        f();
      };
    });

/** Errors become a fallback value, then complete. */
export const onErrorReturn = <A, B>(fallback: (err: unknown) => B): Operator<A, A | B> =>
  catchError<A, B>((err) => [fallback(err)]);
