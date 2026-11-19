import { observable, subscribeChild } from '../core/observable.js';
import { createSubscription } from '../core/subscription.js';
import type { Observable, Operator, Sink, Subscription } from '../core/types.js';
import { EMPTY, from, throwError, timer, type ObservableInput } from '../creation/creation.js';
import { asyncScheduler, type Scheduler } from '../scheduler/scheduler.js';
import { assertNever, immediate, natural } from '../policies/core.js';
import type { AfterDelay, ImmediateDelay, NonNegativeInt, Signal } from '../policies/core.js';

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
export type RetryDecisionPolicy = (error: unknown, attempt: number) => RetryDecision;

/** A repeat limit includes the initial source connection. */
export type RepeatLimit =
  | { readonly kind: 'unlimited' }
  | { readonly kind: 'total-runs'; readonly count: NonNegativeInt };

/** A retry limit counts additional connections following source errors. */
export type RetryLimit =
  | { readonly kind: 'unlimited' }
  | { readonly kind: 'additional-retries'; readonly count: NonNegativeInt };

/** Receives the completed-run count, starting at one. */
export type RepeatSignal = (completedRuns: number) => Signal;

/** Receives the current retry count, starting at one; reset-on-next resets it. */
export type RetrySignal = (error: unknown, retryNumber: number) => Signal;

export type RepeatDelay =
  | ImmediateDelay
  | AfterDelay
  | { readonly kind: 'on-signal'; readonly signal: RepeatSignal };

export type RetryDelay =
  | ImmediateDelay
  | AfterDelay
  | { readonly kind: 'on-signal'; readonly signal: RetrySignal };

export type RepeatPolicy = {
  readonly limit: RepeatLimit;
  readonly delay: RepeatDelay;
};

/** Resetting happens on every source next notification, not on completion. */
export type RetryCounter =
  | { readonly kind: 'keep-count' }
  | { readonly kind: 'reset-on-next' };

export type RetryPolicy = {
  readonly limit: RetryLimit;
  readonly delay: RetryDelay;
  readonly counter: RetryCounter;
};

export const unlimitedRuns = (): RepeatLimit => Object.freeze({ kind: 'unlimited' });
export const totalRuns = (count: number): RepeatLimit => Object.freeze({ kind: 'total-runs', count: natural(count) });
export const unlimitedRetries = (): RetryLimit => Object.freeze({ kind: 'unlimited' });
export const additionalRetries = (count: number): RetryLimit => Object.freeze({ kind: 'additional-retries', count: natural(count) });
export const repeatOnSignal = (signal: RepeatSignal): RepeatDelay => Object.freeze({ kind: 'on-signal', signal });
export const retryOnSignal = (signal: RetrySignal): RetryDelay => Object.freeze({ kind: 'on-signal', signal });
export const keepCount = (): RetryCounter => Object.freeze({ kind: 'keep-count' });
export const resetOnNext = (): RetryCounter => Object.freeze({ kind: 'reset-on-next' });

export const defaultRepeatPolicy = (): RepeatPolicy => Object.freeze({ limit: unlimitedRuns(), delay: immediate() });
export const defaultRetryPolicy = (): RetryPolicy =>
  Object.freeze({ limit: unlimitedRetries(), delay: immediate(), counter: keepCount() });

const repeatCount = (limit: RepeatLimit): number => {
  switch (limit.kind) {
    case 'unlimited': return Infinity;
    case 'total-runs': return limit.count;
    default: return assertNever(limit);
  }
};

const retryCount = (limit: RetryLimit): number => {
  switch (limit.kind) {
    case 'unlimited': return Infinity;
    case 'additional-retries': return limit.count;
    default: return assertNever(limit);
  }
};

const resetsOnNext = (counter: RetryCounter): boolean => {
  switch (counter.kind) {
    case 'keep-count': return false;
    case 'reset-on-next': return true;
    default: return assertNever(counter);
  }
};

/**
 * One signal next permits one restart. A dedicated scope can close the signal
 * even while its synchronous producer is still running. Empty completion
 * completes the result; a signal error replaces the triggering source error.
 */
const waitForRestart = <A>(signal: Signal, sink: Sink<A>, restart: () => void): void => {
  const scope = createSubscription();
  const unlink = sink.add(scope.unsubscribe);
  scope.add(unlink);
  const signalSink: Sink<A> = {
    next: sink.next,
    error: sink.error,
    complete: sink.complete,
    get closed() { return sink.closed || scope.closed; },
    add: scope.add,
  };
  subscribeChild(from(signal), signalSink, {
    next: () => {
      scope.unsubscribe();
      if (!sink.closed) restart();
    },
  });
};

/**
 * Interpret the retry policy directly. The limit counts additional retries:
 * two permits three total connections when the counter is kept. A source
 * completion ends the result. Every value resets the count in reset-on-next
 * mode, so the lifetime number of retries can exceed the finite limit.
 */
export const retry = <A>(policy: RetryPolicy = defaultRetryPolicy(), scheduler: Scheduler = asyncScheduler): Operator<A, A> => {
  const count = retryCount(policy.limit);
  const delay: RetryDelay = { ...policy.delay };
  const reset = resetsOnNext(policy.counter);
  if (count === 0) return (source) => source;

  return (source) => observable<A>((sink) => {
    let retries = 0;
    let current: Subscription | null = null;

    const restart = (error: unknown): void => {
      current?.unsubscribe();
      current = null;
      if (sink.closed) return;
      switch (delay.kind) {
        case 'immediate':
          connectSource();
          return;
        case 'after':
          waitForRestart(timer(delay.duration, undefined, scheduler), sink, connectSource);
          return;
        case 'on-signal': {
          const signal = delay.signal;
          waitForRestart(signal(error, retries), sink, connectSource);
          return;
        }
        default: return assertNever(delay);
      }
    };

    const connectSource = (): void => {
      if (sink.closed) return;
      let failedSynchronously = false;
      let synchronousError: unknown;
      current = subscribeChild(source, sink, {
        next: (value) => {
          if (reset) retries = 0;
          sink.next(value);
        },
        error: (error) => {
          if (retries >= count) {
            sink.error(error);
            return;
          }
          retries += 1;
          if (current) restart(error);
          else {
            failedSynchronously = true;
            synchronousError = error;
          }
        },
      });
      // Wait until the synchronous producer has returned its teardown before
      // closing the old connection and selecting the next action.
      if (failedSynchronously) restart(synchronousError);
    };

    connectSource();
  });
};

/**
 * Resubscribes to the source when it errors, as `policy` decides. The
 * attempt counter counts every failure since subscription.
 *
 *   source.pipe(retryWith(timesWithDelay(3, 1000)))
 *   source.pipe(retryWith((e, n) => (isNetworkError(e) && n <= 3 ? retryAfter(1000) : giveUp)))
 */
export const retryWith = <A>(policy: RetryDecisionPolicy, scheduler: Scheduler = asyncScheduler): Operator<A, A> =>
  retry<A>({ limit: unlimitedRetries(), delay: retryOnSignal(decisionSignal(policy, scheduler)), counter: keepCount() }, scheduler);

/**
 * Like `retryWith`, but every value resets the attempt counter, so the
 * policy limits consecutive failures. Suits long-lived connections:
 *
 *   socket$.pipe(retryConsecutiveWith(exponentialBackoff(5, 100)))
 */
export const retryConsecutiveWith = <A>(policy: RetryDecisionPolicy, scheduler: Scheduler = asyncScheduler): Operator<A, A> =>
  retry<A>({ limit: unlimitedRetries(), delay: retryOnSignal(decisionSignal(policy, scheduler)), counter: resetOnNext() }, scheduler);

const decisionSignal = (policy: RetryDecisionPolicy, scheduler: Scheduler): RetrySignal => (error, attempt) => {
  const decision = policy(error, attempt);
  switch (decision._tag) {
    case 'RetryNow': return [undefined];
    case 'RetryAfter': return timer(decision.ms, undefined, scheduler);
    case 'GiveUp': return throwError(() => error);
    default: return assertNever(decision);
  }
};

/**
 * Interpret the repeat policy directly. Total runs include the first source
 * connection; zero returns EMPTY without connecting. Only completion repeats:
 * source errors pass through. Immediate and after(0) remain distinct modes.
 */
export const repeat = <A>(policy: RepeatPolicy = defaultRepeatPolicy(), scheduler: Scheduler = asyncScheduler): Operator<A, A> => {
  const count = repeatCount(policy.limit);
  const delay: RepeatDelay = { ...policy.delay };
  if (count === 0) return () => EMPTY;

  return (source) => observable<A>((sink) => {
    let completedRuns = 0;
    let current: Subscription | null = null;

    const restart = (): void => {
      current?.unsubscribe();
      current = null;
      if (sink.closed) return;
      switch (delay.kind) {
        case 'immediate':
          connectSource();
          return;
        case 'after':
          waitForRestart(timer(delay.duration, undefined, scheduler), sink, connectSource);
          return;
        case 'on-signal': {
          const signal = delay.signal;
          waitForRestart(signal(completedRuns), sink, connectSource);
          return;
        }
        default: return assertNever(delay);
      }
    };

    const connectSource = (): void => {
      if (sink.closed) return;
      let completedSynchronously = false;
      current = subscribeChild(source, sink, {
        complete: () => {
          completedRuns += 1;
          if (completedRuns >= count) {
            sink.complete();
            return;
          }
          if (current) restart();
          else completedSynchronously = true;
        },
      });
      if (completedSynchronously) restart();
    };

    connectSource();
  });
};

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
