/**
 * Error vocabulary: `import { retry, timesWithDelay } from 'rxjs-fp/errors'`.
 *
 * The kernel handles errors with `catchError` and resubscribes with
 * `retryWith(policy)`, `retryConsecutiveWith(policy)` and `repeat`. A policy
 * is a plain function `(error, attempt) => RetryDecision`, and the decisions
 * are data: `retryNow`, `retryAfter(ms)`, `giveUp`. This module adds the
 * usual policies and the operators defined with them.
 *
 * Why the resubscribing operators stay in the kernel: written as recursion
 * (`catchError` that returns the retried source), each retry adds one
 * subscription layer that lives until the stream ends. The values and timing
 * would match RxJS, but a reconnect loop running for days would keep growing.
 *
 * Built only on the kernel (checked by test/layering.test.ts), and each
 * definition confirmed against RxJS 7 by test/differential.test.ts.
 */
import { catchError, giveUp, isTimeoutError, retryAfter, retryNow, retryWith, throwError } from '../kernel.js';
import type { ObservableInput, Operator, RetryPolicy, TimeoutInfo } from '../kernel.js';

// The family's kernel members, so the family is importable from one place
export { catchError, finalize, tap, retryWith, retryConsecutiveWith, repeat, retryNow, retryAfter, giveUp } from '../kernel.js';
export type { RetryDecision, RetryPolicy } from '../kernel.js';

// --- policies ---------------------------------------------------------------

/** Retry at once, up to `count` times. */
export const times =
  (count: number): RetryPolicy =>
  (_error, attempt) =>
    attempt <= count ? retryNow : giveUp;

/** Retry after `ms`, up to `count` times. */
export const timesWithDelay =
  (count: number, ms: number): RetryPolicy =>
  (_error, attempt) =>
    attempt <= count ? retryAfter(ms) : giveUp;

/** Retry up to `count` times, waiting `initialMs`, then twice as long each time. */
export const exponentialBackoff =
  (count: number, initialMs: number): RetryPolicy =>
  (_error, attempt) =>
    attempt <= count ? retryAfter(initialMs * 2 ** (attempt - 1)) : giveUp;

// --- operators --------------------------------------------------------------

/** Resubscribes on error, up to `count` times (forever by default). */
export const retry = <A>(count: number = Number.POSITIVE_INFINITY): Operator<A, A> => retryWith<A>(times(count));

/** Handles only a `TimeoutError`, with what `timeout` knew: values seen and the last one. Other errors pass. */
export const catchTimeout = <A, B>(handler: (info: TimeoutInfo<A>) => ObservableInput<B>): Operator<A, A | B> =>
  catchError<A, B>((err) => (isTimeoutError(err) ? handler(err.info as TimeoutInfo<A>) : throwError(() => err)));

/** Replaces an error with one last value, computed from the error, then completes. */
export const onErrorReturn = <A, B>(fallback: (err: unknown) => B): Operator<A, A | B> =>
  catchError<A, B>((err) => [fallback(err)]);
