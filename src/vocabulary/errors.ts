/**
 * Error vocabulary: `import { retry, timesWithDelay } from 'rxjs-fp/errors'`.
 *
 * The kernel interprets RepeatPolicy and RetryPolicy directly. The optional
 * `retryWith` and `retryConsecutiveWith` vocabulary adapts a pure decision
 * function `(error, attempt) => RetryDecision` to that same retry engine.
 * Decisions are data: `retryNow`, `retryAfter(ms)`, `giveUp`.
 *
 * Why the resubscribing operators stay in the kernel: written as recursion
 * (`catchError` that returns the retried source), each retry adds one
 * subscription layer that lives until the stream ends. The values and timing
 * would match RxJS, but a reconnect loop running for days would keep growing.
 *
 * Built only on the kernel (checked by test/layering.test.ts), and each
 * definition confirmed against RxJS 7 by test/differential.test.ts.
 */
import { catchError, giveUp, isTimeoutError, retryAfter, retryNow, throwError } from '../kernel.js';
import type { ObservableInput, Operator, RetryDecisionPolicy, TimeoutInfo } from '../kernel.js';

// The family's kernel members, so the family is importable from one place
export {
  catchError, finalize, tap, retry, retryWith, retryConsecutiveWith, repeat,
  retryNow, retryAfter, giveUp, totalRuns, unlimitedRuns, additionalRetries,
  unlimitedRetries, repeatOnSignal, retryOnSignal, keepCount, resetOnNext,
  defaultRepeatPolicy, defaultRetryPolicy,
} from '../kernel.js';
export type {
  RetryDecision, RetryDecisionPolicy, RepeatLimit, RepeatSignal, RepeatDelay,
  RepeatPolicy, RetryLimit, RetrySignal, RetryDelay, RetryCounter, RetryPolicy,
} from '../kernel.js';

// --- policies ---------------------------------------------------------------

/** Retry at once, up to `count` times. */
export const times =
  (count: number): RetryDecisionPolicy =>
  (_error, attempt) =>
    attempt <= count ? retryNow : giveUp;

/** Retry after `ms`, up to `count` times. */
export const timesWithDelay =
  (count: number, ms: number): RetryDecisionPolicy =>
  (_error, attempt) =>
    attempt <= count ? retryAfter(ms) : giveUp;

/** Retry up to `count` times, waiting `initialMs`, then twice as long each time. */
export const exponentialBackoff =
  (count: number, initialMs: number): RetryDecisionPolicy =>
  (_error, attempt) =>
    attempt <= count ? retryAfter(initialMs * 2 ** (attempt - 1)) : giveUp;

// --- operators --------------------------------------------------------------

/** Handles only a `TimeoutError`, with what `timeout` knew: values seen and the last one. Other errors pass. */
export const catchTimeout = <A, B>(handler: (info: TimeoutInfo<A>) => ObservableInput<B>): Operator<A, A | B> =>
  catchError<A, B>((err) => (isTimeoutError(err) ? handler(err.info as TimeoutInfo<A>) : throwError(() => err)));

/** Replaces an error with one last value, computed from the error, then completes. */
export const onErrorReturn = <A, B>(fallback: (err: unknown) => B): Operator<A, A | B> =>
  catchError<A, B>((err) => [fallback(err)]);
