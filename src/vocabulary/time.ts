/**
 * Time vocabulary: `import { debounceTime } from 'rxjs-fp/time'`.
 *
 * Each `…Time` operator is a kernel operator whose criterion is a timer:
 *
 *   delay(ms)         every value waits in parallel       mergeMap over timer(ms)
 *   debounceTime(ms)  each value cancels the previous wait debounce(() => timer(ms))
 *   throttleTime(ms)  values are ignored while a wait runs throttle(() => timer(ms))
 *   auditTime(ms)     the latest value when the wait ends  audit(() => timer(ms))
 *   sampleTime(ms)    the latest value on every tick       sample(interval(ms)), interval = timer(ms, ms)
 *
 * Built only on the kernel (checked by test/layering.test.ts), and each
 * definition confirmed against RxJS 7 by test/differential.test.ts.
 */
import { asyncScheduler, audit, debounce, Leading, LeadingAndTrailing, map, sample, throttleWith, timer, Trailing } from '../kernel.js';
import type { Observable, Operator, Scheduler, ThrottleStrategy } from '../kernel.js';
import { mergeMap } from './concurrency.js';

// The family's kernel members, so the family is importable from one place
export { debounce, throttleWith, audit, sample, buffer, window, timeout, observeOn, subscribeOn } from '../kernel.js';
export { Leading, Trailing, LeadingAndTrailing, timer } from '../kernel.js';
export type { ThrottleStrategy } from '../kernel.js';

/** A projection: emits `value` once, `ms` later. The building block of `delay`. */
export const emitAfter =
  (ms: number, scheduler: Scheduler = asyncScheduler) =>
  <A>(value: A): Observable<A> =>
    timer(ms, undefined, scheduler).pipe(map(() => value));

/**
 * Shifts every value forward in time by `ms`. Completes once the source has
 * completed and the last delayed value is out. Errors are immediate.
 */
export const delay = <A>(ms: number, scheduler: Scheduler = asyncScheduler): Operator<A, A> =>
  mergeMap<A, A>(emitAfter(ms, scheduler));

/** Emits a value only after `ms` of silence. A pending value is flushed on completion. */
export const debounceTime = <A>(ms: number, scheduler: Scheduler = asyncScheduler): Operator<A, A> =>
  debounce<A>(() => timer(ms, undefined, scheduler));

/**
 * Lets values through in windows of `ms`, as the `strategy` says. With a
 * trailing strategy, a pending trailing value delays completion until its
 * window ends. Usually used through the three named variants below.
 */
export const throttleTimeWith =
  (strategy: ThrottleStrategy) =>
  <A>(ms: number, scheduler: Scheduler = asyncScheduler): Operator<A, A> =>
    throttleWith(strategy)<A>(() => timer(ms, undefined, scheduler));

/** Emits the first value of each `ms` window at once. */
export const throttleTime = throttleTimeWith(Leading);
/** Emits the last value of each `ms` window when the window ends. */
export const throttleTimeTrailing = throttleTimeWith(Trailing);
/** Emits the first value of each `ms` window at once, and the last one when the window ends. */
export const throttleTimeLeadingAndTrailing = throttleTimeWith(LeadingAndTrailing);

/**
 * After a value arrives, waits `ms` and then emits the most recent value.
 * If the source completes during a window, the pending value is still
 * emitted at the end of the window, then the result completes.
 */
export const auditTime = <A>(ms: number, scheduler: Scheduler = asyncScheduler): Operator<A, A> =>
  audit<A>(() => timer(ms, undefined, scheduler));

/** Emits the most recent value (if a new one arrived) every `ms`. */
export const sampleTime = <A>(ms: number, scheduler: Scheduler = asyncScheduler): Operator<A, A> =>
  sample<A>(timer(ms, ms, scheduler)); // = sample(interval(ms)), as RxJS defines it
