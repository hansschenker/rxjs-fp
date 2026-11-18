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
 * Every buffer operator but one is its window counterpart with each window
 * collected into an array: buffer…(…) = window…(…), then mergeMap(toArray()).
 * The exception is `bufferWhen` (kernel): RxJS 7 opens a new window when the
 * closing notifier completes, but ignores that completion for a buffer.
 *
 * Built only on the kernel (checked by test/layering.test.ts), and each
 * definition confirmed against RxJS 7 by test/differential.test.ts.
 */
import {
  asyncScheduler,
  audit,
  debounce,
  defer,
  from,
  Leading,
  LeadingAndTrailing,
  map,
  sample,
  scan,
  take,
  throttleWith,
  timer,
  Trailing,
  window,
  windowTime,
  windowTimeEvery,
  windowTimeEveryOrCount,
  windowTimeOrCount,
  windowToggle,
} from '../kernel.js';
import type { Observable, ObservableInput, Operator, Scheduler, ThrottleStrategy } from '../kernel.js';
import { mergeMap } from './concurrency.js';
import { toArray } from './values.js';

// The family's kernel members, so the family is importable from one place
export { debounce, throttleWith, audit, sample, window, timeout, timeoutFirst, timeoutFirstAndEach, observeOn, subscribeOn } from '../kernel.js';
export { windowTime, windowTimeEvery, windowTimeOrCount, windowTimeEveryOrCount, windowToggle, windowWhen, bufferWhen } from '../kernel.js';
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

// --- throttle with a duration selector ---------------------------------------

/** Emits the first value of each window at once; the window lasts until `durationSelector(value)` emits. */
export const throttle = throttleWith(Leading);
/** Emits the last value of each window when `durationSelector(value)` emits. */
export const throttleTrailing = throttleWith(Trailing);
/** Emits the first value of each window at once, and the last one when the window ends. */
export const throttleLeadingAndTrailing = throttleWith(LeadingAndTrailing);

// --- time stamps ---------------------------------------------------------------

/** Pairs every value with the scheduler time at which it arrived. */
export const timestamp = <A>(scheduler: Scheduler = asyncScheduler): Operator<A, { readonly value: A; readonly timestamp: number }> =>
  map((value: A) => ({ value, timestamp: scheduler.now() }));

type Interval<A> = { readonly value: A; readonly interval: number };

/** Pairs every value with the time (ms) since the previous value, or since subscription for the first. */
export const timeInterval =
  <A>(scheduler: Scheduler = asyncScheduler): Operator<A, Interval<A>> =>
  (source) =>
    defer(() =>
      source.pipe(
        timestamp<A>(scheduler),
        scan<{ readonly value: A; readonly timestamp: number }, { readonly last: number; readonly out: Interval<A> | null }>(
          (acc, t) => ({ last: t.timestamp, out: { value: t.value, interval: t.timestamp - acc.last } }),
          { last: scheduler.now(), out: null },
        ),
        map((s) => s.out as Interval<A>),
      ),
    );

/**
 * Delays each value until `delayDurationSelector(value, index)` emits its
 * first value. A duration that completes without emitting drops the value.
 */
export const delayWhen = <A>(delayDurationSelector: (value: A, index: number) => ObservableInput<unknown>): Operator<A, A> =>
  mergeMap((a: A, i: number) => from(delayDurationSelector(a, i)).pipe(take(1), map(() => a)));

// --- buffers: windows collected into arrays -------------------------------------

const collected =
  <A>(windows: Operator<A, Observable<A>>): Operator<A, A[]> =>
  (source) =>
    source.pipe(windows, mergeMap((w: Observable<A>) => w.pipe(toArray())));

/** Collects values into an array, emitted whenever `closingNotifier` emits (and on completion). */
export const buffer = <A>(closingNotifier: ObservableInput<unknown>): Operator<A, A[]> => collected(window<A>(closingNotifier));

/** Collects values into arrays of `timeSpan` ms. */
export const bufferTime = <A>(timeSpan: number, scheduler: Scheduler = asyncScheduler): Operator<A, A[]> =>
  collected(windowTime<A>(timeSpan, scheduler));

/** Starts a new `timeSpan`-ms buffer every `interval` ms, so buffers overlap or leave gaps. */
export const bufferTimeEvery = <A>(timeSpan: number, interval: number, scheduler: Scheduler = asyncScheduler): Operator<A, A[]> =>
  collected(windowTimeEvery<A>(timeSpan, interval, scheduler));

/** Like `bufferTime`, but emits a buffer early once it holds `maxSize` values. */
export const bufferTimeOrCount = <A>(timeSpan: number, maxSize: number, scheduler: Scheduler = asyncScheduler): Operator<A, A[]> =>
  collected(windowTimeOrCount<A>(timeSpan, maxSize, scheduler));

/** `bufferTimeEvery` and `bufferTimeOrCount` together. */
export const bufferTimeEveryOrCount = <A>(
  timeSpan: number,
  interval: number,
  maxSize: number,
  scheduler: Scheduler = asyncScheduler,
): Operator<A, A[]> => collected(windowTimeEveryOrCount<A>(timeSpan, interval, maxSize, scheduler));

/** Opens a buffer for each value of `openings`, closed when `closingSelector(openValue)` emits. */
export const bufferToggle = <A, O>(openings: ObservableInput<O>, closingSelector: (openValue: O) => ObservableInput<unknown>): Operator<A, A[]> =>
  collected(windowToggle<A, O>(openings, closingSelector));
