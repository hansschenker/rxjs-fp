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
 * `observeOn` and `subscribeOn` are compositions as well: `delay` applied to
 * the notifications instead of the values (`materialize`, `delay`,
 * `dematerialize`), and a `timer` flattened into the source (`mergeMap`).
 *
 * Every buffer operator but one is its window counterpart with each window
 * collected into an array: buffer…(…) = window…(…), then mergeMap(toArray()).
 * The exception is `bufferWhen` (kernel): RxJS 7 opens a new window when the
 * closing notifier completes, but ignores that completion for a buffer.
 *
 * The windows themselves are vocabulary too, except the three kernel shapes
 * `window`, `windowToggle` and `windowWhen`: `windowTime` is `windowWhen`
 * closed by a timer, `windowTimeEvery` is `windowToggle` opened by an
 * interval, and `windowCount` is `windowToggle` driven by counts of the
 * source's own values (see `afterDelivery` below).
 *
 * Built only on the kernel (checked by test/layering.test.ts), and each
 * definition confirmed against RxJS 7 by test/differential.test.ts.
 */
import {
  asyncScheduler,
  audit,
  connect,
  debounce,
  defer,
  dematerialize,
  from,
  LeadingAndTrailing,
  map,
  materialize,
  race,
  repeat,
  sample,
  scan,
  take,
  throttle,
  throttleWith,
  timer,
  Trailing,
  window,
  windowToggle,
  windowWhen,
} from '../kernel.js';
import type { Notification, Observable, ObservableInput, Operator, Scheduler, ThrottleMode } from '../kernel.js';
import { concat, merge, mergeMap } from './concurrency.js';
import { of } from './creation.js';
import { skip, toArray } from './values.js';

// The family's kernel members, so the family is importable from one place
export { debounce, throttle, throttleWith, audit, sample, window, timeout, timeoutFirst, timeoutFirstAndEach } from '../kernel.js';
export { windowToggle, windowWhen, bufferWhen } from '../kernel.js';
export { Leading, Trailing, LeadingAndTrailing, timer } from '../kernel.js';
export type { ThrottleMode, ThrottleStrategy } from '../kernel.js';
export { deadlineAfter, deadlineAt, defaultTimeoutErrorPolicy } from '../kernel.js';
export type {
  Deadline, TimeoutTiming, RaiseTimeoutError, SwitchTo, TimeoutOutcome,
  TimeoutPolicy, TimeoutErrorPolicy, TimeoutSwitchPolicy, TimeoutInfo,
} from '../kernel.js';

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

/**
 * Re-delivers every notification through the scheduler, after `delayMs`:
 * `delay` applied to the notifications instead of the values, so completion
 * and errors wait their turn behind the values before them.
 */
export const observeOn =
  <A>(scheduler: Scheduler, delayMs = 0): Operator<A, A> =>
  (source) =>
    source.pipe(materialize<A>(), delay<Notification<A>>(delayMs, scheduler), dematerialize<A>());

/** Subscribes to the source through the scheduler, after `delayMs`: a timer flattened into the source. */
export const subscribeOn =
  <A>(scheduler: Scheduler, delayMs = 0): Operator<A, A> =>
  (source) =>
    timer(delayMs, undefined, scheduler).pipe(mergeMap(() => source));

/** Emits a value only after `ms` of silence. A pending value is flushed on completion. */
export const debounceTime = <A>(ms: number, scheduler: Scheduler = asyncScheduler): Operator<A, A> =>
  debounce<A>(() => timer(ms, undefined, scheduler));

/**
 * A timer supplies the duration; the canonical throttle interprets the mode.
 * The scheduler remains the last argument, separate from emission policy.
 */
export const throttleTime = <A>(
  ms: number,
  mode: ThrottleMode = 'leading-only',
  scheduler: Scheduler = asyncScheduler,
): Operator<A, A> => throttle<A>(() => timer(ms, undefined, scheduler), mode);

/** Partial application of the same canonical interpreter. */
export const throttleTimeWith =
  (mode: ThrottleMode) =>
  <A>(ms: number, scheduler: Scheduler = asyncScheduler): Operator<A, A> =>
    throttleTime<A>(ms, mode, scheduler);

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

// --- windows by count and by time --------------------------------------------------

/**
 * Runs `build` with two multicast views of the source. Subscribers of
 * `values` receive each value first; `ticks` receives it afterwards, because
 * the inner `connect` subscribes its feeder to `values` only after `build`'s
 * result has. An operator subscribed to `values` can therefore take its
 * notifiers from `ticks` and count only values it has already delivered —
 * the ordering a hand-written producer gets from statement order. A notifier
 * subscribed to `ticks` while a tick is being delivered first sees the next
 * one (Subjects deliver to a snapshot of their observers).
 */
const afterDelivery = <A, B>(build: (values: Observable<A>, ticks: Observable<A>) => Observable<B>): Operator<A, B> =>
  connect<A, B>((values) => values.pipe(connect<A, B>((ticks) => build(values, ticks))));

/** Emits the n-th value (1-based) and completes. */
const nthValue =
  <A>(n: number): Operator<A, A> =>
  (source) =>
    source.pipe(skip(n - 1), take(1));

/**
 * Windows of `windowSize` values, a new one starting every
 * `startWindowEvery` values (default: `windowSize`, back to back).
 *
 * `windowToggle` whose openings and closings are counts of the source's own
 * values: open now and after every `startWindowEvery`-th value, close each
 * window after its `windowSize`-th. Counted through `afterDelivery`, so a
 * window never sees the value that opened it and always sees the one that
 * closes it. The opening count is re-subscribed (`repeat`) after each window
 * opens, which keeps every closing ahead of the next opening on the same
 * value: a full window closes before the next one opens, as in RxJS.
 */
export const windowCount = <A>(windowSize: number, startWindowEvery = 0): Operator<A, Observable<A>> => {
  const startEvery = startWindowEvery > 0 ? startWindowEvery : windowSize;
  return afterDelivery<A, Observable<A>>((values, ticks) =>
    values.pipe(
      windowToggle<A, unknown>(concat(of(0), ticks.pipe(nthValue(startEvery), repeat())), () => ticks.pipe(nthValue(windowSize))),
    ),
  );
};

/** Opens a window now and on every `interval` tick: the openings of `windowTimeEvery…`. */
const everyInterval = (interval: number, scheduler: Scheduler): Observable<unknown> => merge(timer(interval, interval, scheduler), of(0));

/**
 * Windows of `timeSpan` ms, back to back: a new window opens when the previous
 * one closes. At a frame where the timer and a value coincide, the timer runs
 * first (it was scheduled first), so the value lands in the new window.
 */
export const windowTime = <A>(timeSpan: number, scheduler: Scheduler = asyncScheduler): Operator<A, Observable<A>> =>
  windowWhen<A>(() => timer(timeSpan, undefined, scheduler));

/** Opens a `timeSpan`-ms window now and every `interval` ms, so windows overlap or leave gaps. */
export const windowTimeEvery = <A>(timeSpan: number, interval: number, scheduler: Scheduler = asyncScheduler): Operator<A, Observable<A>> =>
  windowToggle<A, unknown>(everyInterval(interval, scheduler), () => timer(timeSpan, undefined, scheduler));

/** Like `windowTime`, but closes a window early once it has emitted `maxSize` values. */
export const windowTimeOrCount = <A>(timeSpan: number, maxSize: number, scheduler: Scheduler = asyncScheduler): Operator<A, Observable<A>> =>
  afterDelivery<A, Observable<A>>((values, ticks) =>
    values.pipe(windowWhen<A>(() => race(timer(timeSpan, undefined, scheduler), ticks.pipe(nthValue(maxSize))))),
  );

/**
 * `windowTimeEvery` and `windowTimeOrCount` together. One documented
 * difference from RxJS (README): a value reaches every open window before the
 * window it fills closes, where RxJS closes that window between deliveries.
 */
export const windowTimeEveryOrCount = <A>(
  timeSpan: number,
  interval: number,
  maxSize: number,
  scheduler: Scheduler = asyncScheduler,
): Operator<A, Observable<A>> =>
  afterDelivery<A, Observable<A>>((values, ticks) =>
    values.pipe(
      windowToggle<A, unknown>(everyInterval(interval, scheduler), () => race(timer(timeSpan, undefined, scheduler), ticks.pipe(nthValue(maxSize)))),
    ),
  );

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
