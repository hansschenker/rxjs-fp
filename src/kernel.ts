/**
 * The kernel: the operators rxjs-fp implements directly.
 *
 * An operator belongs here only if no composition of other operators
 * reproduces its behaviour exactly (values, timing, completion, errors,
 * teardown). Everything else is vocabulary, defined in `src/vocabulary/`
 * as compositions of what this module exports. See docs/kernel.md.
 *
 * This module also exports the types and the composition helpers that the
 * vocabulary needs, so vocabulary files import from here and nowhere else.
 */

// Types and composition helpers
export type { Observable, Observer, Operator, Sink, Subscription, Teardown, ValueOf, ValuesOf } from './core/types.js';
export type { ObservableInput } from './creation/creation.js';
export type { Scheduler } from './scheduler/scheduler.js';
export { asyncScheduler } from './scheduler/scheduler.js';
export { pipe, flow, identity } from './core/pipe.js';
export { fromMachine, cont, halt, flushWith, flushError } from './core/machine.js';
export { emptyError, argumentOutOfRangeError, notFoundError, sequenceError, isTimeoutError } from './core/errors.js';
export type { TimeoutInfo } from './core/errors.js';
export { some, none, isSome } from './fp/option.js';
export type { Option } from './fp/option.js';

// Creation
export { observable } from './core/observable.js';
export { from, defer, EMPTY, NEVER, throwError, timer, unfold, using } from './creation/creation.js';
export { fromIterable, fromArrayLike, fromPromise, fromAsyncIterable, fromSubscribable, fromEventPattern, animationFrames } from './creation/creation.js';
export type { DomEventTargetLike, NodeEventEmitterLike } from './creation/creation.js';
export { scheduled } from './creation/scheduled.js';
export { bindCallback, bindNodeCallback } from './creation/bind.js';

// Value-based
export { map, scan, reduce, tap } from './operators/transform.js';
export { filter, take, takeLast, takeWhile, skipWhile, distinct, distinctUntilChanged } from './operators/filtering.js';
export { defaultIfEmpty, materialize, dematerialize } from './operators/transform.js';
export type { Notification } from './operators/transform.js';
export { throwIfEmpty, sequenceEqual } from './operators/conditional.js';

// Time-based
export { debounce, throttleWith, audit, sample, timeout, timeoutFirst, timeoutFirstAndEach, observeOn, subscribeOn } from './operators/time.js';
export { Leading, Trailing, LeadingAndTrailing } from './operators/time.js';
export type { ThrottleStrategy } from './operators/time.js';
export { window, windowToggle, windowWhen, bufferWhen } from './operators/window.js';
export { groupBy, groupByUntil } from './operators/group.js';
export type { GroupedObservable } from './operators/group.js';

// Concurrency: one operator, four strategies
export { flatMapWith, scanWith, Merge, Concat, Switch, Exhaust, expand } from './operators/flatten.js';
export type { FlattenStrategy } from './operators/flatten.js';

// Combination
export { combineLatest, zip, withLatestFrom, race } from './operators/combine.js';

// Errors and effects
export { catchError, finalize, retryWith, retryConsecutiveWith, repeat, retryNow, retryAfter, giveUp } from './operators/error.js';
export type { RetryDecision, RetryPolicy } from './operators/error.js';

// Multicasting
export {
  createSubject,
  createBehaviorSubject,
  createReplaySubject,
  createAsyncSubject,
  connectable,
  connectableOnce,
  connect,
  refCount,
  shareWith,
  WhileSubscribed,
  Cached,
} from './subject/subject.js';
export type { Subject, Connectable, ShareLifecycle } from './subject/subject.js';
