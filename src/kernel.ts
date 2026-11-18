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
export { fromMachine } from './core/machine.js';

// Creation
export { observable } from './core/observable.js';
export { from, defer, EMPTY, NEVER, throwError, timer, unfold, fromEvent, using } from './creation/creation.js';

// Value-based
export { map, scan, reduce, tap } from './operators/transform.js';
export { filter, take, takeWhile, skipWhile, distinct, distinctUntilChanged, takeUntil } from './operators/filtering.js';

// Time-based
export { debounce, throttleWith, audit, sample, timeout, observeOn, subscribeOn } from './operators/time.js';
export { Leading, Trailing, LeadingAndTrailing } from './operators/time.js';
export type { ThrottleStrategy } from './operators/time.js';
export { buffer, window } from './operators/window.js';

// Concurrency: one operator, four strategies
export { flatMapWith, Merge, Concat, Switch, Exhaust, expand } from './operators/flatten.js';
export type { FlattenStrategy } from './operators/flatten.js';

// Combination
export { combineLatest, zip, withLatestFrom, race } from './operators/combine.js';

// Errors and effects
export { catchError, finalize } from './operators/error.js';

// Multicasting
export {
  createSubject,
  createBehaviorSubject,
  createReplaySubject,
  createAsyncSubject,
  connectable,
  shareWith,
  WhileSubscribed,
  Cached,
} from './subject/subject.js';
