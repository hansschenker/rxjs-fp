/**
 * Multicast vocabulary: `import { shareReplay, publish } from 'rxjs-fp/multicast'`.
 *
 * Two views of one idea, a Subject between the source and its subscribers:
 *
 *   automatic connection   shareWith(connector, lifecycle)   → share, shareReplay, shareLatest
 *   explicit connection    connectableOnce(source, connector) → publish, publishReplay, publishBehavior, publishLast
 *
 * Each named operator only chooses the Subject (plain, replay, behaviour,
 * async) and, for `shareWith`, the lifecycle (`WhileSubscribed`, `Cached`).
 *
 * Built only on the kernel (checked by test/layering.test.ts), and each
 * definition confirmed against RxJS 7 by test/differential.test.ts.
 */
import {
  Cached,
  connectableOnce,
  createAsyncSubject,
  createBehaviorSubject,
  createReplaySubject,
  createSubject,
  shareWith,
  WhileSubscribed,
} from '../kernel.js';
import type { Connectable, Observable, Operator } from '../kernel.js';

// The family's kernel members, so the family is importable from one place
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
} from '../kernel.js';
export type { Subject, Connectable, ShareLifecycle } from '../kernel.js';

// --- automatic connection ---------------------------------------------------

/** One subscription to the source while anyone listens; a later subscriber starts it again. */
export const share = <A>(): Operator<A, A> => shareWith<A>(createSubject, WhileSubscribed);

/** One subscription to the source, kept with its last `bufferSize` values for late subscribers. */
export const shareReplay = <A>(bufferSize: number = Number.POSITIVE_INFINITY): Operator<A, A> =>
  shareWith<A>(() => createReplaySubject<A>(bufferSize), Cached);

/** Like `share`, and a late subscriber gets the latest value at once. For state streams. */
export const shareLatest = <A>(): Operator<A, A> => shareWith<A>(() => createReplaySubject<A>(1), WhileSubscribed);

// --- explicit connection: the theater --------------------------------------

/** A theater with no recording: subscribers see what is played after they arrive. */
export const publish = <A>(source: Observable<A>): Connectable<A> => connectableOnce(source, () => createSubject<A>());

/** A theater that records the last `bufferSize` values and replays them to late subscribers. */
export const publishReplay = <A>(source: Observable<A>, bufferSize: number = Number.POSITIVE_INFINITY): Connectable<A> =>
  connectableOnce(source, () => createReplaySubject<A>(bufferSize));

/** A theater that always shows a current value, starting with `initial`. */
export const publishBehavior = <A>(source: Observable<A>, initial: A): Connectable<A> =>
  connectableOnce(source, () => createBehaviorSubject(initial));

/** A theater that shows only the last value, when the source completes. */
export const publishLast = <A>(source: Observable<A>): Connectable<A> => connectableOnce(source, () => createAsyncSubject<A>());
