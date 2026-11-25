/**
 * Multicast vocabulary: `import { shareReplay, publish } from 'rxjs-fp/multicast'`.
 *
 * Two views of one idea, a Subject between the source and its subscribers:
 *
 *   automatic connection   share(policy)                    → shareLatest
 *   explicit connection    connectableOnce(source, connector) → publish, publishReplay, publishBehavior, publishLast
 *
 * Each named composition chooses a Subject and lifecycle policy. The canonical
 * share interpreter lives in the kernel; shareReplay and shareWith are the
 * kernel's own specializations of it, re-exported here.
 *
 * Built only on the kernel (checked by test/layering.test.ts), and each
 * definition confirmed against RxJS 7 by test/differential.test.ts.
 */
import {
  connectableOnce,
  createAsyncSubject,
  createBehaviorSubject,
  createReplaySubject,
  createSubject,
  defaultSharePolicy,
  share,
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
  share,
  shareReplay,
  defaultSharePolicy,
  defaultShareReplayPolicy,
  shareWith,
  WhileSubscribed,
  Cached,
} from '../kernel.js';
export type {
  Subject,
  Connectable,
  ShareLifecycle,
  ResetPolicy,
  SharePolicy,
  ReplayBounds,
  IdlePolicy,
  ShareReplayPolicy,
} from '../kernel.js';

// --- automatic connection ---------------------------------------------------

/** Like `share`, and a late subscriber gets the latest value at once. For state streams. */
export const shareLatest = <A>(): Operator<A, A> =>
  share<A>({ ...defaultSharePolicy<A>(), connector: () => createReplaySubject<A>(1) });

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
