import { observable } from '../core/observable.js';
import type { Observable, Teardown } from '../core/types.js';
import type { Scheduler } from '../scheduler/scheduler.js';
import { flatMapWith, Merge } from '../operators/flatten.js';
import { dematerialize, map, materialize } from '../operators/transform.js';
import { from, timer, type ObservableInput } from './creation.js';

const isIterable = (x: unknown): x is Iterable<unknown> =>
  x != null && typeof (x as Iterable<unknown>)[Symbol.iterator] === 'function';
const isObservableLike = (x: unknown): boolean =>
  typeof x === 'object' && x !== null && (typeof (x as { subscribe?: unknown }).subscribe === 'function' || typeof (x as { then?: unknown }).then === 'function' || Symbol.asyncIterator in x);

/** Emits each value of an iterable in its own scheduler task, then completes. */
const scheduleIterable = <A>(iterable: Iterable<A>, scheduler: Scheduler): Observable<A> =>
  observable<A>((sink) => {
    let it: Iterator<A> | null = null;
    let done = false;
    let cancel: Teardown = () => {};
    const step = (): void => {
      let r: IteratorResult<A>;
      try {
        it ??= iterable[Symbol.iterator]();
        r = it.next();
      } catch (e) {
        done = true;
        sink.error(e);
        return;
      }
      if (r.done) {
        done = true;
        sink.complete();
        return;
      }
      sink.next(r.value);
      if (!sink.closed) cancel = scheduler.schedule(step, 0);
    };
    cancel = scheduler.schedule(step, 0);
    return () => {
      cancel();
      if (!done) it?.return?.();
    };
  });

/**
 * `subscribeOn(scheduler)` then `observeOn(scheduler)`, spelled with kernel
 * operators because the kernel may not import the vocabulary. The public
 * definitions are in src/vocabulary/time.ts: a timer flattened into the
 * source, then `delay(0)` applied to the notifications.
 */
const onScheduler =
  <A>(scheduler: Scheduler) =>
  (source: Observable<A>): Observable<A> => {
    const later = <B>(value: B): Observable<B> => timer(0, undefined, scheduler).pipe(map(() => value));
    const mergeMap = flatMapWith(Merge());
    return later(source).pipe(
      mergeMap((s: Observable<A>) => s),
      materialize<A>(),
      mergeMap(later),
      dematerialize<A>(),
    );
  };

/**
 * Converts `input` like `from`, but runs it on `scheduler`: iterables and
 * array-likes emit one value per scheduler task; Observables, promises and
 * async iterables are subscribed and observed on the scheduler.
 */
export const scheduled = <A>(input: ObservableInput<A>, scheduler: Scheduler): Observable<A> => {
  if (!isObservableLike(input)) {
    if (isIterable(input)) return scheduleIterable(input as Iterable<A>, scheduler);
    return scheduleIterable(Array.from(input as ArrayLike<A>), scheduler);
  }
  return onScheduler<A>(scheduler)(from(input));
};
