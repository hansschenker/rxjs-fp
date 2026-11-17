import { observable } from '../core/observable.js';
import type { Observable, Teardown } from '../core/types.js';
import type { Scheduler } from '../scheduler/scheduler.js';
import { observeOn, subscribeOn } from '../operators/time.js';
import { from, type ObservableInput } from './creation.js';

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
 * Converts `input` like `from`, but runs it on `scheduler`: iterables and
 * array-likes emit one value per scheduler task; Observables, promises and
 * async iterables are subscribed and observed on the scheduler.
 */
export const scheduled = <A>(input: ObservableInput<A>, scheduler: Scheduler): Observable<A> => {
  if (!isObservableLike(input)) {
    if (isIterable(input)) return scheduleIterable(input as Iterable<A>, scheduler);
    return scheduleIterable(Array.from(input as ArrayLike<A>), scheduler);
  }
  return from(input).pipe(subscribeOn<A>(scheduler), observeOn<A>(scheduler));
};

/**
 * A `for` loop as an Observable:
 *
 *   for (let state = initial; condition(state); state = iterate(state)) emit(state)
 *
 * Pure functions in, values out. Map the states with `map`; for a loop that
 * runs forever, pass `() => true` and stop it with `take`. See also `unfold`.
 *
 * With a `scheduler`, each value is emitted in its own scheduler task
 * instead of synchronously.
 */
export const generate = <S>(
  initial: S,
  condition: (state: S) => boolean,
  iterate: (state: S) => S,
  scheduler?: Scheduler,
): Observable<S> => {
  const states: Iterable<S> = {
    [Symbol.iterator]: () => {
      let state = initial;
      let started = false;
      return {
        next: (): IteratorResult<S> => {
          if (started) state = iterate(state);
          started = true;
          if (!condition(state)) return { done: true, value: undefined };
          return { done: false, value: state };
        },
      };
    },
  };
  return scheduler ? scheduleIterable(states, scheduler) : from(states);
};
