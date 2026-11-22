import { observable, subscribeChild } from '../core/observable.js';
import { cont, flushWith, fromMachine } from '../core/machine.js';
import type { Observer, Operator } from '../core/types.js';

/**
 * Transformation operators.
 *
 * Stateless operators (`map`, `tap`) use the shell directly with a local
 * index counter; stateful ones are pure Mealy machines run by `fromMachine`.
 */

/** Projects each value. `f` receives the value and its index. */
export const map =
  <A, B>(f: (value: A, index: number) => B): Operator<A, B> =>
  (source) =>
    observable((sink) => {
      let index = 0;
      return subscribeChild(source, sink, { next: (a) => sink.next(f(a, index++)) });
    });

/** Like `reduce`, but emits every intermediate accumulation. */
export const scan = <A, S>(f: (acc: S, value: A, index: number) => S, seed: S): Operator<A, S> =>
  fromMachine<S, A, S>({
    init: () => seed,
    step: (acc, a, i) => {
      const next = f(acc, a, i);
      return cont(next, [next]);
    },
  });

/** Folds the whole sequence; emits the result on completion. */
export const reduce = <A, S>(f: (acc: S, value: A, index: number) => S, seed: S): Operator<A, S> =>
  fromMachine<S, A, S>({
    init: () => seed,
    step: (acc, a, i) => cont(f(acc, a, i)),
    flush: (acc) => flushWith([acc]),
  });

/** Emits `value` if the source completes without emitting. */
export const defaultIfEmpty = <A, B>(value: B): Operator<A, A | B> =>
  fromMachine<boolean, A, A | B>({
    init: () => false,
    step: (_seen, a) => cont(true, [a]),
    flush: (seen) => flushWith(seen ? [] : [value]),
  });

/** Performs side effects for notifications without changing the stream. */
export const tap =
  <A>(observerOrNext: Partial<Observer<A>> | ((value: A) => void)): Operator<A, A> =>
  (source) =>
    observable((sink) => {
      const o = typeof observerOrNext === 'function' ? { next: observerOrNext } : observerOrNext;
      return subscribeChild(source, sink, {
        next: (a) => {
          o.next?.(a);
          sink.next(a);
        },
        error: (e) => {
          o.error?.(e);
          sink.error(e);
        },
        complete: () => {
          o.complete?.();
          sink.complete();
        },
      });
    });

// ---------------------------------------------------------------------------
// Notifications as data. The operators are vocabulary (src/vocabulary/values.ts):
// materialize is map, concat with one C and catchError to one E; dematerialize
// is takeWhile, then mergeMap into of or throwError.
// ---------------------------------------------------------------------------

export type Notification<A> =
  | { readonly kind: 'N'; readonly value: A }
  | { readonly kind: 'E'; readonly error: unknown }
  | { readonly kind: 'C' };

export const nextNotification = <A>(value: A): Notification<A> => ({ kind: 'N', value });
export const errorNotification = (error: unknown): Notification<never> => ({ kind: 'E', error });
export const COMPLETE_NOTIFICATION: Notification<never> = Object.freeze({ kind: 'C' });

/** Feeds a notification to an observer. */
export const observeNotification = <A>(n: Notification<A>, o: Partial<Observer<A>>): void => {
  switch (n.kind) {
    case 'N':
      o.next?.(n.value);
      return;
    case 'E':
      o.error?.(n.error);
      return;
    case 'C':
      o.complete?.();
  }
};

