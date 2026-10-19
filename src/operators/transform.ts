import { observable, subscribeChild } from '../core/observable.js';
import { cont, flushWith, fromMachine } from '../core/machine.js';
import type { Observable, Observer, Operator } from '../core/types.js';

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

/** Replaces every value with a constant. */
export const as = <B>(value: B): Operator<unknown, B> => map(() => value);

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

/** Emits `[previous, current]` pairs. */
export const pairwise = <A>(): Operator<A, readonly [A, A]> =>
  fromMachine<{ readonly has: boolean; readonly prev: A | undefined }, A, readonly [A, A]>({
    init: () => ({ has: false, prev: undefined }),
    step: (s, a) => (s.has ? cont({ has: true, prev: a }, [[s.prev as A, a] as const]) : cont({ has: true, prev: a })),
  });

/** Buffers values into arrays of `size`, starting a new buffer every `every` values. */
export const bufferCount = <A>(size: number, every: number = size): Operator<A, readonly A[]> =>
  fromMachine<{ readonly buffers: ReadonlyArray<readonly A[]> }, A, readonly A[]>({
    init: () => ({ buffers: [] }),
    step: (s, a, i) => {
      const opened = i % every === 0 ? [...s.buffers, []] : s.buffers;
      const filled = opened.map((b) => [...b, a]);
      const full = filled.filter((b) => b.length >= size);
      return cont({ buffers: filled.filter((b) => b.length < size) }, full);
    },
    flush: (s) => flushWith(s.buffers.filter((b) => b.length > 0)),
  });

/** Counts values (optionally only those matching `predicate`). */
export const count = <A>(predicate: (value: A, index: number) => boolean = () => true): Operator<A, number> =>
  fromMachine<number, A, number>({
    init: () => 0,
    step: (n, a, i) => cont(predicate(a, i) ? n + 1 : n),
    flush: (n) => flushWith([n]),
  });

/** Emits `value` if the source completes without emitting. */
export const defaultIfEmpty = <A, B>(value: B): Operator<A, A | B> =>
  fromMachine<boolean, A, A | B>({
    init: () => false,
    step: (_seen, a) => cont(true, [a]),
    flush: (seen) => flushWith(seen ? [] : [value]),
  });

/** Collects all values into one array, emitted on completion. */
export const toArray =
  <A>(): Operator<A, A[]> =>
  (source) =>
    observable((sink) => {
      const acc: A[] = []; // essential per-subscription state, local to the shell
      return subscribeChild(source, sink, {
        next: (a) => {
          acc.push(a);
        },
        complete: () => {
          sink.next(acc);
          sink.complete();
        },
      });
    });

/** Pairs every value with its index as `{ index, value }`. */
export const withIndex = <A>(): Operator<A, { readonly index: number; readonly value: A }> =>
  map((value, index) => ({ index, value }));

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
// Notifications as data: materialize / dematerialize
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

/** Turns every notification (including terminal ones) into a value. */
export const materialize =
  <A>(): Operator<A, Notification<A>> =>
  (source) =>
    observable((sink) =>
      subscribeChild(source, sink, {
        next: (a) => sink.next(nextNotification(a)),
        error: (e) => {
          sink.next(errorNotification(e));
          sink.complete();
        },
        complete: () => {
          sink.next(COMPLETE_NOTIFICATION);
          sink.complete();
        },
      }),
    );

/** The inverse of `materialize`. */
export const dematerialize = <A>(): Operator<Notification<A>, A> =>
  (source: Observable<Notification<A>>) =>
    observable<A>((sink) =>
      subscribeChild(source, sink, {
        next: (n) => observeNotification(n, sink),
      }),
    );

