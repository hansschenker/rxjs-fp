import { observable, subscribeChild } from '../core/observable.js';
import { cont, flushError, flushWith, fromMachine, halt } from '../core/machine.js';
import { emptyError } from '../core/errors.js';
import type { Observable, Operator, Subscription } from '../core/types.js';
import { EMPTY } from '../creation/creation.js';
import { isSome, type Option } from '../fp/option.js';

/** Keeps values that satisfy the predicate. Narrows the type for refinements. */
export function filter<A, B extends A>(refinement: (value: A, index: number) => value is B): Operator<A, B>;
export function filter<A>(predicate: (value: A, index: number) => boolean): Operator<A, A>;
export function filter<A>(predicate: (value: A, index: number) => boolean): Operator<A, A> {
  return (source) =>
    observable((sink) => {
      let index = 0;
      return subscribeChild(source, sink, {
        next: (a) => {
          if (predicate(a, index++)) sink.next(a);
        },
      });
    });
}

/**
 * Map and filter in one step: `some(b)` emits `b`, `none` drops the value.
 * Exceptions thrown by `f` are routed to `error` (a bug in the earlier
 * v7-fp fork reported them as unhandled instead).
 */
export const filterMap =
  <A, B>(f: (value: A, index: number) => Option<B>): Operator<A, B> =>
  (source) =>
    observable((sink) => {
      let index = 0;
      return subscribeChild(source, sink, {
        next: (a) => {
          const ob = f(a, index++);
          if (isSome(ob)) sink.next(ob.value);
        },
      });
    });

/** Emits the first `n` values, then completes. */
export const take = <A>(n: number): Operator<A, A> =>
  n <= 0
    ? () => EMPTY
    : fromMachine<number, A, A>({
        init: () => 0,
        step: (seen, a) => (seen + 1 >= n ? halt(seen + 1, [a]) : cont(seen + 1, [a])),
      });

/** Emits only the last `n` values, on completion. */
export const takeLast = <A>(n: number): Operator<A, A> =>
  n <= 0
    ? () => EMPTY
    : fromMachine<readonly A[], A, A>({
        init: () => [],
        step: (buf, a) => cont(buf.length >= n ? [...buf.slice(1), a] : [...buf, a]),
        flush: (buf) => flushWith(buf),
      });

/** Skips the first `n` values. */
export const skip = <A>(n: number): Operator<A, A> => filter<A>((_, i) => i >= n);

/** Emits while the predicate holds; completes at the first failure (optionally emitting it). */
export const takeWhile = <A>(predicate: (value: A, index: number) => boolean, inclusive = false): Operator<A, A> =>
  fromMachine<null, A, A>({
    init: () => null,
    step: (s, a, i) => (predicate(a, i) ? cont(s, [a]) : halt(s, inclusive ? [a] : [])),
  });

/** Skips while the predicate holds, then emits everything. */
export const skipWhile = <A>(predicate: (value: A, index: number) => boolean): Operator<A, A> =>
  fromMachine<boolean, A, A>({
    init: () => true,
    step: (skipping, a, i) => (skipping && predicate(a, i) ? cont(true) : cont(false, [a])),
  });

/** Drops values equal to the previous one (by `equals`, default `===`). */
export const distinctUntilChanged = <A>(equals: (x: A, y: A) => boolean = (x, y) => x === y): Operator<A, A> =>
  fromMachine<{ readonly has: boolean; readonly last: A | undefined }, A, A>({
    init: () => ({ has: false, last: undefined }),
    step: (s, a) => (s.has && equals(s.last as A, a) ? cont(s) : cont({ has: true, last: a }, [a])),
  });

/** Drops values whose key has been seen before. The key set is essential state, kept in the shell. */
export const distinct =
  <A, K = A>(keySelector: (value: A) => K = (a) => a as unknown as K): Operator<A, A> =>
  (source) =>
    observable((sink) => {
      const seen = new Set<K>();
      return subscribeChild(source, sink, {
        next: (a) => {
          const k = keySelector(a);
          if (!seen.has(k)) {
            seen.add(k);
            sink.next(a);
          }
        },
      });
    });

const NO_DEFAULT: unique symbol = Symbol('rxjs-fp/no-default');

/**
 * Emits the first value (matching `predicate`) and completes. Errors with
 * `EmptyError` if there is none, unless a default value is given.
 */
export function first<A>(): Operator<A, A>;
export function first<A, B extends A>(predicate: (value: A, index: number) => value is B): Operator<A, B>;
export function first<A>(predicate: (value: A, index: number) => boolean): Operator<A, A>;
export function first<A, D>(predicate: (value: A, index: number) => boolean, defaultValue: D): Operator<A, A | D>;
export function first<A, D>(
  predicate: (value: A, index: number) => boolean = () => true,
  defaultValue: D | typeof NO_DEFAULT = NO_DEFAULT,
): Operator<A, A | D> {
  return fromMachine<null, A, A | D>({
    init: () => null,
    step: (s, a, i) => (predicate(a, i) ? halt(s, [a]) : cont(s)),
    flush: () => (defaultValue === NO_DEFAULT ? flushError(emptyError()) : flushWith([defaultValue])),
  });
}

/** Emits the last value (matching `predicate`) on completion; `EmptyError` if none and no default. */
export function last<A>(): Operator<A, A>;
export function last<A>(predicate: (value: A, index: number) => boolean): Operator<A, A>;
export function last<A, D>(predicate: (value: A, index: number) => boolean, defaultValue: D): Operator<A, A | D>;
export function last<A, D>(
  predicate: (value: A, index: number) => boolean = () => true,
  defaultValue: D | typeof NO_DEFAULT = NO_DEFAULT,
): Operator<A, A | D> {
  return fromMachine<Option<A>, A, A | D>({
    init: () => ({ _tag: 'None' }),
    step: (s, a, i) => cont(predicate(a, i) ? { _tag: 'Some', value: a } : s),
    flush: (s) =>
      isSome(s) ? flushWith([s.value]) : defaultValue === NO_DEFAULT ? flushError(emptyError()) : flushWith([defaultValue]),
  });
}

/** Emits only the value at `index`; errors with `EmptyError` if the source is shorter. */
export const elementAt = <A>(index: number): Operator<A, A> => first<A>((_, i) => i === index);

/** Discards all values, keeps the terminal notification. */
export const ignoreElements =
  (): Operator<unknown, never> =>
  (source) =>
    observable<never>((sink) => subscribeChild(source, sink, { next: () => {} }));

/** Mirrors the source until `notifier` emits, then completes. */
export const takeUntil =
  <A>(notifier: Observable<unknown>): Operator<A, A> =>
  (source) =>
    observable((sink) => {
      subscribeChild(notifier, sink, { next: () => sink.complete(), complete: () => {} });
      if (sink.closed) return;
      return subscribeChild(source, sink);
    });

/** Skips source values until `notifier` emits. */
export const skipUntil =
  <A>(notifier: Observable<unknown>): Operator<A, A> =>
  (source) =>
    observable((sink) => {
      let open = false;
      let n: Subscription | undefined;
      n = subscribeChild(notifier, sink, {
        next: () => {
          open = true;
          n?.unsubscribe();
        },
        complete: () => {},
      });
      if (open) n.unsubscribe();
      return subscribeChild(source, sink, {
        next: (a) => {
          if (open) sink.next(a);
        },
      });
    });
