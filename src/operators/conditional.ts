import { observable } from '../core/observable.js';
import { cont, flushError, flushWith, fromMachine, halt } from '../core/machine.js';
import { emptyError, notFoundError, sequenceError } from '../core/errors.js';
import type { Observable, Operator } from '../core/types.js';
import { identity } from '../core/pipe.js';
import type { ObservableInput } from '../creation/creation.js';
import { subscribeInner } from './inner.js';

/**
 * Conditional, boolean and mathematical operators.
 *
 * Almost all of them are pure Mealy machines: the decision logic lives in
 * `step` and `flush`, and can be tested with `runMachine` alone.
 */

/** Emits `true` if every value satisfies `predicate`; `false` (and completes) at the first one that does not. */
export const every = <A>(predicate: (value: A, index: number) => boolean): Operator<A, boolean> =>
  fromMachine<null, A, boolean>({
    init: () => null,
    step: (s, a, i) => (predicate(a, i) ? cont(s) : halt(s, [false])),
    flush: () => flushWith([true]),
  });

/** Emits the first value matching `predicate` and completes; `undefined` if there is none. */
export function find<A, B extends A>(predicate: (value: A, index: number) => value is B): Operator<A, B | undefined>;
export function find<A>(predicate: (value: A, index: number) => boolean): Operator<A, A | undefined>;
export function find<A>(predicate: (value: A, index: number) => boolean): Operator<A, A | undefined> {
  return fromMachine<null, A, A | undefined>({
    init: () => null,
    step: (s, a, i) => (predicate(a, i) ? halt(s, [a]) : cont(s)),
    flush: () => flushWith([undefined]),
  });
}

/** Emits the index of the first value matching `predicate` and completes; `-1` if there is none. */
export const findIndex = <A>(predicate: (value: A, index: number) => boolean): Operator<A, number> =>
  fromMachine<null, A, number>({
    init: () => null,
    step: (s, a, i) => (predicate(a, i) ? halt(s, [i]) : cont(s)),
    flush: () => flushWith([-1]),
  });

/** Emits `false` at the first value, or `true` if the source completes empty. */
export const isEmpty = <A>(): Operator<A, boolean> =>
  fromMachine<null, A, boolean>({
    init: () => null,
    step: (s) => halt(s, [false]),
    flush: () => flushWith([true]),
  });

/** Errors with `factory()` (default: `EmptyError`) if the source completes without emitting. */
export const throwIfEmpty = <A>(factory: () => unknown = emptyError): Operator<A, A> =>
  fromMachine<boolean, A, A>({
    init: () => false,
    step: (_seen, a) => cont(true, [a]),
    flush: (seen) => (seen ? flushWith([]) : flushError(factory())),
  });

/** Skips the last `n` values: each value is emitted once `n` newer ones have arrived. */
export const skipLast = <A>(n: number): Operator<A, A> =>
  n <= 0
    ? identity
    : fromMachine<readonly A[], A, A>({
        init: () => [],
        step: (buf, a) => (buf.length < n ? cont([...buf, a]) : cont([...buf.slice(1), a], [buf[0] as A])),
      });

/** `distinctUntilChanged` on one property of the values. */
export const distinctUntilKeyChanged = <A, K extends keyof A>(
  key: K,
  equals: (x: A[K], y: A[K]) => boolean = (x, y) => x === y,
): Operator<A, A> =>
  fromMachine<{ readonly has: boolean; readonly last: A[K] | undefined }, A, A>({
    init: () => ({ has: false, last: undefined }),
    step: (s, a) => (s.has && equals(s.last as A[K], a[key]) ? cont(s) : cont({ has: true, last: a[key] }, [a])),
  });

type SingleState<A> = { readonly seen: boolean; readonly match: { readonly value: A } | null };

/**
 * Emits the one value matching `predicate` (default: any value).
 *
 * Base case: exactly one match, which is emitted on completion.
 * Edge cases, each an error:
 *  - a second match: `SequenceError`, immediately
 *  - values, but none matched: `NotFoundError`
 *  - no values at all: `EmptyError`
 */
export function single<A, B extends A>(predicate: (value: A, index: number) => value is B): Operator<A, B>;
export function single<A>(predicate?: (value: A, index: number) => boolean): Operator<A, A>;
export function single<A>(predicate?: (value: A, index: number) => boolean): Operator<A, A> {
  return fromMachine<SingleState<A>, A, A>({
    init: () => ({ seen: false, match: null }),
    step: (s, a, i) => {
      if (predicate && !predicate(a, i)) return cont({ seen: true, match: s.match });
      // a machine step cannot emit an error; throwing routes it to `error` through the shell
      if (s.match) throw sequenceError();
      return cont({ seen: true, match: { value: a } });
    },
    flush: (s) => (s.match ? flushWith([s.match.value]) : flushError(s.seen ? notFoundError() : emptyError())),
  });
}

const defaultCompare = <A>(x: A, y: A): number => (x < y ? -1 : x > y ? 1 : 0);

const extremum =
  (sign: 1 | -1) =>
  <A>(comparer: (x: A, y: A) => number = defaultCompare): Operator<A, A> =>
    fromMachine<{ readonly value: A } | null, A, A>({
      init: () => null,
      step: (best, a) => cont(best === null || sign * comparer(a, best.value) > 0 ? { value: a } : best),
      flush: (best) => flushWith(best ? [best.value] : []),
    });

/** Emits the largest value on completion (by `comparer`, default `<`). Completes empty for an empty source. */
export const max: <A>(comparer?: (x: A, y: A) => number) => Operator<A, A> = extremum(1);

/** Emits the smallest value on completion (by `comparer`, default `<`). Completes empty for an empty source. */
export const min: <A>(comparer?: (x: A, y: A) => number) => Operator<A, A> = extremum(-1);

/**
 * Emits `true` if the source and `compareTo` emit equal sequences
 * (by `comparator`, default `===`), `false` as soon as they differ.
 * Both are subscribed at once; values that arrive early are buffered.
 */
export const sequenceEqual =
  <A>(compareTo: ObservableInput<A>, comparator: (a: A, b: A) => boolean = (a, b) => a === b): Operator<A, boolean> =>
  (source: Observable<A>) =>
    observable<boolean>((sink) => {
      interface Side {
        readonly buffer: A[];
        complete: boolean;
      }
      const left: Side = { buffer: [], complete: false };
      const right: Side = { buffer: [], complete: false };
      const emit = (equal: boolean): void => {
        sink.next(equal);
        sink.complete();
      };
      // `self` received a value: compare it with the oldest unmatched one of `other`
      const onValue = (self: Side, other: Side, a: A, aIsLeft: boolean): void => {
        if (other.buffer.length === 0) {
          if (other.complete) emit(false);
          else self.buffer.push(a);
          return;
        }
        const b = other.buffer.shift() as A;
        if (!(aIsLeft ? comparator(a, b) : comparator(b, a))) emit(false);
      };
      const onComplete = (self: Side, other: Side): void => {
        self.complete = true;
        // like RxJS, decide only once both sides are done
        if (other.complete) emit(other.buffer.length === 0);
      };
      const l = subscribeInner(source, sink, {
        next: (a) => onValue(left, right, a, true),
        complete: () => onComplete(left, right),
      });
      const r = subscribeInner(compareTo, sink, {
        next: (b) => onValue(right, left, b, false),
        complete: () => onComplete(right, left),
      });
      return () => {
        l.close();
        r.close();
      };
    });
