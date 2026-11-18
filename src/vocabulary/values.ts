/**
 * Value vocabulary: `import { first } from 'rxjs-fp/values'`.
 *
 * Everything here is a composition of the value kernel:
 * `map`, `filter`, `reduce`, `take`, `takeLast`, `skipWhile`,
 * `distinctUntilChanged`, `defaultIfEmpty`, `throwIfEmpty`.
 *
 * Most follow one sentence: "pick the value with `filter` and `take(1)`,
 * then say what an empty stream means". `first`, `elementAt`, `find`,
 * `findIndex`, `every` and `isEmpty` differ only in those three choices.
 *
 * Built only on the kernel (checked by test/layering.test.ts), and each
 * definition confirmed against RxJS 7 by test/differential.test.ts.
 */
import {
  argumentOutOfRangeError,
  defaultIfEmpty,
  distinctUntilChanged,
  emptyError,
  filter,
  flow,
  map,
  reduce,
  skipWhile,
  take,
  takeLast,
  throwIfEmpty,
} from '../kernel.js';
import type { Operator } from '../kernel.js';

// The family's kernel members, so the family is importable from one place
export {
  map,
  filter,
  scan,
  reduce,
  take,
  takeLast,
  takeWhile,
  skipWhile,
  distinct,
  distinctUntilChanged,
  defaultIfEmpty,
  throwIfEmpty,
  fromMachine,
} from '../kernel.js';

type Predicate<A> = (value: A, index: number) => boolean;
const always = (): boolean => true;
const NO_DEFAULT: unique symbol = Symbol('rxjs-fp/no-default');

/** What an empty stream means: the default value if there is one, otherwise the error. */
const orIfEmpty = <A, D>(defaultValue: D | typeof NO_DEFAULT, error: () => unknown): Operator<A, A | D> =>
  defaultValue === NO_DEFAULT ? throwIfEmpty<A>(error) : defaultIfEmpty<A, D>(defaultValue);

// --- transforming -----------------------------------------------------------

/** Replaces every value with `value`. */
export const as = <B>(value: B): Operator<unknown, B> => map(() => value);

/** Pairs every value with its index. */
export const withIndex = <A>(): Operator<A, { readonly index: number; readonly value: A }> =>
  map((value: A, index) => ({ index, value }));

// --- counting and collecting ------------------------------------------------

/** On completion, emits how many values passed `predicate` (all values by default). */
export const count = <A>(predicate: Predicate<A> = always): Operator<A, number> =>
  reduce<A, number>((n, a, i) => (predicate(a, i) ? n + 1 : n), 0);

/** A persistent list: adding a value shares the rest, so collecting stays linear and pure. */
type List<A> = { readonly head: A; readonly tail: List<A> } | null;
const listToArray = <A>(list: List<A>): A[] => {
  const out: A[] = [];
  for (let l = list; l !== null; l = l.tail) out.push(l.head);
  return out.reverse();
};

/** On completion, emits all values as one array. */
export const toArray = <A>(): Operator<A, A[]> =>
  flow(
    reduce<A, List<A>>((tail, head) => ({ head, tail }), null),
    map(listToArray),
  );

// --- skipping ---------------------------------------------------------------

/** Drops the first `n` values. */
export const skip = <A>(n: number): Operator<A, A> => skipWhile<A>((_, i) => i < n);

/** Drops every value; keeps completion and errors. */
export const ignoreElements = (): Operator<unknown, never> => filter((): boolean => false) as Operator<unknown, never>;

/** Drops a value whose `key` equals the previous value's `key`. */
export const distinctUntilKeyChanged = <A, K extends keyof A>(
  key: K,
  equals: (x: A[K], y: A[K]) => boolean = (x, y) => x === y,
): Operator<A, A> => distinctUntilChanged<A>((x, y) => equals(x[key], y[key]));

// --- picking one value ------------------------------------------------------

/** The first value (passing `predicate`), then completes. Empty: `defaultValue`, or `EmptyError`. */
export function first<A>(): Operator<A, A>;
export function first<A, B extends A>(predicate: (value: A, index: number) => value is B): Operator<A, B>;
export function first<A>(predicate: Predicate<A>): Operator<A, A>;
export function first<A, D>(predicate: Predicate<A> | null | undefined, defaultValue: D): Operator<A, A | D>;
export function first<A, D>(predicate?: Predicate<A> | null, defaultValue: D | typeof NO_DEFAULT = NO_DEFAULT): Operator<A, A | D> {
  return flow(filter<A>(predicate ?? always), take<A>(1), orIfEmpty<A, D>(defaultValue, emptyError));
}

/** The last value (passing `predicate`), on completion. Empty: `defaultValue`, or `EmptyError`. */
export function last<A>(): Operator<A, A>;
export function last<A>(predicate: Predicate<A>): Operator<A, A>;
export function last<A, D>(predicate: Predicate<A> | null | undefined, defaultValue: D): Operator<A, A | D>;
export function last<A, D>(predicate?: Predicate<A> | null, defaultValue: D | typeof NO_DEFAULT = NO_DEFAULT): Operator<A, A | D> {
  return flow(filter<A>(predicate ?? always), takeLast<A>(1), orIfEmpty<A, D>(defaultValue, emptyError));
}

/** The value at `index`. Too few values: `defaultValue`, or `ArgumentOutOfRangeError`. */
export const elementAt = <A, D = A>(index: number, ...defaultValue: [] | [D]): Operator<A, A | D> => {
  if (index < 0) throw argumentOutOfRangeError();
  return flow(
    filter<A>((_, i) => i === index),
    take<A>(1),
    orIfEmpty<A, D>(defaultValue.length > 0 ? (defaultValue[0] as D) : NO_DEFAULT, argumentOutOfRangeError),
  );
};

/** The first value passing `predicate`, or `undefined` if none does. */
export function find<A, B extends A>(predicate: (value: A, index: number) => value is B): Operator<A, B | undefined>;
export function find<A>(predicate: Predicate<A>): Operator<A, A | undefined>;
export function find<A>(predicate: Predicate<A>): Operator<A, A | undefined> {
  return flow(filter<A>(predicate), take<A>(1), defaultIfEmpty<A, undefined>(undefined));
}

/** The index of the first value passing `predicate`, or -1 if none does. */
export const findIndex = <A>(predicate: Predicate<A>): Operator<A, number> =>
  flow(
    map<A, number>((a, i) => (predicate(a, i) ? i : -1)),
    filter((i: number) => i >= 0),
    take<number>(1),
    defaultIfEmpty<number, number>(-1),
  );

// --- yes/no answers ---------------------------------------------------------

/** `false` as soon as a value fails `predicate`; `true` if the source completes first. */
export const every = <A>(predicate: Predicate<A>): Operator<A, boolean> =>
  flow(
    filter<A>((a, i) => !predicate(a, i)),
    take<A>(1),
    as(false),
    defaultIfEmpty<boolean, boolean>(true),
  );

/** `false` as soon as a value arrives; `true` if the source completes first. */
export const isEmpty = <A>(): Operator<A, boolean> => flow(take<A>(1), as(false), defaultIfEmpty<boolean, boolean>(true));
