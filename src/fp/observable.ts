import type { Observable, Operator } from '../core/types.js';
import { EMPTY, of, throwError } from '../vocabulary/creation.js';
import { map } from '../operators/transform.js';
import { filter as filterOp } from '../operators/filtering.js';
import { filterMap as filterMapOp } from '../vocabulary/values.js';
import { concat, merge, mergeMap } from '../vocabulary/concurrency.js';
import { catchError } from '../operators/error.js';
import { isLeft, isRight, left, right, type Either } from './either.js';
import { isNone, none, some, type Option } from './option.js';
import type * as TC from './typeclasses.js';
import type { Separated } from './typeclasses.js';

/**
 * Lawful typeclass instances for Observable, kept to the ones that read
 * naturally to an RxJS user: Functor (`map`), Compactable and Filterable
 * (`filter`, `filterMap`, `partition`, `compact`, `separate`), and the two
 * Monoids (`merge` and `concat`, with `EMPTY` as identity).
 *
 * The Monad, Applicative and Alternative instances (`MonadMerge`,
 * `MonadConcat`, `ap`, `apLatest`, `alt`, `orElse`) were removed: an RxJS
 * user writes `mergeMap`, `concatMap`, `combineLatest`, `merge` and
 * `catchError`. The Monad laws are still tested, directly on `of`,
 * `mergeMap` and `concatMap` (test/laws.test.ts).
 */

export const URI = 'Observable' as const;
export type URI = typeof URI;

// Typeclass functions are unary: operators pass an index as a second
// argument, which must not leak into lawful instances.
const _map = <A, B>(fa: Observable<A>, f: (a: A) => B): Observable<B> => map((a: A) => f(a))(fa);
const _mergeChain = <A, B>(fa: Observable<A>, f: (a: A) => Observable<B>): Observable<B> => mergeMap((a: A) => f(a))(fa);
const _filterMap = <A, B>(fa: Observable<A>, f: (a: A) => Option<B>): Observable<B> => filterMapOp((a: A) => f(a))(fa);
const _partitionMap = <A, B, C>(fa: Observable<A>, f: (a: A) => Either<B, C>): Separated<Observable<B>, Observable<C>> => ({
  left: filterMapOp((a: A) => {
    const e = f(a);
    return isLeft(e) ? some(e.left) : none;
  })(fa),
  right: filterMapOp((a: A) => {
    const e = f(a);
    return isRight(e) ? some(e.right) : none;
  })(fa),
});

export const ObservableFunctor: TC.Functor<URI> = { URI, map: _map };

export const ObservableCompactable: TC.Compactable<URI> = {
  URI,
  compact: (fa) => _filterMap(fa, (o) => o),
  separate: (fa) => _partitionMap(fa, (e) => e),
};

/**
 * Note: `partition`/`partitionMap`/`separate` return two Observables that
 * each subscribe to the source (cold semantics). Share the source first if
 * it has side effects.
 */
export const ObservableFilterable: TC.Filterable<URI> = {
  URI,
  map: _map,
  compact: ObservableCompactable.compact,
  separate: ObservableCompactable.separate,
  filter: <A>(fa: Observable<A>, p: (a: A) => boolean) => filterOp((a: A) => p(a))(fa),
  filterMap: _filterMap,
  partition: <A>(fa: Observable<A>, p: (a: A) => boolean) => ({
    left: filterOp((a: A) => !p(a))(fa),
    right: filterOp((a: A) => p(a))(fa),
  }),
  partitionMap: _partitionMap,
};

/** Monoid under `merge` with `EMPTY` as identity. */
export const getMergeMonoid = <A>(): TC.Monoid<Observable<A>> => ({ concat: (x, y) => merge(x, y), empty: EMPTY });

/** Monoid under `concat` with `EMPTY` as identity. */
export const getConcatMonoid = <A>(): TC.Monoid<Observable<A>> => ({ concat: (x, y) => concat(x, y), empty: EMPTY });

// ---------------------------------------------------------------------------
// Data-last (pipeable) combinators
// ---------------------------------------------------------------------------

/** Monadic bind (`mergeMap` without index). */
export const chain = <A, B>(f: (a: A) => Observable<B>): Operator<A, B> => mergeMap((a: A) => f(a));

export const flatten = <A>(ffa: Observable<Observable<A>>): Observable<A> => _mergeChain(ffa, (fa) => fa);

export const compact = <A>(fa: Observable<Option<A>>): Observable<A> => ObservableCompactable.compact(fa);

export const separate = <E, A>(fa: Observable<Either<E, A>>): Separated<Observable<E>, Observable<A>> =>
  ObservableCompactable.separate(fa);

// ---------------------------------------------------------------------------
// Natural transformations
// ---------------------------------------------------------------------------

/** `some(a)` → `of(a)`, `none` → `EMPTY` */
export const fromOption = <A>(o: Option<A>): Observable<A> => (isNone(o) ? EMPTY : of(o.value));

/** `right(a)` → `of(a)`, `left(e)` → error `e` */
export const fromEither = <E, A>(e: Either<E, A>): Observable<A> => (isLeft(e) ? throwError(() => e.left) : of(e.right));

/** Moves errors into the value channel: values become `right`, an error becomes a final `left`. */
export const attempt =
  <A>(): Operator<A, Either<unknown, A>> =>
  (fa) =>
    catchError<Either<unknown, A>, Either<unknown, A>>((e) => of(left(e)))(map((a: A) => right<A, unknown>(a))(fa));

/** Inverse of `attempt` for a single stream: `left(e)` raises `e`. */
export const rethrow =
  <E, A>(): Operator<Either<E, A>, A> =>
  (fa) =>
    mergeMap((e: Either<E, A>) => fromEither(e))(fa);


// ---------------------------------------------------------------------------
// Do-notation
// ---------------------------------------------------------------------------

/** Starting point for do-notation: an Observable of the empty record. */
export const Do: Observable<{}> = of({});

/** Starts do-notation from an existing stream: `{ [name]: value }`. */
export const bindTo =
  <N extends string>(name: N) =>
  <A>(fa: Observable<A>): Observable<{ readonly [K in N]: A }> =>
    _map(fa, (a) => ({ [name]: a }) as { readonly [K in N]: A });

/** Binds the result of an effectful step under `name` (via `chain`). */
export const bind =
  <N extends string, A, B>(name: Exclude<N, keyof A>, f: (a: A) => Observable<B>) =>
  (fa: Observable<A>): Observable<{ readonly [K in keyof A | N]: K extends keyof A ? A[K] : B }> =>
    _mergeChain(fa, (a) => _map(f(a), (b) => ({ ...a, [name]: b }) as never));

/** Binds a pure computation under `name` (via `map`). */
export const let_ =
  <N extends string, A, B>(name: Exclude<N, keyof A>, f: (a: A) => B) =>
  (fa: Observable<A>): Observable<{ readonly [K in keyof A | N]: K extends keyof A ? A[K] : B }> =>
    _map(fa, (a) => ({ ...a, [name]: f(a) }) as never);
