import type { Observable, Operator } from '../core/types.js';
import { EMPTY, of, throwError } from '../creation/creation.js';
import { map } from '../operators/transform.js';
import { filter as filterOp, filterMap as filterMapOp } from '../operators/filtering.js';
import { concatMap, mergeMap } from '../operators/flatten.js';
import { combineLatest, concat, merge } from '../operators/combine.js';
import { catchError } from '../operators/error.js';
import { isLeft, isRight, left, right, type Either } from './either.js';
import { isNone, none, some, type Option } from './option.js';
import type * as TC from './typeclasses.js';
import type { Separated } from './typeclasses.js';

/**
 * Lawful typeclass instances for Observable.
 *
 * Design decisions (each one fixes an inconsistency found in the earlier
 * rxjs-source-v7-fp experiment):
 *  - `ap` is *derived from* `chain`, so Monad and Apply agree. A
 *    `combineLatest`-based ap is useful, but it is not the Monad's ap, so
 *    it is exported separately as `apLatest` and makes no law claims.
 *  - `alt` is `merge` and `zero` is `EMPTY`. They satisfy the Alt/Plus laws
 *    and agree with the merge Monoid. Error recovery (`catchError`) is not
 *    `alt`; it is exported as `orElse`.
 */

export const URI = 'Observable' as const;
export type URI = typeof URI;

// Typeclass functions are unary: operators pass an index as a second
// argument, which must not leak into lawful instances.
const _map = <A, B>(fa: Observable<A>, f: (a: A) => B): Observable<B> => map((a: A) => f(a))(fa);
const _of = <A>(a: A): Observable<A> => of(a);
const _mergeChain = <A, B>(fa: Observable<A>, f: (a: A) => Observable<B>): Observable<B> => mergeMap((a: A) => f(a))(fa);
const _concatChain = <A, B>(fa: Observable<A>, f: (a: A) => Observable<B>): Observable<B> => concatMap((a: A) => f(a))(fa);
const apFrom =
  (chainF: <A, B>(fa: Observable<A>, f: (a: A) => Observable<B>) => Observable<B>) =>
  <A, B>(fab: Observable<(a: A) => B>, fa: Observable<A>): Observable<B> =>
    chainF(fab, (f) => _map(fa, f));
const _alt = <A>(fx: Observable<A>, fy: () => Observable<A>): Observable<A> => merge(fx, fy());
const _zero = <A>(): Observable<A> => EMPTY;
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

/** The default Monad: `chain` = `mergeMap`. */
export const MonadMerge: TC.Monad<URI> = { URI, map: _map, of: _of, chain: _mergeChain, ap: apFrom(_mergeChain) };

/** A second lawful Monad: `chain` = `concatMap` (sequential). */
export const MonadConcat: TC.Monad<URI> = { URI, map: _map, of: _of, chain: _concatChain, ap: apFrom(_concatChain) };

export const ObservablePlus: TC.Plus<URI> = { URI, map: _map, alt: _alt, zero: _zero };

export const ObservableAlternative: TC.Alternative<URI> = { ...MonadMerge, alt: _alt, zero: _zero };

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

/** Apply, derived from chain. `pipe(fab, ap(fa))` */
export const ap =
  <A>(fa: Observable<A>) =>
  <B>(fab: Observable<(a: A) => B>): Observable<B> =>
    MonadMerge.ap(fab, fa);

/**
 * A "latest values" apply built on `combineLatest`. Handy for UI state,
 * but it is NOT the Monad's `ap` and makes no law claims.
 */
export const apLatest =
  <A>(fa: Observable<A>) =>
  <B>(fab: Observable<(a: A) => B>): Observable<B> =>
    map(([f, a]: [(a: A) => B, A]) => f(a))(combineLatest([fab, fa] as const));

/** `alt` as an operator: merges in the alternative. */
export const alt =
  <A>(that: () => Observable<A>): Operator<A, A> =>
  (fa) =>
    _alt(fa, that);

/** Error recovery: on error, continue with `that(err)`. Not part of Alt. */
export const orElse = <A, B>(that: (err: unknown) => Observable<B>): Operator<A, A | B> => catchError((e: unknown) => that(e));

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
