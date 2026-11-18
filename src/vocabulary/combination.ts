/**
 * Combination vocabulary: `import { combineLatestWith } from 'rxjs-fp/combination'`.
 *
 * Built on the combination kernel (`combineLatest`, `zip`, `withLatestFrom`,
 * `race`) and the concurrency vocabulary (`merge`, `concat`, `mergeMap`).
 *
 *   takeUntil(n)          merge the notifier and the source's end (both as "stop") into the source, stop at the first "stop"
 *   forkJoin(sources)     combineLatest of each source's last value
 *   startWith / endWith   concat with the values
 *   onErrorResumeNext     concat, each source with its error replaced by EMPTY
 *   …With                 the operator form: the source joins the others
 *   …All                  collect the inner Observables with toArray, then combine them
 *
 * Built only on the kernel (checked by test/layering.test.ts), and each
 * definition confirmed against RxJS 7 by test/differential.test.ts.
 */
import { catchError, combineLatest, EMPTY, filter, from, isSome, map, none, race, some, take, takeLast, takeWhile, zip } from '../kernel.js';
import type { Observable, ObservableInput, Operator, Option, ValueOf, ValuesOf } from '../kernel.js';
import { concat, merge, mergeMap } from './concurrency.js';
import { toArray } from './values.js';

// The family's kernel members, so the family is importable from one place
export { combineLatest, zip, withLatestFrom, race } from '../kernel.js';

type Sources = readonly Observable<unknown>[];
type InputValue<T extends readonly ObservableInput<unknown>[]> = T[number] extends ObservableInput<infer V> ? V : never;

/**
 * Mirrors the source until `notifier` emits, then completes. The notifier is
 * subscribed first; if it emits at once, the source is never subscribed.
 *
 * Both inputs become one stream of `Option`s: the source's values as `some`,
 * and a `none` ("stop") from whichever comes first, the notifier's first
 * value or the source's completion.
 */
export const takeUntil =
  <A>(notifier: Observable<unknown>): Operator<A, A> =>
  (source) =>
    merge(
      notifier.pipe(take(1), map((): Option<A> => none)),
      concat(source.pipe(map((a: A) => some(a))), from([none as Option<A>])),
    ).pipe(
      takeWhile(isSome),
      map((o) => (o as { readonly value: A }).value),
    );

/** Waits for all sources to complete, then emits their last values together. Completes empty if one is empty. */
export const forkJoin = <const T extends Sources>(sources: T): Observable<ValuesOf<T>> =>
  sources.length === 0
    ? EMPTY
    : (combineLatest(sources.map((s) => s.pipe(takeLast(1)))) as unknown as Observable<ValuesOf<T>>);

/** Emits the given values first. */
export const startWith =
  <A, const V extends readonly unknown[]>(...values: V): Operator<A, A | V[number]> =>
  (source) =>
    concat(from<V[number]>(values), source);

/** Emits the given values after the source completes. */
export const endWith =
  <A, const V extends readonly unknown[]>(...values: V): Operator<A, A | V[number]> =>
  (source) =>
    concat(source, from<V[number]>(values));

/** Subscribes to the sources one after another; an error just moves on to the next. */
export const onErrorResumeNext = <const T extends readonly ObservableInput<unknown>[]>(...sources: T): Observable<InputValue<T>> =>
  concat(...sources.map((s) => from(s as ObservableInput<InputValue<T>>).pipe(catchError(() => EMPTY))));

/** Operator form of `combineLatest`: `[source, ...others]`. */
export const combineLatestWith =
  <A, const T extends Sources>(...others: T): Operator<A, [A, ...ValuesOf<T>]> =>
  (source) =>
    combineLatest([source, ...others]) as unknown as Observable<[A, ...ValuesOf<T>]>;

/** Operator form of `zip`: `[source, ...others]`. */
export const zipWith =
  <A, const T extends Sources>(...others: T): Operator<A, [A, ...ValuesOf<T>]> =>
  (source) =>
    zip([source, ...others]) as unknown as Observable<[A, ...ValuesOf<T>]>;

/** Operator form of `race`: mirrors whichever of the source and `others` notifies first. */
export const raceWith =
  <A, const T extends Sources>(...others: T): Operator<A, A | ValueOf<T[number]>> =>
  (source) =>
    race(source, ...others);

/** Operator form of `onErrorResumeNext`. */
export const onErrorResumeNextWith =
  <A, const T extends readonly ObservableInput<unknown>[]>(...sources: T): Operator<A, A | InputValue<T>> =>
  (source) =>
    onErrorResumeNext(source, ...sources) as Observable<A | InputValue<T>>;

/** Collects the source's inner Observables until it completes, then subscribes to all of them with `combineLatest`. */
export const combineLatestAll = <A>(): Operator<ObservableInput<A>, A[]> =>
  (source) =>
    source.pipe(
      toArray(),
      mergeMap((inputs: ObservableInput<A>[]) => combineLatest(inputs.map((i) => from(i))) as Observable<A[]>),
    );

/** Collects the source's inner Observables until it completes, then `zip`s them. */
export const zipAll = <A>(): Operator<ObservableInput<A>, A[]> =>
  (source) =>
    source.pipe(
      toArray(),
      mergeMap((inputs: ObservableInput<A>[]) => zip(inputs.map((i) => from(i))) as Observable<A[]>),
    );

/** Splits a source into the values that satisfy `predicate` and those that do not (two cold Observables). */
export function partition<A, B extends A>(
  source: ObservableInput<A>,
  predicate: (value: A, index: number) => value is B,
): [Observable<B>, Observable<Exclude<A, B>>];
export function partition<A>(source: ObservableInput<A>, predicate: (value: A, index: number) => boolean): [Observable<A>, Observable<A>];
export function partition<A>(source: ObservableInput<A>, predicate: (value: A, index: number) => boolean): [Observable<A>, Observable<A>] {
  const s = from(source);
  return [s.pipe(filter(predicate)), s.pipe(filter((a: A, i: number) => !predicate(a, i)))];
}
