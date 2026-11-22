/**
 * Combination vocabulary: `import { combineLatestWith } from 'rxjs-fp/combination'`.
 *
 * Built on the combination kernel (`combineLatest`, `zip`, `race`) and the
 * concurrency vocabulary (`merge`, `concat`, `mergeMap`). Several definitions
 * start from `ended(source)`: the values as `some`, then one `none` for the end.
 *
 *   takeUntil(n)          merge the notifier and the source's end (both as "stop") into the source, stop at the first "stop"
 *   skipUntil(n)          the same merge, but the notifier's first value opens a gate instead
 *   withLatestFrom(…o)    merge the others, tagged, into the ended source; a machine keeps their latest values and emits on source values
 *   sequenceEqual(b)      zip the two ended sequences, then every pair must agree
 *   forkJoin(sources)     combineLatest of each source's last value
 *   startWith / endWith   concat with the values
 *   onErrorResumeNext     concat, each source with its error replaced by EMPTY
 *   …With                 the operator form: the source joins the others
 *   …All                  collect the inner Observables with toArray, then combine them
 *
 * Built only on the kernel (checked by test/layering.test.ts), and each
 * definition confirmed against RxJS 7 by test/differential.test.ts.
 */
import { catchError, combineLatest, cont, filter, from, fromMachine, halt, isSome, map, none, race, scan, some, take, takeLast, takeWhile, zip } from '../kernel.js';
import type { Observable, ObservableInput, Operator, Option, ValueOf, ValuesOf } from '../kernel.js';
import { concat, merge, mergeMap } from './concurrency.js';
import { EMPTY } from './creation.js';
import { every, toArray } from './values.js';

// The family's kernel members, so the family is importable from one place
export { combineLatest, zip, race } from '../kernel.js';

type Sources = readonly Observable<unknown>[];
type InputValue<T extends readonly ObservableInput<unknown>[]> = T[number] extends ObservableInput<infer V> ? V : never;

/** A sequence with its end made visible: every value as `some`, then one `none`. Errors pass through. */
const ended = <A>(source: Observable<A>): Observable<Option<A>> =>
  concat(source.pipe(map((a: A): Option<A> => some(a))), from([none as Option<A>]));

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
    merge(notifier.pipe(take(1), map((): Option<A> => none)), ended(source)).pipe(
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

/** An arrival in the merged stream of `withLatestFrom`: from one of the others, or from the source (`-1`). */
type Arrival = { readonly from: number; readonly value: unknown };

/**
 * Combines each source value with the latest values of `others`; source
 * values arriving before every other has emitted are dropped. The others
 * are subscribed first, as in RxJS, so synchronous others are ready.
 *
 * The `skipUntil` shape, generalised: the others (tagged) and the ended
 * source become one stream, and a machine keeps the latest value of each
 * other. A source value is emitted with them once every slot is filled; the
 * source's `none` halts the machine, while the others' ends do not matter.
 */
export const withLatestFrom =
  <A, const T extends Sources>(...others: T): Operator<A, [A, ...ValuesOf<T>]> =>
  (source) =>
    merge(
      ...others.map((other, i) => other.pipe(map((value): Option<Arrival> => some({ from: i, value })))),
      ended(source.pipe(map((value: A): Arrival => ({ from: -1, value })))),
    ).pipe(
      fromMachine<ReadonlyArray<Option<unknown>>, Option<Arrival>, [A, ...ValuesOf<T>]>({
        init: () => others.map((): Option<unknown> => none),
        step: (latest, arrival) => {
          if (!isSome(arrival)) return halt(latest);
          const { from: i, value } = arrival.value;
          if (i !== -1) return cont(latest.map((o, j) => (j === i ? some(value) : o)));
          if (!latest.every(isSome)) return cont(latest);
          return cont(latest, [[value, ...latest.map((o) => (o as { readonly value: unknown }).value)] as [A, ...ValuesOf<T>]]);
        },
      }),
    );

/**
 * Emits `true` if the source and `compareTo` emit equal sequences (by
 * `comparator`, default `===`), `false` as soon as they differ: `zip` the two
 * ended sequences and ask `every` pair to agree, `none` with `none` included.
 * A value paired with the other side's end is a difference, so sequences of
 * different length are unequal (RxJS 7.8.2 can answer `true` there: README).
 */
export const sequenceEqual =
  <A>(compareTo: ObservableInput<A>, comparator: (a: A, b: A) => boolean = (a, b) => a === b): Operator<A, boolean> =>
  (source) =>
    zip([ended(source), ended(from(compareTo))]).pipe(
      every(([x, y]) => (isSome(x) && isSome(y) ? comparator(x.value, y.value) : !isSome(x) && !isSome(y))),
    );

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

const OPEN: unique symbol = Symbol('rxjs-fp/open');
type Gated<A> = typeof OPEN | Option<A>;
type Gate<A> = { readonly open: boolean; readonly out: Option<A> };

/**
 * Drops source values until `notifier` emits, then mirrors the source. The
 * mirror image of `takeUntil`: the notifier's first value opens a gate
 * instead of stopping, and the source's end still stops.
 */
export const skipUntil =
  <A>(notifier: Observable<unknown>): Operator<A, A> =>
  (source) =>
    merge(notifier.pipe(take(1), map((): Gated<A> => OPEN)), ended(source)).pipe(
      takeWhile((x: Gated<A>) => x === OPEN || isSome(x)),
      scan<Gated<A>, Gate<A>>((gate, x) => (x === OPEN ? { open: true, out: none } : { open: gate.open, out: gate.open ? x : none }), {
        open: false,
        out: none,
      }),
      filter((gate: Gate<A>) => isSome(gate.out)),
      map((gate) => (gate.out as { readonly value: A }).value),
    );
