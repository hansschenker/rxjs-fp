/**
 * Concurrency vocabulary: `import { concatMap } from 'rxjs-fp/concurrency'`.
 *
 * Every operator here is the kernel's `flatMapWith` with one of its four
 * strategies, which decide what happens when a value starts nested work
 * while earlier work is still running:
 *
 *   Merge(n)  run it alongside, up to n at once; the rest wait
 *   Concat    queue it until the running work completes
 *   Switch    cancel the running work, start the new
 *   Exhaust   ignore it until the running work completes
 *
 * Built only on the kernel (checked by test/layering.test.ts), and each
 * definition confirmed against RxJS 7 by test/differential.test.ts.
 */
import { Concat, Exhaust, flatMapWith, from, identity, Merge, Switch } from '../kernel.js';
import type { Observable, ObservableInput, Operator, ValueOf } from '../kernel.js';

// The family's kernel members, so the family is importable from one place
export { flatMapWith, Merge, Concat, Switch, Exhaust, expand } from '../kernel.js';
export type { FlattenStrategy } from '../kernel.js';

type Project<A, B> = (value: A, index: number) => ObservableInput<B>;

/** Runs each value's nested work alongside the others, up to `concurrent` at once. */
export const mergeMap = <A, B>(project: Project<A, B>, concurrent: number = Number.POSITIVE_INFINITY): Operator<A, B> =>
  flatMapWith(Merge(concurrent))(project);

/** Runs each value's nested work after the previous one completes, in order. */
export const concatMap = <A, B>(project: Project<A, B>): Operator<A, B> => flatMapWith(Concat)(project);

/** Cancels the running nested work when a new value arrives. */
export const switchMap = <A, B>(project: Project<A, B>): Operator<A, B> => flatMapWith(Switch)(project);

/** Ignores new values while nested work is running. */
export const exhaustMap = <A, B>(project: Project<A, B>): Operator<A, B> => flatMapWith(Exhaust)(project);

/** `mergeMap` for a stream whose values are already Observables. */
export const mergeAll = <B>(concurrent?: number): Operator<ObservableInput<B>, B> =>
  mergeMap<ObservableInput<B>, B>(identity, concurrent);
/** `concatMap` for a stream whose values are already Observables. */
export const concatAll = <B>(): Operator<ObservableInput<B>, B> => concatMap<ObservableInput<B>, B>(identity);
/** `switchMap` for a stream whose values are already Observables. */
export const switchAll = <B>(): Operator<ObservableInput<B>, B> => switchMap<ObservableInput<B>, B>(identity);
/** `exhaustMap` for a stream whose values are already Observables. */
export const exhaustAll = <B>(): Operator<ObservableInput<B>, B> => exhaustMap<ObservableInput<B>, B>(identity);

type Sources = readonly Observable<unknown>[];
type Union<T extends Sources> = ValueOf<T[number]>;

/** Subscribes to all sources at once and interleaves their values. */
export const merge = <const T extends Sources>(...sources: T): Observable<Union<T>> =>
  mergeAll<Union<T>>()(from(sources as unknown as Observable<Union<T>>[]));

/** Subscribes to the sources one after another. */
export const concat = <const T extends Sources>(...sources: T): Observable<Union<T>> =>
  concatAll<Union<T>>()(from(sources as unknown as Observable<Union<T>>[]));

/** Operator form of `merge`: the source alongside the others. */
export const mergeWith =
  <A, const T extends Sources>(...others: T): Operator<A, A | Union<T>> =>
  (source) =>
    merge(source, ...others);

/** Operator form of `concat`: the source, then the others. */
export const concatWith =
  <A, const T extends Sources>(...others: T): Operator<A, A | Union<T>> =>
  (source) =>
    concat(source, ...others);
