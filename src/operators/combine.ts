import { observable, subscribeChild } from '../core/observable.js';
import type { Observable, Operator, Subscription, ValueOf, ValuesOf } from '../core/types.js';
import { EMPTY, from, of } from '../creation/creation.js';
import { concatAll, mergeAll } from './flatten.js';

/** Subscribes to all sources at once and interleaves their values. */
export const merge = <const T extends readonly Observable<unknown>[]>(...sources: T): Observable<ValueOf<T[number]>> =>
  mergeAll<ValueOf<T[number]>>()(from(sources as unknown as Observable<ValueOf<T[number]>>[]));

/** Subscribes to the sources one after another. */
export const concat = <const T extends readonly Observable<unknown>[]>(...sources: T): Observable<ValueOf<T[number]>> =>
  concatAll<ValueOf<T[number]>>()(from(sources as unknown as Observable<ValueOf<T[number]>>[]));

/** Operator form of `merge`. */
export const mergeWith =
  <A, const T extends readonly Observable<unknown>[]>(...others: T): Operator<A, A | ValueOf<T[number]>> =>
  (source) =>
    merge(source, ...others);

/** Operator form of `concat`. */
export const concatWith =
  <A, const T extends readonly Observable<unknown>[]>(...others: T): Operator<A, A | ValueOf<T[number]>> =>
  (source) =>
    concat(source, ...others);

/** Emits the given values first. */
export const startWith =
  <A, const V extends readonly unknown[]>(...values: V): Operator<A, A | V[number]> =>
  (source) =>
    concat(of(...values), source);

/** Emits the given values after the source completes. */
export const endWith =
  <A, const V extends readonly unknown[]>(...values: V): Operator<A, A | V[number]> =>
  (source) =>
    concat(source, of(...values));

const runAll = <T extends readonly Observable<unknown>[]>(
  sources: T,
  sink: { readonly closed: boolean },
  subscribeOne: (source: Observable<unknown>, i: number) => Subscription,
): (() => void) => {
  const subs: Subscription[] = [];
  for (let i = 0; i < sources.length && !sink.closed; i++) subs.push(subscribeOne(sources[i] as Observable<unknown>, i));
  return () => subs.forEach((s) => s.unsubscribe());
};

/**
 * Emits a tuple of the latest values whenever any source emits, once every
 * source has emitted at least once. The result type is the tuple of the
 * sources' value types.
 */
export const combineLatest = <const T extends readonly Observable<unknown>[]>(sources: T): Observable<ValuesOf<T>> =>
  sources.length === 0
    ? EMPTY
    : observable<ValuesOf<T>>((sink) => {
        const n = sources.length;
        const values: unknown[] = new Array(n);
        const has: boolean[] = new Array(n).fill(false);
        let ready = 0;
        let completed = 0;
        return runAll(sources, sink, (src, i) =>
          subscribeChild(src, sink, {
            next: (v) => {
              if (!has[i]) {
                has[i] = true;
                ready++;
              }
              values[i] = v;
              if (ready === n) sink.next(values.slice() as ValuesOf<T>);
            },
            complete: () => {
              completed++;
              if (completed === n || !has[i]) sink.complete();
            },
          }),
        );
      });

/** Pairs values by index: the k-th tuple holds the k-th value of every source. */
export const zip = <const T extends readonly Observable<unknown>[]>(sources: T): Observable<ValuesOf<T>> =>
  sources.length === 0
    ? EMPTY
    : observable<ValuesOf<T>>((sink) => {
        const buffers: unknown[][] = sources.map(() => []);
        const done: boolean[] = sources.map(() => false);
        return runAll(sources, sink, (src, i) =>
          subscribeChild(src, sink, {
            next: (v) => {
              buffers[i]!.push(v);
              if (buffers.every((b) => b.length > 0)) {
                sink.next(buffers.map((b) => b.shift()) as ValuesOf<T>);
                if (buffers.some((b, j) => b.length === 0 && done[j])) sink.complete();
              }
            },
            complete: () => {
              done[i] = true;
              if (buffers[i]!.length === 0) sink.complete();
            },
          }),
        );
      });

/** Waits for all sources to complete and emits the tuple of their last values. */
export const forkJoin = <const T extends readonly Observable<unknown>[]>(sources: T): Observable<ValuesOf<T>> =>
  sources.length === 0
    ? EMPTY
    : observable<ValuesOf<T>>((sink) => {
        const n = sources.length;
        const values: unknown[] = new Array(n);
        const has: boolean[] = new Array(n).fill(false);
        let completed = 0;
        return runAll(sources, sink, (src, i) =>
          subscribeChild(src, sink, {
            next: (v) => {
              has[i] = true;
              values[i] = v;
            },
            complete: () => {
              if (!has[i]) {
                sink.complete();
                return;
              }
              completed++;
              if (completed === n) {
                sink.next(values as ValuesOf<T>);
                sink.complete();
              }
            },
          }),
        );
      });

/** Mirrors whichever source notifies first; the others are unsubscribed. */
export const race = <const T extends readonly Observable<unknown>[]>(...sources: T): Observable<ValueOf<T[number]>> =>
  observable<ValueOf<T[number]>>((sink) => {
    let winner = -1;
    const subs: Subscription[] = [];
    const win = (i: number): boolean => {
      if (winner === -1) {
        winner = i;
        subs.forEach((s, j) => j !== i && s.unsubscribe());
      }
      return winner === i;
    };
    for (let i = 0; i < sources.length && winner === -1 && !sink.closed; i++) {
      subs[i] = subscribeChild(sources[i] as Observable<ValueOf<T[number]>>, sink, {
        next: (v) => win(i) && sink.next(v),
        error: (e) => win(i) && sink.error(e),
        complete: () => win(i) && sink.complete(),
      });
    }
    return () => subs.forEach((s) => s.unsubscribe());
  });

/**
 * Combines each source value with the latest values of `others`. Source
 * values arriving before every other has emitted are dropped.
 */
export const withLatestFrom =
  <A, const T extends readonly Observable<unknown>[]>(...others: T): Operator<A, [A, ...ValuesOf<T>]> =>
  (source) =>
    observable<[A, ...ValuesOf<T>]>((sink) => {
      const n = others.length;
      const values: unknown[] = new Array(n);
      const has: boolean[] = new Array(n).fill(false);
      let ready = 0;
      const stopOthers = runAll(others, sink, (src, i) =>
        subscribeChild(src, sink, {
          next: (v) => {
            if (!has[i]) {
              has[i] = true;
              ready++;
            }
            values[i] = v;
          },
          complete: () => {},
        }),
      );
      const main = subscribeChild(source, sink, {
        next: (a) => {
          if (ready === n) sink.next([a, ...values] as [A, ...ValuesOf<T>]);
        },
      });
      return () => {
        stopOthers();
        main.unsubscribe();
      };
    });

/** Operator form of `combineLatest`: `[source, ...others]`. */
export const combineLatestWith =
  <A, const T extends readonly Observable<unknown>[]>(...others: T): Operator<A, [A, ...ValuesOf<T>]> =>
  (source) =>
    combineLatest([source, ...others]) as unknown as Observable<[A, ...ValuesOf<T>]>;

/** Operator form of `zip`: `[source, ...others]`. */
export const zipWith =
  <A, const T extends readonly Observable<unknown>[]>(...others: T): Operator<A, [A, ...ValuesOf<T>]> =>
  (source) =>
    zip([source, ...others]) as unknown as Observable<[A, ...ValuesOf<T>]>;
