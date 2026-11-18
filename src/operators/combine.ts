import { observable, subscribeChild } from '../core/observable.js';
import type { Observable, Operator, Subscription, ValueOf, ValuesOf } from '../core/types.js';
import { EMPTY } from '../creation/creation.js';

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
 *
 * Base case: completes when every source has completed.
 * Edge case, a deliberate difference from RxJS: a source that completes
 * without ever emitting completes the result at once, since no tuple can
 * be produced any more. RxJS waits for the other sources to complete.
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

