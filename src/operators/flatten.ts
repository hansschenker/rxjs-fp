import { observable, subscribeChild } from '../core/observable.js';
import type { Operator, Subscription } from '../core/types.js';
import { defer, from, type ObservableInput } from '../creation/creation.js';
import { tap } from './transform.js';
import { positiveInteger } from '../policies/core.js';

/**
 * Flattening strategies as data.
 *
 * mergeMap, concatMap, switchMap and exhaustMap differ only in one policy:
 * what to do with a new outer value while inner Observables are running.
 * Here that policy is a plain value, interpreted by one function.
 *
 *   Merge(n)  run up to n inners concurrently, queue the rest
 *   Concat    Merge(1): run inners one after another
 *   Switch    cancel the running inner, start the new one
 *   Exhaust   ignore new values while an inner is running
 */
export type FlattenStrategy =
  | { readonly _tag: 'Merge'; readonly concurrent: number }
  | { readonly _tag: 'Switch' }
  | { readonly _tag: 'Exhaust' };

/**
 * A concurrency limit: a positive integer, or none when omitted. Anything
 * else throws at construction — an explicit `Infinity` included, since the
 * 0.2.0 policies name an unlimited bound by leaving it out, never by a number.
 */
const concurrency = (value?: number): number => (value === undefined ? Number.POSITIVE_INFINITY : positiveInteger(value));

export const Merge = (concurrent?: number): FlattenStrategy =>
  Object.freeze({ _tag: 'Merge', concurrent: concurrency(concurrent) });
export const Concat: FlattenStrategy = Merge(1);
export const Switch: FlattenStrategy = Object.freeze({ _tag: 'Switch' });
export const Exhaust: FlattenStrategy = Object.freeze({ _tag: 'Exhaust' });

/** The one interpreter behind every higher-order flattening operator. */
export const flatMapWith =
  (strategy: FlattenStrategy) =>
  <A, B>(project: (value: A, index: number) => ObservableInput<B>): Operator<A, B> =>
  (source) =>
    observable<B>((sink) => {
      const concurrent = strategy._tag === 'Merge' ? strategy.concurrent : 1;
      const queue: A[] = [];
      let active = 0;
      let index = 0;
      let outerDone = false;
      let current: Subscription | null = null;
      // Switch: only the newest inner may speak. A generation counter makes
      // this hold even when a new outer value arrives re-entrantly while an
      // inner is still being subscribed.
      let generation = 0;

      const tryComplete = (): void => {
        if (outerDone && active === 0 && queue.length === 0) sink.complete();
      };

      const runInner = (a: A): void => {
        const inner = from(project(a, index++));
        const gen = ++generation;
        const isCurrent = (): boolean => strategy._tag !== 'Switch' || gen === generation;
        active++;
        const sub = subscribeChild(inner, sink, {
          next: (b) => {
            if (isCurrent()) sink.next(b);
          },
          complete: () => {
            if (!isCurrent()) return;
            active--;
            drain();
            tryComplete();
          },
        });
        if (strategy._tag === 'Switch') {
          if (isCurrent()) current = sub;
          else sub.unsubscribe(); // superseded while subscribing
        }
      };

      const drain = (): void => {
        while (queue.length > 0 && active < concurrent && !sink.closed) runInner(queue.shift() as A);
      };

      return subscribeChild(source, sink, {
        next: (a) => {
          switch (strategy._tag) {
            case 'Merge':
              if (active < concurrent) runInner(a);
              else queue.push(a);
              return;
            case 'Switch':
              current?.unsubscribe();
              current = null;
              active = 0; // the previous inner is cancelled, finished or superseded
              runInner(a);
              return;
            case 'Exhaust':
              if (active === 0) runInner(a);
              return;
          }
        },
        complete: () => {
          outerDone = true;
          tryComplete();
        },
      });
    });

/**
 * Recursively projects every value: each emitted value is also fed back
 * into `project`. Emits all results; completes when nothing is left.
 */
export const expand = <A>(project: (value: A, index: number) => ObservableInput<A>, concurrent?: number): Operator<A, A> => {
  const limit = concurrency(concurrent);
  return (source) =>
    observable<A>((sink) => {
      const queue: A[] = [];
      let active = 0;
      let index = 0;
      let outerDone = false;

      const tryComplete = (): void => {
        if (outerDone && active === 0 && queue.length === 0) sink.complete();
      };
      const emitAndExpand = (a: A): void => {
        sink.next(a);
        if (active < limit) runInner(a);
        else queue.push(a);
      };
      const runInner = (a: A): void => {
        active++;
        subscribeChild(from(project(a, index++)), sink, {
          next: emitAndExpand,
          complete: () => {
            active--;
            while (queue.length > 0 && active < limit && !sink.closed) runInner(queue.shift() as A);
            tryComplete();
          },
        });
      };
      return subscribeChild(source, sink, {
        next: emitAndExpand,
        complete: () => {
          outerDone = true;
          tryComplete();
        },
      });
    });
};

/**
 * `scan` whose accumulator returns an Observable, flattened with `strategy`;
 * each emitted state is the accumulator for the next value. The concurrency
 * twin of `flatMapWith`: used through `mergeScan` and `switchScan`.
 */
export const scanWith =
  (strategy: FlattenStrategy) =>
  <A, S>(accumulator: (acc: S, value: A, index: number) => ObservableInput<S>, seed: S): Operator<A, S> =>
  (source) =>
    defer(() => {
      let state = seed;
      return flatMapWith(strategy)((a: A, i: number) => accumulator(state, a, i))(source).pipe(
        tap((s: S) => {
          state = s;
        }),
      );
    });
