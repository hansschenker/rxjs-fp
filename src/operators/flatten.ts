import { observable, subscribeChild } from '../core/observable.js';
import type { Operator, Subscription } from '../core/types.js';
import { defer, from, type ObservableInput } from '../creation/creation.js';
import { tap } from './transform.js';

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

export const Merge = (concurrent: number = Number.POSITIVE_INFINITY): FlattenStrategy =>
  Object.freeze({ _tag: 'Merge', concurrent: Math.max(1, concurrent) });
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

/** Projects each value to an Observable and merges them (optionally bounded). */
export const mergeMap = <A, B>(
  project: (value: A, index: number) => ObservableInput<B>,
  concurrent: number = Number.POSITIVE_INFINITY,
): Operator<A, B> => flatMapWith(Merge(concurrent))(project);

/** Projects each value to an Observable and concatenates them in order. */
export const concatMap = <A, B>(project: (value: A, index: number) => ObservableInput<B>): Operator<A, B> =>
  flatMapWith(Concat)(project);

/** Projects each value to an Observable, cancelling the previous one. */
export const switchMap = <A, B>(project: (value: A, index: number) => ObservableInput<B>): Operator<A, B> =>
  flatMapWith(Switch)(project);

/** Projects each value to an Observable, ignoring values while one is active. */
export const exhaustMap = <A, B>(project: (value: A, index: number) => ObservableInput<B>): Operator<A, B> =>
  flatMapWith(Exhaust)(project);

const id = <X>(x: X): X => x;

export const mergeAll = <B>(concurrent?: number): Operator<ObservableInput<B>, B> =>
  mergeMap<ObservableInput<B>, B>(id, concurrent);
export const concatAll = <B>(): Operator<ObservableInput<B>, B> => concatMap<ObservableInput<B>, B>(id);
export const switchAll = <B>(): Operator<ObservableInput<B>, B> => switchMap<ObservableInput<B>, B>(id);
export const exhaustAll = <B>(): Operator<ObservableInput<B>, B> => exhaustMap<ObservableInput<B>, B>(id);

/**
 * Recursively projects every value: each emitted value is also fed back
 * into `project`. Emits all results; completes when nothing is left.
 */
export const expand =
  <A>(project: (value: A, index: number) => ObservableInput<A>, concurrent: number = Number.POSITIVE_INFINITY): Operator<A, A> =>
  (source) =>
    observable<A>((sink) => {
      const limit = Math.max(1, concurrent);
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

/**
 * `scan` whose accumulator returns an Observable, flattened with `strategy`.
 * Every inner value is emitted and becomes the new accumulated state.
 * The state is per subscription (created in `defer`).
 */
const scanWith =
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

/** `scan` with an Observable-returning accumulator, merged (up to `concurrent` at once). */
export const mergeScan = <A, S>(
  accumulator: (acc: S, value: A, index: number) => ObservableInput<S>,
  seed: S,
  concurrent: number = Number.POSITIVE_INFINITY,
): Operator<A, S> => scanWith(Merge(concurrent))(accumulator, seed);

/** `scan` with an Observable-returning accumulator; a new value cancels the running inner. */
export const switchScan = <A, S>(accumulator: (acc: S, value: A, index: number) => ObservableInput<S>, seed: S): Operator<A, S> =>
  scanWith(Switch)(accumulator, seed);
