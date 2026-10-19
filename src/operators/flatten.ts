import { observable, subscribeChild } from '../core/observable.js';
import type { Operator, Subscription } from '../core/types.js';
import { from, type ObservableInput } from '../creation/creation.js';

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

      const tryComplete = (): void => {
        if (outerDone && active === 0 && queue.length === 0) sink.complete();
      };

      const runInner = (a: A): void => {
        const inner = from(project(a, index++));
        active++;
        const sub = subscribeChild(inner, sink, {
          next: (b) => sink.next(b),
          complete: () => {
            active--;
            drain();
            tryComplete();
          },
        });
        if (strategy._tag === 'Switch') current = sub;
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
              if (current !== null && !current.closed) {
                current.unsubscribe();
                active--;
              }
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
