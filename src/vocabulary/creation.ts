/**
 * Creation vocabulary: `import { of } from 'rxjs-fp/creation'`.
 *
 *   of(...values)        from(values)
 *   EMPTY                of()
 *   throwError(f)        EMPTY.pipe(throwIfEmpty(f))
 *   range(start, count)  unfold: emit n, continue with n + 1, until start + count
 *   interval(ms)         timer(ms, ms)
 *   iif(test, a, b)      defer(() => test() ? a : b)
 *   fromEvent(t, type)   fromEventPattern with add/remove listener on t
 *   generate(…)          from (or scheduled) over the iterable of states
 *
 * Built only on the kernel (checked by test/layering.test.ts), and each
 * definition confirmed against RxJS 7 by test/differential.test.ts.
 */
import { asyncScheduler, defer, from, fromEventPattern, NEVER, none, scheduled, some, throwIfEmpty, timer, unfold } from '../kernel.js';
import type { DomEventTargetLike, NodeEventEmitterLike, Observable, ObservableInput, Scheduler } from '../kernel.js';

// The family's kernel members, so the family is importable from one place
export { observable, from, defer, NEVER, timer, unfold, using } from '../kernel.js';
export { fromIterable, fromArrayLike, fromPromise, fromAsyncIterable, fromSubscribable, fromEventPattern, scheduled } from '../kernel.js';
export { animationFrames, bindCallback, bindNodeCallback } from '../kernel.js';

/** Emits the given values synchronously, then completes. */
export const of = <T extends readonly unknown[]>(...values: T): Observable<T[number]> => from<T[number]>(values);

/** Completes immediately: `of()` with no values. */
export const EMPTY: Observable<never> = of();

/**
 * Errors immediately with `factory()`, evaluated once per subscription: the
 * empty sequence with its completion replaced by an error. A factory that
 * throws is itself the error, as in RxJS.
 */
export const throwError = (factory: () => unknown): Observable<never> => EMPTY.pipe(throwIfEmpty(factory));

/** Emits `count` consecutive integers starting at `start`. */
export const range = (start: number, count: number): Observable<number> =>
  unfold(start, (n) => (n < start + count ? some([n, n + 1] as const) : none));

/** Emits 0, 1, 2, … every `period` ms. */
export const interval = (period: number, scheduler: Scheduler = asyncScheduler): Observable<number> =>
  timer(period, period, scheduler);

/** Chooses between two Observables at subscription time. */
export const iif = <A, B>(condition: () => boolean, onTrue: ObservableInput<A>, onFalse: ObservableInput<B>): Observable<A | B> =>
  defer<A | B>(() => (condition() ? onTrue : onFalse));

/** Deprecated alias of `EMPTY`, kept for RxJS compatibility. */
export const empty = (): Observable<never> => EMPTY;

/** Deprecated alias of `NEVER`, kept for RxJS compatibility. */
export const never = (): Observable<never> => NEVER;

/**
 * Emits `initial`, then `iterate(state)` repeatedly, while `condition(state)`
 * holds. With a scheduler, each state is emitted in its own scheduled task.
 */
export const generate = <S>(initial: S, condition: (state: S) => boolean, iterate: (state: S) => S, scheduler?: Scheduler): Observable<S> => {
  const states: Iterable<S> = {
    [Symbol.iterator]: () => {
      // per-iteration state, local to one run of the iterator
      let state = initial;
      let started = false;
      return {
        next: (): IteratorResult<S> => {
          if (started) state = iterate(state);
          started = true;
          if (!condition(state)) return { done: true, value: undefined };
          return { done: false, value: state };
        },
      };
    },
  };
  return scheduler ? scheduled(states, scheduler) : from(states);
};

type Listener<E> = (event: E) => void;

/** Emits the events of a DOM `EventTarget` or a Node `EventEmitter`. */
export function fromEvent<K extends keyof HTMLElementEventMap>(target: HTMLElement, type: K, options?: boolean | AddEventListenerOptions): Observable<HTMLElementEventMap[K]>;
export function fromEvent<K extends keyof WindowEventMap>(target: Window, type: K, options?: boolean | AddEventListenerOptions): Observable<WindowEventMap[K]>;
export function fromEvent<K extends keyof DocumentEventMap>(target: Document, type: K, options?: boolean | AddEventListenerOptions): Observable<DocumentEventMap[K]>;
export function fromEvent<E>(target: DomEventTargetLike<E>, type: string, options?: boolean | AddEventListenerOptions): Observable<E>;
export function fromEvent<E>(target: NodeEventEmitterLike<E>, type: string | symbol): Observable<E>;
export function fromEvent<E>(
  target: DomEventTargetLike<E> | NodeEventEmitterLike<E>,
  type: string | symbol,
  options?: boolean | AddEventListenerOptions,
): Observable<E> {
  // the listener passes on only the first argument, and is the signal used to remove it
  const add = (emit: (e: E) => void): Listener<E> => {
    const listener: Listener<E> = (e) => emit(e);
    if ('addEventListener' in target) target.addEventListener(type as string, listener, options);
    else target.on(type, listener);
    return listener;
  };
  const remove = (_emit: unknown, listener: unknown): void => {
    if ('addEventListener' in target) target.removeEventListener(type as string, listener as Listener<E>, options);
    else target.off(type, listener as Listener<E>);
  };
  return fromEventPattern<E>(add, remove);
}
