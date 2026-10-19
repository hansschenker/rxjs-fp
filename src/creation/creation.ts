import { observable, isObservable, subscribeChild } from '../core/observable.js';
import type { Observable, Subscribable } from '../core/types.js';
import { createSubscription } from '../core/subscription.js';
import { asyncScheduler, type Scheduler } from '../scheduler/scheduler.js';
import { isSome, type Option } from '../fp/option.js';

/** Everything `from` understands. */
export type ObservableInput<A> =
  | Observable<A>
  | Subscribable<A>
  | PromiseLike<A>
  | Iterable<A>
  | AsyncIterable<A>
  | ArrayLike<A>;

/** Emits the given values synchronously, then completes. */
export const of = <T extends readonly unknown[]>(...values: T): Observable<T[number]> =>
  observable((sink) => {
    for (const v of values) {
      if (sink.closed) return;
      sink.next(v);
    }
    sink.complete();
  });

/** Completes immediately. */
export const EMPTY: Observable<never> = observable((sink) => sink.complete());
export const empty = (): Observable<never> => EMPTY;

/** Never emits, never terminates. */
export const NEVER: Observable<never> = observable(() => {});
export const never = (): Observable<never> => NEVER;

/** Errors immediately with the value produced by `factory` (evaluated per subscription). */
export const throwError = (factory: () => unknown): Observable<never> =>
  observable((sink) => sink.error(factory()));

/** Creates the Observable lazily, once per subscription. */
export const defer = <A>(factory: () => ObservableInput<A>): Observable<A> =>
  observable((sink) => subscribeChild(from(factory()), sink));

export const fromIterable = <A>(iterable: Iterable<A>): Observable<A> =>
  observable((sink) => {
    const it = iterable[Symbol.iterator]();
    let exhausted = false;
    try {
      for (;;) {
        if (sink.closed) return;
        const r = it.next();
        if (r.done) {
          exhausted = true;
          break;
        }
        sink.next(r.value);
      }
      sink.complete();
    } finally {
      // only close the iterator when we stopped early (like a `for…of` break)
      if (!exhausted) it.return?.();
    }
  });

export const fromArrayLike = <A>(xs: ArrayLike<A>): Observable<A> =>
  observable((sink) => {
    for (let i = 0; i < xs.length && !sink.closed; i++) sink.next(xs[i] as A);
    sink.complete();
  });

export const fromPromise = <A>(p: PromiseLike<A>): Observable<A> =>
  observable((sink) => {
    p.then(
      (v) => {
        sink.next(v);
        sink.complete();
      },
      (e) => sink.error(e),
    );
  });

export const fromAsyncIterable = <A>(xs: AsyncIterable<A>): Observable<A> =>
  observable((sink) => {
    const it = xs[Symbol.asyncIterator]();
    let exhausted = false;
    const pull = (): void => {
      if (sink.closed) return;
      let step: Promise<IteratorResult<A>>;
      try {
        step = Promise.resolve(it.next());
      } catch (e) {
        sink.error(e);
        return;
      }
      step.then(
        (r) => {
          if (r.done) {
            exhausted = true;
            sink.complete();
          } else {
            sink.next(r.value);
            pull();
          }
        },
        (e) => {
          exhausted = true;
          sink.error(e);
        },
      );
    };
    pull();
    return () => {
      if (exhausted) return;
      try {
        Promise.resolve(it.return?.()).catch(() => {});
      } catch {
        /* a throwing return() must not break teardown */
      }
    };
  });

/**
 * Adopts any foreign Observable implementation (RxJS, zen-observable, …).
 *
 * The foreign side receives a subscriber that looks like an RxJS Subscriber
 * (`closed`, `add`, `remove`, `unsubscribe`), so a synchronous foreign
 * source sees our downstream close and stops producing.
 */
export const fromSubscribable = <A>(s: Subscribable<A>): Observable<A> =>
  observable((sink) => {
    const scope = createSubscription();
    sink.add(scope.unsubscribe);
    const removers = new Map<unknown, () => void>();
    const subscriber = {
      next: (v: A) => sink.next(v),
      error: (e: unknown) => sink.error(e),
      complete: () => sink.complete(),
      get closed() {
        return sink.closed || scope.closed;
      },
      add: (t: unknown) => {
        if (!t) return;
        const f = typeof t === 'function' ? (t as () => void) : () => (t as { unsubscribe(): void }).unsubscribe();
        removers.set(t, scope.add(f));
      },
      remove: (t: unknown) => {
        removers.get(t)?.();
        removers.delete(t);
      },
      unsubscribe: () => scope.unsubscribe(),
    };
    const sub = s.subscribe(subscriber);
    scope.add(() => sub.unsubscribe());
    return scope.unsubscribe;
  });

const isPromiseLike = (x: unknown): x is PromiseLike<unknown> =>
  typeof x === 'object' && x !== null && typeof (x as PromiseLike<unknown>).then === 'function';
const isAsyncIterable = (x: unknown): x is AsyncIterable<unknown> =>
  typeof x === 'object' && x !== null && Symbol.asyncIterator in x;
const isIterable = (x: unknown): x is Iterable<unknown> =>
  x != null && typeof (x as Iterable<unknown>)[Symbol.iterator] === 'function';
const isSubscribable = (x: unknown): x is Subscribable<unknown> =>
  typeof x === 'object' && x !== null && typeof (x as Subscribable<unknown>).subscribe === 'function';

/** Converts any `ObservableInput` into an rxjs-fp Observable. */
export const from = <A>(input: ObservableInput<A>): Observable<A> => {
  if (isObservable(input)) return input as Observable<A>;
  if (isSubscribable(input)) return fromSubscribable(input as Subscribable<A>);
  if (isPromiseLike(input)) return fromPromise(input as PromiseLike<A>);
  if (isAsyncIterable(input)) return fromAsyncIterable(input as AsyncIterable<A>);
  if (isIterable(input)) return fromIterable(input as Iterable<A>);
  if (typeof input === 'object' && input !== null && 'length' in input) return fromArrayLike(input as ArrayLike<A>);
  return throwError(() => new TypeError(`rxjs-fp: cannot convert ${String(input)} to an Observable`));
};

/** Emits `count` consecutive integers starting at `start`. */
export const range = (start: number, count: number): Observable<number> =>
  observable((sink) => {
    for (let i = 0; i < count && !sink.closed; i++) sink.next(start + i);
    sink.complete();
  });

/**
 * The anamorphism: unfolds a sequence from a seed with a *pure* function.
 * `f(seed)` returns `some([value, nextSeed])` to emit, or `none` to complete.
 */
export const unfold = <S, A>(seed: S, f: (s: S) => Option<readonly [A, S]>): Observable<A> =>
  observable((sink) => {
    let s = seed;
    for (;;) {
      if (sink.closed) return;
      const r = f(s);
      if (!isSome(r)) break;
      sink.next(r.value[0]);
      s = r.value[1];
    }
    sink.complete();
  });

/** Emits 0, 1, 2, … every `period` ms. */
export const interval = (period: number, scheduler: Scheduler = asyncScheduler): Observable<number> =>
  timer(period, period, scheduler);

/**
 * After `dueTime` ms emits 0; then, if `period` is given, 1, 2, … every
 * `period` ms. Rescheduling is recursive through the scheduler.
 */
export const timer = (dueTime: number, period?: number, scheduler: Scheduler = asyncScheduler): Observable<number> =>
  observable((sink) => {
    let n = 0;
    let cancel = (): void => {};
    const tick = (): void => {
      sink.next(n++);
      if (period === undefined) sink.complete();
      else if (!sink.closed) cancel = scheduler.schedule(tick, Math.max(0, period));
    };
    cancel = scheduler.schedule(tick, Math.max(0, dueTime));
    return () => cancel();
  });

/** Chooses between two Observables at subscription time. */
export const iif = <A, B>(condition: () => boolean, onTrue: ObservableInput<A>, onFalse: ObservableInput<B>): Observable<A | B> =>
  defer<A | B>(() => (condition() ? onTrue : onFalse));

// ---------------------------------------------------------------------------
// fromEvent
// ---------------------------------------------------------------------------

export interface DomEventTargetLike<E> {
  addEventListener(type: string, listener: (event: E) => void, options?: boolean | AddEventListenerOptions): void;
  removeEventListener(type: string, listener: (event: E) => void, options?: boolean | EventListenerOptions): void;
}

export interface NodeEventEmitterLike<E> {
  on(type: string | symbol, listener: (event: E) => void): unknown;
  off(type: string | symbol, listener: (event: E) => void): unknown;
}

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
  return observable<E>((sink) => {
    const handler = (e: E): void => sink.next(e);
    if ('addEventListener' in target) {
      target.addEventListener(type as string, handler, options);
      return () => target.removeEventListener(type as string, handler, options);
    }
    target.on(type, handler);
    return () => {
      target.off(type, handler);
    };
  });
}
