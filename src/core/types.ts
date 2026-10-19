/**
 * Core types of rxjs-fp.
 *
 * Everything here is a plain, readonly record of functions. There are no
 * classes, no `this`, no prototypes: an Observable is a value whose only
 * capability is `subscribe`, exactly like Paul Taylor's definition:
 *
 *   "An Observable is a function that, when invoked, returns
 *    0 to ∞ values between now and the end of time."
 */

/** A function that releases a resource. Idempotent by contract. */
export type Teardown = () => void;

/** The consumer side of the protocol: `next* (error | complete)?` */
export interface Observer<A> {
  readonly next: (value: A) => void;
  readonly error: (err: unknown) => void;
  readonly complete: () => void;
}

/** A disposable handle for a running Observable execution. */
export interface Subscription {
  /** `true` once `unsubscribe` ran or the execution terminated. */
  readonly closed: boolean;
  /** Releases every resource held by this execution. Idempotent. */
  readonly unsubscribe: () => void;
  /**
   * Registers a finalizer. If the subscription is already closed the
   * finalizer runs immediately. Returns a function that removes the
   * finalizer again (used to avoid leaks for short-lived children).
   */
  readonly add: (finalizer: Teardown) => Teardown;
}

/**
 * The producer-facing side: an Observer that also knows whether the
 * execution is still alive and can register finalizers.
 *
 * Producers must stop emitting once `closed` is true; the sink silently
 * ignores anything sent after termination anyway (grammar enforcement).
 */
export interface Sink<A> extends Observer<A> {
  readonly closed: boolean;
  readonly add: (finalizer: Teardown) => Teardown;
}

/**
 * The subscriber function: given a sink, start producing and optionally
 * return the teardown logic for this one execution.
 */
export type Producer<A> = (sink: Sink<A>) => Teardown | Subscription | void;

/** Anything with a compatible `subscribe` (RxJS, zen-observable, …). */
export interface Subscribable<A> {
  subscribe(observer: Partial<Observer<A>>): { unsubscribe(): void };
}

/** The interop key used by RxJS 7 when `Symbol.observable` is not polyfilled. */
export const OBSERVABLE_KEY = '@@observable' as const;

/** Brand used by `isObservable`. */
export const OBSERVABLE_BRAND: unique symbol = Symbol.for('rxjs-fp/Observable');

export interface Observable<A> {
  readonly [OBSERVABLE_BRAND]: true;
  /**
   * Starts one independent execution. Calling it twice runs the producer
   * twice (cold semantics, "subscribing is calling").
   */
  readonly subscribe: (observer?: Partial<Observer<A>> | ((value: A) => void)) => Subscription;
  /** Convenience: `obs.pipe(f, g)` is `g(f(obs))`. A closure, not a method using `this`. */
  readonly pipe: PipeMethod<A>;
  /** Interop: lets RxJS `from(...)` consume rxjs-fp Observables. */
  readonly [OBSERVABLE_KEY]: () => Observable<A>;
}

/** A transformation from one Observable to another (RxJS's `OperatorFunction`). */
export type Operator<A, B> = (source: Observable<A>) => Observable<B>;

/** Extracts the value type of an Observable. */
export type ValueOf<O> = O extends Observable<infer A> ? A : never;

/** Maps a tuple of Observables to the tuple of their value types. */
export type ValuesOf<T extends readonly Observable<unknown>[]> = { -readonly [K in keyof T]: ValueOf<T[K]> };

/* prettier-ignore */
export interface PipeMethod<A> {
  (): Observable<A>;
  <B>(ab: Operator<A, B>): Observable<B>;
  <B, C>(ab: Operator<A, B>, bc: Operator<B, C>): Observable<C>;
  <B, C, D>(ab: Operator<A, B>, bc: Operator<B, C>, cd: Operator<C, D>): Observable<D>;
  <B, C, D, E>(ab: Operator<A, B>, bc: Operator<B, C>, cd: Operator<C, D>, de: Operator<D, E>): Observable<E>;
  <B, C, D, E, F>(ab: Operator<A, B>, bc: Operator<B, C>, cd: Operator<C, D>, de: Operator<D, E>, ef: Operator<E, F>): Observable<F>;
  <B, C, D, E, F, G>(ab: Operator<A, B>, bc: Operator<B, C>, cd: Operator<C, D>, de: Operator<D, E>, ef: Operator<E, F>, fg: Operator<F, G>): Observable<G>;
  <B, C, D, E, F, G, H>(ab: Operator<A, B>, bc: Operator<B, C>, cd: Operator<C, D>, de: Operator<D, E>, ef: Operator<E, F>, fg: Operator<F, G>, gh: Operator<G, H>): Observable<H>;
  <B, C, D, E, F, G, H, I>(ab: Operator<A, B>, bc: Operator<B, C>, cd: Operator<C, D>, de: Operator<D, E>, ef: Operator<E, F>, fg: Operator<F, G>, gh: Operator<G, H>, hi: Operator<H, I>): Observable<I>;
  <B, C, D, E, F, G, H, I, J>(ab: Operator<A, B>, bc: Operator<B, C>, cd: Operator<C, D>, de: Operator<D, E>, ef: Operator<E, F>, fg: Operator<F, G>, gh: Operator<G, H>, hi: Operator<H, I>, ij: Operator<I, J>): Observable<J>;
  (...operators: ReadonlyArray<Operator<any, any>>): Observable<unknown>;
}
