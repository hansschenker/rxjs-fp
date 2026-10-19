import { createSubscription, isSubscription } from './subscription.js';
import { reportUnhandledError } from './report.js';
import { pipeArray } from './pipe.js';
import {
  OBSERVABLE_BRAND,
  OBSERVABLE_KEY,
  type Observable,
  type Observer,
  type Operator,
  type Producer,
  type Sink,
  type Subscription,
  type Teardown,
} from './types.js';

/**
 * Internal marker: an observer created by an operator for its upstream.
 * Subscribing with such an observer links the new execution to the
 * downstream sink *before* the producer runs, so that synchronous early
 * termination (e.g. `take(1)` on an infinite synchronous source) stops the
 * upstream loop immediately.
 */
const PARENT: unique symbol = Symbol('rxjs-fp/parent');

interface ChildObserver<A> extends Observer<A> {
  readonly [PARENT]: Sink<unknown>;
}

const isChildObserver = <A>(o: unknown): o is ChildObserver<A> =>
  typeof o === 'object' && o !== null && PARENT in o;

const noop = (): void => {};

/**
 * Normalizes what was passed to `subscribe`. Foreign observers (e.g. an
 * RxJS Subscriber) may be objects whose methods rely on their receiver, so
 * they are always invoked as `o.next(v)`, never detached.
 */
const toObserver = <A>(o?: Partial<Observer<A>> | ((value: A) => void)): Observer<A> => {
  if (typeof o === 'function') return { next: o, error: reportUnhandledError, complete: noop };
  if (o == null) return { next: noop, error: reportUnhandledError, complete: noop };
  return {
    next: (v) => {
      o.next?.(v);
    },
    error: (e) => {
      if (typeof o.error === 'function') o.error(e);
      else reportUnhandledError(e);
    },
    complete: () => {
      o.complete?.();
    },
  };
};

/**
 * Wraps a consumer observer into a sink that enforces the Observable
 * grammar `next* (error | complete)?`:
 *  - nothing is delivered after a terminal notification or after unsubscribe
 *  - a terminal notification tears the execution down
 *  - a consumer callback that throws is reported, never re-entered
 *
 * `internal` sinks (operator-to-operator) let exceptions propagate to the
 * operator, which routes them to `error`; consumer sinks report them.
 */
const createSink = <A>(observer: Observer<A>, subscription: Subscription, internal: boolean): Sink<A> => {
  let stopped = false;
  const guard = (f: () => void): void => {
    if (internal) {
      f();
      return;
    }
    try {
      f();
    } catch (e) {
      reportUnhandledError(e);
    }
  };
  return Object.freeze({
    next: (value: A) => {
      if (stopped || subscription.closed) return;
      guard(() => observer.next(value));
    },
    error: (err: unknown) => {
      if (stopped || subscription.closed) return;
      stopped = true;
      try {
        guard(() => observer.error(err));
      } finally {
        subscription.unsubscribe();
      }
    },
    complete: () => {
      if (stopped || subscription.closed) return;
      stopped = true;
      try {
        guard(() => observer.complete());
      } finally {
        subscription.unsubscribe();
      }
    },
    get closed() {
      return stopped || subscription.closed;
    },
    add: subscription.add,
  });
};

/**
 * Creates an Observable from a producer function.
 *
 * The result is a frozen record of closures. `subscribe` is literally
 * "call the producer with a fresh sink", so two subscriptions run the
 * producer twice and never share state (cold by default).
 */
export const observable = <A>(producer: Producer<A>): Observable<A> => {
  const subscribe = (o?: Partial<Observer<A>> | ((value: A) => void)): Subscription => {
    const subscription = createSubscription();
    const child = isChildObserver<A>(o);
    if (child) {
      // Link to the downstream execution first (see PARENT above) and
      // unlink again when we finish on our own, so parents don't leak.
      const parent = o[PARENT];
      const unlink = parent.add(subscription.unsubscribe);
      subscription.add(unlink);
    }
    const sink = createSink(child ? o : toObserver(o), subscription, child);
    if (subscription.closed) return subscription; // downstream already gone
    try {
      const teardown = producer(sink);
      if (typeof teardown === 'function') subscription.add(teardown);
      else if (isSubscription(teardown)) subscription.add(teardown.unsubscribe);
    } catch (e) {
      sink.error(e);
    }
    return subscription;
  };

  let self: Observable<A>;
  const pipe = (...operators: ReadonlyArray<Operator<any, any>>) => pipeArray(operators)(self);
  self = Object.freeze({
    [OBSERVABLE_BRAND]: true as const,
    subscribe,
    pipe: pipe as Observable<A>['pipe'],
    [OBSERVABLE_KEY]: () => self,
  });
  return self;
};

/** Type guard for rxjs-fp Observables. */
export const isObservable = (x: unknown): x is Observable<unknown> =>
  typeof x === 'object' && x !== null && (x as Record<PropertyKey, unknown>)[OBSERVABLE_BRAND] === true;

/** Handlers an operator supplies for its upstream. Missing ones forward to the sink. */
export interface ChildHandlers<A> {
  readonly next?: (value: A) => void;
  readonly error?: (err: unknown) => void;
  readonly complete?: () => void;
}

/**
 * Subscribes an operator to its upstream on behalf of `sink`.
 *
 * - Exceptions thrown by any handler (i.e. by user functions such as a
 *   projection or predicate) are routed to `sink.error`. Nothing an
 *   operator does can escape the pipeline as an unhandled error.
 * - The upstream execution is linked to `sink`: when the downstream closes,
 *   the upstream is unsubscribed, synchronously.
 */
export const subscribeChild = <A, B = A>(source: Observable<A>, sink: Sink<B>, handlers: ChildHandlers<A> = {}): Subscription => {
  const route = (f: () => void): void => {
    try {
      f();
    } catch (e) {
      sink.error(e);
    }
  };
  const { next, error, complete } = handlers;
  const observer: ChildObserver<A> = {
    [PARENT]: sink as Sink<unknown>,
    next: next ? (v) => route(() => next(v)) : (v) => sink.next(v as unknown as B),
    error: error ? (e) => route(() => error(e)) : (e) => sink.error(e),
    complete: complete ? () => route(complete) : () => sink.complete(),
  };
  return source.subscribe(observer);
};

/** Turns a Teardown | Subscription into a Teardown. */
export const toTeardown = (t: Teardown | Subscription): Teardown => (typeof t === 'function' ? t : t.unsubscribe);
