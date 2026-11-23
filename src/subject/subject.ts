import { observable, subscribeChild, subscribeWithin } from '../core/observable.js';
import { createSubscription } from '../core/subscription.js';
import { objectUnsubscribedError } from '../core/errors.js';
import { reportUnhandledError } from '../core/report.js';
import type { Observable, Observer, Operator, Sink, Subscription } from '../core/types.js';
import { asyncScheduler, type Scheduler } from '../scheduler/scheduler.js';
import { from, type ObservableInput } from '../creation/creation.js';
import { assertNever, unlimited } from '../policies/core.js';
import type { Limit, PositiveInt, PositiveMilliseconds, Signal } from '../policies/core.js';

/**
 * A Subject is shared state by definition: one producer, many observers.
 * Here it is a record of closures over a private observer set. Sharing is
 * something you opt into explicitly; plain Observables stay cold.
 */
export interface Subject<A> extends Observable<A>, Observer<A> {
  /** `true` while at least one observer is subscribed. */
  readonly observed: boolean;
  /** `true` after `dispose()`. */
  readonly disposed: boolean;
  /** A read-only view that hides `next`/`error`/`complete`. */
  readonly asObservable: () => Observable<A>;
  /** Drops all observers; further use throws `ObjectUnsubscribedError`. */
  readonly dispose: () => void;
}

type Terminal = { readonly kind: 'E'; readonly error: unknown } | { readonly kind: 'C' } | null;

interface SubjectCore<A> {
  readonly subject: Subject<A>;
  readonly terminal: () => Terminal;
}

/**
 * @internal Builds a subject; active observers are registered before variants
 * replay values, so a reentrant next during replay also reaches the newcomer.
 */
const makeSubject = <A>(hooks: {
  readonly onNext?: (a: A) => void;
  readonly onSubscribe?: (sink: Sink<A>) => void;
}): SubjectCore<A> => {
  const observers = new Set<Sink<A>>();
  let terminal: Terminal = null;
  let disposed = false;

  const check = (): void => {
    if (disposed) throw objectUnsubscribedError();
  };

  const obs = observable<A>((sink) => {
    check();
    if (!terminal) observers.add(sink);
    sink.add(() => {
      observers.delete(sink);
    });
    hooks.onSubscribe?.(sink);
    if (terminal) {
      if (terminal.kind === 'E') sink.error(terminal.error);
      else sink.complete();
      return;
    }
  });

  const next = (a: A): void => {
    check();
    if (terminal) return;
    hooks.onNext?.(a);
    for (const o of Array.from(observers)) o.next(a);
  };
  const error = (e: unknown): void => {
    check();
    if (terminal) return;
    terminal = { kind: 'E', error: e };
    const snapshot = Array.from(observers);
    observers.clear();
    for (const o of snapshot) o.error(e);
  };
  const complete = (): void => {
    check();
    if (terminal) return;
    terminal = { kind: 'C' };
    const snapshot = Array.from(observers);
    observers.clear();
    for (const o of snapshot) o.complete();
  };

  // like RxJS 7: subscribing to a disposed subject throws synchronously,
  // directly and through the asObservable view
  const guarded = (view: Observable<A>): Observable<A> =>
    Object.freeze({
      ...view,
      subscribe: (o?: Parameters<Observable<A>['subscribe']>[0]) => {
        check();
        return view.subscribe(o);
      },
    });
  const asObservable = (): Observable<A> => guarded(observable<A>((sink) => subscribeChild(obs, sink)));

  const subject: Subject<A> = Object.freeze({
    ...guarded(obs),
    next,
    error,
    complete,
    asObservable,
    dispose: () => {
      disposed = true;
      observers.clear();
    },
    get observed() {
      return observers.size > 0;
    },
    get disposed() {
      return disposed;
    },
  });
  return { subject, terminal: () => terminal };
};

/** A plain multicasting Subject. */
export const createSubject = <A>(): Subject<A> => makeSubject<A>({}).subject;

export interface BehaviorSubject<A> extends Subject<A> {
  /** The current value. Throws the error if the subject errored. */
  readonly value: A;
  readonly getValue: () => A;
}

/** A Subject with a current value, replayed to every new subscriber. */
export const createBehaviorSubject = <A>(initial: A): BehaviorSubject<A> => {
  let current = initial;
  const core = makeSubject<A>({
    onNext: (a) => {
      current = a;
    },
    onSubscribe: (sink) => {
      if (core.terminal() === null) sink.next(current);
    },
  });
  const getValue = (): A => {
    const t = core.terminal();
    if (t && t.kind === 'E') throw t.error;
    if (core.subject.disposed) throw objectUnsubscribedError();
    return current;
  };
  return Object.freeze({
    ...core.subject,
    getValue,
    get value() {
      return getValue();
    },
    get observed() {
      return core.subject.observed;
    },
    get disposed() {
      return core.subject.disposed;
    },
  });
};

/** A Subject that replays the last `bufferSize` values (within `windowTime` ms) to new subscribers. */
export const createReplaySubject = <A>(
  bufferSize: number = Number.POSITIVE_INFINITY,
  windowTime: number = Number.POSITIVE_INFINITY,
  scheduler: Scheduler = asyncScheduler,
): Subject<A> => {
  let buffer: ReadonlyArray<{ readonly at: number; readonly value: A }> = [];
  const trim = (): void => {
    const now = scheduler.now();
    const fresh = buffer.filter((e) => now - e.at < windowTime);
    buffer = fresh.length > bufferSize ? fresh.slice(fresh.length - bufferSize) : fresh;
  };
  const core = makeSubject<A>({
    onNext: (value) => {
      buffer = [...buffer, { at: scheduler.now(), value }];
      trim();
    },
    onSubscribe: (sink) => {
      trim();
      for (const e of buffer) {
        if (sink.closed) return;
        sink.next(e.value);
      }
    },
  });
  return core.subject;
};

/** Emits only the last value, and only when the subject completes. */
export const createAsyncSubject = <A>(): Subject<A> => {
  let has = false;
  let last: A | undefined;
  const inner = makeSubject<A>({});
  const outer = makeSubject<A>({
    onSubscribe: (sink) => {
      const t = inner.terminal();
      if (t && t.kind === 'C' && has) sink.next(last as A);
    },
  });
  return Object.freeze({
    ...outer.subject,
    next: (a: A) => {
      if (inner.terminal() === null) {
        has = true;
        last = a;
      }
    },
    complete: () => {
      if (inner.terminal() !== null) return;
      inner.subject.complete();
      if (has) outer.subject.next(last as A);
      outer.subject.complete();
    },
    error: (e: unknown) => {
      inner.subject.error(e);
      outer.subject.error(e);
    },
    get observed() {
      return outer.subject.observed;
    },
    get disposed() {
      return outer.subject.disposed;
    },
  });
};

// ---------------------------------------------------------------------------
// share / shareReplay: cold -> hot, explicitly
// ---------------------------------------------------------------------------

/**
 * When a shared connection starts over. Lifecycles are data, like
 * flattening and throttle strategies:
 *
 *   WhileSubscribed  connected while anyone listens; starts over after an
 *                    error, after completion, and when the last subscriber leaves
 *   Cached           connects once and keeps the result, even when nobody
 *                    listens; starts over only after an error
 *
 * "Starting over" means a new Subject from the connector and a new
 * subscription to the source on the next subscriber.
 */
export type ShareLifecycle = { readonly _tag: 'WhileSubscribed' } | { readonly _tag: 'Cached' };

export const WhileSubscribed: ShareLifecycle = Object.freeze({ _tag: 'WhileSubscribed' });
export const Cached: ShareLifecycle = Object.freeze({ _tag: 'Cached' });

/** A reset signal is triggered by its first next; completion alone keeps state. */
export type ResetPolicy<Args extends unknown[]> =
  | Readonly<{ kind: 'keep-state' }>
  | Readonly<{ kind: 'reset-now' }>
  | Readonly<{ kind: 'reset-on-next'; signal: (...args: Args) => Signal }>;

/** Three independent lifecycle decisions, plus the shared Subject factory. */
export type SharePolicy<A> = Readonly<{
  connector: () => Subject<A>;
  onError: ResetPolicy<[error: unknown]>;
  onComplete: ResetPolicy<[]>;
  onNoSubscribers: ResetPolicy<[]>;
}>;

export const defaultSharePolicy = <A>(): SharePolicy<A> => Object.freeze({
  connector: createSubject<A>,
  onError: Object.freeze({ kind: 'reset-now' }),
  onComplete: Object.freeze({ kind: 'reset-now' }),
  onNoSubscribers: Object.freeze({ kind: 'reset-now' }),
});

/** Shared-connection callbacks have no downstream error channel of their own. */
const reportCallbackError = (effect: () => void): void => {
  try {
    effect();
  } catch (error) {
    reportUnhandledError(error);
  }
};

const startReset = <Args extends unknown[]>(
  reset: () => void,
  policy: ResetPolicy<Args>,
  ...args: Args
): Subscription | null => {
  switch (policy.kind) {
    case 'keep-state':
      return null;
    case 'reset-now':
      reset();
      return null;
    case 'reset-on-next': {
      const makeSignal = policy.signal;
      // Create the scope before connecting: a synchronous first next must
      // close the actual notifier, including an iterable's remaining values.
      const scope = createSubscription();
      subscribeWithin(from(makeSignal(...args)), scope, {
        next: () => reportCallbackError(() => {
          scope.unsubscribe();
          reset();
        }),
        error: (error) => {
          reportUnhandledError(error);
          scope.unsubscribe();
        },
        complete: scope.unsubscribe,
      });
      return scope;
    }
    default:
      return assertNever(policy);
  }
};

/**
 * Share one source connection and interpret independent reset policies.
 * Reset after a terminal event enables future subscriptions; current observers
 * still receive that terminal event. A zero-observer reset also disconnects the
 * active source. A returning observer cancels a pending reset of that kind.
 * Reset notifier errors are reported as unhandled, as in RxJS 7.8.2.
 */
export const share = <A>(policy: SharePolicy<A> = defaultSharePolicy<A>()): Operator<A, A> => {
  const connector = policy.connector;
  const onError = { ...policy.onError };
  const onComplete = { ...policy.onComplete };
  const onNoSubscribers = { ...policy.onNoSubscribers };

  return (source) => {
    // This state belongs to the shared observable, deliberately spanning its
    // observers. Applying the operator to another source gets independent state.
    let subject: Subject<A> | null = null;
    let connection: Subscription | null = null;
    let resetConnection: Subscription | null = null;
    let refCount = 0;
    let hasErrored = false;
    let hasCompleted = false;

    const cancelReset = (): void => {
      resetConnection?.unsubscribe();
      resetConnection = null;
    };
    const reset = (): void => {
      cancelReset();
      connection = subject = null;
      hasErrored = hasCompleted = false;
    };
    const resetAndDisconnect = (): void => {
      const active = connection;
      reset();
      active?.unsubscribe();
    };

    return observable<A>((sink) => {
      refCount++;
      if (!hasErrored && !hasCompleted) cancelReset();

      const destination = (subject ??= connector());
      // Register before replay or source connection so synchronous take(1)
      // also closes the upstream connection without leaking a reference.
      sink.add(() => {
        refCount--;
        if (refCount === 0 && !hasErrored && !hasCompleted) {
          resetConnection = startReset(resetAndDisconnect, onNoSubscribers);
        }
      });
      subscribeChild(destination, sink);

      if (connection === null && refCount > 0) {
        const active = createSubscription();
        connection = active;
        subscribeWithin(source, active, {
          next: (value) => reportCallbackError(() => destination.next(value)),
          error: (error) => {
            try {
              reportCallbackError(() => {
                hasErrored = true;
                cancelReset();
                resetConnection = startReset(reset, onError, error);
                destination.error(error);
              });
            } finally {
              active.unsubscribe();
            }
          },
          complete: () => {
            try {
              reportCallbackError(() => {
                hasCompleted = true;
                cancelReset();
                resetConnection = startReset(reset, onComplete);
                destination.complete();
              });
            } finally {
              active.unsubscribe();
            }
          },
        });
      }
    });
  };
};

export type ReplayBounds = Readonly<{
  maxValues: Limit<PositiveInt>;
  maxAge: Limit<PositiveMilliseconds>;
}>;

export type IdlePolicy = 'keep-connected' | 'disconnect-and-reset';

/** Replay count and age bounds apply together; idle behavior is independent. */
export type ShareReplayPolicy = Readonly<{
  replay: ReplayBounds;
  onNoSubscribers: IdlePolicy;
}>;

export const defaultShareReplayPolicy = (): ShareReplayPolicy => Object.freeze({
  replay: Object.freeze({ maxValues: unlimited<PositiveInt>(), maxAge: unlimited<PositiveMilliseconds>() }),
  onNoSubscribers: 'keep-connected',
});

const replayBound = <N extends number>(limit: Limit<N>): number => {
  switch (limit.kind) {
    case 'unlimited':
      return Number.POSITIVE_INFINITY;
    case 'limited':
      // ReplaySubject in RxJS 7.8.2 normalizes each bound to at least one.
      return Math.max(1, limit.value);
    default:
      return assertNever(limit);
  }
};

const idleReset = (policy: IdlePolicy): ResetPolicy<[]> => {
  switch (policy) {
    case 'keep-connected':
      return { kind: 'keep-state' };
    case 'disconnect-and-reset':
      return { kind: 'reset-now' };
    default:
      return assertNever(policy);
  }
};

/**
 * Share with replay and retain successful completion, even when idle behavior
 * is disconnect-and-reset. Expired values do not cause a completed source to
 * reconnect. The explicit scheduler supplies the age clock; replay is immediate.
 */
export const shareReplay = <A>(
  policy: ShareReplayPolicy = defaultShareReplayPolicy(),
  scheduler: Scheduler = asyncScheduler,
): Operator<A, A> => {
  const count = replayBound(policy.replay.maxValues);
  const age = replayBound(policy.replay.maxAge);
  return share<A>({
    connector: () => createReplaySubject<A>(count, age, scheduler),
    onError: { kind: 'reset-now' },
    onComplete: { kind: 'keep-state' },
    onNoSubscribers: idleReset(policy.onNoSubscribers),
  });
};

/**
 * Turns a cold Observable hot: all subscribers share one source
 * subscription, multicast through the Subject that `connector` creates,
 * restarted as `lifecycle` says.
 *
 *   source.pipe(shareWith(() => createReplaySubject(10, 5000), Cached))
 */
export const shareWith = <A>(connector: () => Subject<A>, lifecycle: ShareLifecycle): Operator<A, A> =>
  lifecycle._tag === 'WhileSubscribed'
    ? share<A>({ ...defaultSharePolicy<A>(), connector })
    : share<A>({
        connector,
        onError: { kind: 'reset-now' },
        onComplete: { kind: 'keep-state' },
        onNoSubscribers: { kind: 'keep-state' },
      });

// ---------------------------------------------------------------------------
// connectable / connect: multicasting with an explicit connection
// ---------------------------------------------------------------------------

export interface Connectable<A> extends Observable<A> {
  /** Subscribes the shared Subject to the source (once; returns the running connection if there is one). */
  readonly connect: () => Subscription;
}

/**
 * Shared by `connectable` and `connectableOnce`: a hot Observable that only
 * starts when you call `connect()`. Subscribers attach to a shared Subject;
 * `connect()` subscribes that Subject to the source. With
 * `resetOnDisconnect`, a fresh Subject replaces it after each connection ends.
 */
const connectableCore = <A>(source: Observable<A>, connector: () => Subject<A>, resetOnDisconnect: boolean): Connectable<A> => {
  // shared by design: the subject and the connection
  let subject = connector();
  let connection: Subscription | null = null;

  const connect = (): Subscription => {
    if (connection && !connection.closed) return connection;
    const conn = createSubscription();
    connection = conn;
    const s = subject;
    if (resetOnDisconnect) conn.add(() => (subject = connector()));
    // The connector's Subject may be held (and disposed) by the caller; like
    // the share connection, report what it throws and keep going.
    subscribeWithin(source, conn, {
      next: (a) => reportCallbackError(() => s.next(a)),
      error: (e) => {
        reportCallbackError(() => s.error(e));
        conn.unsubscribe();
      },
      complete: () => {
        reportCallbackError(() => s.complete());
        conn.unsubscribe();
      },
    });
    return conn;
  };

  return Object.freeze({
    ...observable<A>((sink) => subscribeChild(subject, sink)),
    connect,
  });
};

/**
 * A hot Observable that only starts when you call `connect()`. After a
 * connection ends (unsubscribed, completed or errored), a fresh Subject from
 * `connector` takes over, so the next `connect()` starts over. The
 * replacement for RxJS's `multicast`/`publish` family.
 */
export const connectable = <A>(source: Observable<A>, connector: () => Subject<A> = createSubject): Connectable<A> =>
  connectableCore(source, connector, true);

/**
 * Like `connectable`, but keeps one Subject for its whole life: after the
 * source completes or errors, later subscribers get that ending (and, with a
 * ReplaySubject, the replayed values) instead of a new start.
 */
export const connectableOnce = <A>(source: Observable<A>, connector: () => Subject<A> = createSubject): Connectable<A> =>
  connectableCore(source, connector, false);

// ---------------------------------------------------------------------------
// publish family: named theaters, and refCount
// ---------------------------------------------------------------------------

/**
 * Connects automatically: the curtain rises when the first subscriber
 * arrives and falls when the last one leaves; the next subscriber connects
 * again. The bridge from the theater view to `share`:
 *
 *   refCount(publish(source))   // shares one source subscription while anyone listens
 *
 * Edge case: unlike `share`, the theater keeps its one Subject, so after the
 * source has completed, later subscribers get that completion instead of a
 * new start.
 */
export const refCount = <A>(connectable: Connectable<A>): Observable<A> => {
  // shared by design: the number of subscribers and the running connection
  let count = 0;
  let connection: Subscription | null = null;
  return observable<A>((sink) => {
    count++;
    sink.add(() => {
      count--;
      if (count === 0 && connection) {
        const c = connection;
        connection = null;
        c.unsubscribe();
      }
    });
    subscribeChild(connectable, sink);
    if (!sink.closed) connection = connectable.connect();
  });
};

/**
 * Shares one subscription to the source within `selector`: the selector
 * receives a multicast view of the source, and the source is connected
 * after the selector's result has been subscribed, so no value is missed.
 */
export const connect =
  <A, B>(selector: (shared: Observable<A>) => ObservableInput<B>, connector: () => Subject<A> = createSubject): Operator<A, B> =>
  (source) =>
    observable<B>((sink) => {
      const subject = connector();
      subscribeChild(from(selector(subject.asObservable())), sink);
      if (sink.closed) return;
      subscribeChild(source, sink, {
        next: (a) => subject.next(a),
        error: (e) => subject.error(e),
        complete: () => subject.complete(),
      });
    });
