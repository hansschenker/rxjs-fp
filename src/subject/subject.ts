import { observable, subscribeChild, subscribeWithin } from '../core/observable.js';
import { createSubscription } from '../core/subscription.js';
import { objectUnsubscribedError } from '../core/errors.js';
import type { Observable, Observer, Operator, Sink, Subscription } from '../core/types.js';
import { asyncScheduler, type Scheduler } from '../scheduler/scheduler.js';
import { from, type ObservableInput } from '../creation/creation.js';

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
 * @internal Builds a subject; `onSubscribe` lets variants (Behavior, Replay)
 * prime a new observer before it is registered.
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
    hooks.onSubscribe?.(sink);
    if (terminal) {
      if (terminal.kind === 'E') sink.error(terminal.error);
      else sink.complete();
      return;
    }
    observers.add(sink);
    return () => {
      observers.delete(sink);
    };
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

  const asObservable = (): Observable<A> => observable<A>((sink) => subscribeChild(obs, sink));

  const subject: Subject<A> = Object.freeze({
    ...obs,
    // like RxJS 7: subscribing to a disposed subject throws synchronously
    subscribe: (o?: Parameters<Observable<A>['subscribe']>[0]) => {
      check();
      return obs.subscribe(o);
    },
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

/**
 * Shared by every share operator: turns a cold Observable hot, so all
 * subscribers share one source subscription, connected on the first
 * subscriber. The three flags say when the connection starts over; the
 * lifecycles in `shareWith` are the combinations that are offered.
 */
const shareCore =
  <A>(connector: () => Subject<A>, resetOnError: boolean, resetOnComplete: boolean, resetOnRefCountZero: boolean): Operator<A, A> =>
  (source) => {

    // The connection is the one piece of state shared between subscribers,
    // which is exactly the point of `share`.
    let subject: Subject<A> | null = null;
    let connection: Subscription | null = null;
    let refCount = 0;
    let terminated = false;

    const reset = (): void => {
      const c = connection;
      connection = null;
      subject = null;
      terminated = false;
      c?.unsubscribe();
    };

    return observable<A>((sink) => {
      refCount++;
      // Registered first, so it also runs when a synchronous source makes
      // the downstream close while we are still connecting.
      sink.add(() => {
        refCount--;
        if (refCount === 0 && resetOnRefCountZero && !terminated) reset();
      });
      const s = (subject ??= connector());
      subscribeChild(s, sink);
      if (connection === null && !terminated && !sink.closed) {
        // The connection exists *before* the source runs: re-entrant
        // subscribers see it (no second connection), and closing it stops
        // even a synchronous source.
        const conn = createSubscription();
        connection = conn;
        subscribeWithin(source, conn, {
          next: (a) => s.next(a),
          error: (e) => {
            terminated = true;
            if (resetOnError) reset();
            s.error(e);
          },
          complete: () => {
            terminated = true;
            if (resetOnComplete) reset();
            s.complete();
          },
        });
      }
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
    ? shareCore<A>(connector, true, true, true)
    : shareCore<A>(connector, true, false, false);

/** Shares one source subscription while anyone listens; late subscribers see only later values. */
export const share = <A>(): Operator<A, A> => shareWith<A>(createSubject, WhileSubscribed);

/**
 * A cache: connects once, replays the last `bufferSize` values (default:
 * all) to every subscriber, and keeps them after completion and when nobody
 * listens. Starts over only after an error.
 */
export const shareReplay = <A>(bufferSize: number = Number.POSITIVE_INFINITY): Operator<A, A> =>
  shareWith<A>(() => createReplaySubject<A>(bufferSize), Cached);

/**
 * Shares one source subscription while anyone listens, and gives every new
 * subscriber the latest value at once. Suits state streams.
 */
export const shareLatest = <A>(): Operator<A, A> => shareWith<A>(() => createReplaySubject<A>(1), WhileSubscribed);

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
    subscribeWithin(source, conn, {
      next: (a) => s.next(a),
      error: (e) => {
        s.error(e);
        conn.unsubscribe();
      },
      complete: () => {
        s.complete();
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
 * The theater view of multicasting: subscribers take their seats, nothing
 * runs until `connect()` raises the curtain, and then everyone seated sees
 * the show from the first value. Each `publish…` function is
 * `connectableOnce` with a different Subject behind the curtain, kept for
 * the theater's whole life, as RxJS's `publish()` keeps its Subject.
 *
 *   const show = publish(source);
 *   show.subscribe(a);
 *   show.subscribe(b);
 *   show.connect();
 *
 * Late arrivals see only what happens after they sit down.
 */
export const publish = <A>(source: Observable<A>): Connectable<A> => connectableOnce(source, () => createSubject<A>());

/** A theater where late arrivals first see the last `bufferSize` values (default: all). */
export const publishReplay = <A>(source: Observable<A>, bufferSize: number = Number.POSITIVE_INFINITY): Connectable<A> =>
  connectableOnce(source, () => createReplaySubject<A>(bufferSize));

/** A theater where everyone sees the current value at once: `initial` before the first one. */
export const publishBehavior = <A>(source: Observable<A>, initial: A): Connectable<A> =>
  connectableOnce(source, () => createBehaviorSubject(initial));

/** A theater that shows only the last value, when the source completes. */
export const publishLast = <A>(source: Observable<A>): Connectable<A> => connectableOnce(source, () => createAsyncSubject<A>());

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
