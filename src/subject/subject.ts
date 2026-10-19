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

export interface ShareConfig<A> {
  /** Factory for the Subject that multicasts one connection. Default: `createSubject`. */
  readonly connector?: () => Subject<A>;
  /** Reset (new Subject, new source subscription) after the source errors. Default: true. */
  readonly resetOnError?: boolean;
  /** Reset after the source completes. Default: true. */
  readonly resetOnComplete?: boolean;
  /** Reset (unsubscribe from the source) when the subscriber count drops to 0. Default: true. */
  readonly resetOnRefCountZero?: boolean;
}

/**
 * Turns a cold Observable hot: all subscribers share one source
 * subscription. The source is connected on the first subscriber.
 */
export const share =
  <A>(config: ShareConfig<A> = {}): Operator<A, A> =>
  (source) => {
    const connector = config.connector ?? createSubject;
    const resetOnError = config.resetOnError ?? true;
    const resetOnComplete = config.resetOnComplete ?? true;
    const resetOnRefCountZero = config.resetOnRefCountZero ?? true;

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

export interface ShareReplayConfig {
  readonly bufferSize?: number;
  readonly windowTime?: number;
  /** Disconnect from the source when no one is subscribed. Default: false (like RxJS). */
  readonly refCount?: boolean;
  readonly scheduler?: Scheduler;
}

/** `share` with a ReplaySubject that is kept after completion. */
export const shareReplay = <A>(config: number | ShareReplayConfig = {}): Operator<A, A> => {
  const cfg: ShareReplayConfig = typeof config === 'number' ? { bufferSize: config } : config;
  return share<A>({
    connector: () => createReplaySubject<A>(cfg.bufferSize, cfg.windowTime, cfg.scheduler),
    resetOnError: true,
    resetOnComplete: false,
    resetOnRefCountZero: cfg.refCount ?? false,
  });
};

// ---------------------------------------------------------------------------
// connectable / connect: multicasting with an explicit connection
// ---------------------------------------------------------------------------

export interface ConnectableConfig<A> {
  /** Factory for the Subject that multicasts one connection. Default: `createSubject`. */
  readonly connector?: () => Subject<A>;
  /** Use a fresh Subject after the connection ends (unsubscribed, completed or errored). Default: true. */
  readonly resetOnDisconnect?: boolean;
}

export interface Connectable<A> extends Observable<A> {
  /** Subscribes the shared Subject to the source (once; returns the running connection if there is one). */
  readonly connect: () => Subscription;
}

/**
 * A hot Observable that only starts when you call `connect()`. Subscribers
 * attach to a shared Subject; `connect()` subscribes that Subject to the
 * source. The replacement for RxJS's `multicast`/`publish` family.
 */
export const connectable = <A>(source: Observable<A>, config: ConnectableConfig<A> = {}): Connectable<A> => {
  const connector = config.connector ?? createSubject;
  const resetOnDisconnect = config.resetOnDisconnect ?? true;
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
 * Shares one subscription to the source within `selector`: the selector
 * receives a multicast view of the source, and the source is connected
 * after the selector's result has been subscribed, so no value is missed.
 */
export const connect =
  <A, B>(selector: (shared: Observable<A>) => ObservableInput<B>, config: { readonly connector?: () => Subject<A> } = {}): Operator<A, B> =>
  (source) =>
    observable<B>((sink) => {
      const subject = (config.connector ?? createSubject)();
      subscribeChild(from(selector(subject.asObservable())), sink);
      if (sink.closed) return;
      subscribeChild(source, sink, {
        next: (a) => subject.next(a),
        error: (e) => subject.error(e),
        complete: () => subject.complete(),
      });
    });
