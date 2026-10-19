import { observable, subscribeChild } from '../core/observable.js';
import type { Observable, Operator, Sink, Teardown } from '../core/types.js';
import type { ObservableInput } from '../creation/creation.js';
import { asyncScheduler, type Scheduler } from '../scheduler/scheduler.js';
import { createSubject, type Subject } from '../subject/subject.js';
import { subscribeInner, type Inner } from './inner.js';

/**
 * Buffering and windowing.
 *
 * Both families cut the source into chunks; they differ only in how a
 * chunk is delivered. A buffer is an array emitted when the chunk closes.
 * A window is an Observable emitted when the chunk opens, which then
 * streams the chunk's values live. The window values are Subjects seen
 * through `asObservable()`, so a consumer cannot push into them.
 *
 * All chunk bookkeeping (open buffers, open windows, timers) is
 * per-subscription state inside the producer.
 */

const removeFrom = <X>(xs: X[], x: X): void => {
  const i = xs.indexOf(x);
  if (i >= 0) xs.splice(i, 1);
};

// ---------------------------------------------------------------------------
// buffer*
// ---------------------------------------------------------------------------

/**
 * Collects values into an array and emits it whenever `closingNotifier`
 * emits. The last buffer is emitted on completion, even if empty.
 */
export const buffer =
  <A>(closingNotifier: ObservableInput<unknown>): Operator<A, A[]> =>
  (source) =>
    observable<A[]>((sink) => {
      let current: A[] = [];
      const src = subscribeChild(source, sink, {
        next: (a) => {
          current.push(a);
        },
        complete: () => {
          sink.next(current);
          sink.complete();
        },
      });
      const n = subscribeInner(closingNotifier, sink, {
        next: () => {
          const b = current;
          current = [];
          sink.next(b);
        },
      });
      return () => {
        src.unsubscribe();
        n.close();
      };
    });

export interface BufferTimeConfig {
  /** Start a new buffer every `creationInterval` ms (overlapping buffers). Default: when the previous one closes. */
  readonly creationInterval?: number | null;
  /** Close a buffer early once it holds this many values. */
  readonly maxBufferSize?: number;
}

/**
 * Emits the values of each `timeSpan`-ms period as an array.
 *
 * Base case: back-to-back buffers of `timeSpan` ms. With
 * `creationInterval`, buffers start on their own timer and may overlap or
 * leave gaps; with `maxBufferSize`, a full buffer is emitted early. Open
 * buffers are emitted on completion.
 */
export const bufferTime =
  <A>(timeSpan: number, scheduler: Scheduler = asyncScheduler, config: BufferTimeConfig = {}): Operator<A, A[]> =>
  (source) =>
    observable<A[]>((sink) => {
      const maxSize = config.maxBufferSize ?? Number.POSITIVE_INFINITY;
      const creationInterval = config.creationInterval ?? null;
      const restartOnEmit = creationInterval === null || creationInterval < 0;
      interface Rec {
        readonly buffer: A[];
        cancel: Teardown;
      }
      const records: Rec[] = [];
      let cancelCreation: Teardown = () => {};

      const emit = (rec: Rec): void => {
        rec.cancel();
        removeFrom(records, rec);
        sink.next(rec.buffer);
        if (restartOnEmit) start();
      };
      const start = (): void => {
        if (sink.closed) return;
        const rec: Rec = { buffer: [], cancel: () => {} };
        records.push(rec);
        rec.cancel = scheduler.schedule(() => emit(rec), timeSpan);
      };
      if (!restartOnEmit) {
        const every = creationInterval as number;
        const tick = (): void => {
          start();
          if (!sink.closed) cancelCreation = scheduler.schedule(tick, every);
        };
        cancelCreation = scheduler.schedule(tick, every);
      }
      start();

      const src = subscribeChild(source, sink, {
        next: (a) => {
          for (const rec of records.slice()) {
            rec.buffer.push(a);
            if (rec.buffer.length >= maxSize) emit(rec);
          }
        },
        complete: () => {
          cancelCreation();
          while (records.length > 0) {
            const rec = records.shift() as Rec;
            rec.cancel();
            sink.next(rec.buffer);
          }
          sink.complete();
        },
      });
      return () => {
        src.unsubscribe();
        cancelCreation();
        records.forEach((r) => r.cancel());
        records.length = 0;
      };
    });

/**
 * Opens a buffer whenever `openings` emits; each buffer is emitted when the
 * Observable returned by `closingSelector(openValue)` emits. Buffers still
 * open at completion are emitted then.
 */
export const bufferToggle =
  <A, O>(openings: ObservableInput<O>, closingSelector: (openValue: O) => ObservableInput<unknown>): Operator<A, A[]> =>
  (source) =>
    observable<A[]>((sink) => {
      const buffers: A[][] = [];
      const closings = new Set<Inner>();
      const open = subscribeInner(openings, sink, {
        next: (o) => {
          const b: A[] = [];
          buffers.push(b);
          const c = subscribeInner(closingSelector(o), sink, {
            next: (_, self) => {
              self.close();
              closings.delete(self);
              removeFrom(buffers, b);
              sink.next(b);
            },
          });
          if (!c.closed) closings.add(c);
        },
      });
      const src = subscribeChild(source, sink, {
        next: (a) => {
          for (const b of buffers) b.push(a);
        },
        complete: () => {
          while (buffers.length > 0) sink.next(buffers.shift() as A[]);
          sink.complete();
        },
      });
      return () => {
        open.close();
        closings.forEach((c) => c.close());
        src.unsubscribe();
      };
    });

/**
 * Starts a buffer immediately; whenever the Observable returned by
 * `closingSelector()` emits, emits the buffer and starts the next one
 * (calling `closingSelector` again). The last buffer is emitted on completion.
 */
export const bufferWhen =
  <A>(closingSelector: () => ObservableInput<unknown>): Operator<A, A[]> =>
  (source) =>
    observable<A[]>((sink) => {
      let current: A[] | null = null;
      let closing: Inner | null = null;
      const openBuffer = (): void => {
        closing?.close();
        const b = current;
        current = [];
        if (b) sink.next(b);
        if (sink.closed) return;
        closing = subscribeInner(closingSelector(), sink, { next: openBuffer });
      };
      openBuffer();
      const src = subscribeChild(source, sink, {
        next: (a) => {
          current?.push(a);
        },
        complete: () => {
          if (current) sink.next(current);
          sink.complete();
        },
      });
      return () => {
        closing?.close();
        src.unsubscribe();
      };
    });

// ---------------------------------------------------------------------------
// window*
// ---------------------------------------------------------------------------

/** Opens a window: emits its read-only view downstream and returns the Subject. */
const openWindow = <A>(sink: Sink<Observable<A>>): Subject<A> => {
  const w = createSubject<A>();
  sink.next(w.asObservable());
  return w;
};

/**
 * Like `buffer`, but emits each chunk as an Observable when it opens. A new
 * window starts whenever `windowBoundaries` emits.
 */
export const window =
  <A>(windowBoundaries: ObservableInput<unknown>): Operator<A, Observable<A>> =>
  (source) =>
    observable<Observable<A>>((sink) => {
      let current = openWindow<A>(sink);
      const fail = (e: unknown): void => {
        current.error(e);
        sink.error(e);
      };
      const src = subscribeChild(source, sink, {
        next: (a) => current.next(a),
        error: fail,
        complete: () => {
          current.complete();
          sink.complete();
        },
      });
      const b = subscribeInner(windowBoundaries, sink, {
        next: () => {
          current.complete();
          current = openWindow<A>(sink);
        },
        error: fail,
      });
      return () => {
        src.unsubscribe();
        b.close();
        current.dispose();
      };
    });

/**
 * Windows of `windowSize` values, a new one starting every
 * `startWindowEvery` values (default: `windowSize`, back to back).
 */
export const windowCount =
  <A>(windowSize: number, startWindowEvery = 0): Operator<A, Observable<A>> =>
  (source) =>
    observable<Observable<A>>((sink) => {
      const startEvery = startWindowEvery > 0 ? startWindowEvery : windowSize;
      const windows: Subject<A>[] = [openWindow<A>(sink)];
      let count = 0;
      const src = subscribeChild(source, sink, {
        next: (a) => {
          for (const w of windows.slice()) w.next(a);
          const c = count - windowSize + 1;
          if (c >= 0 && c % startEvery === 0) windows.shift()?.complete();
          if (++count % startEvery === 0) windows.push(openWindow<A>(sink));
        },
        error: (e) => {
          while (windows.length > 0) windows.shift()?.error(e);
          sink.error(e);
        },
        complete: () => {
          while (windows.length > 0) windows.shift()?.complete();
          sink.complete();
        },
      });
      return () => {
        src.unsubscribe();
        windows.forEach((w) => w.dispose());
      };
    });

export interface WindowTimeConfig {
  /** Start a new window every `creationInterval` ms (overlapping windows). Default: when the previous one closes. */
  readonly creationInterval?: number | null;
  /** Close a window early once it has emitted this many values. */
  readonly maxWindowSize?: number;
}

/** Like `bufferTime`, but emits each period as an Observable when it opens. */
export const windowTime =
  <A>(timeSpan: number, scheduler: Scheduler = asyncScheduler, config: WindowTimeConfig = {}): Operator<A, Observable<A>> =>
  (source) =>
    observable<Observable<A>>((sink) => {
      const maxSize = config.maxWindowSize ?? Number.POSITIVE_INFINITY;
      const creationInterval = config.creationInterval ?? null;
      const restartOnClose = creationInterval === null || creationInterval < 0;
      interface Rec {
        readonly window: Subject<A>;
        seen: number;
        cancel: Teardown;
      }
      const records: Rec[] = [];
      let cancelCreation: Teardown = () => {};

      const close = (rec: Rec): void => {
        rec.window.complete();
        rec.cancel();
        removeFrom(records, rec);
        if (restartOnClose) start();
      };
      const start = (): void => {
        if (sink.closed) return;
        const rec: Rec = { window: openWindow<A>(sink), seen: 0, cancel: () => {} };
        records.push(rec);
        rec.cancel = scheduler.schedule(() => close(rec), timeSpan);
      };
      if (!restartOnClose) {
        const every = creationInterval as number;
        const tick = (): void => {
          start();
          if (!sink.closed) cancelCreation = scheduler.schedule(tick, every);
        };
        cancelCreation = scheduler.schedule(tick, every);
      }
      start();

      const terminate = (f: (o: { error: (e: unknown) => void; complete: () => void }) => void): void => {
        cancelCreation();
        const open = records.splice(0);
        open.forEach((r) => {
          r.cancel();
          f(r.window);
        });
        f(sink);
      };
      const src = subscribeChild(source, sink, {
        next: (a) => {
          for (const rec of records.slice()) {
            rec.window.next(a);
            if (++rec.seen >= maxSize) close(rec);
          }
        },
        error: (e) => terminate((o) => o.error(e)),
        complete: () => terminate((o) => o.complete()),
      });
      return () => {
        src.unsubscribe();
        cancelCreation();
        records.splice(0).forEach((r) => {
          r.cancel();
          r.window.dispose();
        });
      };
    });

/**
 * Opens a window whenever `openings` emits; each window completes when the
 * Observable returned by `closingSelector(openValue)` emits.
 */
export const windowToggle =
  <A, O>(openings: ObservableInput<O>, closingSelector: (openValue: O) => ObservableInput<unknown>): Operator<A, Observable<A>> =>
  (source) =>
    observable<Observable<A>>((sink) => {
      const windows: Subject<A>[] = [];
      const closings = new Set<Inner>();
      const fail = (e: unknown): void => {
        while (windows.length > 0) windows.shift()?.error(e);
        sink.error(e);
      };
      const open = subscribeInner(openings, sink, {
        next: (o) => {
          let closingInput: ObservableInput<unknown>;
          try {
            closingInput = closingSelector(o);
          } catch (e) {
            fail(e);
            return;
          }
          const w = openWindow<A>(sink);
          windows.push(w);
          const c = subscribeInner(closingInput, sink, {
            next: (_, self) => {
              self.close();
              closings.delete(self);
              removeFrom(windows, w);
              w.complete();
            },
            error: fail,
          });
          if (!c.closed) closings.add(c);
        },
        error: fail,
      });
      const src = subscribeChild(source, sink, {
        next: (a) => {
          for (const w of windows.slice()) w.next(a);
        },
        error: fail,
        complete: () => {
          while (windows.length > 0) windows.shift()?.complete();
          sink.complete();
        },
      });
      return () => {
        open.close();
        closings.forEach((c) => c.close());
        src.unsubscribe();
        windows.splice(0).forEach((w) => w.dispose());
      };
    });

/**
 * Opens a window immediately; whenever the Observable returned by
 * `closingSelector()` emits or completes, the window completes and the next
 * one opens.
 */
export const windowWhen =
  <A>(closingSelector: () => ObservableInput<unknown>): Operator<A, Observable<A>> =>
  (source) =>
    observable<Observable<A>>((sink) => {
      let current: Subject<A> | null = null;
      let closing: Inner | null = null;
      const fail = (e: unknown): void => {
        current?.error(e);
        sink.error(e);
      };
      const next = (): void => {
        closing?.close();
        current?.complete();
        if (sink.closed) return;
        current = openWindow<A>(sink);
        let closingInput: ObservableInput<unknown>;
        try {
          closingInput = closingSelector();
        } catch (e) {
          fail(e);
          return;
        }
        closing = subscribeInner(closingInput, sink, { next, complete: next, error: fail });
      };
      next();
      const src = subscribeChild(source, sink, {
        next: (a) => current?.next(a),
        error: fail,
        complete: () => {
          current?.complete();
          sink.complete();
        },
      });
      return () => {
        closing?.close();
        src.unsubscribe();
        current?.dispose();
      };
    });
