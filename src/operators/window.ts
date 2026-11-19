import { observable, subscribeChild } from '../core/observable.js';
import type { Observable, Operator, Sink } from '../core/types.js';
import type { ObservableInput } from '../creation/creation.js';
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
 * Kernel: not `windowWhen` with `toArray`. RxJS 7 opens a new window when the
 * closing notifier completes, but ignores that completion for a buffer.
 *
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
