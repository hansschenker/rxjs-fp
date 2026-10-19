import { observable } from '../core/observable.js';
import type { Observable, Subscription, Teardown } from '../core/types.js';
import { createVirtualScheduler, type VirtualScheduler } from '../scheduler/scheduler.js';
import { createSubject } from '../subject/subject.js';

/**
 * Marble testing on virtual time (the idea André Staltz brought to RxJS 5).
 *
 * Syntax (1 character = 1 frame = 1 virtual millisecond):
 *   ' '   ignored (use it to align diagrams)
 *   '-'   one frame passes
 *   'a'   a value (looked up in `values`, else the character itself)
 *   '|'   complete
 *   '#'   error (the `error` argument, default 'error')
 *   '(ab)' synchronous group: all events at the frame of '('
 *   '^'   subscription point (hot observables, subscription marbles)
 *   '!'   unsubscription point (subscription marbles)
 *
 * The scheduler is passed explicitly to any time-based operator under
 * test: `debounceTime(3, scheduler)`. Nothing global is patched.
 */

export type RecordedEvent<A = unknown> =
  | { readonly frame: number; readonly kind: 'N'; readonly value: A }
  | { readonly frame: number; readonly kind: 'E'; readonly error: unknown }
  | { readonly frame: number; readonly kind: 'C' };

export interface ParsedMarbles<A> {
  readonly events: ReadonlyArray<RecordedEvent<A>>;
  readonly subscribeFrame: number;
  readonly unsubscribeFrame: number;
}

/** Parses a marble diagram into timed events. Pure. */
export const parseMarbles = <A = string>(
  marbles: string,
  values?: Readonly<Record<string, A>>,
  error: unknown = 'error',
): ParsedMarbles<A> => {
  const events: RecordedEvent<A>[] = [];
  let frame = 0;
  let groupFrame = -1;
  let subscribeFrame = -1;
  let unsubscribeFrame = -1;
  const at = (): number => (groupFrame >= 0 ? groupFrame : frame);
  const advance = (): void => {
    if (groupFrame < 0) frame++;
  };
  for (const c of marbles) {
    switch (c) {
      case ' ':
        break;
      case '-':
        advance();
        break;
      case '(':
        if (groupFrame >= 0) throw new Error(`rxjs-fp marbles: nested group in "${marbles}"`);
        groupFrame = frame;
        frame++;
        break;
      case ')':
        if (groupFrame < 0) throw new Error(`rxjs-fp marbles: unmatched ")" in "${marbles}"`);
        groupFrame = -1;
        frame++;
        break;
      case '^':
        subscribeFrame = at();
        advance();
        break;
      case '!':
        unsubscribeFrame = at();
        advance();
        break;
      case '|':
        events.push({ frame: at(), kind: 'C' });
        if (groupFrame >= 0) frame++;
        advance();
        break;
      case '#':
        events.push({ frame: at(), kind: 'E', error });
        if (groupFrame >= 0) frame++;
        advance();
        break;
      default: {
        const value = values && c in values ? (values[c] as A) : (c as unknown as A);
        events.push({ frame: at(), kind: 'N', value });
        if (groupFrame >= 0) frame++;
        advance();
      }
    }
  }
  if (groupFrame >= 0) throw new Error(`rxjs-fp marbles: unclosed group in "${marbles}"`);
  return { events, subscribeFrame, unsubscribeFrame };
};

/** Subscribes to `source` on `scheduler` and records every notification with its virtual time. */
export const record = <A>(
  source: Observable<A>,
  scheduler: VirtualScheduler,
  subscribeAt = 0,
  unsubscribeAt = -1,
): RecordedEvent<A>[] => {
  const out: RecordedEvent<A>[] = [];
  let sub: Subscription | null = null;
  scheduler.schedule(() => {
    sub = source.subscribe({
      next: (value) => out.push({ frame: scheduler.now(), kind: 'N', value }),
      error: (error) => out.push({ frame: scheduler.now(), kind: 'E', error }),
      complete: () => out.push({ frame: scheduler.now(), kind: 'C' }),
    });
  }, subscribeAt);
  if (unsubscribeAt >= 0) scheduler.schedule(() => sub?.unsubscribe(), unsubscribeAt);
  return out;
};

const isErrorLike = (x: unknown): x is Error =>
  typeof x === 'object' && x !== null && typeof (x as Error).message === 'string' && typeof (x as Error).name === 'string' && 'stack' in x;

/** Structural equality used by `toBe` (Errors compare by name and message). */
export const deepEqual = (x: unknown, y: unknown): boolean => {
  if (Object.is(x, y)) return true;
  if (isErrorLike(x) || isErrorLike(y)) {
    return isErrorLike(x) && isErrorLike(y) && x.name === y.name && x.message === y.message;
  }
  if (typeof x !== 'object' || typeof y !== 'object' || x === null || y === null) return false;
  if (Array.isArray(x) !== Array.isArray(y)) return false;
  const kx = Object.keys(x);
  const ky = Object.keys(y);
  if (kx.length !== ky.length) return false;
  return kx.every((k) => deepEqual((x as Record<string, unknown>)[k], (y as Record<string, unknown>)[k]));
};

const show = (events: ReadonlyArray<RecordedEvent>): string =>
  events
    .map((e) =>
      e.kind === 'N' ? `${e.frame}:${JSON.stringify(e.value)}` : e.kind === 'E' ? `${e.frame}:#${String((e.error as Error)?.message ?? e.error)}` : `${e.frame}:|`,
    )
    .join('  ');

export interface MarbleHelpers {
  /** The virtual scheduler to pass to time-based operators. */
  readonly scheduler: VirtualScheduler;
  /** A cold Observable: the diagram restarts at every subscription. */
  readonly cold: <A = string>(marbles: string, values?: Readonly<Record<string, A>>, error?: unknown) => Observable<A>;
  /** A hot Observable: events happen at fixed times relative to '^', shared by all subscribers. */
  readonly hot: <A = string>(marbles: string, values?: Readonly<Record<string, A>>, error?: unknown) => Observable<A>;
  /** Registers an expectation, checked after the virtual clock has been flushed. */
  readonly expectObservable: <A>(
    source: Observable<A>,
    subscriptionMarbles?: string,
  ) => { readonly toBe: (marbles: string, values?: Readonly<Record<string, unknown>>, error?: unknown) => void };
  /** Number of frames until the first '|' in a diagram (handy for durations). */
  readonly time: (marbles: string) => number;
}

export interface MarbleOptions {
  /** Stop flushing after this many frames (protects against infinite streams). Default 1000. */
  readonly maxFrames?: number;
}

/**
 * Runs a marble test. Throws an Error describing the first mismatch, so it
 * works with any test runner.
 */
export const runMarbles = (body: (helpers: MarbleHelpers) => void, options: MarbleOptions = {}): void => {
  const scheduler = createVirtualScheduler();
  const checks: Array<() => string | null> = [];

  const cold = <A = string>(marbles: string, values?: Readonly<Record<string, A>>, error?: unknown): Observable<A> => {
    const { events } = parseMarbles<A>(marbles, values, error);
    return observable<A>((sink) => {
      const cancels: Teardown[] = events.map((e) =>
        scheduler.schedule(() => {
          if (e.kind === 'N') sink.next(e.value);
          else if (e.kind === 'E') sink.error(e.error);
          else sink.complete();
        }, e.frame),
      );
      return () => cancels.forEach((c) => c());
    });
  };

  const hot = <A = string>(marbles: string, values?: Readonly<Record<string, A>>, error?: unknown): Observable<A> => {
    const { events, subscribeFrame } = parseMarbles<A>(marbles, values, error);
    const offset = Math.max(0, subscribeFrame);
    const subject = createSubject<A>();
    for (const e of events) {
      const t = e.frame - offset;
      if (t < 0) continue;
      scheduler.schedule(() => {
        if (e.kind === 'N') subject.next(e.value);
        else if (e.kind === 'E') subject.error(e.error);
        else subject.complete();
      }, t);
    }
    return subject.asObservable();
  };

  const expectObservable: MarbleHelpers['expectObservable'] = (source, subscriptionMarbles) => {
    const sub = subscriptionMarbles ? parseMarbles(subscriptionMarbles) : null;
    const actual = record(source, scheduler, Math.max(0, sub?.subscribeFrame ?? 0), sub?.unsubscribeFrame ?? -1);
    return {
      toBe: (marbles, values, error) => {
        const expected = parseMarbles(marbles, values, error).events;
        checks.push(() => {
          const ok =
            actual.length === expected.length &&
            actual.every((a, i) => {
              const e = expected[i]!;
              if (a.frame !== e.frame || a.kind !== e.kind) return false;
              if (a.kind === 'N' && e.kind === 'N') return deepEqual(a.value, e.value);
              if (a.kind === 'E' && e.kind === 'E') return deepEqual(a.error, e.error);
              return true;
            });
          return ok ? null : `Marble mismatch for "${marbles}"\n  expected: ${show(expected)}\n  actual:   ${show(actual)}`;
        });
      },
    };
  };

  const time = (marbles: string): number => {
    const c = parseMarbles(marbles).events.find((e) => e.kind === 'C');
    return c ? c.frame : marbles.replace(/ /g, '').length;
  };

  body({ scheduler, cold, hot, expectObservable, time });
  scheduler.flush(options.maxFrames ?? 1000);
  const failures = checks.map((c) => c()).filter((x): x is string => x !== null);
  if (failures.length > 0) throw new Error(failures.join('\n\n'));
};
