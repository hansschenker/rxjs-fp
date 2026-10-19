import { describe, expect, it } from 'vitest';
import * as R from 'rxjs';
import {
  EMPTY,
  auditTime,
  catchError,
  concat,
  createReplaySubject,
  createSubject,
  createVirtualScheduler,
  defer,
  delay,
  elementAt,
  first,
  from,
  isArgumentOutOfRangeError,
  isObjectUnsubscribedError,
  observable,
  of,
  share,
  switchMap,
  take,
  throttleTime,
  unfold,
  type Observable,
} from '../src/index.js';
import { some } from '../src/fp/index.js';
import { record } from '../src/testing/index.js';
import { collect } from './helpers.js';

/** Regression tests for the bugs confirmed by the independent review against RxJS 7.8. */

const counting = (n: number) => {
  let produced = 0;
  const src = observable<number>((sink) => {
    for (let i = 0; i < n && !sink.closed; i++) {
      produced++;
      sink.next(i);
    }
    sink.complete();
  });
  return { src, produced: () => produced };
};

describe('share', () => {
  it('stops a synchronous source when the last subscriber leaves (was: ran all 1000 values)', () => {
    const c = counting(1000);
    expect(collect(c.src.pipe(share(), take(1)))).toEqual([0, '|']);
    expect(c.produced()).toBe(1);
  });

  it('does not connect twice for a re-entrant subscriber', () => {
    let connections = 0;
    const shared = defer(() => {
      connections++;
      return of(1, 2, 3);
    }).pipe(share());
    const log: string[] = [];
    shared.subscribe({
      next: (x) => {
        log.push(`A${x}`);
        if (x === 1) shared.subscribe({ next: (y) => log.push(`B${y}`), complete: () => log.push('BC') });
      },
      complete: () => log.push('AC'),
    });
    expect(log).toEqual(['A1', 'A2', 'B2', 'A3', 'B3', 'AC', 'BC']);
    expect(connections).toBe(1);
  });

  it('works on an endless synchronous source', () => {
    const naturals = unfold(0, (n) => some([n, n + 1] as const));
    expect(collect(naturals.pipe(share(), take(2)))).toEqual([0, 1, '|']);
  });
});

describe('switchMap', () => {
  it('cancels the old inner when a new outer value arrives re-entrantly', () => {
    const outer = createSubject<number>();
    const late = createSubject<string>();
    const log: string[] = [];
    outer
      .pipe(switchMap((n) => (n === 1 ? concat(of('a1'), late) : of(`b${n}`))))
      .subscribe({
        next: (v) => {
          log.push(v);
          if (v === 'a1') outer.next(2);
        },
        complete: () => log.push('C'),
      });
    outer.next(1);
    late.next('LATE');
    outer.complete();
    expect(log).toEqual(['a1', 'b2', 'C']);
  });
});

describe('time operators', () => {
  it('throttleTime leaves no timer after downstream closes', () => {
    const vs = createVirtualScheduler();
    const s = createSubject<number>();
    s.pipe(throttleTime(10, vs), take(1)).subscribe();
    s.next(1);
    expect(vs.pending()).toBe(0);
  });

  it('throttleTime with trailing defers completion to the end of the window', () => {
    const vs = createVirtualScheduler();
    const src = observable<number>((sink) => {
      sink.next(1);
      vs.schedule(() => sink.next(2), 2);
      vs.schedule(() => sink.complete(), 4);
    });
    const out = record(src.pipe(throttleTime(10, vs, { leading: true, trailing: true })), vs);
    vs.flush();
    expect(out).toEqual([
      { frame: 0, kind: 'N', value: 1 },
      { frame: 10, kind: 'N', value: 2 },
      { frame: 10, kind: 'C' },
    ]);
  });

  it('auditTime emits the pending value when the source completes mid-window', () => {
    const vs = createVirtualScheduler();
    const src = observable<number>((sink) => {
      sink.next(1);
      vs.schedule(() => sink.complete(), 5);
    });
    const out = record(src.pipe(auditTime(10, vs)), vs);
    vs.flush();
    expect(out).toEqual([
      { frame: 10, kind: 'N', value: 1 },
      { frame: 10, kind: 'C' },
    ]);
  });

  it('delay completes with the last delayed value, and immediately for EMPTY', () => {
    const vs = createVirtualScheduler();
    const src = observable<number>((sink) => {
      sink.next(1);
      vs.schedule(() => sink.complete(), 50);
    });
    const out = record(src.pipe(delay(100, vs)), vs);
    const empty = record(EMPTY.pipe(delay(100, vs)), vs);
    vs.flush();
    expect(out).toEqual([
      { frame: 100, kind: 'N', value: 1 },
      { frame: 100, kind: 'C' },
    ]);
    expect(empty).toEqual([{ frame: 0, kind: 'C' }]);
  });
});

describe('interop: unsubscription crosses the boundary synchronously', () => {
  it('RxJS → rxjs-fp: take(1) stops a synchronous RxJS source', () => {
    let produced = 0;
    const theirs = new R.Observable<number>((s) => {
      for (let i = 0; i < 100_000 && !s.closed; i++) {
        produced++;
        s.next(i);
      }
      s.complete();
    });
    expect(collect(from<number>(theirs).pipe(take(1)))).toEqual([0, '|']);
    expect(produced).toBe(1);
  });

  it('rxjs-fp → RxJS: R.take(1) stops a synchronous rxjs-fp source', () => {
    const c = counting(100_000);
    const values: number[] = [];
    R.from(c.src as unknown as R.InteropObservable<number>)
      .pipe(R.take(1))
      .subscribe((v) => values.push(v));
    expect(values).toEqual([0]);
    expect(c.produced()).toBe(1);
  });

  it('a producer may return a foreign Subscription as teardown', () => {
    let torn = false;
    const o = observable<number>(
      () =>
        new R.Subscription(() => {
          torn = true;
        }) as never,
    );
    o.subscribe().unsubscribe();
    expect(torn).toBe(true);
  });
});

describe('smaller semantic fixes', () => {
  it('ReplaySubject drops values exactly at the windowTime boundary', () => {
    const vs = createVirtualScheduler();
    const s = createReplaySubject<number>(Infinity, 10, vs);
    s.next(1);
    vs.advanceBy(10);
    expect(collect(s.pipe(take(1)))).toEqual([]);
  });

  it("catchError's `caught` is the resulting Observable (retry with handling in place)", () => {
    let n = 0;
    const src: Observable<number> = observable((sink) => {
      sink.next(++n);
      sink.error('x');
    });
    expect(collect(src.pipe(catchError((_e, caught) => caught), take(4)))).toEqual([1, 2, 3, 4, '|']);
  });

  it('from(iterable) does not call return() after normal exhaustion', () => {
    let returns = 0;
    const iterable: Iterable<number> = {
      [Symbol.iterator]: () => {
        let i = 0;
        return {
          next: () => (i < 2 ? { value: i++, done: false } : { value: undefined, done: true }),
          return: () => {
            returns++;
            return { value: undefined, done: true };
          },
        };
      },
    };
    expect(collect(from(iterable))).toEqual([0, 1, '|']);
    expect(returns).toBe(0);
    collect(from(iterable).pipe(take(1)));
    expect(returns).toBe(1);
  });

  it('elementAt uses ArgumentOutOfRangeError and supports a default', () => {
    let err: unknown;
    of(1).pipe(elementAt(3)).subscribe({ error: (e) => (err = e) });
    expect(isArgumentOutOfRangeError(err)).toBe(true);
    expect(collect(of(1).pipe(elementAt(3, 'd')))).toEqual(['d', '|']);
    expect(() => elementAt(-1)).toThrow();
  });

  it('subscribing to a disposed Subject throws synchronously', () => {
    const s = createSubject<number>();
    s.dispose();
    let caught: unknown;
    try {
      s.subscribe();
    } catch (e) {
      caught = e;
    }
    expect(isObjectUnsubscribedError(caught)).toBe(true);
  });

  it('first accepts null as predicate with a default', () => {
    expect(collect(EMPTY.pipe(first(null, 'd')))).toEqual(['d', '|']);
  });

  it('the virtual scheduler stops a zero-delay infinite loop instead of hanging', () => {
    const vs = createVirtualScheduler();
    const loop = (): void => {
      vs.schedule(loop, 0);
    };
    vs.schedule(loop, 0);
    expect(() => vs.flush(Infinity, 1000)).toThrow(/zero-delay/);
  });
});
