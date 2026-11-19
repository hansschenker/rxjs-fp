import { describe, expect, it } from 'vitest';
import {
  EMPTY,
  concat,
  map,
  mergeMap,
  of,
  take,
  tap,
  toArray,
  windowCount,
  windowTime,
  windowTimeEvery,
  windowTimeEveryOrCount,
  windowTimeOrCount,
  type Observable,
} from '../src/index.js';
import { runMarbles } from '../src/testing/index.js';

/** Every window's open, values and close, in delivery order. */
const tagged = <A>(w$: Observable<Observable<A>>): Observable<string> =>
  w$.pipe(
    mergeMap((w: Observable<A>, i: number) => concat(of(`${i}:open`), w.pipe(map((v) => `${i}:${String(v)}`)), of(`${i}:close`))),
  );

type Helpers = Parameters<Parameters<typeof runMarbles>[0]>[0];

/** Runs `build` on virtual time and returns `frame:notification` strings. */
const trace = (build: (h: Helpers) => Observable<string>): string[] => {
  const out: string[] = [];
  runMarbles((h) => {
    build(h).subscribe({
      next: (v) => out.push(`${h.scheduler.now()}:${v}`),
      error: () => out.push(`${h.scheduler.now()}:#`),
      complete: () => out.push(`${h.scheduler.now()}:|`),
    });
  });
  return out;
};

const collect = (source: Observable<string>): string[] => {
  const out: string[] = [];
  source.subscribe({ next: (v) => out.push(v), complete: () => out.push('|') });
  return out;
};

describe('windowCount is a composition over windowToggle', () => {
  it('counts a synchronous source and opens the trailing empty window RxJS opens', () => {
    expect(collect(of(1, 2, 3).pipe(windowCount(3), tagged))).toEqual(['0:open', '0:1', '0:2', '0:3', '0:close', '1:open', '1:close', '|']);
  });

  it('overlapping windows never see the value that opened them and always see the one that closes them', () => {
    expect(collect(of(1, 2, 3, 4).pipe(windowCount(2, 1), tagged))).toEqual([
      '0:open',
      '0:1',
      '1:open',
      '0:2',
      '1:2',
      '0:close',
      '2:open',
      '1:3',
      '2:3',
      '1:close',
      '3:open',
      '2:4',
      '3:4',
      '2:close',
      '4:open',
      '3:close',
      '4:close',
      '|',
    ]);
  });

  it('gaps: windows shorter than the start interval', () => {
    const out: number[][] = [];
    of(1, 2, 3, 4, 5)
      .pipe(
        windowCount(1, 2),
        mergeMap((w) => w.pipe(toArray())),
      )
      .subscribe((a) => out.push(a));
    expect(out).toEqual([[1], [3], [5]]);
  });

  it('an empty source completes without re-subscribing the opening count forever', () => {
    expect(collect(EMPTY.pipe(windowCount(2, 1), tagged))).toEqual(['0:open', '0:close', '|']);
  });

  it('a consumer that stops after the first window never starts the source', () => {
    let seen = 0;
    of(1, 2, 3)
      .pipe(
        tap(() => seen++),
        windowCount(2),
        take(1),
      )
      .subscribe();
    expect(seen).toBe(0);
  });
});

describe('windowTime… are compositions over windowWhen / windowToggle', () => {
  it('windowTime: the boundary timer runs before a value at the same frame, so the value lands in the new window', () => {
    expect(trace(({ cold, scheduler }) => cold('---a|').pipe(windowTime(3, scheduler), tagged))).toEqual([
      '0:0:open',
      '3:0:close',
      '3:1:open',
      '3:1:a',
      '4:1:close',
      '4:|',
    ]);
  });

  it('windowTimeOrCount closes a full window at once and the next opens in the same frame', () => {
    expect(trace(({ cold, scheduler }) => cold('ab-c|').pipe(windowTimeOrCount(5, 2, scheduler), tagged))).toEqual([
      '0:0:open',
      '0:0:a',
      '1:0:b',
      '1:0:close',
      '1:1:open',
      '3:1:c',
      '4:1:close',
      '4:|',
    ]);
  });

  it('windowTimeEvery: same-frame ties follow scheduling order', () => {
    // frame 3: the interval, scheduled first, ticks before the first window's timer;
    // frame 6: the second window's timer, scheduled at 3 before the interval re-armed, fires before the interval.
    expect(trace(({ cold, scheduler }) => cold('-------|').pipe(windowTimeEvery(3, 3, scheduler), tagged))).toEqual([
      '0:0:open',
      '3:1:open',
      '3:0:close',
      '6:1:close',
      '6:2:open',
      '7:2:close',
      '7:|',
    ]);
  });

  it('windowTimeEveryOrCount: a value reaches every open window before the full one closes (documented difference from RxJS)', () => {
    expect(trace(({ cold, scheduler }) => cold('-a-a|').pipe(windowTimeEveryOrCount(4, 3, 2, scheduler), tagged))).toEqual([
      '0:0:open',
      '1:0:a',
      '3:1:open',
      '3:0:a',
      '3:1:a',
      '3:0:close',
      '4:1:close',
      '4:|',
    ]);
  });
});
