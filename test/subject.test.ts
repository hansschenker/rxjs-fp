import { describe, expect, it } from 'vitest';
import {
  createAsyncSubject,
  createBehaviorSubject,
  createReplaySubject,
  createSubject,
  isObjectUnsubscribedError,
  map,
  observable,
  of,
  share,
  shareReplay,
  take,
} from '../src/index.js';
import { runMarbles } from '../src/testing/index.js';
import { collect } from './helpers.js';

describe('subjects', () => {
  it('Subject multicasts and replays the terminal notification to late subscribers', () => {
    const s = createSubject<number>();
    const a: number[] = [];
    const b: number[] = [];
    s.subscribe((x) => a.push(x));
    s.next(1);
    s.subscribe((x) => b.push(x));
    s.next(2);
    s.complete();
    expect(a).toEqual([1, 2]);
    expect(b).toEqual([2]);
    expect(collect(s)).toEqual(['|']);
  });

  it('observed reflects subscribers; dispose makes further use throw', () => {
    const s = createSubject<number>();
    expect(s.observed).toBe(false);
    const sub = s.subscribe();
    expect(s.observed).toBe(true);
    sub.unsubscribe();
    expect(s.observed).toBe(false);
    s.dispose();
    let caught: unknown;
    try {
      s.next(1);
    } catch (e) {
      caught = e;
    }
    expect(isObjectUnsubscribedError(caught)).toBe(true);
  });

  it('BehaviorSubject has a current value', () => {
    const s = createBehaviorSubject(0);
    const seen: number[] = [];
    s.subscribe((x) => seen.push(x));
    s.next(1);
    expect(s.value).toBe(1);
    expect(collect(s.pipe(take(1)))).toEqual([1, '|']);
    expect(seen).toEqual([0, 1]);
  });

  it('ReplaySubject replays a bounded buffer', () => {
    const s = createReplaySubject<number>(2);
    s.next(1);
    s.next(2);
    s.next(3);
    expect(collect(s.pipe(take(2)))).toEqual([2, 3, '|']);
  });

  it('AsyncSubject emits the last value on completion', () => {
    const s = createAsyncSubject<number>();
    const seen: Array<number | string> = [];
    s.subscribe({ next: (x) => seen.push(x), complete: () => seen.push('|') });
    s.next(1);
    s.next(2);
    expect(seen).toEqual([]);
    s.complete();
    expect(seen).toEqual([2, '|']);
    expect(collect(s)).toEqual([2, '|']);
  });

  it('asObservable hides the observer side', () => {
    const view = createSubject<number>().asObservable() as unknown as Record<string, unknown>;
    expect(view['next']).toBeUndefined();
  });
});

describe('share / shareReplay (cold → hot, explicitly)', () => {
  it('share runs the source once for concurrent subscribers', () => {
    let runs = 0;
    runMarbles(({ cold, expectObservable }) => {
      const src = cold('-a-b-c|');
      const counted = observable<string>((sink) => {
        runs++;
        return src.subscribe(sink);
      });
      const shared = counted.pipe(share());
      expectObservable(shared).toBe('-a-b-c|');
      expectObservable(shared.pipe(map((x) => x.toUpperCase()))).toBe('-A-B-C|');
    });
    expect(runs).toBe(1);
  });

  it('share resets after completion (re-subscribes)', () => {
    let runs = 0;
    const src = observable<number>((sink) => {
      runs++;
      sink.next(runs);
      sink.complete();
    }).pipe(share());
    expect(collect(src)).toEqual([1, '|']);
    expect(collect(src)).toEqual([2, '|']);
  });

  it('shareReplay keeps the buffer after completion', () => {
    let runs = 0;
    const src = observable<number>((sink) => {
      runs++;
      sink.next(10);
      sink.next(20);
      sink.complete();
    }).pipe(shareReplay(1));
    expect(collect(src)).toEqual([10, 20, '|']);
    expect(collect(src)).toEqual([20, '|']);
    expect(runs).toBe(1);
  });

  it('plain observables stay cold', () => {
    const o = of(1, 2);
    expect(collect(o)).toEqual(collect(o));
  });
});
