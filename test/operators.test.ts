import { describe, expect, it } from 'vitest';
import {
  auditTime,
  bufferCount,
  catchError,
  combineLatest,
  concat,
  concatMap,
  count,
  debounceTime,
  defaultIfEmpty,
  delay,
  distinct,
  distinctUntilChanged,
  dematerialize,
  exhaustMap,
  expand,
  filter,
  finalize,
  first,
  forkJoin,
  isEmptyError,
  isTimeoutError,
  last,
  map,
  materialize,
  merge,
  mergeMap,
  of,
  pairwise,
  race,
  reduce,
  repeat,
  retry,
  scan,
  skip,
  skipWhile,
  startWith,
  endWith,
  switchMap,
  take,
  takeLast,
  takeUntil,
  takeWhile,
  throttleTime,
  throwError,
  timeout,
  toArray,
  withLatestFrom,
  zip,
  EMPTY,
  interval,
  sampleTime,
  filterMap,
} from '../src/index.js';
import { none, some } from '../src/fp/index.js';
import { runMarbles } from '../src/testing/index.js';
import { collect } from './helpers.js';

describe('transformation', () => {
  it('map / scan / reduce', () => {
    runMarbles(({ cold, expectObservable }) => {
      const src = cold('-a-b-c|', { a: 1, b: 2, c: 3 });
      expectObservable(src.pipe(map((x) => x * 10))).toBe('-a-b-c|', { a: 10, b: 20, c: 30 });
      expectObservable(src.pipe(scan((acc, x) => acc + x, 0))).toBe('-a-b-c|', { a: 1, b: 3, c: 6 });
      expectObservable(src.pipe(reduce((acc, x) => acc + x, 0))).toBe('------(a|)', { a: 6 });
    });
  });

  it('pairwise / bufferCount / count / toArray / defaultIfEmpty', () => {
    expect(collect(of(1, 2, 3).pipe(pairwise()))).toEqual([[1, 2], [2, 3], '|']);
    expect(collect(of(1, 2, 3, 4, 5).pipe(bufferCount(2)))).toEqual([[1, 2], [3, 4], [5], '|']);
    expect(collect(of(1, 2, 3, 4).pipe(bufferCount(3, 1)))).toEqual([[1, 2, 3], [2, 3, 4], [3, 4], [4], '|']);
    expect(collect(of(1, 2, 3).pipe(count((x) => x > 1)))).toEqual([2, '|']);
    expect(collect(of(1, 2, 3).pipe(toArray()))).toEqual([[1, 2, 3], '|']);
    expect(collect(EMPTY.pipe(defaultIfEmpty('x')))).toEqual(['x', '|']);
  });

  it('materialize / dematerialize round-trip', () => {
    const src = concat(of(1, 2), throwError(() => new Error('e')));
    expect(collect(src.pipe(materialize(), dematerialize()))).toEqual(collect(src));
  });
});

describe('filtering', () => {
  it('filter narrows types and filterMap maps+filters', () => {
    const xs = of<(number | string)[]>(1, 'a', 2);
    const nums = xs.pipe(filter((x): x is number => typeof x === 'number'));
    expect(collect(nums)).toEqual([1, 2, '|']);
    expect(collect(of(1, 2, 3, 4).pipe(filterMap((x) => (x % 2 === 0 ? some(x * 10) : none))))).toEqual([20, 40, '|']);
  });

  it('take / takeLast / skip / takeWhile / skipWhile', () => {
    expect(collect(of(1, 2, 3, 4).pipe(take(2)))).toEqual([1, 2, '|']);
    expect(collect(of(1, 2).pipe(take(0)))).toEqual(['|']);
    expect(collect(of(1, 2, 3, 4).pipe(takeLast(2)))).toEqual([3, 4, '|']);
    expect(collect(of(1, 2, 3, 4).pipe(skip(2)))).toEqual([3, 4, '|']);
    expect(collect(of(1, 2, 3, 1).pipe(takeWhile((x) => x < 3)))).toEqual([1, 2, '|']);
    expect(collect(of(1, 2, 3, 1).pipe(takeWhile((x) => x < 3, true)))).toEqual([1, 2, 3, '|']);
    expect(collect(of(1, 2, 3, 1).pipe(skipWhile((x) => x < 3)))).toEqual([3, 1, '|']);
  });

  it('distinct / distinctUntilChanged', () => {
    expect(collect(of(1, 1, 2, 2, 1).pipe(distinctUntilChanged()))).toEqual([1, 2, 1, '|']);
    expect(collect(of(1, 1, 2, 2, 1).pipe(distinct()))).toEqual([1, 2, '|']);
  });

  it('first / last with EmptyError and defaults', () => {
    expect(collect(of(1, 2, 3).pipe(first((x) => x > 1)))).toEqual([2, '|']);
    expect(collect(of(1, 2, 3).pipe(last()))).toEqual([3, '|']);
    expect(collect(EMPTY.pipe(first(() => true, 'd')))).toEqual(['d', '|']);
    let err: unknown;
    EMPTY.pipe(first()).subscribe({ error: (e) => (err = e) });
    expect(isEmptyError(err)).toBe(true);
    EMPTY.pipe(last()).subscribe({ error: (e) => (err = e) });
    expect(isEmptyError(err)).toBe(true);
  });

  it('takeUntil', () => {
    runMarbles(({ cold, hot, expectObservable }) => {
      const src = cold('-a-b-c-d|');
      const stop = hot('-----x');
      expectObservable(src.pipe(takeUntil(stop))).toBe('-a-b-|');
    });
  });
});

describe('flattening strategies', () => {
  const marbles = {
    outer: '-a---b|',
    inner: 'x-y|',
  };

  it('mergeMap interleaves', () => {
    runMarbles(({ cold, expectObservable }) => {
      const outer = cold('-a-b|');
      const inner = cold('x--y|');
      expectObservable(outer.pipe(mergeMap(() => inner))).toBe('-x-xy-y|');
    });
  });

  it('concatMap queues', () => {
    runMarbles(({ cold, expectObservable }) => {
      const outer = cold(marbles.outer);
      const inner = cold('x---y|');
      expectObservable(outer.pipe(concatMap(() => inner))).toBe('-x---yx---y|');
    });
  });

  it('switchMap cancels the previous inner', () => {
    runMarbles(({ cold, expectObservable }) => {
      const outer = cold('-a-b|');
      const inner = cold('x--y|');
      expectObservable(outer.pipe(switchMap(() => inner))).toBe('-x-x--y|');
    });
  });

  it('exhaustMap ignores while busy', () => {
    runMarbles(({ cold, expectObservable }) => {
      const outer = cold('-a-b----c|');
      const inner = cold('x--y|');
      expectObservable(outer.pipe(exhaustMap(() => inner))).toBe('-x--y---x--y|');
    });
  });

  it('mergeMap with concurrency 1 behaves like concatMap', () => {
    runMarbles(({ cold, expectObservable }) => {
      const outer = cold('-ab|');
      const inner = cold('x-y|');
      expectObservable(outer.pipe(mergeMap(() => inner, 1))).toBe('-x-yx-y|');
    });
  });

  it('expand', () => {
    expect(collect(of(1).pipe(expand((x) => (x < 8 ? of(x * 2) : EMPTY))))).toEqual([1, 2, 4, 8, '|']);
  });

  it('an error in the projection goes to error', () => {
    const o = of(1).pipe(
      mergeMap((): never => {
        throw new Error('proj');
      }),
    );
    expect(collect(o)).toEqual(['#proj']);
  });
});

describe('combination', () => {
  it('merge / concat / startWith / endWith', () => {
    runMarbles(({ cold, expectObservable }) => {
      const a = cold('a-a|');
      const b = cold('-b-b|');
      expectObservable(merge(a, b)).toBe('abab|');
      expectObservable(concat(a, b)).toBe('a-a-b-b|');
    });
    expect(collect(of(2).pipe(startWith(0, 1), endWith(3)))).toEqual([0, 1, 2, 3, '|']);
  });

  it('combineLatest emits typed tuples', () => {
    runMarbles(({ cold, expectObservable }) => {
      const a = cold('a---b|', { a: 1, b: 2 });
      const b = cold('--x---y|', { x: 'x', y: 'y' });
      const c = combineLatest([a, b] as const);
      expectObservable(c).toBe('--p-q-r|', { p: [1, 'x'], q: [2, 'x'], r: [2, 'y'] });
    });
  });

  it('zip pairs by index', () => {
    expect(collect(zip([of(1, 2, 3), of('a', 'b')] as const))).toEqual([[1, 'a'], [2, 'b'], '|']);
  });

  it('forkJoin emits the last values once', () => {
    runMarbles(({ cold, expectObservable }) => {
      expectObservable(forkJoin([cold('a-b|'), cold('--x---y|')] as const)).toBe('-------(r|)', { r: ['b', 'y'] });
      expectObservable(forkJoin([cold('a|'), cold('|')] as const)).toBe('|');
    });
  });

  it('race picks the first to notify', () => {
    runMarbles(({ cold, expectObservable }) => {
      expectObservable(race(cold('---a|'), cold('-b-b|'))).toBe('-b-b|');
    });
  });

  it('withLatestFrom', () => {
    runMarbles(({ cold, hot, expectObservable }) => {
      const src = cold('-a--b--c|');
      const other = hot('--x----y|');
      expectObservable(src.pipe(withLatestFrom(other))).toBe('----b--c|', { b: ['b', 'x'], c: ['c', 'y'] });
    });
  });
});

describe('time (virtual scheduler, injected)', () => {
  it('delay', () => {
    runMarbles(({ cold, scheduler, expectObservable }) => {
      expectObservable(cold('a-b|').pipe(delay(2, scheduler))).toBe('--a-b|');
    });
  });

  it('debounceTime', () => {
    runMarbles(({ cold, scheduler, expectObservable }) => {
      expectObservable(cold('ab---c-d----|').pipe(debounceTime(2, scheduler))).toBe('---b-----d--|');
    });
  });

  it('debounceTime flushes on completion', () => {
    runMarbles(({ cold, scheduler, expectObservable }) => {
      expectObservable(cold('a-b|').pipe(debounceTime(5, scheduler))).toBe('---(b|)');
    });
  });

  it('throttleTime (leading)', () => {
    runMarbles(({ cold, scheduler, expectObservable }) => {
      expectObservable(cold('abc---de|').pipe(throttleTime(3, scheduler))).toBe('a-----d-|');
    });
  });

  it('auditTime', () => {
    runMarbles(({ cold, scheduler, expectObservable }) => {
      expectObservable(cold('abc---de---|').pipe(auditTime(3, scheduler))).toBe('---c-----e-|');
    });
  });

  it('sampleTime', () => {
    runMarbles(({ cold, scheduler, expectObservable }) => {
      expectObservable(cold('ab-c-----|').pipe(sampleTime(3, scheduler))).toBe('---b--c--|');
    });
  });

  it('timeout errors with TimeoutError', () => {
    runMarbles(({ cold, scheduler, expectObservable }) => {
      const err = Object.assign(new Error('Timeout has occurred'), { name: 'TimeoutError' });
      expectObservable(cold('a----b|').pipe(timeout(3, scheduler))).toBe('a--#', undefined, err);
    });
    let e: unknown;
    runMarbles(({ cold, scheduler }) => {
      cold('-----a|').pipe(timeout(2, scheduler)).subscribe({ error: (x) => (e = x) });
    });
    expect(isTimeoutError(e)).toBe(true);
  });

  it('interval on virtual time', () => {
    runMarbles(({ scheduler, expectObservable }) => {
      expectObservable(interval(2, scheduler).pipe(take(3))).toBe('--a-b-(c|)', { a: 0, b: 1, c: 2 });
    });
  });
});

describe('error handling', () => {
  it('catchError switches to a fallback', () => {
    runMarbles(({ cold, expectObservable }) => {
      expectObservable(cold('a-#').pipe(catchError(() => cold('x|')))).toBe('a-x|');
    });
  });

  it('retry resubscribes', () => {
    runMarbles(({ cold, expectObservable }) => {
      expectObservable(cold('a#').pipe(retry(2))).toBe('aaa#');
    });
  });

  it('retry with delay uses the scheduler', () => {
    runMarbles(({ cold, scheduler, expectObservable }) => {
      expectObservable(cold('a#').pipe(retry({ count: 1, delay: 2, scheduler }))).toBe('a--a#');
    });
  });

  it('repeat resubscribes on completion', () => {
    expect(collect(of(1, 2).pipe(repeat(3)))).toEqual([1, 2, 1, 2, 1, 2, '|']);
  });

  it('finalize runs on complete, error and unsubscribe', () => {
    const calls: string[] = [];
    of(1).pipe(finalize(() => calls.push('complete'))).subscribe();
    throwError(() => 'x').pipe(finalize(() => calls.push('error'))).subscribe({ error: () => {} });
    const sub = interval(1000).pipe(finalize(() => calls.push('unsubscribe'))).subscribe();
    sub.unsubscribe();
    expect(calls).toEqual(['complete', 'error', 'unsubscribe']);
  });
});
