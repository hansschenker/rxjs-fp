import { describe, expect, expectTypeOf, it } from 'vitest';
import * as rx from 'rxjs';
import {
  EMPTY,
  NEVER,
  animationFrames,
  audit,
  bindCallback,
  bindNodeCallback,
  connectable,
  createQueueScheduler,
  createReplaySubject,
  createSubject,
  createVirtualScheduler,
  every,
  fromEventPattern,
  generate,
  groupBy,
  isEmptyError,
  isNotFoundError,
  isSequenceError,
  map,
  mergeMap,
  of,
  onErrorResumeNext,
  partition,
  scheduled,
  single,
  take,
  throttle,
  throwError,
  toArray,
  using,
  windowCount,
  type GroupedObservable,
  type Observable,
} from '../src/index.js';
import { collect } from './helpers.js';

describe('creation', () => {
  it('generate is a for loop', () => {
    expect(collect(generate({ initialState: 1, condition: (x) => x < 20, iterate: (x) => x * 2 }))).toEqual([1, 2, 4, 8, 16, '|']);
    const squares = generate({ initialState: 0, condition: (x) => x < 3, iterate: (x) => x + 1, resultSelector: (x) => `${x * x}` });
    expectTypeOf(squares).toEqualTypeOf<Observable<string>>();
    expect(collect(squares)).toEqual(['0', '1', '4', '|']);
    // infinite without a condition: take(n) stops it
    expect(collect(generate({ initialState: 0, iterate: (x) => x + 1 }).pipe(take(3)))).toEqual([0, 1, 2, '|']);
  });

  it('generate and scheduled with a scheduler emit one value per task', () => {
    const s = createVirtualScheduler();
    const out: unknown[] = [];
    generate({ initialState: 0, condition: (x) => x < 3, iterate: (x) => x + 1, scheduler: s }).subscribe({
      next: (v) => out.push(v),
      complete: () => out.push('|'),
    });
    expect(out).toEqual([]);
    s.flush();
    expect(out).toEqual([0, 1, 2, '|']);

    const out2: unknown[] = [];
    scheduled([1, 2], s).subscribe((v) => out2.push(v));
    scheduled(of('x'), s).subscribe((v) => out2.push(v));
    expect(out2).toEqual([]);
    s.flush();
    // same order as RxJS: an Observable input needs two hops (subscribeOn, observeOn)
    expect(out2).toEqual([1, 2, 'x']);
  });

  it('using releases the resource when the execution ends', () => {
    const log: string[] = [];
    const o = using(
      () => ({ unsubscribe: () => log.push('released') }),
      () => of(1, 2),
    );
    expect(collect(o)).toEqual([1, 2, '|']);
    expect(log).toEqual(['released']);
    using(() => ({ unsubscribe: () => log.push('released again') }), () => NEVER).subscribe().unsubscribe();
    expect(log).toEqual(['released', 'released again']);
  });

  it('fromEventPattern adds and removes the handler', () => {
    const handlers = new Set<(...a: unknown[]) => void>();
    const o = fromEventPattern<number | unknown[]>(
      (h) => {
        handlers.add(h);
        return 'token';
      },
      (h, token) => {
        expect(token).toBe('token');
        handlers.delete(h);
      },
    );
    const out: unknown[] = [];
    const sub = o.subscribe((v) => out.push(v));
    handlers.forEach((h) => h(1));
    handlers.forEach((h) => h(2, 3));
    sub.unsubscribe();
    expect(handlers.size).toBe(0);
    expect(out).toEqual([1, [2, 3]]);
  });

  it('bindCallback calls once and replays to later subscribers', () => {
    let calls = 0;
    const add = (a: number, b: number, cb: (sum: number) => void): void => {
      calls++;
      cb(a + b);
    };
    const bound = bindCallback(add);
    const sum$ = bound(1, 2);
    expectTypeOf(sum$).toEqualTypeOf<Observable<number>>();
    expect(collect(sum$)).toEqual([3, '|']);
    expect(collect(sum$)).toEqual([3, '|']);
    expect(calls).toBe(1);
    const pair = bindCallback((cb: (a: string, b: number) => void) => cb('x', 1))();
    expectTypeOf(pair).toEqualTypeOf<Observable<[string, number]>>();
    expect(collect(pair)).toEqual([['x', 1], '|']);
  });

  it('bindNodeCallback turns err into an error notification', () => {
    const read = (path: string, cb: (err: Error | null, data?: string) => void): void =>
      path === 'ok' ? cb(null, 'data') : cb(new Error('ENOENT'));
    const read$ = bindNodeCallback(read);
    expect(collect(read$('ok'))).toEqual(['data', '|']);
    expect(collect(read$('missing'))).toEqual(['#ENOENT']);
  });

  it('bindCallback with a scheduler delivers asynchronously', () => {
    const s = createVirtualScheduler();
    const out: unknown[] = [];
    bindCallback((cb: (x: number) => void) => cb(7), s)().subscribe((v) => out.push(v));
    expect(out).toEqual([]);
    s.flush();
    expect(out).toEqual([7]);
  });

  it('partition splits by predicate, with refinements', () => {
    const [nums, strs] = partition(of<(number | string)[]>(1, 'a', 2), (x): x is number => typeof x === 'number');
    expectTypeOf(nums).toEqualTypeOf<Observable<number>>();
    expectTypeOf(strs).toEqualTypeOf<Observable<string>>();
    expect(collect(nums)).toEqual([1, 2, '|']);
    expect(collect(strs)).toEqual(['a', '|']);
  });

  it('onErrorResumeNext skips over errors and always completes', () => {
    const o = onErrorResumeNext(of(1), throwError(() => new Error('x')), of(2));
    expectTypeOf(o).toEqualTypeOf<Observable<number>>();
    expect(collect(o)).toEqual([1, 2, '|']);
  });

  it('animationFrames emits elapsed time per frame', () => {
    const v = createVirtualScheduler();
    const frames = { now: v.now, schedule: (work: () => void) => v.schedule(work, 16) };
    const out: unknown[] = [];
    animationFrames(frames).pipe(take(3), map((f) => f.elapsed)).subscribe((x) => out.push(x));
    v.flush();
    expect(out).toEqual([16, 32, 48]);
  });
});

describe('queueScheduler', () => {
  it('runs synchronously but trampolines nested work', () => {
    const q = createQueueScheduler();
    const log: string[] = [];
    q.schedule(() => {
      log.push('outer start');
      q.schedule(() => log.push('inner'));
      log.push('outer end');
    });
    expect(log).toEqual(['outer start', 'outer end', 'inner']);
  });

  it('turns deep recursion into a loop', () => {
    const q = createQueueScheduler();
    let n = 0;
    const step = (): void => {
      if (++n < 100_000) q.schedule(step);
    };
    q.schedule(step);
    expect(n).toBe(100_000);
  });

  it('matches RxJS ordering for scheduled arrays', () => {
    const q = createQueueScheduler();
    const ours = collect(scheduled([1, 2, 3], q));
    const theirs: unknown[] = [];
    rx.scheduled([1, 2, 3], rx.queueScheduler).subscribe({ next: (v) => theirs.push(v), complete: () => theirs.push('|') });
    expect(ours).toEqual(theirs);
  });
});

describe('connectable', () => {
  it('starts on connect() and multicasts', () => {
    // without resetOnDisconnect the replayed values outlive the finished connection
    const c = connectable(of(1, 2), { connector: () => createReplaySubject<number>(), resetOnDisconnect: false });
    const a: unknown[] = [];
    const b: unknown[] = [];
    c.subscribe((v) => a.push(v));
    expect(a).toEqual([]);
    c.connect();
    c.subscribe((v) => b.push(v));
    expect(a).toEqual([1, 2]);
    expect(b).toEqual([1, 2]);
  });

  it('connect() returns the running connection, and reconnects after it ends', () => {
    let subscriptions = 0;
    const src = createSubject<number>();
    const counted: Observable<number> = { ...src, subscribe: (o?: Parameters<typeof src.subscribe>[0]) => (subscriptions++, src.subscribe(o)) };
    const c = connectable(counted);
    const first = c.connect();
    expect(c.connect()).toBe(first);
    first.unsubscribe();
    c.connect();
    expect(subscriptions).toBe(2);
  });
});

describe('single and every', () => {
  it('single reports the right error for each edge case', () => {
    const err = (o: Observable<unknown>): unknown => {
      let e: unknown;
      o.subscribe({ error: (x) => (e = x) });
      return e;
    };
    expect(collect(of(1).pipe(single()))).toEqual([1, '|']);
    expect(isSequenceError(err(of(1, 2).pipe(single())))).toBe(true);
    expect(isNotFoundError(err(of(1, 2).pipe(single((x) => x > 5))))).toBe(true);
    expect(isEmptyError(err(EMPTY.pipe(single())))).toBe(true);
  });

  it('every stops at the first failure', () => {
    const positive = every((x: number) => x > 0);
    expect(collect(of(1, 2, 3).pipe(positive))).toEqual([true, '|']);
    expect(collect(of(1, -1, 3).pipe(positive))).toEqual([false, '|']);
  });
});

describe('edge cases of duration selectors', () => {
  it('a synchronous duration ends audit at once instead of leaving it stuck', () => {
    expect(collect(of(1, 2, 3).pipe(audit(() => of(0))))).toEqual([1, 2, 3, '|']);
    const theirs: unknown[] = [];
    rx.of(1, 2, 3).pipe(rx.audit(() => rx.of(0))).subscribe({ next: (v) => theirs.push(v), complete: () => theirs.push('|') });
    expect(theirs).toEqual([1, 2, 3, '|']);
  });

  it('a synchronous duration does not block throttle forever', () => {
    expect(collect(of(1, 2, 3).pipe(throttle(() => of(0))))).toEqual([1, 2, 3, '|']);
  });

  it('a duration selector that throws goes to error', () => {
    const o = of(1).pipe(
      audit((): Observable<never> => {
        throw new Error('dur');
      }),
    );
    expect(collect(o)).toEqual(['#dur']);
  });
});

describe('groupBy', () => {
  it('types the groups', () => {
    const g = of(1, 2, 3).pipe(groupBy((x) => (x % 2 === 0 ? 'even' : 'odd'), { element: (x) => `${x}` }));
    expectTypeOf(g).toEqualTypeOf<Observable<GroupedObservable<'even' | 'odd', string>>>();
  });

  it('keeps the source alive while a group is still subscribed', () => {
    const src = createSubject<number>();
    let group: GroupedObservable<string, number> | null = null;
    const seen: number[] = [];
    const outer = src.pipe(groupBy(() => 'k')).subscribe((g) => (group = g));
    src.next(1);
    const inner = (group as unknown as GroupedObservable<string, number>).subscribe((v) => seen.push(v));
    outer.unsubscribe();
    expect(src.observed).toBe(true);
    src.next(2);
    inner.unsubscribe();
    expect(src.observed).toBe(false);
    expect(seen).toEqual([2]);
  });

  it('a throwing key selector errors every group and the outer', () => {
    const out: string[] = [];
    of(1, 2)
      .pipe(
        groupBy((x) => {
          if (x === 2) throw new Error('key');
          return x;
        }),
        mergeMap((g) => g.pipe(toArray())),
      )
      .subscribe({ error: (e) => out.push((e as Error).message) });
    expect(out).toEqual(['key']);
  });
});

describe('windows are read-only', () => {
  it('a window cannot be pushed into', () => {
    of(1).pipe(windowCount(1)).subscribe((w) => {
      expect('next' in w).toBe(false);
    });
  });
});
