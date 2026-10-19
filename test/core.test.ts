import { describe, expect, it, vi } from 'vitest';
import {
  EMPTY,
  createSubscription,
  filter,
  isObservable,
  map,
  observable,
  of,
  pipe,
  range,
  take,
  unfold,
  type Observable,
} from '../src/index.js';
import { some } from '../src/fp/index.js';
import { collect } from './helpers.js';

describe('Observable = a function you can cancel', () => {
  it('is lazy: nothing runs until subscribe', () => {
    const producer = vi.fn(() => {});
    observable(producer);
    expect(producer).not.toHaveBeenCalled();
  });

  it("is cold: Paul Taylor's random-number test gives two different numbers", () => {
    let n = 0;
    const random = observable<number>((sink) => {
      sink.next(++n); // stands in for Math.random()
      sink.complete();
    });
    const a = collect(random);
    const b = collect(random);
    expect(a).toEqual([1, '|']);
    expect(b).toEqual([2, '|']);
  });

  it('runs teardown on unsubscribe, exactly once', () => {
    const teardown = vi.fn();
    const sub = observable(() => teardown).subscribe();
    sub.unsubscribe();
    sub.unsubscribe();
    expect(teardown).toHaveBeenCalledTimes(1);
    expect(sub.closed).toBe(true);
  });

  it('runs teardown after complete', () => {
    const teardown = vi.fn();
    observable<number>((sink) => {
      sink.complete();
      return teardown;
    }).subscribe();
    expect(teardown).toHaveBeenCalledTimes(1);
  });

  it('enforces the grammar next* (error|complete)?', () => {
    const o = observable<number>((sink) => {
      sink.next(1);
      sink.complete();
      sink.next(2);
      sink.error('late');
      sink.complete();
    });
    expect(collect(o)).toEqual([1, '|']);
  });

  it('routes a throwing producer to error', () => {
    const o = observable<number>(() => {
      throw new Error('bad producer');
    });
    expect(collect(o)).toEqual(['#bad producer']);
  });

  it('stops an infinite synchronous source when downstream completes (linking)', () => {
    let produced = 0;
    const naturals = unfold(0, (n) => {
      produced++;
      return some([n, n + 1] as const);
    });
    expect(collect(pipe(naturals, take(3)))).toEqual([0, 1, 2, '|']);
    expect(produced).toBe(3);
  });

  it('routes exceptions from user functions in operators to error', () => {
    const o = of(1, 2, 3).pipe(
      map((x) => {
        if (x === 2) throw new Error('boom');
        return x;
      }),
    );
    expect(collect(o)).toEqual([1, '#boom']);
  });

  it('reports a throwing consumer as unhandled instead of re-entering the pipeline', () => {
    vi.useFakeTimers();
    try {
      const seen: number[] = [];
      of(1, 2).subscribe((x) => {
        seen.push(x);
        if (x === 1) throw new Error('consumer bug');
      });
      expect(seen).toEqual([1, 2]);
      expect(() => vi.runAllTimers()).toThrow('consumer bug');
    } finally {
      vi.useRealTimers();
    }
  });

  it('has a pipe closure equivalent to the free pipe function', () => {
    const viaMethod = collect(range(1, 5).pipe(filter((x) => x % 2 === 1), map((x) => x * 10)));
    const viaFn = collect(pipe(range(1, 5), filter((x: number) => x % 2 === 1), map((x: number) => x * 10)));
    expect(viaMethod).toEqual([10, 30, 50, '|']);
    expect(viaFn).toEqual(viaMethod);
  });

  it('values are frozen records', () => {
    const o: Observable<number> = of(1);
    expect(Object.isFrozen(o)).toBe(true);
    expect(Object.isFrozen(o.subscribe())).toBe(true);
    expect(isObservable(o)).toBe(true);
    expect(isObservable({ subscribe: () => {} })).toBe(false);
    expect(isObservable(EMPTY)).toBe(true);
  });
});

describe('Subscription', () => {
  it('runs finalizers added after close immediately', () => {
    const s = createSubscription();
    s.unsubscribe();
    const f = vi.fn();
    s.add(f);
    expect(f).toHaveBeenCalledTimes(1);
  });

  it('can remove a finalizer', () => {
    const s = createSubscription();
    const f = vi.fn();
    const remove = s.add(f);
    remove();
    s.unsubscribe();
    expect(f).not.toHaveBeenCalled();
  });

  it('collects finalizer errors into an UnsubscriptionError and still runs all', () => {
    const s = createSubscription();
    const ok = vi.fn();
    s.add(() => {
      throw new Error('a');
    });
    s.add(ok);
    expect(() => s.unsubscribe()).toThrowError(/1 error/);
    expect(ok).toHaveBeenCalled();
  });
});
