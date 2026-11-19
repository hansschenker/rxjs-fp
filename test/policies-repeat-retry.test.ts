import { describe, expect, it } from 'vitest';
import fc from 'fast-check';
import * as rx from 'rxjs';
import { TestScheduler } from 'rxjs/testing';
import { observable } from '../src/core/observable.js';
import type { Operator } from '../src/core/types.js';
import { EMPTY, NEVER, defer, from, throwError, timer } from '../src/creation/creation.js';
import { take } from '../src/operators/filtering.js';
import {
  additionalRetries,
  defaultRepeatPolicy,
  defaultRetryPolicy,
  keepCount,
  repeat,
  repeatOnSignal,
  resetOnNext,
  retry,
  retryOnSignal,
  totalRuns,
} from '../src/operators/error.js';
import { after, immediate, milliseconds, natural } from '../src/policies/core.js';
import { createVirtualScheduler } from '../src/scheduler/scheduler.js';
import { collect } from './helpers.js';
import { marbleArb, runOurs, runTheirs, RUNS } from './differential.js';

describe('direct repeat and retry policy interpreters', () => {
  it('repeat counts total runs while retry counts additional attempts', () => {
    let repeated = 0;
    let retried = 0;
    expect(collect(defer(() => [++repeated]).pipe(repeat({ limit: totalRuns(2), delay: immediate() })))).toEqual([1, 2, '|']);
    expect(collect(defer(() => {
      retried += 1;
      return throwError(() => 'failure');
    }).pipe(retry({ limit: additionalRetries(2), delay: immediate(), counter: keepCount() })))).toEqual(['#failure']);
    expect(repeated).toBe(2);
    expect(retried).toBe(3);
  });

  it('zero repeat never connects or evaluates its signal; zero retry still connects once', () => {
    let connects = 0;
    const source = defer(() => {
      connects += 1;
      return throwError(() => 'source');
    });
    const forbiddenSignal = (): never => { throw new Error('signal must not run'); };
    expect(collect(source.pipe(repeat({ limit: totalRuns(0), delay: repeatOnSignal(forbiddenSignal) })))).toEqual(['|']);
    expect(connects).toBe(0);
    expect(collect(source.pipe(retry({ limit: additionalRetries(0), delay: retryOnSignal(forbiddenSignal), counter: keepCount() })))).toEqual(['#source']);
    expect(connects).toBe(1);
  });

  it('repeat matches RxJS for finite counts and immediate or scheduled waiting', () => {
    fc.assert(fc.property(marbleArb(6), fc.integer({ min: 0, max: 4 }), fc.option(fc.integer({ min: 0, max: 3 }), { nil: undefined }), (marbles, count, delay) => {
      const inputs = { a: { marbles } };
      const ours = runOurs(inputs, ({ a }, scheduler) => a.pipe(repeat({ limit: totalRuns(count), delay: delay === undefined ? immediate() : after(delay) }, scheduler)));
      const theirs = runTheirs(inputs, ({ a }) => a.pipe(rx.repeat({ count, delay })));
      expect(ours).toEqual(theirs);
    }), { numRuns: RUNS });
  });

  it('retry matches RxJS across counts, reset choices, and waiting modes', () => {
    fc.assert(fc.property(marbleArb(6), fc.integer({ min: 0, max: 3 }), fc.boolean(), fc.option(fc.integer({ min: 0, max: 3 }), { nil: undefined }), (marbles, count, reset, delay) => {
      const inputs = { a: { marbles } };
      const ours = runOurs(inputs, ({ a }, scheduler) => a.pipe(retry({
        limit: additionalRetries(count),
        delay: delay === undefined ? immediate() : after(delay),
        counter: reset ? resetOnNext() : keepCount(),
      }, scheduler), take(10)));
      const theirs = runTheirs(inputs, ({ a }) => a.pipe(rx.retry({ count, delay, resetOnSuccess: reset }), rx.take(10)));
      expect(ours).toEqual(theirs);
    }), { numRuns: RUNS });
  });

  it('signal next, empty completion, error, and silence match RxJS for both operators', () => {
    const cases = [
      { ours: () => [1, 2], theirs: () => rx.of(1, 2) },
      { ours: () => EMPTY, theirs: () => rx.EMPTY },
      { ours: () => throwError(() => 'signal'), theirs: () => rx.throwError(() => 'signal') },
      { ours: () => NEVER, theirs: () => rx.NEVER },
    ];
    for (const signal of cases) {
      expect(runOurs({ a: { marbles: 'a|' } }, ({ a }) => a.pipe(repeat({
        limit: totalRuns(3), delay: repeatOnSignal(signal.ours),
      })))).toEqual(runTheirs({ a: { marbles: 'a|' } }, ({ a }) => a.pipe(rx.repeat({ count: 3, delay: signal.theirs }))));
      expect(runOurs({ a: { marbles: 'a#' } }, ({ a }) => a.pipe(retry({
        limit: additionalRetries(2), delay: retryOnSignal(signal.ours), counter: keepCount(),
      })))).toEqual(runTheirs({ a: { marbles: 'a#' } }, ({ a }) => a.pipe(rx.retry({ count: 2, delay: signal.theirs }))));
    }
  });

  it('factories receive one-based counts and no implicit object receiver', () => {
    const repeatCalls: Array<readonly [unknown, number]> = [];
    const retryCalls: Array<readonly [unknown, unknown, number]> = [];
    function repeatSignal(this: unknown, completedRuns: number) {
      repeatCalls.push([this, completedRuns]);
      return [undefined];
    }
    function retrySignal(this: unknown, error: unknown, retryNumber: number) {
      retryCalls.push([this, error, retryNumber]);
      return [undefined];
    }
    expect(collect(from(['a']).pipe(repeat({ limit: totalRuns(3), delay: repeatOnSignal(repeatSignal) })))).toEqual(['a', 'a', 'a', '|']);
    expect(collect(throwError(() => 'failure').pipe(retry({
      limit: additionalRetries(2), delay: retryOnSignal(retrySignal), counter: keepCount(),
    })))).toEqual(['#failure']);
    expect(repeatCalls).toEqual([[undefined, 1], [undefined, 2]]);
    expect(retryCalls).toEqual([[undefined, 'failure', 1], [undefined, 'failure', 2]]);
  });

  it.each(['repeat', 'retry'] as const)('%s finishes synchronous source teardown before the next source starts', (mode) => {
    const runOursTrace = () => {
      let run = 0;
      const trace: string[] = [];
      const source = observable<number>((sink) => {
        const id = ++run;
        trace.push(`start${id}`);
        sink.next(id);
        if (mode === 'repeat') sink.complete();
        else sink.error('source');
        return () => { trace.push(`close${id}`); };
      });
      const op: Operator<number, number> = mode === 'repeat'
        ? repeat({ limit: totalRuns(3), delay: immediate() })
        : retry({ limit: additionalRetries(2), delay: immediate(), counter: keepCount() });
      source.pipe(op).subscribe({ next: (n) => trace.push(`next${n}`), complete: () => trace.push('complete'), error: () => trace.push('error') });
      return trace;
    };
    const runNativeTrace = () => {
      let run = 0;
      const trace: string[] = [];
      const source = new rx.Observable<number>((sink) => {
        const id = ++run;
        trace.push(`start${id}`);
        sink.next(id);
        if (mode === 'repeat') sink.complete();
        else sink.error('source');
        return () => { trace.push(`close${id}`); };
      });
      source.pipe(mode === 'repeat' ? rx.repeat(3) : rx.retry(2)).subscribe({
        next: (n) => trace.push(`next${n}`), complete: () => trace.push('complete'), error: () => trace.push('error'),
      });
      return trace;
    };
    const actual = runOursTrace();
    expect(actual).toEqual(runNativeTrace());
    expect(actual.indexOf('close1')).toBeLessThan(actual.indexOf('start2'));
    expect(actual.indexOf('close2')).toBeLessThan(actual.indexOf('start3'));
  });

  it.each(['repeat', 'retry'] as const)('%s closes a synchronous iterable signal on its first value', (mode) => {
    let signalPulls = 0;
    let signalCloses = 0;
    let sourceRuns = 0;
    function* signal() {
      try {
        signalPulls += 1;
        yield 'restart';
        signalPulls += 1;
        throw new Error('the signal must not be pulled twice');
      } finally {
        signalCloses += 1;
      }
    }
    const source = defer<number>(() => {
      sourceRuns += 1;
      if (sourceRuns > 1) return NEVER;
      return mode === 'repeat' ? from([1]) : throwError(() => 'source');
    });
    const op: Operator<number, number> = mode === 'repeat'
      ? repeat({ limit: totalRuns(2), delay: repeatOnSignal(signal) })
      : retry({ limit: additionalRetries(1), delay: retryOnSignal(signal), counter: keepCount() });
    const errors: unknown[] = [];
    const sub = source.pipe(op).subscribe({ error: (e) => errors.push(e) });
    expect({ signalPulls, signalCloses, sourceRuns, errors }).toEqual({ signalPulls: 1, signalCloses: 1, sourceRuns: 2, errors: [] });
    sub.unsubscribe();
  });

  it.each(['repeat', 'retry'] as const)('%s cancels a pending scheduled restart on downstream close', (mode) => {
    const scheduler = createVirtualScheduler();
    let runs = 0;
    const source = defer<number>(() => {
      runs += 1;
      return mode === 'repeat' ? from([1]) : throwError(() => 'source');
    });
    const op: Operator<number, number> = mode === 'repeat'
      ? repeat({ limit: totalRuns(3), delay: after(5) }, scheduler)
      : retry({ limit: additionalRetries(2), delay: after(5), counter: keepCount() }, scheduler);
    const sub = source.pipe(op).subscribe();
    expect(scheduler.pending()).toBe(1);
    scheduler.advanceTo(2);
    sub.unsubscribe();
    scheduler.flush();
    expect(runs).toBe(1);
    expect(scheduler.pending()).toBe(0);
  });

  it('reset-on-next restarts the retry budget and signal count after every value', () => {
    const counts: number[] = [];
    let runs = 0;
    const source = observable<number>((sink) => {
      sink.next(++runs);
      if (runs < 4) sink.error('source');
      else sink.complete();
    });
    expect(collect(source.pipe(retry({
      limit: additionalRetries(1),
      delay: retryOnSignal((_error, n) => { counts.push(n); return [undefined]; }),
      counter: resetOnNext(),
    })))).toEqual([1, 2, 3, 4, '|']);
    expect(counts).toEqual([1, 1, 1]);
  });

  it('immediate is synchronous while after(0) waits for the scheduler', () => {
    const sync: unknown[] = [];
    from(['a']).pipe(repeat({ limit: totalRuns(2), delay: immediate() })).subscribe((v) => sync.push(v));
    sync.push('after subscribe');
    expect(sync).toEqual(['a', 'a', 'after subscribe']);

    const scheduler = createVirtualScheduler();
    const deferred: unknown[] = [];
    from(['a']).pipe(repeat({ limit: totalRuns(2), delay: after(0) }, scheduler)).subscribe((v) => deferred.push(v));
    deferred.push('after subscribe');
    expect(deferred).toEqual(['a', 'after subscribe']);
    scheduler.flush();

    const native: unknown[] = [];
    new TestScheduler(() => {}).run(() => {
      rx.of('a').pipe(rx.repeat({ count: 2, delay: 0 })).subscribe((v) => native.push(v));
      native.push('after subscribe');
    });
    expect(deferred).toEqual(native);
    expect(deferred).toEqual(['a', 'after subscribe', 'a']);
  });

  it('policy choices are captured at operator creation and execution state is per connection', () => {
    const scheduler = createVirtualScheduler();
    const policy = {
      limit: { kind: 'total-runs' as const, count: natural(2) },
      delay: { kind: 'after' as const, duration: milliseconds(2) },
    };
    const op = repeat<number>(policy, scheduler);
    policy.limit.count = natural(4);
    policy.delay.duration = milliseconds(100);
    const first: number[] = [];
    const second: number[] = [];
    const source = from([1]).pipe(op);
    source.subscribe((v) => first.push(v));
    source.subscribe((v) => second.push(v));
    scheduler.flush(10);
    expect(first).toEqual([1, 1]);
    expect(second).toEqual([1, 1]);
    expect(scheduler.pending()).toBe(0);
  });

  it('signal-factory exceptions become result errors after source teardown', () => {
    const trace: string[] = [];
    const source = observable<number>((sink) => {
      sink.complete();
      return () => { trace.push('source closed'); };
    });
    source.pipe(repeat({ limit: totalRuns(2), delay: repeatOnSignal(() => {
      trace.push('factory');
      throw new Error('factory failure');
    }) })).subscribe({ error: (e) => trace.push((e as Error).message) });
    expect(trace).toEqual(['source closed', 'factory', 'factory failure']);
  });

  it('signal promises are accepted and source errors are not repeated', async () => {
    const values: Array<number | string> = [];
    await new Promise<void>((resolve, reject) => {
      from([1]).pipe(repeat({ limit: totalRuns(2), delay: repeatOnSignal(() => Promise.resolve('ready')) })).subscribe({
        next: (v) => values.push(v), error: reject, complete: () => { values.push('|'); resolve(); },
      });
    });
    expect(values).toEqual([1, 1, '|']);
    expect(collect(throwError(() => 'source').pipe(repeat(defaultRepeatPolicy())))).toEqual(['#source']);
    expect(collect(from([1]).pipe(retry(defaultRetryPolicy())))).toEqual([1, '|']);
  });

  it('count-sensitive signals have the same virtual-time behavior as native RxJS', () => {
    expect(runOurs({ a: { marbles: 'a|' } }, ({ a }, scheduler) => a.pipe(repeat({
      limit: totalRuns(3), delay: repeatOnSignal((n) => timer(n * 2, undefined, scheduler)),
    }, scheduler)))).toEqual(runTheirs({ a: { marbles: 'a|' } }, ({ a }, scheduler) => a.pipe(rx.repeat({
      count: 3, delay: (n) => rx.timer(n * 2, scheduler),
    }))));
    expect(runOurs({ a: { marbles: 'a#' } }, ({ a }, scheduler) => a.pipe(retry({
      limit: additionalRetries(2), delay: retryOnSignal((_e, n) => timer(n * 2, undefined, scheduler)), counter: keepCount(),
    }, scheduler)))).toEqual(runTheirs({ a: { marbles: 'a#' } }, ({ a }, scheduler) => a.pipe(rx.retry({
      count: 2, delay: (_e, n) => rx.timer(n * 2, scheduler),
    }))));
  });
});
