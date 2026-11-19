import { describe, expect, expectTypeOf, it } from 'vitest';
import fc from 'fast-check';
import * as rx from 'rxjs';
import * as fp from '../src/index.js';
import { record, runMarbles } from '../src/testing/index.js';
import { collect } from './helpers.js';
import { marbleArb, runOurs, runTheirs, RUNS } from './differential.js';

const modes: readonly fp.ThrottleMode[] = [
  'leading-only', 'trailing-only', 'leading-and-trailing', 'suppress-values',
];

const nativeModes: Record<fp.ThrottleMode, rx.ThrottleConfig> = {
  'leading-only': { leading: true, trailing: false },
  'trailing-only': { leading: false, trailing: true },
  'leading-and-trailing': { leading: true, trailing: true },
  'suppress-values': { leading: false, trailing: false },
};

const timings: readonly { name: string; ours: fp.TimeoutTiming; theirs: rx.TimeoutConfig<string> }[] = [
  {
    name: 'first only',
    ours: { kind: 'first-only', deadline: fp.deadlineAfter(3) },
    theirs: { first: 3 },
  },
  {
    name: 'each gap including the first',
    ours: { kind: 'each-gap', duration: fp.positiveMilliseconds(3) },
    theirs: { each: 3 },
  },
  {
    name: 'first then each',
    ours: { kind: 'first-then-each', deadline: fp.deadlineAfter(5), duration: fp.positiveMilliseconds(2) },
    theirs: { first: 5, each: 2 },
  },
  {
    name: 'absolute first deadline',
    ours: { kind: 'first-only', deadline: fp.deadlineAt(new Date(3)) },
    theirs: { first: new Date(3) },
  },
];

describe('algebraic throttle modes', () => {
  it.each(modes)('%s matches RxJS duration-signal behavior', (mode) => {
    fc.assert(fc.property(marbleArb(), marbleArb(), (source, duration) => {
      const inputs = { a: { marbles: source }, d: { marbles: duration } };
      expect(runOurs(inputs, ({ a, d }) => a.pipe(fp.throttle(() => d, mode))))
        .toEqual(runTheirs(inputs, ({ a, d }) => a.pipe(rx.throttle(() => d, nativeModes[mode]))));
    }), { numRuns: RUNS });
  });

  it.each(modes)('%s matches RxJS with a timer, including terminal races', (mode) => {
    fc.assert(fc.property(marbleArb(), (source) => {
      const inputs = { a: { marbles: source } };
      expect(runOurs(inputs, ({ a }, scheduler) => a.pipe(fp.throttleTime(3, mode, scheduler))))
        .toEqual(runTheirs(inputs, ({ a }, scheduler) => a.pipe(rx.throttleTime(3, scheduler, nativeModes[mode]))));
    }), { numRuns: RUNS });
  });

  it.each(modes)('%s preserves synchronous next versus empty duration semantics', (mode) => {
    for (const emits of [true, false]) {
      const ours = collect(fp.of(1, 2, 3).pipe(fp.throttle(() => emits ? fp.of(0) : fp.EMPTY, mode)));
      const theirs: unknown[] = [];
      rx.of(1, 2, 3).pipe(rx.throttle(() => emits ? rx.of(0) : rx.EMPTY, nativeModes[mode])).subscribe({
        next: (value) => theirs.push(value),
        complete: () => theirs.push('|'),
      });
      expect(ours).toEqual(theirs);
    }
  });

  it('closes a synchronous duration producer on its first signal', () => {
    let produced = 0;
    let closed = 0;
    const duration = fp.observable<number>((sink) => {
      for (let i = 0; i < 10 && !sink.closed; i++) {
        produced++;
        sink.next(i);
      }
      return () => { closed++; };
    });
    expect(collect(fp.of(1, 2).pipe(fp.throttle(() => duration)))).toEqual([1, 2, '|']);
    expect(produced).toBe(2);
    expect(closed).toBe(2);
  });

  it('marks a duration closed before its teardown can re-enter the source', () => {
    const source = fp.createSubject<number>();
    const trigger = fp.createSubject<number>();
    const output: number[] = [];
    let injected = false;
    const duration = fp.observable<number>((sink) => {
      const subscription = trigger.subscribe(sink);
      return () => {
        subscription.unsubscribe();
        if (!injected) { injected = true; source.next(2); }
      };
    });
    const subscription = source.pipe(fp.throttle(() => duration)).subscribe((value) => output.push(value));
    source.next(1);
    trigger.next(0);
    source.next(3);
    subscription.unsubscribe();

    const nativeSource = new rx.Subject<number>();
    const nativeTrigger = new rx.Subject<number>();
    const expected: number[] = [];
    let nativeInjected = false;
    const nativeDuration = new rx.Observable<number>((sink) => {
      const nativeSubscription = nativeTrigger.subscribe(sink);
      return () => {
        nativeSubscription.unsubscribe();
        if (!nativeInjected) { nativeInjected = true; nativeSource.next(2); }
      };
    });
    const nativeSubscription = nativeSource.pipe(rx.throttle(() => nativeDuration)).subscribe((value) => expected.push(value));
    nativeSource.next(1);
    nativeTrigger.next(0);
    nativeSource.next(3);
    nativeSubscription.unsubscribe();
    expect(output).toEqual(expected);
    expect(output).toEqual([1, 2, 3]);
  });

  it('suppress-values still calls the selector and forwards duration errors', () => {
    const selected: number[] = [];
    const error = new Error('duration failed');
    let actual: unknown;
    const values: number[] = [];
    fp.of(1, 2).pipe(fp.throttle((value) => {
      selected.push(value);
      return fp.throwError(() => error);
    }, 'suppress-values')).subscribe({ next: (value) => values.push(value), error: (value) => { actual = value; } });
    expect(selected).toEqual([1]);
    expect(values).toEqual([]);
    expect(actual).toBe(error);
  });

  it('aliases compose the canonical interpreter and retain scheduler-last order', () => {
    expect(fp.Leading).toBe('leading-only');
    expect(fp.Trailing).toBe('trailing-only');
    expect(fp.LeadingAndTrailing).toBe('leading-and-trailing');
    for (const mode of modes) {
      const inputs = { a: { marbles: 'abc---de|' } };
      const direct = runOurs(inputs, ({ a }, scheduler) => a.pipe(fp.throttleTime(3, mode, scheduler)));
      const partial = runOurs(inputs, ({ a }, scheduler) => a.pipe(fp.throttleTimeWith(mode)(3, scheduler)));
      const signal = runOurs(inputs, ({ a }, scheduler) => a.pipe(fp.throttleWith(mode)(() => fp.timer(3, undefined, scheduler))));
      expect(partial).toEqual(direct);
      expect(signal).toEqual(direct);
    }
  });

  it('cancellation releases an active duration without a trailing emission', () => {
    const scheduler = fp.createVirtualScheduler();
    const source = fp.createSubject<number>();
    const output: number[] = [];
    const subscription = source.pipe(fp.throttleTime(5, 'trailing-only', scheduler)).subscribe((value) => output.push(value));
    source.next(1);
    expect(scheduler.pending()).toBe(1);
    subscription.unsubscribe();
    expect(scheduler.pending()).toBe(0);
    scheduler.advanceBy(10);
    expect(output).toEqual([]);
  });
});

describe('algebraic timeout policy', () => {
  it.each(timings)('$name matches RxJS notification timing', ({ ours, theirs }) => {
    fc.assert(fc.property(marbleArb(), (source) => {
      const inputs = { a: { marbles: source } };
      expect(runOurs(inputs, ({ a }, scheduler) => a.pipe(fp.timeout(fp.defaultTimeoutErrorPolicy(ours), scheduler))))
        .toEqual(runTheirs(inputs, ({ a }, scheduler) => a.pipe(rx.timeout({ ...theirs, scheduler }))));
    }), { numRuns: RUNS });
  });

  it.each(timings)('$name switches to the fallback at the same time as RxJS', ({ ours, theirs }) => {
    fc.assert(fc.property(marbleArb(), (source) => {
      const inputs = { a: { marbles: source }, b: { marbles: '--z|' } };
      expect(runOurs(inputs, ({ a, b }, scheduler) => a.pipe(fp.timeout({
        timing: ours,
        outcome: { kind: 'switch-to', fallback: () => b },
        meta: null,
      }, scheduler)))).toEqual(runTheirs(inputs, ({ a, b }, scheduler) => a.pipe(rx.timeout({
        ...theirs, scheduler, with: () => b,
      }))));
    }), { numRuns: RUNS });
  });

  it('preserves meaningful timeout metadata and closes source before invoking fallback', () => {
    const scheduler = fp.createVirtualScheduler();
    const log: string[] = [];
    const meta = { request: 'search-7' };
    let info: fp.TimeoutInfo<number, typeof meta> | undefined;
    const source = fp.observable<number>((sink) => {
      log.push('source-open');
      sink.next(42);
      return () => { log.push('source-close'); };
    });
    const policy: fp.TimeoutSwitchPolicy<number, string, typeof meta> = {
      timing: { kind: 'each-gap', duration: fp.positiveMilliseconds(3) },
      outcome: {
        kind: 'switch-to',
        fallback: (details) => {
          info = details;
          log.push('fallback-factory');
          return fp.observable<string>((sink) => {
            log.push('fallback-open');
            return scheduler.schedule(() => { sink.next('replacement'); sink.complete(); }, 10);
          });
        },
      },
      meta,
    };
    const events = record(source.pipe(fp.timeout(policy, scheduler)), scheduler);
    scheduler.flush();
    expect(log).toEqual(['source-open', 'source-close', 'fallback-factory', 'fallback-open']);
    // Deliberate retained rxjs-fp contract: 7.8.2 clears lastValue during
    // teardown in some timeout paths; this implementation preserves it.
    expect(info).toEqual({ seen: 1, lastValue: 42, meta });
    expect(info?.meta).toBe(meta);
    expect(events).toEqual([
      { frame: 0, kind: 'N', value: 42 },
      { frame: 13, kind: 'N', value: 'replacement' },
      { frame: 13, kind: 'C' },
    ]);
    expect(scheduler.pending()).toBe(0);
  });

  it('default errors include null metadata and the last value seen', () => {
    runMarbles(({ cold, scheduler }) => {
      let error: unknown;
      cold('ab-----c|').pipe(fp.timeout(fp.defaultTimeoutErrorPolicy({
        kind: 'each-gap', duration: fp.positiveMilliseconds(3),
      }), scheduler)).subscribe({ error: (value) => { error = value; } });
      scheduler.advanceTo(4);
      expect(fp.isTimeoutError(error)).toBe(true);
      if (!fp.isTimeoutError(error)) throw new Error('Expected TimeoutError');
      expect(error.info).toEqual({ seen: 2, lastValue: 'b', meta: null });
    });
    expect(fp.timeoutError({ seen: 1, lastValue: 'legacy' }).info)
      .toEqual({ seen: 1, lastValue: 'legacy', meta: null });
  });

  it('snapshots mutable deadlines and calls the captured fallback without a receiver', () => {
    const scheduler = fp.createVirtualScheduler();
    const deadline = { kind: 'after' as const, duration: fp.milliseconds(3) };
    let calls = 0;
    let receiver: unknown = 'not called';
    const outcome = {
      kind: 'switch-to' as const,
      fallback: function (this: unknown, _info: fp.TimeoutInfo<number, null>) {
        receiver = this;
        calls++;
        return fp.of('captured');
      },
    };
    const operator = fp.timeout({ timing: { kind: 'first-only', deadline }, outcome, meta: null }, scheduler);
    deadline.duration = fp.milliseconds(50);
    outcome.fallback = () => fp.of('mutated');
    expect(calls).toBe(0);
    const events = record(fp.NEVER.pipe(operator), scheduler);
    scheduler.flush();
    expect(events).toEqual([{ frame: 3, kind: 'N', value: 'captured' }, { frame: 3, kind: 'C' }]);
    expect(calls).toBe(1);
    expect(receiver).toBeUndefined();
  });

  it('snapshots Date timestamps and evaluates absolute time at connection', () => {
    const date = new Date(105);
    const deadline = fp.deadlineAt(date);
    date.setTime(999);
    const scheduler = fp.createVirtualScheduler(100);
    const events = record(fp.NEVER.pipe(fp.timeout({
      timing: { kind: 'first-only', deadline },
      outcome: { kind: 'switch-to', fallback: () => fp.of('expired') },
      meta: null,
    }, scheduler)), scheduler);
    scheduler.flush();
    expect(events).toEqual([{ frame: 105, kind: 'N', value: 'expired' }, { frame: 105, kind: 'C' }]);
    expect(() => fp.deadlineAt(new Date(Number.NaN))).toThrow();
  });

  it('does not leave timers after synchronous completion or a downstream stop', () => {
    const scheduler = fp.createVirtualScheduler();
    const policy = fp.defaultTimeoutErrorPolicy<number>({ kind: 'each-gap', duration: fp.positiveMilliseconds(3) });
    expect(collect(fp.of(1, 2).pipe(fp.timeout(policy, scheduler)))).toEqual([1, 2, '|']);
    expect(scheduler.pending()).toBe(0);
    expect(collect(fp.of(1, 2).pipe(fp.timeout(policy, scheduler), fp.take(1)))).toEqual([1, '|']);
    expect(scheduler.pending()).toBe(0);
  });

  it.each(['before timeout', 'during fallback'])('downstream cancellation %s releases work', (stage) => {
    const scheduler = fp.createVirtualScheduler();
    let calls = 0;
    const subscription = fp.NEVER.pipe(fp.timeout({
      timing: { kind: 'first-only', deadline: fp.deadlineAfter(3) },
      outcome: { kind: 'switch-to', fallback: () => { calls++; return fp.timer(10, undefined, scheduler); } },
      meta: null,
    }, scheduler)).subscribe();
    scheduler.advanceTo(stage === 'before timeout' ? 1 : 3);
    subscription.unsubscribe();
    expect(scheduler.pending()).toBe(0);
    scheduler.advanceTo(30);
    expect(calls).toBe(stage === 'before timeout' ? 0 : 1);
  });

  it('routes fallback factory and fallback stream errors through the result', () => {
    for (const fromFactory of [true, false]) {
      const scheduler = fp.createVirtualScheduler();
      const expected = new Error('fallback failed');
      let actual: unknown;
      fp.NEVER.pipe(fp.timeout({
        timing: { kind: 'first-only', deadline: fp.deadlineAfter(1) },
        outcome: { kind: 'switch-to', fallback: () => {
          if (fromFactory) throw expected;
          return fp.throwError(() => expected);
        } },
        meta: null,
      }, scheduler)).subscribe({ error: (error) => { actual = error; } });
      scheduler.flush();
      expect(actual).toBe(expected);
      expect(scheduler.pending()).toBe(0);
    }
  });

  it('closes a synchronous source before a synchronously scheduled fallback starts', () => {
    const log: string[] = [];
    let produced = 0;
    const scheduler: fp.Scheduler = { now: () => 0, schedule: (work) => { work(); return () => {}; } };
    const source = fp.observable<number>((sink) => {
      for (let i = 0; i < 10 && !sink.closed; i++) { produced++; sink.next(i); }
      return () => { log.push('source-close'); };
    });
    const result = collect(source.pipe(fp.timeout({
      timing: { kind: 'each-gap', duration: fp.positiveMilliseconds(1) },
      outcome: { kind: 'switch-to', fallback: () => { log.push('factory'); return fp.of('fallback'); } },
      meta: null,
    }, scheduler)));
    expect(produced).toBe(1);
    expect(log).toEqual(['source-close', 'factory']);
    expect(result).toEqual([0, 'fallback', '|']);
  });

  it('keeps independent state for separate connections', () => {
    const scheduler = fp.createVirtualScheduler();
    const seen: number[] = [];
    const source = fp.observable<number>((sink) => { sink.next(7); });
    const result = source.pipe(fp.timeout({
      timing: { kind: 'each-gap', duration: fp.positiveMilliseconds(2) },
      outcome: { kind: 'switch-to', fallback: (info) => { seen.push(info.seen); return fp.EMPTY; } },
      meta: null,
    }, scheduler));
    result.subscribe();
    scheduler.advanceTo(1);
    result.subscribe();
    scheduler.flush();
    expect(seen).toEqual([1, 1]);
  });

  it('preserves source/output relationships and rejects invalid policy forms', () => {
    const timing: fp.TimeoutTiming = { kind: 'each-gap', duration: fp.positiveMilliseconds(3) };
    const errorPolicy = fp.defaultTimeoutErrorPolicy<number>(timing);
    expectTypeOf(fp.of(1).pipe(fp.timeout(errorPolicy))).toEqualTypeOf<fp.Observable<number>>();
    const fallbackPolicy: fp.TimeoutSwitchPolicy<number, string, { request: string }> = {
      timing,
      outcome: { kind: 'switch-to', fallback: (info) => fp.of(info.meta.request) },
      meta: { request: 'x' },
    };
    expectTypeOf(fp.of(1).pipe(fp.timeout(fallbackPolicy))).toEqualTypeOf<fp.Observable<number | string>>();
    const general: fp.TimeoutPolicy<number, string, { request: string }> = fallbackPolicy;
    expectTypeOf(fp.of(1).pipe(fp.timeout(general))).toEqualTypeOf<fp.Observable<number | string>>();

    const rejectedForms = () => {
      // @ts-expect-error numeric timeout is replaced by a required timing policy
      fp.timeout(3);
      // @ts-expect-error no timing is not a valid timeout policy
      fp.defaultTimeoutErrorPolicy({});
      // @ts-expect-error a recurring duration must have the positive brand
      fp.defaultTimeoutErrorPolicy({ kind: 'each-gap', duration: fp.milliseconds(0) });
      // @ts-expect-error Boolean records are replaced by the named emission mode
      fp.throttle(() => fp.EMPTY, { leading: true });
      // @ts-expect-error scheduler is last, after the emission mode
      fp.throttleTime(3, fp.asyncScheduler);
    };
    expect(rejectedForms).toBeTypeOf('function');
    expect(() => fp.positiveMilliseconds(0)).toThrow();
  });
});
