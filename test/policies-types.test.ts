import { describe, expect, expectTypeOf, it } from 'vitest';
import * as fp from '../src/index.js';
import * as errors from '../src/vocabulary/errors.js';
import * as time from '../src/vocabulary/time.js';
import * as multicast from '../src/vocabulary/multicast.js';

/** These deliberate type errors must be checked by tsc, never executed. */
const rejectUnsupportedPolicies = (): void => {
  // @ts-expect-error repeat requires a RepeatPolicy, not the former numeric argument
  fp.repeat(2);
  // @ts-expect-error native RepeatConfig is not the canonical policy
  fp.repeat({ count: 2, delay: 5 });
  // @ts-expect-error retry requires a RetryPolicy, not a number
  fp.retry(2);
  // @ts-expect-error native RetryConfig is not the canonical policy
  fp.retry({ count: 2, resetOnSuccess: true });
  // @ts-expect-error repeat total runs and retry additional retries are distinct choices
  fp.repeat({ limit: fp.additionalRetries(2), delay: fp.immediate() });
  // @ts-expect-error retry cannot interpret a total-runs limit
  fp.retry({ limit: fp.totalRuns(2), delay: fp.immediate(), counter: fp.keepCount() });
  // @ts-expect-error source-run counts require validated integer data
  fp.repeat({ limit: { kind: 'total-runs', count: 2 }, delay: fp.immediate() });
  // @ts-expect-error an on-signal delay must provide its signal factory
  fp.repeat({ limit: fp.totalRuns(2), delay: { kind: 'on-signal' } });
  // @ts-expect-error retry's independent counter policy is required
  fp.retry({ limit: fp.additionalRetries(2), delay: fp.immediate() });
  // @ts-expect-error duration is not an immediate-delay field
  fp.repeat({ limit: fp.totalRuns(2), delay: { kind: 'immediate', duration: fp.milliseconds(5) } });

  // @ts-expect-error throttle takes one named emission mode, not Boolean config fields
  fp.throttle(() => fp.EMPTY, { leading: true, trailing: false });
  // @ts-expect-error throttleTime also takes a named emission mode
  fp.throttleTime(5, { leading: false, trailing: true });
  // @ts-expect-error the full named mode must be selected
  fp.throttle(() => fp.EMPTY, 'leading');
  // @ts-expect-error the scheduler follows the mode rather than occupying its position
  fp.throttleTime(5, fp.asyncScheduler);

  // @ts-expect-error timeout requires explicit timing and outcome policy
  fp.timeout(5);
  // @ts-expect-error native TimeoutConfig is not the canonical policy
  fp.timeout({ each: 5, with: () => fp.of('fallback') });
  // @ts-expect-error a timeout cannot omit its timing decision
  fp.timeout({ outcome: { kind: 'raise-timeout-error' }, meta: null });
  // @ts-expect-error first-then-each requires both its deadline and gap duration
  fp.defaultTimeoutErrorPolicy({ kind: 'first-then-each', deadline: fp.deadlineAfter(5) });
  // @ts-expect-error each-gap requires the positive-duration brand
  fp.defaultTimeoutErrorPolicy({ kind: 'each-gap', duration: fp.milliseconds(0) });
  // @ts-expect-error an absolute date is captured by deadlineAt, not accepted as policy data
  fp.defaultTimeoutErrorPolicy({ kind: 'first-only', deadline: new Date(0) });
  // @ts-expect-error switch-to requires a fallback factory
  fp.timeout({ timing: { kind: 'each-gap', duration: fp.positiveMilliseconds(5) }, outcome: { kind: 'switch-to' }, meta: null });
  // @ts-expect-error a raise-timeout-error alternative cannot also supply a fallback
  fp.timeout({ timing: { kind: 'each-gap', duration: fp.positiveMilliseconds(5) }, outcome: { kind: 'raise-timeout-error', fallback: () => fp.of('x') }, meta: null });

  // @ts-expect-error native ShareConfig fields do not form a SharePolicy
  fp.share({ resetOnError: false });
  // @ts-expect-error a resetting signal decision must supply its factory
  fp.share({ ...fp.defaultSharePolicy<number>(), onError: { kind: 'reset-on-next' } });
  // @ts-expect-error resetting immediately and providing a signal are separate alternatives
  fp.share({ ...fp.defaultSharePolicy<number>(), onComplete: { kind: 'reset-now', signal: () => fp.EMPTY } });
  // @ts-expect-error the connector must construct a Subject, not just an Observable
  fp.share({ ...fp.defaultSharePolicy<number>(), connector: () => fp.of(1) });
  // @ts-expect-error shareReplay no longer accepts a naked buffer size
  fp.shareReplay(1);
  // @ts-expect-error native ShareReplayConfig is not the canonical policy
  fp.shareReplay({ bufferSize: 1, refCount: true });
  // @ts-expect-error a replay count requires PositiveInt rather than NonNegativeInt
  fp.shareReplay({ replay: { maxValues: fp.limited(fp.natural(0)), maxAge: fp.unlimited() }, onNoSubscribers: 'keep-connected' });
  // @ts-expect-error idle connection lifetime is a named choice, not a Boolean
  fp.shareReplay({ ...fp.defaultShareReplayPolicy(), onNoSubscribers: true });

  // @ts-expect-error validated counts are not interchangeable with duration data
  const count: fp.NonNegativeInt = fp.milliseconds(1);
  // @ts-expect-error arbitrary numbers cannot fabricate a validated positive count
  const positive: fp.PositiveInt = 1;
  // @ts-expect-error returned policy fields are readonly
  fp.totalRuns(2).kind = 'unlimited';
  void count;
  void positive;
};

describe('algebraic policy static contracts', () => {
  it('retains the negative type checks without executing unsupported calls', () => {
    expect(typeof rejectUnsupportedPolicies).toBe('function');
  });

  it('root and family repeat/retry APIs preserve their source type', () => {
    const source = fp.of(1, 2);
    const repeated = source.pipe(fp.repeat({ limit: fp.totalRuns(2), delay: fp.immediate() }));
    const retried = source.pipe(fp.retry({
      limit: fp.additionalRetries(2), delay: fp.after(0), counter: fp.resetOnNext(),
    }));
    expectTypeOf(repeated).toEqualTypeOf<fp.Observable<number>>();
    expectTypeOf(retried).toEqualTypeOf<fp.Observable<number>>();
    expectTypeOf(source.pipe(errors.repeat(errors.defaultRepeatPolicy()))).toEqualTypeOf<fp.Observable<number>>();
    expectTypeOf(source.pipe(errors.retry(errors.defaultRetryPolicy()))).toEqualTypeOf<fp.Observable<number>>();
    expect(errors.repeat).toBe(fp.repeat);
    expect(errors.retry).toBe(fp.retry);
  });

  it('root and time-family throttle modes preserve source values', () => {
    const source = fp.of(1, 2);
    expectTypeOf(source.pipe(fp.throttle(() => fp.EMPTY, 'suppress-values'))).toEqualTypeOf<fp.Observable<number>>();
    expectTypeOf(source.pipe(time.throttle(() => fp.EMPTY, 'leading-and-trailing'))).toEqualTypeOf<fp.Observable<number>>();
    expectTypeOf(source.pipe(time.throttleTime(5, 'trailing-only', fp.asyncScheduler))).toEqualTypeOf<fp.Observable<number>>();
    expect(time.throttle).toBe(fp.throttle);
  });

  it('default timeout error policies preserve contextual A without an explicit type argument', () => {
    const result = fp.of(1).pipe(fp.timeout(fp.defaultTimeoutErrorPolicy({
      kind: 'each-gap', duration: fp.positiveMilliseconds(5),
    })));
    const familyResult = fp.of(1).pipe(time.timeout(time.defaultTimeoutErrorPolicy({
      kind: 'first-only', deadline: time.deadlineAfter(5),
    })));
    expectTypeOf(result).toEqualTypeOf<fp.Observable<number>>();
    expectTypeOf(familyResult).toEqualTypeOf<fp.Observable<number>>();
    expect(time.timeout).toBe(fp.timeout);
  });

  it('a default timeout error policy kept in a constant needs its type argument; inline it is inferred', () => {
    const timing = { kind: 'each-gap', duration: fp.positiveMilliseconds(5) } as const;
    // stored in a constant there is no contextual type, so A defaults to unknown and widens the stream
    const stored = fp.defaultTimeoutErrorPolicy(timing);
    expectTypeOf(fp.of(1).pipe(fp.timeout(stored))).toEqualTypeOf<fp.Observable<unknown>>();
    // the fix is on the user side: name the value type (examples/search/search.ts does)
    const typed = fp.defaultTimeoutErrorPolicy<number>(timing);
    expectTypeOf(fp.of(1).pipe(fp.timeout(typed))).toEqualTypeOf<fp.Observable<number>>();
  });

  it('a timeout fallback adds B and receives precisely typed metadata and source history', () => {
    type Meta = Readonly<{ requestId: string; retryable: boolean }>;
    const policy: fp.TimeoutSwitchPolicy<number, string, Meta> = {
      timing: { kind: 'first-then-each', deadline: fp.deadlineAfter(5), duration: fp.positiveMilliseconds(3) },
      meta: { requestId: 'catalog', retryable: false },
      outcome: {
        kind: 'switch-to',
        fallback: (info) => {
          expectTypeOf(info).toEqualTypeOf<fp.TimeoutInfo<number, Meta>>();
          expectTypeOf(info.lastValue).toEqualTypeOf<number | null>();
          expectTypeOf(info.meta.requestId).toEqualTypeOf<string>();
          return fp.of(info.meta.requestId);
        },
      },
    };
    expectTypeOf(fp.of(1).pipe(fp.timeout(policy))).toEqualTypeOf<fp.Observable<number | string>>();
    expectTypeOf(fp.of(1).pipe(time.timeout(policy))).toEqualTypeOf<fp.Observable<number | string>>();

    const inferred = fp.of(1).pipe(fp.timeout({
      timing: { kind: 'each-gap', duration: fp.positiveMilliseconds(5) },
      meta: { requestId: 'inferred' },
      outcome: {
        kind: 'switch-to',
        fallback: (info: fp.TimeoutInfo<number, { requestId: string }>) => fp.of(info.meta.requestId),
      },
    }));
    expectTypeOf(inferred).toEqualTypeOf<fp.Observable<number | string>>();
  });

  it('root and multicast-family policies retain A and expose the same interpreters', () => {
    const source = fp.of(1, 2);
    expectTypeOf(source.pipe(fp.share())).toEqualTypeOf<fp.Observable<number>>();
    expectTypeOf(source.pipe(fp.share(fp.defaultSharePolicy()))).toEqualTypeOf<fp.Observable<number>>();
    expectTypeOf(source.pipe(multicast.share(multicast.defaultSharePolicy<number>()))).toEqualTypeOf<fp.Observable<number>>();
    expectTypeOf(source.pipe(fp.shareReplay(fp.defaultShareReplayPolicy()))).toEqualTypeOf<fp.Observable<number>>();
    expectTypeOf(source.pipe(multicast.shareReplay(multicast.defaultShareReplayPolicy(), fp.asyncScheduler))).toEqualTypeOf<fp.Observable<number>>();
    expect(multicast.share).toBe(fp.share);
    expect(multicast.shareReplay).toBe(fp.shareReplay);
  });

  it('numeric validators return their distinct branded domains', () => {
    expectTypeOf(fp.natural(0)).toEqualTypeOf<fp.NonNegativeInt>();
    expectTypeOf(fp.positiveInteger(1)).toEqualTypeOf<fp.PositiveInt>();
    expectTypeOf(fp.milliseconds(0)).toEqualTypeOf<fp.Milliseconds>();
    expectTypeOf(fp.positiveMilliseconds(0.5)).toEqualTypeOf<fp.PositiveMilliseconds>();
    expectTypeOf(fp.epochMilliseconds(-1)).toEqualTypeOf<fp.EpochMilliseconds>();
  });
});

describe('validated domains and constructor immutability', () => {
  it('counts accept safe integers with their specified zero boundary', () => {
    for (const value of [0, 1, Number.MAX_SAFE_INTEGER]) expect(fp.natural(value)).toBe(value);
    for (const value of [1, Number.MAX_SAFE_INTEGER]) expect(fp.positiveInteger(value)).toBe(value);
    for (const value of [-1, 0.5, Number.MAX_SAFE_INTEGER + 1, NaN, Infinity, -Infinity]) {
      expect(() => fp.natural(value)).toThrowError(/Expected/);
      expect(() => fp.positiveInteger(value)).toThrowError(/Expected/);
    }
    expect(() => fp.positiveInteger(0)).toThrowError(/positive/);
  });

  it('durations allow fractional finite values and distinguish zero from positive', () => {
    for (const value of [0, 0.125, 1, Number.MAX_VALUE]) expect(fp.milliseconds(value)).toBe(value);
    for (const value of [Number.MIN_VALUE, 0.125, 1, Number.MAX_VALUE]) expect(fp.positiveMilliseconds(value)).toBe(value);
    for (const value of [-1, -0.125, NaN, Infinity, -Infinity]) {
      expect(() => fp.milliseconds(value)).toThrowError(/Expected/);
      expect(() => fp.positiveMilliseconds(value)).toThrowError(/Expected/);
    }
    expect(() => fp.positiveMilliseconds(0)).toThrowError(/positive/);
  });

  it('absolute timestamps allow past dates and enforce JavaScript Date limits', () => {
    const boundary = 8_640_000_000_000_000;
    for (const value of [-boundary, -1, 0, 1, boundary]) expect(fp.epochMilliseconds(value)).toBe(value);
    for (const value of [-boundary - 1, boundary + 1, 0.5, NaN, Infinity, -Infinity]) {
      expect(() => fp.epochMilliseconds(value)).toThrowError(/Expected/);
    }
    const date = new Date(1234);
    const deadline = fp.deadlineAt(date);
    date.setTime(9999);
    expect(deadline).toEqual({ kind: 'at', epochMillis: 1234 });
    expect(() => fp.deadlineAt(new Date(NaN))).toThrowError(/Expected/);
  });

  it('constructors freeze their own records while preserving supplied callback identity', () => {
    const repeatSignal = () => fp.EMPTY;
    const retrySignal = (_error: unknown, _count: number) => fp.EMPTY;
    const repeatPolicy = fp.defaultRepeatPolicy();
    const retryPolicy = fp.defaultRetryPolicy();
    const timeoutPolicy = fp.defaultTimeoutErrorPolicy({ kind: 'first-only', deadline: fp.deadlineAfter(1) });
    const sharePolicy = fp.defaultSharePolicy<number>();
    const replayPolicy = fp.defaultShareReplayPolicy();
    const records = [
      fp.unlimited(), fp.limited(fp.natural(1)), fp.immediate(), fp.after(0),
      fp.unlimitedRuns(), fp.totalRuns(0), fp.unlimitedRetries(), fp.additionalRetries(0),
      fp.repeatOnSignal(repeatSignal), fp.retryOnSignal(retrySignal), fp.keepCount(), fp.resetOnNext(),
      repeatPolicy, repeatPolicy.limit, repeatPolicy.delay,
      retryPolicy, retryPolicy.limit, retryPolicy.delay, retryPolicy.counter,
      timeoutPolicy, timeoutPolicy.outcome, fp.deadlineAt(new Date(0)),
      sharePolicy, sharePolicy.onError, sharePolicy.onComplete, sharePolicy.onNoSubscribers,
      replayPolicy, replayPolicy.replay, replayPolicy.replay.maxValues, replayPolicy.replay.maxAge,
    ];
    for (const record of records) expect(Object.isFrozen(record)).toBe(true);
    const repeatDelay = fp.repeatOnSignal(repeatSignal);
    const retryDelay = fp.retryOnSignal(retrySignal);
    if (repeatDelay.kind !== 'on-signal' || retryDelay.kind !== 'on-signal') throw new Error('Wrong constructor variant');
    expect(repeatDelay.signal).toBe(repeatSignal);
    expect(retryDelay.signal).toBe(retrySignal);
    expect(Object.isFrozen(repeatSignal)).toBe(false);
    expect(Object.isFrozen(retrySignal)).toBe(false);
  });
});
