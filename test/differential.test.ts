import { describe, expect, it } from 'vitest';
import fc from 'fast-check';
import * as rx from 'rxjs';
import * as fp from '../src/index.js';
import type { Observable } from '../src/index.js';
import { completingMarbleArb, emittingMarbleArb, marbleArb, runOurs, runTheirs, RUNS, tagWindowsRx, type Input } from './differential.js';

/**
 * Every operator added to close the gap with RxJS 7.8 is checked here
 * against RxJS itself, on randomly generated marble diagrams.
 */

const tagWindows = <A>(w$: Observable<Observable<A>>): Observable<string> =>
  w$.pipe(fp.mergeMap((w: Observable<A>, i: number) => w.pipe(fp.map((v) => `${i}:${String(v)}`))));

type Both<K extends string> = {
  readonly ours: (i: Record<K, Observable<string>>, s: fp.Scheduler) => Observable<unknown>;
  readonly theirs: (i: Record<K, rx.Observable<string>>, s: rx.SchedulerLike) => rx.Observable<unknown>;
};

const same = <K extends string>(inputs: Readonly<Record<K, Input>>, both: Both<K>): void => {
  expect(runOurs(inputs, both.ours)).toEqual(runTheirs(inputs, both.theirs));
};

/** Property: for random cold inputs, rxjs-fp and RxJS record the same notifications. */
const prop1 = (name: string, both: Both<'a'>, arb = marbleArb()) =>
  it(name, () => {
    fc.assert(
      fc.property(arb, (m) => same({ a: { marbles: m } }, both)),
      { numRuns: RUNS },
    );
  });

const prop2 = (name: string, both: Both<'a' | 'b'>, arbA = marbleArb(), arbB = marbleArb(8, 'xy')) =>
  it(name, () => {
    fc.assert(
      fc.property(arbA, arbB, (a, b) => same({ a: { marbles: a }, b: { marbles: b } }, both)),
      { numRuns: RUNS },
    );
  });

describe('conditional and mathematical operators match RxJS', () => {
  prop1('every', { ours: ({ a }) => a.pipe(fp.every((x) => x !== 'c')), theirs: ({ a }) => a.pipe(rx.every((x) => x !== 'c')) });
  prop1('find', { ours: ({ a }) => a.pipe(fp.find((x) => x === 'c')), theirs: ({ a }) => a.pipe(rx.find((x) => x === 'c')) });
  prop1('findIndex', {
    ours: ({ a }) => a.pipe(fp.findIndex((x) => x === 'c')),
    theirs: ({ a }) => a.pipe(rx.findIndex((x) => x === 'c')),
  });
  prop1('isEmpty', { ours: ({ a }) => a.pipe(fp.isEmpty()), theirs: ({ a }) => a.pipe(rx.isEmpty()) });
  prop1('throwIfEmpty', { ours: ({ a }) => a.pipe(fp.throwIfEmpty()), theirs: ({ a }) => a.pipe(rx.throwIfEmpty()) });
  prop1('skipLast', { ours: ({ a }) => a.pipe(fp.skipLast(2)), theirs: ({ a }) => a.pipe(rx.skipLast(2)) });
  prop1('single', { ours: ({ a }) => a.pipe(fp.single()), theirs: ({ a }) => a.pipe(rx.single()) }, marbleArb(3));
  prop1('single with predicate', {
    ours: ({ a }) => a.pipe(fp.single((x) => x === 'c')),
    theirs: ({ a }) => a.pipe(rx.single((x) => x === 'c')),
  });
  prop1('max / min', {
    ours: ({ a }) => fp.merge(a.pipe(fp.max()), a.pipe(fp.min())),
    theirs: ({ a }) => rx.merge(a.pipe(rx.max()), a.pipe(rx.min())),
  });
  prop1('distinctUntilKeyChanged', {
    ours: ({ a }) => a.pipe(fp.map((k) => ({ k })), fp.distinctUntilKeyChanged('k'), fp.map((o) => o.k)),
    theirs: ({ a }) => a.pipe(rx.map((k) => ({ k })), rx.distinctUntilKeyChanged('k'), rx.map((o) => o.k)),
  });
  prop2(
    'sequenceEqual',
    {
      ours: ({ a, b }) => a.pipe(fp.sequenceEqual(b)),
      theirs: ({ a, b }) => a.pipe(rx.sequenceEqual(b)),
    },
    marbleArb(8, 'ab'),
    marbleArb(8, 'ab'),
  );
});

describe('duration-selector operators match RxJS', () => {
  prop2('audit', { ours: ({ a, b }) => a.pipe(fp.audit(() => b)), theirs: ({ a, b }) => a.pipe(rx.audit(() => b)) });
  prop2('debounce', { ours: ({ a, b }) => a.pipe(fp.debounce(() => b)), theirs: ({ a, b }) => a.pipe(rx.debounce(() => b)) });
  prop2('throttle (leading)', { ours: ({ a, b }) => a.pipe(fp.throttle(() => b)), theirs: ({ a, b }) => a.pipe(rx.throttle(() => b)) });
  prop2('throttle (trailing)', {
    ours: ({ a, b }) => a.pipe(fp.throttleTrailing(() => b)),
    theirs: ({ a, b }) => a.pipe(rx.throttle(() => b, { leading: false, trailing: true })),
  });
  prop2('throttle (leading + trailing)', {
    ours: ({ a, b }) => a.pipe(fp.throttleLeadingAndTrailing(() => b)),
    theirs: ({ a, b }) => a.pipe(rx.throttle(() => b, { leading: true, trailing: true })),
  });
  prop2('sample', { ours: ({ a, b }) => a.pipe(fp.sample(b)), theirs: ({ a, b }) => a.pipe(rx.sample(b)) });
  prop2('delayWhen', { ours: ({ a, b }) => a.pipe(fp.delayWhen(() => b)), theirs: ({ a, b }) => a.pipe(rx.delayWhen(() => b)) });
  prop1('timestamp / timeInterval', {
    ours: ({ a }, s) => a.pipe(fp.timestamp(s), fp.timeInterval(s), fp.map((x) => `${x.value.value}@${x.value.timestamp}+${x.interval}`)),
    theirs: ({ a }) => a.pipe(rx.timestamp(), rx.timeInterval(), rx.map((x) => `${x.value.value}@${x.value.timestamp}+${x.interval}`)),
  });
  prop1('timeout config: each', {
    ours: ({ a }, s) => a.pipe(fp.timeout(3, s)),
    theirs: ({ a }) => a.pipe(rx.timeout({ each: 3 })),
  });
  prop1('timeout config: first only', {
    ours: ({ a }, s) => a.pipe(fp.timeoutFirst(3, s)),
    theirs: ({ a }) => a.pipe(rx.timeout({ first: 3 })),
  });
  prop1('timeout config: with', {
    ours: ({ a }, s) => a.pipe(fp.timeout(2, s), fp.catchTimeout((i) => fp.of(`seen${i.seen}`))),
    theirs: ({ a }) => a.pipe(rx.timeout({ each: 2, with: (i) => rx.of(`seen${i.seen}`) })),
  });
  prop1('retryWith(timesWithDelay) vs retry({ count, delay })', {
    ours: ({ a }, s) => a.pipe(fp.retryWith(fp.timesWithDelay(2, 3), s), fp.take(12)),
    theirs: ({ a }) => a.pipe(rx.retry({ count: 2, delay: 3 }), rx.take(12)),
  });
  prop1('retryConsecutiveWith(times) vs retry({ count, resetOnSuccess })', {
    ours: ({ a }) => a.pipe(fp.retryConsecutiveWith(fp.times(1)), fp.take(12)),
    theirs: ({ a }) => a.pipe(rx.retry({ count: 1, resetOnSuccess: true }), rx.take(12)),
  });
  prop1('retryWith(exponentialBackoff) vs retry({ count, delay: fn })', {
    ours: ({ a }, s) => a.pipe(fp.retryWith(fp.exponentialBackoff(3, 1), s), fp.take(12)),
    theirs: ({ a }) => a.pipe(rx.retry({ count: 3, delay: (_e, n) => rx.timer(2 ** (n - 1)) }), rx.take(12)),
  });
  prop1('timeout config: first and each', {
    ours: ({ a }, s) => a.pipe(fp.timeoutFirstAndEach(4, 2, s)),
    theirs: ({ a }) => a.pipe(rx.timeout({ first: 4, each: 2 })),
  });
});

describe('buffer and window operators match RxJS', () => {
  prop2('buffer', { ours: ({ a, b }) => a.pipe(fp.buffer(b)), theirs: ({ a, b }) => a.pipe(rx.buffer(b)) });
  prop1('bufferTime', {
    ours: ({ a }, s) => a.pipe(fp.bufferTime(3, s)),
    theirs: ({ a }) => a.pipe(rx.bufferTime(3)),
  }, completingMarbleArb());
  prop1('bufferTime with creation interval and max size', {
    ours: ({ a }, s) => a.pipe(fp.bufferTimeEveryOrCount(4, 3, 2, s)),
    theirs: ({ a }) => a.pipe(rx.bufferTime(4, 3, 2)),
  }, completingMarbleArb());
  prop1('bufferTimeEvery', {
    ours: ({ a }, s) => a.pipe(fp.bufferTimeEvery(4, 3, s)),
    theirs: ({ a }) => a.pipe(rx.bufferTime(4, 3)),
  }, completingMarbleArb());
  prop1('bufferTimeOrCount', {
    ours: ({ a }, s) => a.pipe(fp.bufferTimeOrCount(4, 2, s)),
    theirs: ({ a }) => a.pipe(rx.bufferTime(4, null, 2)),
  }, completingMarbleArb());
  prop2('bufferToggle', {
    ours: ({ a, b }, s) => a.pipe(fp.bufferToggle(b, () => fp.timer(2, undefined, s))),
    theirs: ({ a, b }) => a.pipe(rx.bufferToggle(b, () => rx.timer(2))),
  });
  prop2('bufferWhen', {
    ours: ({ a, b }) => a.pipe(fp.bufferWhen(() => b)),
    theirs: ({ a, b }) => a.pipe(rx.bufferWhen(() => b)),
  }, marbleArb(), marbleArb(6, 'xy').filter((m) => /^-/.test(m)));
  prop2('window', {
    ours: ({ a, b }) => tagWindows(a.pipe(fp.window(b))),
    theirs: ({ a, b }) => tagWindowsRx(a.pipe(rx.window(b))),
  });
  prop1('windowCount', {
    ours: ({ a }) => tagWindows(a.pipe(fp.windowCount(2, 1))),
    theirs: ({ a }) => tagWindowsRx(a.pipe(rx.windowCount(2, 1))),
  });
  prop1('windowCount back to back', {
    ours: ({ a }) => tagWindows(a.pipe(fp.windowCount(3))),
    theirs: ({ a }) => tagWindowsRx(a.pipe(rx.windowCount(3))),
  });
  prop1('windowTime', {
    ours: ({ a }, s) => tagWindows(a.pipe(fp.windowTime(3, s))),
    theirs: ({ a }) => tagWindowsRx(a.pipe(rx.windowTime(3))),
  }, completingMarbleArb());
  prop1('windowTime with creation interval and max size', {
    ours: ({ a }, s) => tagWindows(a.pipe(fp.windowTimeEveryOrCount(4, 3, 2, s))),
    theirs: ({ a }) => tagWindowsRx(a.pipe(rx.windowTime(4, 3, 2))),
  }, completingMarbleArb());
  prop1('windowTimeEvery', {
    ours: ({ a }, s) => tagWindows(a.pipe(fp.windowTimeEvery(4, 3, s))),
    theirs: ({ a }) => tagWindowsRx(a.pipe(rx.windowTime(4, 3))),
  }, completingMarbleArb());
  prop1('windowTimeOrCount', {
    ours: ({ a }, s) => tagWindows(a.pipe(fp.windowTimeOrCount(4, 2, s))),
    theirs: ({ a }) => tagWindowsRx(a.pipe(rx.windowTime(4, null, 2))),
  }, completingMarbleArb());
  prop2('windowToggle', {
    ours: ({ a, b }, s) => tagWindows(a.pipe(fp.windowToggle(b, () => fp.timer(2, undefined, s)))),
    theirs: ({ a, b }) => tagWindowsRx(a.pipe(rx.windowToggle(b, () => rx.timer(2)))),
  }, completingMarbleArb(), completingMarbleArb(8, 'xy'));
  prop2('windowWhen', {
    ours: ({ a, b }) => tagWindows(a.pipe(fp.windowWhen(() => b))),
    theirs: ({ a, b }) => tagWindowsRx(a.pipe(rx.windowWhen(() => b))),
  }, marbleArb(), marbleArb(6, 'xy').filter((m) => /^-/.test(m)));
});

describe('higher-order operators match RxJS', () => {
  prop1('groupBy', {
    ours: ({ a }) => a.pipe(fp.groupBy((x) => x < 'c'), fp.mergeMap((g) => g.pipe(fp.map((v) => `${g.key}:${v}`)))),
    theirs: ({ a }) => a.pipe(rx.groupBy((x) => x < 'c'), rx.mergeMap((g) => g.pipe(rx.map((v) => `${g.key}:${v}`)))),
  });
  prop2('groupByUntil, with element as a map on each group', {
    ours: ({ a, b }) =>
      a.pipe(
        fp.groupByUntil((x: string) => x, () => b),
        fp.mergeMap((g, i) => g.pipe(fp.map((x) => x.toUpperCase()), fp.map((v) => `${i}${g.key}:${v}`))),
      ),
    theirs: ({ a, b }) =>
      a.pipe(
        rx.groupBy((x) => x, { element: (x) => x.toUpperCase(), duration: () => b }),
        rx.mergeMap((g, i) => g.pipe(rx.map((v) => `${i}${g.key}:${v}`))),
      ),
  }, marbleArb(10, 'ab'));
  prop2('mergeScan', {
    ours: ({ a, b }) => a.pipe(fp.mergeScan((acc, x) => b.pipe(fp.map((y) => acc + x + y)), '', 2)),
    theirs: ({ a, b }) => a.pipe(rx.mergeScan((acc, x) => b.pipe(rx.map((y) => acc + x + y)), '', 2)),
  }, marbleArb(6), marbleArb(4, 'xy'));
  prop2('switchScan', {
    ours: ({ a, b }) => a.pipe(fp.switchScan((acc, x) => b.pipe(fp.map((y) => acc + x + y)), '')),
    theirs: ({ a, b }) => a.pipe(rx.switchScan((acc, x) => b.pipe(rx.map((y) => acc + x + y)), '')),
  }, marbleArb(6), marbleArb(4, 'xy'));
  prop2('combineLatestAll', {
    ours: ({ a, b }) => fp.of(a, b).pipe(fp.combineLatestAll()),
    theirs: ({ a, b }) => rx.of(a, b).pipe(rx.combineLatestAll()),
    // inputs that complete without a value are left out: `combineLatest`
    // deliberately completes early there (see additions.test.ts)
  }, emittingMarbleArb(), emittingMarbleArb(8, 'xy'));
  prop2('zipAll', {
    ours: ({ a, b }) => fp.of(a, b).pipe(fp.zipAll()),
    theirs: ({ a, b }) => rx.of(a, b).pipe(rx.zipAll()),
  });
  prop2('raceWith', {
    ours: ({ a, b }) => a.pipe(fp.raceWith(b)),
    theirs: ({ a, b }) => a.pipe(rx.raceWith(b)),
  });
  prop2('onErrorResumeNextWith', {
    ours: ({ a, b }) => a.pipe(fp.onErrorResumeNextWith(b, a)),
    theirs: ({ a, b }) => a.pipe(rx.onErrorResumeNextWith(b, a)),
  });
  prop2('connect', {
    ours: ({ a }) => a.pipe(fp.connect((s) => fp.merge(s.pipe(fp.map((x) => `1${x}`)), s.pipe(fp.skip(1))))),
    theirs: ({ a }) => a.pipe(rx.connect((s) => rx.merge(s.pipe(rx.map((x) => `1${x}`)), s.pipe(rx.skip(1))))),
  });
});


/**
 * The share family over a scripted scenario with several subscribers:
 * A subscribes · 1 · B subscribes late · 2 · A and B leave · C subscribes ·
 * 3 · the source completes · D subscribes. Records what each subscriber
 * sees and how often the source was subscribed.
 */
describe('share lifecycles match RxJS', () => {
  type Log = { readonly seen: Record<string, unknown[]>; readonly sourceSubscriptions: number };

  const scenarioOurs = (operator: fp.Operator<number, number>): Log => {
    const seen: Record<string, unknown[]> = { A: [], B: [], C: [], D: [] };
    let sourceSubscriptions = 0;
    const feed = fp.createSubject<number>();
    const source = fp.observable<number>((sink) => {
      sourceSubscriptions++;
      return fp.subscribeChild(feed, sink);
    });
    const shared = source.pipe(operator);
    const on = (name: string) => ({ next: (v: number) => seen[name]!.push(v), complete: () => seen[name]!.push('|') });
    const a = shared.subscribe(on('A'));
    feed.next(1);
    const b = shared.subscribe(on('B'));
    feed.next(2);
    a.unsubscribe();
    b.unsubscribe();
    shared.subscribe(on('C'));
    feed.next(3);
    feed.complete();
    shared.subscribe(on('D'));
    return { seen, sourceSubscriptions };
  };

  const scenarioTheirs = (operator: rx.MonoTypeOperatorFunction<number>): Log => {
    const seen: Record<string, unknown[]> = { A: [], B: [], C: [], D: [] };
    let sourceSubscriptions = 0;
    const feed = new rx.Subject<number>();
    const source = new rx.Observable<number>((subscriber) => {
      sourceSubscriptions++;
      return feed.subscribe(subscriber);
    });
    const shared = source.pipe(operator);
    const on = (name: string) => ({ next: (v: number) => seen[name]!.push(v), complete: () => seen[name]!.push('|') });
    const a = shared.subscribe(on('A'));
    feed.next(1);
    const b = shared.subscribe(on('B'));
    feed.next(2);
    a.unsubscribe();
    b.unsubscribe();
    shared.subscribe(on('C'));
    feed.next(3);
    feed.complete();
    shared.subscribe(on('D'));
    return { seen, sourceSubscriptions };
  };

  it('share() vs share()', () => {
    expect(scenarioOurs(fp.share())).toEqual(scenarioTheirs(rx.share()));
  });
  it('shareReplay(2) vs shareReplay(2)', () => {
    expect(scenarioOurs(fp.shareReplay(2))).toEqual(scenarioTheirs(rx.shareReplay(2)));
  });
  it('shareLatest() vs share({ connector: () => new ReplaySubject(1) })', () => {
    expect(scenarioOurs(fp.shareLatest())).toEqual(scenarioTheirs(rx.share({ connector: () => new rx.ReplaySubject<number>(1) })));
  });
  it('shareWith(replay(1), Cached) vs shareReplay(1)', () => {
    expect(scenarioOurs(fp.shareWith(() => fp.createReplaySubject<number>(1), fp.Cached))).toEqual(scenarioTheirs(rx.shareReplay(1)));
  });
});
