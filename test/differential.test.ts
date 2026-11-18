import { describe, expect, it } from 'vitest';
import fc from 'fast-check';
import * as rx from 'rxjs';
import * as fp from '../src/index.js';
import type { Observable } from '../src/index.js';
import { none, some } from '../src/fp/index.js';
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

/**
 * The publish family as a theater: A and B take their seats · 0 before the
 * curtain · connect() · 1 · C arrives late · 2 · disconnect · 3 during the
 * break · connect() again · 4 · the source completes · D arrives after the
 * end. Records what each subscriber sees and how often the source was
 * subscribed.
 */
describe('publish family matches RxJS', () => {
  type Log = { readonly seen: Record<string, unknown[]>; readonly sourceSubscriptions: number };
  interface Feed {
    readonly next: (v: number) => void;
    readonly complete: () => void;
  }
  interface Show {
    readonly subscribe: (o: { next: (v: number) => void; complete: () => void }) => { unsubscribe: () => void };
    readonly connect: () => { unsubscribe: () => void };
  }

  const theater = (feed: Feed, show: Show, counter: () => number): Log => {
    const seen: Record<string, unknown[]> = { A: [], B: [], C: [], D: [] };
    const on = (name: string) => ({ next: (v: number) => seen[name]!.push(v), complete: () => seen[name]!.push('|') });
    show.subscribe(on('A'));
    show.subscribe(on('B'));
    feed.next(0);
    const first = show.connect();
    feed.next(1);
    show.subscribe(on('C'));
    feed.next(2);
    first.unsubscribe();
    feed.next(3);
    show.connect();
    feed.next(4);
    feed.complete();
    show.subscribe(on('D'));
    return { seen, sourceSubscriptions: counter() };
  };

  const ours = (make: (source: fp.Observable<number>) => fp.Connectable<number>): Log => {
    let n = 0;
    const feed = fp.createSubject<number>();
    const source = fp.observable<number>((sink) => {
      n++;
      return fp.subscribeChild(feed, sink);
    });
    return theater(feed, make(source), () => n);
  };
  const theirs = (op: rx.UnaryFunction<rx.Observable<number>, rx.Observable<number>>): Log => {
    let n = 0;
    const feed = new rx.Subject<number>();
    const source = new rx.Observable<number>((subscriber) => {
      n++;
      return feed.subscribe(subscriber);
    });
    return theater(feed, source.pipe(op) as rx.ConnectableObservable<number>, () => n);
  };

  it('publish', () => {
    expect(ours(fp.publish)).toEqual(theirs(rx.publish()));
  });
  it('publishReplay(2)', () => {
    expect(ours((s) => fp.publishReplay(s, 2))).toEqual(theirs(rx.publishReplay(2)));
  });
  it('publishBehavior(-1)', () => {
    expect(ours((s) => fp.publishBehavior(s, -1))).toEqual(theirs(rx.publishBehavior(-1)));
  });
  it('publishLast', () => {
    expect(ours(fp.publishLast)).toEqual(theirs(rx.publishLast()));
  });

  // refCount: A subscribes · 1 · B late · 2 · A and B leave · C · 3 · complete · D
  const refCountScenario = (feed: Feed, shared: { subscribe: Show['subscribe'] }, counter: () => number): Log => {
    const seen: Record<string, unknown[]> = { A: [], B: [], C: [], D: [] };
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
    return { seen, sourceSubscriptions: counter() };
  };
  const oursRefCount = (make: (source: fp.Observable<number>) => fp.Connectable<number>): Log => {
    let n = 0;
    const feed = fp.createSubject<number>();
    const source = fp.observable<number>((sink) => {
      n++;
      return fp.subscribeChild(feed, sink);
    });
    return refCountScenario(feed, fp.refCount(make(source)), () => n);
  };
  const theirsRefCount = (op: rx.UnaryFunction<rx.Observable<number>, rx.ConnectableObservable<number>>): Log => {
    let n = 0;
    const feed = new rx.Subject<number>();
    const source = new rx.Observable<number>((subscriber) => {
      n++;
      return feed.subscribe(subscriber);
    });
    return refCountScenario(feed, source.pipe(op, rx.refCount()), () => n);
  };

  it('refCount(publish(source))', () => {
    expect(oursRefCount(fp.publish)).toEqual(theirsRefCount(rx.publish()));
  });
  it('refCount(publishReplay(source, 2))', () => {
    expect(oursRefCount((s) => fp.publishReplay(s, 2))).toEqual(
      theirsRefCount(rx.publishReplay(2) as rx.UnaryFunction<rx.Observable<number>, rx.ConnectableObservable<number>>),
    );
  });
});

describe('concurrency vocabulary matches RxJS (identity tests, docs/kernel.md)', () => {
  // Each value of `a` starts nested work: `b`, tagged with the value that started it.
  prop2('mergeMap', {
    ours: ({ a, b }) => a.pipe(fp.mergeMap((x: string) => b.pipe(fp.map((y) => x + y)))),
    theirs: ({ a, b }) => a.pipe(rx.mergeMap((x) => b.pipe(rx.map((y) => x + y)))),
  });
  prop2('mergeMap with concurrent = 2', {
    ours: ({ a, b }) => a.pipe(fp.mergeMap((x: string) => b.pipe(fp.map((y) => x + y)), 2)),
    theirs: ({ a, b }) => a.pipe(rx.mergeMap((x) => b.pipe(rx.map((y) => x + y)), 2)),
  });
  prop2('concatMap', {
    ours: ({ a, b }) => a.pipe(fp.concatMap((x: string) => b.pipe(fp.map((y) => x + y)))),
    theirs: ({ a, b }) => a.pipe(rx.concatMap((x) => b.pipe(rx.map((y) => x + y)))),
  });
  prop2('switchMap', {
    ours: ({ a, b }) => a.pipe(fp.switchMap((x: string) => b.pipe(fp.map((y) => x + y)))),
    theirs: ({ a, b }) => a.pipe(rx.switchMap((x) => b.pipe(rx.map((y) => x + y)))),
  });
  prop2('exhaustMap', {
    ours: ({ a, b }) => a.pipe(fp.exhaustMap((x: string) => b.pipe(fp.map((y) => x + y)))),
    theirs: ({ a, b }) => a.pipe(rx.exhaustMap((x) => b.pipe(rx.map((y) => x + y)))),
  });
  prop2('mergeAll / concatAll / switchAll / exhaustAll', {
    ours: ({ a, b }) => {
      const inner = a.pipe(fp.map((x) => b.pipe(fp.map((y) => x + y))));
      return fp.merge(
        inner.pipe(fp.mergeAll<string>(), fp.map((v) => `m${v}`)),
        inner.pipe(fp.concatAll<string>(), fp.map((v) => `c${v}`)),
        inner.pipe(fp.switchAll<string>(), fp.map((v) => `s${v}`)),
        inner.pipe(fp.exhaustAll<string>(), fp.map((v) => `e${v}`)),
      );
    },
    theirs: ({ a, b }) => {
      const inner = a.pipe(rx.map((x) => b.pipe(rx.map((y) => x + y))));
      return rx.merge(
        inner.pipe(rx.mergeAll(), rx.map((v) => `m${v}`)),
        inner.pipe(rx.concatAll(), rx.map((v) => `c${v}`)),
        inner.pipe(rx.switchAll(), rx.map((v) => `s${v}`)),
        inner.pipe(rx.exhaustAll(), rx.map((v) => `e${v}`)),
      );
    },
  });
  prop2('merge / concat', {
    ours: ({ a, b }) => fp.merge(fp.concat(a, b).pipe(fp.map((v) => `c${v}`)), fp.merge(a, b)),
    theirs: ({ a, b }) => rx.merge(rx.concat(a, b).pipe(rx.map((v) => `c${v}`)), rx.merge(a, b)),
  });
  prop2('mergeWith / concatWith', {
    ours: ({ a, b }) => fp.merge(a.pipe(fp.mergeWith(b)), a.pipe(fp.concatWith(b), fp.map((v) => `c${v}`))),
    theirs: ({ a, b }) => rx.merge(a.pipe(rx.mergeWith(b)), a.pipe(rx.concatWith(b), rx.map((v) => `c${v}`))),
  });
  prop2('startWith / endWith (internal concat in combine.ts)', {
    ours: ({ a }) => a.pipe(fp.startWith('s'), fp.endWith('e')),
    theirs: ({ a }) => a.pipe(rx.startWith('s'), rx.endWith('e')),
  });
});

describe('time vocabulary matches RxJS (identity tests, docs/kernel.md)', () => {
  prop1('delay', { ours: ({ a }, s) => a.pipe(fp.delay(3, s)), theirs: ({ a }, s) => a.pipe(rx.delay(3, s)) });
  prop1('debounceTime', { ours: ({ a }, s) => a.pipe(fp.debounceTime(2, s)), theirs: ({ a }, s) => a.pipe(rx.debounceTime(2, s)) });
  prop1('throttleTime', { ours: ({ a }, s) => a.pipe(fp.throttleTime(3, s)), theirs: ({ a }, s) => a.pipe(rx.throttleTime(3, s)) });
  prop1('throttleTimeTrailing', {
    ours: ({ a }, s) => a.pipe(fp.throttleTimeTrailing(3, s)),
    theirs: ({ a }, s) => a.pipe(rx.throttleTime(3, s, { leading: false, trailing: true })),
  });
  prop1('throttleTimeLeadingAndTrailing', {
    ours: ({ a }, s) => a.pipe(fp.throttleTimeLeadingAndTrailing(3, s)),
    theirs: ({ a }, s) => a.pipe(rx.throttleTime(3, s, { leading: true, trailing: true })),
  });
  prop1('auditTime', { ours: ({ a }, s) => a.pipe(fp.auditTime(3, s)), theirs: ({ a }, s) => a.pipe(rx.auditTime(3, s)) });
  prop1('sampleTime (the tie-ordering fix)', {
    ours: ({ a }, s) => a.pipe(fp.sampleTime(3, s)),
    theirs: ({ a }, s) => a.pipe(rx.sampleTime(3, s)),
  });
  prop1('emitAfter as a projection: spacing values out with concatMap', {
    ours: ({ a }, s) => a.pipe(fp.concatMap(fp.emitAfter(2, s))),
    theirs: ({ a }, s) => a.pipe(rx.concatMap((x) => rx.timer(2, s).pipe(rx.map(() => x)))),
  });
});

describe('values vocabulary matches RxJS (identity tests, docs/kernel.md)', () => {
  const isC = (x: string) => x === 'c';
  prop1('as / withIndex', {
    ours: ({ a }) => fp.merge(a.pipe(fp.as('z')), a.pipe(fp.withIndex(), fp.map(({ index, value }) => `${index}${value}`))),
    theirs: ({ a }) => rx.merge(a.pipe(rx.map(() => 'z')), a.pipe(rx.map((value, index) => `${index}${value}`))),
  });
  prop1('skip', { ours: ({ a }) => a.pipe(fp.skip(2)), theirs: ({ a }) => a.pipe(rx.skip(2)) });
  prop1('ignoreElements', { ours: ({ a }) => a.pipe(fp.ignoreElements()), theirs: ({ a }) => a.pipe(rx.ignoreElements()) });
  prop1('count / count with predicate', {
    ours: ({ a }) => fp.merge(a.pipe(fp.count()), a.pipe(fp.count(isC), fp.map((n) => `c${n}`))),
    theirs: ({ a }) => rx.merge(a.pipe(rx.count()), a.pipe(rx.count(isC), rx.map((n) => `c${n}`))),
  });
  prop1('toArray', { ours: ({ a }) => a.pipe(fp.toArray()), theirs: ({ a }) => a.pipe(rx.toArray()) });
  prop1('first: plain, with predicate, with default', {
    ours: ({ a }) =>
      fp.merge(a.pipe(fp.first(), fp.map((v) => `1${v}`), fp.catchError(() => fp.of('E1'))),
        a.pipe(fp.first(isC), fp.map((v) => `2${v}`), fp.catchError(() => fp.of('E2'))),
        a.pipe(fp.first(isC, 'd'), fp.map((v) => `3${v}`))),
    theirs: ({ a }) =>
      rx.merge(a.pipe(rx.first(), rx.map((v) => `1${v}`), rx.catchError(() => rx.of('E1'))),
        a.pipe(rx.first(isC), rx.map((v) => `2${v}`), rx.catchError(() => rx.of('E2'))),
        a.pipe(rx.first(isC, 'd'), rx.map((v) => `3${v}`))),
  });
  prop1('first: the EmptyError itself', { ours: ({ a }) => a.pipe(fp.first(isC)), theirs: ({ a }) => a.pipe(rx.first(isC)) });
  prop1('last: plain, with predicate, with default', {
    ours: ({ a }) =>
      fp.merge(a.pipe(fp.last(), fp.map((v) => `1${v}`), fp.catchError(() => fp.of('E1'))),
        a.pipe(fp.last(isC), fp.map((v) => `2${v}`), fp.catchError(() => fp.of('E2'))),
        a.pipe(fp.last(isC, 'd'), fp.map((v) => `3${v}`))),
    theirs: ({ a }) =>
      rx.merge(a.pipe(rx.last(), rx.map((v) => `1${v}`), rx.catchError(() => rx.of('E1'))),
        a.pipe(rx.last(isC), rx.map((v) => `2${v}`), rx.catchError(() => rx.of('E2'))),
        a.pipe(rx.last(isC, 'd'), rx.map((v) => `3${v}`))),
  });
  prop1('last: the EmptyError itself', { ours: ({ a }) => a.pipe(fp.last(isC)), theirs: ({ a }) => a.pipe(rx.last(isC)) });
  prop1('elementAt / elementAt with default', {
    ours: ({ a }) => fp.merge(a.pipe(fp.elementAt(2), fp.catchError(() => fp.of('E'))), a.pipe(fp.elementAt(3, 'd'), fp.map((v) => `d${v}`))),
    theirs: ({ a }) => rx.merge(a.pipe(rx.elementAt(2), rx.catchError(() => rx.of('E'))), a.pipe(rx.elementAt(3, 'd'), rx.map((v) => `d${v}`))),
  });
  prop1('elementAt: the ArgumentOutOfRangeError itself', { ours: ({ a }) => a.pipe(fp.elementAt(2)), theirs: ({ a }) => a.pipe(rx.elementAt(2)) });
  prop1('every / isEmpty / find / findIndex together', {
    ours: ({ a }) =>
      fp.merge(a.pipe(fp.every((x) => x !== 'c'), fp.map((v) => `e${v}`)), a.pipe(fp.isEmpty(), fp.map((v) => `i${v}`)),
        a.pipe(fp.find(isC), fp.map((v) => `f${v}`)), a.pipe(fp.findIndex(isC), fp.map((v) => `x${v}`))),
    theirs: ({ a }) =>
      rx.merge(a.pipe(rx.every((x) => x !== 'c'), rx.map((v) => `e${v}`)), a.pipe(rx.isEmpty(), rx.map((v) => `i${v}`)),
        a.pipe(rx.find(isC), rx.map((v) => `f${v}`)), a.pipe(rx.findIndex(isC), rx.map((v) => `x${v}`))),
  });
});

describe('creation vocabulary matches RxJS (identity tests, docs/kernel.md)', () => {
  it('of / range, including empty and negative counts', () => {
    fc.assert(
      fc.property(fc.array(fc.string({ maxLength: 2 }), { maxLength: 6 }), fc.integer({ min: -3, max: 10 }), fc.integer({ min: -2, max: 6 }), (xs, start, n) => {
        same({}, { ours: () => fp.merge(fp.of(...xs), fp.range(start, n)), theirs: () => rx.merge(rx.of(...xs), rx.range(start, n)) });
      }),
      { numRuns: RUNS },
    );
  });
  it('interval', () => {
    fc.assert(
      fc.property(fc.integer({ min: 1, max: 5 }), fc.integer({ min: 0, max: 6 }), (ms, n) => {
        same({}, { ours: (_, s) => fp.interval(ms, s).pipe(fp.take(n)), theirs: (_, s) => rx.interval(ms, s).pipe(rx.take(n)) });
      }),
      { numRuns: RUNS },
    );
  });
  prop2('iif chooses at subscription time', {
    ours: ({ a, b }) => {
      let flip = false;
      const chosen = fp.iif(() => (flip = !flip), a, b);
      return fp.concat(chosen, chosen);
    },
    theirs: ({ a, b }) => {
      let flip = false;
      const chosen = rx.iif(() => (flip = !flip), a, b);
      return rx.concat(chosen, chosen);
    },
  });
});

describe('combination vocabulary matches RxJS (identity tests, docs/kernel.md)', () => {
  prop2('takeUntil', { ours: ({ a, b }) => a.pipe(fp.takeUntil(b)), theirs: ({ a, b }) => a.pipe(rx.takeUntil(b)) });
  it('takeUntil: a notifier that emits at subscription stops the source before it is subscribed', () => {
    const run = (subscribeTo: (onSubscribe: () => void) => { unsubscribe: () => void }) => {
      let subscribed = false;
      subscribeTo(() => (subscribed = true)).unsubscribe();
      return subscribed;
    };
    const ours = run((on) => fp.defer(() => (on(), fp.of(1))).pipe(fp.takeUntil(fp.of('x'))).subscribe({}));
    const theirs = run((on) => rx.defer(() => (on(), rx.of(1))).pipe(rx.takeUntil(rx.of('x'))).subscribe({}));
    expect(ours).toBe(theirs);
    expect(ours).toBe(false);
  });
  prop2('forkJoin', { ours: ({ a, b }) => fp.forkJoin([a, b]), theirs: ({ a, b }) => rx.forkJoin([a, b]) });
  it('forkJoin of no sources completes at once', () => {
    same({}, { ours: () => fp.forkJoin([]), theirs: () => rx.forkJoin([]) });
  });
  prop2('onErrorResumeNext / onErrorResumeNextWith', {
    ours: ({ a, b }) => fp.merge(fp.onErrorResumeNext(a, b), a.pipe(fp.onErrorResumeNextWith(b), fp.map((v) => `w${v}`))),
    theirs: ({ a, b }) => rx.merge(rx.onErrorResumeNext(a, b), a.pipe(rx.onErrorResumeNextWith(b), rx.map((v) => `w${v}`))),
  });
  // emitting inputs only for combineLatestWith: `combineLatest` deliberately completes
  // early when an input completes without a value (see additions.test.ts)
  prop2('combineLatestWith / zipWith / raceWith', {
    ours: ({ a, b }) =>
      fp.merge(a.pipe(fp.combineLatestWith(b), fp.map((t) => `c${t.join('')}`)), a.pipe(fp.zipWith(b), fp.map((t) => `z${t.join('')}`)),
        a.pipe(fp.raceWith(b), fp.map((v) => `r${v}`))),
    theirs: ({ a, b }) =>
      rx.merge(a.pipe(rx.combineLatestWith(b), rx.map((t) => `c${t.join('')}`)), a.pipe(rx.zipWith(b), rx.map((t) => `z${t.join('')}`)),
        a.pipe(rx.raceWith(b), rx.map((v) => `r${v}`))),
  }, emittingMarbleArb(), emittingMarbleArb(8, 'xy'));
  prop2('combineLatestAll / zipAll', {
    ours: ({ a, b }) => {
      const inners = a.pipe(fp.map((x) => b.pipe(fp.map((y) => x + y))));
      return fp.merge(inners.pipe(fp.combineLatestAll<string>(), fp.map((t) => `c${t.join('')}`)), inners.pipe(fp.zipAll<string>(), fp.map((t) => `z${t.join('')}`)));
    },
    theirs: ({ a, b }) => {
      const inners = a.pipe(rx.map((x) => b.pipe(rx.map((y) => x + y))));
      return rx.merge(inners.pipe(rx.combineLatestAll(), rx.map((t) => `c${t.join('')}`)), inners.pipe(rx.zipAll(), rx.map((t) => `z${t.join('')}`)));
    },
  }, emittingMarbleArb(), emittingMarbleArb(8, 'xy'));
  prop1('partition', {
    ours: ({ a }) => {
      const [cs, rest] = fp.partition(a, (x) => x === 'c');
      return fp.merge(cs, rest.pipe(fp.map((v) => `r${v}`)));
    },
    theirs: ({ a }) => {
      const [cs, rest] = rx.partition(a, (x) => x === 'c');
      return rx.merge(cs, rest.pipe(rx.map((v) => `r${v}`)));
    },
  });
});

describe('errors vocabulary matches RxJS (identity tests, docs/kernel.md)', () => {
  prop1('onErrorReturn', {
    ours: ({ a }) => a.pipe(fp.onErrorReturn(() => 'fallback')),
    theirs: ({ a }) => a.pipe(rx.catchError(() => rx.of('fallback'))),
  });
  prop1('exponentialBackoff', {
    ours: ({ a }, s) => a.pipe(fp.retryWith(fp.exponentialBackoff(3, 1), s)),
    theirs: ({ a }, s) => a.pipe(rx.retry({ count: 3, delay: (_e, n) => rx.timer(2 ** (n - 1), s) })),
  });
});

describe('former outliers match RxJS (identity tests, docs/kernel.md)', () => {
  prop1('pairwise', { ours: ({ a }) => a.pipe(fp.pairwise(), fp.map((p) => p.join(''))), theirs: ({ a }) => a.pipe(rx.pairwise(), rx.map((p) => p.join(''))) });
  prop1('bufferCount / bufferCount every', {
    ours: ({ a }) => fp.merge(a.pipe(fp.bufferCount(2), fp.map((b) => b.join(''))), a.pipe(fp.bufferCount(3, 1), fp.map((b) => `e${b.join('')}`))),
    theirs: ({ a }) => rx.merge(a.pipe(rx.bufferCount(2), rx.map((b) => b.join(''))), a.pipe(rx.bufferCount(3, 1), rx.map((b) => `e${b.join('')}`))),
  });
  prop1('filterMap', {
    ours: ({ a }) => a.pipe(fp.filterMap((x: string, i) => (x === 'c' ? none : some(`${x}${i}`)))),
    theirs: ({ a }) => a.pipe(rx.map((x, i) => [x, i] as const), rx.filter(([x]) => x !== 'c'), rx.map(([x, i]) => `${x}${i}`)),
  });
  prop2('skipUntil', { ours: ({ a, b }) => a.pipe(fp.skipUntil(b)), theirs: ({ a, b }) => a.pipe(rx.skipUntil(b)) });
  for (const [name, ours, theirs] of [
    ['completes empty', () => fp.EMPTY, () => rx.EMPTY],
    ['never closes', () => fp.NEVER, () => rx.NEVER],
    ['errors', () => fp.throwError(() => 'closing'), () => rx.throwError(() => 'closing')],
  ] as const)
    prop2(`bufferToggle when the closing ${name}`, {
      ours: ({ a, b }) => a.pipe(fp.bufferToggle(b, ours), fp.map((x) => x.join(''))),
      theirs: ({ a, b }) => a.pipe(rx.bufferToggle(b, theirs), rx.map((x) => x.join(''))),
    });
  it('generate, with and without a scheduler', () => {
    fc.assert(
      fc.property(fc.integer({ min: -2, max: 6 }), fc.boolean(), (n, withScheduler) => {
        same({}, {
          ours: (_, s) => fp.generate(0, (i) => i < n, (i) => i + 1, withScheduler ? s : undefined),
          theirs: (_, s) => rx.generate(0, (i) => i < n, (i) => i + 1, withScheduler ? s : undefined),
        });
      }),
      { numRuns: RUNS },
    );
  });
  it('empty() and never()', () => {
    same({}, { ours: () => fp.merge(fp.empty(), fp.of('x')), theirs: () => rx.merge(rx.EMPTY, rx.of('x')) });
    same({}, { ours: (_, s) => fp.never().pipe(fp.takeUntil(fp.timer(3, undefined, s))), theirs: (_, s) => rx.NEVER.pipe(rx.takeUntil(rx.timer(3, s))) });
  });
  it('fromEvent on a Node-style emitter and a DOM-style target: same events, listener removed on unsubscribe', () => {
    const nodeLike = () => {
      const ls = new Set<(e: string) => void>();
      return { ls, on: (_t: string | symbol, l: (e: string) => void) => ls.add(l), off: (_t: string | symbol, l: (e: string) => void) => ls.delete(l), emit: (e: string) => ls.forEach((l) => l(e)) };
    };
    const domLike = () => {
      const ls = new Set<(e: string) => void>();
      return { ls, addEventListener: (_t: string, l: (e: string) => void) => ls.add(l), removeEventListener: (_t: string, l: (e: string) => void) => ls.delete(l), emit: (e: string) => ls.forEach((l) => l(e)) };
    };
    for (const make of [nodeLike, domLike]) {
      const run = (subscribe: (t: ReturnType<typeof make>, out: string[]) => { unsubscribe: () => void }) => {
        const t = make();
        const out: string[] = [];
        t.emit('before');
        const sub = subscribe(t, out);
        const during = t.ls.size;
        t.emit('a');
        t.emit('b');
        sub.unsubscribe();
        t.emit('after');
        return { out, during, after: t.ls.size };
      };
      const ours = run((t, out) => fp.fromEvent<string>(t as never, 'x').subscribe((e) => out.push(e)));
      const theirs = run((t, out) => rx.fromEvent<string>(t as never, 'x').subscribe((e) => out.push(e)));
      expect(ours).toEqual(theirs);
      expect(ours).toEqual({ out: ['a', 'b'], during: 1, after: 0 });
    }
  });
});
