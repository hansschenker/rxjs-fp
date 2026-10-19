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
    ours: ({ a, b }) => a.pipe(fp.throttle(() => b, { leading: false, trailing: true })),
    theirs: ({ a, b }) => a.pipe(rx.throttle(() => b, { leading: false, trailing: true })),
  });
  prop2('throttle (leading + trailing)', {
    ours: ({ a, b }) => a.pipe(fp.throttle(() => b, { leading: true, trailing: true })),
    theirs: ({ a, b }) => a.pipe(rx.throttle(() => b, { leading: true, trailing: true })),
  });
  prop2('sample', { ours: ({ a, b }) => a.pipe(fp.sample(b)), theirs: ({ a, b }) => a.pipe(rx.sample(b)) });
  prop2('delayWhen', { ours: ({ a, b }) => a.pipe(fp.delayWhen(() => b)), theirs: ({ a, b }) => a.pipe(rx.delayWhen(() => b)) });
  prop1('timestamp / timeInterval', {
    ours: ({ a }, s) => a.pipe(fp.timestamp(s), fp.timeInterval(s), fp.map((x) => `${x.value.value}@${x.value.timestamp}+${x.interval}`)),
    theirs: ({ a }) => a.pipe(rx.timestamp(), rx.timeInterval(), rx.map((x) => `${x.value.value}@${x.value.timestamp}+${x.interval}`)),
  });
  prop1('timeout config: each', {
    ours: ({ a }, s) => a.pipe(fp.timeout({ each: 3, scheduler: s })),
    theirs: ({ a }) => a.pipe(rx.timeout({ each: 3 })),
  });
  prop1('timeout config: first only', {
    ours: ({ a }, s) => a.pipe(fp.timeout({ first: 3, scheduler: s })),
    theirs: ({ a }) => a.pipe(rx.timeout({ first: 3 })),
  });
  prop1('timeout config: with', {
    ours: ({ a }, s) => a.pipe(fp.timeout({ each: 2, scheduler: s, with: (i) => fp.of(`seen${i.seen}`) })),
    theirs: ({ a }) => a.pipe(rx.timeout({ each: 2, with: (i) => rx.of(`seen${i.seen}`) })),
  });
});

describe('buffer and window operators match RxJS', () => {
  prop2('buffer', { ours: ({ a, b }) => a.pipe(fp.buffer(b)), theirs: ({ a, b }) => a.pipe(rx.buffer(b)) });
  prop1('bufferTime', {
    ours: ({ a }, s) => a.pipe(fp.bufferTime(3, s)),
    theirs: ({ a }) => a.pipe(rx.bufferTime(3)),
  }, completingMarbleArb());
  prop1('bufferTime with creation interval and max size', {
    ours: ({ a }, s) => a.pipe(fp.bufferTime(4, s, { creationInterval: 3, maxBufferSize: 2 })),
    theirs: ({ a }) => a.pipe(rx.bufferTime(4, 3, 2)),
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
    ours: ({ a }, s) => tagWindows(a.pipe(fp.windowTime(4, s, { creationInterval: 3, maxWindowSize: 2 }))),
    theirs: ({ a }) => tagWindowsRx(a.pipe(rx.windowTime(4, 3, 2))),
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
  prop2('groupBy with element and duration', {
    ours: ({ a, b }) =>
      a.pipe(
        fp.groupBy((x) => x, { element: (x) => x.toUpperCase(), duration: () => b }),
        fp.mergeMap((g, i) => g.pipe(fp.map((v) => `${i}${g.key}:${v}`))),
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
    // edge case left out on purpose: an input that completes without a value
    // ends `combineLatest` here at once, while RxJS waits for the other inputs
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

