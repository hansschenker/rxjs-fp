import { describe, expect, it, vi } from 'vitest';
import * as rx from 'rxjs';
import { observable } from '../src/core/observable.js';
import type { Observable, Observer, Sink } from '../src/core/types.js';
import * as reporting from '../src/core/report.js';
import { EMPTY, timer } from '../src/vocabulary/creation.js';
import { take } from '../src/operators/filtering.js';
import type { Scheduler } from '../src/scheduler/scheduler.js';
import {
  createBehaviorSubject,
  createReplaySubject,
  defaultSharePolicy,
  defaultShareReplayPolicy,
  share,
  shareReplay,
  type ResetPolicy,
  type SharePolicy,
  type ShareReplayPolicy,
} from '../src/subject/subject.js';
import { limited, positiveInteger, positiveMilliseconds, unlimited } from '../src/policies/core.js';

type Mode = 'ours' | 'native';
type Write = (...entry: unknown[]) => void;
type SubscriptionView = { readonly closed: boolean; readonly unsubscribe: () => void };
type View = { readonly subscribe: (observer: Partial<Observer<number>>) => SubscriptionView };
type ProducerSink = Pick<Sink<number>, 'next' | 'error' | 'complete' | 'closed'>;
type Sources = { readonly ours: Observable<number>; readonly native: rx.Observable<number> };

const pair = (producer: (sink: ProducerSink) => void | (() => void)): Sources => ({
  ours: observable<number>(producer),
  native: new rx.Observable<number>(producer),
});

const manual = (write: Write = () => {}): Sources & {
  next: (value: number) => void;
  error: (error: unknown) => void;
  complete: () => void;
  count: () => number;
} => {
  let current: ProducerSink | undefined;
  let connections = 0;
  return {
    ...pair((sink) => {
      current = sink;
      const run = ++connections;
      write('source-connect', run);
      return () => write('source-close', run);
    }),
    next: (value) => current?.next(value),
    error: (error) => current?.error(error),
    complete: () => current?.complete(),
    count: () => connections,
  };
};

const selectShare = (
  mode: Mode,
  source: Sources,
  policy: SharePolicy<number>,
  config: rx.ShareConfig<number>,
): View => mode === 'ours'
  ? source.ours.pipe(share(policy))
  : source.native.pipe(rx.share(config));

const selectReplay = (
  mode: Mode,
  source: Sources,
  policy: ShareReplayPolicy,
  config: rx.ShareReplayConfig,
  scheduler?: Scheduler,
): View => mode === 'ours'
  ? source.ours.pipe(shareReplay(policy, scheduler))
  : source.native.pipe(rx.shareReplay(config));

const on = (name: string, write: Write): Observer<number> => ({
  next: (value) => write(name, 'next', value),
  error: (error) => write(name, 'error', error),
  complete: () => write(name, 'complete'),
});

const same = <A>(scenario: (mode: Mode) => A): A => {
  const native = scenario('native');
  const ours = scenario('ours');
  expect(ours).toEqual(native);
  return ours;
};

const virtual = () => {
  const clock = new rx.VirtualTimeScheduler();
  const scheduler: Scheduler = {
    now: () => clock.now(),
    schedule: (work, delay = 0) => {
      const task = clock.schedule(work, delay);
      return () => task.unsubscribe();
    },
  };
  const trace: unknown[][] = [];
  const write: Write = (...entry) => trace.push([clock.now(), ...entry]);
  return { clock, scheduler, trace, write };
};

const reset = (enabled: boolean): ResetPolicy<[]> => ({ kind: enabled ? 'reset-now' : 'keep-state' });
const replayOne = (idle: ShareReplayPolicy['onNoSubscribers']): ShareReplayPolicy => ({
  replay: { maxValues: limited(positiveInteger(1)), maxAge: unlimited() },
  onNoSubscribers: idle,
});

describe('share directly interprets independent reset policies', () => {
  it('all eight reset combinations match native disconnect and terminal behavior', () => {
    for (const onError of [false, true]) for (const onComplete of [false, true]) {
      for (const onZero of [false, true]) for (const terminal of ['error', 'complete'] as const) {
        same((mode) => {
          const trace: unknown[][] = [];
          const write: Write = (...entry) => trace.push(entry);
          const source = manual(write);
          const shared = selectShare(mode, source, {
            ...defaultSharePolicy<number>(),
            onError: reset(onError),
            onComplete: reset(onComplete),
            onNoSubscribers: reset(onZero),
          }, { resetOnError: onError, resetOnComplete: onComplete, resetOnRefCountZero: onZero });
          const a = shared.subscribe(on('A', write));
          source.next(1);
          const b = shared.subscribe(on('B', write));
          source.next(2);
          a.unsubscribe();
          b.unsubscribe();
          const c = shared.subscribe(on('C', write));
          source.next(3);
          if (terminal === 'error') source.error('failure');
          else source.complete();
          const d = shared.subscribe(on('D', write));
          source.next(4);
          c.unsubscribe();
          d.unsubscribe();
          return { trace, connections: source.count() };
        });
      }
    }
  });

  it('synchronous take(1) disconnects immediately and a future subscriber starts fresh', () => {
    const result = same((mode) => {
      let connections = 0;
      let produced = 0;
      let teardowns = 0;
      const source = pair((sink) => {
        connections++;
        for (let value = 1; value <= 4 && !sink.closed; value++) {
          produced++;
          sink.next(value);
        }
        sink.complete();
        return () => { teardowns++; };
      });
      const shared = mode === 'ours'
        ? source.ours.pipe(share<number>(), take(1))
        : source.native.pipe(rx.share(), rx.take(1));
      const trace: unknown[][] = [];
      shared.subscribe(on('A', (...entry) => trace.push(entry)));
      shared.subscribe(on('B', (...entry) => trace.push(entry)));
      return { connections, produced, teardowns, trace };
    });
    expect(result).toMatchObject({ connections: 2, produced: 2, teardowns: 2 });
  });

  it('a returning observer cancels a pending zero-observer reset; a later departure starts another', () => {
    const result = same((mode) => {
      const { clock, scheduler, trace, write } = virtual();
      const source = manual(write);
      const shared = selectShare(mode, source, {
        ...defaultSharePolicy<number>(),
        onNoSubscribers: { kind: 'reset-on-next', signal: () => timer(5, undefined, scheduler) },
      }, { resetOnRefCountZero: () => rx.timer(5, clock) });
      const a = shared.subscribe(on('A', write));
      let b: SubscriptionView | undefined;
      clock.schedule(() => a.unsubscribe(), 1);
      clock.schedule(() => { b = shared.subscribe(on('B', write)); }, 3);
      clock.schedule(() => source.next(7), 7);
      clock.schedule(() => b?.unsubscribe(), 8);
      clock.schedule(() => {
        shared.subscribe(on('C', write));
        source.next(14);
        source.complete();
      }, 14);
      clock.flush();
      return { trace, connections: source.count() };
    });
    expect(result.connections).toBe(2);
    expect(result.trace).toContainEqual([13, 'source-close', 1]);
  });

  it('same-time reconnection/reset races follow scheduler order', () => {
    for (const returningFirst of [true, false]) {
      const result = same((mode) => {
        const { clock, scheduler, trace, write } = virtual();
        const source = manual(write);
        const shared = selectShare(mode, source, {
          ...defaultSharePolicy<number>(),
          onNoSubscribers: { kind: 'reset-on-next', signal: () => timer(5, undefined, scheduler) },
        }, { resetOnRefCountZero: () => rx.timer(5, clock) });
        const a = shared.subscribe(on('A', write));
        const returnAtFive = () => { shared.subscribe(on('B', write)); };
        if (returningFirst) clock.schedule(returnAtFive, 5);
        a.unsubscribe();
        if (!returningFirst) clock.schedule(returnAtFive, 5);
        clock.schedule(() => source.complete(), 6);
        clock.flush();
        return { trace, connections: source.count() };
      });
      expect(result.connections).toBe(returningFirst ? 1 : 2);
    }
  });

  it('source completion cancels an idle reset and applies the separate completion policy', () => {
    const result = same((mode) => {
      const { clock, scheduler, trace, write } = virtual();
      const source = manual(write);
      const shared = selectShare(mode, source, {
        ...defaultSharePolicy<number>(),
        onComplete: { kind: 'keep-state' },
        onNoSubscribers: { kind: 'reset-on-next', signal: () => timer(5, undefined, scheduler) },
      }, { resetOnComplete: false, resetOnRefCountZero: () => rx.timer(5, clock) });
      const a = shared.subscribe(on('A', write));
      clock.schedule(() => a.unsubscribe(), 1);
      clock.schedule(() => source.complete(), 2);
      clock.schedule(() => { shared.subscribe(on('B', write)); }, 7);
      clock.flush();
      return { trace, connections: source.count() };
    });
    expect(result.connections).toBe(1);
    expect(result.trace).toContainEqual([7, 'B', 'complete']);
  });

  it('late observers do not cancel a pending terminal reset, and error factories receive the source error', () => {
    for (const terminal of ['error', 'complete'] as const) {
      const result = same((mode) => {
        const { clock, scheduler, trace, write } = virtual();
        const source = manual(write);
        const errors: unknown[] = [];
        const shared = selectShare(mode, source, {
          ...defaultSharePolicy<number>(),
          connector: () => createReplaySubject<number>(1),
          onError: { kind: 'reset-on-next', signal: (error) => {
            errors.push(error);
            return timer(5, undefined, scheduler);
          } },
          onComplete: { kind: 'reset-on-next', signal: () => timer(5, undefined, scheduler) },
        }, {
          connector: () => new rx.ReplaySubject<number>(1),
          resetOnError: (error) => { errors.push(error); return rx.timer(5, clock); },
          resetOnComplete: () => rx.timer(5, clock),
        });
        shared.subscribe(on('A', write));
        source.next(1);
        if (terminal === 'error') source.error('failure');
        else source.complete();
        clock.schedule(() => { shared.subscribe(on('B', write)); }, 3);
        clock.schedule(() => {
          const c = shared.subscribe(on('C', write));
          source.next(6);
          c.unsubscribe();
        }, 6);
        clock.flush();
        return { trace, errors, connections: source.count() };
      });
      expect(result.connections).toBe(2);
      expect(result.errors).toEqual(terminal === 'error' ? ['failure'] : []);
      expect(result.trace).toContainEqual([3, 'B', 'next', 1]);
    }
  });

  it('reset notifier completion alone retains the terminal state', () => {
    const result = same((mode) => {
      let connections = 0;
      const source = pair((sink) => { sink.next(++connections); sink.complete(); });
      const shared = selectShare(mode, source, {
        ...defaultSharePolicy<number>(),
        onComplete: { kind: 'reset-on-next', signal: () => EMPTY },
      }, { resetOnComplete: () => rx.EMPTY });
      const trace: unknown[][] = [];
      shared.subscribe(on('A', (...entry) => trace.push(entry)));
      shared.subscribe(on('B', (...entry) => trace.push(entry)));
      return { connections, trace };
    });
    expect(result).toEqual({ connections: 1, trace: [['A', 'next', 1], ['A', 'complete'], ['B', 'complete']] });
  });

  it('a synchronous reset signal closes its actual iterator on the first next', () => {
    const result = same((mode) => {
      let connections = 0;
      let produced = 0;
      let closed = 0;
      const signal = function* () {
        try {
          produced++;
          yield 'reset';
          produced++;
          yield 'must not run';
        } finally { closed++; }
      };
      const source = pair((sink) => { sink.next(++connections); sink.complete(); });
      const shared = selectShare(mode, source, {
        ...defaultSharePolicy<number>(),
        onComplete: { kind: 'reset-on-next', signal },
      }, { resetOnComplete: signal });
      shared.subscribe({});
      shared.subscribe({});
      return { connections, produced, closed };
    });
    expect(result).toEqual({ connections: 2, produced: 2, closed: 2 });
  });

  it('reset signal errors and throwing factories use the unhandled-error route', () => {
    for (const factoryThrows of [false, true]) {
      same((mode) => {
        vi.useFakeTimers();
        const reported: unknown[] = [];
        const previous = rx.config.onUnhandledError;
        const spy = vi.spyOn(reporting, 'reportUnhandledError').mockImplementation((error) => { reported.push(error); });
        rx.config.onUnhandledError = (error) => { reported.push(error); };
        try {
          const source = pair((sink) => { sink.next(1); sink.complete(); });
          const ours = () => {
            if (factoryThrows) throw 'control failure';
            return observable<never>((sink) => sink.error('control failure'));
          };
          const theirs = () => {
            if (factoryThrows) throw 'control failure';
            return rx.throwError(() => 'control failure');
          };
          const shared = selectShare(mode, source, {
            ...defaultSharePolicy<number>(),
            onComplete: { kind: 'reset-on-next', signal: ours },
          }, { resetOnComplete: theirs });
          const trace: unknown[][] = [];
          const a = shared.subscribe(on('A', (...entry) => trace.push(entry)));
          const b = shared.subscribe(on('B', (...entry) => trace.push(entry)));
          vi.runAllTimers();
          a.unsubscribe();
          b.unsubscribe();
          return { trace, reported };
        } finally {
          spy.mockRestore();
          rx.config.onUnhandledError = previous;
          vi.useRealTimers();
        }
      });
    }
  });

  it('a synchronous reentrant observer shares the established source connection', () => {
    const result = same((mode) => {
      let connections = 0;
      const trace: unknown[][] = [];
      const source = pair((sink) => {
        connections++;
        sink.next(1);
        sink.next(2);
        sink.complete();
      });
      const shared = selectShare(mode, source, defaultSharePolicy<number>(), {});
      shared.subscribe({
        next: (value) => {
          trace.push(['A', value]);
          if (value === 1) shared.subscribe({ next: (v) => trace.push(['B', v]) });
        },
      });
      return { connections, trace };
    });
    expect(result).toEqual({ connections: 1, trace: [['A', 1], ['A', 2], ['B', 2]] });
  });

  it('a replayed initial value can close the only observer before the source connects', () => {
    const result = same((mode) => {
      let connections = 0;
      const source = pair((sink) => { connections++; sink.next(9); });
      const trace: unknown[] = [];
      if (mode === 'ours') {
        source.ours.pipe(share<number>({
          ...defaultSharePolicy<number>(),
          connector: () => createBehaviorSubject(5),
        }), take(1)).subscribe((value) => trace.push(value));
      } else {
        source.native.pipe(rx.share({ connector: () => new rx.BehaviorSubject(5) }), rx.take(1))
          .subscribe((value) => trace.push(value));
      }
      return { connections, trace };
    });
    expect(result).toEqual({ connections: 0, trace: [5] });
  });

  it('captures reset tags/factories and calls factories without a policy receiver', () => {
    for (const useSignal of [false, true]) {
      const result = same((mode) => {
        let connections = 0;
        const receivers: boolean[] = [];
        const signal = function (this: unknown) { receivers.push(this === undefined); return []; };
        const policy: SharePolicy<number> = {
          ...defaultSharePolicy<number>(),
          onComplete: useSignal ? { kind: 'reset-on-next', signal } : { kind: 'keep-state' },
        };
        const config: rx.ShareConfig<number> = { resetOnComplete: useSignal ? signal : false };
        const ours = share(policy);
        const theirs = rx.share(config);
        Object.assign(policy.onComplete, { kind: 'reset-now', signal: () => [0] });
        config.resetOnComplete = true;
        const source = pair((sink) => { sink.next(++connections); sink.complete(); });
        const shared: View = mode === 'ours' ? source.ours.pipe(ours) : source.native.pipe(theirs);
        shared.subscribe({});
        shared.subscribe({});
        return { connections, receivers };
      });
      expect(result).toEqual({ connections: 1, receivers: useSignal ? [true] : [] });
    }
  });
});

describe('shareReplay interprets replay bounds and idle policy', () => {
  it('both idle policies match native connection and cache behavior', () => {
    for (const refCount of [false, true]) {
      const result = same((mode) => {
        const trace: unknown[][] = [];
        const write: Write = (...entry) => trace.push(entry);
        const source = manual(write);
        const shared = selectReplay(mode, source, replayOne(refCount ? 'disconnect-and-reset' : 'keep-connected'), {
          bufferSize: 1, refCount,
        });
        const a = shared.subscribe(on('A', write));
        source.next(1);
        a.unsubscribe();
        source.next(2);
        shared.subscribe(on('B', write));
        source.next(3);
        source.complete();
        shared.subscribe(on('C', write));
        return { trace, connections: source.count() };
      });
      expect(result.connections).toBe(refCount ? 2 : 1);
      expect(result.trace).toContainEqual(['C', 'next', 3]);
    }
  });

  it('successful completion remains cached even with disconnect-and-reset', () => {
    const result = same((mode) => {
      let connections = 0;
      const source = pair((sink) => { sink.next(++connections); sink.complete(); });
      const shared = selectReplay(mode, source, replayOne('disconnect-and-reset'), { bufferSize: 1, refCount: true });
      const trace: unknown[][] = [];
      shared.subscribe(on('A', (...entry) => trace.push(entry)));
      shared.subscribe(on('B', (...entry) => trace.push(entry)));
      return { connections, trace };
    });
    expect(result).toEqual({ connections: 1, trace: [['A', 'next', 1], ['A', 'complete'], ['B', 'next', 1], ['B', 'complete']] });
  });

  it('source errors reset replay state for the next observer', () => {
    const result = same((mode) => {
      let connections = 0;
      const source = pair((sink) => { sink.next(++connections); sink.error('failed'); });
      const shared = selectReplay(mode, source, defaultShareReplayPolicy(), { refCount: false });
      const trace: unknown[][] = [];
      shared.subscribe(on('A', (...entry) => trace.push(entry)));
      shared.subscribe(on('B', (...entry) => trace.push(entry)));
      return { connections, trace };
    });
    expect(result.connections).toBe(2);
    expect(result.trace).toContainEqual(['B', 'next', 2]);
  });

  it('count and age limits apply together, expiry does not refresh, and the scheduler only supplies time', () => {
    const result = same((mode) => {
      let now = 0;
      const clock: Scheduler = {
        now: () => now,
        schedule: () => { throw new Error('replay must not schedule delivery'); },
      };
      const nativeClock = clock as unknown as rx.SchedulerLike;
      const trace: unknown[][] = [];
      const write: Write = (...entry) => trace.push([now, ...entry]);
      const source = manual();
      const policy: ShareReplayPolicy = {
        replay: { maxValues: limited(positiveInteger(2)), maxAge: limited(positiveMilliseconds(5)) },
        onNoSubscribers: 'disconnect-and-reset',
      };
      const shared = selectReplay(mode, source, policy, { bufferSize: 2, windowTime: 5, refCount: true, scheduler: nativeClock }, clock);
      shared.subscribe(on('A', write));
      source.next(1);
      now = 2;
      source.next(2);
      now = 4;
      source.next(3);
      source.complete();
      now = 7;
      shared.subscribe(on('B', write));
      now = 10;
      shared.subscribe(on('C', write));
      return { trace, connections: source.count() };
    });
    expect(result.connections).toBe(1);
    expect(result.trace.filter((entry) => entry[1] === 'B')).toEqual([[7, 'B', 'next', 3], [7, 'B', 'complete']]);
    expect(result.trace.filter((entry) => entry[1] === 'C')).toEqual([[10, 'C', 'complete']]);
  });

  it('reentrant live values reach an observer while it is receiving cached values', () => {
    const result = same((mode) => {
      const source = manual();
      const shared = selectReplay(mode, source, {
        replay: { maxValues: limited(positiveInteger(2)), maxAge: unlimited() },
        onNoSubscribers: 'keep-connected',
      }, { bufferSize: 2, refCount: false });
      const first: number[] = [];
      const second: number[] = [];
      shared.subscribe({ next: (value) => first.push(value) });
      source.next(1);
      source.next(2);
      shared.subscribe({ next: (value) => {
        second.push(value);
        if (value === 1) source.next(3);
      } });
      source.complete();
      return { first, second };
    });
    expect(result).toEqual({ first: [1, 2, 3], second: [1, 3, 2] });
  });

  it('replay cancellation stops the remaining buffer and retains completed cache', () => {
    const result = same((mode) => {
      let connections = 0;
      const source = pair((sink) => { connections++; sink.next(1); sink.next(2); sink.complete(); });
      const values: number[] = [];
      if (mode === 'ours') {
        const shared = source.ours.pipe(shareReplay<number>(defaultShareReplayPolicy()));
        shared.subscribe();
        shared.pipe(take(1)).subscribe((value) => values.push(value));
        shared.subscribe((value) => values.push(value));
      } else {
        const shared = source.native.pipe(rx.shareReplay());
        shared.subscribe();
        shared.pipe(rx.take(1)).subscribe((value) => values.push(value));
        shared.subscribe((value) => values.push(value));
      }
      return { connections, values };
    });
    expect(result).toEqual({ connections: 1, values: [1, 1, 2] });
  });
});
