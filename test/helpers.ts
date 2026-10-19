import fc from 'fast-check';
import { observable, type Observable } from '../src/index.js';
import { createVirtualScheduler, record, type RecordedEvent, type VirtualScheduler } from '../src/testing/index.js';

/** A timed stream specification: values at relative delays plus a terminal. */
export interface TimedSpec {
  readonly events: ReadonlyArray<{ readonly dt: number; readonly v: number }>;
  readonly end: 'complete' | 'error' | 'never';
  readonly endDt: number;
}

export const timedSpec = (maxLen = 5): fc.Arbitrary<TimedSpec> =>
  fc.record({
    events: fc.array(fc.record({ dt: fc.integer({ min: 0, max: 3 }), v: fc.integer({ min: -20, max: 20 }) }), { maxLength: maxLen }),
    end: fc.constantFrom<'complete' | 'error' | 'never'>('complete', 'complete', 'complete', 'error', 'never'),
    endDt: fc.integer({ min: 0, max: 3 }),
  });

/** Only completing streams (for laws that need termination, e.g. concat-based). */
export const completingSpec = (maxLen = 5): fc.Arbitrary<TimedSpec> =>
  timedSpec(maxLen).map((s) => ({ ...s, end: 'complete' as const }));

/** Builds a cold Observable from a spec, scheduled on `s`. */
export const fromSpec =
  (s: VirtualScheduler) =>
  (spec: TimedSpec): Observable<number> =>
    observable<number>((sink) => {
      const cancels: Array<() => void> = [];
      let t = 0;
      for (const e of spec.events) {
        t += e.dt;
        cancels.push(s.schedule(() => sink.next(e.v), t));
      }
      t += spec.endDt;
      if (spec.end === 'complete') cancels.push(s.schedule(() => sink.complete(), t));
      if (spec.end === 'error') cancels.push(s.schedule(() => sink.error('boom'), t));
      return () => cancels.forEach((c) => c());
    });

/** Runs `build` on a fresh virtual scheduler and returns the recorded notifications. */
export const run = <A>(build: (s: VirtualScheduler) => Observable<A>, maxTime = 500): RecordedEvent<A>[] => {
  const s = createVirtualScheduler();
  const out = record(build(s), s);
  s.flush(maxTime);
  return out;
};

/**
 * Normalizes a recording so that events in the same frame are compared as
 * a multiset (merge gives no ordering guarantee for simultaneous events).
 */
export const bySimultaneity = <A>(events: ReadonlyArray<RecordedEvent<A>>): string[] => {
  const groups = new Map<number, string[]>();
  for (const e of events) {
    const key = JSON.stringify(e);
    const g = groups.get(e.frame) ?? [];
    g.push(key);
    groups.set(e.frame, g);
  }
  return [...groups.entries()].sort((a, b) => a[0] - b[0]).flatMap(([, g]) => g.sort());
};

/** Collects synchronous values of an Observable. */
export const collect = <A>(o: Observable<A>): Array<A | '|' | `#${string}`> => {
  const out: Array<A | '|' | `#${string}`> = [];
  o.subscribe({
    next: (v) => out.push(v),
    error: (e) => out.push(`#${(e as Error)?.message ?? String(e)}`),
    complete: () => out.push('|'),
  });
  return out;
};
