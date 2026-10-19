import fc from 'fast-check';
import * as rx from 'rxjs';
import { TestScheduler } from 'rxjs/testing';
import type { Observable } from '../src/index.js';
import { runMarbles, record, type RecordedEvent } from '../src/testing/index.js';
import type { VirtualScheduler } from '../src/testing/index.js';

/**
 * Differential testing against RxJS 7.8: the same marble inputs are fed to
 * an rxjs-fp pipeline and to the equivalent RxJS pipeline, both on virtual
 * time, and the recorded notifications (frame, kind, value) must match.
 */

export interface Input {
  readonly marbles: string;
  readonly hot?: boolean;
}

type Rec = { frame: number; kind: string; value?: unknown; error?: string };

const normError = (e: unknown): string =>
  typeof e === 'object' && e !== null && 'name' in e && 'message' in e
    ? `${(e as Error).name}:${(e as Error).message}`
    : String(e);

const norm = (events: ReadonlyArray<RecordedEvent<unknown>>): Rec[] =>
  events.map((e) =>
    e.kind === 'N' ? { frame: e.frame, kind: 'N', value: e.value } : e.kind === 'E' ? { frame: e.frame, kind: 'E', error: normError(e.error) } : { frame: e.frame, kind: 'C' },
  );

export const runOurs = <K extends string>(
  inputs: Readonly<Record<K, Input>>,
  build: (i: Record<K, Observable<string>>, s: VirtualScheduler) => Observable<unknown>,
  maxFrames = 200,
): Rec[] => {
  let out: RecordedEvent<unknown>[] = [];
  runMarbles(
    ({ cold, hot, scheduler }) => {
      const obs = Object.fromEntries(
        Object.entries<Input>(inputs).map(([k, v]) => [k, v.hot ? hot(v.marbles) : cold(v.marbles)]),
      ) as Record<K, Observable<string>>;
      out = record(build(obs, scheduler), scheduler, 0, maxFrames);
    },
    { maxFrames },
  );
  return norm(out);
};

export const runTheirs = <K extends string>(
  inputs: Readonly<Record<K, Input>>,
  build: (i: Record<K, rx.Observable<string>>, s: rx.SchedulerLike) => rx.Observable<unknown>,
  maxFrames = 200,
): Rec[] => {
  const ts = new TestScheduler(() => true);
  ts.maxFrames = maxFrames;
  const out: Rec[] = [];
  ts.run(({ cold, hot }) => {
    const obs = Object.fromEntries(
      Object.entries<Input>(inputs).map(([k, v]) => [k, v.hot ? hot(v.marbles) : cold(v.marbles)]),
    ) as unknown as Record<K, rx.Observable<string>>;
    // run mode ignores maxFrames, so cut infinite pipelines off explicitly
    const sub = build(obs, ts).subscribe({
      next: (value) => out.push({ frame: ts.now(), kind: 'N', value }),
      error: (e) => out.push({ frame: ts.now(), kind: 'E', error: normError(e) }),
      complete: () => out.push({ frame: ts.now(), kind: 'C' }),
    });
    ts.schedule(() => sub.unsubscribe(), maxFrames);
  });
  return out;
};

/** Random cold marble diagrams: frames, values a–e, then complete, error or nothing. */
export const marbleArb = (maxLen = 10, alphabet = 'abcde'): fc.Arbitrary<string> =>
  fc
    .tuple(
      fc.array(fc.constantFrom('-', '-', ...alphabet.split('')), { maxLength: maxLen }),
      fc.constantFrom('|', '|', '|', '#', ''),
    )
    .map(([cs, end]) => cs.join('') + end);

/** Like `marbleArb`, but always completes (and never errors). */
export const completingMarbleArb = (maxLen = 10, alphabet = 'abcde'): fc.Arbitrary<string> =>
  fc.array(fc.constantFrom('-', '-', ...alphabet.split('')), { maxLength: maxLen }).map((cs) => cs.join('') + '|');

/** Fresh RxJS window/group flattening: each inner value tagged with its window index. */
export const tagWindowsRx = <A>(w$: rx.Observable<rx.Observable<A>>): rx.Observable<string> =>
  w$.pipe(rx.mergeMap((w, i) => w.pipe(rx.map((v) => `${i}:${String(v)}`))));

export const RUNS = Number(process.env['LAW_RUNS'] ?? 200);

/** Like `marbleArb`, but at least one value comes before the end. */
export const emittingMarbleArb = (maxLen = 10, alphabet = 'abcde'): fc.Arbitrary<string> =>
  fc.tuple(fc.constantFrom(...alphabet.split('')), marbleArb(maxLen, alphabet)).map(([v, m]) => v + m);
