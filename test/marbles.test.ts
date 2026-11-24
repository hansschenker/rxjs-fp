import { describe, expect, it } from 'vitest';
import fc from 'fast-check';
import { TestScheduler } from 'rxjs/testing';
import { of } from '../src/index.js';
import { parseMarbles, runMarbles } from '../src/testing/index.js';
import { marbleArb, RUNS } from './differential.js';

/**
 * `parseMarbles` is pure, so it is tested here on its own: once against the
 * grammar in its doc comment, once against RxJS's own parser. `runMarbles`
 * is covered everywhere else; here only its failure report is pinned.
 */

/** Our parse as `frame:kind:payload` strings, frames relative to `^` as `hot` sees them. */
const ours = (m: string, values?: Readonly<Record<string, unknown>>, error?: unknown): string[] => {
  const { events, subscribeFrame } = parseMarbles(m, values, error);
  const offset = Math.max(0, subscribeFrame);
  return events.map((e) =>
    e.kind === 'N' ? `${e.frame - offset}:N:${String(e.value)}` : e.kind === 'E' ? `${e.frame - offset}:E:${String(e.error)}` : `${e.frame - offset}:C`,
  );
};

/** RxJS's parse of the same diagram, inside `run()` so one character is one frame there too. */
const theirs = (m: string): string[] =>
  new TestScheduler(() => undefined).run(() =>
    TestScheduler.parseMarbles(m, undefined, undefined, false, true).map(({ frame, notification: n }) =>
      n.kind === 'N' ? `${frame}:N:${String(n.value)}` : n.kind === 'E' ? `${frame}:E:${String(n.error)}` : `${frame}:C`,
    ),
  );

/** Subscription marbles: RxJS says Infinity where we say -1 for an absent marker. */
const subscriptionOurs = (m: string): { readonly sub: number; readonly unsub: number } => {
  const { subscribeFrame, unsubscribeFrame } = parseMarbles(m);
  return { sub: subscribeFrame, unsub: unsubscribeFrame };
};
const subscriptionTheirs = (m: string): { readonly sub: number; readonly unsub: number } =>
  new TestScheduler(() => undefined).run(() => {
    const log = TestScheduler.parseMarblesAsSubscriptions(m, true);
    const finite = (f: number): number => (f === Infinity ? -1 : f);
    return { sub: finite(log.subscribedFrame), unsub: finite(log.unsubscribedFrame) };
  });

describe('parseMarbles: the grammar', () => {
  it('one character is one frame; values come from the record or the character itself', () => {
    expect(ours('a-b|')).toEqual(['0:N:a', '2:N:b', '3:C']);
    expect(ours('a-b|', { a: 1, b: 2 })).toEqual(['0:N:1', '2:N:2', '3:C']);
  });

  it('spaces are ignored, so diagrams can be aligned', () => {
    expect(ours('a -b |')).toEqual(ours('a-b|'));
  });

  it('# errors with the given error, "error" by default', () => {
    expect(ours('--#')).toEqual(['2:E:error']);
    expect(ours('--#', undefined, 'boom')).toEqual(['2:E:boom']);
  });

  it('a group stamps its events with the frame of "(" while every character still takes a frame', () => {
    expect(ours('(ab)|')).toEqual(['0:N:a', '0:N:b', '4:C']);
    expect(ours('(ab)-c|')).toEqual(['0:N:a', '0:N:b', '5:N:c', '6:C']);
    expect(ours('--(a|)')).toEqual(['2:N:a', '2:C']);
    // a '-' inside a group advances time like any other character (RxJS agrees)
    expect(ours('(a-b)|')).toEqual(['0:N:a', '0:N:b', '5:C']);
  });

  it('^ and ! record the subscription and unsubscription frames, -1 when absent', () => {
    expect(subscriptionOurs('--^--!')).toEqual({ sub: 2, unsub: 5 });
    expect(subscriptionOurs('^---')).toEqual({ sub: 0, unsub: -1 });
    expect(subscriptionOurs('---')).toEqual({ sub: -1, unsub: -1 });
    expect(subscriptionOurs('-(^!)')).toEqual({ sub: 1, unsub: 1 });
  });

  it('the three malformed groups throw and quote the diagram', () => {
    expect(() => parseMarbles('(a(b))')).toThrowError('nested group in "(a(b))"');
    expect(() => parseMarbles('a)')).toThrowError('unmatched ")" in "a)"');
    expect(() => parseMarbles('(ab')).toThrowError('unclosed group in "(ab"');
  });
});

describe('parseMarbles agrees with RxJS 7.8 TestScheduler.parseMarbles', () => {
  const diagrams = ['', '-', '|', '#', 'a|', 'a-b|', '--a--b--|', '-a-#', 'ab(cd)e|', '(ab)|', '(a-b)|', '(ab)-c|', '--(a|)', '-a-(b#)', '(a|)', 'a-^-b-|', '--^(ab)|', 'ab c|'];

  it.each(diagrams)('%j', (m) => {
    expect(ours(m)).toEqual(theirs(m));
  });

  it('property: random cold diagrams parse identically', () => {
    fc.assert(
      fc.property(marbleArb(), (m) => {
        expect(ours(m)).toEqual(theirs(m));
      }),
      { numRuns: RUNS },
    );
  });

  it.each(['^', '--^--!', '^---!', '(^!)', '--(^!)', '-^-(-!)', '---'])('subscription marbles %j', (m) => {
    expect(subscriptionOurs(m)).toEqual(subscriptionTheirs(m));
  });
});

describe('runMarbles reports every mismatch, not just the first', () => {
  it('two failed expectations both appear in the single Error', () => {
    let message = '';
    try {
      runMarbles(({ expectObservable }) => {
        expectObservable(of('a')).toBe('b|');
        expectObservable(of('c')).toBe('d|');
      });
    } catch (e) {
      message = (e as Error).message;
    }
    expect(message).toContain('Marble mismatch for "b|"');
    expect(message).toContain('Marble mismatch for "d|"');
    expect(message).toContain('expected: 0:"b"  1:|');
    expect(message).toContain('actual:   0:"a"  0:|');
  });
});
